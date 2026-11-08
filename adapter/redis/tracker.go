package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/danish-a1/goqueue/adapter"
	"github.com/redis/go-redis/v9"
)

var _ adapter.Tracker = (*RedisStore)(nil)

// RegisterQueue records the queue and its dead list key ("" = no Redis DLQ).
func (rs *RedisStore) RegisterQueue(ctx context.Context, queue, deadKey string) error {
	return rs.client.HSet(ctx, KeyQueues, queue, deadKey).Err()
}

// RecordResult bumps the global, daily and per-queue counters. Errors are
// logged and swallowed so tracking never affects job processing.
func (rs *RedisStore) RecordResult(ctx context.Context, queue string, success bool) {
	now := time.Now()
	kinds := []string{"processed"}
	if !success {
		kinds = append(kinds, "failed")
	}
	_, err := rs.client.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, kind := range kinds {
			total := KeyStatProcessed
			if kind == "failed" {
				total = KeyStatFailed
			}
			daily := KeyStatDaily(kind, now)
			p.Incr(ctx, total)
			p.Incr(ctx, daily)
			p.Expire(ctx, daily, DailyStatTTL)
			p.Incr(ctx, KeyStatQueue(queue, kind))
		}
		return nil
	})
	if err != nil {
		rs.logger.Error("failed to record job result", "queue", queue, "error", err)
	}
}

// Heartbeat refreshes the process hash (TTL HeartbeatTTL) and set membership.
func (rs *RedisStore) Heartbeat(ctx context.Context, p adapter.ProcessInfo) error {
	key := KeyProcess(p.ID)
	_, err := rs.client.Pipelined(ctx, func(pl redis.Pipeliner) error {
		pl.HSet(ctx, key,
			"host", p.Host,
			"pid", p.PID,
			"queue", p.Queue,
			"concurrency", p.Concurrency,
			"started_at", p.StartedAt.UTC().Unix(),
			"beat", time.Now().UTC().Unix(),
		)
		pl.Expire(ctx, key, HeartbeatTTL)
		pl.SAdd(ctx, KeyProcesses, p.ID)
		return nil
	})
	return err
}

// RemoveProcess drops the process from the set and deletes its hash and work hash.
func (rs *RedisStore) RemoveProcess(ctx context.Context, processID string) error {
	_, err := rs.client.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.SRem(ctx, KeyProcesses, processID)
		p.Del(ctx, KeyProcess(processID), KeyProcessWork(processID))
		return nil
	})
	return err
}

// JobStarted marks a job as busy on the process.
func (rs *RedisStore) JobStarted(ctx context.Context, processID, queue, jobID, jobName string) {
	raw, err := json.Marshal(map[string]string{
		"queue":      queue,
		"job_name":   jobName,
		"started_at": strconv.FormatInt(time.Now().UTC().Unix(), 10),
	})
	if err == nil {
		err = rs.client.HSet(ctx, KeyProcessWork(processID), jobID, raw).Err()
	}
	if err != nil {
		rs.logger.Error("failed to mark job started", "jobID", jobID, "error", err)
	}
}

// JobFinished clears the job from the process work hash.
func (rs *RedisStore) JobFinished(ctx context.Context, processID, jobID string) {
	if err := rs.client.HDel(ctx, KeyProcessWork(processID), jobID).Err(); err != nil {
		rs.logger.Error("failed to mark job finished", "jobID", jobID, "error", err)
	}
}
