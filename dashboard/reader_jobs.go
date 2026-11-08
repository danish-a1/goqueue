package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/danish-a1/goqueue/adapter/dlq"
	gqredis "github.com/danish-a1/goqueue/adapter/redis"
	"github.com/danish-a1/goqueue/job"
)

// Source selects which job list to read.
type Source string

const (
	SourcePending Source = "pending"
	SourceRetry   Source = "retry"
	SourceDead    Source = "dead"
	SourceDeleted Source = "deleted"
)

// JobRow is one job in any list; unused fields stay zero.
type JobRow struct {
	ID, JobName, Queue, Error              string
	RetryCount                             int
	EnqueuedAt, FailedAt, DueAt, DeletedAt time.Time
	DeletedBy, From                        string
	Args                                   json.RawMessage
}

// Jobs returns one page (per clamped 1..100) newest first and the list total.
// Retry is ordered by due time ascending. Malformed members are skipped.
func (r *reader) Jobs(ctx context.Context, queue string, src Source, page, per int) ([]JobRow, int64, error) {
	per = clamp(per, 1, 100)
	start := int64(max(page, 1)-1) * int64(per)
	end := start + int64(per) - 1
	deadKey, err := r.rdb.HGet(ctx, gqredis.KeyQueues, queue).Result()
	if err == redis.Nil {
		return nil, 0, fmt.Errorf("unknown queue %q", queue)
	}
	if err != nil {
		return nil, 0, err
	}
	switch src {
	case SourcePending:
		return r.listRows(ctx, gqredis.KeyPending(queue), queue, start, end, false, pendingRow)
	case SourceDead:
		if deadKey == "" {
			return nil, 0, nil
		}
		return r.listRows(ctx, deadKey, queue, start, end, true, deadRow)
	case SourceDeleted:
		return r.listRows(ctx, gqredis.KeyDeleted(queue), queue, start, end, true, deletedRow)
	case SourceRetry:
		return r.retryRows(ctx, queue, start, end)
	}
	return nil, 0, fmt.Errorf("unknown source %q", src)
}

type rowFn func(raw, queue string) (JobRow, bool)

func (r *reader) listRows(ctx context.Context, key, queue string, start, end int64, fromTail bool, fn rowFn) ([]JobRow, int64, error) {
	lo, hi := start, end
	if fromTail {
		lo, hi = -end-1, -start-1
	}
	var total *redis.IntCmd
	var items *redis.StringSliceCmd
	if _, err := r.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		total, items = p.LLen(ctx, key), p.LRange(ctx, key, lo, hi)
		return nil
	}); err != nil {
		return nil, 0, err
	}
	vals := items.Val()
	if fromTail {
		slices.Reverse(vals)
	}
	return decodeRows(vals, queue, fn), total.Val(), nil
}

func (r *reader) retryRows(ctx context.Context, queue string, start, end int64) ([]JobRow, int64, error) {
	key := gqredis.KeyRetry(queue)
	var total *redis.IntCmd
	var items *redis.ZSliceCmd
	if _, err := r.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		total, items = p.ZCard(ctx, key), p.ZRangeWithScores(ctx, key, start, end)
		return nil
	}); err != nil {
		return nil, 0, err
	}
	rows := make([]JobRow, 0, len(items.Val()))
	for _, z := range items.Val() {
		s, _ := z.Member.(string)
		if row, ok := pendingRow(s, queue); ok {
			row.DueAt = time.Unix(int64(z.Score), 0).UTC()
			rows = append(rows, row)
		}
	}
	return rows, total.Val(), nil
}

func decodeRows(vals []string, queue string, fn rowFn) []JobRow {
	rows := make([]JobRow, 0, len(vals))
	for _, v := range vals {
		if row, ok := fn(v, queue); ok {
			rows = append(rows, row)
		}
	}
	return rows
}

func pendingRow(raw, queue string) (JobRow, bool) {
	var j job.RedisQueuedJob
	if json.Unmarshal([]byte(raw), &j) != nil || j.ID == "" {
		return JobRow{}, false
	}
	row := JobRow{ID: j.ID, JobName: j.JobName, Queue: queue, RetryCount: j.RetryCount,
		EnqueuedAt: j.EnqueuedAt, Error: j.ErrorMessage, Args: j.Job}
	if j.FailedAt != nil {
		row.FailedAt = *j.FailedAt
	}
	return row, true
}

func deadRow(raw, queue string) (JobRow, bool) {
	e, err := dlq.DecodeEntry([]byte(raw))
	if err != nil || e.JobID == "" {
		return JobRow{}, false
	}
	if e.Queue == "" {
		e.Queue = queue
	}
	return JobRow{ID: e.JobID, JobName: e.JobName, Queue: e.Queue, Error: e.Error, FailedAt: e.FailedAt, Args: e.Job}, true
}

func deletedRow(raw, queue string) (JobRow, bool) {
	var d struct {
		Entry     json.RawMessage `json:"entry"`
		JobID     string          `json:"job_id"`
		Queue     string          `json:"queue"`
		From      string          `json:"from"`
		DeletedAt time.Time       `json:"deleted_at"`
		DeletedBy string          `json:"deleted_by_ip"`
	}
	if json.Unmarshal([]byte(raw), &d) != nil || d.JobID == "" {
		return JobRow{}, false
	}
	row, ok := pendingRow(string(d.Entry), queue)
	if !ok {
		row, _ = deadRow(string(d.Entry), queue)
	}
	row.ID, row.From, row.DeletedAt, row.DeletedBy = d.JobID, d.From, d.DeletedAt, d.DeletedBy
	if d.Queue != "" {
		row.Queue = d.Queue
	} else if row.Queue == "" {
		row.Queue = queue
	}
	return row, true
}
