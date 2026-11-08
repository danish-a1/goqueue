package dlq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/job"
	"github.com/redis/go-redis/v9"
)

const dlqPrefix = "dlq:"

type RedisDLQ struct {
	client *redis.Client
	logger logger.Logger
}

func NewRedisDLQ(client *redis.Client, logger logger.Logger) *RedisDLQ {
	return &RedisDLQ{
		client: client,
		logger: logger,
	}
}

// DLQKey returns the Redis list key holding dead jobs of queueName.
func (r *RedisDLQ) DLQKey(queueName string) string { return dlqPrefix + queueName }

// Push appends the dead job at the tail (newest last), keeps the newest
// MaxEntries and refreshes the list TTL.
func (r *RedisDLQ) Push(ctx context.Context, job *job.JobContext, jobErr error) error {
	entry, err := NewEntry(job, jobErr)
	if err != nil {
		r.logger.Error("Failed to build DLQ entry", "error", err)
		return err
	}

	entryData, err := json.Marshal(entry)
	if err != nil {
		r.logger.Error("Failed to marshal DLQ entry", "error", err)
		return fmt.Errorf("failed to marshal DLQ entry: %w", err)
	}

	dlqKey := r.DLQKey(job.QueueName)
	if err := r.client.RPush(ctx, dlqKey, entryData).Err(); err != nil {
		r.logger.Error("Failed to push to DLQ", "error", err)
		return fmt.Errorf("failed to push to DLQ: %w", err)
	}
	if err := r.client.LTrim(ctx, dlqKey, -MaxEntries, -1).Err(); err != nil {
		r.logger.Error("Failed to trim DLQ", "error", err, "queue", job.QueueName)
	}
	if err := r.client.Expire(ctx, dlqKey, EntryTTL).Err(); err != nil {
		r.logger.Error("Failed to set DLQ TTL", "error", err, "queue", job.QueueName)
	}

	r.logger.Info("Job pushed to DLQ", "jobID", job.JobID, "queue", job.QueueName)
	return nil
}
