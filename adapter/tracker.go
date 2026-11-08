package adapter

import (
	"context"
	"time"
)

// ProcessInfo describes one running worker process for heartbeats.
type ProcessInfo struct {
	ID          string
	Host        string
	Queue       string
	PID         int
	Concurrency int
	StartedAt   time.Time
}

// Tracker is optionally implemented by stores that record queue metadata,
// counters and worker heartbeats (currently only the Redis store). Stores
// that do not implement it are simply not tracked.
type Tracker interface {
	// RegisterQueue records queue -> dead list key. overwrite=false keeps an
	// existing entry (producers); overwrite=true replaces it (consumers, which
	// know the real DLQ adapter).
	RegisterQueue(ctx context.Context, queue, deadKey string, overwrite bool) error
	// RecordResult counts one attempt: processed+1 always, failed+1 when !success.
	RecordResult(ctx context.Context, queue string, success bool)
	Heartbeat(ctx context.Context, p ProcessInfo) error
	RemoveProcess(ctx context.Context, processID string) error
	JobStarted(ctx context.Context, processID, queue, jobID, jobName string)
	JobFinished(ctx context.Context, processID, jobID string)
}
