package queue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/danish-a1/goqueue/adapter/dlq"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/job"
)

type recordingDLQ struct{}

func (r *recordingDLQ) Push(ctx context.Context, j *job.JobContext, err error) error { return nil }

func newRedisQueueConfig(t *testing.T) config.Config {
	t.Helper()
	mr := miniredis.RunT(t)
	return config.Config{
		Driver:           config.DriverRedis,
		DriverConfig:     config.RedisConfig{Addr: mr.Addr()},
		MaxWorkers:       1,
		ConcurrencyLimit: 1,
	}
}

func closeQueue(t *testing.T, q *Queue) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = q.Shutdown(ctx)
}

func TestNewQueueRedisKeepsCallerDLQAdapter(t *testing.T) {
	custom := &recordingDLQ{}
	cfg := newRedisQueueConfig(t).WithDLQAdapter(custom)

	q, err := NewQueue("keep_custom_dlq", cfg, time.Second)
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer closeQueue(t, q)

	if q.config.DLQAdapter != custom {
		t.Fatalf("expected caller DLQ adapter to be kept, got %T", q.config.DLQAdapter)
	}
}

func TestNewQueueRedisDefaultsToRedisDLQ(t *testing.T) {
	q, err := NewQueue("default_dlq", newRedisQueueConfig(t), time.Second)
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	defer closeQueue(t, q)

	if _, ok := q.config.DLQAdapter.(*dlq.RedisDLQ); !ok {
		t.Fatalf("expected default *dlq.RedisDLQ, got %T", q.config.DLQAdapter)
	}
}
