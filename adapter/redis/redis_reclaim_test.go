package redis

import (
	"context"
	"testing"

	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/manager"
)

func newReclaimStore(t *testing.T) (*RedisStore, func()) {
	t.Helper()
	mr, client := setupTestRedis(t)
	log := logger.NewZapLogger()
	mgr := manager.NewRedisClientManager(mr.Addr(), "", "", 0, log)
	cfg := config.NewRedisConfig(mr.Addr(), "", "", 0)
	rs := NewRedisStore(client, cfg, mgr, mr.Addr(), 0, log)
	return rs, func() {
		_ = rs.Stop()
		_ = client.Close()
		mr.Close()
	}
}

// Jobs left in processing:<queue> by a crashed process go back onto the queue.
func TestReclaimProcessingMovesJobsBackToQueue(t *testing.T) {
	rs, done := newReclaimStore(t)
	defer done()
	ctx := context.Background()

	if err := rs.client.RPush(ctx, processingQueueName+"jobs", `{"a":1}`, `{"b":2}`).Err(); err != nil {
		t.Fatalf("seed processing list: %v", err)
	}

	moved, err := rs.ReclaimProcessing("jobs")
	if err != nil {
		t.Fatalf("ReclaimProcessing: %v", err)
	}
	if moved != 2 {
		t.Fatalf("expected 2 jobs moved, got %d", moved)
	}
	if n := rs.client.LLen(ctx, processingQueueName+"jobs").Val(); n != 0 {
		t.Fatalf("expected empty processing list, got %d", n)
	}
	if n := rs.client.LLen(ctx, "jobs").Val(); n != 2 {
		t.Fatalf("expected 2 jobs back on the queue, got %d", n)
	}
}

func TestReclaimProcessingWithNothingToMove(t *testing.T) {
	rs, done := newReclaimStore(t)
	defer done()

	moved, err := rs.ReclaimProcessing("empty")
	if err != nil {
		t.Fatalf("ReclaimProcessing: %v", err)
	}
	if moved != 0 {
		t.Fatalf("expected 0 jobs moved, got %d", moved)
	}
}
