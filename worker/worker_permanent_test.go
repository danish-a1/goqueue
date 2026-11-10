package worker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danish-a1/goqueue/adapter/memory"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/registry"
	"github.com/danish-a1/goqueue/job"
)

var errRejected = errors.New("400 bad request")

var permanentRuns atomic.Int32

// PermanentFailJob fails with a wrapped permanent error, the way a handler
// wraps a 4xx from an API it calls.
type PermanentFailJob struct{}

func (j *PermanentFailJob) Process(context.Context) error {
	permanentRuns.Add(1)
	return fmt.Errorf("search contact: %w", job.Permanent(errRejected))
}

func registerPermanentFailJob() {
	defer func() { _ = recover() }()
	registry.Register("PermanentFailJob", func() job.Job { return &PermanentFailJob{} })
}

// A permanent error must skip the remaining retries and go straight to the
// DLQ, keeping the original error reachable for errors.Is.
func TestPermanentErrorSkipsRetriesAndDeadLetters(t *testing.T) {
	registerPermanentFailJob()
	permanentRuns.Store(0)
	queueName := "permanent_queue"
	dlq := &capturingDLQ{}
	cfg := config.Config{
		Driver: config.DriverMemory, MaxWorkers: 1, ConcurrencyLimit: 1,
		MaxRetryAttempts: 5, RetryDelay: 10 * time.Millisecond,
		DLQAdapter: dlq,
	}
	store := memory.NewInMemoryStore(queueName, cfg, logger.NewZapLogger())
	w := NewWorker(store, cfg, queueName, nil, logger.NewZapLogger())
	if err := store.Push(queueName, &PermanentFailJob{}); err != nil {
		t.Fatalf("push: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := w.Start(ctx, 1); err != nil {
		t.Fatalf("start: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(dlq.pushed()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // a scheduled retry would run by now
	cancel()
	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	_ = w.Shutdown(sctx)

	errs := dlq.pushed()
	if len(errs) != 1 {
		t.Fatalf("want 1 DLQ push, got %d", len(errs))
	}
	if !errors.Is(errs[0], errRejected) {
		t.Fatalf("DLQ error %v does not wrap the original error", errs[0])
	}
	if n := permanentRuns.Load(); n != 1 {
		t.Fatalf("permanent job ran %d times; want 1 (no retries)", n)
	}
}

func TestPermanentNilAndIsPermanent(t *testing.T) {
	if job.Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
	if job.IsPermanent(errRejected) {
		t.Fatal("plain error reported as permanent")
	}
	if !job.IsPermanent(fmt.Errorf("wrap: %w", job.Permanent(errRejected))) {
		t.Fatal("wrapped permanent error not detected")
	}
	if got := job.Permanent(errRejected).Error(); got != errRejected.Error() {
		t.Fatalf("Error() = %q, want the original message", got)
	}
}
