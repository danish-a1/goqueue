package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danish-a1/goqueue/adapter/memory"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/registry"
	"github.com/danish-a1/goqueue/job"
)

// drainGate blocks in Process until released, ignoring ctx like a handler
// finishing a non-interruptible write.
var drainGate struct {
	started chan struct{}
	release chan struct{}
	done    atomic.Bool
}

type DrainJob struct{}

func (j *DrainJob) Process(context.Context) error {
	close(drainGate.started)
	<-drainGate.release
	drainGate.done.Store(true)
	return nil
}

func registerDrainJob() {
	defer func() { _ = recover() }()
	registry.Register("DrainJob", func() job.Job { return &DrainJob{} })
}

// Apps cancel the worker ctx (signal ctx) before calling Shutdown. The worker
// must still wait for the in-flight handler instead of abandoning it, so
// Shutdown only returns once the job really finished.
func TestShutdownWaitsForInFlightJobAfterCtxCancel(t *testing.T) {
	registerDrainJob()
	drainGate.started = make(chan struct{})
	drainGate.release = make(chan struct{})
	drainGate.done.Store(false)

	queueName := "drain_queue"
	dlq := &capturingDLQ{}
	cfg := config.Config{
		Driver: config.DriverMemory, MaxWorkers: 1, ConcurrencyLimit: 1, MaxRetryAttempts: 1,
		DLQAdapter: dlq,
	}
	store := memory.NewInMemoryStore(queueName, cfg, logger.NewZapLogger())
	w := NewWorker(store, cfg, queueName, nil, logger.NewZapLogger())
	if err := store.Push(queueName, &DrainJob{}); err != nil {
		t.Fatalf("push: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := w.Start(ctx, 1); err != nil {
		t.Fatalf("start: %v", err)
	}
	select {
	case <-drainGate.started:
	case <-time.After(3 * time.Second):
		t.Fatal("job never started")
	}

	cancel()
	release := drainGate.release
	time.AfterFunc(150*time.Millisecond, func() { close(release) })
	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	if err := w.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if !drainGate.done.Load() {
		t.Fatal("Shutdown returned before the in-flight job finished")
	}
	if errs := dlq.pushed(); len(errs) != 0 {
		t.Fatalf("completed job was dead-lettered as failed: %v", errs)
	}
}
