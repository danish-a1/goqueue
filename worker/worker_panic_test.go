package worker

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danish-a1/goqueue/adapter/memory"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/registry"
	"github.com/danish-a1/goqueue/job"
)

type PanicJob struct{}

func (j *PanicJob) Process(ctx context.Context) error { panic("job exploded") }

type capturingDLQ struct {
	mu   sync.Mutex
	errs []error
}

func (c *capturingDLQ) Push(ctx context.Context, j *job.JobContext, err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, err)
	return nil
}

func (c *capturingDLQ) pushed() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.errs...)
}

func registerPanicJob() {
	defer func() { _ = recover() }()
	registry.Register("PanicJob", func() job.Job { return &PanicJob{} })
}

// A panic inside Process runs on the handler goroutine; it must become a job
// failure (sent to the DLQ) instead of crashing the process.
func TestWorkerRecoversPanicInJobHandler(t *testing.T) {
	registerPanicJob()
	queueName := "panic_queue"
	dlqAdapter := &capturingDLQ{}
	cfg := config.Config{
		Driver:           config.DriverMemory,
		JobTimeout:       2 * time.Second,
		MaxRetryAttempts: 1,
		MaxWorkers:       1,
		ConcurrencyLimit: 1,
		DLQAdapter:       dlqAdapter,
	}
	store := memory.NewInMemoryStore(queueName, cfg, logger.NewZapLogger())
	w := NewWorker(store, cfg, queueName, nil, logger.NewZapLogger())
	if err := store.Push(queueName, &PanicJob{}); err != nil {
		t.Fatalf("push: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Start(ctx, 1); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		_ = w.Shutdown(sctx)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(dlqAdapter.pushed()) == 0 {
		time.Sleep(50 * time.Millisecond)
	}
	errs := dlqAdapter.pushed()
	if len(errs) != 1 {
		t.Fatalf("expected the panicking job in the DLQ once, got %d", len(errs))
	}
	if errs[0] == nil || !strings.Contains(errs[0].Error(), "panic") {
		t.Fatalf("expected a panic error, got %v", errs[0])
	}
}
