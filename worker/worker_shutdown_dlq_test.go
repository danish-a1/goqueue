package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/danish-a1/goqueue/adapter/memory"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/registry"
	"github.com/danish-a1/goqueue/job"
)

// failGate blocks in Process until released, then fails.
var failGate struct {
	started chan struct{}
	release chan struct{}
}

type FailAfterReleaseJob struct{}

func (j *FailAfterReleaseJob) Process(context.Context) error {
	close(failGate.started)
	<-failGate.release
	return errors.New("boom")
}

func registerFailAfterReleaseJob() {
	defer func() { _ = recover() }()
	registry.Register("FailAfterReleaseJob", func() job.Job { return &FailAfterReleaseJob{} })
}

// ctxCheckingDLQ fails like a real network DLQ (Redis RPush, SQL insert) when
// handed a canceled context.
type ctxCheckingDLQ struct {
	mu     sync.Mutex
	pushed int
	ctxErr error
}

func (d *ctxCheckingDLQ) Push(ctx context.Context, _ *job.JobContext, _ error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		d.ctxErr = err
		return err
	}
	d.pushed++
	return nil
}

// A job that fails on its last attempt during shutdown (worker ctx already
// canceled) must still reach the DLQ. Pushing with the canceled worker ctx
// made the push fail, and the job was acked anyway, so it was lost.
func TestDLQPushSurvivesWorkerCtxCancel(t *testing.T) {
	registerFailAfterReleaseJob()
	failGate.started = make(chan struct{})
	failGate.release = make(chan struct{})

	queueName := "dlq_shutdown_queue"
	dlq := &ctxCheckingDLQ{}
	cfg := config.Config{
		Driver: config.DriverMemory, MaxWorkers: 1, ConcurrencyLimit: 1, MaxRetryAttempts: 1,
		DLQAdapter: dlq,
	}
	store := memory.NewInMemoryStore(queueName, cfg, logger.NewZapLogger())
	w := NewWorker(store, cfg, queueName, nil, logger.NewZapLogger())
	if err := store.Push(queueName, &FailAfterReleaseJob{}); err != nil {
		t.Fatalf("push: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := w.Start(ctx, 1); err != nil {
		t.Fatalf("start: %v", err)
	}
	select {
	case <-failGate.started:
	case <-time.After(3 * time.Second):
		t.Fatal("job never started")
	}

	cancel()
	release := failGate.release
	time.AfterFunc(50*time.Millisecond, func() { close(release) })
	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	if err := w.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	if dlq.pushed != 1 {
		t.Fatalf("job not dead-lettered (pushed=%d, push ctx err=%v)", dlq.pushed, dlq.ctxErr)
	}
}
