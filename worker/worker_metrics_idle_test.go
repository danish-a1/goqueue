package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danish-a1/goqueue/adapter/memory"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
)

// emptyMetricsStore returns no metrics immediately, like the SQS
// (WaitTimeSeconds 0) and database stores, or any store whose call errors.
type emptyMetricsStore struct {
	*memory.InMemoryStore
	calls      atomic.Int64
	err        error
	nextMetric atomic.Pointer[config.JobMetrics]
}

func (s *emptyMetricsStore) DequeueMetrics(string) (config.JobMetrics, error) {
	s.calls.Add(1)
	if m := s.nextMetric.Swap(nil); m != nil {
		return *m, nil
	}
	return config.JobMetrics{}, s.err
}

func startMetricsIdleWorker(t *testing.T, store *emptyMetricsStore, onComplete func(config.JobMetrics)) {
	t.Helper()
	cfg := config.Config{Driver: config.DriverMemory, MaxWorkers: 1, ConcurrencyLimit: 1, MaxRetryAttempts: 1, OnJobComplete: onComplete}
	store.InMemoryStore = memory.NewInMemoryStore("metrics_idle", cfg, logger.NewZapLogger())
	w := NewWorker(store, cfg, "metrics_idle", nil, logger.NewZapLogger())
	ctx, cancel := context.WithCancel(context.Background())
	if err := w.Start(ctx, 1); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		_ = w.Shutdown(sctx)
	})
}

// TestMetricsWorkerDoesNotSpinWhenIdle guards against a hot loop: an empty or
// failing DequeueMetrics must not be called back to back.
func TestMetricsWorkerDoesNotSpinWhenIdle(t *testing.T) {
	for name, err := range map[string]error{"empty": nil, "error": context.DeadlineExceeded} {
		t.Run(name, func(t *testing.T) {
			store := &emptyMetricsStore{err: err}
			startMetricsIdleWorker(t, store, func(config.JobMetrics) {})
			time.Sleep(300 * time.Millisecond)
			if n := store.calls.Load(); n > 3 {
				t.Fatalf("DequeueMetrics called %d times in 300ms while idle; want <= 3", n)
			}
		})
	}
}

// TestMetricsWorkerDeliversMetric checks metrics still reach OnJobComplete.
func TestMetricsWorkerDeliversMetric(t *testing.T) {
	store := &emptyMetricsStore{}
	store.nextMetric.Store(&config.JobMetrics{QueueName: "metrics_idle", JobID: "j1"})
	got := make(chan string, 1)
	startMetricsIdleWorker(t, store, func(m config.JobMetrics) { got <- m.JobID })
	select {
	case id := <-got:
		if id != "j1" {
			t.Fatalf("got job %q, want j1", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("metric not delivered to OnJobComplete")
	}
}
