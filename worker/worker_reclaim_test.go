package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/danish-a1/goqueue/adapter/memory"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
)

// reclaimingStore is an in-memory store that records reclaim calls.
type reclaimingStore struct {
	*memory.InMemoryStore
	mu     sync.Mutex
	queues []string
}

func (s *reclaimingStore) ReclaimProcessing(queueName string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues = append(s.queues, queueName)
	return 0, nil
}

func (s *reclaimingStore) calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.queues...)
}

func startReclaimWorker(t *testing.T, cfg config.Config) *reclaimingStore {
	t.Helper()
	store := &reclaimingStore{InMemoryStore: memory.NewInMemoryStore("reclaim_queue", cfg, logger.NewZapLogger())}
	w := NewWorker(store, cfg, "reclaim_queue", nil, logger.NewZapLogger())
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
	return store
}

func reclaimConfig() config.Config {
	return config.Config{Driver: config.DriverMemory, MaxWorkers: 1, ConcurrencyLimit: 1, MaxRetryAttempts: 1}
}

func TestWorkerStartReclaimsProcessingWhenEnabled(t *testing.T) {
	store := startReclaimWorker(t, reclaimConfig().WithReclaimProcessingOnStart(true))
	if got := store.calls(); len(got) != 1 || got[0] != "reclaim_queue" {
		t.Fatalf("expected one reclaim of reclaim_queue at start, got %v", got)
	}
}

func TestWorkerStartDoesNotReclaimByDefault(t *testing.T) {
	store := startReclaimWorker(t, reclaimConfig())
	if got := store.calls(); len(got) != 0 {
		t.Fatalf("expected no reclaim by default, got %v", got)
	}
}
