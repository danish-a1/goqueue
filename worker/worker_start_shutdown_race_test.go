package worker

import (
	"context"
	"testing"
	"time"

	"github.com/danish-a1/goqueue/adapter/memory"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
)

// Shutdown right after Start must not race: every wg.Add in Start has to
// happen before Start returns, or it can run concurrently with Shutdown's
// wg.Wait (a data race, caught under -race).
func TestWorkerShutdownImmediatelyAfterStart(t *testing.T) {
	log := logger.NewZapLogger()
	for i := 0; i < 50; i++ {
		cfg := config.NewInMemoryConfig()
		cfg.MaxWorkers, cfg.ConcurrencyLimit = 1, 1
		store := memory.NewInMemoryStore("race_queue", cfg, log)
		w := NewWorker(store, cfg, "race_queue", nil, log)

		startCtx, stop := context.WithCancel(context.Background())
		if err := w.Start(startCtx, 1); err != nil {
			stop()
			t.Fatalf("start: %v", err)
		}
		stop()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := w.Shutdown(ctx)
		cancel()
		if err != nil {
			t.Fatalf("iteration %d: Shutdown returned %v, want nil", i, err)
		}
	}
}
