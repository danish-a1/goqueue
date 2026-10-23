package worker

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	redisstore "github.com/danish-a1/goqueue/adapter/redis"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/manager"
)

// Workers blocked on an empty Redis queue must still stop promptly on Shutdown,
// whether or not the caller cancels the Start context first (signal.NotifyContext
// apps do, others pass a long-lived context).
func TestWorkerShutdownWithIdleRedisWorkers(t *testing.T) {
	for name, cancelStart := range map[string]bool{"start ctx canceled": true, "start ctx live": false} {
		t.Run(name, func(t *testing.T) { assertIdleShutdownIsPrompt(t, cancelStart) })
	}
}

func assertIdleShutdownIsPrompt(t *testing.T, cancelStart bool) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer client.Close()

	log := logger.NewZapLogger()
	cfg := config.NewRedisConfig(mr.Addr(), "", "", 0)
	cfg.MaxWorkers, cfg.ConcurrencyLimit, cfg.MaxRetryAttempts = 3, 3, 1
	mgr := manager.NewRedisClientManager(mr.Addr(), "", "", 0, log)
	store := redisstore.NewRedisStore(client, cfg, mgr, mr.Addr(), 0, log)
	defer func() { _ = store.Stop() }()

	w := NewWorker(store, cfg, "idle_queue", nil, log)
	startCtx, stop := context.WithCancel(context.Background())
	defer stop()
	if err := w.Start(startCtx, 3); err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // let every worker block in Pop
	if cancelStart {
		stop()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	err := w.Shutdown(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Shutdown returned %v after %v, want nil", err, elapsed)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("Shutdown took %v, want < 2s for idle workers", elapsed)
	}
}
