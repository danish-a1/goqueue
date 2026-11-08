package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/danish-a1/goqueue/adapter/redis"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/manager"
	"github.com/danish-a1/goqueue/internal/registry"
	"github.com/danish-a1/goqueue/job"
	goredis "github.com/redis/go-redis/v9"
)

type failingJob struct{}

func (j *failingJob) Process(ctx context.Context) error { return errors.New("boom") }

func TestWorker_RetrySetsLastError(t *testing.T) {
	func() {
		defer func() { _ = recover() }()
		registry.Register("failingJob", func() job.Job { return &failingJob{} })
	}()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	lg := logger.NewZapLogger()
	rcfg := config.NewRedisConfig(mr.Addr(), "", "", 0)
	store := redis.NewRedisStore(client, rcfg, manager.NewRedisClientManager(mr.Addr(), "", "", 0, lg), mr.Addr(), 0, lg)

	queueName := "retry_error_q"
	cfg := config.Config{
		Driver:           config.DriverRedis,
		JobTimeout:       2 * time.Second,
		MaxRetryAttempts: 3,
		RetryDelay:       time.Hour, // keep the job in the retry ZSET
		MaxWorkers:       1,
		ConcurrencyLimit: 1,
	}
	w := NewWorker(store, cfg, queueName, nil, lg)
	if err := store.Push(queueName, &failingJob{}); err != nil {
		t.Fatalf("Push: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.Start(ctx, 1); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		sctx, sc := context.WithTimeout(context.Background(), 2*time.Second)
		defer sc()
		_ = w.Shutdown(sctx)
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if members, _ := mr.ZMembers("retry:" + queueName); len(members) == 1 {
			var q job.RedisQueuedJob
			if err := json.Unmarshal([]byte(members[0]), &q); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if q.ErrorMessage != "boom" {
				t.Errorf("error_message = %q, want boom", q.ErrorMessage)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("job never landed in retry ZSET")
}
