package worker

import (
	"context"
	"regexp"
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

var processIDRe = regexp.MustCompile(`^.+:\d+:hb_q:[0-9a-f]{8}$`)

func newTrackedWorker(t *testing.T, queue string, retryDelay time.Duration) (*Worker, *redis.RedisStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	lg := logger.NewZapLogger()
	rcfg := config.NewRedisConfig(mr.Addr(), "", "", 0)
	store := redis.NewRedisStore(client, rcfg, manager.NewRedisClientManager(mr.Addr(), "", "", 0, lg), mr.Addr(), 0, lg)
	cfg := config.Config{
		Driver:           config.DriverRedis,
		JobTimeout:       2 * time.Second,
		MaxRetryAttempts: 3,
		RetryDelay:       retryDelay,
		MaxWorkers:       1,
		ConcurrencyLimit: 1,
	}
	return NewWorker(store, cfg, queue, nil, lg), store, mr
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestWorker_HeartbeatLifecycle(t *testing.T) {
	w, _, mr := newTrackedWorker(t, "hb_q", time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.Start(ctx, 1); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var id string
	waitFor(t, "process registration", func() bool {
		ids, _ := mr.SMembers(redis.KeyProcesses)
		if len(ids) != 1 {
			return false
		}
		id = ids[0]
		return mr.Exists(redis.KeyProcess(id))
	})
	if !processIDRe.MatchString(id) {
		t.Errorf("process id %q does not match <host>:<pid>:<queue>:<8 hex>", id)
	}

	mr.HSet(redis.KeyProcessWork(id), "stale", `{"queue":"hb_q"}`)
	sctx, sc := context.WithTimeout(context.Background(), 2*time.Second)
	defer sc()
	if err := w.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if ok, _ := mr.IsMember(redis.KeyProcesses, id); ok || mr.Exists(redis.KeyProcess(id)) || mr.Exists(redis.KeyProcessWork(id)) {
		t.Error("Shutdown must remove process set member, hash and work hash")
	}
}

type okJob struct{}

func (j *okJob) Process(ctx context.Context) error { return nil }

func TestWorker_RecordsResults(t *testing.T) {
	func() {
		defer func() { _ = recover() }()
		registry.Register("okJob", func() job.Job { return &okJob{} })
	}()
	func() {
		defer func() { _ = recover() }()
		registry.Register("failingJob", func() job.Job { return &failingJob{} })
	}()
	q := "results_q"
	w, store, mr := newTrackedWorker(t, q, time.Hour)
	if err := store.Push(q, &okJob{}); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := store.Push(q, &failingJob{}); err != nil {
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

	waitFor(t, "processed=2 failed=1", func() bool {
		p, _ := mr.Get(redis.KeyStatProcessed)
		f, _ := mr.Get(redis.KeyStatFailed)
		return p == "2" && f == "1"
	})
	if v, _ := mr.Get(redis.KeyStatQueue(q, "processed")); v != "2" {
		t.Errorf("queue processed = %q, want 2", v)
	}
	if v, _ := mr.Get(redis.KeyStatQueue(q, "failed")); v != "1" {
		t.Errorf("queue failed = %q, want 1", v)
	}
	// After an attempt finishes nothing may remain in the busy hash.
	ids, _ := mr.SMembers(redis.KeyProcesses)
	for _, id := range ids {
		if mr.Exists(redis.KeyProcessWork(id)) {
			t.Errorf("work hash for %s should be empty after attempts finish", id)
		}
	}
}

func TestWorker_OverMaxRetriesRecordsNoStat(t *testing.T) {
	q := "over_max_q"
	w, _, mr := newTrackedWorker(t, q, time.Hour)
	// Job already past max retries goes straight to the DLQ; no attempt executes.
	w.processJobSafely(context.Background(), 0, job.JobContext{
		JobID:      "over",
		Job:        &okJob{},
		RetryCount: 99,
	})
	if mr.Exists(redis.KeyStatProcessed) || mr.Exists(redis.KeyStatFailed) {
		t.Error("over-max-retries DLQ path must not record processed/failed")
	}
}
