package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/danish-a1/goqueue/adapter"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/manager"
)

var _ adapter.Tracker = (*RedisStore)(nil)

func newTrackerStore(t *testing.T) (*RedisStore, *miniredis.Miniredis) {
	t.Helper()
	mr, client := setupTestRedis(t)
	t.Cleanup(mr.Close)
	t.Cleanup(func() { _ = client.Close() })
	lg := logger.NewZapLogger()
	cfg := config.NewRedisConfig(mr.Addr(), "", "", 0)
	return NewRedisStore(client, cfg, manager.NewRedisClientManager(mr.Addr(), "", "", 0, lg), mr.Addr(), 0, lg), mr
}

func TestRecordResult_CountsTotalsDailyAndQueue(t *testing.T) {
	store, mr := newTrackerStore(t)
	ctx := context.Background()
	q := "track_q"
	before := time.Now().UTC()
	store.RecordResult(ctx, q, true)
	store.RecordResult(ctx, q, true)
	store.RecordResult(ctx, q, false)
	// The UTC day may roll over mid-test; use whichever daily key was written.
	day := before
	if !mr.Exists(KeyStatDaily("processed", day)) {
		day = time.Now().UTC()
	}

	want := map[string]string{
		KeyStatProcessed:               "3",
		KeyStatFailed:                  "1",
		KeyStatDaily("processed", day): "3",
		KeyStatDaily("failed", day):    "1",
		KeyStatQueue(q, "processed"):   "3",
		KeyStatQueue(q, "failed"):      "1",
	}
	for k, v := range want {
		got, err := mr.Get(k)
		if err != nil || got != v {
			t.Errorf("%s = %q (err %v), want %q", k, got, err, v)
		}
	}
	for _, kind := range []string{"processed", "failed"} {
		ttl := mr.TTL(KeyStatDaily(kind, day))
		if ttl <= DailyStatTTL-time.Minute || ttl > DailyStatTTL {
			t.Errorf("daily %s ttl = %v, want ~%v", kind, ttl, DailyStatTTL)
		}
	}
}

func TestKeyNames(t *testing.T) {
	day := time.Date(2026, 11, 8, 23, 59, 0, 0, time.UTC)
	cases := map[string]string{
		KeyStatDaily("processed", day): "goqueue:stat:processed:2026-11-08",
		KeyStatQueue("q", "failed"):    "goqueue:stat:q:failed",
		KeyProcess("p1"):               "goqueue:process:p1",
		KeyProcessWork("p1"):           "goqueue:process:p1:work",
		KeyDeleted("q"):                "goqueue:deleted:q",
		KeyPending("q"):                "q",
		KeyRetry("q"):                  "retry:q",
		KeyIndex("q"):                  "job_index:q",
		KeyQueues:                      "goqueue:queues",
		KeyProcesses:                   "goqueue:processes",
		KeyStatProcessed:               "goqueue:stat:processed",
		KeyStatFailed:                  "goqueue:stat:failed",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("key = %q, want %q", got, want)
		}
	}
	if HeartbeatTTL != 60*time.Second || HeartbeatEvery != 10*time.Second || DailyStatTTL != 180*24*time.Hour {
		t.Error("tracking durations differ from spec")
	}
	// A non-UTC time must still map to its UTC day.
	loc := time.FixedZone("x", 3*3600)
	if got := KeyStatDaily("failed", time.Date(2026, 11, 9, 1, 0, 0, 0, loc)); got != "goqueue:stat:failed:2026-11-08" {
		t.Errorf("non-UTC day key = %q", got)
	}
}

func TestRegisterQueue_StoresDeadKey(t *testing.T) {
	store, mr := newTrackerStore(t)
	if err := store.RegisterQueue(context.Background(), "q", "dlq:custom", true); err != nil {
		t.Fatalf("RegisterQueue: %v", err)
	}
	if got := mr.HGet(KeyQueues, "q"); got != "dlq:custom" {
		t.Errorf("HGET = %q, want dlq:custom", got)
	}
	if err := store.RegisterQueue(context.Background(), "nodlq", "", true); err != nil {
		t.Fatalf("RegisterQueue: %v", err)
	}
	if !mr.Exists(KeyQueues) || mr.HGet(KeyQueues, "nodlq") != "" {
		t.Error("empty dead key must register the queue with an empty value")
	}
	if keys, _ := mr.HKeys(KeyQueues); len(keys) != 2 {
		t.Errorf("registry fields = %v, want q and nodlq", keys)
	}
}

func TestHeartbeat_ExpiresAfterTTL(t *testing.T) {
	store, mr := newTrackerStore(t)
	p := adapter.ProcessInfo{ID: "h:1:q:abcd1234", Host: "h", Queue: "q", PID: 1, Concurrency: 2, StartedAt: time.Now()}
	if err := store.Heartbeat(context.Background(), p); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if !mr.Exists(KeyProcess(p.ID)) {
		t.Fatal("process hash missing after heartbeat")
	}
	if ttl := mr.TTL(KeyProcess(p.ID)); ttl != HeartbeatTTL {
		t.Errorf("ttl = %v, want %v", ttl, HeartbeatTTL)
	}
	if ok, _ := mr.IsMember(KeyProcesses, p.ID); !ok {
		t.Error("process id not in processes set")
	}
	mr.FastForward(61 * time.Second)
	if mr.Exists(KeyProcess(p.ID)) {
		t.Error("process hash should expire after 61s")
	}
}

func TestRemoveProcess_DropsSetHashAndWork(t *testing.T) {
	store, mr := newTrackerStore(t)
	ctx := context.Background()
	p := adapter.ProcessInfo{ID: "h:1:q:abcd1234", Host: "h", Queue: "q", PID: 1, Concurrency: 1, StartedAt: time.Now()}
	_ = store.Heartbeat(ctx, p)
	store.JobStarted(ctx, p.ID, "q", "j1", "Name")
	if err := store.RemoveProcess(ctx, p.ID); err != nil {
		t.Fatalf("RemoveProcess: %v", err)
	}
	if ok, _ := mr.IsMember(KeyProcesses, p.ID); ok || mr.Exists(KeyProcess(p.ID)) || mr.Exists(KeyProcessWork(p.ID)) {
		t.Error("process set member, hash and work hash must all be removed")
	}
}

func TestJobStartedFinished(t *testing.T) {
	store, mr := newTrackerStore(t)
	ctx := context.Background()
	store.JobStarted(ctx, "p1", "q", "job-1", "SendEmail")
	raw := mr.HGet(KeyProcessWork("p1"), "job-1")
	if raw == "" {
		t.Fatal("work hash missing job-1")
	}
	var w struct {
		Queue     string `json:"queue"`
		JobName   string `json:"job_name"`
		StartedAt any    `json:"started_at"`
	}
	if err := json.Unmarshal([]byte(raw), &w); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if w.Queue != "q" || w.JobName != "SendEmail" || w.StartedAt == nil {
		t.Errorf("work entry = %+v", w)
	}
	store.JobFinished(ctx, "p1", "job-1")
	if mr.HGet(KeyProcessWork("p1"), "job-1") != "" {
		t.Error("job-1 should be removed after JobFinished")
	}
}

func TestRecordResult_RedisDown_NoPanic(t *testing.T) {
	store, mr := newTrackerStore(t)
	mr.Close()
	ctx := context.Background()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic with Redis down: %v", r)
		}
	}()
	store.RecordResult(ctx, "q", false)
	store.JobStarted(ctx, "p", "q", "j", "n")
	store.JobFinished(ctx, "p", "j")
}

func TestWorkHash_HasTTLAndHeartbeatRefreshes(t *testing.T) {
	store, mr := newTrackerStore(t)
	ctx := context.Background()
	p := adapter.ProcessInfo{ID: "h:1:q:abcd1234", Host: "h", Queue: "q", PID: 1, Concurrency: 1, StartedAt: time.Now()}
	store.JobStarted(ctx, p.ID, "q", "j1", "Name")
	if ttl := mr.TTL(KeyProcessWork(p.ID)); ttl != HeartbeatTTL {
		t.Fatalf("work ttl after JobStarted = %v, want %v", ttl, HeartbeatTTL)
	}
	mr.FastForward(40 * time.Second)
	if err := store.Heartbeat(ctx, p); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if ttl := mr.TTL(KeyProcessWork(p.ID)); ttl != HeartbeatTTL {
		t.Errorf("work ttl after Heartbeat = %v, want %v", ttl, HeartbeatTTL)
	}
	mr.FastForward(61 * time.Second)
	if mr.Exists(KeyProcessWork(p.ID)) {
		t.Error("work hash should expire without heartbeats")
	}
}

func TestRegisterQueue_OverwriteFlag(t *testing.T) {
	store, mr := newTrackerStore(t)
	ctx := context.Background()
	if err := store.RegisterQueue(ctx, "q", "dlq:consumer", true); err != nil {
		t.Fatalf("RegisterQueue: %v", err)
	}
	if err := store.RegisterQueue(ctx, "q", "dlq:q", false); err != nil {
		t.Fatalf("RegisterQueue: %v", err)
	}
	if got := mr.HGet(KeyQueues, "q"); got != "dlq:consumer" {
		t.Errorf("non-overwriting register replaced key: %q", got)
	}
	if err := store.RegisterQueue(ctx, "q", "dlq:other", true); err != nil {
		t.Fatalf("RegisterQueue: %v", err)
	}
	if got := mr.HGet(KeyQueues, "q"); got != "dlq:other" {
		t.Errorf("overwriting register = %q, want dlq:other", got)
	}
	if err := store.RegisterQueue(ctx, "fresh", "dlq:fresh", false); err != nil {
		t.Fatalf("RegisterQueue: %v", err)
	}
	if got := mr.HGet(KeyQueues, "fresh"); got != "dlq:fresh" {
		t.Errorf("non-overwriting register on new queue = %q", got)
	}
}
