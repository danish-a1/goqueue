package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	gqredis "github.com/danish-a1/goqueue/adapter/redis"
	"github.com/danish-a1/goqueue/job"
)

func newTestReader(t *testing.T) (*reader, *miniredis.Miniredis, redis.UniversalClient) {
	t.Helper()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	return newReader(rc), mr, rc
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func queuedJob(t *testing.T, id, name string, retry int) string {
	return mustJSON(t, job.RedisQueuedJob{
		Job: json.RawMessage(`{"n":1}`), ID: id, JobName: name,
		EnqueuedAt: time.Now().UTC(), RetryCount: retry,
	})
}

func seedQueue(t *testing.T, mr *miniredis.Miniredis, rc redis.UniversalClient, q string, pending, retry, dead, deleted int) {
	t.Helper()
	ctx := context.Background()
	deadKey := "dlq:" + q
	rc.HSet(ctx, gqredis.KeyQueues, q, deadKey)
	for i := 0; i < pending; i++ {
		rc.LPush(ctx, gqredis.KeyPending(q), queuedJob(t, fmt.Sprintf("%s-p%d", q, i), "Job", 0))
	}
	for i := 0; i < retry; i++ {
		rc.ZAdd(ctx, gqredis.KeyRetry(q), redis.Z{Score: float64(1000 + i), Member: queuedJob(t, fmt.Sprintf("%s-r%d", q, i), "Job", 2)})
	}
	for i := 0; i < dead; i++ {
		rc.RPush(ctx, deadKey, mustJSON(t, map[string]any{
			"job": map[string]int{"n": i}, "job_id": fmt.Sprintf("%s-d%d", q, i), "job_name": "Job",
			"queue": q, "error": "boom", "failed_at": time.Now().UTC(),
		}))
	}
	for i := 0; i < deleted; i++ {
		rc.RPush(ctx, gqredis.KeyDeleted(q), mustJSON(t, map[string]any{
			"entry":  json.RawMessage(queuedJob(t, fmt.Sprintf("%s-x%d", q, i), "Job", 0)),
			"job_id": fmt.Sprintf("%s-x%d", q, i), "queue": q, "from": "pending",
			"deleted_at": time.Now().UTC(), "deleted_by_ip": "203.0.113.7",
		}))
	}
}

func seedProcess(t *testing.T, rc redis.UniversalClient, id string, work map[string]string) {
	t.Helper()
	ctx := context.Background()
	rc.SAdd(ctx, gqredis.KeyProcesses, id)
	rc.HSet(ctx, gqredis.KeyProcess(id), "host", "h1", "pid", 42, "queue", "a", "concurrency", 4,
		"started_at", time.Now().UTC().Unix(), "beat", time.Now().UTC().Unix())
	rc.Expire(ctx, gqredis.KeyProcess(id), time.Minute)
	for jobID, q := range work {
		rc.HSet(ctx, gqredis.KeyProcessWork(id), jobID, mustJSON(t, map[string]string{
			"queue": q, "job_name": "Job", "started_at": fmt.Sprint(time.Now().UTC().Unix()),
		}))
	}
}

func TestSummary_CountsPerQueue(t *testing.T) {
	r, mr, rc := newTestReader(t)
	ctx := context.Background()
	seedQueue(t, mr, rc, "a", 3, 2, 4, 1)
	seedQueue(t, mr, rc, "b", 5, 1, 0, 2)
	seedProcess(t, rc, "p1", map[string]string{"j1": "a", "j2": "a", "j3": "b"})
	rc.Set(ctx, gqredis.KeyStatProcessed, 100, 0)
	rc.Set(ctx, gqredis.KeyStatFailed, 7, 0)
	rc.Set(ctx, gqredis.KeyStatQueue("a", "processed"), 60, 0)
	rc.Set(ctx, gqredis.KeyStatQueue("a", "failed"), 5, 0)
	rc.Set(ctx, gqredis.KeyStatQueue("b", "processed"), 40, 0)
	rc.Set(ctx, gqredis.KeyStatQueue("b", "failed"), 2, 0)

	s, err := r.Summary(ctx)
	if err != nil {
		t.Fatalf("Summary: %v", err)
	}
	if s.Processed != 100 || s.Failed != 7 {
		t.Errorf("totals = %d/%d, want 100/7", s.Processed, s.Failed)
	}
	if s.Pending != 8 || s.Busy != 3 || s.Retry != 3 || s.Dead != 4 || s.Deleted != 3 || s.Processes != 1 {
		t.Errorf("aggregate = %+v", s)
	}
	if len(s.Queues) != 2 {
		t.Fatalf("queues = %d, want 2", len(s.Queues))
	}
	byName := map[string]QueueSummary{}
	for _, q := range s.Queues {
		byName[q.Name] = q
	}
	a := byName["a"]
	want := QueueSummary{Name: "a", DeadKey: "dlq:a", Pending: 3, Busy: 2, Retry: 2, Dead: 4, Deleted: 1, Processed: 60, Failed: 5}
	if a != want {
		t.Errorf("queue a = %+v, want %+v", a, want)
	}
	if b := byName["b"]; b.Pending != 5 || b.Busy != 1 || b.Retry != 1 || b.Dead != 0 || b.Deleted != 2 || b.Processed != 40 || b.Failed != 2 {
		t.Errorf("queue b = %+v", b)
	}
	if s.At.IsZero() {
		t.Error("At not set")
	}
}

func TestHistory_ClampsAndOrders(t *testing.T) {
	r, _, rc := newTestReader(t)
	ctx := context.Background()
	today := time.Now().UTC()
	for i := 0; i < 3; i++ {
		d := today.AddDate(0, 0, -i)
		rc.Set(ctx, gqredis.KeyStatDaily("processed", d), 10*(i+1), 0)
		rc.Set(ctx, gqredis.KeyStatDaily("failed", d), i, 0)
	}
	got, err := r.History(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[2].Day != today.Format("2006-01-02") || got[2].Processed != 10 || got[2].Failed != 0 {
		t.Errorf("newest = %+v", got[2])
	}
	if got[0].Day != today.AddDate(0, 0, -2).Format("2006-01-02") || got[0].Processed != 30 || got[0].Failed != 2 {
		t.Errorf("oldest = %+v", got[0])
	}
	for _, c := range []struct{ in, want int }{{0, 1}, {-5, 1}, {1000, 180}} {
		g, err := r.History(ctx, c.in)
		if err != nil || len(g) != c.want {
			t.Errorf("History(%d): len=%d err=%v, want len %d", c.in, len(g), err, c.want)
		}
	}
}

func TestJobs_Pagination(t *testing.T) {
	r, mr, rc := newTestReader(t)
	seedQueue(t, mr, rc, "a", 60, 0, 0, 0)
	ctx := context.Background()

	rows, total, err := r.Jobs(ctx, "a", "pending", 3, 25)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 10 || total != 60 {
		t.Fatalf("page3: rows=%d total=%d, want 10/60", len(rows), total)
	}
	first, _, _ := r.Jobs(ctx, "a", "pending", 1, 25)
	if first[0].ID != "a-p59" { // LPUSH: newest at head
		t.Errorf("newest first = %q, want a-p59", first[0].ID)
	}
	if rows[9].ID != "a-p0" || rows[0].JobName != "Job" || rows[0].Queue != "a" {
		t.Errorf("rows = %+v", rows)
	}
	big, _, _ := r.Jobs(ctx, "a", "pending", 1, 1000)
	if len(big) != 60 {
		t.Errorf("per clamp to 100: got %d rows, want 60", len(big))
	}
	if _, _, err := r.Jobs(ctx, "nope", "pending", 1, 25); err == nil {
		t.Error("unknown queue: want error")
	}
}

func TestJobs_RetryOrderedByDue(t *testing.T) {
	r, mr, rc := newTestReader(t)
	seedQueue(t, mr, rc, "a", 0, 3, 0, 0)
	rows, total, err := r.Jobs(context.Background(), "a", "retry", 1, 25)
	if err != nil || total != 3 || len(rows) != 3 {
		t.Fatalf("rows=%d total=%d err=%v", len(rows), total, err)
	}
	if rows[0].ID != "a-r0" || rows[0].DueAt.Unix() != 1000 || rows[2].DueAt.Unix() != 1002 || rows[0].RetryCount != 2 {
		t.Errorf("rows = %+v", rows)
	}
}

func TestJobs_DeadAndDeleted(t *testing.T) {
	r, mr, rc := newTestReader(t)
	seedQueue(t, mr, rc, "a", 0, 0, 3, 2)
	ctx := context.Background()

	rows, total, err := r.Jobs(ctx, "a", "dead", 1, 2)
	if err != nil || total != 3 || len(rows) != 2 {
		t.Fatalf("dead: rows=%d total=%d err=%v", len(rows), total, err)
	}
	if rows[0].ID != "a-d2" || rows[1].ID != "a-d1" || rows[0].Error != "boom" {
		t.Errorf("dead rows = %+v", rows)
	}
	rows, _, _ = r.Jobs(ctx, "a", "dead", 2, 2)
	if len(rows) != 1 || rows[0].ID != "a-d0" {
		t.Errorf("dead page2 = %+v", rows)
	}

	del, total, err := r.Jobs(ctx, "a", "deleted", 1, 25)
	if err != nil || total != 2 || len(del) != 2 {
		t.Fatalf("deleted: rows=%d total=%d err=%v", len(del), total, err)
	}
	if del[0].ID != "a-x1" || del[0].DeletedBy != "203.0.113.7" || del[0].From != "pending" || del[0].DeletedAt.IsZero() {
		t.Errorf("deleted row = %+v", del[0])
	}
}

func TestJobs_EmptyDeadKey(t *testing.T) {
	r, _, rc := newTestReader(t)
	rc.HSet(context.Background(), gqredis.KeyQueues, "a", "")
	rows, total, err := r.Jobs(context.Background(), "a", "dead", 1, 25)
	if err != nil || total != 0 || len(rows) != 0 {
		t.Errorf("rows=%d total=%d err=%v, want empty", len(rows), total, err)
	}
}

func TestJobs_DeadLegacyEntries(t *testing.T) {
	r, _, rc := newTestReader(t)
	ctx := context.Background()
	rc.HSet(ctx, gqredis.KeyQueues, "a", "dlq:a")
	ts := "2026-11-08T10:00:00.123456789+01:00"
	rc.RPush(ctx, "dlq:a", `{"queue_name":"a","job_id":"old1","error":"x","failed_at":"`+ts+`","job":{"k":1}}`)

	rows, total, err := r.Jobs(ctx, "a", "dead", 1, 25)
	if err != nil || total != 1 || len(rows) != 1 {
		t.Fatalf("rows=%d total=%d err=%v", len(rows), total, err)
	}
	row := rows[0]
	if row.ID != "old1" || row.Queue != "a" || row.JobName != "" || row.Error != "x" {
		t.Errorf("row = %+v", row)
	}
	want, _ := time.Parse(time.RFC3339Nano, ts)
	if !row.FailedAt.Equal(want) {
		t.Errorf("FailedAt = %v, want %v", row.FailedAt, want)
	}
	if string(row.Args) != `{"k":1}` {
		t.Errorf("Args = %s", row.Args)
	}
}

func TestProcesses_PrunesDead(t *testing.T) {
	r, mr, rc := newTestReader(t)
	ctx := context.Background()
	seedProcess(t, rc, "live", map[string]string{"j1": "a"})
	rc.SAdd(ctx, gqredis.KeyProcesses, "ghost") // hash expired / never existed
	_ = mr

	ps, err := r.Processes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 1 || ps[0].ID != "live" || ps[0].Host != "h1" || ps[0].PID != 42 || ps[0].Concurrency != 4 {
		t.Fatalf("processes = %+v", ps)
	}
	if len(ps[0].Busy) != 1 || ps[0].Busy[0].JobID != "j1" || ps[0].Busy[0].Queue != "a" || ps[0].Busy[0].JobName != "Job" || ps[0].Busy[0].StartedAt.IsZero() {
		t.Errorf("busy = %+v", ps[0].Busy)
	}
	if ok, _ := rc.SIsMember(ctx, gqredis.KeyProcesses, "ghost").Result(); ok {
		t.Error("ghost not pruned from set")
	}
	if ok, _ := rc.SIsMember(ctx, gqredis.KeyProcesses, "live").Result(); !ok {
		t.Error("live process wrongly pruned")
	}
}

func TestSummary_30Queues_Under50ms(t *testing.T) {
	r, mr, rc := newTestReader(t)
	for i := 0; i < 30; i++ {
		seedQueue(t, mr, rc, fmt.Sprintf("q%02d", i), 5, 2, 2, 1)
	}
	best := time.Hour
	for i := 0; i < 3; i++ {
		start := time.Now()
		s, err := r.Summary(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(s.Queues) != 30 || s.Pending != 150 {
			t.Fatalf("summary = %d queues, pending %d", len(s.Queues), s.Pending)
		}
		if d := time.Since(start); d < best {
			best = d
		}
	}
	if best >= 50*time.Millisecond {
		t.Errorf("best Summary = %v, want < 50ms", best)
	}
}
