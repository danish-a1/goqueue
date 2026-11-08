package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/danish-a1/goqueue/adapter/dlq"
	gqredis "github.com/danish-a1/goqueue/adapter/redis"
	"github.com/danish-a1/goqueue/job"
)

const actIP = "198.51.100.9"

func newTestActions(t *testing.T) (*actions, *miniredis.Miniredis, redis.UniversalClient) {
	t.Helper()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	return newActions(rc), mr, rc
}

// seedIndexed seeds a queue and fills job_index for pending and retry members.
func seedIndexed(t *testing.T, rc redis.UniversalClient, q string, pending, retry, dead, deleted int) {
	t.Helper()
	seedQueue(t, rc, q, pending, retry, dead, deleted)
	ctx := context.Background()
	for _, m := range rc.LRange(ctx, gqredis.KeyPending(q), 0, -1).Val() {
		var j job.RedisQueuedJob
		_ = json.Unmarshal([]byte(m), &j)
		rc.HSet(ctx, gqredis.KeyIndex(q), j.ID, m)
	}
	for _, m := range rc.ZRange(ctx, gqredis.KeyRetry(q), 0, -1).Val() {
		var j job.RedisQueuedJob
		_ = json.Unmarshal([]byte(m), &j)
		rc.HSet(ctx, gqredis.KeyIndex(q), j.ID, m)
	}
}

func mustDo(t *testing.T, a *actions, act Action, q string, src Source, ids ...string) ActionResult {
	t.Helper()
	res, err := a.Do(context.Background(), act, q, src, ids, actIP)
	if err != nil {
		t.Fatalf("Do(%s,%s): %v", act, src, err)
	}
	return res
}

func eqStrings(t *testing.T, name string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
}

func decodeQueued(t *testing.T, s string) job.RedisQueuedJob {
	t.Helper()
	var j job.RedisQueuedJob
	if err := json.Unmarshal([]byte(s), &j); err != nil {
		t.Fatalf("decode queued job %q: %v", s, err)
	}
	return j
}

func TestRetry_FromRetryZSet_MovesToPending(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 0, 1, 0, 0)
	member := rc.ZRange(ctx, gqredis.KeyRetry("q"), 0, -1).Val()[0]

	res := mustDo(t, a, "retry", "q", SourceRetry, "q-r0")
	eqStrings(t, "Done", res.Done, []string{"q-r0"})
	if len(res.NotFound) != 0 {
		t.Fatalf("NotFound = %v", res.NotFound)
	}
	if n := rc.ZCard(ctx, gqredis.KeyRetry("q")).Val(); n != 0 {
		t.Fatalf("retry zset len = %d", n)
	}
	pend := rc.LRange(ctx, gqredis.KeyPending("q"), 0, -1).Val()
	eqStrings(t, "pending", pend, []string{member})
	if got := rc.HGet(ctx, gqredis.KeyIndex("q"), "q-r0").Val(); got != member {
		t.Fatalf("index changed: %q", got)
	}
}

func TestRetry_FromDead_RebuildsEnvelope(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 0, 0, 2, 0)

	start := time.Now().Add(-time.Second)
	res := mustDo(t, a, "retry", "q", SourceDead, "q-d1")
	eqStrings(t, "Done", res.Done, []string{"q-d1"})

	dead := rc.LRange(ctx, "dlq:q", 0, -1).Val()
	if len(dead) != 1 {
		t.Fatalf("dead len = %d, want 1", len(dead))
	}
	pend := rc.LRange(ctx, gqredis.KeyPending("q"), 0, -1).Val()
	if len(pend) != 1 {
		t.Fatalf("pending len = %d", len(pend))
	}
	j := decodeQueued(t, pend[0])
	if j.ID != "q-d1" || j.JobName != "Job" || j.RetryCount != 0 || string(j.Job) != `{"n":1}` {
		t.Fatalf("rebuilt job = %+v job=%s", j, j.Job)
	}
	if j.EnqueuedAt.Before(start) {
		t.Fatalf("EnqueuedAt not fresh: %v", j.EnqueuedAt)
	}
	if got := rc.HGet(ctx, gqredis.KeyIndex("q"), "q-d1").Val(); got != pend[0] {
		t.Fatalf("index = %q, want pending member", got)
	}
}

func TestRetry_FromDead_NoJobName_NotFound(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	rc.HSet(ctx, gqredis.KeyQueues, "q", "dlq:q")
	rc.RPush(ctx, "dlq:q", `{"job":{"n":1},"job_id":"nn","queue":"q","error":"x","failed_at":"2026-11-08T10:00:00Z"}`)

	res := mustDo(t, a, "retry", "q", SourceDead, "nn")
	eqStrings(t, "NotFound", res.NotFound, []string{"nn"})
	if len(res.Done) != 0 {
		t.Fatalf("Done = %v", res.Done)
	}
	if rc.LLen(ctx, "dlq:q").Val() != 1 || rc.LLen(ctx, gqredis.KeyPending("q")).Val() != 0 {
		t.Fatal("lists must be unchanged")
	}
}

func TestKill_FromPending_ToDeadKeyFromRegistry(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 1, 0, 0, 0)
	rc.HSet(ctx, gqredis.KeyQueues, "q", "dlq:custom")

	res := mustDo(t, a, "kill", "q", SourcePending, "q-p0")
	eqStrings(t, "Done", res.Done, []string{"q-p0"})
	if rc.LLen(ctx, gqredis.KeyPending("q")).Val() != 0 {
		t.Fatal("pending not emptied")
	}
	dead := rc.LRange(ctx, "dlq:custom", 0, -1).Val()
	if len(dead) != 1 {
		t.Fatalf("dead len = %d", len(dead))
	}
	e, err := dlq.DecodeEntry([]byte(dead[0]))
	if err != nil {
		t.Fatal(err)
	}
	if e.JobID != "q-p0" || e.JobName != "Job" || e.Queue != "q" || e.Error != "killed from dashboard" || string(e.Job) != `{"n":1}` || e.FailedAt.IsZero() {
		t.Fatalf("entry = %+v", e)
	}
	if ttl := rc.TTL(ctx, "dlq:custom").Val(); ttl <= 0 || ttl > dlq.EntryTTL {
		t.Fatalf("ttl = %v", ttl)
	}
}

func TestKill_FromRetry_ToDead(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 0, 1, 0, 0)
	res := mustDo(t, a, "kill", "q", SourceRetry, "q-r0")
	eqStrings(t, "Done", res.Done, []string{"q-r0"})
	if rc.ZCard(ctx, gqredis.KeyRetry("q")).Val() != 0 || rc.LLen(ctx, "dlq:q").Val() != 1 {
		t.Fatal("expected retry empty and one dead entry")
	}
}

func TestKill_NoDeadKey_400(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 1, 0, 0, 0)
	rc.HSet(ctx, gqredis.KeyQueues, "q", "")

	_, err := a.Do(ctx, "kill", "q", SourcePending, []string{"q-p0"}, actIP)
	if !errors.Is(err, ErrBadRequest) {
		t.Fatalf("err = %v, want ErrBadRequest", err)
	}
	if rc.LLen(ctx, gqredis.KeyPending("q")).Val() != 1 {
		t.Fatal("job must stay pending")
	}
}

func TestDelete_FromPending_ToDeletedWithIP(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 1, 0, 0, 0)
	orig := rc.LRange(ctx, gqredis.KeyPending("q"), 0, -1).Val()[0]

	res := mustDo(t, a, "delete", "q", SourcePending, "q-p0")
	eqStrings(t, "Done", res.Done, []string{"q-p0"})
	if rc.LLen(ctx, gqredis.KeyPending("q")).Val() != 0 {
		t.Fatal("pending not emptied")
	}
	del := rc.LRange(ctx, gqredis.KeyDeleted("q"), 0, -1).Val()
	if len(del) != 1 {
		t.Fatalf("deleted len = %d", len(del))
	}
	var env struct {
		Entry       json.RawMessage `json:"entry"`
		JobID       string          `json:"job_id"`
		Queue       string          `json:"queue"`
		From        string          `json:"from"`
		DeletedAt   time.Time       `json:"deleted_at"`
		DeletedByIP string          `json:"deleted_by_ip"`
	}
	if err := json.Unmarshal([]byte(del[0]), &env); err != nil {
		t.Fatal(err)
	}
	if env.JobID != "q-p0" || env.Queue != "q" || env.From != "pending" || env.DeletedByIP != actIP || env.DeletedAt.IsZero() {
		t.Fatalf("envelope = %+v", env)
	}
	if string(env.Entry) != orig {
		t.Fatalf("entry = %s, want original member %s", env.Entry, orig)
	}
}

func TestDelete_FromDead_FromRetry(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 0, 1, 1, 0)
	mustDo(t, a, "delete", "q", SourceDead, "q-d0")
	mustDo(t, a, "delete", "q", SourceRetry, "q-r0")
	if rc.LLen(ctx, "dlq:q").Val() != 0 || rc.ZCard(ctx, gqredis.KeyRetry("q")).Val() != 0 {
		t.Fatal("sources not emptied")
	}
	if n := rc.LLen(ctx, gqredis.KeyDeleted("q")).Val(); n != 2 {
		t.Fatalf("deleted len = %d", n)
	}
}

func TestDelete_DeletedListTrimmed1000AndTTL(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 1, 0, 0, 0)
	for i := 0; i < 1000; i++ {
		rc.RPush(ctx, gqredis.KeyDeleted("q"), fmt.Sprintf(`{"job_id":"old%d"}`, i))
	}
	mustDo(t, a, "delete", "q", SourcePending, "q-p0")

	if n := rc.LLen(ctx, gqredis.KeyDeleted("q")).Val(); n != 1000 {
		t.Fatalf("deleted len = %d, want 1000", n)
	}
	first := rc.LIndex(ctx, gqredis.KeyDeleted("q"), 0).Val()
	if first != `{"job_id":"old1"}` {
		t.Fatalf("oldest not trimmed, head = %s", first)
	}
	last := rc.LIndex(ctx, gqredis.KeyDeleted("q"), -1).Val()
	var env struct {
		JobID string `json:"job_id"`
	}
	_ = json.Unmarshal([]byte(last), &env)
	if env.JobID != "q-p0" {
		t.Fatalf("newest not at tail: %s", last)
	}
	if ttl := rc.TTL(ctx, gqredis.KeyDeleted("q")).Val(); ttl <= 29*24*time.Hour || ttl > 30*24*time.Hour {
		t.Fatalf("ttl = %v, want ~30d", ttl)
	}
}

func TestRestore_FromDeleted_ToPending(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 0, 0, 0, 1)

	start := time.Now().Add(-time.Second)
	res := mustDo(t, a, "restore", "q", SourceDeleted, "q-x0")
	eqStrings(t, "Done", res.Done, []string{"q-x0"})
	if rc.LLen(ctx, gqredis.KeyDeleted("q")).Val() != 0 {
		t.Fatal("deleted not emptied")
	}
	pend := rc.LRange(ctx, gqredis.KeyPending("q"), 0, -1).Val()
	if len(pend) != 1 {
		t.Fatalf("pending len = %d", len(pend))
	}
	j := decodeQueued(t, pend[0])
	if j.ID != "q-x0" || j.JobName != "Job" || j.RetryCount != 0 || j.EnqueuedAt.Before(start) {
		t.Fatalf("restored = %+v", j)
	}
	if rc.HGet(ctx, gqredis.KeyIndex("q"), "q-x0").Val() != pend[0] {
		t.Fatal("index missing after restore")
	}
}

func TestRestore_FromDeleted_DlqEntryShape(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	rc.HSet(ctx, gqredis.KeyQueues, "q", "dlq:q")
	entry := mustJSON(t, dlq.Entry{Job: json.RawMessage(`{"n":5}`), JobID: "e1", JobName: "Job", Queue: "q", Error: "x", FailedAt: time.Now().UTC()})
	rc.RPush(ctx, gqredis.KeyDeleted("q"), mustJSON(t, map[string]any{
		"entry": json.RawMessage(entry), "job_id": "e1", "queue": "q", "from": "dead",
		"deleted_at": time.Now().UTC(), "deleted_by_ip": "1.2.3.4",
	}))
	res := mustDo(t, a, "restore", "q", SourceDeleted, "e1")
	eqStrings(t, "Done", res.Done, []string{"e1"})
	pend := rc.LRange(ctx, gqredis.KeyPending("q"), 0, -1).Val()
	if len(pend) != 1 {
		t.Fatalf("pending len = %d", len(pend))
	}
	if j := decodeQueued(t, pend[0]); j.ID != "e1" || j.JobName != "Job" || string(j.Job) != `{"n":5}` {
		t.Fatalf("restored = %+v job=%s", j, j.Job)
	}
}

func TestAction_JobAlreadyPopped_NotFound_NoDuplicate(t *testing.T) {
	for _, act := range []Action{"kill", "delete"} {
		t.Run(string(act), func(t *testing.T) {
			a, _, rc := newTestActions(t)
			ctx := context.Background()
			seedIndexed(t, rc, "q", 1, 0, 0, 0)
			// Worker pops the job; the index still points at the stale member.
			rc.Del(ctx, gqredis.KeyPending("q"))

			res := mustDo(t, a, act, "q", SourcePending, "q-p0")
			eqStrings(t, "NotFound", res.NotFound, []string{"q-p0"})
			if len(res.Done) != 0 {
				t.Fatalf("Done = %v", res.Done)
			}
			if rc.LLen(ctx, "dlq:q").Val() != 0 || rc.LLen(ctx, gqredis.KeyDeleted("q")).Val() != 0 {
				t.Fatal("destination must be unchanged")
			}
		})
	}
}

func TestAction_UnknownID_NotFound_MixedBatch(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 2, 0, 0, 0)
	res := mustDo(t, a, "delete", "q", SourcePending, "q-p0", "nope", "q-p1", "ghost")
	eqStrings(t, "Done", res.Done, []string{"q-p0", "q-p1"})
	eqStrings(t, "NotFound", res.NotFound, []string{"nope", "ghost"})
	if n := rc.LLen(ctx, gqredis.KeyDeleted("q")).Val(); n != 2 {
		t.Fatalf("deleted len = %d", n)
	}
}

func TestAction_InvalidMatrix_400(t *testing.T) {
	allowed := map[Action][]Source{
		"retry":   {SourceRetry, SourceDead},
		"kill":    {SourcePending, SourceRetry},
		"delete":  {SourcePending, SourceRetry, SourceDead},
		"restore": {SourceDeleted},
	}
	srcs := []Source{SourcePending, SourceRetry, SourceDead, SourceDeleted}
	a, _, rc := newTestActions(t)
	seedIndexed(t, rc, "q", 1, 1, 1, 1)
	for act, ok := range allowed {
		for _, src := range srcs {
			if func() bool {
				for _, s := range ok {
					if s == src {
						return true
					}
				}
				return false
			}() {
				continue
			}
			_, err := a.Do(context.Background(), act, "q", src, []string{"q-p0"}, actIP)
			if !errors.Is(err, ErrBadRequest) {
				t.Errorf("%s from %s: err = %v, want ErrBadRequest", act, src, err)
			}
		}
	}
	if _, err := a.Do(context.Background(), "explode", "q", SourcePending, []string{"q-p0"}, actIP); !errors.Is(err, ErrBadRequest) {
		t.Errorf("unknown action: err = %v", err)
	}
	if _, err := a.Do(context.Background(), "retry", "q", "bogus", []string{"q-p0"}, actIP); !errors.Is(err, ErrBadRequest) {
		t.Errorf("unknown source: err = %v", err)
	}
}

func TestAction_IDLimitsAndUnknownQueue_400(t *testing.T) {
	a, _, rc := newTestActions(t)
	seedIndexed(t, rc, "q", 1, 0, 0, 0)
	ctx := context.Background()
	ids := make([]string, 101)
	for i := range ids {
		ids[i] = fmt.Sprintf("id%d", i)
	}
	if _, err := a.Do(ctx, "delete", "q", SourcePending, ids, actIP); !errors.Is(err, ErrBadRequest) {
		t.Errorf("101 ids: err = %v", err)
	}
	if rc.LLen(ctx, gqredis.KeyPending("q")).Val() != 1 {
		t.Fatal("nothing may be processed on a rejected call")
	}
	if _, err := a.Do(ctx, "delete", "q", SourcePending, ids[:100], actIP); err != nil {
		t.Errorf("100 ids must be allowed: %v", err)
	}
	if _, err := a.Do(ctx, "delete", "q", SourcePending, nil, actIP); !errors.Is(err, ErrBadRequest) {
		t.Errorf("empty ids: err = %v", err)
	}
	if _, err := a.Do(ctx, "delete", "no-such-queue", SourcePending, []string{"x"}, actIP); !errors.Is(err, ErrBadRequest) {
		t.Errorf("unknown queue: err = %v", err)
	}
}

func TestAction_IndexConsistency(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		act  Action
		src  Source
		id   string
		want bool
	}{
		{"kill", SourcePending, "q-p0", false},
		{"kill", SourceRetry, "q-r0", false},
		{"delete", SourcePending, "q-p0", false},
		{"delete", SourceRetry, "q-r0", false},
		{"retry", SourceRetry, "q-r0", true},
		{"retry", SourceDead, "q-d0", true},
		{"restore", SourceDeleted, "q-x0", true},
	} {
		t.Run(string(tc.act)+"_"+string(tc.src), func(t *testing.T) {
			a, _, rc := newTestActions(t)
			seedIndexed(t, rc, "q", 1, 1, 1, 1)
			mustDo(t, a, tc.act, "q", tc.src, tc.id)
			if got := rc.HExists(ctx, gqredis.KeyIndex("q"), tc.id).Val(); got != tc.want {
				t.Fatalf("index has %s = %v, want %v", tc.id, got, tc.want)
			}
		})
	}
}

func TestKill_ConcurrentSameJob_ExactlyOneDone(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 1, 0, 0, 0)

	var wg sync.WaitGroup
	results := make([]ActionResult, 2)
	errs := make([]error, 2)
	start := make(chan struct{})
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = a.Do(ctx, "kill", "q", SourcePending, []string{"q-p0"}, actIP)
		}(i)
	}
	close(start)
	wg.Wait()

	done, nf := 0, 0
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %v", i, errs[i])
		}
		done += len(results[i].Done)
		nf += len(results[i].NotFound)
	}
	if done != 1 || nf != 1 {
		t.Fatalf("done=%d notFound=%d, want 1/1", done, nf)
	}
	if n := rc.LLen(ctx, "dlq:q").Val(); n != 1 {
		t.Fatalf("dead len = %d, want 1", n)
	}
}

func TestRestore_UnregisteredQueue_400(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	rc.RPush(ctx, gqredis.KeyDeleted("q"), `{"entry":{"job":{},"id":"x","job_name":"Job"},"job_id":"x"}`)
	if _, err := a.Do(ctx, "restore", "q", SourceDeleted, []string{"x"}, actIP); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("err = %v, want ErrBadRequest", err)
	}
	if rc.LLen(ctx, gqredis.KeyDeleted("q")).Val() != 1 {
		t.Fatal("deleted list must be unchanged")
	}
}

func TestRetry_FromDead_LiveCopyInProcessing_NotFound(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 0, 0, 1, 0)
	// Worker still holds q-d0 in processing (DLQ push happens before Ack).
	live := queuedJob(t, "q-d0", "Job", 3)
	rc.LPush(ctx, "processing:q", live)
	rc.HSet(ctx, gqredis.KeyIndex("q"), "q-d0", live)

	res := mustDo(t, a, "retry", "q", SourceDead, "q-d0")
	eqStrings(t, "NotFound", res.NotFound, []string{"q-d0"})
	if len(res.Done) != 0 {
		t.Fatalf("Done = %v", res.Done)
	}
	if got := rc.HGet(ctx, gqredis.KeyIndex("q"), "q-d0").Val(); got != live {
		t.Fatalf("index overwritten: %q", got)
	}
	if rc.LLen(ctx, "dlq:q").Val() != 1 || rc.LLen(ctx, gqredis.KeyPending("q")).Val() != 0 {
		t.Fatal("dead entry must stay and pending must stay empty")
	}
}

func TestDelete_FromDead_KeepsLivePendingIndex(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 1, 0, 0, 0)
	rc.RPush(ctx, "dlq:q", mustJSON(t, dlq.Entry{Job: json.RawMessage(`{"n":1}`), JobID: "q-p0",
		JobName: "Job", Queue: "q", Error: "old", FailedAt: time.Now().UTC()}))
	live := rc.LIndex(ctx, gqredis.KeyPending("q"), 0).Val()

	res := mustDo(t, a, "delete", "q", SourceDead, "q-p0")
	eqStrings(t, "Done", res.Done, []string{"q-p0"})
	if got := rc.HGet(ctx, gqredis.KeyIndex("q"), "q-p0").Val(); got != live {
		t.Fatalf("index = %q, want live pending member", got)
	}
	pend := rc.LRange(ctx, gqredis.KeyPending("q"), 0, -1).Val()
	eqStrings(t, "pending", pend, []string{live})
}

func TestAction_DuplicateIDs_Deduped(t *testing.T) {
	a, _, rc := newTestActions(t)
	ctx := context.Background()
	seedIndexed(t, rc, "q", 2, 0, 0, 0)
	res := mustDo(t, a, "delete", "q", SourcePending, "q-p1", "q-p0", "q-p1", "nope", "nope")
	eqStrings(t, "Done", res.Done, []string{"q-p1", "q-p0"})
	eqStrings(t, "NotFound", res.NotFound, []string{"nope"})
	if n := rc.LLen(ctx, gqredis.KeyDeleted("q")).Val(); n != 2 {
		t.Fatalf("deleted len = %d", n)
	}
}
