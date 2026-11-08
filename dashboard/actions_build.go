package dashboard

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/danish-a1/goqueue/adapter/dlq"
	gqredis "github.com/danish-a1/goqueue/adapter/redis"
	"github.com/danish-a1/goqueue/job"
)

const killedError = "killed from dashboard"

// deletedEntry is one member of goqueue:deleted:<queue>; Entry is the original member verbatim.
type deletedEntry struct {
	Entry       json.RawMessage `json:"entry"`
	JobID       string          `json:"job_id"`
	Queue       string          `json:"queue"`
	From        Source          `json:"from"`
	DeletedAt   time.Time       `json:"deleted_at"`
	DeletedByIP string          `json:"deleted_by_ip"`
}

// resolve returns the exact current source member per ID ("" = not there). Pending and
// retry use job_index (one HMGET); dead and deleted scan the list, which is capped at
// dlq.MaxEntries, with the newest member winning for a repeated ID. A dead list can be
// shared by several queues (custom DLQ key), so entries of another queue are skipped.
func (a *actions) resolve(ctx context.Context, t target, ids []string) ([]string, error) {
	out := make([]string, len(ids))
	if t.src == SourcePending || t.src == SourceRetry {
		vals, err := a.rdb.HMGet(ctx, gqredis.KeyIndex(t.queue), ids...).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}
		for i, v := range vals {
			out[i], _ = v.(string)
		}
		return out, nil
	}
	items, err := a.rdb.LRange(ctx, t.srcKey, -dlq.MaxEntries, -1).Result()
	if err != nil {
		return nil, err
	}
	byID := make(map[string]string, len(items))
	for _, m := range items {
		var k struct {
			JobID  string `json:"job_id"`
			Queue  string `json:"queue"`
			Legacy string `json:"queue_name"` // pre-0.3.0 dlq.Entry field
		}
		if json.Unmarshal([]byte(m), &k) != nil || k.JobID == "" {
			continue
		}
		if q := cmp.Or(k.Queue, k.Legacy); q == "" || q == t.queue {
			byID[k.JobID] = m
		}
	}
	for i, id := range ids {
		out[i] = byID[id]
	}
	return out, nil
}

// destination builds the member written to t.dstKey for one source member.
func (t target) destination(id, member string) (string, bool) {
	switch {
	case t.act == ActionDelete:
		return marshal(deletedEntry{Entry: json.RawMessage(member), JobID: id, Queue: t.queue,
			From: t.src, DeletedAt: t.now, DeletedByIP: t.actorIP})
	case t.act == ActionKill:
		var j job.RedisQueuedJob
		if json.Unmarshal([]byte(member), &j) != nil {
			return "", false
		}
		return marshal(dlq.Entry{Job: j.Job, JobID: id, JobName: j.JobName, Queue: t.queue,
			Error: killedError, FailedAt: t.now})
	case t.src == SourceRetry: // retry from the retry ZSET: same member back to pending
		return member, true
	case t.src == SourceDead:
		return t.requeue(id, member)
	default: // restore from deleted
		var d struct { // only the original member; a bad deleted_at must not block a restore
			Entry json.RawMessage `json:"entry"`
		}
		if json.Unmarshal([]byte(member), &d) != nil {
			return "", false
		}
		return t.requeue(id, string(d.Entry))
	}
}

// requeue rebuilds a fresh pending envelope from a queued job or a dlq.Entry. Without a
// job name no worker could decode the job, so it is refused.
func (t target) requeue(id, raw string) (string, bool) {
	var j job.RedisQueuedJob
	if json.Unmarshal([]byte(raw), &j) != nil {
		return "", false
	}
	if j.ID == "" { // not a queued envelope: dlq.Entry shape
		e, err := dlq.DecodeEntry([]byte(raw))
		if err != nil {
			return "", false
		}
		j = job.RedisQueuedJob{Job: e.Job, JobName: e.JobName}
	}
	if j.JobName == "" || len(j.Job) == 0 {
		return "", false
	}
	return marshal(job.RedisQueuedJob{Job: j.Job, ID: id, JobName: j.JobName, EnqueuedAt: t.now})
}

func marshal(v any) (string, bool) {
	b, err := json.Marshal(v)
	return string(b), err == nil
}
