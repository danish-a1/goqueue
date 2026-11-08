package dlq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/job"
	"github.com/redis/go-redis/v9"
)

type dlqTestJob struct {
	Value string `json:"value"`
}

func (j *dlqTestJob) Process(ctx context.Context) error { return nil }

func newTestRedisDLQ(t *testing.T) (*RedisDLQ, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisDLQ(client, logger.NewZapLogger()), mr
}

func readEntries(t *testing.T, mr *miniredis.Miniredis, key string) []Entry {
	t.Helper()
	raw, err := mr.List(key)
	if err != nil {
		t.Fatalf("read DLQ list %s: %v", key, err)
	}
	entries := make([]Entry, 0, len(raw))
	for _, item := range raw {
		var e Entry
		if err := json.Unmarshal([]byte(item), &e); err != nil {
			t.Fatalf("decode DLQ entry: %v", err)
		}
		entries = append(entries, e)
	}
	return entries
}

func TestRedisDLQPushStoresJobAndError(t *testing.T) {
	d, mr := newTestRedisDLQ(t)
	jc := &job.JobContext{Job: &dlqTestJob{Value: "x"}, JobID: "job-1", QueueName: "orders"}

	if err := d.Push(context.Background(), jc, errors.New("boom")); err != nil {
		t.Fatalf("Push returned error: %v", err)
	}

	entries := readEntries(t, mr, "dlq:orders")
	if len(entries) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(entries))
	}
	if entries[0].JobID != "job-1" || entries[0].Queue != "orders" {
		t.Errorf("unexpected entry ids: %+v", entries[0])
	}
	if entries[0].Error != "boom" {
		t.Errorf("expected error %q, got %q", "boom", entries[0].Error)
	}
	if string(entries[0].Job) != `{"value":"x"}` {
		t.Errorf("unexpected job payload: %s", entries[0].Job)
	}
}

func TestRedisDLQPushWithNilErrorDoesNotPanic(t *testing.T) {
	d, mr := newTestRedisDLQ(t)
	jc := &job.JobContext{Job: &dlqTestJob{Value: "y"}, JobID: "job-2", QueueName: "orders"}

	if err := d.Push(context.Background(), jc, nil); err != nil {
		t.Fatalf("Push returned error: %v", err)
	}

	entries := readEntries(t, mr, "dlq:orders")
	if len(entries) != 1 || entries[0].Error != "" {
		t.Fatalf("expected one entry with empty error, got %+v", entries)
	}
}

func TestRedisDLQ_PushTrimsAndExpires(t *testing.T) {
	d, mr := newTestRedisDLQ(t)
	total := MaxEntries + 2
	for i := 0; i < total; i++ {
		jc := &job.JobContext{Job: &dlqTestJob{Value: "v"}, JobID: fmt.Sprintf("job-%d", i), QueueName: "orders"}
		if err := d.Push(context.Background(), jc, errors.New("boom")); err != nil {
			t.Fatalf("Push %d: %v", i, err)
		}
	}
	raw, err := mr.List("dlq:orders")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(raw) != MaxEntries {
		t.Fatalf("LLEN = %d, want %d", len(raw), MaxEntries)
	}
	if ttl := mr.TTL("dlq:orders"); ttl <= 0 {
		t.Errorf("TTL = %v, want > 0", ttl)
	}
	var newest map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw[len(raw)-1]), &newest); err != nil {
		t.Fatalf("decode tail: %v", err)
	}
	if string(newest["job_id"]) != fmt.Sprintf("%q", fmt.Sprintf("job-%d", total-1)) {
		t.Errorf("tail job_id = %s, want newest at tail", newest["job_id"])
	}
	if _, ok := newest["queue"]; !ok {
		t.Error("field queue missing")
	}
	if _, ok := newest["queue_name"]; ok {
		t.Error("legacy field queue_name must be absent")
	}
}

func TestRedisDLQ_DLQKey(t *testing.T) {
	d, _ := newTestRedisDLQ(t)
	if got := d.DLQKey("q"); got != "dlq:q" {
		t.Errorf("DLQKey = %q, want dlq:q", got)
	}
	var _ KeyProvider = d
}
