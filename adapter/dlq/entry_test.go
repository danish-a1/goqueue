package dlq

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/danish-a1/goqueue/job"
)

type testJob struct {
	Value string `json:"value"`
}

func (j *testJob) Process(ctx context.Context) error { return nil }

func TestNewEntry_SetsJobNameAndTruncatesError(t *testing.T) {
	jc := &job.JobContext{Job: &testJob{Value: "x"}, JobID: "j1", QueueName: "q"}
	// 255 ASCII bytes then a 3-byte rune: the 256-byte cut lands inside the rune. Total > 300 bytes.
	long := strings.Repeat("a", 255) + strings.Repeat("€", 20)

	e, err := NewEntry(jc, errors.New(long))
	if err != nil {
		t.Fatalf("NewEntry error: %v", err)
	}
	if len(e.Error) > MaxErrorLen || !utf8.ValidString(e.Error) {
		t.Errorf("error not truncated safely: len=%d valid=%v", len(e.Error), utf8.ValidString(e.Error))
	}
	if e.JobName != "testJob" {
		t.Errorf("JobName = %q, want testJob", e.JobName)
	}
	if e.JobID != "j1" || e.Queue != "q" || e.FailedAt.IsZero() {
		t.Errorf("unexpected entry: %+v", e)
	}
	if string(e.Job) != `{"value":"x"}` {
		t.Errorf("job payload = %s", e.Job)
	}
}

func TestDecodeEntry_LegacyQueueName(t *testing.T) {
	e, err := DecodeEntry([]byte(`{"queue_name":"q","job_id":"1","job":{}}`))
	if err != nil {
		t.Fatalf("DecodeEntry error: %v", err)
	}
	if e.Queue != "q" {
		t.Errorf("Queue = %q, want q", e.Queue)
	}
}

func TestDecodeEntry_FleetleryShape(t *testing.T) {
	in := `{"failed_at":"2026-11-08T10:00:00.123456789+01:00","queue":"q","job_id":"1","error":"x","job":{"a":1}}`
	e, err := DecodeEntry([]byte(in))
	if err != nil {
		t.Fatalf("DecodeEntry error: %v", err)
	}
	if e.FailedAt.IsZero() || e.Queue != "q" || e.JobID != "1" || e.Error != "x" {
		t.Errorf("unexpected entry: %+v", e)
	}
}
