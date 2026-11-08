package dlq

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/danish-a1/goqueue/adapter/utils"
	"github.com/danish-a1/goqueue/job"
)

const (
	// MaxEntries caps how many dead jobs are kept per queue.
	MaxEntries = 1000
	// EntryTTL is how long a DLQ list lives after its last push.
	EntryTTL = 30 * 24 * time.Hour
	// MaxErrorLen is the maximum stored error size in bytes.
	MaxErrorLen = 256
)

// Entry is the single serialized shape of a dead job.
type Entry struct {
	Job      json.RawMessage `json:"job"`
	JobID    string          `json:"job_id"`
	JobName  string          `json:"job_name,omitempty"`
	Queue    string          `json:"queue"`
	Error    string          `json:"error"`
	FailedAt time.Time       `json:"failed_at"`
}

// KeyProvider is implemented by DLQ adapters that store into a Redis list.
type KeyProvider interface{ DLQKey(queueName string) string }

// NewEntry builds an Entry from a failed job; the error is truncated UTF-8-safely.
func NewEntry(jc *job.JobContext, jobErr error) (Entry, error) {
	data, err := json.Marshal(jc.Job)
	if err != nil {
		return Entry{}, fmt.Errorf("failed to marshal job for DLQ: %w", err)
	}
	msg := ""
	if jobErr != nil {
		msg = utils.TruncateUTF8(jobErr.Error(), MaxErrorLen)
	}
	return Entry{
		Job:      data,
		JobID:    jc.JobID,
		JobName:  utils.GetJobName(jc.Job),
		Queue:    jc.QueueName,
		Error:    msg,
		FailedAt: time.Now(),
	}, nil
}

// DecodeEntry parses an entry, accepting the legacy "queue_name" field.
func DecodeEntry(b []byte) (Entry, error) {
	var e struct {
		Entry
		LegacyQueue string `json:"queue_name"`
	}
	if err := json.Unmarshal(b, &e); err != nil {
		return Entry{}, fmt.Errorf("failed to decode DLQ entry: %w", err)
	}
	if e.Queue == "" {
		e.Queue = e.LegacyQueue
	}
	return e.Entry, nil
}
