package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/danish-a1/goqueue/adapter"
	"github.com/danish-a1/goqueue/adapter/redis"
)

// trackTimeout bounds every tracking call so a slow Redis cannot stall jobs.
const trackTimeout = 2 * time.Second

// trackCtx is deliberately detached from the job ctx so a cancelled job
// still records its result.
func trackCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), trackTimeout)
}

func newProcessID(queue string) string {
	host, _ := os.Hostname()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s:%d:%s:%s", host, os.Getpid(), queue, hex.EncodeToString(b))
}

// startHeartbeat registers this worker process and refreshes it until
// shutdown or ctx cancel. The goroutine is tracked by w.wg.
func (w *Worker) startHeartbeat(ctx context.Context, concurrency int) {
	if w.tracker == nil {
		return
	}
	host, _ := os.Hostname()
	info := adapter.ProcessInfo{ID: w.processID, Host: host, Queue: w.queueName, PID: os.Getpid(), Concurrency: concurrency, StartedAt: time.Now()}
	beat := func() {
		c, cancel := trackCtx()
		defer cancel()
		if err := w.tracker.Heartbeat(c, info); err != nil {
			w.logger.Error("Heartbeat failed", "queue", w.queueName, "error", err)
		}
	}
	beat()
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		t := time.NewTicker(redis.HeartbeatEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.shutdownCh:
				return
			case <-t.C:
				beat()
			}
		}
	}()
}

// removeProcess deletes this process's tracking keys on shutdown.
func (w *Worker) removeProcess() {
	if w.tracker == nil || w.processID == "" {
		return
	}
	c, cancel := trackCtx()
	defer cancel()
	if err := w.tracker.RemoveProcess(c, w.processID); err != nil {
		w.logger.Error("Failed to remove process", "queue", w.queueName, "error", err)
	}
}

func (w *Worker) trackStarted(jobID, jobName string) {
	if w.tracker == nil {
		return
	}
	c, cancel := trackCtx()
	defer cancel()
	w.tracker.JobStarted(c, w.processID, w.queueName, jobID, jobName)
}

// trackFinished clears the busy entry and counts the attempt.
func (w *Worker) trackFinished(jobID string, success bool) {
	if w.tracker == nil {
		return
	}
	c, cancel := trackCtx()
	defer cancel()
	w.tracker.JobFinished(c, w.processID, jobID)
	w.tracker.RecordResult(c, w.queueName, success)
}
