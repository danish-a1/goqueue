package dashboard

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/danish-a1/goqueue/adapter"
	gqredis "github.com/danish-a1/goqueue/adapter/redis"
)

// QueueSummary is the per-queue counter row.
type QueueSummary struct {
	Name, DeadKey                       string
	Pending, Busy, Retry, Dead, Deleted int64
	Processed, Failed                   int64
}

// Summary is the dashboard overview.
type Summary struct {
	Processed, Failed, Pending, Busy, Retry, Dead, Deleted int64
	Processes                                              int
	Queues                                                 []QueueSummary
	At                                                     time.Time
}

// DayStat is one UTC day of history.
type DayStat struct {
	Day               string
	Processed, Failed int64
}

// Process is a live worker process with its in-flight jobs.
type Process struct {
	adapter.ProcessInfo
	Busy []BusyJob
}

// BusyJob is one in-flight job.
type BusyJob struct {
	JobID, Queue, JobName string
	StartedAt             time.Time
}

type reader struct{ rdb redis.UniversalClient }

func newReader(rdb redis.UniversalClient) *reader { return &reader{rdb: rdb} }

func clamp(v, lo, hi int) int { return min(max(v, lo), hi) }

func unixTime(s string) time.Time {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return time.Time{}
	}
	return time.Unix(n, 0).UTC()
}

// Summary reads all counters: one HGETALL, one pipeline over every queue, then the process reads.
func (r *reader) Summary(ctx context.Context) (Summary, error) {
	queues, err := r.rdb.HGetAll(ctx, gqredis.KeyQueues).Result()
	if err != nil {
		return Summary{}, err
	}
	type cmds struct{ pending, retry, dead, deleted, proc, fail *redis.Cmd }
	names := make([]string, 0, len(queues))
	for n := range queues {
		names = append(names, n)
	}
	cs := make([]cmds, len(names))
	var total, totalFail *redis.StringCmd
	cmders, err := r.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		total, totalFail = p.Get(ctx, gqredis.KeyStatProcessed), p.Get(ctx, gqredis.KeyStatFailed)
		for i, n := range names {
			c := &cs[i]
			c.pending = p.Do(ctx, "LLEN", gqredis.KeyPending(n))
			c.retry = p.Do(ctx, "ZCARD", gqredis.KeyRetry(n))
			c.deleted = p.Do(ctx, "LLEN", gqredis.KeyDeleted(n))
			if queues[n] != "" {
				c.dead = p.Do(ctx, "LLEN", queues[n])
			}
			c.proc = p.Do(ctx, "GET", gqredis.KeyStatQueue(n, "processed"))
			c.fail = p.Do(ctx, "GET", gqredis.KeyStatQueue(n, "failed"))
		}
		return nil
	})
	if err := pipeErr(cmders, err); err != nil {
		return Summary{}, err
	}
	procs, err := r.Processes(ctx)
	if err != nil {
		return Summary{}, err
	}
	busy := map[string]int64{}
	for _, p := range procs {
		for _, b := range p.Busy {
			busy[b.Queue]++
		}
	}
	s := Summary{At: time.Now().UTC(), Processes: len(procs), Queues: make([]QueueSummary, len(names))}
	s.Processed, _ = total.Int64()
	s.Failed, _ = totalFail.Int64()
	for i, n := range names {
		c := cs[i]
		q := QueueSummary{Name: n, DeadKey: queues[n], Busy: busy[n]}
		q.Pending, _ = c.pending.Int64()
		q.Retry, _ = c.retry.Int64()
		q.Deleted, _ = c.deleted.Int64()
		if c.dead != nil {
			q.Dead, _ = c.dead.Int64()
		}
		q.Processed, _ = intCmd(c.proc)
		q.Failed, _ = intCmd(c.fail)
		s.Queues[i] = q
		s.Pending += q.Pending
		s.Busy += q.Busy
		s.Retry += q.Retry
		s.Dead += q.Dead
		s.Deleted += q.Deleted
	}
	return s, nil
}

// pipeErr returns the first real command error; redis.Nil (missing counter) is expected.
func pipeErr(cmds []redis.Cmder, err error) error {
	if err == nil {
		return nil
	}
	for _, c := range cmds {
		if e := c.Err(); e != nil && !errors.Is(e, redis.Nil) {
			return e
		}
	}
	if errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}

func intCmd(c *redis.Cmd) (int64, error) {
	s, err := c.Text()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(s, 10, 64)
}

// History returns the last days UTC days (clamped 1..180), oldest first.
func (r *reader) History(ctx context.Context, days int) ([]DayStat, error) {
	days = clamp(days, 1, 180)
	now := time.Now().UTC()
	out := make([]DayStat, days)
	proc, fail := make([]*redis.StringCmd, days), make([]*redis.StringCmd, days)
	cmders, err := r.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i := range out {
			d := now.AddDate(0, 0, i-days+1)
			out[i].Day = d.Format("2006-01-02")
			proc[i] = p.Get(ctx, gqredis.KeyStatDaily("processed", d))
			fail[i] = p.Get(ctx, gqredis.KeyStatDaily("failed", d))
		}
		return nil
	})
	if err := pipeErr(cmders, err); err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Processed, _ = proc[i].Int64()
		out[i].Failed, _ = fail[i].Int64()
	}
	return out, nil
}

// Processes lists live processes; set members whose heartbeat hash expired are removed.
func (r *reader) Processes(ctx context.Context) ([]Process, error) {
	ids, err := r.rdb.SMembers(ctx, gqredis.KeyProcesses).Result()
	if err != nil || len(ids) == 0 {
		return nil, err
	}
	hs, ws := make([]*redis.MapStringStringCmd, len(ids)), make([]*redis.MapStringStringCmd, len(ids))
	_, err = r.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, id := range ids {
			hs[i], ws[i] = p.HGetAll(ctx, gqredis.KeyProcess(id)), p.HGetAll(ctx, gqredis.KeyProcessWork(id))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var out []Process
	var dead []any
	for i, id := range ids {
		h := hs[i].Val()
		if len(h) == 0 {
			dead = append(dead, id)
			continue
		}
		pid, _ := strconv.Atoi(h["pid"])
		conc, _ := strconv.Atoi(h["concurrency"])
		out = append(out, Process{
			ProcessInfo: adapter.ProcessInfo{ID: id, Host: h["host"], Queue: h["queue"], PID: pid,
				Concurrency: conc, StartedAt: unixTime(h["started_at"])},
			Busy: busyJobs(ws[i].Val()),
		})
	}
	slices.SortFunc(out, func(a, b Process) int {
		return cmp.Or(a.StartedAt.Compare(b.StartedAt), cmp.Compare(a.ID, b.ID))
	})
	if len(dead) > 0 {
		r.rdb.SRem(ctx, gqredis.KeyProcesses, dead...)
	}
	return out, nil
}

func busyJobs(work map[string]string) []BusyJob {
	out := make([]BusyJob, 0, len(work))
	for id, raw := range work {
		var w struct {
			Queue     string `json:"queue"`
			JobName   string `json:"job_name"`
			StartedAt string `json:"started_at"`
		}
		if json.Unmarshal([]byte(raw), &w) != nil {
			continue
		}
		out = append(out, BusyJob{JobID: id, Queue: w.Queue, JobName: w.JobName, StartedAt: unixTime(w.StartedAt)})
	}
	slices.SortFunc(out, func(a, b BusyJob) int {
		return cmp.Or(a.StartedAt.Compare(b.StartedAt), cmp.Compare(a.JobID, b.JobID))
	})
	return out
}
