package redis

import "time"

// Tracking keys. This file is the single source of truth for key names; the
// dashboard reads exactly these keys.
const (
	KeyQueues        = "goqueue:queues"    // HASH queue -> dead list key ("" = no Redis DLQ)
	KeyProcesses     = "goqueue:processes" // SET process IDs
	KeyStatProcessed = "goqueue:stat:processed"
	KeyStatFailed    = "goqueue:stat:failed"
	HeartbeatTTL     = 60 * time.Second
	HeartbeatEvery   = 10 * time.Second
	DailyStatTTL     = 180 * 24 * time.Hour
)

// KeyStatDaily is goqueue:stat:<kind>:<YYYY-MM-DD> (UTC day).
func KeyStatDaily(kind string, day time.Time) string {
	return "goqueue:stat:" + kind + ":" + day.UTC().Format("2006-01-02")
}

// KeyStatQueue is goqueue:stat:<queue>:<kind>.
func KeyStatQueue(queue, kind string) string { return "goqueue:stat:" + queue + ":" + kind }

// KeyProcess is the heartbeat hash of one worker process.
func KeyProcess(id string) string { return "goqueue:process:" + id }

// KeyProcessWork is the HASH jobID -> {"queue","job_name","started_at"} of in-flight jobs.
func KeyProcessWork(id string) string { return KeyProcess(id) + ":work" }

// KeyDeleted is the per-queue LIST of deleted-entry envelopes ({"entry","job_id","queue","from","deleted_at","deleted_by_ip"}), capped like the DLQ.
func KeyDeleted(queue string) string { return "goqueue:deleted:" + queue }

// KeyPending is the pending list (the queue name itself).
func KeyPending(queue string) string { return queue }

// KeyRetry is the retry ZSET.
func KeyRetry(queue string) string { return retryQueuePrefix + queue }

// KeyIndex is the job index hash.
func KeyIndex(queue string) string { return "job_index:" + queue }
