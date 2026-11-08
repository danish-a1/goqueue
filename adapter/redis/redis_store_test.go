package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/alicebob/miniredis/v2"
	"github.com/danish-a1/goqueue/config"
	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/danish-a1/goqueue/internal/manager"
	"github.com/danish-a1/goqueue/internal/registry"
	"github.com/danish-a1/goqueue/job"
	"github.com/redis/go-redis/v9"
)

// TestJob is a simple job implementation for testing
type TestJob struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

// Process implements the job.Job interface
func (j *TestJob) Process(ctx context.Context) error {
	return nil
}

// setupTestRedis creates a miniredis instance for testing
func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	// Create a mini Redis server
	miniRedis, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Error creating miniredis: %v", err)
	}

	// Create a Redis client connected to the mini Redis server
	client := redis.NewClient(&redis.Options{
		Addr: miniRedis.Addr(),
		DB:   0,
	})

	return miniRedis, client
}

// TestRedisPushOnly tests just the Push operation
func TestRedisPushOnly(t *testing.T) {
	// Setup miniredis for testing
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	// Create test logger
	testLogger := logger.NewZapLogger()

	// Create Redis manager
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", "", 0, testLogger)

	// Create configuration
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", "", 0)

	// Create Redis store
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	// Ensure registry is clean for this run by registering the test job if not present
	if _, ok := registry.GetFromRegistery("TestJob"); !ok {
		registry.Register("TestJob", func() job.Job { return &TestJob{} })
	}

	// Create a test job
	testJob := &TestJob{
		ID:   "test123",
		Data: "test data",
	}

	// Define a queue name for testing
	queueName := "test_queue_push_only"

	// Test pushing a job
	err := store.Push(queueName, testJob)
	if err != nil {
		t.Fatalf("Error pushing job: %v", err)
	}

	// Verify the queue length in Redis
	queueLen, err := client.LLen(context.Background(), queueName).Result()
	if err != nil {
		t.Fatalf("Error getting queue length: %v", err)
	}
	if queueLen != 1 {
		t.Errorf("Expected queue length of 1, got %d", queueLen)
	}

	// Verify that we created an index entry
	indexKey := fmt.Sprintf(JobIndexKeyFormat, queueName)
	count, err := client.HLen(context.Background(), indexKey).Result()
	if err != nil {
		t.Fatalf("Error getting job index count: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 job in index, got %d", count)
	}
}

// TestRedisPushBatch tests the PushBatch operation
func TestRedisPushBatch(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	// Register a unique job name for this test
	if _, ok := registry.GetFromRegistery("TestJob"); !ok {
		registry.Register("TestJob", func() job.Job { return &TestJob{} })
	}

	// Prepare batch jobs
	jobs := []job.Job{
		&TestJob{ID: "id1", Data: "one"},
		&TestJob{ID: "id2", Data: "two"},
	}
	queueName := "test_queue_push_batch"

	// Push the batch
	if err := store.PushBatch(queueName, jobs); err != nil {
		t.Fatalf("PushBatch returned error: %v", err)
	}

	// Verify queue length
	queueLen, err := client.LLen(context.Background(), queueName).Result()
	if err != nil {
		t.Fatalf("Error getting queue length: %v", err)
	}
	if queueLen != int64(len(jobs)) {
		t.Errorf("Expected queue length %d, got %d", len(jobs), queueLen)
	}

	// Verify index entries
	indexKey := fmt.Sprintf(JobIndexKeyFormat, queueName)
	count, err := client.HLen(context.Background(), indexKey).Result()
	if err != nil {
		t.Fatalf("Error getting job index count: %v", err)
	}
	if count != int64(len(jobs)) {
		t.Errorf("Expected %d jobs in index, got %d", len(jobs), count)
	}
}

// TestRedisPopAndAck tests Pop and Ack operations together
func TestRedisPopAndAck(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	// Register job (idempotent)
	if _, ok := registry.GetFromRegistery("TestJob"); !ok {
		registry.Register("TestJob", func() job.Job { return &TestJob{} })
	}

	// Push a job using Push so index + list are created
	queueName := "test_queue_pop_ack"
	testJob := &TestJob{ID: "popid", Data: "popdata"}
	if err := store.Push(queueName, testJob); err != nil {
		t.Fatalf("Push error: %v", err)
	}

	// Pop the job
	jobCtx, err := store.Pop(queueName)
	if err != nil {
		t.Fatalf("Pop error: %v", err)
	}
	if jobCtx.Job == nil {
		t.Fatal("Expected job from Pop, got nil")
	}

	// Verify job type and data
	popped, ok := jobCtx.Job.(*TestJob)
	if !ok {
		t.Fatalf("Expected *TestJob, got %T", jobCtx.Job)
	}
	if popped.ID != "popid" || popped.Data != "popdata" {
		t.Errorf("Popped job data mismatch: %+v", popped)
	}

	// Acknowledge the job
	if err := store.Ack(queueName, jobCtx.JobID); err != nil {
		t.Fatalf("Ack error: %v", err)
	}

	// After ack, processing queue should be empty
	processingKey := processingQueueName + queueName
	plen, err := client.LLen(context.Background(), processingKey).Result()
	if err != nil {
		t.Fatalf("Error getting processing queue length: %v", err)
	}
	if plen != 0 {
		t.Errorf("Expected processing queue empty after Ack, got %d", plen)
	}

	// Index should have removed the job
	indexKey := fmt.Sprintf(JobIndexKeyFormat, queueName)
	count, err := client.HLen(context.Background(), indexKey).Result()
	if err != nil {
		t.Fatalf("Error getting index HLen: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected index empty after Ack, got %d", count)
	}
}

// TestEnqueueDequeueMetrics verifies metrics can be enqueued and dequeued
func TestEnqueueDequeueMetrics(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	queueName := "test_metrics_queue"
	metrics := config.JobMetrics{
		QueueName: queueName,
		JobID:     "m1",
		Duration:  150 * time.Millisecond,
		Error:     nil,
		Timestamp: time.Now(),
	}

	if err := store.EnqueueMetrics(metrics); err != nil {
		t.Fatalf("EnqueueMetrics error: %v", err)
	}

	// Dequeue metrics; DequeueMetrics pops the item without an ack list
	got, err := store.DequeueMetrics(queueName)
	if err != nil {
		t.Fatalf("DequeueMetrics error: %v", err)
	}

	if got.JobID != metrics.JobID || got.QueueName != metrics.QueueName {
		t.Fatalf("Dequeued metrics mismatch. want=%+v got=%+v", metrics, got)
	}
}

// TestRedisIsHealthy validates IsHealthy returns true when manager has no negative status
func TestRedisIsHealthy(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	if !store.IsHealthy() {
		t.Fatalf("Expected store to be healthy when Redis is available")
	}
}

// BadJob simulates a job that fails JSON marshaling
type BadJob struct{}

func (b *BadJob) Process(ctx context.Context) error { return nil }

func (b *BadJob) MarshalJSON() ([]byte, error) { return nil, fmt.Errorf("intentional marshal failure") }

// TestPushWhenRedisUnavailable verifies Push returns an error when Redis is down
func TestPushWhenRedisUnavailable(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	// capture addr then close server to simulate unavailability
	addr := miniRedis.Addr()
	miniRedis.Close()

	// Create store that points to closed redis
	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(addr, "", "", 0, testLogger)
	cfg := config.NewRedisConfig(addr, "", "", 0)
	store := NewRedisStore(client, cfg, redisManager, addr, 0, testLogger)

	// Ensure registry has TestJob
	if _, ok := registry.GetFromRegistery("TestJob"); !ok {
		registry.Register("TestJob", func() job.Job { return &TestJob{} })
	}

	// Attempt to push; should return error due to underlying Redis being unavailable
	testJob := &TestJob{ID: "u1", Data: "unavail"}
	if err := store.Push("q_unavail", testJob); err == nil {
		t.Fatalf("Expected error when pushing to unavailable Redis, got nil")
	} else {
		t.Logf("Push returned expected error: %v", err)
	}
}

// TestPushBatchWithMarshalError ensures PushBatch continues when some jobs fail to marshal
func TestPushBatchWithMarshalError(t *testing.T) {
	miniRedis, client := setupTestRedis(t)
	defer miniRedis.Close()

	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", "", 0)
	store := NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)

	// Ensure registry has TestJob
	if _, ok := registry.GetFromRegistery("TestJob"); !ok {
		registry.Register("TestJob", func() job.Job { return &TestJob{} })
	}

	jobs := []job.Job{
		&BadJob{},
		&TestJob{ID: "good1", Data: "ok"},
	}
	queueName := "q_pushbatch_marshal_err"

	// PushBatch should succeed (skip bad job) and push the good job
	if err := store.PushBatch(queueName, jobs); err != nil {
		t.Fatalf("PushBatch error: %v", err)
	}

	// Verify only the good job is enqueued
	qlen, err := client.LLen(context.Background(), queueName).Result()
	if err != nil {
		t.Fatalf("LLen error: %v", err)
	}
	if qlen != 1 {
		t.Fatalf("Expected 1 job in queue, got %d", qlen)
	}

	indexKey := fmt.Sprintf(JobIndexKeyFormat, queueName)
	hlen, err := client.HLen(context.Background(), indexKey).Result()
	if err != nil {
		t.Fatalf("HLen error: %v", err)
	}
	if hlen != 1 {
		t.Fatalf("Expected 1 job in index, got %d", hlen)
	}
}

func newMetricsTestStore(t *testing.T) (*miniredis.Miniredis, *RedisStore) {
	t.Helper()
	miniRedis, client := setupTestRedis(t)
	t.Cleanup(miniRedis.Close)
	testLogger := logger.NewZapLogger()
	redisManager := manager.NewRedisClientManager(miniRedis.Addr(), "", "", 0, testLogger)
	cfg := config.NewRedisConfig(miniRedis.Addr(), "", "", 0)
	return miniRedis, NewRedisStore(client, cfg, redisManager, miniRedis.Addr(), 0, testLogger)
}

func TestDequeueMetrics_DoesNotGrowAckList(t *testing.T) {
	miniRedis, store := newMetricsTestStore(t)
	q := "q"

	for i := 0; i < 3; i++ {
		m := config.JobMetrics{QueueName: q, JobID: fmt.Sprintf("m%d", i), Timestamp: time.Now()}
		if err := store.EnqueueMetrics(m); err != nil {
			t.Fatalf("EnqueueMetrics error: %v", err)
		}
	}
	for i := 0; i < 3; i++ {
		got, err := store.DequeueMetrics(q)
		if err != nil {
			t.Fatalf("DequeueMetrics error: %v", err)
		}
		// EnqueueMetrics LPUSHes, dequeue pops the tail: FIFO.
		if want := fmt.Sprintf("m%d", i); got.JobID != want || got.QueueName != q {
			t.Fatalf("dequeue %d: want JobID %s, got %+v", i, want, got)
		}
	}
	if miniRedis.Exists(q + ":metrics:ack") {
		t.Error("metrics ack list must not exist after DequeueMetrics")
	}
}

func TestRetryJobWithMetadata_StoresLastError(t *testing.T) {
	miniRedis, store := newMetricsTestStore(t)
	if _, ok := registry.GetFromRegistery("TestJob"); !ok {
		registry.Register("TestJob", func() job.Job { return &TestJob{} })
	}
	q := "retry_last_error_q"
	if err := store.Push(q, &TestJob{ID: "r1", Data: "d"}); err != nil {
		t.Fatalf("Push error: %v", err)
	}
	jc, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop error: %v", err)
	}
	// 300 bytes: 'a' * 255 then 3-byte rune straddling the 256 cut.
	long := strings.Repeat("a", 255) + strings.Repeat("€", 15)
	jc.LastError = long

	if err := store.RetryJobWithMetadata(q, jc, time.Hour); err != nil {
		t.Fatalf("RetryJobWithMetadata error: %v", err)
	}

	members, err := miniRedis.ZMembers("retry:" + q)
	if err != nil || len(members) != 1 {
		t.Fatalf("expected 1 retry member, got %v (err %v)", members, err)
	}
	var queued job.RedisQueuedJob
	if err := json.Unmarshal([]byte(members[0]), &queued); err != nil {
		t.Fatalf("decode retry member: %v", err)
	}
	if queued.ErrorMessage == "" || len(queued.ErrorMessage) > 256 || !utf8.ValidString(queued.ErrorMessage) {
		t.Errorf("ErrorMessage not truncated safely: len=%d valid=%v", len(queued.ErrorMessage), utf8.ValidString(queued.ErrorMessage))
	}
	if queued.FailedAt == nil {
		t.Error("FailedAt must be set")
	}
	if got := miniRedis.HGet(fmt.Sprintf(JobIndexKeyFormat, q), jc.JobID); got != members[0] {
		t.Errorf("job_index payload differs from retry member:\n index=%s\n member=%s", got, members[0])
	}
}

func TestPop_CarriesErrorMessageIntoLastError(t *testing.T) {
	miniRedis, store := newMetricsTestStore(t)
	if _, ok := registry.GetFromRegistery("TestJob"); !ok {
		registry.Register("TestJob", func() job.Job { return &TestJob{} })
	}
	q := "pop_last_error_q"
	withErr := `{"job":{"id":"a","data":"b"},"id":"j1","job_name":"TestJob","enqueued_at":"2026-01-01T00:00:00Z","retry_count":1,"error_message":"boom","failed_at":"2026-01-01T00:00:01Z"}`
	miniRedis.Lpush(q, withErr)
	jc, err := store.Pop(q)
	if err != nil {
		t.Fatalf("Pop error: %v", err)
	}
	if jc.LastError != "boom" {
		t.Errorf("LastError = %q, want boom", jc.LastError)
	}

	// Legacy envelope without the new fields still decodes.
	legacy := `{"job":{"id":"a","data":"b"},"id":"j2","job_name":"TestJob","enqueued_at":"2026-01-01T00:00:00Z","retry_count":0}`
	miniRedis.Lpush(q, legacy)
	jc, err = store.Pop(q)
	if err != nil {
		t.Fatalf("Pop legacy error: %v", err)
	}
	if jc.JobID != "j2" || jc.LastError != "" {
		t.Errorf("legacy envelope: %+v", jc)
	}
}
