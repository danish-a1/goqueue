// Command dashboard runs the goqueue dashboard against an in-memory Redis
// (miniredis) seeded with one queue holding pending, retry and dead jobs.
//
//	cd examples && go run ./dashboard
//
// Open http://localhost:8080/goqueue-ui and sign in with the printed token.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	gqredis "github.com/danish-a1/goqueue/adapter/redis"
	"github.com/danish-a1/goqueue/dashboard"
	"github.com/danish-a1/goqueue/job"
)

const queue = "email-queue"

func main() {
	mr, err := miniredis.Run()
	if err != nil {
		log.Fatal(err)
	}
	defer mr.Close()
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rc.Close() }()
	if err := seed(context.Background(), rc); err != nil {
		log.Fatal(err)
	}

	b := make([]byte, 32)
	_, _ = rand.Read(b)
	token := hex.EncodeToString(b)
	h, err := dashboard.New(dashboard.Options{BasePath: "/goqueue-ui", AccessToken: token, Redis: rc})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("dashboard: http://localhost:8080/goqueue-ui")
	fmt.Println("token:", token)
	srv := &http.Server{Addr: "localhost:8080", Handler: h, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

// seed registers the queue and writes jobs in the shapes the goqueue Redis store uses.
func seed(ctx context.Context, rc *redis.Client) error {
	now := time.Now().UTC()
	envelope := func(id string, retry int) string {
		b, _ := json.Marshal(job.RedisQueuedJob{Job: json.RawMessage(`{"to":"user@example.com","subject":"<b>Welcome</b>"}`),
			ID: id, JobName: "EmailJob", EnqueuedAt: now, RetryCount: retry})
		return string(b)
	}
	p := rc.TxPipeline()
	p.HSet(ctx, gqredis.KeyQueues, queue, "dlq:"+queue)
	p.Set(ctx, gqredis.KeyStatProcessed, 1234, 0)
	p.Set(ctx, gqredis.KeyStatFailed, 56, 0)
	for d := range 30 {
		day := now.AddDate(0, 0, -d)
		p.Set(ctx, gqredis.KeyStatDaily("processed", day), 40+d%7*5, 0)
		p.Set(ctx, gqredis.KeyStatDaily("failed", day), d%5, 0)
	}
	for i := range 5 {
		p.LPush(ctx, gqredis.KeyPending(queue), envelope(fmt.Sprintf("pending-%d", i), 0))
	}
	for i := range 3 {
		p.ZAdd(ctx, gqredis.KeyRetry(queue), redis.Z{Score: float64(now.Add(time.Duration(i+1) * time.Minute).Unix()),
			Member: envelope(fmt.Sprintf("retry-%d", i), i+1)})
	}
	for i := range 2 {
		b, _ := json.Marshal(map[string]any{"job": json.RawMessage(`{"to":"bounce@example.com"}`),
			"job_id": fmt.Sprintf("dead-%d", i), "job_name": "EmailJob", "queue": queue,
			"error": "smtp: 550 mailbox unavailable", "failed_at": now})
		p.RPush(ctx, "dlq:"+queue, string(b))
	}
	_, err := p.Exec(ctx)
	return err
}
