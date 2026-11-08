package manager

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/danish-a1/goqueue/internal/logger"
)

// An ACL username given to the manager must reach the Redis client, or the
// connection authenticates as the default user and fails on ACL servers.
func TestGetClient_SendsACLUsername(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()
	mr.RequireUserAuth("queue-user", "s3cret")

	mgr := NewRedisClientManager(mr.Addr(), "queue-user", "s3cret", 0, logger.NewZapLogger())
	client := mgr.GetClient(mr.Addr(), "s3cret", 0)
	defer func() { _ = mgr.CloseAll() }()

	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping with ACL user failed: %v", err)
	}
}
