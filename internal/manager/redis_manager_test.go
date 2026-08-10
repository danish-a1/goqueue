package manager

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/danish-a1/goqueue/internal/logger"
)

// A failed health check must never close the shared *redis.Client: other
// goroutines (e.g. a worker blocked in BRPopLPush) may be using it
// concurrently, and callers that already hold the pointer (RedisStore, the
// DLQ adapter) never learn about a replacement stored in the manager's map.
// Closing it turns one transient ping timeout into a permanent
// "redis: client is closed" failure for the lifetime of the process.
func TestCheckAllClientsHealth_DoesNotCloseClientOnPingFailure(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	mgr := NewRedisClientManager(mr.Addr(), "", "", 0, logger.NewZapLogger())
	client := mgr.GetClient(mr.Addr(), "", 0)
	key := mgr.Key(mr.Addr(), 0)

	ctx := context.Background()

	mgr.checkAllClientsHealth(ctx)
	if !mgr.IsHealthy(key) {
		t.Fatalf("expected client to be healthy while miniredis is up")
	}

	// Simulate a transient outage: every command now errors, but the
	// connection itself stays intact. The health check must mark the
	// client unhealthy without closing or replacing it.
	mr.SetError("simulated outage")
	mgr.checkAllClientsHealth(ctx)
	if mgr.IsHealthy(key) {
		t.Fatalf("expected client to be marked unhealthy during simulated outage")
	}

	if got := mgr.GetClient(mr.Addr(), "", 0); got != client {
		t.Fatalf("GetClient returned a different client after a failed health check; consumers holding the original pointer would be left using a closed client forever")
	}

	mr.SetError("")

	if _, pingErr := client.Ping(ctx).Result(); pingErr != nil && strings.Contains(pingErr.Error(), "client is closed") {
		t.Fatalf("client was closed by the health check; commands now fail permanently even after the outage cleared: %v", pingErr)
	}
}

// Once Redis starts responding again, the health check should report the
// same client (self-healing via go-redis's own connection pool) as healthy
// again — it should never have been swapped out during the outage.
func TestCheckAllClientsHealth_RecoversHealthOnSameClient(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	mgr := NewRedisClientManager(mr.Addr(), "", "", 0, logger.NewZapLogger())
	client := mgr.GetClient(mr.Addr(), "", 0)
	key := mgr.Key(mr.Addr(), 0)

	ctx := context.Background()

	mr.SetError("simulated outage")
	mgr.checkAllClientsHealth(ctx)
	if mgr.IsHealthy(key) {
		t.Fatalf("expected client to be marked unhealthy during simulated outage")
	}

	mr.SetError("")
	mgr.checkAllClientsHealth(ctx)
	if !mgr.IsHealthy(key) {
		t.Fatalf("expected client to recover to healthy once redis responds again")
	}

	if got := mgr.GetClient(mr.Addr(), "", 0); got != client {
		t.Fatalf("expected the same client instance to be reused across the outage")
	}
}
