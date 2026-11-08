# Changelog

All notable changes to GoQueue will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-11-08

### Added

- `dashboard` package: a password-protected web UI served as a plain `http.Handler` (Redis driver only, stdlib only, no new dependencies). Shows totals, per-day history, per-queue counts, job IDs, busy workers, retries, dead and deleted jobs, with Retry / Kill / Delete / Restore (single and batch, atomic Lua moves, nothing is hard-deleted). Mount it with `dashboard.New(dashboard.Options{...})`. An empty access token disables it (404 on every path); tokens must be at least 32 bytes. See `docs/dashboard.md`.
- Tracking keys (Redis): `goqueue:queues` registry, `goqueue:stat:*` processed/failed counters (global, daily, per queue), `goqueue:process:*` worker heartbeats and busy-job hashes, `goqueue:deleted:<queue>` deleted-job lists. Key names live in `adapter/redis/keys.go`. `NewQueue` registers its queue with `HSETNX` (a producer never overwrites an existing dead key); `Worker.Start` registers the consumer's real dead key with `HSET`, so a producer on the default DLQ cannot hide a consumer's custom DLQ list.
- Dashboard actions return `{"Done","NotFound","Skipped"}`. `Skipped` holds entries still at the source that cannot be moved (missing job name or unreadable data); the UI keeps their rows.
- `examples/dashboard`: runnable demo on miniredis.

### Changed

- DLQ entries (Redis) now share one shape (`dlq.Entry`): field `queue_name` is renamed `queue`, `job_name` is added, and the error is capped at 256 bytes. `dlq.DecodeEntry` still reads legacy `queue_name`.
- `RedisDLQ.Push` now uses `RPUSH` (newest entry at the tail instead of the head), keeps at most 1000 entries per queue and expires the list after 30 days.
- Retry envelopes carry `error_message` and `failed_at` (last failed attempt); `Pop` exposes it as `JobContext.LastError`. Old envelopes without these fields still decode.

### Breaking

- `RedisDLQ` ordering: the newest dead job is now at the tail of the list, not the head. Anything that reads the list by index (`LINDEX 0`, `LRANGE 0 n`) now sees the oldest entries first.
- `RedisDLQ` field rename: `queue_name` is now `queue`. Readers must use `dlq.DecodeEntry` or accept both names. Entries already stored with `queue_name` stay readable through `DecodeEntry`.

### Upgrade notes

Legacy `RedisDLQ` lists (0.2.9 and older) were written with `LPUSH` (newest entry at the head) and never capped. 0.3.0 pushes with `RPUSH` and then `LTRIM -1000 -1`, which keeps the tail. On the first dead job after the upgrade:

- a legacy list longer than 1000 entries loses its newest entries (they sit at the head), and
- the remaining legacy entries show in reverse order in the dashboard (oldest as newest).

Before upgrading, with the old workers stopped:

1. Find long lists: `redis-cli --scan --pattern 'dlq:*' | while read k; do echo "$(redis-cli LLEN "$k") $k"; done | sort -rn`
2. Archive or drain every `dlq:*` list longer than 1000 entries, for example `redis-cli COPY dlq:<queue> dlq-archive:<queue>` (Redis 6.2+) and process or drop the archive yourself.
3. Reverse each legacy list in place once, so the newest entry is at the tail. Run it exactly once per list (a second run reverses it back):

```bash
redis-cli EVAL "local v = redis.call('LRANGE', KEYS[1], 0, -1); redis.call('DEL', KEYS[1]); for i = #v, 1, -1 do redis.call('RPUSH', KEYS[1], v[i]) end; return #v" 1 dlq:<queue>
```

Then start the 0.3.0 workers. Custom DLQ adapters that already `RPUSH` with a cap (for example fleetlery's) are unaffected.

### Fixed

- `<queue>:metrics:ack` no longer grows without bound: `DequeueMetrics` uses `BRPOP` instead of `BRPOPLPUSH` and never wrote an ack for metrics. `MetricsAckQueueSuffix` is deprecated. Clean up existing lists with: `redis-cli --scan --pattern '*:metrics:ack' | xargs redis-cli del`.

## [0.2.9] - 2026-10-23

### Fixed

- Idle workers now stop promptly on shutdown. The Redis `Pop` blocked forever on an empty queue (`BRPOPLPUSH` with timeout 0), so `Shutdown` always waited the full timeout and returned `context.DeadlineExceeded`. `Pop` now blocks for at most 1 second and returns an empty job, letting the worker loop re-check for shutdown.
- The metrics goroutine now also exits on `Shutdown`, not only when the `Start` context is canceled, so `Shutdown` no longer times out when the caller keeps that context alive.

## [0.0.1] - 2025-08-15

### Added

- Initial release of GoQueue
- Multiple backend support (Memory, Redis, AWS SQS)
- Job processing with automatic retries and exponential backoff
- Dead Letter Queue (DLQ) support
- Middleware pipeline for job customization
- Configurable worker pools with graceful shutdown
- Metrics collection and observability
- Comprehensive test coverage with unit, integration, and E2E tests
- Documentation and examples
- Code quality enforcement with golangci-lint

### Features

- **Memory Adapter**: In-memory job queue for development and testing
- **Redis Adapter**: Production-ready Redis backend with connection management
- **SQS Adapter**: AWS SQS integration for cloud-native applications
- **Job Middleware**: Extensible middleware system for cross-cutting concerns
- **Retry Logic**: Configurable retry attempts with exponential backoff
- **DLQ Support**: Failed job handling with pluggable DLQ adapters
- **Worker Management**: Concurrent worker pools with lifecycle management
- **Metrics**: Built-in metrics collection for monitoring and observability

[Unreleased]: https://github.com/danish-a1/goqueue/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/danish-a1/goqueue/compare/v0.2.9...v0.3.0
[0.2.9]: https://github.com/danish-a1/goqueue/compare/v0.2.8...v0.2.9
[0.0.1]: https://github.com/danish-a1/goqueue/releases/tag/v0.0.1
