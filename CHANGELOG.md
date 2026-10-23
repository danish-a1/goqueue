# Changelog

All notable changes to GoQueue will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- TBD

### Changed

- TBD

### Fixed

- TBD

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

[Unreleased]: https://github.com/danish-a1/goqueue/compare/v0.2.9...HEAD
[0.2.9]: https://github.com/danish-a1/goqueue/compare/v0.2.8...v0.2.9
[0.0.1]: https://github.com/danish-a1/goqueue/releases/tag/v0.0.1
