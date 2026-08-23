# Changelog

All notable changes to Arachne are documented here. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows [SemVer](https://semver.org/) at the workspace level.

## [2.0.0] - 2026-08-23

### Changed
- **Merged `v2-refactor` into `main`** (previous prototype tagged `legacy-kafka`). The trunk is now the NATS JetStream + ScyllaDB workspace: `arachne-core`, `arachne-worker`, `arachne-coordinator`, `arachne-cli`, `arachne-tools`.
- Wire `worker.max_redirects` into the reqwest client (was a dead config flag).
- Remove dead `politeness.adaptive_throttling` flag (never read; returns in M2 as a real EWMA implementation).
- Config honesty defaults in `config/default.toml`: `max_concurrent_requests` 10000 → 512, `default_crawl_delay_ms` 10 → 250.

### Added
- `docs/ADR/000-charter.md` — project charter: user archetypes, differentiators, non-goals with revisit triggers.
- CI skeleton (GitHub Actions): fmt + clippy `-D warnings` + tests on ubuntu/windows, cargo-deny license/advisory gates.

## [legacy-kafka]

The original Kafka/Redpanda prototype (seeder/coordinator/worker over rdkafka). Preserved for history under the `legacy-kafka` tag; superseded by the v2 architecture.
