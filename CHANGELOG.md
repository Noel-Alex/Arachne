# Changelog

All notable changes to Arachne are documented here. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows [SemVer](https://semver.org/) at the workspace level.

## [2.1.0] - 2026-08-24

### Added
- **Multi-format media harvesting**: `TaskKind` now covers `VideoFile` (mp4/mkv/webm/avi/mov), `DocumentFile` (pdf/epub/doc(x)/ppt(x)/txt), and `BinaryFile` alongside `AudioFile`. All flow through the streaming downloader with per-kind magic-byte verification; audio keeps its lofty probe + quality gates.
- **Provenance chain end-to-end**: every manifest row carries `license_url`, `origin_page_url` (Jamendo shareurl, archive.org `/details/<id>`, FMA dataset repo) and `discovered_from_url` (the page that linked the file). Organic discovery preserves the discovering page's URL through admission into the permanent manifest.
- **PostgreSQL backend (new default)** via sqlx behind an `ArachneRepo` facade; legacy ScyllaDB kept behind `database.backend = "scylla"` (docker profile `legacy-scylla`). Lease claiming is now atomic (`FOR UPDATE SKIP LOCKED`); upserts are single-statement `ON CONFLICT`.
- Grafana dashboard provisioned with the stack ("Arachne — Crawl & Harvest Overview": throughput, harvest/reject rates, bandwidth, discovery pressure, duration percentiles).
- Audio observability: `arachne_audio_{harvested,rejected,failed}_total` counters + always-on terminal log line per media task; `arachne_messages_malformed_total` for poison pills.

### Fixed
- **P0**: coordinator result processor never persisted batches nor ACKed messages (flush block accidentally deleted during M1 wiring) — crawled_pages was dead and results redelivered forever.
- JetStream consumer config: `ack_wait` 30s → 600s (long downloads were redelivered mid-flight), `max_deliver` 3 → 100 (transient failures no longer terminate delivery silently).
- `.part` staging collisions under redelivery: exclusive OS file lock on the primary staging path, private fallback files for contended attempts, write-through the locked handle (Windows reopen self-collision), resume offset from lock state.
- Media commit no longer buffers whole files in RAM — streams via object_store BufWriter (256KB chunks).
- Media fetches now honor robots.txt + crawl-delay like pages; `set_domain_delay` no longer resets the limiter budget on every call.
- Job cache TTL (60s) + terminal-status guard: paused/cancelled jobs stop admitting within a minute instead of never.
- WAV/OGG magic-byte allowlist accepted only `audio/wav|audio/ogg` but infer reports `audio/x-wav|application/ogg` — genuine files were quarantined.
- lofty probed by file extension (staging files are `.part`) — switched to content sniffing; probe moved to `spawn_blocking`.
- Dedup-skip resolved real store paths instead of the `already-present:` placeholder that leaked into manifests.
- Scylla dialect errors in track queries (no `OR`, no inline comments); claim split into pending + expired-lease statements with `ALLOW FILTERING`.
- `active_tasks` gauge leak on the audio path.
- Disk-quota check now enforces the free-space floor (`media.min_free_bytes`) in addition to the total-bytes cap.
- Repo hygiene: removed ~120MB of unrelated legacy datasets and the stale duplicate monitoring tree from git.

### Changed
- README rewritten for the current architecture; prometheus.yml updated (Scylla scrape removed, host-gateway note added).

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
