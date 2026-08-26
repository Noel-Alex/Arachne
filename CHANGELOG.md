# Changelog

All notable changes to Arachne are documented here. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows [SemVer](https://semver.org/) at the workspace level.

## [Unreleased]

### Added
- **Sitemap & RSS/Atom feed discovery wired end-to-end**: crawled pages classify their own links into sitemap/feed candidates (`discovery::wire`), fetch them robots-respected and hard-capped (3+3 probes per page, 10s timeout, 5MB bodies, one index recursion level), and fold child page URLs plus audio enclosures back into discovery so they flow through coordinator admission like any other URL.
- **Job policy enforcement at admission**: `follow_external_links = false` now gates candidates by seed-root lineage (ASCII-case-insensitive root-domain match against the job's parsed seeds); `job.max_pages_per_domain` enforced on top of the global per-domain cap.
- **Live coordinator gauges**: `arachne_frontier_size` (CRAWL_TASKS stream depth) and `arachne_jobs_running`, refreshed every 15s; worker page-size histogram `arachne_page_size_bytes`.
- **Browser-style charset cascade** for page decoding: Content-Type header charset → BOM sniff → `<meta charset>`/http-equiv scan of the first 2048 bytes → UTF-8 with replacement.
- Stable content ids for organically-discovered media: SHA-256 of the URL (first 16 hex chars) instead of the unspecified `DefaultHasher` algorithm.

### Fixed
- **Video files keep their sniffed container extension** (mp4/m4v/mov): ISO-BMFF ftyp boxes no longer mislabel real video as `.m4a`; audio tasks on the shared container still get `.m4a`.
- **m4a and Ogg-Opus magic bytes actually recognized**: verification tokens updated to infer 0.16's real mime strings (`audio/m4a`, `audio/opus`, `audio/x-wav`, `audio/x-flac`) — genuine files that were quarantined now pass.
- **FMA bulk-zip tasks store as zip**: subset-archive tasks (`source_id` ending `-archive`) accept `application/zip` payloads and commit to the store instead of quarantining a multi-GB download after transfer; probing is skipped for them.
- **Disk quota no longer double-counts dedup hits**: only newly-committed bytes increment the total-bytes counter; dedup-skip resolves existing store paths.
- **Postgres admission hot path is single-round-trip**: batch URL checks and result inserts use unnest-array statements joined against the table (pinned by tests), instead of per-row queries.
- **CLI robustness**: UTF-8-safe truncation in status output (`char_indices` boundaries), RFC-compliant CSV escaping in export, nonzero exit with the valid list on an unknown `harvest -s` source.

### Security
- **Redirect-following egress bypass closed**: the worker's reqwest client now uses `guarded_redirect_policy`, re-validating EVERY 30x hop through the static SSRF guard (`arachne-core/src/egress.rs`) — previously `Policy::limited(n)` followed redirects to any host while only the original URL was checked.
- **SSRF guard widened**: IPv4-mapped IPv6 addresses (::ffff:0:0/96) re-checked against IPv4 rules, unique-local fc00::/7, link-local fe80::/10, and "this network" 0.0.0.0/8 blocked alongside loopback/private/link-local/cloud-metadata ranges and non-standard ports.
- SECURITY.md supported-versions row updated (2.1.x).

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
