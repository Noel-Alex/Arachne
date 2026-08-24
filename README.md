# Arachne

A high-throughput, versatile web crawler and **media harvest engine** in pure Rust. Crawl pages at scale, or mass-harvest licensed audio/video/documents into a content-addressed store with a complete, license-tracked manifest — built for ML dataset construction (its first customer: [Sivana](https://github.com/Noel-Alex/Sivana), an audio-fingerprinting app), data-science pipelines, and AI agents.

---

## What it does

- **Page crawling** — polite, robots-respecting, deduplicated crawling of arbitrary sites with per-job limits (depth, page counts, domains, content size, topic focus).
- **Media harvesting** — `arachne harvest` pulls entire licensed catalogs (Jamendo ~500k CC tracks, Internet Archive netlabels, the FMA research corpus) through a streaming downloader: resumable, magic-byte-verified, probed, quality-gated, quarantined-not-deleted on failure.
- **Organic media discovery** — pages crawled normally also yield direct audio/video/document links, which flow back through admission as download tasks (license-gated).
- **Provenance end-to-end** — every stored file traces back to its source: origin catalog page URL, license + license deed URL, and the exact page that linked it.
- **Handoff bundles** — `arachne tracks-export` emits `manifest.jsonl.zst` + summary + `attribution.txt`, ready for downstream ML pipelines.

## Architecture

```
                    +----------------+
                    |   arachne CLI  |  seed · crawl · harvest · tracks-export
                    +--------+-------+
                             |
                             v
                    +----------------+
                    | NATS JetStream |  CRAWL_TASKS / CRAWL_RESULTS / DISCOVERED_URLS
                    +--------+-------+
                             |
            +----------------+----------------+
            v                v                v
     +-------------+  +-------------+  +-------------+
     |   worker    |  |   worker    |  |   worker    |   fetch → verify → probe → store
     +------+------+  +------+------+  +------+------+
            |                |                |
            +----------------+----------------+
                             v
                    +--------------------+
                    |    coordinator     |   admission · dedup · manifest writes
                    +----+----------+----+
                         |          |
                         v          v
                  +---------+   +----------------------------+
                  | Postgres|   | content-addressed store    |
                  | (or     |   | <source>/<coll>/<sha[0:2]>/|
                  | Scylla) |   | <sha>.<ext>  (FS now, S3 later)
                  +---------+   +----------------------------+
```

- **Storage**: PostgreSQL by default (sqlx). Legacy ScyllaDB backend retained behind `database.backend = "scylla"` — see [docs](#docs).
- **Politeness**: robots.txt caching + per-domain rate limiting (`governor`) applied to *both* page and media fetches; per-host concurrency caps for downloads; archive.org's documented bulk envelope honored by default.
- **Durability**: manual ACK-on-success everywhere; crash-safe staging files with OS-level locks and Range resume; lease-based track claiming recovers crashed downloads.

## Workspace

| Crate | Role |
|---|---|
| `arachne-core` | Models, repository facade (Postgres/Scylla), NATS manager, politeness, robots, discovery (sitemaps/feeds/media links), audio probing, content-addressed media store, metrics |
| `arachne-worker` | Fetches tasks: HTML pipeline for pages, streaming binary pipeline for audio/video/documents |
| `arachne-coordinator` | Consumes results & discovered URLs; admission control, dedup, job policy, track-manifest completion |
| `arachne-cli` | The `arachne` binary (see below) |
| `arachne-tools` | Source adapters (jamendo, archive-org, fma) + stress tools |

## Quick start

### 1. Infrastructure

```bash
docker compose up -d postgres nats
```

(Add `prometheus loki grafana promtail` for monitoring; Scylla is available under the `legacy-scylla` profile.)

### 2. Build & run the pipeline

```bash
cargo build --release
./target/release/arachne-coordinator &
./target/release/arachne-worker &
```

### 3. Harvest something real

```bash
# Jamendo (~500k CC tracks; free client_id from developer.jamendo.com)
export JAMENDO_CLIENT_ID=your_id
./target/release/arachne harvest -s jamendo --limit 1000

# Internet Archive netlabels (CC-licensed; --contact is REQUIRED by their bots policy)
./target/release/arachne harvest -s archive-org --contact you@example.com --limit 200

# FMA research corpus (offline enumeration from fma_metadata.zip; no API key)
./target/release/arachne harvest -s fma-small        # 8k×30s clips (7 GB)
./target/release/arachne harvest -s fma              # full large subset (106k tracks, ~100 GB zip)
```

Downloads run through any started workers; watch progress via logs or Grafana.

### 4. Export the handoff bundle

```bash
./target/release/arachne tracks-export -s jamendo -o ./handoff
# → handoff/manifest.jsonl.zst  (one TrackRecord per line, zstd)
# → handoff/manifest.json       (counts by status/format/license, totals)
# → handoff/attribution.txt     (per-track credits grouped by license)
```

Each manifest row carries title/artist/album/year, duration/bitrate/format, sha256, byte size, store path, license id **and deed URL**, plus `origin_page_url` / `discovered_from_url` provenance.

### 5. Page crawling

```bash
./target/release/arachne crawl \
  --seeds "https://example.org" \
  --max-pages 5000 --max-depth 3 \
  --allowed-domains "example.org" \
  --default-license "cc-by" \
  --name demo-crawl
```

`--default-license` governs organically-discovered media: audio/video/PDF links found on crawled pages only become download tasks if a license can be attributed.

## Media kinds

| Kind | Verification | Extra processing |
|---|---|---|
| `AudioFile` | magic bytes (mp3/flac/ogg/wav/m4a/aac) | lofty probe: duration/bitrate/tags + quality gates (30s–30min, ≥96kbps defaults) |
| `VideoFile` | magic bytes (mp4/mkv/webm/avi/mov) | — |
| `DocumentFile` | magic bytes (pdf/epub/doc(x)/ppt(x)/txt) | — |
| `BinaryFile` | none (opaque asset) | — |

Rejected/failed files are **quarantined** under `media_store/quarantine/<reason>/`, never silently deleted.

## Observability

- Metrics: worker `:9191/metrics`, coordinator `:9192/metrics`
- `docker compose up -d prometheus grafana loki promtail` → Grafana at `http://localhost:3000` (admin/admin) with a pre-provisioned **Arachne — Crawl & Harvest Overview** dashboard: throughput, harvest/reject rates, bandwidth, discovery pressure, crawl-duration percentiles.

> Linux hosts: add `extra_hosts: ["host.docker.internal:host-gateway"]` to the prometheus service so it can scrape host-run binaries.

## Configuration

Layered: defaults ← `config/default.toml` ← `ARACHNE_*` env vars (`__` nests sections). Key sections: `[database]` (backend/url/pool), `[nats]`, `[worker]`, `[politeness]`, `[media]` (store dir, size caps, quality gates, per-host concurrency), `[storage]`.

## Docs

- [`docs/ADR/000-charter.md`](docs/ADR/000-charter.md) — project charter, non-goals, roadmap milestones
- [`docs/SOURCES.md`](docs/SOURCES.md) — per-source etiquette & legal notes (read before bulk runs; archive.org wants an email first)
- [`docs/CONTRACT.md`](docs/CONTRACT.md) — the Sivana handoff contract (schema, guarantees, licensing posture)
- [`docs/SCALING_GUIDE.md`](docs/SCALING_GUIDE.md) — throughput architecture notes (Scylla-era; storage section superseded by the Postgres migration)

## Status & roadmap

Done: M0 consolidation · M1 Sivana end-to-end (live-validated on Docker Postgres+NATS).
Next: M2 fleet-safe spine (domain-sharded subjects, DLQ, stable durable consumers), M3 extraction/catalog quality, M4 REST+MCP API (the programmatic/web interface), M5 recipes & HF datasets.

## License

MIT
