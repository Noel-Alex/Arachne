# Arachne v2: The Seeker

A production-grade, high-performance distributed web crawling toolkit built in Rust and powered by NATS JetStream and ScyllaDB.

---

## Overview

Arachne is an extensible, modular web crawler designed for high-throughput topic-driven ingestion, site-scoped crawling, rate-limited politeness compliance, and full pipeline observability.

Key design highlights in v2:
- **Pure Rust Stack**: Eliminates C dependencies (moved from Redpanda/Kafka `rdkafka` to NATS JetStream `async-nats`).
- **Decoupled Architecture**: Modular workspace with `arachne-core`, `arachne-worker`, `arachne-coordinator`, and `arachne-cli`.
- **Ethical Politeness Engine**: `robots.txt` caching (`texting_robots`), adaptive per-domain rate limiting (`governor`).
- **Smart Deduplication**: In-memory Bloom filters combined with ScyllaDB batch checks to eliminate redundant work.
- **Rich CLI & Configuration**: Layered TOML + Environment + CLI options for job control, page limits, depth limits, and topic-focused crawling.

---

## Architecture

```
                       +-------------------+
                       |    arachne CLI    |
                       +---------+---------+
                                 |
                                 v
                       +-------------------+
                       | NATS JetStream    |
                       | (CRAWL_TASKS)     |
                       +---------+---------+
                                 |
           +---------------------+---------------------+
           |                     |                     |
           v                     v                     v
  +-----------------+   +-----------------+   +-----------------+
  | arachne-worker  |   | arachne-worker  |   | arachne-worker  |
  +--------+--------+   +--------+--------+   +--------+--------+
           |                     |                     |
           +---------------------+---------------------+
                                 |
                   (CRAWL_RESULTS / DISCOVERED_URLS)
                                 v
                       +-------------------+
                       |arachne-coordinator|
                       +---------+---------+
                                 |
                        +--------+--------+
                        |                 |
                        v                 v
                   +----------+     +------------+
                   | ScyllaDB |     | FS / S3    |
                   +----------+     +------------+
```

---

## Workspace Structure

- `arachne-core`: Shared types, domain logic, NATS client, Scylla repository, politeness engine, content extraction, metrics, and configuration.
- `arachne-worker`: Stateless worker nodes that fetch pages, respect politeness, extract links/metadata, save content, and publish results.
- `arachne-coordinator`: Coordinates crawl jobs, enforces page/domain limits, performs bloom filter + DB deduplication, and queues new tasks.
- `arachne-cli`: Command-line tool (`arachne`) for seeding, starting jobs, inspecting status, exporting data, and checking domain metadata.

---

## Quick Start

### 1. Infrastructure Setup
Start NATS JetStream, ScyllaDB, Prometheus, Loki, Promtail, and Grafana:

```bash
docker-compose up -d
```

### 2. Build binaries
```bash
cargo build --release
```

### 3. Start Coordinator and Workers
```bash
./target/release/arachne-coordinator &
./target/release/arachne-worker &
```

### 4. CLI Usage

#### Seed URLs directly or from a file
```bash
# Seed individual URLs
./target/release/arachne seed --urls https://news.ycombinator.com https://en.wikipedia.org

# Seed from a file or stdin
./target/release/arachne seed --file urls.txt
cat urls.txt | ./target/release/arachne seed --stdin
```

#### Start a Crawl Job with Limits & Filters
```bash
./target/release/arachne crawl \
  --seeds "https://news.ycombinator.com" \
  --max-pages 5000 \
  --max-pages-per-domain 500 \
  --max-depth 3 \
  --allowed-domains "news.ycombinator.com,ycombinator.com" \
  --topic "rust,systems,compiler" \
  --max-content-size 2MB \
  --name "hn-rust-crawl"
```

#### Check Job Status & Inspect Domains
```bash
# List all jobs
./target/release/arachne status

# Inspect specific job
./target/release/arachne status --job-id <UUID>

# Inspect domain metadata (robots.txt, crawl delays)
./target/release/arachne inspect ycombinator.com
```

---

## Metrics & Observability

- **Metrics endpoints**: Worker (`:9191/metrics`), Coordinator (`:9192/metrics`), NATS (`:8222/metrics`)
- **Grafana Dashboards**: Default dashboard provisioning set up at `http://localhost:3000` (User: `admin`, Password: `admin`)

---

## License

Licensed under the MIT License.