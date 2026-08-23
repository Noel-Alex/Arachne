# ADR-000: Arachne Charter — Positioning, Users, and Non-Goals

- Status: Accepted
- Date: 2026-08-23
- Deciders: Noel-Alex, ox-alpha (planning session)

## Context

Arachne began as a Kafka/ScyllaDB prototype ("The Seeker" for the Logos search-engine ecosystem) and was refactored into a NATS JetStream + ScyllaDB workspace (`v2-refactor`, merged to `main` 2026-08-23). The tool is intended to be a **universal, extremely high-throughput web crawler/scraper**. Without a written charter, scope grows without a filter; this document is that filter.

## Decision

### What Arachne is

A pure-Rust, embeddable, horizontally scalable crawler/scraper platform that can do anything anyone could conceivably want with web data collection — within the non-goals below.

### Who it serves (user archetypes)

1. **Rust embedders** — applications like [Sivana](https://github.com/Noel-Alex/Sivana) (Shazam-style audio fingerprinting) that need bulk acquisition pipelines and embed `arachne-core` as a library or run the binaries alongside their app.
2. **AI agents** — programs (human, MCP clients, REST callers) that need ad-hoc fetching, site crawls, and structured extraction as a service.
3. **Polite harvest operators** — people running large legal bulk-acquisition campaigns (corpora, ML datasets, archives) who must respect robots.txt and per-domain budgets fleet-wide.

### Differentiators (why choose Arachne)

1. **Pure-Rust single binary, embeddable as a library** — no C toolchain friction, no Python runtime; `arachne-core` builds clean on Windows/Linux/macOS.
2. **NATS JetStream scale-out** — lightweight brokers (30MB RAM vs 2GB for Kafka), WorkQueue semantics, leaf-node topology designed in (residential-swarm friendly).
3. **Agent-native interfaces** — REST control plane and MCP tools are first-class, not afterthoughts.
4. **Verified throughput** — benchmarked pipeline stages with published numbers rather than aspirational claims.

### Non-goals (do not build these without revisiting this ADR)

| Non-goal | Revisit trigger |
|---|---|
| WARC archival & replay | Someone needs replayable archives; response-header capture keeps retro-WARC possible |
| MinHash-LSH near-dup pipeline | Exact-hash + inline simhash insufficient for a real corpus consumer |
| Stealth/fingerprint HTTP client (wreq/BoringSSL), captcha solving | A needed source actually 403s polite requests; FetchProfile seam built first so this bolts on |
| Commercial proxy pool | Swarm model (volunteer devices on home IPs via NATS leaf nodes) fails to deliver IP diversity |
| Python/TypeScript SDKs | REST contract frozen AND a second real consumer exists |
| TUI/web dashboards | Grafana demonstrably insufficient for daily operation |
| Kubernetes manifests / HPA / multi-tenancy / leader-election HA | More than one operator, more than one beefy box |
| Rhai/Lua scripting escape hatch, XPath recipes | CSS+JSON-path+JSON-LD recipes demonstrably cannot express a real target |
| PDF/DOCX extraction, OCR | Real consumer blocked by absence; no production-grade pure-Rust OCR anyway |
| Realtime WS/SSE source ingestion, h3 transport, OTel tracing | P3 until a workload demands them |
| Audio fingerprinting / near-dup audio | Sivana's job by handoff contract — Arachne moves bytes and metadata only |

### Charter rule against bloat

New capabilities become **modules behind feature flags in existing crates**, not new crates. Crate count is capped at current membership (core, worker, coordinator, cli, tools) until an RFC amends this ADR.

### Canonical user agent

All HTTP traffic identifies with one UA built from config:

```
ArachneBot/2.0 (+{repo_url}; contact={contact})
```

`config.worker.user_agent` is the single source of truth; robots.txt manager and HTTP client must use the same string.

## Consequences

- Every future capability proposal maps to a user archetype + differentiator, or cites a revisit trigger from the table.
- M0 config honesty pass (wiring `max_redirects`, removing dead `adaptive_throttling`) already reflects "no dead flags" policy.
- The residential-swarm model is a design reservation, not a build commitment: shard-affinity politeness (M2) makes each volunteer device's home IP its own politeness budget when swarm rollout happens.
