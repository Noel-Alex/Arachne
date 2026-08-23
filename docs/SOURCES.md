# Source Adapters — Etiquette & Legal Notes

Arachne harvests audio from sources that **explicitly permit** it. Every task
carries a mandatory license identifier; unlicensed audio never enters the
manifest. Specs below were verified live against production APIs in August
2026.

## Sources

### Jamendo (`arachne harvest -s jamendo`)

- **Catalog**: ~500k Creative Commons tracks. Metadata enumeration ≈ 2,500 API
  requests total (page size 200) — fits the free non-commercial tier (35k
  requests/month).
- **Auth**: client_id from https://developer.jamendo.com — pass
  `--jamendo-client-id` or set `JAMENDO_CLIENT_ID`.
- **Behavior baked into the adapter**:
  - `order=id_asc` + `type=single albumtrack` for complete deterministic walks.
  - `audiodownload_allowed=false` tracks are skipped (no downloadable file).
  - Over-quota responses return HTTP-success with a warning field and empty
    results — treated as an error, never as end-of-catalog.
  - Only CC-license URLs parse; unknown licenses are never admitted.
- **Terms caution**: standard terms restrict caching/offline access and require
  per-track attribution (artist + Jamendo credit + link back via shareurl).
  For wholesale mirroring beyond personal/experimental use, email
  talk-to-us@jamendo.com first.

### Internet Archive (`arachne harvest -s archive-org`)

- **Catalog**: `netlabels` collection (~80k items, ~86% carry explicit CC
  license URLs). Filtered to redistributable licenses (cc-by, cc-by-sa, cc0,
  pd-mark, pd-us) by default.
- **Auth**: none required. BUT a descriptive User-Agent with a contact address
  is **mandatory** per their Bots policy — pass `--contact you@example.com`
  or set `ARACHNE_CONTACT`.
- **Etiquette (enforced by adapter defaults)**:
  - Cursor-based `/services/search/v1/scrape` enumeration (advancedsearch.php
    hard-fails past 10k results).
  - ~1s inter-request spacing, honoring their documented `-j4 --delay 1`
    envelope.
  - 429s honored; metadata responses cacheable via `item_last_updated`.
- **BEFORE ANY SUSTAINED BULK RUN**: email info@archive.org describing scope.
- **NEVER enable bulk harvest of `georgeblood` or `etree`**: those collections
  are research/private-study only (IA rights statements; UMG/Sony/Concord sued
  IA over Great-78 in 2023; US sound recordings 1923–1946 protected 100 years
  under the Music Modernization Act). The adapter refuses them under its
  redistributable-only default.

### FMA dataset (`arachne harvest -s fma[|-small|-medium]`)

- **Catalog**: the classic ISMIR-2017 corpus (106,574 tracks) as static zips on
  the EPFL mirror. Enumeration is offline via `fma_metadata.zip` →
  `tracks.csv` — zero API calls.
- **Subsets**: `fma_small` (8k×30s clips, 7.2 GB), `fma_medium` (25k, 23 GB),
  `fma_large` (106,574×30s, ~100 GB), `fma_full` (full-length originals,
  ~943 GB — quota-aware staging strongly advised).
- **Licensing**: per-track, from tracks.csv's license column (human-readable
  CC titles). NC/ND variants are excluded by the adapter's
  redistributable-only default; `FMA-Limited: Download Only` rows never admit.
- **Do NOT scrape freemusicarchive.org**: its API is dead and the owners ask
  integrators not to stress their servers. The static mirror is the sanctioned
  path.
- **Integrity**: SHA1s are pinned in the mdeff/fma README (the old checksums
  file is gone). Cite Defferrard et al., ISMIR 2017 in downstream artifacts.

## Adding a source

Source adapters are modules in `arachne-tools/src/adapters/` (charter rule:
modules, not crates). Contract:

1. Enumerate the catalog (API walk or offline metadata).
2. For each item build a pending `TrackRecord` + `CrawlTask` pair via
   `adapters::build_task_and_record` — license MANDATORY, empty/unknown ⇒ skip.
3. `admit()` inserts the manifest row IF NOT EXISTS; only newly-admitted tasks
   get published, so re-runs resume instead of re-downloading.
4. Respect the source's documented politeness envelope (delays, UA, caps).

Wire it into `arachne-cli/src/commands/harvest.rs`.
