# Arachne ⇄ Sivana Handoff Contract

**Version:** 1 (M1) · **Status:** draft for review by the Sivana team
(**github.com/Noel-Alex/Sivana**)

This document defines what Sivana can rely on when consuming an Arachne audio
corpus: formats, quality bounds, metadata fields, licensing posture, and the
delivery mechanism. Any breaking change bumps the version and is listed at the
bottom.

## 1. What Sivana receives

Per harvest source, a directory containing:

| File | Contents |
|---|---|
| `manifest.jsonl.zst` | One JSON object per line, zstd-compressed — see §3 |
| `manifest.json` | Summary: counts by status/format/license, total bytes & hours |
| `attribution.txt` | Human-readable credits grouped by license — required by most CC deeds |

Audio files themselves live in a **content-addressed store**:
`<store>/<source>/<collection>/<sha256[0:2]>/<sha256>.<ext>`.
`object_path` in the manifest points at each file (`file://…` for local paths).

## 2. Audio guarantees

- **Codecs**: mp3, flac, ogg/oga, wav, m4a/opus/aac (magic-byte verified —
  extension lies are quarantined, not delivered).
- **Duration**: 30 s – 1800 s (config `media.min/max_duration_secs`). Shorter
  clips fail the gate; longer DJ mixes are excluded.
- **Bitrate**: ≥ 96 kbps where reported (lossless passes unconditionally).
- **Integrity**: every file's sha256 in the manifest matches its bytes;
  duplicates across sources collapse to one stored file.
- **Known gap**: files whose tags carry no duration still pass if within size
  limits — Sivana must tolerate small metadata gaps.

### Non-audio media kinds (v1.1)

The store also holds `VideoFile` (mp4/mkv/webm/avi/mov), `DocumentFile`
(pdf/epub/doc(x)/ppt(x)/txt), and opaque `BinaryFile`. All three are
magic-byte-verified against their requested kind (BinaryFile accepts any
recognizable-or-opaque blob) and stored content-addressed like audio — but
they carry **no probe metadata and no quality gates**: duration/bitrate/tags
checks remain audio-only, so video/document rows have null probe fields and
must not be assumed to meet any duration or bitrate bound.

### FMA bulk-archive convention

FMA subset downloads (`fma`, `fma-large`, …) ship as ONE task whose payload is
the whole subset zip, marked by a `source_id` ending in `-archive`
(e.g. `fma_large-archive`). The zip is accepted with magic bytes
`application/zip` (skipping audio probing) and committed to the store as-is;
post-download extraction happens downstream of this contract.

## 3. Manifest row schema (`TrackRecord`, per line of manifest.jsonl.zst)

```jsonc
{
  "source": "jamendo",            // adapter id: jamendo | archive-org | fma | discovered
  "source_id": "1848357",         // stable id within source; archive-org uses "<item>|<file>"
  "job_id": "…uuid…",             // Arachne job that harvested it
  "url": "https://…",             // origin URL (for license verification / re-fetch)
  "title": "…", "artist": "…", "album": "…",   // tag data preferred over source data
  "year": 2013, "genre": "…",
  "license": "cc-by-nc-sa",       // MANDATORY, SPDX-ish — see §4
  "license_url": "https://creativecommons.org/licenses/by-nc-sa/3.0/",
  // Human-facing catalog page (Jamendo shareurl, archive.org /details/<id>,
  // FMA dataset repo). Verify provenance / attribution here.
  "origin_page_url": "https://www.jamendo.com/track/1848357",
  // The page that linked/discovered this file (equals origin page for
  // adapters; the crawled page for organic discovery).
  "discovered_from_url": "https://www.jamendo.com/track/1848357",
  "collection": "…",              // album/collection within the source
  "duration_secs": 213.4,
  "bitrate_kbps": 192,            // null for lossless
  "format": "mp3",
  "sha256": "…64 hex…",
  "bytes": 3411234,
  "object_path": "file:///…/<sha256>.mp3",
  "status": "done",               // exports default to done-only
  "error": null                   // set for rejected/failed rows
}
```

## 4. Licensing posture — NEEDS SIVANA SIGN-OFF

Default export filter: **redistributable licenses only** — the exporter
enforces this itself, shipping only the cc-by / cc-by-sa / cc0 / public-domain
rows from the table below unless the operator explicitly passes the
`--all-licenses` escape hatch to include everything admitted to the manifest
(`--only-done` additionally excludes any track whose download failed).

| License class | Included by default | Notes |
|---|---|---|
| cc-by, cc-by-sa, cc0-1.0, pd-mark, pd-us | ✅ | safe to redistribute with attribution |
| cc-by-nc*, cc-by-nd*, cc-by-nc-nd | ⚠️ included in DB, excluded from *distribution* decision pending | NC blocks commercial use; ND may block fingerprint-derived derivatives |
| unknown | ❌ never admitted | no license ⇒ no task |

**Open questions for Sivana:**
1. Is the app commercial (ads/paid tier)? If yes, NC-licensed tracks must be
   excluded entirely, not just flagged.
2. Does fingerprinting + serving count as "derivative work" under ND terms?
   Conservative answer today: exclude ND.
3. Attribution surface: Sivana should display `attribution.txt` content or
   per-track credit in an "about this song" view — most CC deeds require it.

## 5. Spoken-word / non-music content

LibriVox-style speech is currently **out of scope** (no language/music
classifier yet). Expect some spoken-word leakage from generic sources; Sivana
should tolerate non-music fingerprints or we add a classifier gate before
their acceptance run.

## 6. Delivery mechanism

Single-machine phase: shared directory / external drive containing the store +
handoff bundle. Fleet phase (M2+): same manifest format, store sharded across
nodes with rsync-compatible layout — no schema change expected.

## 7. Acceptance test

The M1 exit demo: Sivana ingests a 50k-track Jamendo handoff end-to-end
(fingerprint all files, spot-check attribution against source licenses). This
contract is "met" when that run completes without manual patching on either
side.

## Changelog

- v1 (2026-08-23): initial draft.
- v1.1 (2026-08-25): documented non-audio media kinds (VideoFile / DocumentFile /
  BinaryFile stored content-addressed, probe metadata + quality gates still
  audio-only) and the FMA bulk-archive zip convention (`source_id` ending in
  `-archive` stores the whole subset zip).
- v1.1.1 (2026-08-26): clarified that non-audio kinds are magic-byte-verified
  but carry no probe metadata or quality gates (audio-only), and documented the
  exporter's enforced redistributable-only default with the `--all-licenses`
  escape hatch.
