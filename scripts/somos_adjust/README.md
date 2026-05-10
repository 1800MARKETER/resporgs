# Somos Monthly Pipeline — The Central Nervous System

This module is the spine that connects every site in the network. Once a month, Somos delivers a snapshot of every toll-free number in the United States — what it spells, who holds it, what state it's in. That snapshot is the substrate every property in the business is built on. This module is what turns the raw delivery into the canonical data layer that all the consumer sites read.

## Why this matters

The whole business is built on knowing, with authority, the state of every toll-free number every month. Specifically:

- **What numbers exist** — all 56M+ across 800/833/844/855/866/877/888
- **Who currently holds each one** — the originating RespOrg
- **What status each one is in** — WORKING, DISCONN, RESERVED, AVAIL, TRANSIT, etc.
- **When that state last changed** — the date stamp Somos provides
- **Which numbers belong to our partner vendors** — derived after applying our control files

Every consumer site queries this data. None of them are useful without it. If this pipeline is broken, the whole network goes stale within a month.

## The architecture

```
                              Somos API (monthly)
                                      │
                                      ▼
                          download_monthly.py → ZIP/ALL.txt
                                      │
                                      ▼ (split_all.py if needed)
                          7 per-AC CD-ROM files
                                      │
                                      ▼
            ┌─────────────────────────┴─────────────────────────┐
            │   somos_adjust/build_adjusted.py (THIS MODULE)    │
            │     reads:  raw CD-ROM + control files            │
            │     applies: AcExc, RO2RO, RO2Stat, Individual    │
            │     fills:   AVAIL gaps for every missing number  │
            │     writes:  cache/adjusted/<YYYY-MM>.parquet     │
            └────────────────────────┬──────────────────────────┘
                                     │
        ┌──────────────┬─────────────┼──────────────┬─────────────────┬───────────────┐
        ▼              ▼             ▼              ▼                 ▼               ▼
   TFOINPUT.txt  MikeOINPUT.txt  vendor_inv     resporg deltas   disconnect       new TFN.com
   (TFN.com      (Mike's site    (ForSales.com  (Resporgs.com    feed (future     unified search
    upload)      upload)         marketplace)   month-over-month) site)            (VanityNumbers.com)
```

The single canonical asset is `cache/adjusted/<YYYY-MM>.parquet`. Every site is a thin transformer that reads that parquet and produces its own view.

Adding a new site to the network is **adding one feed-builder script** — never re-running parse + adjustments.

## The four control files

These four files in `C:\MonthlyProcessing2\` are how we layer business logic on top of Somos's raw data. The order they're applied matters; each step's output is the next step's input.

### 1. `RestrictedACExc.txt` — phone-pattern adjustments

Override status / date / 4th-field / resporg for any phone matching a 12-char pattern with `*` wildcards. First-match-wins. 29 active rules currently. Used for one-off corrections at the area-code-, exchange-, or specific-number level.

### 2. `RO2RO.txt` — RespOrg renames

Rename one resporg ID to another using a 5-char pattern with `*` wildcards. First-match-wins. 39 active rules currently.

This is the primary mechanism for **vendor attribution**. Real resporg IDs from carriers get remapped to our synthetic vendor codes:

```
RZA02 → 1RING    (Primary Wave's numbers attributed to RingCentral vendor)
PNN02 → 1DIDW    (PNN's numbers attributed to fonePBX/DIDWW vendor)
HTC02 → GJK01    (Hometown's numbers attributed to Bizwi)
ZXS** → HVN02    (any ZXS-prefix attributed to Call Haven)
...
```

Without this file, no vendor would have any inventory. This is what makes a "vendor" a vendor — Sanity's `enableForm: true` flag declares them, but RO2RO is what routes inventory to them.

### 3. `RO2Stat.txt` — status overrides per resporg+status

After a resporg has been renamed (post-RO2RO), if it matches a rule and the current status matches the rule's status pattern, override the status. 10 active rules. Used for vendor-specific status grooming (e.g. forcing 1RING numbers to show as REQUEST status).

### 4. `Individual number adjustment file-S.txt` — per-number resporg overrides

A list of 10-digit phones each with a target 5-char resporg. Currently 7,845 entries, all targeting `GJK01`. This is how vendors with no separate resporg (like GJK at NumberBarn) get individual numbers attributed to them.

**Bud's bug**: His `MoDatProc.exe` only successfully applies entries 1-2 of this file, dropping the other 7,843 silently. We fixed this by switching from his sequential-pointer scan to an O(1) dict lookup.

## What's a "vendor" — three sources of truth

Vendor identity has three places it has to be consistent. The system breaks down silently if these drift:

1. **Sanity CMS** (`RESPORGS\clean\resporg.json`) — `enableForm: true` is the canonical declaration. As Bill put it: *"the Sanity 'request form' option is the ultimate truth."* 18 vendors flagged today.
2. **RO2RO.txt** — the operational mechanism that actually routes inventory to those vendor codes. The "1*"-prefixed codes (1RING, 1DIDW, etc.) only exist because RO2RO creates them at run time.
3. **Individual number adjustment file** — for vendors whose inventory lives at a third-party resporg they don't directly control.

When a new vendor signs up:
- Add a Sanity `resporg` doc with `enableForm: true`
- Add their resporg(s) to RO2RO.txt (or Individual file if they don't have one)
- The next monthly run automatically surfaces them on every site

## Module layout

```
somos_adjust/
├── __init__.py
├── control_files.py            ✅ parsers for the 4 control files
├── adjustments.py              ✅ 4 pure-function adjustment passes
├── build_adjusted.py           ✅ orchestrator: CD-ROM + ctrls → adjusted parquet
├── output_legacy.py            ✅ adjusted parquet → TFOINPUT.txt + MikeOINPUT.txt
├── output_vnfs.py              ✅ adjusted parquet → VanityNumbersForSales SQLite
├── build_tollfree_intel.py     ✅ adjusted parquet × master_vanity → tollfree_intel.db
│                                  (the SQLite VanityNumbers.com PhonePlatinumWire reads
│                                  for the per-NPA "in use with X" toll-free strip)
├── make_month.py               ✅ single-driver for all the steps (--skip-existing for idempotent re-runs)
├── validate_against_legacy.py  ✅ byte-diff our TFOINPUT/MikeOINPUT vs Bud's gold-standard outputs
└── tests/
```

### `control_files.py` — parsers
Each control file has a typed dataclass and a load function. `load_all()` returns a `ControlBundle` with all four. Comments (`<>`-prefixed) and `FILE END` markers handled. Per-file validation logs and skips bad rows rather than aborting (we want partial success on data errors).

### `adjustments.py` — passes
Four pure functions (`apply_ac_exc`, `apply_ro2ro`, `apply_ro2stat`, `apply_individual`) plus `apply_all` for the canonical sequence. Each takes a record dict + relevant control rules; mutates and returns the dict. No I/O, fully unit-testable.

The Individual pass uses a dict for O(1) lookup — sidestepping Bud's pointer-advance bug entirely. We don't try to reproduce Bud's bug for compatibility; we produce the **correct** result, which is a strict superset of Bud's output (we apply 7,845 individual overrides where Bud applies 2).

### `build_adjusted.py` — orchestrator (next to build)
Reads the 7 per-AC CD-ROM files, parses each record, runs `apply_all`, fills AVAIL gaps for every missing number in the 200-0000..999-9999 range per area code, writes adjusted parquet with this schema:

```
number      uint64    10-digit phone as integer (8002000000)
prefix      uint16    area code (800, 833, ...)
status      str       7-char (WORKING, DISCONN, RESERVED, ...)
resporg     str       5-char post-adjustment vendor/resporg code
rpfx        str       2-char resporg prefix (for Resporgs.com aggregations)
date        str       YY/MM/DD
fourth      str       2-char template/age code
```

### `output_legacy.py` — byte-compat outputs (next-next to build)
Read adjusted parquet, emit:
- `TFOINPUT.txt` — fixed 40-byte records, `phone-AAA-EEE-NNNN  ,STATUS  ,DATE   ,4T,RESPORG`
- `MikeOINPUT.txt` — variable-width CSV, `AAAEEENNNN,status,date,4t,resporg`

Goal: byte-identical to Bud's output, except for the 7,843 GJK rows the Individual fix correctly adds.

### `output_vnfs.py` — VanityNumbersForSales feed
Filter adjusted parquet to vendor codes (the 18 from Sanity), join the local-prospector `digit_index` for category tagging, emit the `inventory_<YYYY-MM>.db` SQLite file used by VanityNumbersForSales.com. Replaces the manual scripts I ran by hand for the May 2026 launch.

## Monthly runbook

Run from `C:\Users\Bill\claude code\RESPORGS\`. All commands are idempotent —
re-running a step overwrites its output.

### One-command workflow (preferred)

```bash
# 1. Pull this month's data from Somos
python scripts/download_monthly.py --month 2026-06

# 2. If Somos delivered as ALL.txt, split into per-AC files
python C:\MonthlyProcessing2\split_all.py    # edit INPUT_FILE/OUTPUT_DIR first

# 3. Run all three pipeline steps (build adjusted parquet → legacy TFOINPUT/MikeOINPUT → VNFS DB):
python -m scripts.somos_adjust.make_month \
    --month 2026-06 \
    --input-dir "C:/Users/Bill/Downloads/2026-06" \
    --deploy        # optional: push VNFS DB to droplet at the end

# 4. Refresh Resporgs.com raw-data cache (separate, existing)
python scripts/cache_months.py --month 2026-06
```

`make_month.py` skips steps you've already done. After re-receiving control
files mid-month, just re-run with `--only legacy` (rebuilds outputs from the
existing adjusted parquet without redoing the 60-minute build):

```bash
# Just rebuild legacy outputs from existing parquet
python -m scripts.somos_adjust.make_month --month 2026-06 --only legacy

# Just refresh VNFS feed from existing parquet
python -m scripts.somos_adjust.make_month --month 2026-06 --only vnfs --deploy
```

### Per-step commands (for debugging)

```bash
# Apply control files → canonical adjusted parquet (~60 min for full 56M)
python -m scripts.somos_adjust.build_adjusted \
    --month 2026-06 --input-dir "C:/Users/Bill/Downloads/2026-06"
# Optional flags for testing: --ac 800 --limit 100000

# Emit legacy outputs (~5-10 min)
python -m scripts.somos_adjust.output_legacy --month 2026-06

# Build VNFS inventory SQLite + deploy
python -m scripts.somos_adjust.output_vnfs --month 2026-06 --deploy
```

### Verifying byte-compat against Bud's known-good output

```bash
# Run the full pipeline for a month where Bud succeeded (e.g. 2025-12) and diff:
python -m scripts.somos_adjust.make_month \
    --month 2025-12 --input-dir "C:/Users/Bill/Downloads/2025-12"
python -m scripts.somos_adjust.validate_against_legacy \
    --month 2025-12 --bud-dir "C:/Users/Bill/Downloads/2025-12"
# Expected: ~7,843 rows differ; all are GJK01 fixes (the Individual-file bug).
# Add --mike to diff MikeOINPUT.txt instead.
```

**Adding a new consumer site to the network**: write a new `output_<site>.py`
that reads `cache/adjusted/<MM>.parquet`, applies that site's filters/joins,
emits whatever format the site needs. Add a runbook step. Done.

**Future: collapse to one driver** — eventually `make_month.py` runs all five
steps with a single `--month` arg and emails a summary. Eventually it runs on
the droplet via cron at 3 AM on the 5th of each month (Somos delivers
around the 1st-3rd).

## Diagnosing a broken run

| Symptom | Probable cause | Fix |
|---|---|---|
| `build_adjusted.py` errors "Missing CD-ROM files" | Somos sent ALL.txt only | Run `split_all.py` first |
| `output_legacy.py` diff vs Bud has > 8K differences | New status code or format change from Somos | Inspect the diffs; may need a control-file update |
| `output_vnfs.py` "no such table: digit_index" | Wrong path to local-prospector tollfree.db | Pass `--tollfree-db` explicitly |
| VNFS site shows 0 numbers for a vendor | Sanity vendor record exists but RO2RO doesn't route any inventory to that code | Update RO2RO.txt or remove the Sanity vendor |
| GJK-style vendor missing 1000s of numbers | Bud's pipeline still in use OR Individual file not read | Verify you're running the Python pipeline; check `Individual number adjustment file-S.txt` mtime |

## How each consumer uses the canonical adjusted parquet

### TollFreeNumbers.com (existing)
Today: consumes `TFOINPUT.txt` via the legacy upload flow. Future: reads `cache/adjusted/<YYYY-MM>.parquet` directly when the new TFN.com on VanityNumbers.com is wired in (Phase 6).

### VanityNumbersForSales.com
Reads the vendor-filtered, category-tagged subset (`output_vnfs.py`). Surfaces the 18 vendors' inventory in `WORKING/FEATURE/REQUEST/RESERVED` statuses with multi-vendor and multi-category filters.

### Resporgs.com
Reads the existing `cache/<YYYY-MM>.parquet` (raw, no adjustments) for resporg-level analytics: month-over-month changes, group memberships, NSP/Fusion analysis, the 81-month trend chart. Some Resporgs queries may eventually want the adjusted data instead — needs a per-query call.

### NewLocalNumbers.com (NLN)
Sister site to Resporgs but for local numbers, not toll-free. Different data source (NALENND, not Somos). Mentioned here only because it's part of the same network and follows the same architectural pattern: one canonical monthly snapshot, multiple consumer views.

### The Disconnect site (planned)
Will read `status = 'DISCONN'` rows from the adjusted parquet, plus a month-over-month diff with the previous month's parquet, to produce "newly disconnected" reports. Each disconnected number gets a long-tail page (per the network strategy in MEMORY).

### Mike's site (existing)
Consumes `MikeOINPUT.txt` via the legacy upload. Untouched by the migration as long as we keep that file byte-compat.

## The Individual-file bug — one paragraph for posterity

Bud's `MoDatProc.exe` (.NET 6 WinForms, March 2024) has a bug where its `WriteNums` function correctly applies entries 1-2 of the Individual adjustment file but silently drops the other 7,843 entries. The pointer logic in the decompiled C# *looks* correct on paper but fails in practice. We don't know the root cause and don't need to — switching to a dict-based O(1) lookup in Python sidesteps it entirely. Verified with a controlled test: input `8002222739` (entry 3 of the file) currently lands at `NDB99` in Bud's output and at `GJK01` in our smoke test, which is the correct behavior.

## References

- **Spec:** `C:\MonthlyProcessing2\MONTHLY_PROCESSING_DOCUMENTATION.md` (17KB) — the algorithm reverse-engineered from the C#
- **Decompiled source:** `C:\MonthlyProcessing2\MoDatProc.decompiled.cs` (53KB) — ground truth for edge cases
- **Splitter:** `C:\MonthlyProcessing2\split_all.py` — adapts new Somos `ALL.txt` format to the 7 per-AC files Bud's program expects
- **Plan:** `C:\Users\Bill\.claude\plans\first-here-s-the-documentation-refactored-donut.md` — module-by-module Phase 5 build plan
- **Resporgs upstream:**
  - `scripts/download_monthly.py` — Somos API client
  - `scripts/months.py` — canonical record reader, knows all the legacy file formats
  - `scripts/cache_months.py` — existing parquet writer (raw, no adjustments)
- **Vendor manifest (downstream):** `C:\Users\Bill\claude code\VanityNumbersForSales\data\vendor_manifest.json`

## Status

✅ All five modules built (`control_files`, `adjustments`, `build_adjusted`, `output_legacy`, `output_vnfs`)
✅ One-command driver `make_month.py` orchestrates the full pipeline with idempotent re-runs
✅ Byte-diff harness `validate_against_legacy.py` for verifying against Bud's gold-standard outputs
✅ 56M-row May 2026 adjusted parquet generated (cache/adjusted/2026-05.parquet, 274 MB)
🔲 Full byte-diff against Bud's December 2025 output (waiting on a Dec 2025 build run)
🔲 Cron deployment on the droplet (currently runs locally on demand)
