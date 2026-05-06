# Resporgs.com

A transparency layer for the toll-free number industry.

Every active North American toll-free number is controlled by a single
"Responsible Organization" (RespOrg) registered with Somos, the national
toll-free registry. Most of those RespOrgs have no public presence; a
handful hold millions of numbers; a few systematically harvest valuable
numbers out of other companies' disconnect pools. This site makes every one
of them visible.

## What's here

- **Profiles** for ~535 RespOrgs — 9-year inventory trajectory, Opportunism
  Index, vanity holdings, visual context (logo, website screenshot, street
  view, satellite)
- **Group pages** — aggregate inventory across shell-network members
  (e.g. Primetel operates under 18 different RespOrg codes)
- **Category pages** — 18 classifications from "Large Telcom" to
  "Misdial Marketing," with aggregate behavior
- **The Pool** (`/pool`) — industry-wide weekly toll-free inventory:
  current % in use, per-NPA fill, 9-year stacked-area of the spare pool,
  and a six-line fan chart of every concurrent exhaust-date forecast
  Somos has published since 2017
- **Number lookup** — paste any toll-free number for its 9-year ownership
  history
- **Lead capture** for number-watch, question-asking, and manual history
  requests

## Data sources

- **Monthly** Somos Number Status Report (81 snapshots, 2018-03 through
  2026-04) — drives all per-RespOrg analytics, parsed into columnar Parquet
- **Weekly** Somos Number Administration Summary (258 PDFs, 2017-10 through
  2026-04) — drives The Pool section. Refreshed by the local `/somos-weekly`
  skill when each Monday's email arrives.
- Sanity CMS export (Bill's 20-year curated directory: logos, addresses,
  human-assigned categories and groups)
- Master Million vanity dataset (for intersecting working inventory with
  known vanity words)
- Google Maps Street View + Static Satellite APIs (per-address imagery)

## Running locally

```
pip install -r requirements.txt
python webapp/app.py
# open http://127.0.0.1:5178
```

Requires: the Parquet `cache/`, `data/`, and `webapp/static/streetview/`
directories populated (not in git — regenerable via scripts).

## Refreshing data after a new monthly Somos snapshot

`cache/YYYY-MM.parquet` is built locally from the per-prefix CD-ROM files
(`scripts/cache_months.py`). The ~16 derived parquets in `data/` are then
rebuilt by chaining the builder scripts — see `scripts/rebuild.sh` for the
canonical order. Two equally-valid rebuild paths are documented in
`plan.md` section 11:
- **On the droplet** via `rebuild.sh` (needs ~4 GB free RAM; the script
  guards against OOM and refuses cleanly if there isn't enough).
- **Locally, then scp** the 16 derived parquets to the droplet — the
  no-disruption path even on a larger server.

## Layout

```
scripts/          # Data-pipeline Python (all idempotent)
  extract_somos_pdfs.py   # weekly NUM-YY-WW PDFs out of email zips
  parse_somos_pdfs.py     # weekly PDFs -> 3 parquets for /pool
  rebuild.sh              # monthly orchestrator (events, ranks, etc.)
webapp/           # Flask app + Jinja templates + CSS + images
  app.py                  # routes: /, /r/<rpfx>, /pool, /pool/<npa>, /pool/exhaust ...
  templates/
  static/
data/             # Derived Parquet event tables + lead sqlite
  resporg_month.parquet                # monthly per-rpfx
  somos_weekly_npa.parquet             # weekly per-NPA snapshot
  somos_weekly_pool.parquet            # weekly pool flow
  somos_exhaust_forecasts.parquet      # weekly 6-horizon forecasts
cache/            # Monthly raw Parquet snapshots (1 per month)
somos_pdfs/       # Raw weekly PDF cache (gitignored)
sanity-export/    # CMS dump (re-export as needed)
plan.md           # Living design doc
```

## Operated by

Bill Quimby — 1-800-MARKETER LLC. 20+ years in the toll-free number industry.
