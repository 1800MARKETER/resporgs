# Resporgs.com

A transparency layer for the toll-free number industry.

Every active North American toll-free number is controlled by a single
"Responsible Organization" (RespOrg) registered with Somos, the national
toll-free registry. Most of those RespOrgs have no public presence; a
handful hold millions of numbers; a few systematically harvest valuable
numbers out of other companies' disconnect pools. This site makes every one
of them visible.

## What's here

- **Profiles** for ~500 RespOrgs — inventory, 4-year trajectory, Opportunism
  Index, vanity holdings, visual context (logo, website screenshot, street
  view, satellite)
- **Group pages** — aggregate inventory across shell-network members
  (e.g. Primetel operates under 18 different RespOrg codes)
- **Category pages** — 18 classifications from "Large Telcom" to
  "Misdial Marketing," with aggregate behavior
- **Number lookup** — paste any toll-free number for its 4-year ownership
  history
- **Lead capture** for number-watch, question-asking, and manual history
  requests

## Data sources

- Monthly Somos Number Status Report (42 consecutive snapshots, 2022-06
  through 2026-04) — parsed into columnar Parquet for fast analytics
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

## Layout

```
scripts/          # Data-pipeline Python (all idempotent)
webapp/           # Flask app + Jinja templates + CSS + images
  app.py
  templates/
  static/
data/             # Derived Parquet event tables + lead sqlite
cache/            # Monthly Parquet snapshots (1 per month)
sanity-export/    # CMS dump (re-export as needed)
plan.md           # Living design doc
```

## Operated by

Bill Quimby — 1-800-MARKETER LLC. 20+ years in the toll-free number industry.
