# Resporgs.com — Rebuild Plan

Status: IN PROGRESS — Phase 2 pipeline live, Phase 3 head start confirmed, V1 build starts next
Last updated: 2026-04-21

---

## 1. Vision

Separate the resporg directory from TollFreeNumbers.com and re-launch it as a standalone informational + research site at **Resporgs.com**. Three simultaneous uses:

- **Research tool** — Bill's internal visibility into who's doing what across the industry
- **Public directory** — destination for the many resporgs with no web presence, where customers already land when searching their name
- **Lead-gen funnel** — qualified traffic flowing back to TFNC/LVNC/VanityNumbers.com

The differentiator: an **AI analytical layer** that produces industry-first insights no one else — possibly not even Somos — can produce, because no one else is differencing the monthly snapshots the way we can.

This isn't a lift-and-shift. It's a reimagining.

---

## 2. Why separate now

- Prerequisite for decommissioning TollFreeNumbers.com v1
- Clean domain boundary lets Resporgs.com rank independently for resporg-name searches
- Reframes a directory page into a reputation + research product
- Establishes the analytical pipeline once, then reuses it across TFNC/LVNC

---

## 3. Data foundation

### Primary: monthly Somos snapshots
Location: `C:\Users\Bill\Downloads\YYYY-MM\`

Per month:
- 7 per-prefix files: `CD-ROM_TFN_Report_{800,833,844,855,866,877,888}_YYYYMMDDHHMMSS.txt` (~300MB each)
- `ALL.txt` — compiled union (~2GB)
- `ALL-RESPORG.txt` / `ALL by RESPORG.txt` — sorted by resporg
- `ALL-by-STATUS.txt` / `ALL by STATUS.txt` — sorted by status
- `MikeOINPUT.txt` / `mikeOINPUT.txt` — prepared update feed (~56M records, all 7 prefixes)
- Disconnect reports (manual, vary in naming): `ALL_disconnect_report_*.xlsx`, `NEW_disconnect_report_*.xlsx`, `DISC *.txt`, `NEW-DISC*.txt`

Fields per record (comma-separated): number (with dashes), status, last-change date (YY/MM/DD), 2-digit template code, **5-char resporg code**.

## Resporg code structure (IMPORTANT)
- Codes are 5 characters.
- **First 2 characters** = the assigned resporg prefix (unique to the organization). Example: `QZ` is Bill's. `TW` is Twilio. `RB`, `IW`, `DL`, etc.
- **Last 3 characters** = sub-code, chosen freely by the resporg. Typical format is 2 digits (`01`, `69`, `99`) but can be letters or mixed.
- **Every resporg has a default `01`** (e.g., `QZA01`). Typical base.
- **Every resporg also has a `99`** — reserved for high-security use.
- Resporgs may use many sub-codes for internal segmentation. Sub-code count per resporg is itself a behavioral fingerprint (most use 1; some use 100+).
- **Analysis key**: group by first-2-char prefix to count "true resporgs," not distinct full codes.

## Status lexicon (Somos snapshots contain 6)
| Status | Meaning |
|---|---|
| WORKING | Active in service |
| TRANSIT | Mid-transfer between resporgs |
| DISCONN | Released, in aging pool |
| RESERVED | Held pending activation |
| UNAVAIL | Blocked |
| ASSIGNED | Rare edge state |

**7th status — SPARE — is NOT in the snapshots.** Spare = absence of any active status = unassigned, sitting in Somos pool. The `MikeOINPUT.txt` file is the full dataset WITH spares added — it's the source of truth for "first assignments" (who grabbed a fresh spare number vs. who took one from the disconnect pool).

## Historical depth
- **Active working set**: 16 months (2025-01 → 2026-04) on `D:\resporgs` — Parquet-cached in RESPORGS/cache/
- **In flight**: additional older months being downloaded from Dropbox (2022-06 → 2024-12) — ~30 more months when complete
- Dropbox source: [shared folder](https://www.dropbox.com/scl/fo/41sofu75un42g2lhxn12o/AJKXl0zs1IV8IER8jeEWGg4?rlkey=w6kuhuihmea0vg0mmj2jdc5co&st=nsrf4dit&dl=0)
- Older data (pre-2022) was deleted long ago (Bud's folders) — not recoverable. 4 years total will be ample.
- When new months arrive: `python scripts/cache_months.py` detects only new files, `python scripts/build_events.py` backfills events

### Secondary
- **Release emails** — references to wholesale resellers (Bandwidth et al.) embedded in customer notifications. Parsing yields a reseller → downstream-resporg map not available anywhere else.
- **TFN.com resporg content** — migrated from Sanity CMS (see below).

### Sanity CMS export (COMPLETE — 2026-04-21)
Full dataset exported via `sanity dataset export`. Project: `52jbeh8g` (1-800-MARKETER), dataset: `blog`. 3,606 docs + 909 assets. Location: `RESPORGS/sanity-export/` and split by type in `RESPORGS/by-type/`.

Inventory:
- **506 resporg** docs — rich schema: title, slug, alias, codeTwoDigit, address, summary + exactMatchMessage (portable text), logoImage, screenShotImage, topNumbers, totalNumbers (hand-cached, will be replaced by live Somos counts), troubleNumber, website, requestForm, enableForm, categories refs
- **2,418 testimonial** docs — `author`, `body`, `reviewDate`. Substantial corpus, suitable for browsing/search UI and AI theme mining.
- **37 resporgGroup** docs — **manually curated by Bill from contact info + human industry knowledge**. Not derived from number movement. This is ground truth for Phase 3.
- **19 resporgCategory**, 17 numberSubCategory, 14 category, 2 numberCategory, 2 industry, 1 subindustry
- 295 comments, 181 keywords, 93 posts, 13 pages, 3 resporgPages
- Drafts and published versions both present; dedupe required (prefer published, fall back to draft).

---

## 4. Core derived dataset: the monthly diff

This one pipeline powers everything downstream. For any two consecutive snapshots S(N) → S(N+1):

- **Transfer events**: (number, from_resporg, to_resporg, month)
- **Disconnect landings**: number had status DISC in S(N), active with new resporg in S(N+1) or S(N+2)
- **First assignments**: number not present in S(N), active in S(N+1)
- **Reactivations** (MUST be filtered out of harvest analysis): prev_resporg == new_resporg after disconnect

Output: a transfer/landing event table spanning the entire historical record.

---

## 5. Proprietary metrics

All derived from the monthly-diff dataset.

| Metric | Definition | What it reveals |
|---|---|---|
| **Opportunism Index** | % of acquired numbers sourced from recent disconnect pool where prior resporg ≠ self | Legit carriers ≈ 0. Pure harvesters ≈ 1. Published per resporg. |
| **Net flow** | Monthly gain − monthly loss | Who's growing, shrinking, dormant |
| **Velocity** | Inventory turnover rate | Active moving operations vs. static holders |
| **Fast-transfer pairs** | A→B within N days, repeated at scale | Shared-controller signal |
| **Strongly-connected components** of the transfer graph | Clusters of resporgs with bidirectional or cyclic transfer relationships | Probable shell-network families |
| **Group aggregate inventory** | Sum of numbers across resolved corporate group | **First-ever published count of numbers per controlling entity** |
| **800-specific acquisition** | Group's aggregate 800-number inventory and acquisition rate | Exposes the real phone sharks in the most valuable prefix |

---

## 6. Three-phase build

### Phase 1 — Directory & Customer-Facing Foundation
Uses only the existing snapshot data. No advanced analysis.

**Deliverables**
- Resporgs.com live with every resporg profile page (migrated content + current inventory)
- Per-profile: current number count, last-update date, basic trajectory chart
- Ask-a-question form prominently placed (Somos-mandated phone-number removal makes this the natural channel)
- Number-watch signup: "notify me when 1-800-X changes status" — primary lead-gen mechanism
- SEO architecture so each resporg ranks for its own name
- CMS decision + content migration from TFN.com

**Exit criteria**: site live, every resporg has a profile, form and watch submissions flowing to Bill's inbox / a CRM.

### Phase 2 — Insight Layer
Adds proprietary metrics on top of the directory. Where the differentiation starts.

**Deliverables**
- Monthly-diff pipeline built and backfilled across all historical snapshots
- Opportunism Index computed per resporg, displayed on profile
- Net-flow and velocity trajectory charts per profile
- "Disconnect harvesters this month" leaderboard
- Customer-evaluation view: "Considering Resporg X? Here's their pattern"
- Public leaderboards: fastest growing, largest shrinking, most dormant, most opportunistic

**Exit criteria**: every profile shows the insight layer; leaderboards published; metrics no competitor can replicate are live.

### Phase 3 — Group Resolution & Shell-Network Mapping
The investigative layer. Depends on Phase 2's flow graph being reliable.

**Critical head start**: the 37 human-curated `resporgGroup` docs already in Sanity are **ground truth**. They were built from contact information and human industry understanding, not from number movement. This means:
- We have a labeled seed set to **validate** the data-driven clustering against (if SCC clustering reproduces the known groups, we trust it to extend them)
- Human judgment + data-driven extension is stronger than either alone — we aren't replacing Bill's knowledge, we're amplifying it
- Phase 3 work becomes **gap-filling**: which resporgs should probably be attached to existing groups? Which new groups emerge that Bill hasn't yet identified?

**Deliverables**
- Fast-transfer pair detection
- SCC clustering of the transfer graph → candidate group families
- Behavioral fingerprinting: timing patterns, batch sizes, vanity preferences, geographic targeting
- Named-entity resolution across resporg names (LLC/DBA variants, similar contact info)
- **Validation step**: does the clustering reproduce the 37 known groups? Report on recall/precision against ground truth before trusting extensions.
- Reseller-relationship map from parsed release emails (Bandwidth and others)
- Group-level aggregate pages: "Primetel Group — actual numbers controlled across N known resporgs"
- Dedicated 800-acquisition view for groups → the phone-shark exposé
- UI in Sanity or custom admin to confirm/reject AI-suggested group memberships (human-in-the-loop)

**Exit criteria**: group pages published with cross-resporg aggregates; "who really controls X" answerable for suspected shell networks; industry first.

---

## 7. Open questions

1. **CMS identity** — what's powering TFN.com today? Bill to check browser-saved credentials.
2. **Historical depth** — how many months back should the flow graph extend for Phase 2? 12? 36? 60+?
3. **Number-watch scope** — free-only lead capture, or a paid tier with richer alerts in Phase 1?
4. **Domain redirects** — TFN.com/resporgs/* → Resporgs.com/* — set up when? Before v1 decommission?
5. **Release-email access** — single mailbox or scattered? How large is the archive?
6. **CMS for Resporgs.com itself** — mirror TFN.com's stack, or fresh? (Probably fresh if existing CMS is creaky.)
7. **Legal sanity check** — Bill believes derived insights are unencumbered. Worth one pass with an IP/contracts lens before publishing group-control data.

---

## 7f. Deploy-in-progress state (pick up here)

**Where we stopped:** Initial commit pushed to https://github.com/1800MARKETER/resporgs as `main`. About to SSH into droplet and build out the systemd/nginx config.

**Droplet layout confirmed:**
- Apps live at `/var/www/<appname>/` owned by `www-data`
- Existing apps: 1cup, PhonePlatinumWire, VanityCellular, tollfreenumbers-com, faith-review, bizbuilding
- Services: `<appname>.service` systemd units using **Gunicorn**
- Nginx sites in `/etc/nginx/sites-available/` (1cup, vanitynumbers, localvanitynumbers, tollfreenumbers, bizbuilding, acup, default)
- Bill's SSH password auth didn't work from Windows PowerShell; he uses DO **Web Console** to get in

**Next command to run on droplet** (never got the output back):
```
echo "=== vanitycellular.service ==="; systemctl cat vanitycellular.service; echo; echo "=== nginx config (probably vanitynumbers) ==="; cat /etc/nginx/sites-available/vanitynumbers; echo; echo "=== venv location ==="; ls /var/www/VanityCellular/ | head -15
```

Once we see that output, we can replicate the pattern for resporgs:
1. `git clone https://github.com/1800MARKETER/resporgs /var/www/resporgs` (then chown www-data)
2. Create Python venv, `pip install -r requirements.txt` + gunicorn
3. Write `/etc/systemd/system/resporgs.service` mirroring vanitycellular.service
4. Write `/etc/nginx/sites-available/resporgs.com` with server_name `resporgs.com www.resporgs.com`
5. Enable the site, reload nginx
6. Run `certbot --nginx -d resporgs.com -d www.resporgs.com`

**Still to rsync up to the droplet** (not in git, large):
- `cache/` (~9 GB of monthly Parquet)
- `sanity-export/` (~150 MB)
- `webapp/static/streetview/` (~500 MB of street/satellite images)
- `data/*.parquet` (derived event + flow tables, < 100 MB)
- `apikey.env` (Google Maps key — secret)

Plus optional: the sibling `local-prospector/data/master_vanity.db` path the Flask app expects for vanity lookups. Either rsync that up too or repoint the MM_DB constant in app.py.

## 7h. Overnight progress log (2026-04-24, while Bill sleeps)

### Performance optimization #1 + #2 — DONE
- **`scripts/build_ranks.py`** ✅ — precomputes 5 rank metrics per rpfx into `data/ranks.parquet` (490 rows). Startup load; profile render does one dict lookup instead of 6 window-function queries. Local speedup modest (DuckDB is fast on small parquets), droplet speedup meaningful.
- **`scripts/build_vanity_precompute.py`** ✅ — materializes the working×MM join once and slices into:
  - `data/vanity_categories.parquet` (52K rows, rpfx×category counts)
  - `data/vanity_top.parquet` (1.26M rows, top-60 per rpfx × category_code-or-NULL)
  - App reads from these instead of running the 2M-row sqlite JOIN per request
  - Droplet has **1.9 GB RAM with no swap** — discovered the hard way via OOM. Rewrote to iterate per-rpfx to bound peak memory.

### Live timing comparison (https://resporgs.com/r/MY)
| Stage | Median load time |
|---|---:|
| Pre-optimization | ~17s |
| After ranks precompute | ~10s |
| After vanity precompute | ~2.1s |
| After flow precompute | ~2.1s (flow queries were already fast) |
| **After rpfx snapshot precompute** | **0.9s (19× faster than original)** |

### Final live timing, all routes (warm cache)
| Route | Time |
|---|---:|
| `/r/MY`, `/r/JW`, `/r/EF`, `/r/AT`, `/r/TW` | 0.8–1.0s |
| `/group/primetel` | 1.4s |
| `/category/misdial-marketing` | 0.6s |
| `/directory` | 0.56s |
| `/faq` | 0.08s |
| `/number/8003569377` | 3.4s (still reads cache/*.parquet wildcard — next target) |

### Precompute pipeline in final form (`scripts/rebuild.sh`, 9 steps)
1. `build_events.py` — event pipeline
2. `build_flow_graph.py` — raw flow edges
3. `enrich_profiles.py` — MM match + age buckets
4. `build_disconnect_episodes.py` — abbreviated/standard split
5. `build_ranks.py` — 5 rank metrics per rpfx
6. `build_vanity_precompute.py` — per-rpfx vanity (slow step, ~16 min on droplet)
7. `build_flow_precompute.py` — inbound/outbound totals + top partners
8. `build_rpfx_snapshot.py` — NPA + status + sub-code breakdowns
9. `systemctl restart resporgs`

### Droplet memory reality
- Only 1.9 GB RAM total, no swap
- Shared with 5 other Flask apps already running
- Any heavyweight DuckDB query must be memory-bounded or it gets OOM-killed silently
- Future precompute scripts should default to per-rpfx / per-month iteration rather than big materialized joins

### Still queued after tonight
- Nginx gzip compression (easy, big win on bytes-over-wire)
- Gunicorn workers 2 → 4 (one-line systemd edit)
- Precompute flow summaries per rpfx (the last live-query hotspot)
- Connection pooling in Flask (currently `duckdb.connect()` per request — a persistent read-only con would help)

## 7i. Progress log (2026-04-24 evening — editor + enrichments + dormant)

Long session covering local editor, two enrichment scripts, a category cleanup, and a new "dormant" lifecycle concept. All local — nothing deployed yet.

### Local Sanity editor — `tools/editor/app.py` (port 5179)
Standalone Flask app for editing resporg metadata against Sanity directly via the Mutations API (no override files — Sanity is the source of truth).
- Reads `clean/resporg.json` + related; writes via `https://<project>.api.sanity.io/v2021-10-21/data/mutate/<dataset>`
- Token: `SANITY_API_KEY` (or `SANITY_API_TOKEN`) in `apikey.env`. Editor role, read+write scope, dataset `blog`
- Routes: `/` list with category/hidden/text filters · `/edit/<doc_id>` form · `/refresh`
- Fields editable in v1: categories (multi-select), hidden toggle, local-only research notes (stored in `data/editor_notes.json`)
- **Auto-strip `unknown`** when any real category is selected — meta slugs (`unknown` / `hidden` / `non-resporg` / `dead`) coexist with real industry tags; everything else replaces them
- Two templates: `tools/editor/templates/{base,index,edit}.html` — self-contained CSS, no shared style with main webapp
- Companion fetch script: `scripts/fetch_sanity_docs.py` pulls fresh resporg/category/group docs after edits without a full export+dedupe cycle (uses GROQ query API; ~2s)

### Category cleanup pass — `scripts/cleanup_unknown_dual.py`
Discovered "Unknown" was inflated by dual-tagging — 48 of 57 Unknown rpfxs were also tagged with another real category (mostly `secondary`, which Bill had forgotten was a legit CLEC-era bucket — Windstream, Paetec, McLeodUSA et al.).
- Script: dry-run by default, `--apply` to PATCH. Strips `unknown` from any rpfx that also has a real (non-meta) category.
- Result: **Unknown 57 → 9** without losing legitimate `secondary` classifications. The 9 remaining are truly unclassified.

### Profile enrichments — two one-shot scripts
Decided NOT to put these on `scripts/rebuild.sh` — addresses and domain ages don't change month-to-month. Manual re-run when Bill edits a batch.

**`scripts/enrich_mailbox.py` → `data/mailbox_flags.parquet`** (73 flagged, 32 high / 41 medium)
- Brand-name regex against `address.street1` for UPS Store, Mail Boxes Etc, PostNet, iPostal, Earth Class Mail, Regus, WeWork, Davinci, Alliance Virtual, etc.
- Generic markers (medium): "Virtual Office", "PMB #", "Coworking", "P.O. Box"
- **Shared-address detector** (high if 3+ rpfxs at same `street1+city+state`, medium if 2): caught the **AU/EZ/LT/NM/PJ/PO/RG cluster — 7 rpfxs at one address.** Bryan Lynott who told Bill "making new resporgs is my game" is presumably involved.

**`scripts/enrich_whois.py` → `data/domain_age.parquet`** (393 rows, 387 with creation date)
- Primary: `python-whois` (added to `requirements.txt`, only needed locally — droplet just reads the parquet)
- Fallback: Archive.org CDX API for domains WHOIS doesn't return
- Deduped at the domain level (one lookup per unique domain, fanned back to per-rpfx rows)
- Cache: `data/domain_whois_cache.json`. Default TTL 90 days; re-runs only re-check stale/missing
- 1s rate limit per WHOIS call → ~5 min for 302 unique domains
- **Notable findings:** voycetel.com registered Feb 2026 (BL — 2 months old, holding a code), dccld.com Sep 2025 (DI — 6 months old). att.com 1986 baseline.

**Profile wiring** (`webapp/app.py` build_profile + `webapp/templates/profile.html`)
- Two new DuckDB lookups; nullable; missing parquet = no badge (graceful degrade)
- Badges next to website link — "Domain since YYYY (Ny)" and "📬 Mailbox: <brand>" / "👥 Shared address (N resporgs)"
- New CSS: `.flag`, `.flag-domain`, `.flag-mailbox` in `webapp/static/style.css`
- `requirements.txt` now lists `python-whois` with a comment that it's only needed where the script runs

### Dormant lifecycle — runtime + Sanity-tagged
**Runtime detection** (any current inventory < 15) — every rpfx gets 14 default UNAVAIL test numbers from Somos, so anything under 15 means at most one real number.
- `build_profile` computes `is_dormant`, `last_active_month`
- Template replaces empty NPA bars + status pie with a **"NO RESULTS FOUND" gas-gauge banner** (image: `webapp/static/img/no-results-empty.jpg`, hand-drawn ink with "800" on the gauge face)
- Trajectory chart still renders — historical evidence stays visible

**Sanity category** (slug `dead`, title "Dormant") — Bill created in Studio with a 1-800-ZOMBIE image asset.
- New module-level set `DORMANT_RPFX` populated at startup from `clean/resporg.json` (parallel to `HIDDEN_RPFX` pattern)
- Sanity image reference → CDN URL helper: `https://cdn.sanity.io/images/<project>/<dataset>/<hash>.<ext>`
- When Sanity-tagged, banner uses the **zombie image** instead of the gas gauge, plus richer copy ("dead or nearly dead phone companies… called 'zombie' because they just won't die")
- Editor's `META_SLUGS` updated to include `dead` so the auto-strip rule preserves it

**Auto-tagger — `scripts/auto_tag_dormant.py`**
- Tags any rpfx that's been ≤14 numbers for 2 consecutive months (configurable `--months`)
- Idempotent — skips rpfxs already tagged
- Dry-run default, `--apply` to write
- Ran tonight: **106 tagged** at 2-month threshold (~22% of 486 real rpfxs)
- Mix of obvious zombies (Voyce, Cypress Telecom, Talkie, Cytel) plus surprises: Salesforce HI, Lumen FG, West Corp CV — all sitting at exactly 14, presumably retained shell codes for legal/regulatory reasons

### Number-page copy task added to roadmap
Bill flagged a customer-education problem: TFN.com search shows "active with this resporg" → customer hears disconnect tone → assumes available. Like a domain that doesn't load. Plan section under 7g now has draft copy: "asking out a married woman" metaphor, watch-this-number free with quarterly heartbeat, signed off by Bill offering alternatives. TFN.com number-result pages should deep-link to `resporgs.com/number/<tfn>` for the canonical explanation.

### Things explicitly tabled tonight (not lost — listed for future)
- **Somos portal access** — Bill has admin-contact data behind login at `https://portal.somos.com/Controls/REG/FindRespOrgContacts.aspx`. Per-number history available there too. Distraction from TFN.com integration; revisit later.
- **Admin / network research** — manual SOS lookups, LinkedIn checks, registered-agent matching. Right v1 is a Sanity field `affiliated_rpfxs` populated by Bill's research, surfaced as "Network: also operates X, Y" without exposing personal info. Deferred.
- **FCC 499-A enrichment** — Bill correctly noted he didn't file one himself, so probably not the right vector. Skip.

### Files created / modified tonight
**New:**
- `tools/editor/app.py` + `templates/{base,index,edit}.html`
- `scripts/fetch_sanity_docs.py`
- `scripts/cleanup_unknown_dual.py`
- `scripts/enrich_mailbox.py`
- `scripts/enrich_whois.py`
- `scripts/auto_tag_dormant.py`
- `webapp/static/img/no-results-empty.jpg`
- `data/mailbox_flags.parquet`, `data/domain_age.parquet`, `data/domain_whois_cache.json`

**Modified:**
- `webapp/app.py` — Sanity CDN constants, `_refresh_dormant_index()`, `DORMANT_RPFX`, `_sanity_image_url()`, mailbox/whois reads in `build_profile`, `is_dormant_*`/`dormant_image_url` context keys
- `webapp/templates/profile.html` — domain badge inside website `<li>`, mailbox flag `<li>`, dormant banner with `dormant-zombie` variant
- `webapp/static/style.css` — `.flag-*`, `.dormant-banner`, `.dormant-img`
- `requirements.txt` — `python-whois>=0.9` (note: not needed in prod)
- `apikey.env` — added `Sanity_API_Key` (case-insensitive loader)

### Status as of end of session
- Local-only. Nothing deployed.
- Dormant tags applied to Sanity (106 docs).
- `clean/*.json` re-fetched, `data/category_trajectories.parquet` rebuilt.
- Webapp at port 5178 needs restart to pick up new `DORMANT_RPFX` index.
- **Deploy sequence when ready:** `git push` code → `git pull` on droplet → `python scripts/fetch_sanity_docs.py` on droplet → `rsync data/{mailbox_flags,domain_age,category_trajectories}.parquet` to droplet → `systemctl restart resporgs`.

---

## 7j. Late-night progress log (2026-04-24/25 — deploy + Somos contacts + 37 new resporgs)

Continuation of 7i. Everything from this section IS deployed live.

### Deploy v1 (commit 7ac040e)
Pushed: editor + enrichments + dormant lifecycle + helper scripts.
- Deploy steps used (record for next time):
  1. `git push origin main`
  2. `ssh -i ~/.ssh/digitalocean_1cup root@104.131.76.98 "cd /var/www/resporgs && git pull"`
  3. `scp` clean/{resporg,resporgCategory,resporgGroup}.json to `/var/www/resporgs/clean/`
  4. `scp` data/{mailbox_flags,domain_age,category_trajectories,somos_contacts,admin_networks}.parquet to `/var/www/resporgs/data/`
  5. `ssh ... "systemctl restart resporgs"`
- Production verified: BL shows zombie banner, AU shows 7-rpfx shared address + 1996 domain, PC shows 1986 att.com domain.

### Asset URL regression + fix (commit 2b1fda7)
**Symptom:** logos disappeared from homepage top-20 after first deploy.
**Cause:** `fetch_sanity_docs.py` writes JSON in GROQ/API format with `{"asset": {"_ref": "image-<hash>-..."}}`, but the existing `asset_url()` only knew the `sanity dataset export` tarball format with `{"_sanityAsset": "image@file://..."}`. So every image returned `None`.
**Fix:** `asset_url()` now handles both shapes — tries the local file path first, falls back to constructing a Sanity CDN URL (`https://cdn.sanity.io/images/52jbeh8g/blog/<hash>.<ext>`). Also handles `file-` prefixed refs for non-image assets.
**Deploy:** `git push` + `git pull` + `systemctl restart resporgs`. No data rebuild needed.

### Somos portal scraping — admin contact extraction
**Goal:** pull primary contact (name + email + phone) for every active resporg.
**Mechanism:** Claude-in-Chrome extension drove Bill's already-logged-in Somos browser tab. URL trick: `Controls/REG/FindRespOrgContacts.aspx?Key=XXX` runs the search inline. The result page has an "Export to CSV" button.
**Optimization:** rather than 488 individual searches by 3-char code, exploited the substring-match nature of Somos search by exporting all sub-codes ending in `01`, `02`, `03`, `04`, `99`. **5 CSV downloads, ~2 minutes total**, 1361 raw rows, 541 unique admin codes covered (vs 488 in Sanity).
**Output files:**
- `data/somos_contacts/suffix_*.csv` (5 raw exports)
- `data/somos_contacts.parquet` — one row per 3-char admin code, parsed addresses + contacts
- `data/admin_networks.parquet` — derived: 91 multi-rpfx clusters where the same email or phone appears across 2+ admin codes

**Network detection findings (validates the technique):**
- **Lumen network: 9 rpfxs** (AL, FG, IN, IX, JS, KS, LG, LV, UL) — `christian.aguilar@lumen.com`
- **Windstream consolidation: 9 rpfxs** (AC, BJ, CD, DH, LO, PF, PK, SI, UU) — `katie.tan@windstream.com`. Reveals the PAETEC/McLeodUSA rollup chain.
- **Fusion Connect: 8 rpfxs** — `robin.krind@fusionconnect.com`
- **Primetel/Mayfair: 7 rpfxs** (AB, FO, HU, JD, OD, OQ, RY) — `agreco@foxteltelecom.com` — **EXACTLY matches Bill's existing `GROUP_OVERRIDES`**. Auto-discovered what was manually configured.
- **Bryan Lynott / ATL Communications:**
  - `cberquist@atlc.com` runs AU's 7 sub-codes
  - `scleland@atlc.com` runs LT, NM, PJ, PO, RG (5 rpfxs)
  - Two named operators inside the 7-rpfx shared-address cluster
- **Call Haven Partners: 5 rpfxs** (HT, HV, NJ, SS, ZX) — `daryl@callhavenpartners.com`
- **National Sales Partners: 5 rpfxs** (EK, KG, NA, RJ, YB) — `ee@nationalsalespartners.com`
- **AT&T: 3 rpfxs** (AM, PC, SH) — `ar2423@att.com`
- **Smaller suspicious clusters:** `ricky.keele@gmail.com` runs 3 rpfxs from a personal Gmail (EJ, TT, XQ).

`scripts/build_somos_contacts.py` ran the parsing. Address parser handles the concatenated "street + city + state + country + zip + phone" string. Contact parser handles "Name email P: phone F: fax" format.

### 37 new resporgs created (commit ??? — actually no commit, Sanity-side only)
**Discovery:** 37 active rpfxs in Somos that didn't exist in Sanity. Notable: CVS Pharmacy (HM), Union Pacific Railroad (UG), Telesign (SY), Ziply Fiber (YG), Pestilence Labs (EF — this was the trigger; Bill went looking for EF to classify it as misdial-marketing and found "unknown resporg").
**Created via `scripts/create_missing_resporgs.py`:** stub Sanity docs with title, codeTwoDigit, slug, address, and category=Unknown. **No category preset** — Bill will classify each one in the editor. (Bill's reason: "Just because a company has numbers in misdial programs doesn't mean that's their main business. Pestilence Labs's website looks legitimate, so they shouldn't be auto-tagged.")
**Street View + satellite:** ran `fetch_streetview.py` after the create — script auto-skips rpfxs with existing images. Pulled 36 of 37 (RH=Porting.com DBA ATLaaS in Vancouver BC failed because the address parser didn't map BC to a 2-letter state). 72 images deployed via scp.

### Open / pickup-tomorrow items

#### High priority
1. **Classify the 37 new resporgs** in the editor (http://localhost:5179, filter Category=Unknown). Some obvious quick wins:
   - HM = CVS Pharmacy → Corporate Client
   - UG = Union Pacific Railroad → Corporate Client
   - SY = Telesign → Telecom Service Provider (or new "Identity/Fraud" category)
   - YG = Ziply Fiber → Regional Phone Company
   - HM, UG types are obviously Corporate Clients
   - Pestilence Labs (EF) — looks legitimate per Bill's spot-check, leave Unknown until reviewed
2. **Rename "Secondary" → "Medium Phone Company"** in Sanity Studio. Leave slug as `secondary` (URLs/code reference it). Bill's reasoning: "Secondary" means CLEC-era resellers in his head; "Medium Phone Company" is more descriptive next to "Small Phone Company".
3. **Fix RH (Porting.com DBA ATLaaS)** — Vancouver BC, address parser dropped the state. Manually edit address in Sanity, re-run `fetch_streetview.py`.

#### Lower priority / future builds
4. **Wire admin networks to profiles.** `data/admin_networks.parquet` exists with 91 clusters. Add a "Network" badge on profile pages: "Also operated by this admin: AB, FO, HU, JD, OD, OQ, RY → Primetel" — auto-discovered group reveals. Could also retire the manual `GROUP_OVERRIDES` in `webapp/app.py` since the admin-email pattern produces the same result.
5. **Fill the remaining ~18 non-dormant gaps** — rpfxs that didn't show up in any of `01/02/03/04/99` batch searches. Could either do targeted single-key searches or grab `05/06/07` next time.
6. **Auto-tag dormant rerun** when a new monthly snapshot lands — script is idempotent, just re-run.
7. **Mailbox flag improvements:**
   - Same-group resporgs at the same address should be filtered out of the shared-address detector (e.g. Pacific Bell + AT&T sibling at the same HQ isn't suspicious, just corporate)
   - Could flag "Cleveland, OH" cluster — National Sales Partners has 4+ entities all in Cleveland
8. **Somos number-history page** at `Controls/NAC/Admin-Resources-Number.aspx` — accepts 10-digit TFNs and returns ownership history. Good v2 for the `/number/<tfn>` lookup page (richer history than just our 4-year window).

### Files touched this session
**New:**
- `scripts/create_missing_resporgs.py` — bulk stub creator
- `data/somos_contacts.parquet`, `data/admin_networks.parquet`, `data/somos_contacts/*.csv`
- 72 new street/satellite images in `webapp/static/streetview/`
- 37 new Sanity resporg docs (live)

**Modified:**
- `webapp/app.py` — `asset_url()` now handles GROQ/CDN format

**Out of scope but worth noting:**
- `apikey.env` lost its `Sanity_API_Key=` prefix at one point (editor save quirk). Bill restored it.
- `data/somos_codes_to_lookup.json` and `data/somos_pages/` are leftover scratch from earlier exploration; safe to delete.

---

## 7g. Post-launch roadmap (2026-04-24 — Bill's priorities after V1 went live)

### Product priorities (with Bill's notes)

#### Hidden category — "Invisible RespOrgs"
- Partner requested the ability to hide specific RespOrgs from public view
- Implementation: reserved category slug like `hidden` (or a boolean `hidden` flag on resporg doc)
- Filters needed:
  - Exclude from `/directory`, `/categories`, `/groups` member lists, homepage top-20, search results, flow-section partner tables
  - `/r/<rpfx>` should 404 for hidden resporgs (full invisibility, not just unlisted)
  - Names in partner tables should be scrubbed to just the code, or hidden entirely
- Bill sees value beyond partner request: useful long-term control primitive

#### Resporg Lock (rename or rebrand of Watch)
- LifeLock-for-toll-free-numbers concept. **Front-page hero image already has a lock on the "Resporgs" letters — this was always the intent.**
- Open question: **"Watch" vs "Lock"** — Watch is neutral/descriptive, Lock is branded/protective
  - Recommendation: brand the product "Resporg Lock" (marketing) but keep the verb "Watch this number" (UX clarity). Two names, one flow.
- Alerts when a specific number changes RespOrg or status
- Launch strategy: free for a limited time to establish the user base + generate leads, then tier it
- Technical pieces needed:
  - Weekly/daily cron comparing new Somos snapshot against prior, emitting events for every watched number
  - Email delivery pipeline (already using `bill@tollfreenumber.com` per plan)
  - Billing system if/when it becomes paid
- Existing `/watch` form is the skeleton — just needs the alerting pipeline behind it

#### Number lookup copy — explain why "active" ≠ "available" (2026-04-24)
- **Problem:** TollFreeNumbers.com already shows "active with this resporg" when customers search. They hear "disconnect message" and assume the number is available. It's like typing a domain into a browser, getting nothing, and thinking the domain is free.
- **Customers also often assume we are the resporg** (i.e. they think we're a phone company that can hand them the number). We are not.
- **Goal:** TFN.com number-result pages should deep-link to the matching `resporgs.com/number/<tfn>` page, which gives the honest explanation.
- **Copy to add to `/number/<tfn>` page** (draft — tune the voice):
  > Even when a number doesn't ring through to an active customer, that doesn't mean it's available. A disconnect message is just a message — the number is almost always still assigned to a RespOrg and, usually, still attached to a customer who stopped answering it or hasn't turned it back on yet. Trying to get it is a bit like asking out a married woman. It *can* work, but that's not usually how it ends.
  >
  > You can still contact the company currently holding the number if you want to try. For most unavailable numbers, though, we don't help with that — it's not a good use of anyone's time.
  >
  > **What we can do:** watch the number for free and tell you if the status ever changes. If it does, you'll have a shot. No spam, just a quarterly heartbeat if nothing's happening.
  >
  > **And honestly — if you need help finding a great alternative, that's the real play. Let me know. I've helped companies find great vanity numbers since before AT&T had a website.** — Bill
- **CTAs on the page:**
  - Primary: "Watch this number free" (already exists)
  - Secondary: "Help me find an alternative" → mailto or form to Bill
  - De-emphasize: "Contact the current holder" (link to profile)
- **Cross-site integration:**
  - TFN.com number results page: after the "active with X" line, add "See full 4-year history and why this probably isn't available → resporgs.com/number/<tfn>"
  - Both sites point at the same canonical explanation on Resporgs.com — single source of truth for the "active ≠ available" education
- **Template:** `webapp/templates/number_lookup.html` — add a new section above/below the timeline, conditional on `result.current` being non-null (i.e. the number IS currently held). For SPARE/unassigned numbers, different copy is appropriate.

#### Number Rescue Service
- Lead-capture form for customers who lost a toll-free number
- Multi-step questionnaire: when, what, how lost, who has it now (if known)
- Honest filter: most situations can't be rescued. Tell them clearly if theirs can't.
- For the recoverable cases, offer a paid rescue service
- Fields to calculate feasibility: current holder's Opportunism Index (sharks don't release), time since disconnect, any active customer attachment
- Form on profile page when current holder is identified? Or standalone page `/rescue`?

#### Blog
- 92 post docs already in Sanity, mostly "Request a quote..." templates (from TFN.com) — NOT real blog posts
- Need a separate post category or flag for "Industry Blog"
- Pages to build: `/blog` (index) + `/blog/<slug>` (individual)
- Bill will write posts as industry issues come up

### Performance priorities

Bill's observation: "for a mostly text website it's not as fast as it should be."

#### Current state — everything computed on-the-fly per request
Every profile page request runs 6+ DuckDB rank queries across `resporg_month.parquet`, plus flow-graph aggregations, plus vanity lookup against the 2M-row MM sqlite DB. Scales poorly with concurrent users.

#### Quick wins (order of impact)
1. **Precompute ranks** — one-time script builds `data/ranks.parquet` with (rpfx, inv_rank, inv_total, opp_rank, ...). Profile request does one row lookup instead of 6 rank scans. **Probably 60% of the profile speed issue.**
2. **Precompute per-rpfx flow summaries** — top 10 sources/destinations/harvest-origins rolled up monthly into `data/flow_summary_<rpfx>.parquet` or one combined file. Profile request becomes one query.
3. **Nginx gzip** — text responses compress 5-10× smaller over the wire. Just a `gzip on` + some MIME types in the nginx config.
4. **More Gunicorn workers** — currently 2. Bump to 4 for parallelism.
5. **Trim directory content from homepage** — Bill flagged "we don't really need all the directory listings and stuff below the top 20. There are sections for categories and directory." The homepage already doesn't include the full directory; the big-bytes pages are `/directory` itself. Still room to paginate it.

#### Larger performance projects
- **HTML page caching** — nginx FastCGI-style cache for profile pages with a 1-day TTL. Invalidate on monthly data refresh. Turns profile pages into ~5ms static HTML delivery.
- **Separate monthly "precompute pass"** — after each new Somos snapshot: compute ranks, flows, enrichments, disconnects, vanities once. Serve the precomputed tables in the app. This is already mostly in place but could be unified into one `rebuild.sh`.
- **Lazy-load images** — Street View + website screenshots use `loading="lazy"` attribute so below-fold images don't block page render.
- **Connection pooling** — DuckDB creates a new connection per request. Keep a global read-only connection for the parquet files.

### Unified monthly pipeline (ingest + disconnect reports + Resporgs data)

Bill already runs a monthly workflow producing disconnect reports (`ALL_disconnect_report_YYYY-MM.xlsx`, `NEW_disconnect_report_YYYY-MM_v2.xlsx`, per-NPA `DISC *.txt` files, vanity filtered lists — all visible in his `D:\resporgs\YYYY-MM\` folders). That workflow reads the same Somos snapshot that feeds Resporgs.com. It makes sense to merge them into one pipeline that runs once per month.

#### Proposed monthly pipeline
```
Somos API download (or file drop) → landed raw on droplet
  ├─> cache/YYYY-MM.parquet          (columnar snapshot for all analytics)
  ├─> ALL_disconnect_report.xlsx     (Bill's existing product)
  ├─> NEW_disconnect_report.xlsx     (Bill's existing product)
  ├─> per-NPA vanity lists           (Bill's existing product)
  ├─> rebuild events + flow graph    (Resporgs.com data)
  ├─> rebuild enrichment             (Resporgs.com: MM%, age buckets)
  ├─> rebuild disconnect episodes    (Resporgs.com: abbreviated vs standard)
  ├─> rebuild precomputed ranks      (Resporgs.com: speed)
  └─> rebuild precomputed flows      (Resporgs.com: speed)
```

All triggered by ONE script invocation (`./rebuild.sh`) or a cron on the 2nd of each month once Somos's report is available.

#### Benefits
- One data ingest, not two
- No risk of the Resporgs.com data diverging from the disconnect-report data
- Automates what's currently a manual laptop-based process
- Once Somos API access is set up on the droplet, removes Bill's laptop from the loop entirely

#### Bridge products (natural next step after unification)
- Expose the **disconnect report** as a Resporgs.com feature for paying subscribers: "Numbers that just disconnected at [RespOrg] this month"
- Show recent DISC activity on profile pages, with a "Watch" CTA per number (feeds Resporg Lock)
- Daily delta against DISC state for early-disconnector resporgs (sub-monthly frequency for real-time vanity drops) — requires daily Somos pulls, which may have an additional cost

#### Existing code to mine (discovered 2026-04-24)

These scripts already do pieces of the pipeline — ported to the unified rebuild, most work is done:

- **`toll-free-autodialer/disconnect_report.py`** — generates the two monthly xlsx reports ("NEW D&T" and "ALL D&T") from `tollfree_intel.db`'s `tf_monthly_scan` table + autodialer search data. Uses openpyxl. Sheets split by category with expired/active separation.
- **`1cup/somos_baseline.py`** — populates a `somos_snapshot` table in `1cup_business.db` from `MikeOINPUT.txt`. Monthly baseline.
- **`1cup/somos_diff.py`** — **literally named "the LifeLock engine"** in its docstring. Diffs two snapshots, writes status/RespOrg changes to a `number_changes` table, prints alert-worthy items. This IS the Resporg Lock backend that's already half-built — just needs email delivery wired to it.
- **`1cup/check_watches.py`** — watch-alerting logic (not yet inspected — probably the missing piece)
- **`1cup/refresh_vanity_data.py`** — periodic refresh of vanity data

**Naming confirmation**: Bill already calls the diff engine "LifeLock" internally. That settles the product name — **Resporg Lock** is the right public brand, and the backend engine exists.

#### Ownership split (clarified 2026-04-24)

The monthly-Somos pipeline and the autodialer-customer-identification pipeline are DIFFERENT projects that just happen to converge in the disconnect report:

- **RESPORGS** — authoritative on monthly Somos data (cache, events, flows, disconnect status/dates/categories). Owns the disconnect report builder itself because the report is fundamentally a Somos product.
- **toll-free-autodialer** — stays separate. Its job is identifying the underlying customer who owned a valuable number via Google searches. Maintains its own DB.
- The disconnect report **JOINS to autodialer's DB as read-only enrichment** for the "who originally owned this number" column — valuable when acquiring just-disconnected vanity numbers.

Target state:
- `RESPORGS/scripts/rebuild.sh` — orchestrator that runs every ingest step
- `RESPORGS/scripts/build_disconnect_reports.py` — reads `cache/*.parquet` + Sanity directory, reaches out to `toll-free-autodialer/data/numbers.db` for optional customer enrichment. Outputs xlsx.
- `RESPORGS/scripts/lifelock_engine.py` — ported from `1cup/somos_diff.py`, emits change events; hook it to the /watch lead capture for Resporg Lock alerts
- `RESPORGS/scripts/check_watches.py` — ported from 1cup; consumes lifelock_engine output + `leads.db` subscribers → sends emails

`toll-free-autodialer/disconnect_report.py` retires once the RESPORGS version is running (it's currently generating the xlsx from autodialer's side; we're inverting the direction so RESPORGS calls autodialer, not the other way around). 1cup's Somos scripts also retire.

#### Migration steps
1. Review the existing scripts (done — mapped above) and decide what to port vs wrap
2. Port the xlsx generator to read from Resporgs.com's `cache/*.parquet` (single source of truth) instead of `tollfree_intel.db`
2. Port that logic into a Python script living in `scripts/` alongside the other pipeline pieces
3. Test: generate a disconnect report from the droplet's cache and diff against Bill's laptop-generated version. They should match.
4. Unified `rebuild.sh` that runs all steps in order
5. Cron-schedule it for the 2nd of each month (once Somos API is wired up; until then, Bill uploads raw files and invokes manually)

### Re-prioritize the existing Section 7e ideas menu

Bill's new priorities re-order the top of that list:
- Previously "future" items that are now **active V2** work:
  - Blog section
  - Resporg Lock / Watch-alert pipeline
  - Number Rescue (new — wasn't previously listed)
  - Hidden category (new)
  - Precompute ranks + flow summaries (new — performance)

---

## 7e. Future work / ideas menu (documented so Bill can prioritize)

Everything below has been proposed but not built. Grouped by theme, roughly
prioritized within each group. Check off or delete as we implement.

### Data enrichment (profile-level)
- [ ] **Peer comparison** — behavioral nearest-neighbor suggestion on every profile (similar Opp.Idx + size + flow signature); helps classify unknowns
- [ ] **Event participation** — top 5 months this rpfx had the biggest gains/losses, linked to event investigation pages
- [ ] **Acquisition cohort map** — detect bulk acquisitions by clustering last-change dates; reveals wholesale deals
- [ ] **First-touch inventory** — numbers held since first assignment, never moved (stable customer signal)
- [ ] **Reactivation rate** — of this rpfx's disconnected numbers, what % did they bring back? (legit carriers reactivate; sharks don't)
- [ ] **Disconnect-age-at-harvest** — when this rpfx harvests, how stale was the number? (aggressiveness signal)
- [ ] **Sub-code usage patterns** — what each sub-code looks like internally (internal operational structure)
- [ ] **Monthly sparkline** on each profile stat card (inventory, harvests, net flow over 42 months)
- [ ] **Bandwidth/Twilio dependency ratio** — % of this rpfx's flow that touches major wholesalers
- [ ] **Mail-drop auto-detection** — flag rpfxs whose Street View shows a UPS Store / Mail Boxes Etc. / commercial mailbox (combined with low median age = strong harvester signal)

### AI + external enrichment
- [ ] **AI-written "About this company"** blurb per profile, built from website scrape + whois + category
- [ ] **AI auto-categorizer** for the 57 "Unknown" rpfxs — look at flow pattern + website + address photo and propose a category (human approves)
- [ ] **AI-written conversational group/category descriptions** — reshape Bill's existing Sanity notes into flowing prose (starting point for edits)
- [ ] **Wikipedia/Crunchbase facts** for recognizable company names
- [ ] **Archive.org historical website screenshots** linked from each profile ("here's what they looked like in 2019")
- [ ] **Press-release correlation** — monitor telecom news, link big directed transfers (like Bandwidth→Mayfair 414K in Dec 2023) to announced deals

### New pages
- [ ] **Leaderboards**: `/leaderboard/harvesters`, `/leaderboard/growth`, `/leaderboard/shrinkers`, `/leaderboard/vanity-holders`, `/leaderboard/sharks`, `/leaderboard/stable-carriers`
- [ ] **Event investigations** — autogenerate an article for each major month (Lumen Dump, Dec 2023 Big Night, Google Voice exit). URL: `/events/<yyyy-mm>-<slug>`
- [ ] **Events calendar** — chronological list of all investigated events
- [ ] **Directory gaps page** — rpfxs lacking Sanity entries, sorted by inventory (so Bill sees what to fill in)
- [ ] **Compare two rpfxs** `/compare?a=MY&b=JW`
- [ ] **"Where did my numbers go?"** — paste a list of phone numbers, get back a 42-month trace of where each one lived. Viral content product.
- [ ] **Industry weather report** — monthly overall health dashboard
- [ ] **Blog section** `/blog` + `/blog/<slug>` — already 92 posts in Sanity; surface them, add categories for "Event investigations" vs general commentary. First piece Bill asked for.
- [ ] **Daily vanity-drop watchlist** — numbers in DISC status at early-disconnector RespOrgs, checked daily

### Ops + automation
- [ ] **Monthly ingest cron** — when new Somos NSR drops, auto-run `cache_months.py` + `build_events.py` + `build_flow_graph.py` + `enrich_profiles.py` + `build_disconnect_episodes.py` + `fetch_streetview.py`, rebuild derived data, push to droplet
- [ ] **Email alerting pipeline** — actually deliver the captured leads (watch, ask, history-review) via bill@tollfreenumber.com
- [ ] **Anomaly detector** — flag any rpfx that loses > 50K in a month → autogenerate event investigation stub
- [ ] **New-rpfx discovery** — when a 2-char prefix appears for the first time, flag it for Bill
- [ ] **Shell-network candidate detector** — cluster analysis on flow patterns, propose new group memberships
- [ ] **Wholesale deal detector** — any single-month directed transfer > 50K → log + generate stub
- [ ] **Automate Somos portal history lookups** (for the /history-review lead-capture pipeline)
- [ ] **Admin dashboard** — pending corrections, new rpfxs, watch-subscriber count, lead inbox
- [ ] **Data-quality flags** — 2023-02/03 duplicate, 2024-04 missing 866, duplicate Sanity entries like SW West Services

### Lead-gen expansions
- [ ] **Enterprise API** with tiered keys (profile + flow + event data for carriers/researchers)
- [ ] **Number watch alert pipeline** (monthly diff → email when watched number changes status)
- [ ] **Paid phone-shark monthly PDF report** subscription
- [ ] **"Request this number" button** on every resporg profile → checkout funnel back to TFNC/LVNC

### UX polish
- [ ] **Directory filters + sort** on `/directory` (by category, Opp.Idx, vanity %)
- [ ] **Search autocomplete** as you type
- [ ] **Mobile responsive pass** — some breakpoints break on narrow screens
- [ ] **Print stylesheets** for lead-capture emails and reports
- [ ] **Share buttons** on profiles and events (X/LinkedIn)
- [ ] **Apply thumb-fallback pattern** (logo → street → satellite → badge) to `/directory`, `/groups` member-list table, and `/search` results
- [ ] **Dark mode**

### Deployment + domain
- [ ] **Move Resporgs.com to its own dedicated IP** (currently shares the droplet with TFNC/LVNC/VanityNumbers/VanityCellular/PhonePlatinumWire) — for SEO inter-site linking benefits
- [ ] **Let's Encrypt auto-renewal** monitoring
- [ ] **CDN in front of static images** (Street View + Sanity assets are the bandwidth hogs)

### Data quality / known issues
- [ ] **Re-acquire 2023-03 snapshot** (currently a duplicate of 2023-02)
- [ ] **Re-acquire missing months**: 2023-07, 2023-08, 2024-01, 2024-04-866
- [ ] **Resolve duplicate Sanity entries**: SW has two entries (West Services + Science West Services)
- [ ] **Write up 57 "Unknown" category rpfxs** — Bill plans to go through these manually using the new Street View hints
- [ ] **Temporal group memberships** — currently flows are attributed to today's group structure; pre-acquisition transfers can look misleading (e.g. Primetel inbound shows Flotrax flows from before Primetel bought them). Requires per-month group assignment history — big project, not urgent.

### Content Bill will write
- [ ] `/transferring` page content (the nav link exists with placeholder)
- [ ] Group descriptions polish (existing Sanity notes are more bullet-list than conversational prose)
- [ ] Opinionated blog posts — shark exposés, industry analysis
- [ ] More FAQ entries as questions come in

---

## 7d. Progress log (2026-04-23 session, second half — polish + disconnect analytics + deploy prep)

### Visualizations added
- **NPA vertical bar chart on profile pages** (ordered 800/888/877/866/855/844/833 — chronological)
- **Status pie chart** with color-coded legend. UNAVAIL count has **14 subtracted** (2 test numbers × 7 NPAs per RespOrg) before rendering — real overage is shown
- **Inventory age pie chart** with a fresh-to-stable color gradient (<1 month: red → 5+ years: deep green)
- **SVG trajectory chart** — y-axis starts at 0 (honest scale), color key added above ("Total active inventory" = blue line, "Numbers harvested from disconnect pool that month" = red bars)

### Disconnect-episode analysis
- New pipeline `scripts/build_disconnect_episodes.py` — walks all 42 monthly parquet caches, groups DISCONN snapshots by (number, rpfx) into consecutive "episodes," classifies each by duration. Runs in ~40 seconds.
- Output: `data/disconnect_summary.parquet` (per-rpfx rollup: `n_abbreviated`, `n_standard`, `n_total`, `abbrev_rate`) and `data/disconnect_episodes.parquet` (per-duration histogram)
- Profile page outbound flow table now splits "Entered multi-month disconnect" into **two rows**:
  - "Standard disconnect aging process (3+ months in DISC)" — going the full aging distance
  - "Abbreviated disconnect period (1–2 months in DISC)" — early release, often in bulk
- Accompanying note on each profile flags **early-disconnector** behavior when `abbrev_rate > 60%`, with language about "worth checking daily for vanity drops"
- Industry distribution: 53% of all episodes last 1 month, 24.5% 2 months, 22% 3+ months; most active "standard" bucket is 4 months (matches the traditional 4-month Somos aging period)

### Vanity holdings — category dropdown
- Per-profile category filter dropdown above the vanity grid. Selecting a category re-renders the grid showing only that category's numbers for this RespOrg
- Dropdown options auto-populated from MM categories that this RespOrg actually holds numbers in, with counts. Default view = all categories, boosted
- Vanity cells now **clickable** — link to `https://tollfreenumbers.com/?status=<last7>` (which shows all NPAs at once on TFN.com). `TFN_EXTERNAL_LINK_PATTERN` in app.py for easy later replacement
- Old "Notable numbers on file (Bill's curated list)" section removed — redundant with the new dynamic grid

### Profile page CTA updates
- Ask-a-question button **promoted** to the header top-right, opposite the logo. Same button still at the bottom of the page. Both direct to `/ask/<rpfx>`
- Watch button **removed** from profile pages (not built out; the /watch route and nav link remain for number-specific watching)

### History review lead capture
- New `/history-review` route + template for "manual full-history review" lead capture — for customers who want deeper than our 4-year window (e.g., earlier sub-code movements, direct Somos portal data)
- New CTA links added:
  - Under the age-bucket pie: "Request a manual history review of a specific toll-free number"
  - On the number-lookup page timeline: button alongside "Watch this number"
- New sqlite table `history_requests` in `data/leads.db`

### Category pages
- **Hero banner image** on every category page from Sanity's category artwork
- Category members list now shows **thumbnail per resporg**: logo where available, else Street View, else satellite, else prefix-code badge placeholder. Fallback chain handled by `_thumb_for()` helper
- Categories index at `/categories` now shows a 96px thumbnail per category (Sanity artwork)

### Group-annotated partner names
- Top trading-partner tables in the flow section now show parent group in parentheses: "Mayfair Communication (Primetel)", "WireStar (Primetel)", etc.
- New helper `_name_with_group(pfx)` + `_refresh_group_index()` — builds a rpfx → group list at startup from Sanity refs + `GROUP_OVERRIDES`

### New route: "Transferring #s"
- Nav link added; placeholder page at `/transferring` with paper-airplane SVG banner and list of topics Bill plans to cover
- Content TBD

### Homepage + directory hero layout fix
- Hero images switched from `object-fit: cover` (cropping to fill) to `object-fit: contain` (showing the whole image, letterboxed if needed) with `max-height: 520px`
- Directory page got its own hero banner (`directory-hero.jpg`)
- Hero text moved out of image overlay onto its own solid background below the image

### Google Maps fetch improvements
- Retry-with-backoff on 403 errors (Google's transient auth hiccups) in `fetch_satellite()` — resolves the cluster of spurious 403s we were seeing
- Adds API requirements: Street View Static + Maps Static + Geocoding (all on same key)
- Final totals after full run: **462 street views**, **489 satellite images**, **zero addresses with no imagery** (everyone with a geocodable address got at least one photo)

## 7c. Progress log (2026-04-23 session — v1 site shipped locally)

### Infrastructure
- Flask preview server running at `http://127.0.0.1:5178` via `preview_start` / `.claude/launch.json` (name: `resporgs-preview`)
- `webapp/` structure: `app.py`, `templates/`, `static/`, `static/img/`, `static/streetview/`
- DNS: `resporgs.com` already pointing to droplet `104.131.76.98` (A records for apex + www + wildcard)
- Lead-capture sqlite at `data/leads.db` with tables `watch_signups`, `questions`, `history_requests`
- Environment variables loaded from `apikey.env` (git-ignored); currently contains `GOOGLE_MAPS_API_KEY`

### Routes live
| URL | Purpose |
|---|---|
| `/` | Homepage — hero banner, "What is a RespOrg", top 20 resporg cards, 18 categories, top 10 groups, FAQ preview |
| `/directory` | Full list of all 481 RespOrgs |
| `/r/<rpfx>` | Resporg profile page |
| `/groups` | All 34 groups ranked by combined inventory |
| `/group/<slug>` | Single group profile with member circles + rank chart |
| `/categories` | 18 categories with summary |
| `/category/<slug>` | Single category with members + aggregate trajectory |
| `/number` + `/number/<tfn>` | Toll-free number history lookup — vanity + digits accepted, redirects to canonical path |
| `/search?q=...` | Unified search — phone → redirects to `/number/<tfn>`, name → results page |
| `/watch` | Watch-signup form with optional `?target=` prefill |
| `/ask` + `/ask/<rpfx>` | Question form (lead capture) |
| `/history-review` | Manual-history request (Somos portal lookup, later to be automated) |
| `/faq` | 14-question FAQ covering basics, usage, and industry patterns |
| `/assets/<path>` | Serves Sanity-exported images (logos, screenshots) |
| `/static/...` | Site assets incl. streetview/satellite images |

### Profile page sections (in order)
1. Header — **logo on left**, title + code + alias + category + group tags
2. Visual context row — website screenshot + street view + satellite view (each clickable → full size)
3. Contact block — website link, "Based in: City, State" (never street address)
4. At-a-glance stat grid — active numbers, 4-year change, Opportunism Index, vanity %, median inventory age, sub-code count; each with **percentile rank** ("#4 of 481")
5. Breakdown row — **NPA vertical bars**, **status pie**, **age-bucket pie** (each with legend + data values)
6. 42-month SVG trajectory chart — y-axis starts at 0, color key, harvest bars overlaid
7. Flow patterns — inbound (Transfer / Harvest / First-assign / Reactivate) + outbound (Transfer / Entered multi-month disconnect / Released directly to spare), with note explaining early-disconnector behavior
8. Top 10 direct-transfer sources/destinations — **group annotated in parentheses**
9. Top vanity holdings — 4-column grid of AC + ALL-CAPS word, 800 boost applied to ranking
10. Sanity narrative — summary, contact-form message, notable numbers (collapsible)
11. CTAs — Ask-a-question + Watch + History-review + Testimonials (if any)

### Data visualizations added
- **SVG trajectory chart** — `render_trajectory_svg()`, y-axis starts at 0, inventory line in blue with fill, red bars for monthly harvest volume; visual legend above
- **Inline SVG pie charts** — `render_pie_svg()`, used for status + age
- **NPA vertical bar chart** — used on both profile and group pages
- **Group rank bar chart** — full list of 34 groups with current one highlighted (on group pages)
- **Member circles** — resporg circles sized proportional to sqrt(inventory), with logos or prefix fallback (on group pages)

### Status-pie UNAVAIL correction
- Every RespOrg has 14 default test numbers (2 per NPA × 7 NPAs) assigned UNAVAIL
- The pie chart subtracts 14 from the UNAVAIL count before rendering. Result: resporgs with exactly 14 show no UNAVAIL slice; resporgs with 15 show 1; resporgs with 20 show 6
- Real percentages recalc against the adjusted total

### Address privacy + visual cues
- Street address never rendered in markup (per Somos rule)
- City and state ARE rendered
- `scripts/fetch_streetview.py` fetches both:
  - **Street View** (Google Street View Static API, $7/1000, free tier covers us) → `webapp/static/streetview/<RPFX>-street.jpg` (462 saved)
  - **Satellite view** (Maps Static API, $2/1000) → `webapp/static/streetview/<RPFX>-satellite.jpg` (489 saved)
- Required APIs enabled on the same Google key: Street View Static + Maps Static + Geocoding
- Script is idempotent, retries 403s (intermittent Google infra hiccups), and logs to `_index.json`

### SEO policy
- Profile / group / category pages: `index, follow`
- Number-lookup pages: **default to `noindex, follow`** (avoid millions of thin pages)
- Number page flips to `index, follow` only when Master-Million match is strong (800 with Mike rank ≤ 5000, OR blended score ≥ 1000)

### Key configuration
- Group membership OVERRIDE in `app.py`: **Flotrax** (AB, FO, HU, JD, OD, OQ, RY) publicly attached to Primetel — revealing the shell relationship. Primetel now shows 18 members / 5.75M combined inventory on `/group/primetel`
- Websites hosted on `tollfreenumbers.com` or `resporgs.com` are hidden (they're our own pages, not the resporg's actual site)

### Lead capture (sqlite `data/leads.db`)
- Watch signups → `watch_signups`
- Ask-a-question → `questions`
- History-review requests → `history_requests`
- Future: email alerts from `bill@tollfreenumber.com` (singular — per Bill's preference), automation of Somos portal lookup

### Open TODOs
- **Vanity-number link pattern** (profile.html "Top vanity holdings" section) — currently every cell links to `https://www.tollfreenumbers.com/{tfn}` via `TFN_EXTERNAL_LINK_PATTERN` in `webapp/app.py`. Verify the actual URL structure on the live TFN.com; update when Resporgs.com-era TollFreeNumbers.com replacement launches.

### Next likely steps
- Actual email sending from captured leads
- Compute and surface **early-disconnect rate** per resporg (TO_SPARE / (TO_SPARE + DISCONNECT)) as a named metric on profiles
- Build a **daily vanity-drop watchlist** — numbers in DISCONN status at early-disconnector RespOrgs, checked daily for SPARE transitions
- Deploy to droplet: nginx server block for `resporgs.com`, Let's Encrypt cert, gunicorn/uvicorn for Flask
- Number-lookup page: enrich with category + group of current holder, link to vanity word if applicable
- Admin dashboard showing new leads and aggregated directory gaps

## 7a. Progress log (2026-04-21 session)

### Infrastructure built
- ✅ Sanity export complete (3,606 docs, 909 assets). Dataset `blog`, project `52jbeh8g`.
- ✅ Dedupe logic separating drafts from published: 498 resporgs, 2,416 testimonials, 34 groups cleaned
- ✅ Month archive on `D:\resporgs`: 16 months 2025-01 → 2026-04 (zips + folders) handled uniformly by [scripts/months.py](scripts/months.py)
- ✅ Handles both filename styles: old `CD-ROM_TFN_Report_*` and new API `Number-Status-NPA-*` (first appeared in 2025-04)
- ✅ Compact Parquet cache in `RESPORGS/cache/`: 16 months × ~200 MB = 3.1 GB (down from ~92 GB raw). [scripts/cache_months.py](scripts/cache_months.py)
- ✅ Multi-month event pipeline: [scripts/build_events.py](scripts/build_events.py) produces `data/pair_totals.parquet` + `data/resporg_month.parquet` (7,742 rows)
- ✅ Analysis report generator: [scripts/analysis.py](scripts/analysis.py) → `data/analysis_report.txt`

### Key metrics validated
- **Opportunism Index** works as predicted:
  - AT&T: not in top harvesters, -119K trajectory over 16 months — legit carrier as stated
  - Twilio: 0.15% Opp. Idx across 1.4M acquired — cleanest mega-carrier
  - Bandwidth: 1.05%
  - Opentext: 0.04% (cleanest with 244K acquired)
- **Stable sharks** across 14 valid transitions (2025-02/03 was a duplicate file):
  - WireStar JW: 72.4%
  - Flotrax FO family: 30-65% across 7 grouped rpfxs (AB, FO, HU, JD, OD, OQ, RY) — all in top 30
  - National Sales Partners NA: 46.4%
  - Crossbow Telecom CB: 47.9%

### Phase 3 validations (shell networks confirmed by data)
- **Primetel group** (human-curated in Sanity): 11 rpfxs, 5.26M combined inventory — **bigger than AT&T's 2.28M**. Multiple members appear in top harvesters (JW 72.4%, CB 47.9%, HL 39.7%, PI 33.6%).
- **Flotrax group**: all 7 curated members appear in top 30 harvesters with consistent 30-65% Opp Idx — shell network confirmed empirically.
- **National Sales Partners shell DISCOVERED** from Bill's contact-info scraping of the 14 new resporgs:
  - 5 additional rpfxs share one Cleveland address (3634 Euclid Ave) and one operator (Ahmed Essa, ee@nationalsalespartners.com)
  - Members: NA (existing Sanity entry) + KG (Protection Plan Center), EK (Fundish), MB (Numbermed), RJ (Ultramemorable), YB (Hi Tech Zone)
  - Pending: create NSP group in Sanity
- **Call Haven Partners expansion**: NJ (Number Prospectors) contact email is `daryl@callhavenpartners.com`, linking to ZX (Haven Partners) + HV (Call Haven). Pending: create new group in Sanity with ZX+HV+NJ.

### Signature investigation: The April 2026 Lumen Dump
- AL (Lumen ALN01) dropped from 330,121 numbers → 118 in 2026-04 (−330K in one month)
- Not an internal Lumen consolidation — all 11 Lumen-group siblings were flat
- ALL 330K numbers went to DISCONN, then 15+ sharks harvested them within the month
- Top recipients: MY 97K, IU 74K, JW 41K, GA 25K, FO 20K, NA 17K, CB 14K, ZX 11K
- **Primetel alone (MY + JW + CB + HL) ate 153K — 47% of the carcass**
- Explains April's industry-wide anomaly: 446K disconnect landings (≈3× normal month), 1.2M transfers, 93.5% overlap (lowest in 16 months)
- Saved as [scripts/investigate_aln.py](scripts/investigate_aln.py) — template for any future large-scale movement event

### Directory gaps filled (14 new resporgs identified with contact info)
EF=Pestilence Labs LLC (623K, biggest unknown), QU=QuestBlue, NJ=Number Prospectors, KI=USA Digital, LK=Lincoln Comm, QO=ParkHill Telecom, GW=Digitorzo, RH=Porting.com/ATLaaS, ES=All8series, EI=MAP Communications, YG=Ziply Fiber, WC=Vertex, QR=Wiretap Telecom, SM=Snapcom. Plus 9 small reservation blocks including CVS, Union Pacific, Telesign, Edify Labs.

### Data anomaly
- **2025-03 snapshot is a byte-for-byte duplicate of 2025-02** (45,256,306 rows, 100% match). Cause unknown — probably a bad download. 14 valid transitions, not 15. If it can be re-acquired from Dropbox/Somos, do so; otherwise harmless.
- **2025-10** showed a 56K AL drop — smaller precursor to the April dump

---

## 7b. Resporgs.com V1 scope (from design conversation)

### Tier 1 — Auto-generated profile pages for every rpfx (~500 pages)
- Rich profiles for the 498 Sanity-curated + stub-plus-data profiles for the 20-30 uncovered rpfxs
- Each profile shows: inventory + status breakdown, 16-month trajectory, Opportunism Index, net flow trend, top trading partners, sub-code usage, group membership
- Driven by the Parquet event tables — monthly rebuild

### Tier 2 — Group pages
- 34 existing Sanity groups + NSP + Call Haven Partners (new) → 36 group pages
- Each shows aggregate inventory across members, combined Opportunism, shell-network evidence
- **Industry-first public group-control data** (first time "Primetel controls 5.26M" exists anywhere)

### Tier 3 — Event investigations (auto-generated)
- Any month where any rpfx loses >50K triggers an investigation page
- Template from the Lumen Dump piece: who lost, who gained, why, the shark feeding breakdown
- Monthly rhythm: first week of each month publishes prior month's events
- Content engine that runs itself — SEO gold no one else has

### Tier 4 — Number-level lookups (lead-gen)
- Reverse lookup: "what RespOrg controls 1-800-X"
- Watch this number: email me on any status change — **highest-intent signal in the industry**
- "Where did my number go?" — full 16-month trace
- Every lookup funnels qualified leads back to TFNC/LVNC/VanityNumbers checkout

### Tier 5 — AI layer (v2)
- Resporg summary bot per profile
- Shell-network detector (automated candidate groups, human-in-loop confirmation)
- Ask-the-directory natural-language chatbot against the Parquet dataset

### Tier 6 — Crowd contributions (v2)
- Corrections/additions submission
- User-submitted intelligence with moderation

### Architecture
- **Hosting**: DigitalOcean droplet `104.131.76.98` (shared with PhonePlatinumWire, VanityCellular, tollfreenumbers-com). New Flask app on fresh port, nginx routes by domain.
- **Static + dynamic hybrid**: static-generate 500+ profile pages + group pages + event investigations (cached, fast, SEO). Flask serves interactive bits (watch signup, ask-a-question, reverse lookup).
- **Data refresh**: monthly after new Somos snapshot. Profile pages rebuild overnight.
- **URL structure**:
  - `resporgs.com/<rpfx>` — single resporg profile (e.g. `/my` = Mayfair)
  - `resporgs.com/group/<slug>` — group page (e.g. `/group/primetel`)
  - `resporgs.com/events/<yyyy-mm>-<slug>` — investigations (e.g. `/events/2026-04-lumen-dump`)
  - `resporgs.com/watch` — number watch signup
  - `resporgs.com/number/<tfn>` — reverse lookup
  - `resporgs.com/leaderboard/harvesters` etc.

### DNS (pending)
- Resporgs.com currently points nowhere
- Point A record → `104.131.76.98`
- Stand up placeholder page + nginx config first, then build behind it

### V1 ship scope (2-week target)
1. Profile pages for all ~500 rpfxs (data-driven, Jinja templates)
2. Opportunism Index + 16-month trajectory on every profile
3. 36 group pages (34 existing + NSP + Call Haven Partners)
4. First event investigation: "The April 2026 Lumen Dump"
5. Ask-a-question form (to Bill's inbox)
6. Number-watch signup (email capture only — alerting pipeline in v2)
7. Basic leaderboards: top harvesters, top gainers, top shrinkers, group-control rankings

### V2 additions
- Reverse number lookup
- Watch-alert pipeline (monthly snapshot diff → email subscribers)
- Event-detection cron
- AI summaries per profile
- Shell-network detector with human-in-loop
- Crowd contributions

---

## 8. Immediate next step

Before committing to Phase 1 architecture, run a **small data-exploration spike**:
- Parse the 2025-06 snapshot and the 2026-04 snapshot
- Build the diff between them (10 months apart — good stress test)
- Validate: transfer events, disconnect landings, reactivation filter, Opportunism Index
- Sanity-check AT&T: Opportunism Index should be near 0
- Sanity-check a known harvester: should be near 1

Deliverable from the spike: a short report validating the pipeline logic against a known ground truth (Bill's industry knowledge). This de-risks Phase 2 before we commit to Phase 1 infrastructure that will feed it.

---

## 9. Sequencing decisions to confirm

- Does the spike happen before or after Phase 1 starts?
- Are Phase 1 and Phase 2 sequential or overlapping?
- Is Phase 3 (group resolution) the real marquee feature that should be teased from Day 1 even if it ships last?
