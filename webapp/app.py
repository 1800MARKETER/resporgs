"""
Local preview server for Resporgs.com profile pages.

Run:
    python webapp/app.py

Then browse:
    http://localhost:5178/
    http://localhost:5178/r/MY      (single profile)
    http://localhost:5178/r/JW
    http://localhost:5178/search?q=primetel
"""

from __future__ import annotations
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import duckdb
from flask import Flask, render_template, abort, send_from_directory, request, redirect, url_for
import sqlite3

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
CACHE = ROOT / "cache"
CLEAN = ROOT / "clean"
ASSET_ROOT = ROOT / "sanity-export" / "blog-export-2026-04-21t16-03-52-563z"
MM_DB = ROOT.parent / "local-prospector" / "data" / "master_vanity.db"

# Group-membership overrides — reveal Flotrax resporgs as part of Primetel.
# Slug-keyed; values are 2-char prefixes to ADD to the group's Sanity membership.
# Bill will eventually sync these into Sanity.
GROUP_OVERRIDES: dict[str, list[str]] = {
    "primetel": ["AB", "FO", "HU", "JD", "OD", "OQ", "RY"],
}

# Hidden RespOrgs — any rpfx here becomes invisible from public views.
# Primary source: Sanity category with slug `hidden` (see _refresh_hidden_index).
# Secondary source: this hardcoded set, useful for urgent hides before a
# Sanity round-trip. Unified into HIDDEN_RPFX at startup.
HIDDEN_OVERRIDE: set[str] = set()

# Current TFN.com search URL takes just the 7-digit suffix and shows all NPAs —
# better UX than sending visitors to a single NPA. Update to new pattern when
# the Resporgs.com-era TollFreeNumbers.com replacement launches.
# {tfn} = 10-digit, {ac} = 3-digit area code, {last7} = 7-digit suffix, {word} = vanity word.
TFN_EXTERNAL_LINK_PATTERN = "https://tollfreenumbers.com/?status={last7}"

app = Flask(__name__)

# ============================================================
# Sanity data in memory (cheap at this size)
# ============================================================

def _load(name: str):
    return json.loads((CLEAN / f"{name}.json").read_text(encoding="utf-8"))


RESPORG_DOCS = _load("resporg")
GROUP_DOCS = _load("resporgGroup")
CATEGORY_DOCS = _load("resporgCategory")
TESTIMONIAL_DOCS = _load("testimonial")

# Per-rpfx precomputed rank lookup (built by scripts/build_ranks.py).
# Loaded once at startup — saves 6 per-request rank queries.
RANKS: dict[str, dict] = {}
_ranks_file = DATA / "ranks.parquet"
if _ranks_file.exists():
    import duckdb as _dd
    for r in _dd.connect().execute(
        f"SELECT * FROM read_parquet('{_ranks_file.as_posix()}')"
    ).fetchall():
        RANKS[r[0]] = {
            "inv_rank": r[1], "inv_total": r[2],
            "opp_rank": r[3], "opp_total": r[4],
            "growth_rank": r[5], "growth_total": r[6],
            "vanity_rank": r[7], "vanity_total": r[8],
            "age_rank": r[9], "age_total": r[10],
        }

# Per-rpfx disconnect-episode counts (computed by scripts/build_disconnect_episodes.py)
DISC_SUMMARY: dict[str, dict] = {}
_ds_file = DATA / "disconnect_summary.parquet"
if _ds_file.exists():
    import duckdb as _dd
    for r in _dd.connect().execute(
        f"SELECT rpfx, n_abbreviated, n_standard, n_total, abbrev_rate "
        f"FROM read_parquet('{_ds_file.as_posix()}')"
    ).fetchall():
        DISC_SUMMARY[r[0]] = {
            "n_abbreviated": r[1],
            "n_standard": r[2],
            "n_total": r[3],
            "abbrev_rate": r[4],
        }

RESPORGS_BY_PREFIX: dict[str, list[dict]] = defaultdict(list)
for d in RESPORG_DOCS:
    code = (d.get("codeTwoDigit") or "").strip().upper()
    if len(code) >= 2:
        RESPORGS_BY_PREFIX[code[:2]].append(d)

CAT_BY_ID = {
    c["_id"].removeprefix("drafts."): {
        "slug": (c.get("slug") or {}).get("current", "unknown"),
        "title": c.get("title", "?"),
    }
    for c in CATEGORY_DOCS
}
GROUP_BY_ID = {
    g["_id"].removeprefix("drafts."): {
        "slug": (g.get("slug") or {}).get("current", "?"),
        "title": g.get("title", "?"),
        "description": g.get("description", ""),
    }
    for g in GROUP_DOCS
}


# ============================================================
# Helpers
# ============================================================

def asset_url(image_field):
    if not image_field:
        return None
    ref = image_field.get("_sanityAsset") or ""
    if "file://" in ref:
        rel = ref.split("file://", 1)[1].lstrip("./")
        return "/assets/" + rel
    return None


def portable_text_to_plain(blocks):
    if not blocks:
        return ""
    return "\n\n".join(
        "".join(child.get("text", "") for child in (blk.get("children") or []))
        for blk in blocks
        if blk.get("_type") == "block"
    ).strip()


def find_testimonials(title, alias):
    needles = []
    for s in (title, alias):
        if not s:
            continue
        s = s.strip()
        if len(s) >= 5 and s.lower() not in {"?", "unknown", "secondary"}:
            needles.append(s.lower())
    if not needles:
        return []
    patterns = [re.compile(r"\b" + re.escape(n) + r"\b") for n in needles]
    hits = []
    for t in TESTIMONIAL_DOCS:
        body = (t.get("body") or "").lower()
        if any(p.search(body) for p in patterns):
            hits.append(t)
    return hits


STATUS_NAMES = {
    1: "WORKING", 2: "TRANSIT", 3: "DISCONN",
    4: "RESERVED", 5: "UNAVAIL", 6: "ASSIGNED", 7: "SUSPEND",
}

# NPAs in the order they were introduced (oldest first, the way the
# industry presents them)
NPA_PREFIXES = (800, 888, 877, 866, 855, 844, 833)


def format_tfn(n: int) -> str:
    """10-digit integer -> '800-XXXXXXX' (Bill's convention: NPA then a 7-digit block)."""
    s = f"{int(n):010d}"
    return f"{s[:3]}-{s[3:]}"


def percentile_label(rank: int | None, total: int | None) -> str | None:
    """'#12 of 481' — bare rank, no tier bucket."""
    if rank is None or total is None or total <= 0:
        return None
    return f"#{rank} of {total}"


# ============================================================
# Data assembly for one profile
# ============================================================

def _latest_month():
    return sorted(p.stem for p in CACHE.glob("*.parquet"))[-1]


def build_profile(rpfx: str) -> dict | None:
    rpfx = rpfx.upper()
    # Hidden rpfxs 404 — no profile page, no mentions, no hints they exist.
    if _is_hidden(rpfx):
        return None
    sanity_docs = RESPORGS_BY_PREFIX.get(rpfx, [])

    con = duckdb.connect()
    month = _latest_month()
    curr = (CACHE / f"{month}.parquet").as_posix()
    resporg_month = (DATA / "resporg_month.parquet").as_posix()
    enrich = (DATA / "enrichment_current.parquet").as_posix()
    vanity_hits = (DATA / "enrichment_vanity_hits.parquet").as_posix()
    flow_graph = (DATA / "flow_graph.parquet")

    # Inventory existence check: is this rpfx real?
    total = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{curr}') WHERE rpfx = '{rpfx}'"
    ).fetchone()[0]
    if total == 0 and not sanity_docs:
        return None

    primary = sanity_docs[0] if sanity_docs else None
    title = (primary.get("title") if primary else None) or f"Unknown RespOrg ({rpfx})"
    alias = primary.get("alias") if primary else None

    # Categories & groups
    cats = []
    grps = []
    for d in sanity_docs:
        for cref in d.get("categories", []) or []:
            cid = cref.get("_ref")
            if cid in CAT_BY_ID:
                cats.append(CAT_BY_ID[cid])
        for gref in d.get("groups", []) or []:
            gid = gref.get("_ref")
            if gid in GROUP_BY_ID:
                grps.append(GROUP_BY_ID[gid])
    # Dedupe
    cats = {c["slug"]: c for c in cats}.values()
    grps = {g["slug"]: g for g in grps}.values()

    # Inventory breakdowns — precomputed by scripts/build_rpfx_snapshot.py.
    # Falls back to live cache scan if the precompute is missing.
    snap_path = (DATA / "rpfx_snapshot.parquet").as_posix()
    sub_path = (DATA / "rpfx_subcodes.parquet").as_posix()
    if Path(snap_path).exists() and Path(sub_path).exists():
        snap_rows = con.execute(
            f"SELECT prefix, status, n FROM read_parquet('{snap_path}') WHERE rpfx = ?",
            [rpfx],
        ).fetchall()
        by_prefix = {}
        by_status = {}
        for pref, st, n in snap_rows:
            by_prefix[pref] = by_prefix.get(pref, 0) + n
            by_status[st] = by_status.get(st, 0) + n
        subcodes = [
            row[0]
            for row in con.execute(
                f"SELECT resporg FROM read_parquet('{sub_path}') WHERE rpfx = ? ORDER BY n DESC",
                [rpfx],
            ).fetchall()
        ]
    else:
        by_prefix = dict(
            con.execute(
                f"SELECT prefix, COUNT(*) FROM read_parquet('{curr}') WHERE rpfx = '{rpfx}' GROUP BY prefix"
            ).fetchall()
        )
        by_status = dict(
            con.execute(
                f"SELECT status, COUNT(*) FROM read_parquet('{curr}') WHERE rpfx = '{rpfx}' GROUP BY status"
            ).fetchall()
        )
        subcodes = [
            row[0]
            for row in con.execute(
                f"SELECT resporg FROM read_parquet('{curr}') WHERE rpfx = '{rpfx}' "
                f"GROUP BY resporg ORDER BY COUNT(*) DESC"
            ).fetchall()
        ]

    # Trajectory
    traj = con.execute(
        f"""
        SELECT month, inventory, acquired, lost, harvested_cross_rpfx,
               appeared_from_spare, disappeared_to_spare
        FROM read_parquet('{resporg_month}')
        WHERE rpfx = '{rpfx}'
        ORDER BY month
        """
    ).fetchall()

    # Aggregates & Opp.Idx
    if traj:
        total_acq = sum(r[2] for r in traj)
        total_harv = sum(r[4] for r in traj)
        opp_idx = total_harv / total_acq if total_acq else 0
        first_m, first_inv = traj[0][0], traj[0][1]
        last_m, last_inv = traj[-1][0], traj[-1][1]
        delta = last_inv - first_inv
        pct = (delta / first_inv * 100) if first_inv else 0
    else:
        total_acq = total_harv = 0
        opp_idx = 0
        first_m = last_m = month
        first_inv = last_inv = total
        delta = pct = 0

    # Rank computations — read from precomputed data/ranks.parquet instead of
    # running 6 fresh ranking queries per request. Rebuilt by scripts/build_ranks.py
    # as part of the monthly pipeline.
    industry_count = con.execute(
        f"SELECT COUNT(DISTINCT rpfx) FROM read_parquet('{curr}')"
    ).fetchone()[0]

    r = RANKS.get(rpfx, {})
    inv_rank, inv_total = r.get("inv_rank"), r.get("inv_total")
    opp_rank, opp_total = r.get("opp_rank"), r.get("opp_total")
    growth_rank, growth_total = r.get("growth_rank"), r.get("growth_total")

    # Enrichment (MM + age)
    enr_row = con.execute(
        f"""
        SELECT working_count, mm_count,
               b_under_1m, b_1_3m, b_3_12m, b_1_2y, b_2_5y, b_5y_plus,
               median_age_months
        FROM read_parquet('{enrich}') WHERE rpfx = '{rpfx}'
        """
    ).fetchone()
    if enr_row:
        wc, mc, *ages, median_age = enr_row
        mm_pct = (mc / wc) if wc else 0
        age_buckets = dict(zip(
            ("under_1m", "_1_3m", "_3_12m", "_1_2y", "_2_5y", "_5y_plus"),
            ages,
        ))
    else:
        wc = mc = median_age = 0
        mm_pct = 0
        age_buckets = {k: 0 for k in ("under_1m", "_1_3m", "_3_12m", "_1_2y", "_2_5y", "_5y_plus")}

    vanity_rank, vanity_total = r.get("vanity_rank"), r.get("vanity_total")
    age_rank, age_total = r.get("age_rank"), r.get("age_total")

    # Vanity holdings — precomputed by scripts/build_vanity_precompute.py.
    # Two parquet tables let us skip the live MM-sqlite join entirely:
    #   vanity_categories.parquet : (rpfx, category_code, category_label, n)
    #   vanity_top.parquet        : (rpfx, category_code, number, word, ord)
    # Default view uses rows where category_code IS NULL.
    vanity_cats_path = (DATA / "vanity_categories.parquet").as_posix()
    vanity_top_path = (DATA / "vanity_top.parquet").as_posix()
    vanity_cat = (request.args.get("vanity_cat") or "").strip().upper()

    vanity_cats: list = []
    vanity: list = []
    if Path(vanity_cats_path).exists() and Path(vanity_top_path).exists():
        vanity_cats = con.execute(
            f"""
            SELECT category_code, category_label, n
            FROM read_parquet('{vanity_cats_path}')
            WHERE rpfx = ?
            ORDER BY n DESC
            """,
            [rpfx],
        ).fetchall()

        if vanity_cat:
            vanity = con.execute(
                f"""
                SELECT number, word, 0 AS boosted
                FROM read_parquet('{vanity_top_path}')
                WHERE rpfx = ? AND category_code = ?
                ORDER BY ord LIMIT 60
                """,
                [rpfx, vanity_cat],
            ).fetchall()
        else:
            vanity = con.execute(
                f"""
                SELECT number, word, 0 AS boosted
                FROM read_parquet('{vanity_top_path}')
                WHERE rpfx = ? AND category_code IS NULL
                ORDER BY ord LIMIT 60
                """,
                [rpfx],
            ).fetchall()

    # Flow data — precomputed by scripts/build_flow_precompute.py.
    # Two small parquets replace 4 live queries against flow_graph.parquet.
    flow = {}
    totals_file = DATA / "flow_totals.parquet"
    partners_file = DATA / "flow_top_partners.parquet"
    if totals_file.exists() and partners_file.exists():
        tot_row = con.execute(
            f"""
            SELECT inbound_transfer, inbound_harvest, inbound_first_assign, inbound_reactivate,
                   outbound_transfer, outbound_disconnect, outbound_to_spare
            FROM read_parquet('{totals_file.as_posix()}')
            WHERE rpfx = ?
            """,
            [rpfx],
        ).fetchone()
        if tot_row:
            inbound = {
                "TRANSFER": tot_row[0],
                "HARVEST": tot_row[1],
                "FIRST_ASSIGN": tot_row[2],
                "REACTIVATE": tot_row[3],
            }
            outbound = {
                "TRANSFER": tot_row[4],
                "DISCONNECT": tot_row[5],
                "TO_SPARE": tot_row[6],
            }
        else:
            inbound = {}
            outbound = {}

        top_sources = con.execute(
            f"""
            SELECT partner_rpfx, n FROM read_parquet('{partners_file.as_posix()}')
            WHERE rpfx = ? AND direction = 'in' ORDER BY ord
            """,
            [rpfx],
        ).fetchall()
        top_dests = con.execute(
            f"""
            SELECT partner_rpfx, n FROM read_parquet('{partners_file.as_posix()}')
            WHERE rpfx = ? AND direction = 'out' ORDER BY ord
            """,
            [rpfx],
        ).fetchall()

        flow = {
            "inbound": inbound,
            "outbound": outbound,
            "top_sources": [
                (s, n, _name_with_group(s))
                for s, n in top_sources
                if not _is_hidden(s)
            ],
            "top_dests": [
                (d, n, _name_with_group(d))
                for d, n in top_dests
                if not _is_hidden(d)
            ],
        }

    # Disconnect-episode split
    disc = DISC_SUMMARY.get(rpfx, {})

    # Testimonials
    testimonials = find_testimonials(title, alias)

    # City / state only — privacy rule: no phone, no street address
    city = state = None
    if primary and primary.get("address"):
        addr = primary["address"]
        city = addr.get("city")
        state = addr.get("state")

    # Hide websites that point back to tollfreenumbers.com (that's our OWN
    # resporg page, not the resporg's real website) or resporgs.com (circular).
    website = None
    if primary and primary.get("website"):
        w = primary["website"].lower()
        if "tollfreenumbers.com" not in w and "resporgs.com" not in w:
            website = primary["website"]

    # Street view + satellite images — pre-fetched by scripts/fetch_streetview.py.
    # We never publish the street address text; these images are the visual cue.
    streetview_url = satellite_url = None
    sv_dir = ROOT / "webapp" / "static" / "streetview"
    if (sv_dir / f"{rpfx}-street.jpg").exists():
        streetview_url = f"/static/streetview/{rpfx}-street.jpg"
    if (sv_dir / f"{rpfx}-satellite.jpg").exists():
        satellite_url = f"/static/streetview/{rpfx}-satellite.jpg"

    trajectory_list = [
        {
            "month": r[0], "inventory": r[1], "acquired": r[2],
            "lost": r[3], "harvested": r[4], "from_spare": r[5],
            "to_spare": r[6],
        }
        for r in traj
    ]

    # --- Chart data (NPA bars + status pie + age pie) ---
    max_npa_count = max(by_prefix.values()) if by_prefix else 1
    npa_chart = [
        {
            "npa": p,
            "count": by_prefix.get(p, 0),
            "pct_of_max": (by_prefix.get(p, 0) / max_npa_count) if max_npa_count else 0,
            "pct_of_total": (by_prefix.get(p, 0) / total) if total else 0,
        }
        for p in NPA_PREFIXES
    ]

    # Every resporg has 14 default test numbers (2 per NPA × 7 NPAs) assigned
    # as UNAVAIL. Subtract those to show only the "real" UNAVAIL count, which
    # is usually 0 — but if a resporg has 15, we should show 1.
    adjusted_status = []
    for k, n in by_status.items():
        name = STATUS_NAMES.get(k, "?")
        adjusted = n - 14 if name == "UNAVAIL" else n
        if adjusted <= 0:
            continue
        adjusted_status.append((k, adjusted))
    status_total = sum(n for _, n in adjusted_status) or 1
    status_slices = []
    for k, n in sorted(adjusted_status, key=lambda x: -x[1]):
        name = STATUS_NAMES.get(k, "?")
        status_slices.append({
            "label": name,
            "value": n,
            "color": STATUS_COLORS.get(name, "#9ca3af"),
            "pct": n / status_total,
        })
    status_pie_svg = render_pie_svg(status_slices)

    age_slices = []
    for label, key in AGE_LABELS:
        v = age_buckets.get(key, 0)
        age_slices.append({
            "label": label,
            "value": v,
            "color": AGE_COLORS[key],
            "pct": (v / wc) if wc else 0,
        })
    age_pie_svg = render_pie_svg(age_slices)

    return {
        "rpfx": rpfx,
        "title": title,
        "alias": alias,
        "primary": primary,
        "logo": asset_url(primary.get("logoImage")) if primary else None,
        "screenshot": asset_url(primary.get("screenShotImage")) if primary else None,
        "categories": list(cats),
        "groups": list(grps),
        "city": city,
        "state": state,
        "website": website,
        "streetview_url": streetview_url,
        "satellite_url": satellite_url,
        "total_inv": total,
        "inv_rank_label": percentile_label(inv_rank, inv_total),
        "industry_count": industry_count,
        "delta": delta, "pct_change": pct,
        "growth_rank_label": percentile_label(growth_rank, growth_total),
        "opp_idx": opp_idx,
        "opp_rank_label": percentile_label(opp_rank, opp_total),
        "total_acq": total_acq,
        "total_harv": total_harv,
        "by_prefix": [(p, by_prefix.get(p, 0)) for p in NPA_PREFIXES],
        "by_status": [
            (STATUS_NAMES.get(k, "?"), v) for k, v in sorted(by_status.items(), key=lambda x: -x[1])
        ],
        "npa_chart": npa_chart,
        "status_slices": status_slices,
        "status_pie_svg": status_pie_svg,
        "age_slices": age_slices,
        "age_pie_svg": age_pie_svg,
        "subcodes": subcodes,
        "trajectory": trajectory_list,
        "trajectory_svg": render_trajectory_svg(trajectory_list),
        "working_count": wc,
        "mm_count": mc,
        "mm_pct": mm_pct,
        "vanity_rank_label": percentile_label(vanity_rank, vanity_total),
        "median_age_months": median_age,
        "age_rank_label": percentile_label(age_rank, age_total),
        "age_buckets": age_buckets,
        "vanity_hits": [
            {
                "ac": f"{int(n) // 10000000:03d}",
                "word": w,
                "external_url": TFN_EXTERNAL_LINK_PATTERN.format(
                    tfn=f"{int(n):010d}",
                    ac=f"{int(n) // 10000000:03d}",
                    last7=f"{int(n) % 10000000:07d}",
                    word=w,
                ),
            }
            for n, w, _boosted in vanity
        ],
        "vanity_cats": [
            {"code": code, "label": label, "n": n}
            for code, label, n in vanity_cats
        ],
        "vanity_cat_selected": vanity_cat or None,
        "flow": flow,
        "disc_episodes": disc,  # {n_abbreviated, n_standard, n_total, abbrev_rate}
        "summary": portable_text_to_plain(primary.get("summary")) if primary else "",
        "message": portable_text_to_plain(primary.get("exactMatchMessage")) if primary else "",
        "notable_numbers": primary.get("topNumbers") if primary else None,
        "testimonials": testimonials,
        "month": month,
    }


STATUS_COLORS = {
    "WORKING":  "#0e7c3a",
    "DISCONN":  "#b42318",
    "TRANSIT":  "#f59e0b",
    "RESERVED": "#7c3aed",
    "UNAVAIL":  "#6b7280",
    "ASSIGNED": "#60a5fa",
    "SUSPEND":  "#7f1d1d",
}

# Fresh → stable. Earlier buckets = recently changed; later = long-held.
AGE_COLORS = {
    "under_1m": "#dc2626",
    "_1_3m":    "#ea580c",
    "_3_12m":   "#eab308",
    "_1_2y":    "#84cc16",
    "_2_5y":    "#16a34a",
    "_5y_plus": "#065f46",
}
AGE_LABELS = [
    ("< 1 month",    "under_1m"),
    ("1–3 months",   "_1_3m"),
    ("3–12 months",  "_3_12m"),
    ("1–2 years",    "_1_2y"),
    ("2–5 years",    "_2_5y"),
    ("5+ years",     "_5y_plus"),
]


def render_pie_svg(slices: list[dict], size: int = 180) -> str:
    """slices = [{'label', 'value', 'color'}]. Return inline SVG."""
    import math
    total = sum(s["value"] for s in slices if s["value"] > 0)
    if total <= 0:
        return ""
    cx = cy = size / 2
    r = size / 2 - 4
    parts: list[str] = []
    angle = -math.pi / 2
    drawn = 0
    for s in slices:
        v = s["value"]
        if v <= 0:
            continue
        pct = v / total
        span = pct * 2 * math.pi
        end_angle = angle + span
        if pct >= 0.999:
            parts.append(
                f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{s["color"]}" '
                f'stroke="white" stroke-width="1"/>'
            )
        else:
            x1 = cx + r * math.cos(angle)
            y1 = cy + r * math.sin(angle)
            x2 = cx + r * math.cos(end_angle)
            y2 = cy + r * math.sin(end_angle)
            large = 1 if span > math.pi else 0
            d = (
                f"M {cx:.2f} {cy:.2f} L {x1:.2f} {y1:.2f} "
                f"A {r:.2f} {r:.2f} 0 {large} 1 {x2:.2f} {y2:.2f} Z"
            )
            parts.append(
                f'<path d="{d}" fill="{s["color"]}" stroke="white" stroke-width="1"/>'
            )
        angle = end_angle
        drawn += 1
    return (
        f'<svg viewBox="0 0 {size} {size}" class="pie-chart" '
        f'xmlns="http://www.w3.org/2000/svg">{"".join(parts)}</svg>'
    )


# 18 visually-distinct colors for multi-series charts. Shared by the
# category growth chart (all 18) and the per-category member pie (first 8).
CATEGORY_PALETTE = [
    "#0b5ed7", "#dc2626", "#16a34a", "#d97706", "#7c3aed",
    "#0891b2", "#be185d", "#65a30d", "#b45309", "#6b21a8",
    "#1e40af", "#991b1b", "#047857", "#a16207", "#701a75",
    "#155e75", "#831843", "#3f6212",
]


def render_multi_line_svg(
    series: list[dict],
    width: int = 860,
    height: int = 340,
) -> str:
    """Multi-line SVG chart, each series normalized to its first non-zero month = 100%.

    series[i] = {"label": str, "color": str, "points": [(month, value), ...]}

    Returns (svg_string, legend_html).
    Legend rows are built alongside for the template.
    """
    if not series:
        return "", ""

    # Union of months across all series, sorted
    all_months = sorted({m for s in series for m, _ in s["points"]})
    if not all_months:
        return "", ""

    pad_l, pad_r, pad_t, pad_b = 56, 16, 18, 32
    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b
    n_months = len(all_months)
    month_to_x = {
        m: pad_l + (i * inner_w / (n_months - 1) if n_months > 1 else 0)
        for i, m in enumerate(all_months)
    }

    # Normalize each series to its first non-zero baseline; compute % of baseline
    normalized_series = []
    for s in series:
        pts_by_month = dict(s["points"])
        baseline = 0
        for m in all_months:
            v = pts_by_month.get(m, 0)
            if v > 0:
                baseline = v
                break
        if baseline == 0:
            continue
        pct_points = []
        for m in all_months:
            v = pts_by_month.get(m)
            if v is None:
                continue
            pct_points.append((m, 100.0 * v / baseline))
        if not pct_points:
            continue
        final_pct = pct_points[-1][1]
        final_inv = pts_by_month.get(all_months[-1], 0)
        normalized_series.append({
            "label": s["label"],
            "color": s["color"],
            "points": pct_points,
            "final_pct": final_pct,
            "final_inv": final_inv,
        })

    # Y axis range: find min/max across all normalized points, pad a little
    all_pcts = [p for s in normalized_series for _, p in s["points"]]
    y_min = min(all_pcts + [100])
    y_max = max(all_pcts + [100])
    span = y_max - y_min
    if span < 20:
        y_max += 10
        y_min -= 10
        span = y_max - y_min
    # Round baselines outward a bit
    y_min = max(0, int(y_min // 25) * 25)
    y_max = int(((y_max // 25) + 1) * 25)
    span = y_max - y_min

    def y_of(pct: float) -> float:
        return pad_t + inner_h - ((pct - y_min) / span) * inner_h

    parts = []

    # Gridlines every 25% of span, with labels
    step = 25
    yval = (y_min // step) * step
    while yval <= y_max:
        y = y_of(yval)
        parts.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width-pad_r}" y2="{y:.1f}" '
            f'stroke="#e5e7eb" stroke-width="1"/>'
        )
        color = "#9ca3af" if yval == 100 else "#6b7280"
        weight = "600" if yval == 100 else "400"
        parts.append(
            f'<text x="{pad_l - 8}" y="{y+3:.1f}" text-anchor="end" font-size="10" '
            f'fill="{color}" font-weight="{weight}">{yval}%</text>'
        )
        yval += step

    # X axis labels: first, middle, last
    for i in (0, n_months // 2, n_months - 1):
        if 0 <= i < n_months:
            m = all_months[i]
            parts.append(
                f'<text x="{month_to_x[m]:.1f}" y="{pad_t + inner_h + 16}" '
                f'text-anchor="middle" font-size="10" fill="#6b7280">{m}</text>'
            )

    # One <path> per series
    for s in normalized_series:
        d = "M " + " L ".join(
            f"{month_to_x[m]:.1f},{y_of(p):.1f}" for m, p in s["points"]
        )
        parts.append(
            f'<path d="{d}" stroke="{s["color"]}" stroke-width="1.6" '
            f'fill="none" stroke-linejoin="round" stroke-linecap="round">'
            f'<title>{s["label"]}: {s["final_pct"]:.0f}% of baseline '
            f'({s["final_inv"]:,} numbers now)</title></path>'
        )

    svg = (
        f'<svg viewBox="0 0 {width} {height}" class="multi-chart" '
        f'xmlns="http://www.w3.org/2000/svg">'
        + "".join(parts)
        + "</svg>"
    )

    # Legend HTML — sorted by final_pct descending (biggest growers first)
    normalized_series.sort(key=lambda s: -s["final_pct"])
    legend_parts = []
    for s in normalized_series:
        delta = s["final_pct"] - 100
        delta_cls = "pos" if delta > 0 else ("neg" if delta < 0 else "")
        legend_parts.append(
            f'<li>'
            f'<span class="pl-swatch" style="background:{s["color"]}"></span>'
            f'<span class="pl-label">{s["label"]}</span>'
            f'<span class="pl-val {delta_cls}">{delta:+.0f}% · '
            f'{s["final_inv"]:,}</span>'
            f'</li>'
        )
    legend_html = "".join(legend_parts)

    return svg, legend_html


def render_trajectory_svg(traj: list[dict], width: int = 780, height: int = 220) -> str:
    """Return an inline SVG line chart of inventory over time, with harvest
    magnitude shown as red bars beneath each point."""
    if not traj:
        return ""
    pad_l, pad_r, pad_t, pad_b = 52, 12, 14, 28
    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b
    n = len(traj)
    inv_vals = [r["inventory"] for r in traj]
    harv_vals = [r["harvested"] for r in traj]
    # Y axis starts at 0 — we're showing absolute scale, not just the change.
    inv_min = 0
    inv_max = max(inv_vals)
    inv_span = max(1, inv_max - inv_min)
    harv_max = max(harv_vals) if max(harv_vals) > 0 else 1

    xs = [pad_l + (i * inner_w / (n - 1) if n > 1 else 0) for i in range(n)]
    ys = [
        pad_t + inner_h - ((v - inv_min) / inv_span) * inner_h
        for v in inv_vals
    ]

    # Line path (inventory)
    d = "M " + " L ".join(f"{x:.1f},{y:.1f}" for x, y in zip(xs, ys))
    # Area fill under line
    d_fill = (
        f"M {xs[0]:.1f},{pad_t + inner_h:.1f} "
        + " ".join(f"L {x:.1f},{y:.1f}" for x, y in zip(xs, ys))
        + f" L {xs[-1]:.1f},{pad_t + inner_h:.1f} Z"
    )

    # Harvest bars (narrow red bars from baseline up, separately scaled)
    harv_bars = []
    max_bar_h = 40
    bar_w = max(2, inner_w / n - 1)
    for x, h in zip(xs, harv_vals):
        if h <= 0:
            continue
        bh = (h / harv_max) * max_bar_h
        harv_bars.append(
            f'<rect x="{x - bar_w/2:.1f}" y="{pad_t + inner_h - bh:.1f}" '
            f'width="{bar_w:.1f}" height="{bh:.1f}" fill="#b42318" opacity="0.55"/>'
        )

    # Y axis: 4 gridlines with values
    gridlines = []
    y_labels = []
    for frac in (0, 0.25, 0.5, 0.75, 1):
        v = inv_min + inv_span * (1 - frac)
        y = pad_t + inner_h * frac
        gridlines.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width-pad_r}" y2="{y:.1f}" stroke="#e5e7eb" stroke-width="1"/>')
        y_labels.append(f'<text x="{pad_l - 6}" y="{y+3:.1f}" text-anchor="end" font-size="10" fill="#6b7280">{_short_num(v)}</text>')

    # X axis: first, middle, last month labels
    x_labels = []
    for i, label in [(0, traj[0]["month"]), (n // 2, traj[n // 2]["month"]), (n - 1, traj[-1]["month"])]:
        x_labels.append(
            f'<text x="{xs[i]:.1f}" y="{pad_t + inner_h + 14}" text-anchor="middle" '
            f'font-size="10" fill="#6b7280">{label}</text>'
        )

    return (
        f'<svg viewBox="0 0 {width} {height}" class="traj-chart" xmlns="http://www.w3.org/2000/svg">'
        + "".join(gridlines)
        + "".join(y_labels)
        + f'<path d="{d_fill}" fill="#0b5ed7" opacity="0.10"/>'
        + f'<path d="{d}" stroke="#0b5ed7" stroke-width="2" fill="none"/>'
        + "".join(harv_bars)
        + "".join(x_labels)
        + "</svg>"
    )


def _short_num(v: float) -> str:
    v = float(v)
    if v >= 1_000_000:
        return f"{v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v/1_000:.0f}K"
    return f"{int(v)}"


def _name_for(pfx: str) -> str:
    if pfx in {"DISC", "SPARE"}:
        return pfx
    docs = RESPORGS_BY_PREFIX.get(pfx, [])
    if docs:
        return docs[0].get("title") or pfx
    return pfx


# rpfx -> [group_title, ...] (built once at import, refreshed via _refresh_group_index())
_GROUP_INDEX: dict[str, list[str]] = {}


def _refresh_group_index():
    _GROUP_INDEX.clear()
    id_to_info: dict[str, tuple[str, str]] = {}
    for g in GROUP_DOCS:
        gid = g["_id"].removeprefix("drafts.")
        slug = (g.get("slug") or {}).get("current", "")
        id_to_info[gid] = (g.get("title", ""), slug)
    # Sanity-declared memberships
    for d in RESPORG_DOCS:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) < 2:
            continue
        pfx = code[:2]
        for gref in d.get("groups", []) or []:
            gid = gref.get("_ref")
            if gid in id_to_info:
                title, _ = id_to_info[gid]
                _GROUP_INDEX.setdefault(pfx, []).append(title)
    # Apply overrides (Flotrax → Primetel, etc.)
    for slug, overrides in GROUP_OVERRIDES.items():
        title_for_slug = next(
            (t for (t, s) in id_to_info.values() if s == slug), None,
        )
        if not title_for_slug:
            continue
        for pfx in overrides:
            names = _GROUP_INDEX.setdefault(pfx, [])
            if title_for_slug not in names:
                names.append(title_for_slug)


_refresh_group_index()


# ============================================================
# Hidden-rpfx index — Sanity category slug 'hidden' + HIDDEN_OVERRIDE
# ============================================================

HIDDEN_RPFX: set[str] = set()


def _refresh_hidden_index():
    HIDDEN_RPFX.clear()
    HIDDEN_RPFX.update(HIDDEN_OVERRIDE)
    hidden_cat_id = next(
        (
            c["_id"].removeprefix("drafts.")
            for c in CATEGORY_DOCS
            if (c.get("slug") or {}).get("current") == "hidden"
        ),
        None,
    )
    if not hidden_cat_id:
        return
    for d in RESPORG_DOCS:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) < 2:
            continue
        for cref in d.get("categories", []) or []:
            if cref.get("_ref") == hidden_cat_id:
                HIDDEN_RPFX.add(code[:2])
                break


_refresh_hidden_index()


def _is_hidden(pfx: str) -> bool:
    return pfx in HIDDEN_RPFX


def _name_with_group(pfx: str) -> str:
    """'Mayfair Communication (Primetel)' or just 'Bandwidth' when no group."""
    if pfx in {"DISC", "SPARE"}:
        return pfx
    base = _name_for(pfx)
    groups = _GROUP_INDEX.get(pfx)
    if groups:
        # If name already IS the group name (e.g. 'Lumen Technologies'), skip the ()
        unique = [g for g in groups if g.lower() != base.lower()]
        if unique:
            return f"{base} ({', '.join(unique)})"
    return base


# ============================================================
# Routes
# ============================================================

def _directory_rows():
    """Shared: full sorted list of RespOrgs with enrichment."""
    con = duckdb.connect()
    month = _latest_month()
    curr = (CACHE / f"{month}.parquet").as_posix()
    rows = con.execute(
        f"""
        SELECT rpfx, COUNT(*) AS n
        FROM read_parquet('{curr}') GROUP BY rpfx ORDER BY n DESC
        """
    ).fetchall()
    enr = dict(
        (r[0], r[1:])
        for r in con.execute(
            f"SELECT rpfx, working_count, mm_count, median_age_months "
            f"FROM read_parquet('{(DATA/'enrichment_current.parquet').as_posix()}')"
        ).fetchall()
    )
    directory = []
    for rpfx, n in rows:
        if _is_hidden(rpfx):
            continue
        docs = RESPORGS_BY_PREFIX.get(rpfx, [])
        title = docs[0].get("title") if docs else None
        logo = None
        if docs and docs[0].get("logoImage"):
            logo = asset_url(docs[0].get("logoImage"))
        wc_mc_age = enr.get(rpfx)
        mm_pct = (wc_mc_age[1] / wc_mc_age[0]) if wc_mc_age and wc_mc_age[0] else None
        directory.append({
            "rpfx": rpfx, "title": title or "(not in directory)",
            "logo": logo,
            "inventory": n,
            "mm_pct": mm_pct,
            "median_age": wc_mc_age[2] if wc_mc_age else None,
        })
    return directory, month


def _category_summaries(limit: int | None = None):
    """Per-category summary with one-liner descriptions from Sanity."""
    directory, _ = _directory_rows()
    inv_by_rpfx = {d["rpfx"]: d["inventory"] for d in directory}
    summary = []
    for c in CATEGORY_DOCS:
        slug = (c.get("slug") or {}).get("current")
        if not slug:
            continue
        cat_id = c["_id"].removeprefix("drafts.")
        members = 0
        inventory = 0
        for d in RESPORG_DOCS:
            for cref in d.get("categories", []) or []:
                if cref.get("_ref") == cat_id:
                    code = (d.get("codeTwoDigit") or "").strip().upper()
                    if len(code) >= 2 and not _is_hidden(code[:2]):
                        members += 1
                        inventory += inv_by_rpfx.get(code[:2], 0)
                    break
        summary.append({
            "slug": slug,
            "title": c.get("title"),
            "description": c.get("description"),
            "members": members,
            "inventory": inventory,
        })
    summary.sort(key=lambda x: -x["inventory"])
    if limit:
        summary = summary[:limit]
    return summary


def _group_summaries(limit: int | None = None):
    con = duckdb.connect()
    month = _latest_month()
    curr = (CACHE / f"{month}.parquet").as_posix()
    resporg_month = (DATA / "resporg_month.parquet").as_posix()
    inv_by_rpfx = dict(
        con.execute(
            f"SELECT rpfx, COUNT(*) FROM read_parquet('{curr}') GROUP BY rpfx"
        ).fetchall()
    )
    out = []
    for g in GROUP_DOCS:
        gid = g["_id"].removeprefix("drafts.")
        slug = (g.get("slug") or {}).get("current")
        if not slug:
            continue
        pfxs = _group_members(gid, slug)
        if not pfxs:
            continue
        inventory = sum(inv_by_rpfx.get(p, 0) for p in pfxs)
        pfx_list = "(" + ",".join(f"'{p}'" for p in pfxs) + ")"
        agg = con.execute(
            f"SELECT SUM(acquired), SUM(harvested_cross_rpfx) "
            f"FROM read_parquet('{resporg_month}') WHERE rpfx IN {pfx_list}"
        ).fetchone()
        total_acq = agg[0] or 0
        total_harv = agg[1] or 0
        opp = (total_harv / total_acq) if total_acq else 0
        out.append({
            "slug": slug, "title": g.get("title"),
            "description": g.get("description", ""),
            "members": len(pfxs), "inventory": inventory, "opp_idx": opp,
        })
    out.sort(key=lambda x: -x["inventory"])
    if limit:
        out = out[:limit]
    return out


@app.route("/")
def index():
    directory, month = _directory_rows()
    top20 = directory[:20]
    total_inventory = sum(d["inventory"] for d in directory)
    return render_template(
        "index.html",
        top20=top20,
        categories=_category_summaries(),
        top_groups=_group_summaries(limit=10),
        month=month,
        total_resporgs=len(directory),
        total_inventory=total_inventory,
    )


@app.route("/directory")
def directory_page():
    directory, month = _directory_rows()
    return render_template(
        "directory.html",
        directory=directory,
        month=month,
        total_resporgs=len(directory),
        total_inventory=sum(d["inventory"] for d in directory),
    )


@app.route("/faq")
def faq():
    return render_template("faq.html",
                           month=_latest_month(),
                           total_resporgs=len(RESPORG_DOCS))


@app.route("/transferring")
def transferring():
    return render_template("transferring.html",
                           month=_latest_month(),
                           total_resporgs=len(RESPORG_DOCS))


@app.route("/r/<rpfx>")
def profile(rpfx):
    data = build_profile(rpfx)
    if data is None:
        abort(404)
    return render_template("profile.html", **data)


@app.route("/assets/<path:filename>")
def assets(filename):
    return send_from_directory(ASSET_ROOT, filename)


def _group_members(group_id: str, slug: str | None = None) -> list[str]:
    """Return 2-char prefixes for every resporg belonging to this group.

    Honors GROUP_OVERRIDES so we can publicly connect known shells without
    waiting for a Sanity round-trip.
    """
    pfxs = set()
    for d in RESPORG_DOCS:
        for gref in d.get("groups", []) or []:
            if gref.get("_ref") == group_id:
                code = (d.get("codeTwoDigit") or "").strip().upper()
                if len(code) >= 2:
                    pfxs.add(code[:2])
    if slug and slug in GROUP_OVERRIDES:
        pfxs.update(GROUP_OVERRIDES[slug])
    # Strip hidden rpfxs — they don't exist as far as the public site is concerned
    pfxs = {p for p in pfxs if not _is_hidden(p)}
    return sorted(pfxs)


@app.route("/groups")
def groups_index():
    con = duckdb.connect()
    month = _latest_month()
    curr = (CACHE / f"{month}.parquet").as_posix()
    inv_by_rpfx = dict(
        con.execute(
            f"SELECT rpfx, COUNT(*) FROM read_parquet('{curr}') GROUP BY rpfx"
        ).fetchall()
    )
    resporg_month = (DATA / "resporg_month.parquet").as_posix()
    group_summary = []
    for g in GROUP_DOCS:
        gid = g["_id"].removeprefix("drafts.")
        if gid in {gg["_id"].removeprefix("drafts.") for gg in GROUP_DOCS}:
            pass
        slug = (g.get("slug") or {}).get("current")
        if not slug:
            continue
        pfxs = _group_members(gid, slug)
        if not pfxs:
            continue
        inventory = sum(inv_by_rpfx.get(p, 0) for p in pfxs)
        # Aggregate Opp.Idx
        pfx_list = "(" + ",".join(f"'{p}'" for p in pfxs) + ")"
        agg = con.execute(
            f"""
            SELECT SUM(acquired), SUM(harvested_cross_rpfx)
            FROM read_parquet('{resporg_month}') WHERE rpfx IN {pfx_list}
            """
        ).fetchone()
        total_acq = agg[0] or 0
        total_harv = agg[1] or 0
        opp_idx = (total_harv / total_acq) if total_acq else 0
        group_summary.append({
            "slug": slug,
            "title": g.get("title"),
            "description": g.get("description", ""),
            "members": len(pfxs),
            "inventory": inventory,
            "opp_idx": opp_idx,
        })
    # Also flag groups in Sanity that have zero resolvable members
    # (so Bill can see what's orphaned)
    group_summary.sort(key=lambda x: -x["inventory"])
    return render_template(
        "groups.html",
        groups=group_summary,
        month=month,
        total_resporgs=len(RESPORG_DOCS),
    )


@app.route("/group/<slug>")
def group_page(slug):
    grp = next(
        (g for g in GROUP_DOCS if (g.get("slug") or {}).get("current") == slug),
        None,
    )
    if not grp:
        abort(404)
    gid = grp["_id"].removeprefix("drafts.")

    con = duckdb.connect()
    month = _latest_month()
    curr = (CACHE / f"{month}.parquet").as_posix()
    resporg_month = (DATA / "resporg_month.parquet").as_posix()
    enr_path = (DATA / "enrichment_current.parquet").as_posix()

    pfxs = _group_members(gid, slug)

    # Member rows
    inv_by_rpfx = dict(
        con.execute(
            f"SELECT rpfx, COUNT(*) FROM read_parquet('{curr}') GROUP BY rpfx"
        ).fetchall()
    )
    enrich_by_rpfx = {
        r[0]: r
        for r in con.execute(
            f"SELECT rpfx, working_count, mm_count, median_age_months "
            f"FROM read_parquet('{enr_path}')"
        ).fetchall()
    }
    # Build member rows with names
    member_rows = []
    for p in pfxs:
        docs = RESPORGS_BY_PREFIX.get(p, [])
        title = docs[0].get("title") if docs else None
        enr = enrich_by_rpfx.get(p, (p, 0, 0, None))
        mm_pct = (enr[2] / enr[1]) if enr[1] else None
        member_rows.append({
            "rpfx": p,
            "title": title,
            "inventory": inv_by_rpfx.get(p, 0),
            "mm_pct": mm_pct,
            "median_age": enr[3],
        })
    member_rows.sort(key=lambda x: -x["inventory"])

    total_inventory = sum(m["inventory"] for m in member_rows)

    # Aggregate trajectory
    if pfxs:
        pfx_list = "(" + ",".join(f"'{p}'" for p in pfxs) + ")"
        traj_rows = con.execute(
            f"""
            SELECT month, SUM(inventory) AS inv, SUM(acquired), SUM(lost),
                   SUM(harvested_cross_rpfx), SUM(appeared_from_spare),
                   SUM(disappeared_to_spare)
            FROM read_parquet('{resporg_month}')
            WHERE rpfx IN {pfx_list}
            GROUP BY month ORDER BY month
            """
        ).fetchall()
        agg = con.execute(
            f"""
            SELECT SUM(acquired), SUM(harvested_cross_rpfx), SUM(lost)
            FROM read_parquet('{resporg_month}') WHERE rpfx IN {pfx_list}
            """
        ).fetchone()
        total_acq = agg[0] or 0
        total_harv = agg[1] or 0
    else:
        traj_rows = []
        total_acq = total_harv = 0

    opp_idx = (total_harv / total_acq) if total_acq else 0
    trajectory = [
        {"month": r[0], "inventory": r[1], "acquired": r[2], "lost": r[3],
         "harvested": r[4], "from_spare": r[5], "to_spare": r[6]}
        for r in traj_rows
    ]
    if trajectory:
        first_inv = trajectory[0]["inventory"]
        last_inv = trajectory[-1]["inventory"]
        delta = last_inv - first_inv
        pct = (delta / first_inv * 100) if first_inv else 0
    else:
        first_inv = last_inv = delta = pct = 0

    # Group-level flow aggregates (sources and destinations OUTSIDE the group)
    top_inbound = top_outbound = top_harvest_from = []
    flow_graph = DATA / "flow_graph.parquet"
    if flow_graph.exists() and pfxs:
        flow_path = flow_graph.as_posix()
        pfx_list = "(" + ",".join(f"'{p}'" for p in pfxs) + ")"
        top_inbound = con.execute(
            f"""SELECT from_node, SUM(n) AS n FROM read_parquet('{flow_path}')
                WHERE to_node IN {pfx_list} AND edge_type = 'TRANSFER'
                  AND from_node NOT IN {pfx_list}
                GROUP BY from_node ORDER BY n DESC LIMIT 12"""
        ).fetchall()
        top_outbound = con.execute(
            f"""SELECT to_node, SUM(n) AS n FROM read_parquet('{flow_path}')
                WHERE from_node IN {pfx_list} AND edge_type = 'TRANSFER'
                  AND to_node NOT IN {pfx_list}
                GROUP BY to_node ORDER BY n DESC LIMIT 12"""
        ).fetchall()
        top_harvest_from = con.execute(
            f"""SELECT prev_rpfx, SUM(n) AS n FROM read_parquet('{flow_path}')
                WHERE to_node IN {pfx_list} AND edge_type = 'HARVEST'
                  AND prev_rpfx IS NOT NULL AND prev_rpfx NOT IN {pfx_list}
                GROUP BY prev_rpfx ORDER BY n DESC LIMIT 12"""
        ).fetchall()

    # Industry rank by combined inventory (vs other groups) — plus data for a
    # rank bar chart showing every group with this one highlighted.
    rank_row = None
    all_groups_inventory = []
    for gg in GROUP_DOCS:
        gg_slug = (gg.get("slug") or {}).get("current")
        gpfxs = _group_members(gg["_id"].removeprefix("drafts."), gg_slug)
        if gpfxs:
            all_groups_inventory.append(
                (gg, sum(inv_by_rpfx.get(p, 0) for p in gpfxs))
            )
    all_groups_inventory.sort(key=lambda x: -x[1])
    rank_chart = []
    max_inv = all_groups_inventory[0][1] if all_groups_inventory else 1
    for i, (gg, inv_v) in enumerate(all_groups_inventory, start=1):
        rank_chart.append(
            {
                "rank": i,
                "title": gg.get("title"),
                "slug": (gg.get("slug") or {}).get("current"),
                "inventory": inv_v,
                "pct": inv_v / max_inv if max_inv else 0,
                "is_self": gg["_id"].removeprefix("drafts.") == gid,
            }
        )
        if gg["_id"].removeprefix("drafts.") == gid:
            rank_row = (i, len(all_groups_inventory))
    inv_rank_label = percentile_label(*rank_row) if rank_row else None

    # NPA bar chart: combined inventory per toll-free prefix
    if pfxs:
        pfx_list = "(" + ",".join(f"'{p}'" for p in pfxs) + ")"
        npa_rows = con.execute(
            f"""
            SELECT prefix, COUNT(*) AS n
            FROM read_parquet('{curr}')
            WHERE rpfx IN {pfx_list}
            GROUP BY prefix
            """
        ).fetchall()
        npa_by = {int(p): n for p, n in npa_rows}
    else:
        npa_by = {}
    max_npa = max(npa_by.values()) if npa_by else 1
    npa_chart = [
        {
            "npa": p,
            "count": npa_by.get(p, 0),
            "pct": (npa_by.get(p, 0) / max_npa) if max_npa else 0,
        }
        for p in NPA_PREFIXES
    ]

    # Build member-circles data with proportional sizing (sqrt of inventory)
    import math
    max_member_inv = member_rows[0]["inventory"] if member_rows else 1
    circles = []
    for m in member_rows:
        docs = RESPORGS_BY_PREFIX.get(m["rpfx"], [])
        logo = None
        if docs and docs[0].get("logoImage"):
            logo = asset_url(docs[0].get("logoImage"))
        ratio = math.sqrt(m["inventory"] / max_member_inv) if max_member_inv else 0
        size = int(30 + ratio * 90)  # 30–120px
        circles.append({
            "rpfx": m["rpfx"],
            "title": m["title"],
            "inventory": m["inventory"],
            "size": size,
            "logo": logo,
        })

    # Dedupe shared logos across the group — when several members use the
    # same logoImage (common for shell-network groups like Primetel where
    # 18 RespOrg codes all reference one mascot), only keep the logo on:
    #   1. The single largest-inventory holder of that logo
    #   2. Any member whose title matches the group's title (case-insensitive)
    # Everyone else falls back to their 2-char prefix badge.
    logo_counts: dict[str, int] = {}
    logo_biggest: dict[str, str] = {}  # logo_url -> rpfx (first/biggest seen)
    for c in circles:  # already sorted by inventory desc
        if c["logo"]:
            logo_counts[c["logo"]] = logo_counts.get(c["logo"], 0) + 1
            logo_biggest.setdefault(c["logo"], c["rpfx"])

    # The group's "common" logo = the most-used logoImage among members.
    # Extend it to any member whose title matches the group name but has no
    # logo of its own (e.g. PI "PrimeTel" which shares Primetel's identity).
    group_logo = max(logo_counts, key=logo_counts.get) if logo_counts else None
    group_title_lower = (grp.get("title") or "").strip().lower()
    if group_logo:
        for c in circles:
            if not c["logo"] and (c["title"] or "").strip().lower() == group_title_lower:
                c["logo"] = group_logo
                # count it so dedupe logic treats it as legitimately shared
                logo_counts[group_logo] = logo_counts.get(group_logo, 0) + 1

    for c in circles:
        if c["logo"] and logo_counts[c["logo"]] > 1:
            title_matches = (c["title"] or "").strip().lower() == group_title_lower
            is_biggest = c["rpfx"] == logo_biggest[c["logo"]]
            if not (title_matches or is_biggest):
                c["logo"] = None

    return render_template(
        "group.html",
        slug=slug,
        title=grp.get("title"),
        description=grp.get("description"),
        body=portable_text_to_plain(grp.get("body")),
        website=grp.get("website"),
        logo=asset_url(grp.get("logoImage")),
        screenshot=asset_url(grp.get("websiteScreenshot")),
        members=member_rows,
        total_inventory=total_inventory,
        inv_rank_label=inv_rank_label,
        opp_idx=opp_idx,
        total_acq=total_acq,
        total_harv=total_harv,
        delta=delta, pct_change=pct,
        trajectory=trajectory,
        trajectory_svg=render_trajectory_svg(trajectory),
        top_inbound=[(s, n, _name_with_group(s)) for s, n in top_inbound],
        top_outbound=[(d, n, _name_with_group(d)) for d, n in top_outbound],
        npa_chart=npa_chart,
        rank_chart=rank_chart,
        circles=circles,
        month=month,
        total_resporgs=len(RESPORG_DOCS),
    )


@app.route("/categories")
def categories_index():
    """List every category with member count + combined inventory."""
    con = duckdb.connect()
    month = _latest_month()
    curr = (CACHE / f"{month}.parquet").as_posix()
    inv_by_rpfx = dict(
        con.execute(
            f"SELECT rpfx, COUNT(*) FROM read_parquet('{curr}') GROUP BY rpfx"
        ).fetchall()
    )
    enrich = dict(
        (r[0], r[1:])
        for r in con.execute(
            f"SELECT rpfx, working_count, mm_count, median_age_months "
            f"FROM read_parquet('{(DATA/'enrichment_current.parquet').as_posix()}')"
        ).fetchall()
    )
    # Aggregate per-category
    cat_summary = []
    for c in CATEGORY_DOCS:
        slug = (c.get("slug") or {}).get("current")
        if not slug:
            continue
        cat_id = c["_id"].removeprefix("drafts.")
        members = set()
        for d in RESPORG_DOCS:
            for cref in d.get("categories", []) or []:
                if cref.get("_ref") == cat_id:
                    code = (d.get("codeTwoDigit") or "").strip().upper()
                    if len(code) >= 2 and not _is_hidden(code[:2]):
                        members.add(code[:2])
        total_inv = sum(inv_by_rpfx.get(p, 0) for p in members)
        working = sum(enrich.get(p, (0, 0, 0))[0] for p in members)
        mm = sum(enrich.get(p, (0, 0, 0))[1] for p in members)
        cat_summary.append(
            {
                "slug": slug,
                "title": c.get("title"),
                "description": c.get("description"),
                "members": len(members),
                "inventory": total_inv,
                "mm_pct": (mm / working) if working else None,
                "image": asset_url(c.get("image")),
            }
        )
    cat_summary.sort(key=lambda x: -x["inventory"])

    # Growth chart — read category_trajectories.parquet if present
    growth_svg = ""
    growth_legend = ""
    traj_path = DATA / "category_trajectories.parquet"
    if traj_path.exists():
        traj_rows = con.execute(
            f"""
            SELECT category_slug, category_title, month, inventory
            FROM read_parquet('{traj_path.as_posix()}')
            ORDER BY category_slug, month
            """
        ).fetchall()
        by_slug: dict[str, dict] = {}
        for slug, title, m, inv in traj_rows:
            by_slug.setdefault(
                slug, {"label": title, "points": []}
            )["points"].append((m, inv))
        # Assign colors deterministically by alphabetical slug order
        series = []
        for i, slug in enumerate(sorted(by_slug)):
            s = by_slug[slug]
            s["color"] = CATEGORY_PALETTE[i % len(CATEGORY_PALETTE)]
            series.append(s)
        growth_svg, growth_legend = render_multi_line_svg(series)

    # Size pie — current inventory share across all categories
    size_slices = []
    total_across = sum(c["inventory"] for c in cat_summary) or 1
    for i, c in enumerate(cat_summary):
        if c["inventory"] <= 0:
            continue
        size_slices.append(
            {
                "slug": c["slug"],
                "label": c["title"],
                "value": c["inventory"],
                "color": CATEGORY_PALETTE[i % len(CATEGORY_PALETTE)],
                "pct": c["inventory"] / total_across,
            }
        )
    size_pie_svg = render_pie_svg(size_slices, size=240)

    return render_template(
        "categories.html",
        categories=cat_summary,
        month=month,
        growth_svg=growth_svg,
        growth_legend=growth_legend,
        size_pie_svg=size_pie_svg,
        size_pie_slices=size_slices,
        total_resporgs=len(RESPORG_DOCS),
    )


@app.route("/category/<slug>")
def category_page(slug):
    cat = next(
        (c for c in CATEGORY_DOCS if (c.get("slug") or {}).get("current") == slug),
        None,
    )
    if not cat:
        abort(404)
    cat_id = cat["_id"].removeprefix("drafts.")

    con = duckdb.connect()
    month = _latest_month()
    curr = (CACHE / f"{month}.parquet").as_posix()
    resporg_month = (DATA / "resporg_month.parquet").as_posix()

    # Members with current inventory
    inv_by_rpfx = dict(
        con.execute(
            f"SELECT rpfx, COUNT(*) FROM read_parquet('{curr}') GROUP BY rpfx"
        ).fetchall()
    )
    enrich_by_rpfx = {
        r[0]: r
        for r in con.execute(
            f"SELECT rpfx, working_count, mm_count, median_age_months "
            f"FROM read_parquet('{(DATA/'enrichment_current.parquet').as_posix()}')"
        ).fetchall()
    }

    sv_dir = ROOT / "webapp" / "static" / "streetview"

    def _thumb_for(pfx: str, doc: dict) -> tuple[str | None, str]:
        """Return (url, kind) where kind is 'logo' | 'street' | 'satellite' | 'none'."""
        if doc.get("logoImage"):
            return asset_url(doc.get("logoImage")), "logo"
        if (sv_dir / f"{pfx}-street.jpg").exists():
            return f"/static/streetview/{pfx}-street.jpg", "street"
        if (sv_dir / f"{pfx}-satellite.jpg").exists():
            return f"/static/streetview/{pfx}-satellite.jpg", "satellite"
        return None, "none"

    member_rows = []
    member_pfxs = set()
    for d in RESPORG_DOCS:
        for cref in d.get("categories", []) or []:
            if cref.get("_ref") == cat_id:
                code = (d.get("codeTwoDigit") or "").strip().upper()
                if len(code) < 2:
                    continue
                pfx = code[:2]
                if pfx in member_pfxs or _is_hidden(pfx):
                    continue
                member_pfxs.add(pfx)
                enr = enrich_by_rpfx.get(pfx, (pfx, 0, 0, None))
                mm_pct = (enr[2] / enr[1]) if enr[1] else None
                thumb_url, thumb_kind = _thumb_for(pfx, d)
                member_rows.append(
                    {
                        "rpfx": pfx,
                        "title": d.get("title"),
                        "inventory": inv_by_rpfx.get(pfx, 0),
                        "mm_pct": mm_pct,
                        "median_age": enr[3],
                        "thumb": thumb_url,
                        "thumb_kind": thumb_kind,
                    }
                )
                break
    member_rows.sort(key=lambda x: -x["inventory"])

    # Cumulative Opportunism Index for the category (members only)
    if member_pfxs:
        pfx_list = "(" + ",".join(f"'{p}'" for p in member_pfxs) + ")"
        agg = con.execute(
            f"""
            SELECT SUM(acquired), SUM(harvested_cross_rpfx), SUM(lost)
            FROM read_parquet('{resporg_month}')
            WHERE rpfx IN {pfx_list}
            """
        ).fetchone()
        total_acq, total_harv, total_lost = agg
        cat_opp_idx = (total_harv / total_acq) if total_acq else 0

        # Trajectory: sum inventory over members per month
        traj = con.execute(
            f"""
            SELECT month, SUM(inventory) AS inv, SUM(acquired), SUM(lost),
                   SUM(harvested_cross_rpfx)
            FROM read_parquet('{resporg_month}')
            WHERE rpfx IN {pfx_list}
            GROUP BY month ORDER BY month
            """
        ).fetchall()
    else:
        total_acq = total_harv = total_lost = 0
        cat_opp_idx = 0
        traj = []

    if traj:
        first_m, first_inv = traj[0][0], traj[0][1]
        last_m, last_inv = traj[-1][0], traj[-1][1]
        delta = last_inv - first_inv
        pct = (delta / first_inv * 100) if first_inv else 0
    else:
        first_m = last_m = month
        first_inv = last_inv = delta = pct = 0

    # Member pie — top 8 + Others bucket when more members exist
    member_pie_slices = []
    if member_rows:
        TOP_N = 8
        for i, m in enumerate(member_rows[:TOP_N]):
            if m["inventory"] <= 0:
                continue
            member_pie_slices.append(
                {
                    "label": m["title"] or m["rpfx"],
                    "value": m["inventory"],
                    "color": CATEGORY_PALETTE[i],
                    "rpfx": m["rpfx"],
                }
            )
        overflow = [m for m in member_rows[TOP_N:] if m["inventory"] > 0]
        if overflow:
            member_pie_slices.append(
                {
                    "label": f"Others ({len(overflow)} RespOrgs)",
                    "value": sum(m["inventory"] for m in overflow),
                    "color": "#9ca3af",
                    "rpfx": None,
                }
            )
    # Give each slice its pct for the legend
    pie_total = sum(s["value"] for s in member_pie_slices) or 1
    for s in member_pie_slices:
        s["pct"] = s["value"] / pie_total
    member_pie_svg = render_pie_svg(member_pie_slices, size=220) if member_pie_slices else ""

    cat_image = asset_url(cat.get("image"))
    return render_template(
        "category.html",
        slug=slug,
        title=cat.get("title"),
        description=cat.get("description"),
        body=portable_text_to_plain(cat.get("body")),
        image=cat_image,
        members=member_rows,
        member_pie_svg=member_pie_svg,
        member_pie_slices=member_pie_slices,
        total_inventory=sum(m["inventory"] for m in member_rows),
        cat_opp_idx=cat_opp_idx,
        total_acq=total_acq,
        total_harv=total_harv,
        first_m=first_m, first_inv=first_inv,
        last_m=last_m, last_inv=last_inv,
        delta=delta, pct_change=pct,
        trajectory=[
            {"month": r[0], "inventory": r[1], "acquired": r[2],
             "lost": r[3], "harvested": r[4]}
            for r in traj
        ],
        month=month,
        total_resporgs=len(RESPORG_DOCS),
    )


def _digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _letters_to_digits(s: str) -> str:
    """Convert a vanity phone like '1-800-FLOWERS' to pure digits '18003569377'."""
    mapping = str.maketrans(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
        "22233344455566677778889999" "22233344455566677778889999",
    )
    translated = s.translate(mapping)
    return _digits_only(translated)


def _normalize_tfn(raw: str) -> str | None:
    """Accept a toll-free number in many formats, return a 10-digit string.

    Handles: '8003569377', '1-800-356-9377', '800-3569377', '1-800-FLOWERS',
             '+1 (800) 356-9377', etc.  Returns None if result isn't a
             valid 10-digit toll-free.
    """
    if not raw:
        return None
    d = _letters_to_digits(raw)
    if d.startswith("1") and len(d) == 11:
        d = d[1:]
    if len(d) != 10:
        return None
    if d[:3] not in {"800", "833", "844", "855", "866", "877", "888"}:
        return None
    return d


@app.route("/search")
def search():
    q = (request.args.get("q") or "").strip()
    if not q:
        return render_template("search.html", q="", results=[])

    # If it looks like a phone number (vanity or digit), route to number lookup
    tfn = _normalize_tfn(q)
    if tfn:
        return redirect(url_for("number_lookup", tfn=tfn))

    ql = q.lower()
    results = []
    seen = set()
    for d in RESPORG_DOCS:
        title = (d.get("title") or "").lower()
        alias = (d.get("alias") or "").lower()
        code = (d.get("codeTwoDigit") or "").lower()
        if ql in title or ql in alias or ql in code:
            key = (d.get("codeTwoDigit") or "")[:2].upper()
            if key in seen or _is_hidden(key):
                continue
            seen.add(key)
            results.append({
                "rpfx": key,
                "title": d.get("title"),
                "alias": d.get("alias"),
            })
    # Also try groups / categories by name
    group_results = []
    for g in GROUP_DOCS:
        t = (g.get("title") or "").lower()
        if ql in t:
            group_results.append({
                "slug": (g.get("slug") or {}).get("current"),
                "title": g.get("title"),
            })
    cat_results = []
    for c in CATEGORY_DOCS:
        t = (c.get("title") or "").lower()
        if ql in t:
            cat_results.append({
                "slug": (c.get("slug") or {}).get("current"),
                "title": c.get("title"),
            })
    return render_template(
        "search.html", q=q, results=results,
        group_results=group_results, cat_results=cat_results,
    )


# ============================================================
# Number lookup
# ============================================================

@app.route("/number")
@app.route("/number/<tfn>")
def number_lookup(tfn: str | None = None):
    # If user submitted the form to /number?q=..., redirect to the
    # canonical path-based URL /number/<tfn> so every valid number has a
    # single static-looking URL.
    if tfn is None:
        q = request.args.get("q") or ""
        normalized = _normalize_tfn(q) if q else None
        if normalized:
            return redirect(url_for("number_lookup", tfn=normalized), code=302)
        return render_template(
            "number_lookup.html", raw=q, result=None,
            month=_latest_month(), total_resporgs=len(RESPORG_DOCS),
        )

    raw = tfn
    normalized = _normalize_tfn(raw)
    if not normalized:
        return render_template(
            "number_lookup.html", raw=raw, result=None,
            month=_latest_month(), total_resporgs=len(RESPORG_DOCS),
        )

    # Canonicalize the URL — if the user came in with a vanity form like
    # /number/1-800-FLOWERS, redirect to the pure-digit canonical form
    if tfn != normalized:
        return redirect(url_for("number_lookup", tfn=normalized), code=301)

    num_int = int(normalized)
    con = duckdb.connect()

    # Use DuckDB's filename column to get the month per row — one scan across all
    rows = con.execute(
        f"""
        SELECT regexp_extract(filename, '(\\d{{4}}-\\d{{2}})', 1) AS month,
               rpfx, resporg, status, yy, mm, dd
        FROM read_parquet('{(CACHE).as_posix()}/*.parquet', filename=true)
        WHERE number = {num_int}
        ORDER BY month
        """
    ).fetchall()

    # Try MM match for vanity word
    try:
        con.install_extension("sqlite")
        con.load_extension("sqlite")
    except Exception:
        pass
    try:
        con.execute(f"ATTACH '{MM_DB.as_posix()}' AS mm (TYPE sqlite, READ_ONLY)")
    except Exception:
        pass
    last7 = normalized[3:]
    mm_row = con.execute(
        f"""
        SELECT UPPER(word), category_label
        FROM mm.vanity WHERE digits = '{last7}' LIMIT 1
        """
    ).fetchone()

    # Build the history timeline — collapse consecutive months with same rpfx+status
    timeline = []
    for row in rows:
        month, rpfx, resporg, status, yy, mm, dd = row
        status_name = STATUS_NAMES.get(status, "?")
        if timeline and timeline[-1]["rpfx"] == rpfx and timeline[-1]["status"] == status_name:
            timeline[-1]["end_month"] = month
        else:
            hidden = _is_hidden(rpfx)
            timeline.append({
                "start_month": month,
                "end_month": month,
                "rpfx": rpfx,
                "resporg": resporg,
                "status": status_name,
                "rpfx_name": "(private)" if hidden else _name_for(rpfx),
                "hidden": hidden,
                "last_change": f"20{yy:02d}-{mm:02d}-{dd:02d}" if yy else None,
            })

    # Current state is the last row of snapshot data (or "not currently active")
    if timeline:
        current = timeline[-1]
    else:
        current = None

    formatted = f"{normalized[:3]}-{normalized[3:]}"

    # SEO gate: most number pages should be NOINDEX to avoid swamping search
    # engines with millions of thin pages. A number is flagged "notable" — and
    # therefore indexable — only if it has a strong Master-Million match.
    # Tune the threshold (or add manual overrides) later.
    is_notable = False
    if mm_row and mm_row[0]:
        # Has a vanity word — check strength via mike_rank or blended_score
        rank_row = con.execute(
            f"SELECT COALESCE(mike_rank, 999999), COALESCE(blended_score, 0) "
            f"FROM mm.vanity WHERE digits = '{last7}' LIMIT 1"
        ).fetchone()
        if rank_row:
            mike_rank_val, score = rank_row
            # 800s with a high Mike-rank vanity OR any NPA with a very high score
            is_notable = (
                (normalized.startswith("800") and mike_rank_val <= 5000)
                or score >= 1000
            )

    return render_template(
        "number_lookup.html",
        raw=raw,
        is_notable=is_notable,
        result={
            "normalized": normalized,
            "formatted": formatted,
            "vanity_word": mm_row[0] if mm_row else None,
            "vanity_category": mm_row[1] if mm_row else None,
            "timeline": timeline,
            "current": current,
        },
        month=_latest_month(),
        total_resporgs=len(RESPORG_DOCS),
    )


# ============================================================
# Watch + Ask forms — capture to sqlite for now; email sends later.
# ============================================================

LEADS_DB = DATA / "leads.db"


def _ensure_leads_db():
    c = sqlite3.connect(LEADS_DB)
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS watch_signups (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email TEXT NOT NULL,
          target TEXT NOT NULL,
          comments TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS questions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email TEXT NOT NULL,
          name TEXT,
          subject_rpfx TEXT,
          question TEXT NOT NULL,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS history_requests (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email TEXT NOT NULL,
          name TEXT,
          number TEXT NOT NULL,
          context TEXT,
          subject_rpfx TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    c.commit()
    c.close()


_ensure_leads_db()


@app.route("/watch", methods=["GET", "POST"])
def watch_signup():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip()
        target = (request.form.get("target") or "").strip()
        comments = (request.form.get("comments") or "").strip()
        if email and target:
            c = sqlite3.connect(LEADS_DB)
            c.execute(
                "INSERT INTO watch_signups (email, target, comments) VALUES (?, ?, ?)",
                (email, target, comments),
            )
            c.commit()
            c.close()
            return render_template(
                "watch.html", submitted=True, email=email, target=target,
                month=_latest_month(), total_resporgs=len(RESPORG_DOCS),
            )
    prefill = request.args.get("target") or ""
    return render_template(
        "watch.html", submitted=False, prefill=prefill,
        month=_latest_month(), total_resporgs=len(RESPORG_DOCS),
    )


@app.route("/history-review", methods=["GET", "POST"])
def history_review():
    """Capture requests for a manual full-history review of a specific TFN.

    Data source is Somos's portal (not yet automated). For now this just
    captures the lead into sqlite so Bill can respond manually.
    """
    prefill_number = request.args.get("number") or ""
    prefill_rpfx = (request.args.get("rpfx") or "").upper()
    if request.method == "POST":
        email = (request.form.get("email") or "").strip()
        name = (request.form.get("name") or "").strip()
        number = (request.form.get("number") or "").strip()
        context = (request.form.get("context") or "").strip()
        subject_rpfx = (request.form.get("subject_rpfx") or prefill_rpfx or "").strip()
        if email and number:
            c = sqlite3.connect(LEADS_DB)
            c.execute(
                "INSERT INTO history_requests (email, name, number, context, subject_rpfx) "
                "VALUES (?, ?, ?, ?, ?)",
                (email, name, number, context, subject_rpfx),
            )
            c.commit()
            c.close()
            return render_template(
                "history_review.html", submitted=True, email=email, number=number,
                month=_latest_month(), total_resporgs=len(RESPORG_DOCS),
            )
    return render_template(
        "history_review.html", submitted=False,
        prefill_number=prefill_number, prefill_rpfx=prefill_rpfx,
        month=_latest_month(), total_resporgs=len(RESPORG_DOCS),
    )


@app.route("/ask", methods=["GET", "POST"])
@app.route("/ask/<rpfx>", methods=["GET", "POST"])
def ask_question(rpfx: str | None = None):
    rpfx_upper = (rpfx or "").upper()
    rpfx_title = _name_for(rpfx_upper) if rpfx_upper else None
    if request.method == "POST":
        email = (request.form.get("email") or "").strip()
        name = (request.form.get("name") or "").strip()
        subject_rpfx = (request.form.get("subject_rpfx") or rpfx_upper or "").strip()
        question = (request.form.get("question") or "").strip()
        if email and question:
            c = sqlite3.connect(LEADS_DB)
            c.execute(
                "INSERT INTO questions (email, name, subject_rpfx, question) VALUES (?, ?, ?, ?)",
                (email, name, subject_rpfx, question),
            )
            c.commit()
            c.close()
            return render_template(
                "ask.html", submitted=True, email=email, rpfx=subject_rpfx,
                rpfx_title=_name_for(subject_rpfx) if subject_rpfx else None,
                month=_latest_month(), total_resporgs=len(RESPORG_DOCS),
            )
    return render_template(
        "ask.html", submitted=False, rpfx=rpfx_upper, rpfx_title=rpfx_title,
        month=_latest_month(), total_resporgs=len(RESPORG_DOCS),
    )


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5178
    app.run(host="127.0.0.1", port=port, debug=True)
