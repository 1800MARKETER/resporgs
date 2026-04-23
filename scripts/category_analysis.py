"""
Aggregate 42-month growth and behavior by resporg CATEGORY (type of phone company).

Answers: "Which category is growing fastest? Who harvests the most? How do
misdial marketers compare to large telcoms?"

Each resporg can have multiple categories; we explode and count each membership
once per category (so a resporg in 2 categories contributes to both totals).
"""

from __future__ import annotations
import json
from collections import defaultdict
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
CLEAN = ROOT / "clean"


def build_rpfx_category_map() -> dict[str, set[str]]:
    """rpfx -> set of category slugs."""
    cats = json.loads((CLEAN / "resporgCategory.json").read_text(encoding="utf-8"))
    cid_to_slug = {
        c["_id"].removeprefix("drafts."): (c.get("slug") or {}).get("current", "unknown")
        for c in cats
    }
    resporgs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    rpfx_cats: dict[str, set[str]] = defaultdict(set)
    for d in resporgs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) < 2:
            continue
        pfx = code[:2]
        for cref in d.get("categories", []) or []:
            cid = cref.get("_ref")
            if cid in cid_to_slug:
                rpfx_cats[pfx].add(cid_to_slug[cid])
    return rpfx_cats


def category_titles() -> dict[str, str]:
    cats = json.loads((CLEAN / "resporgCategory.json").read_text(encoding="utf-8"))
    return {
        (c.get("slug") or {}).get("current", "unknown"): c.get("title", "?")
        for c in cats
    }


def main():
    rpfx_cats = build_rpfx_category_map()
    titles = category_titles()

    # Load per-(rpfx, month) aggregates
    con = duckdb.connect()
    rows = con.execute(f"""
      SELECT month, rpfx, inventory, acquired, lost, harvested_cross_rpfx,
             appeared_from_spare, transfers_in, transfers_out
      FROM read_parquet('{(DATA/"resporg_month.parquet").as_posix()}')
    """).fetchall()

    # Bucket each row into every category the rpfx belongs to
    # cat_totals[category][month] = {inventory: X, acquired: Y, ...}
    per_cat_month: dict[str, dict[str, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )
    uncat = 0
    uncat_rpfxs: set[str] = set()
    for month, rpfx, inv, acq, lost, harv, app, ti, to_ in rows:
        cats_for = rpfx_cats.get(rpfx, set()) or {"(uncategorized)"}
        if "(uncategorized)" in cats_for:
            uncat += 1
            uncat_rpfxs.add(rpfx)
        for cat in cats_for:
            c = per_cat_month[cat][month]
            c["inventory"] += inv
            c["acquired"] += acq
            c["lost"] += lost
            c["harvested"] += harv
            c["from_spare"] += app
            c["transfers_in"] += ti
            c["transfers_out"] += to_

    months_sorted = sorted({m for cat in per_cat_month.values() for m in cat})
    first_m = months_sorted[0]
    last_m = months_sorted[-1]

    # Category-level 4-year summary
    print(f"\n{'='*100}")
    print("CATEGORY SUMMARY — {} through {}".format(first_m, last_m))
    print(f"{'='*100}")
    print(
        f"{'category':<28} {'start_inv':>12} {'end_inv':>12} {'delta':>12} "
        f"{'%chg':>7} {'cumul_harv':>11} {'cumul_acq':>12} {'OppIdx':>8}"
    )
    summary_rows = []
    for cat, by_month in per_cat_month.items():
        start = by_month.get(first_m, {}).get("inventory", 0)
        end = by_month.get(last_m, {}).get("inventory", 0)
        delta = end - start
        pct = (delta / start * 100) if start else 0
        total_harv = sum(d["harvested"] for d in by_month.values())
        total_acq = sum(d["acquired"] for d in by_month.values())
        opp = (total_harv / total_acq) if total_acq else 0
        summary_rows.append(
            (cat, start, end, delta, pct, total_harv, total_acq, opp)
        )

    # Sort by absolute delta (growth) descending
    for cat, start, end, delta, pct, harv, acq, opp in sorted(summary_rows, key=lambda r: -r[3]):
        label = titles.get(cat, cat)[:27]
        print(
            f"{label:<28} {start:>12,} {end:>12,} {delta:>+12,} {pct:>+6.1f}% "
            f"{harv:>11,} {acq:>12,} {opp:>7.2%}"
        )

    # % growth ranking (separate — because absolute is dominated by big categories)
    print(f"\n{'='*100}")
    print(f"CATEGORY GROWTH RATE (%, first to last month; categories with start_inv > 50,000)")
    print(f"{'='*100}")
    print(f"{'category':<28} {'start_inv':>12} {'end_inv':>12} {'%chg':>8}")
    for cat, start, end, delta, pct, *_ in sorted(summary_rows, key=lambda r: -r[4]):
        if start < 50000:
            continue
        label = titles.get(cat, cat)[:27]
        print(f"{label:<28} {start:>12,} {end:>12,} {pct:>+7.1f}%")

    if uncat:
        print(f"\nNote: {len(uncat_rpfxs)} uncategorized rpfxs contributed {uncat} rows.")


if __name__ == "__main__":
    main()
