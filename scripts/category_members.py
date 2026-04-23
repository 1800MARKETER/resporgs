"""
List the resporg members of a given category slug, sorted by current inventory.

Usage:
    python scripts/category_members.py messaging
    python scripts/category_members.py unknown
"""

from __future__ import annotations
import json
import sys
from collections import defaultdict
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
CACHE = ROOT / "cache"
CLEAN = ROOT / "clean"


def main():
    if len(sys.argv) < 2:
        print("Usage: category_members.py <category_slug>")
        sys.exit(1)
    slug = sys.argv[1].lower()

    cats = json.loads((CLEAN / "resporgCategory.json").read_text(encoding="utf-8"))
    cat_id_for_slug = None
    for c in cats:
        if (c.get("slug") or {}).get("current") == slug:
            cat_id_for_slug = c["_id"].removeprefix("drafts.")
            cat_title = c.get("title", slug)
            break
    if cat_id_for_slug is None:
        print(f"No category with slug={slug!r}. Known slugs:")
        for c in cats:
            print(f"  {(c.get('slug') or {}).get('current')}")
        sys.exit(1)

    resporgs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    members = []
    for d in resporgs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) < 2:
            continue
        pfx = code[:2]
        for cref in d.get("categories", []) or []:
            if cref.get("_ref") == cat_id_for_slug:
                members.append(
                    (pfx, d.get("title", "?"), d.get("alias", "") or "", code)
                )
                break

    # Latest month inventory
    latest_month = sorted(p.stem for p in CACHE.glob("*.parquet"))[-1]
    con = duckdb.connect()
    inv = {
        r[0]: r[1]
        for r in con.execute(
            f"""
            SELECT rpfx, COUNT(*)
            FROM read_parquet('{(CACHE/f"{latest_month}.parquet").as_posix()}')
            GROUP BY rpfx
            """
        ).fetchall()
    }
    # All-time Opportunism Index
    opp_rows = con.execute(
        f"""
        SELECT rpfx,
               SUM(acquired) AS acq,
               SUM(harvested_cross_rpfx) AS harv,
               SUM(lost) AS lost
        FROM read_parquet('{(DATA/"resporg_month.parquet").as_posix()}')
        GROUP BY rpfx
        """
    ).fetchall()
    opp_by_rpfx = {r[0]: r for r in opp_rows}

    print(f"\nCategory: {cat_title} ({slug}) — {len(members)} resporgs")
    print(f"Inventory snapshot: {latest_month}\n")
    print(
        f"{'rpfx':<4}  {'inventory':>11}  {'acq42mo':>10}  "
        f"{'harv42mo':>10}  {'OppIdx':>7}  title"
    )
    print("-" * 95)
    members.sort(key=lambda m: -inv.get(m[0], 0))
    for pfx, title, alias, code in members:
        cur_inv = inv.get(pfx, 0)
        opp_rec = opp_by_rpfx.get(pfx, (pfx, 0, 0, 0))
        acq, harv = opp_rec[1], opp_rec[2]
        opp = (harv / acq) if acq else 0
        label = (title or alias or "?")[:55]
        print(
            f"{pfx:<4}  {cur_inv:>11,}  {acq:>10,}  "
            f"{harv:>10,}  {opp:>6.1%}  {label}"
        )


if __name__ == "__main__":
    main()
