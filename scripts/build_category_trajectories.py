"""
Precompute per-category monthly inventory totals for the /categories
growth chart.

Each RespOrg can belong to multiple categories (per Sanity); each
membership contributes that rpfx's monthly inventory to its category's
total for that month.

Output: data/category_trajectories.parquet
  category_slug   VARCHAR
  category_title  VARCHAR
  month           VARCHAR
  inventory       BIGINT
"""

from __future__ import annotations
import json
import time
from collections import defaultdict
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
CLEAN = ROOT / "clean"
DATA = ROOT / "data"


def main():
    t0 = time.time()

    # rpfx -> set(category_slug)
    cat_docs = json.loads((CLEAN / "resporgCategory.json").read_text(encoding="utf-8"))
    id_to_slug: dict[str, str] = {}
    slug_to_title: dict[str, str] = {}
    for c in cat_docs:
        cid = c["_id"].removeprefix("drafts.")
        slug = (c.get("slug") or {}).get("current")
        if not slug:
            continue
        id_to_slug[cid] = slug
        slug_to_title[slug] = c.get("title", slug)

    resporg_docs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    rpfx_to_cats: dict[str, set[str]] = defaultdict(set)
    for d in resporg_docs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) < 2:
            continue
        pfx = code[:2]
        for cref in d.get("categories", []) or []:
            cid = cref.get("_ref")
            if cid in id_to_slug:
                rpfx_to_cats[pfx].add(id_to_slug[cid])

    print(f"  {len(rpfx_to_cats):,} rpfxs across {len(slug_to_title):,} categories")

    # Read monthly rpfx inventory from resporg_month.parquet
    con = duckdb.connect()
    rows = con.execute(
        f"""
        SELECT rpfx, month, inventory
        FROM read_parquet('{(DATA / "resporg_month.parquet").as_posix()}')
        """
    ).fetchall()
    print(f"  {len(rows):,} (rpfx, month) inventory rows")

    # Explode rpfx -> category; sum per (category, month)
    per_cat_month: dict[tuple[str, str], int] = defaultdict(int)
    for rpfx, month, inv in rows:
        for slug in rpfx_to_cats.get(rpfx, ()):
            per_cat_month[(slug, month)] += inv

    # Write out
    out_rows = [
        {
            "category_slug": slug,
            "category_title": slug_to_title.get(slug, slug),
            "month": month,
            "inventory": inv,
        }
        for (slug, month), inv in per_cat_month.items()
    ]
    out_rows.sort(key=lambda r: (r["category_slug"], r["month"]))

    tbl = pa.Table.from_pylist(out_rows)
    out = DATA / "category_trajectories.parquet"
    pq.write_table(tbl, out, compression="zstd")
    print(f"  wrote {out.name}: {len(out_rows):,} rows in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
