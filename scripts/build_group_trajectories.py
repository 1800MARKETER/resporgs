"""
Precompute per-group monthly inventory totals for the /groups growth chart.

Each group has a set of member RespOrgs (from Sanity, plus any
GROUP_OVERRIDES baked into the webapp). We sum each member rpfx's
monthly inventory into the group total for each month.

Output: data/group_trajectories.parquet
  group_slug    VARCHAR
  group_title   VARCHAR
  month         VARCHAR
  inventory     BIGINT

Mirrors scripts/build_category_trajectories.py for parity.
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

# Keep in sync with webapp/app.py — known shell-cluster overrides that
# aren't yet reflected in Sanity. Public-facing groups should pick these
# up so the chart matches the membership that's actually rendered.
GROUP_OVERRIDES: dict[str, list[str]] = {
    "primetel": ["AB", "FO", "HU", "JD", "OD", "OQ", "RY"],
}


def main():
    t0 = time.time()

    # group_id -> slug, slug -> title
    grp_docs = json.loads((CLEAN / "resporgGroup.json").read_text(encoding="utf-8"))
    id_to_slug: dict[str, str] = {}
    slug_to_title: dict[str, str] = {}
    for g in grp_docs:
        gid = g["_id"].removeprefix("drafts.")
        slug = (g.get("slug") or {}).get("current")
        if not slug:
            continue
        id_to_slug[gid] = slug
        slug_to_title[slug] = g.get("title", slug)

    # Skip rpfxs flagged hidden / non-resporg via Sanity categories
    cat_docs = json.loads((CLEAN / "resporgCategory.json").read_text(encoding="utf-8"))
    skip_cat_ids = {
        c["_id"].removeprefix("drafts.")
        for c in cat_docs
        if (c.get("slug") or {}).get("current") in {"hidden", "non-resporg"}
    }

    resporg_docs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    rpfx_to_groups: dict[str, set[str]] = defaultdict(set)
    for d in resporg_docs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) < 2:
            continue
        if code[0] == "1":
            continue  # synthetic vendor code
        pfx = code[:2]
        d_cat_ids = {cref.get("_ref") for cref in d.get("categories", []) or []}
        if d_cat_ids & skip_cat_ids:
            continue  # tagged hidden / non-resporg
        for gref in d.get("groups", []) or []:
            gid = gref.get("_ref")
            if gid in id_to_slug:
                rpfx_to_groups[pfx].add(id_to_slug[gid])

    # Layer in GROUP_OVERRIDES — connect known shells without a Sanity round-trip
    for slug, pfxs in GROUP_OVERRIDES.items():
        for p in pfxs:
            rpfx_to_groups[p].add(slug)

    # Drop groups with zero resolvable members so they don't litter the chart
    groups_with_members = {s for slugs in rpfx_to_groups.values() for s in slugs}
    print(f"  {len(rpfx_to_groups):,} rpfxs across {len(groups_with_members):,} groups")

    # Read monthly rpfx inventory
    con = duckdb.connect()
    rows = con.execute(
        f"""
        SELECT rpfx, month, inventory
        FROM read_parquet('{(DATA / "resporg_month.parquet").as_posix()}')
        """
    ).fetchall()
    print(f"  {len(rows):,} (rpfx, month) inventory rows")

    # Explode rpfx -> group; sum per (group, month)
    per_grp_month: dict[tuple[str, str], int] = defaultdict(int)
    for rpfx, month, inv in rows:
        for slug in rpfx_to_groups.get(rpfx, ()):
            per_grp_month[(slug, month)] += inv

    out_rows = [
        {
            "group_slug": slug,
            "group_title": slug_to_title.get(slug, slug),
            "month": month,
            "inventory": inv,
        }
        for (slug, month), inv in per_grp_month.items()
    ]
    out_rows.sort(key=lambda r: (r["group_slug"], r["month"]))

    tbl = pa.Table.from_pylist(out_rows)
    out = DATA / "group_trajectories.parquet"
    pq.write_table(tbl, out, compression="zstd")
    print(f"  wrote {out.name}: {len(out_rows):,} rows in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
