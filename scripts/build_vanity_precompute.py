"""
Precompute per-rpfx vanity holdings.

Profile pages hit two live queries today — both join working numbers against
the 2M-row Master Million sqlite DB. On the droplet that cost 10-17 seconds
per request. This script does the join once and slices it two ways:

  data/vanity_categories.parquet — (rpfx, category_code, category_label, n)
  data/vanity_top.parquet        — (rpfx, category_code, number, word, ord)
      where category_code is NULL for the default "all categories" top 60.

Processes rpfx-by-rpfx to stay inside the droplet's 1.9 GB RAM budget
(materializing the full 9.5M-match join OOMs).
"""

from __future__ import annotations
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
CACHE = ROOT / "cache"
DATA = ROOT / "data"
MM_DB = ROOT.parent / "local-prospector" / "data" / "master_vanity.db"

TOP_N = 60


def main():
    months = sorted(p.stem for p in CACHE.glob("*.parquet"))
    if not months:
        raise SystemExit("No cached months — run cache_months.py first.")
    curr = (CACHE / f"{months[-1]}.parquet").as_posix()

    con = duckdb.connect()
    con.install_extension("sqlite")
    con.load_extension("sqlite")
    con.execute(f"ATTACH '{MM_DB.as_posix()}' AS mm (TYPE sqlite, READ_ONLY)")

    # List of distinct rpfxs with any working inventory
    rpfxs = [
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT rpfx FROM read_parquet('{curr}') WHERE status = 1 ORDER BY rpfx"
        ).fetchall()
    ]
    print(f"Processing {len(rpfxs)} rpfxs...")
    t0 = time.time()

    cats_rows: list[dict] = []
    top_rows: list[dict] = []

    for i, rpfx in enumerate(rpfxs, 1):
        # Join working × vanity just for THIS rpfx — tiny per-rpfx result set.
        # matches_rpfx is at most the working count for this rpfx × hit rate,
        # which is at most a few million and usually much less.
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE matches_rpfx AS
            SELECT
              s.number,
              UPPER(v.word) AS word,
              v.category_code,
              v.category_label,
              COALESCE(v.blended_score, 0)
                * CASE WHEN s.number / 10000000 = 800 THEN 1.05 ELSE 1.0 END AS boosted,
              COALESCE(v.mike_rank, 999999) AS mike_rank
            FROM (
              SELECT number, LPAD((number % 10000000)::VARCHAR, 7, '0') AS last7
              FROM read_parquet('{curr}')
              WHERE rpfx = '{rpfx}' AND status = 1
            ) s
            JOIN mm.vanity v ON s.last7 = v.digits
            WHERE v.word IS NOT NULL
            """
        )

        # Category rollup
        for row in con.execute(
            """
            SELECT category_code, MAX(category_label), COUNT(*)
            FROM matches_rpfx
            WHERE category_code IS NOT NULL
            GROUP BY category_code
            """
        ).fetchall():
            cats_rows.append(
                {
                    "rpfx": rpfx,
                    "category_code": row[0],
                    "category_label": row[1],
                    "n": row[2],
                }
            )

        # Default top-60 (category_code NULL)
        for n, row in enumerate(
            con.execute(
                f"""
                SELECT number, word
                FROM matches_rpfx
                ORDER BY boosted DESC, mike_rank ASC
                LIMIT {TOP_N}
                """
            ).fetchall(),
            start=1,
        ):
            top_rows.append(
                {
                    "rpfx": rpfx,
                    "category_code": None,
                    "number": row[0],
                    "word": row[1],
                    "ord": n,
                }
            )

        # Top-60 per category
        for row in con.execute(
            f"""
            SELECT category_code, number, word,
                   ROW_NUMBER() OVER (
                     PARTITION BY category_code ORDER BY boosted DESC, mike_rank ASC
                   ) AS ord
            FROM matches_rpfx
            WHERE category_code IS NOT NULL
            QUALIFY ord <= {TOP_N}
            """
        ).fetchall():
            top_rows.append(
                {
                    "rpfx": rpfx,
                    "category_code": row[0],
                    "number": row[1],
                    "word": row[2],
                    "ord": row[3],
                }
            )

        if i % 50 == 0:
            print(f"  {i}/{len(rpfxs)} rpfxs — {time.time()-t0:.1f}s elapsed")

    # Write out
    cats_tbl = pa.Table.from_pylist(cats_rows)
    out_cats = DATA / "vanity_categories.parquet"
    pq.write_table(cats_tbl, out_cats, compression="zstd")
    print(f"  {out_cats.name}: {len(cats_rows):,} (rpfx, category) rows")

    top_tbl = pa.Table.from_pylist(top_rows)
    out_top = DATA / "vanity_top.parquet"
    pq.write_table(top_tbl, out_top, compression="zstd")
    print(f"  {out_top.name}: {len(top_rows):,} top-{TOP_N} rows")

    print(f"Total: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
