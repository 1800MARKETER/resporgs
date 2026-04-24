"""
Precompute per-rpfx vanity holdings.

Profile pages hit two live queries today — both join working numbers against
the 2M-row Master Million sqlite DB. On the droplet that costs 5-10 seconds
per request. This script materializes the join once and slices it two ways:

  data/vanity_categories.parquet — (rpfx, category_code, category_label, n)
      for the category filter dropdown

  data/vanity_top.parquet — (rpfx, category_code, number, word, ord)
      where category_code is NULL for the default "all categories" view, or
      the actual category_code for a per-category top-60. ord = 1..60.

Profile render becomes a plain WHERE + ORDER BY against the precompute.
"""

from __future__ import annotations
import time
from pathlib import Path

import duckdb

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

    t0 = time.time()

    # Materialize the rpfx × working × vanity join once. This is the single
    # expensive step — everything downstream reads from this small temp table.
    con.execute(
        f"""
        CREATE TEMP TABLE matches AS
        SELECT
          s.rpfx,
          s.number,
          UPPER(v.word)     AS word,
          v.category_code,
          v.category_label,
          COALESCE(v.blended_score, 0)
            * CASE WHEN s.number / 10000000 = 800 THEN 1.05 ELSE 1.0 END AS boosted,
          COALESCE(v.mike_rank, 999999) AS mike_rank
        FROM (
          SELECT rpfx, number,
                 LPAD((number % 10000000)::VARCHAR, 7, '0') AS last7
          FROM read_parquet('{curr}')
          WHERE status = 1
        ) s
        JOIN mm.vanity v ON s.last7 = v.digits
        WHERE v.word IS NOT NULL
        """
    )
    n_matches = con.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
    print(f"  materialized {n_matches:,} vanity matches in {time.time()-t0:.1f}s")

    # ---- vanity_categories.parquet ----
    t1 = time.time()
    out_cats = DATA / "vanity_categories.parquet"
    con.execute(
        f"""
        COPY (
          SELECT rpfx, category_code, MAX(category_label) AS category_label, COUNT(*) AS n
          FROM matches
          WHERE category_code IS NOT NULL
          GROUP BY rpfx, category_code
          ORDER BY rpfx, n DESC
        ) TO '{out_cats.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)
        """
    )
    n_cats = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_cats.as_posix()}')").fetchone()[0]
    print(f"  {out_cats.name}: {n_cats:,} (rpfx, category) rows — {time.time()-t1:.1f}s")

    # ---- vanity_top.parquet ----
    # Two concatenated chunks: NULL-category "default" top 60 per rpfx, and
    # per-category top 60 per (rpfx, category_code).
    t2 = time.time()
    out_top = DATA / "vanity_top.parquet"
    con.execute(
        f"""
        COPY (
          WITH default_view AS (
            SELECT rpfx,
                   CAST(NULL AS VARCHAR) AS category_code,
                   number, word, boosted, mike_rank,
                   ROW_NUMBER() OVER (
                     PARTITION BY rpfx
                     ORDER BY boosted DESC, mike_rank ASC
                   ) AS ord
            FROM matches
          ),
          per_cat AS (
            SELECT rpfx, category_code,
                   number, word, boosted, mike_rank,
                   ROW_NUMBER() OVER (
                     PARTITION BY rpfx, category_code
                     ORDER BY boosted DESC, mike_rank ASC
                   ) AS ord
            FROM matches
            WHERE category_code IS NOT NULL
          )
          SELECT rpfx, category_code, number, word, ord
          FROM default_view WHERE ord <= {TOP_N}
          UNION ALL
          SELECT rpfx, category_code, number, word, ord
          FROM per_cat WHERE ord <= {TOP_N}
          ORDER BY rpfx, category_code NULLS FIRST, ord
        ) TO '{out_top.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)
        """
    )
    n_top = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_top.as_posix()}')").fetchone()[0]
    print(f"  {out_top.name}: {n_top:,} top-{TOP_N} rows — {time.time()-t2:.1f}s")

    print(f"Total runtime: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
