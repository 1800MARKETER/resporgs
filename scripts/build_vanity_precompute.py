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
    # Droplets are RAM-tight (1.9GB / no swap) — a single UNION over a 9.5M
    # materialized table OOMs. Write two passes into two files, then merge by
    # copying both into a single output parquet at the end. Each pass is bounded.
    t2 = time.time()
    out_top = DATA / "vanity_top.parquet"
    default_tmp = DATA / "_vanity_top_default.tmp.parquet"
    percat_tmp = DATA / "_vanity_top_percat.tmp.parquet"

    # Pass 1: default view (NULL category_code) — top TOP_N per rpfx
    con.execute(
        f"""
        COPY (
          SELECT rpfx, CAST(NULL AS VARCHAR) AS category_code, number, word, ord
          FROM (
            SELECT rpfx, number, word,
                   ROW_NUMBER() OVER (
                     PARTITION BY rpfx ORDER BY boosted DESC, mike_rank ASC
                   ) AS ord
            FROM matches
          )
          WHERE ord <= {TOP_N}
        ) TO '{default_tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)
        """
    )

    # Pass 2: per-category — top TOP_N per (rpfx, category_code)
    con.execute(
        f"""
        COPY (
          SELECT rpfx, category_code, number, word, ord
          FROM (
            SELECT rpfx, category_code, number, word,
                   ROW_NUMBER() OVER (
                     PARTITION BY rpfx, category_code ORDER BY boosted DESC, mike_rank ASC
                   ) AS ord
            FROM matches
            WHERE category_code IS NOT NULL
          )
          WHERE ord <= {TOP_N}
        ) TO '{percat_tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)
        """
    )

    # Drop the big temp table — no longer needed
    con.execute("DROP TABLE matches")

    # Combine both passes into the final file
    con.execute(
        f"""
        COPY (
          SELECT * FROM read_parquet('{default_tmp.as_posix()}')
          UNION ALL
          SELECT * FROM read_parquet('{percat_tmp.as_posix()}')
        ) TO '{out_top.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)
        """
    )
    default_tmp.unlink()
    percat_tmp.unlink()

    n_top = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_top.as_posix()}')").fetchone()[0]
    print(f"  {out_top.name}: {n_top:,} top-{TOP_N} rows — {time.time()-t2:.1f}s")

    print(f"Total runtime: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
