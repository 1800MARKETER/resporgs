"""
Precompute per-resporg enrichment data:

  - Master Million matches: what % of their WORKING inventory is a known vanity
  - Age distribution: how old are the last-change dates in their inventory
  - Top vanity holdings: best-scored MM matches they hold

Writes: data/enrichment_current.parquet
         data/enrichment_vanity_hits.parquet  (top 100 per rpfx for display)
"""

from __future__ import annotations
from pathlib import Path
import time

import duckdb
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
CACHE = ROOT / "cache"
DATA = ROOT / "data"
MM_DB = ROOT.parent / "local-prospector" / "data" / "master_vanity.db"

STATUS_WORKING = 1


def main():
    # Use the most recent cache month as the reference for "current inventory"
    months = sorted(p.stem for p in CACHE.glob("*.parquet"))
    curr_month = months[-1]
    curr_path = (CACHE / f"{curr_month}.parquet").as_posix()

    con = duckdb.connect()
    con.install_extension("sqlite")
    con.load_extension("sqlite")
    con.execute(f"ATTACH '{MM_DB.as_posix()}' AS mm (TYPE sqlite, READ_ONLY)")

    # We'll translate the snapshot's `yy/mm/dd` to an age in months relative to
    # the snapshot month itself (so "how old is the last-change relative to now").
    # yy is 2-digit (e.g. 25 -> 2025). Assume 20yy for all (the toll-free system
    # didn't exist in 19yy for active numbers, so all yy < 70 are 20yy safely).
    snap_y, snap_m = (int(x) for x in curr_month.split("-"))

    print(f"Computing MM matches + age distribution for {curr_month}...")
    t0 = time.time()

    # MM match rate per rpfx
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE working AS
        SELECT rpfx,
               number,
               yy, mm AS chg_mm, dd,
               LPAD((number % 10000000)::VARCHAR, 7, '0') AS last7
        FROM read_parquet('{curr_path}')
        WHERE status = {STATUS_WORKING}
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE mm_by_rpfx AS
        SELECT w.rpfx,
               COUNT(*) AS working_count,
               COUNT(v.digits) AS mm_count
        FROM working w
        LEFT JOIN mm.vanity v ON w.last7 = v.digits
        GROUP BY w.rpfx
        """
    )

    # Age distribution per rpfx — months since last-change date
    # Bucket into: <1m, 1-3m, 3-12m, 12-24m, 24-60m, 60+m
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE age_by_rpfx AS
        WITH w AS (
          SELECT rpfx,
            CASE WHEN yy = 0 THEN NULL
                 ELSE (({snap_y} - (2000 + yy::INT)) * 12 + ({snap_m} - chg_mm::INT))
            END AS age_months
          FROM working
        )
        SELECT rpfx,
          COUNT(*) FILTER (WHERE age_months <  1)              AS b_under_1m,
          COUNT(*) FILTER (WHERE age_months BETWEEN 1 AND 2)   AS b_1_3m,
          COUNT(*) FILTER (WHERE age_months BETWEEN 3 AND 11)  AS b_3_12m,
          COUNT(*) FILTER (WHERE age_months BETWEEN 12 AND 23) AS b_1_2y,
          COUNT(*) FILTER (WHERE age_months BETWEEN 24 AND 59) AS b_2_5y,
          COUNT(*) FILTER (WHERE age_months >= 60)             AS b_5y_plus,
          MEDIAN(age_months)::INT                              AS median_age_months
        FROM w
        GROUP BY rpfx
        """
    )

    # Join MM + age into one enrichment table
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE enrichment AS
        SELECT m.rpfx,
               m.working_count, m.mm_count,
               a.b_under_1m, a.b_1_3m, a.b_3_12m, a.b_1_2y, a.b_2_5y, a.b_5y_plus,
               a.median_age_months
        FROM mm_by_rpfx m
        JOIN age_by_rpfx a USING(rpfx)
        """
    )

    out = DATA / "enrichment_current.parquet"
    con.execute(
        f"COPY (SELECT * FROM enrichment) TO '{out.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)"
    )
    n_enr = con.execute("SELECT COUNT(*) FROM enrichment").fetchone()[0]
    print(f"  Wrote {n_enr:,} rpfxs to {out.name}")

    # Top 50 vanity holdings per rpfx (for display on profile page)
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE top_hits AS
        WITH joined AS (
          SELECT w.rpfx,
                 w.number,
                 v.digits,
                 v.word,
                 v.category_code,
                 v.category_label,
                 v.blended_score,
                 v.mike_rank,
                 v.rank,
                 ROW_NUMBER() OVER (PARTITION BY w.rpfx ORDER BY COALESCE(v.blended_score,0) DESC,
                                                                   COALESCE(v.mike_rank,999999) ASC) AS rn
          FROM working w
          JOIN mm.vanity v ON w.last7 = v.digits
        )
        SELECT rpfx, number, digits, word, category_code, category_label,
               blended_score, mike_rank, rank
        FROM joined
        WHERE rn <= 50
        """
    )
    out2 = DATA / "enrichment_vanity_hits.parquet"
    con.execute(
        f"COPY (SELECT * FROM top_hits) TO '{out2.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)"
    )
    n_hits = con.execute("SELECT COUNT(*) FROM top_hits").fetchone()[0]
    print(f"  Wrote {n_hits:,} top-50 vanity hits to {out2.name}")

    print(f"Done in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
