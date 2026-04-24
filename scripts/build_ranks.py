"""
Precompute per-rpfx rankings so profile pages don't scan the full
event tables 6× per request.

Produces: data/ranks.parquet with columns:
  rpfx
  inv_rank, inv_total             — current-month inventory (all rpfxs)
  opp_rank, opp_total             — 42-mo Opportunism Index, acquired>1000
  growth_rank, growth_total       — 42-mo delta, start_inv>10000
  vanity_rank, vanity_total       — MM match %, working>5000
  age_rank, age_total             — median inventory age, working>5000

Re-run whenever data changes — part of the monthly rebuild pipeline.
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


def main():
    months = sorted(p.stem for p in CACHE.glob("*.parquet"))
    if not months:
        raise SystemExit("No cached months found — run cache_months.py first.")
    curr = (CACHE / f"{months[-1]}.parquet").as_posix()
    resporg_month = (DATA / "resporg_month.parquet").as_posix()
    enr = (DATA / "enrichment_current.parquet").as_posix()

    con = duckdb.connect()

    t0 = time.time()

    con.execute(
        f"""
        CREATE TEMP TABLE inv_r AS
        WITH x AS (
          SELECT rpfx, COUNT(*)::BIGINT AS value
          FROM read_parquet('{curr}') GROUP BY rpfx
        )
        SELECT rpfx,
               RANK() OVER (ORDER BY value DESC)  AS inv_rank,
               COUNT(*) OVER ()                    AS inv_total
        FROM x
        """
    )

    con.execute(
        f"""
        CREATE TEMP TABLE opp_r AS
        WITH x AS (
          SELECT rpfx,
                 SUM(harvested_cross_rpfx)::DOUBLE / NULLIF(SUM(acquired), 0) AS value
          FROM read_parquet('{resporg_month}')
          GROUP BY rpfx HAVING SUM(acquired) > 1000
        )
        SELECT rpfx,
               RANK() OVER (ORDER BY value DESC) AS opp_rank,
               COUNT(*) OVER ()                   AS opp_total
        FROM x
        """
    )

    con.execute(
        f"""
        CREATE TEMP TABLE growth_r AS
        WITH m AS (
          SELECT rpfx, month, inventory,
                 ROW_NUMBER() OVER (PARTITION BY rpfx ORDER BY month) AS rn_a,
                 ROW_NUMBER() OVER (PARTITION BY rpfx ORDER BY month DESC) AS rn_d
          FROM read_parquet('{resporg_month}') WHERE inventory > 0
        ),
        x AS (
          SELECT f.rpfx, l.inventory - f.inventory AS value
          FROM (SELECT * FROM m WHERE rn_a = 1) f
          JOIN (SELECT * FROM m WHERE rn_d = 1) l USING(rpfx)
          WHERE f.inventory > 10000
        )
        SELECT rpfx,
               RANK() OVER (ORDER BY value DESC) AS growth_rank,
               COUNT(*) OVER ()                   AS growth_total
        FROM x
        """
    )

    con.execute(
        f"""
        CREATE TEMP TABLE vanity_r AS
        WITH x AS (
          SELECT rpfx, mm_count::DOUBLE / NULLIF(working_count, 0) AS value
          FROM read_parquet('{enr}') WHERE working_count > 5000
        )
        SELECT rpfx,
               RANK() OVER (ORDER BY value DESC) AS vanity_rank,
               COUNT(*) OVER ()                   AS vanity_total
        FROM x
        """
    )

    con.execute(
        f"""
        CREATE TEMP TABLE age_r AS
        WITH x AS (
          SELECT rpfx, median_age_months AS value
          FROM read_parquet('{enr}') WHERE working_count > 5000
        )
        SELECT rpfx,
               RANK() OVER (ORDER BY value DESC) AS age_rank,
               COUNT(*) OVER ()                   AS age_total
        FROM x
        """
    )

    # Full-outer join across all rank tables to get one row per rpfx
    combined = con.execute(
        """
        SELECT
          COALESCE(i.rpfx, o.rpfx, g.rpfx, v.rpfx, a.rpfx) AS rpfx,
          i.inv_rank, i.inv_total,
          o.opp_rank, o.opp_total,
          g.growth_rank, g.growth_total,
          v.vanity_rank, v.vanity_total,
          a.age_rank, a.age_total
        FROM inv_r i
        FULL OUTER JOIN opp_r    o USING (rpfx)
        FULL OUTER JOIN growth_r g USING (rpfx)
        FULL OUTER JOIN vanity_r v USING (rpfx)
        FULL OUTER JOIN age_r    a USING (rpfx)
        """
    ).fetchall()

    cols = [
        "rpfx",
        "inv_rank", "inv_total",
        "opp_rank", "opp_total",
        "growth_rank", "growth_total",
        "vanity_rank", "vanity_total",
        "age_rank", "age_total",
    ]
    tbl = pa.table({col: [r[i] for r in combined] for i, col in enumerate(cols)})
    out = DATA / "ranks.parquet"
    pq.write_table(tbl, out, compression="zstd")

    print(f"Wrote {len(combined)} rpfx ranks to {out} in {time.time() - t0:.1f}s")
    print(f"Preview:")
    print(f"  {'rpfx':<5} {'inv':>6} {'opp':>6} {'growth':>8} {'vanity':>8} {'age':>6}")
    for r in combined[:10]:
        print(f"  {r[0]:<5} {str(r[1] or '-'):>6} {str(r[3] or '-'):>6} "
              f"{str(r[5] or '-'):>8} {str(r[7] or '-'):>8} {str(r[9] or '-'):>6}")


if __name__ == "__main__":
    main()
