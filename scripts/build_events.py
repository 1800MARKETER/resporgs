"""
Build the multi-month event dataset from the cached Parquet snapshots.

Produces two tables:

  data/resporg_month.parquet — per (resporg_prefix, month) roll-up:
    inventory, acquired, lost, transfers_in, transfers_out,
    harvested_cross_rpfx, harvested_own_reactivation, appeared_from_spare,
    disappeared_to_spare

  data/pair_totals.parquet — per (prev_month, curr_month) totals:
    transfers, landings, reactivations, appeared, disappeared, unchanged

Uses DuckDB to stream FULL OUTER JOINs across consecutive months without
loading everything into RAM. Status code 1=WORKING, 3=DISCONN.
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
DATA.mkdir(exist_ok=True)

STATUS_WORKING = 1
STATUS_DISCONN = 3


def month_files() -> list[tuple[str, Path]]:
    return sorted((p.stem, p) for p in CACHE.glob("*.parquet"))


def months_between(a: str, b: str) -> int:
    """How many calendar months from YYYY-MM a to YYYY-MM b (inclusive diff)."""
    ay, am = (int(x) for x in a.split("-"))
    by, bm = (int(x) for x in b.split("-"))
    return (by - ay) * 12 + (bm - am)


def build():
    months = month_files()
    if len(months) < 2:
        raise SystemExit("Need at least 2 months in cache/")
    print(f"Found {len(months)} monthly parquets.")

    con = duckdb.connect()

    pair_totals_rows = []
    rpfx_month_rows = []

    for (prev_m, prev_f), (curr_m, curr_f) in zip(months, months[1:]):
        t0 = time.time()

        # --- pair-level totals ---
        pair_row = con.execute(
            f"""
            WITH j AS (
              SELECT a.rpfx AS a_pfx, a.status AS a_st,
                     b.rpfx AS b_pfx, b.status AS b_st
              FROM read_parquet('{prev_f.as_posix()}') a
              FULL OUTER JOIN read_parquet('{curr_f.as_posix()}') b
                USING(number)
            )
            SELECT
              COUNT(*) FILTER (WHERE a_pfx IS NOT NULL AND b_pfx IS NOT NULL
                               AND a_pfx <> b_pfx)                       AS transfers,
              COUNT(*) FILTER (WHERE a_pfx IS NOT NULL AND b_pfx IS NOT NULL
                               AND a_pfx <> b_pfx AND a_st = {STATUS_DISCONN})
                                                                         AS landings,
              COUNT(*) FILTER (WHERE a_pfx = b_pfx
                               AND a_st = {STATUS_DISCONN}
                               AND b_st = {STATUS_WORKING})              AS reactivations,
              COUNT(*) FILTER (WHERE a_pfx IS NULL)                      AS appeared,
              COUNT(*) FILTER (WHERE b_pfx IS NULL)                      AS disappeared,
              COUNT(*) FILTER (WHERE a_pfx = b_pfx AND a_st = b_st)      AS unchanged
            FROM j
            """
        ).fetchone()
        mc = months_between(prev_m, curr_m)
        pair_totals_rows.append(
            {
                "prev_month": prev_m,
                "curr_month": curr_m,
                "months_covered": mc,
                "transfers": pair_row[0],
                "landings": pair_row[1],
                "reactivations": pair_row[2],
                "appeared": pair_row[3],
                "disappeared": pair_row[4],
                "unchanged": pair_row[5],
            }
        )

        # --- per-rpfx rollup for the CURRENT month transition ---
        # We report the row under the curr_month as the "state reached this month".
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE j AS
            SELECT a.rpfx AS a_pfx, a.status AS a_st,
                   b.rpfx AS b_pfx, b.status AS b_st
            FROM read_parquet('{prev_f.as_posix()}') a
            FULL OUTER JOIN read_parquet('{curr_f.as_posix()}') b
              USING(number)
            """
        )

        # Acquisitions: rows in this month, from the perspective of b_pfx
        acq = con.execute(
            f"""
            SELECT
              b_pfx AS rpfx,
              COUNT(*) FILTER (WHERE a_pfx IS NULL)                              AS appeared_from_spare,
              COUNT(*) FILTER (WHERE a_pfx IS NOT NULL AND a_pfx <> b_pfx)       AS transfers_in,
              COUNT(*) FILTER (WHERE a_pfx IS NOT NULL AND a_pfx <> b_pfx
                                     AND a_st = {STATUS_DISCONN})                AS harvested_cross_rpfx,
              COUNT(*) FILTER (WHERE a_pfx = b_pfx
                                     AND a_st = {STATUS_DISCONN}
                                     AND b_st = {STATUS_WORKING})                AS harvested_own_reactivation,
              COUNT(*)                                                           AS inventory
            FROM j
            WHERE b_pfx IS NOT NULL
            GROUP BY b_pfx
            """
        ).fetchall()

        # Losses: rows from the perspective of a_pfx
        lost = {
            r[0]: (r[1], r[2])
            for r in con.execute(
                f"""
                SELECT
                  a_pfx,
                  COUNT(*) FILTER (WHERE b_pfx IS NULL)                        AS disappeared_to_spare,
                  COUNT(*) FILTER (WHERE b_pfx IS NOT NULL AND a_pfx <> b_pfx) AS transfers_out
                FROM j
                WHERE a_pfx IS NOT NULL
                GROUP BY a_pfx
                """
            ).fetchall()
        }

        seen = set()
        for rpfx, appeared, t_in, harv, reac, inv in acq:
            disap, t_out = lost.get(rpfx, (0, 0))
            rpfx_month_rows.append(
                {
                    "rpfx": rpfx,
                    "month": curr_m,
                    "months_covered": mc,
                    "inventory": inv,
                    "acquired": appeared + t_in,
                    "lost": disap + t_out,
                    "transfers_in": t_in,
                    "transfers_out": t_out,
                    "harvested_cross_rpfx": harv,
                    "harvested_own_reactivation": reac,
                    "appeared_from_spare": appeared,
                    "disappeared_to_spare": disap,
                }
            )
            seen.add(rpfx)

        # rpfxs that existed in prev_m but have zero inventory in curr_m
        for rpfx, (disap, t_out) in lost.items():
            if rpfx in seen:
                continue
            rpfx_month_rows.append(
                {
                    "rpfx": rpfx,
                    "month": curr_m,
                    "months_covered": mc,
                    "inventory": 0,
                    "acquired": 0,
                    "lost": disap + t_out,
                    "transfers_in": 0,
                    "transfers_out": t_out,
                    "harvested_cross_rpfx": 0,
                    "harvested_own_reactivation": 0,
                    "appeared_from_spare": 0,
                    "disappeared_to_spare": disap,
                }
            )

        dur = time.time() - t0
        print(
            f"  {prev_m} -> {curr_m}: transfers={pair_row[0]:>8,} "
            f"landings={pair_row[1]:>7,} appeared={pair_row[3]:>7,} ({dur:.1f}s)"
        )

    # Add a baseline inventory row for the FIRST month (no diff against earlier)
    first_m, first_f = months[0]
    first_inv = con.execute(
        f"""
        SELECT rpfx, COUNT(*) AS inventory
        FROM read_parquet('{first_f.as_posix()}')
        GROUP BY rpfx
        """
    ).fetchall()
    for rpfx, inv in first_inv:
        rpfx_month_rows.append(
            {
                "rpfx": rpfx,
                "month": first_m,
                "months_covered": 0,
                "inventory": inv,
                "acquired": 0,
                "lost": 0,
                "transfers_in": 0,
                "transfers_out": 0,
                "harvested_cross_rpfx": 0,
                "harvested_own_reactivation": 0,
                "appeared_from_spare": 0,
                "disappeared_to_spare": 0,
            }
        )

    # Write outputs
    pair_tbl = pa.Table.from_pylist(pair_totals_rows)
    pq.write_table(pair_tbl, DATA / "pair_totals.parquet", compression="zstd")
    rpfx_tbl = pa.Table.from_pylist(rpfx_month_rows)
    pq.write_table(rpfx_tbl, DATA / "resporg_month.parquet", compression="zstd")

    print(f"\nWrote {len(pair_totals_rows)} pair totals to {DATA / 'pair_totals.parquet'}")
    print(f"Wrote {len(rpfx_month_rows)} (rpfx, month) rows to {DATA / 'resporg_month.parquet'}")


if __name__ == "__main__":
    build()
