"""
Build the full industry flow graph across all month pairs.

Each edge row: (curr_month, from_node, to_node, edge_type, n, prev_rpfx)
  - from_node, to_node are real 2-char rpfxs OR one of: 'DISC', 'SPARE'
  - edge_type enumerates: TRANSFER, HARVEST, DISCONNECT, REACTIVATE, FIRST_ASSIGN, TO_SPARE
  - prev_rpfx is the number's prior owner when edge_type == HARVEST
    (so we can still attribute "X harvested from DISC-ex-Y" without per-owner DISC nodes)

Output: data/flow_graph.parquet

Status codes: 1=WORKING 2=TRANSIT 3=DISCONN 4=RESERVED 5=UNAVAIL 6=ASSIGNED 7=SUSPEND
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

DISC = 3


def main():
    months = sorted(p.stem for p in CACHE.glob("*.parquet"))
    con = duckdb.connect()

    all_rows = []
    for prev_m, curr_m in zip(months, months[1:]):
        t0 = time.time()
        # One query covering all 6 edge types, yielding grouped counts.
        # Each group: (from_node, to_node, edge_type, prev_rpfx, n)
        rows = con.execute(
            f"""
            WITH j AS (
              SELECT a.rpfx AS a_pfx, a.status AS a_st,
                     b.rpfx AS b_pfx, b.status AS b_st
              FROM read_parquet('{(CACHE/f"{prev_m}.parquet").as_posix()}') a
              FULL OUTER JOIN read_parquet('{(CACHE/f"{curr_m}.parquet").as_posix()}') b
                USING(number)
            ),
            classified AS (
              SELECT
                CASE
                  WHEN a_pfx IS NULL             THEN 'SPARE'
                  WHEN b_pfx IS NULL             THEN a_pfx
                  WHEN a_pfx = b_pfx AND a_st = {DISC} AND b_st = 1 THEN 'DISC'
                  WHEN a_pfx <> b_pfx AND a_st = {DISC} THEN 'DISC'
                  WHEN a_pfx = b_pfx AND a_st <> {DISC} AND b_st = {DISC} THEN a_pfx
                  ELSE a_pfx
                END AS from_node,
                CASE
                  WHEN b_pfx IS NULL                                     THEN 'SPARE'
                  WHEN a_pfx = b_pfx AND a_st <> {DISC} AND b_st = {DISC} THEN 'DISC'
                  ELSE b_pfx
                END AS to_node,
                CASE
                  WHEN a_pfx IS NULL                                     THEN 'FIRST_ASSIGN'
                  WHEN b_pfx IS NULL                                     THEN 'TO_SPARE'
                  WHEN a_pfx = b_pfx AND a_st = {DISC} AND b_st = 1       THEN 'REACTIVATE'
                  WHEN a_pfx = b_pfx AND a_st <> {DISC} AND b_st = {DISC} THEN 'DISCONNECT'
                  WHEN a_pfx <> b_pfx AND a_st = {DISC}                   THEN 'HARVEST'
                  WHEN a_pfx <> b_pfx                                     THEN 'TRANSFER'
                  ELSE 'OTHER'
                END AS edge_type,
                CASE
                  WHEN a_pfx <> b_pfx AND a_st = {DISC} THEN a_pfx  -- preserve origin for HARVEST
                  ELSE NULL
                END AS prev_rpfx
              FROM j
              WHERE NOT (a_pfx IS NOT NULL AND b_pfx IS NOT NULL
                         AND a_pfx = b_pfx AND a_st = b_st)
            )
            SELECT from_node, to_node, edge_type, prev_rpfx, COUNT(*) AS n
            FROM classified
            WHERE edge_type <> 'OTHER'
            GROUP BY from_node, to_node, edge_type, prev_rpfx
            """
        ).fetchall()
        for from_node, to_node, etype, prev_rpfx, n in rows:
            all_rows.append(
                {
                    "curr_month": curr_m,
                    "from_node": from_node,
                    "to_node": to_node,
                    "edge_type": etype,
                    "prev_rpfx": prev_rpfx,
                    "n": n,
                }
            )
        print(f"  {prev_m} -> {curr_m}: {len(rows):>6,} edge groups ({time.time()-t0:.1f}s)")

    tbl = pa.Table.from_pylist(all_rows)
    out = DATA / "flow_graph.parquet"
    pq.write_table(tbl, out, compression="zstd")
    print(
        f"\nWrote {len(all_rows):,} edge rows to {out} "
        f"({out.stat().st_size/1e6:.1f} MB)"
    )


if __name__ == "__main__":
    main()
