"""
Build the directed transfer graph: for each (from_rpfx, to_rpfx) pair
and each month pair, how many numbers flowed, and how many of those
were harvested from the disconnect pool.

Output: data/partner_flows.parquet
  curr_month, from_rpfx, to_rpfx, n_transfers, n_from_disconn

This is the edge list of the industry-wide flow graph. It powers:
  - "Top trading partners" on every profile
  - Fast-transfer pair detection for shell-network work
  - Wholesale/reseller relationship inference
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

STATUS_DISCONN = 3


def main():
    months = sorted(p.stem for p in CACHE.glob("*.parquet"))
    con = duckdb.connect()

    rows = []
    for prev_m, curr_m in zip(months, months[1:]):
        t0 = time.time()
        pairs = con.execute(
            f"""
            SELECT
              a.rpfx AS from_rpfx,
              b.rpfx AS to_rpfx,
              COUNT(*) AS n_transfers,
              COUNT(*) FILTER (WHERE a.status = {STATUS_DISCONN}) AS n_from_disconn
            FROM read_parquet('{(CACHE/f"{prev_m}.parquet").as_posix()}') a
            JOIN read_parquet('{(CACHE/f"{curr_m}.parquet").as_posix()}') b
              USING(number)
            WHERE a.rpfx IS NOT NULL AND b.rpfx IS NOT NULL
              AND a.rpfx != b.rpfx
            GROUP BY a.rpfx, b.rpfx
            """
        ).fetchall()
        for from_rpfx, to_rpfx, n_t, n_d in pairs:
            rows.append(
                {
                    "curr_month": curr_m,
                    "from_rpfx": from_rpfx,
                    "to_rpfx": to_rpfx,
                    "n_transfers": n_t,
                    "n_from_disconn": n_d,
                }
            )
        print(f"  {prev_m} -> {curr_m}: {len(pairs):>6,} pairs ({time.time()-t0:.1f}s)")

    tbl = pa.Table.from_pylist(rows)
    out = DATA / "partner_flows.parquet"
    pq.write_table(tbl, out, compression="zstd")
    print(f"\nWrote {len(rows):,} rows to {out} ({out.stat().st_size/1e6:.1f} MB)")


if __name__ == "__main__":
    main()
