"""
Precompute per-rpfx snapshot aggregates from the latest monthly parquet.

Profile pages currently scan the 48M-row cache/YYYY-MM.parquet 3× per
request (NPA breakdown, status breakdown, sub-code list). Those scans are
the remaining bottleneck after ranks/vanity/flow precomputes.

Output:
  data/rpfx_snapshot.parquet — one row per (rpfx, prefix, status) with count
  data/rpfx_subcodes.parquet — (rpfx, resporg, count) for sub-code display

Profile render becomes 2 small-parquet lookups instead of 3 big cache scans.
"""

from __future__ import annotations
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
CACHE = ROOT / "cache"
DATA = ROOT / "data"


def main():
    months = sorted(p.stem for p in CACHE.glob("*.parquet"))
    if not months:
        raise SystemExit("No cached months — run cache_months.py first.")
    curr = (CACHE / f"{months[-1]}.parquet").as_posix()

    con = duckdb.connect()

    t0 = time.time()
    out_snap = DATA / "rpfx_snapshot.parquet"
    con.execute(
        f"""
        COPY (
          SELECT rpfx, prefix, status, COUNT(*) AS n
          FROM read_parquet('{curr}')
          GROUP BY rpfx, prefix, status
          ORDER BY rpfx, prefix, status
        ) TO '{out_snap.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)
        """
    )
    n1 = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_snap.as_posix()}')").fetchone()[0]
    print(f"  {out_snap.name}: {n1:,} rows — {time.time()-t0:.1f}s")

    t1 = time.time()
    out_sub = DATA / "rpfx_subcodes.parquet"
    con.execute(
        f"""
        COPY (
          SELECT rpfx, resporg, COUNT(*) AS n
          FROM read_parquet('{curr}')
          GROUP BY rpfx, resporg
          ORDER BY rpfx, n DESC
        ) TO '{out_sub.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)
        """
    )
    n2 = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_sub.as_posix()}')").fetchone()[0]
    print(f"  {out_sub.name}: {n2:,} rows — {time.time()-t1:.1f}s")

    print(f"Total: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
