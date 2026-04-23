"""
Analyze DISCONN "episodes" per resporg across all 42 monthly snapshots.

For each (number, rpfx) episode in DISCONN, determine how many consecutive
monthly snapshots the number sat in DISCONN under that rpfx. Classify:

  - 1–2 snapshots   → abbreviated disconnect (released early, likely bulk)
  - 3+ snapshots    → standard aging (full ~4-month process)

Writes: data/disconnect_episodes.parquet
  rpfx, duration_months, n_events
And a small roll-up at data/disconnect_summary.parquet
  rpfx, n_abbreviated, n_standard, abbrev_rate
"""

from __future__ import annotations
from pathlib import Path
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
CACHE = ROOT / "cache"
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

STATUS_DISCONN = 3


def main():
    t0 = time.time()
    con = duckdb.connect()

    # DuckDB's filename= column lets us tag each row with its source month.
    # We only need DISC rows, which is a tiny slice of the full cache.
    cache_glob = (CACHE / "*.parquet").as_posix()
    print("Extracting DISCONN snapshots across all months...")
    con.execute(f"""
      CREATE TABLE disc_snapshots AS
      SELECT
        number,
        rpfx,
        regexp_extract(filename, '(\\d{{4}}-\\d{{2}})', 1) AS month,
        CAST(substring(regexp_extract(filename, '(\\d{{4}}-\\d{{2}})', 1), 1, 4) AS INTEGER) * 12 +
          CAST(substring(regexp_extract(filename, '(\\d{{4}}-\\d{{2}})', 1), 6, 2) AS INTEGER) AS month_idx
      FROM read_parquet('{cache_glob}', filename=true)
      WHERE status = {STATUS_DISCONN}
    """)
    n_disc = con.execute("SELECT COUNT(*) FROM disc_snapshots").fetchone()[0]
    print(f"  {n_disc:,} DISCONN-status rows across all months")

    # Identify consecutive-month "runs" per (number, rpfx) using the classic
    # "month_idx - row_number" grouping trick.
    print("Detecting consecutive-month episodes...")
    con.execute("""
      CREATE TABLE runs AS
      SELECT number, rpfx,
             month_idx - ROW_NUMBER() OVER (PARTITION BY number, rpfx ORDER BY month_idx)
               AS run_id,
             month_idx
      FROM disc_snapshots
    """)

    con.execute("""
      CREATE TABLE episodes AS
      SELECT rpfx,
             COUNT(*) AS duration_months,
             MIN(month_idx) AS start_month_idx
      FROM runs
      GROUP BY number, rpfx, run_id
    """)
    n_ep = con.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
    print(f"  {n_ep:,} distinct disconnect episodes")

    # Aggregate per-rpfx duration histogram
    hist = con.execute("""
      SELECT rpfx, duration_months, COUNT(*) AS n_events
      FROM episodes
      GROUP BY rpfx, duration_months
      ORDER BY rpfx, duration_months
    """).fetchall()

    out1 = DATA / "disconnect_episodes.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [{"rpfx": r, "duration_months": d, "n_events": n} for r, d, n in hist]
        ),
        out1, compression="zstd",
    )
    print(f"  wrote {out1.name} ({len(hist):,} rows)")

    # Per-rpfx summary: abbreviated (1–2 mo) vs standard (3+)
    summary = con.execute("""
      SELECT rpfx,
             SUM(CASE WHEN duration_months <= 2 THEN n_events ELSE 0 END)  AS n_abbreviated,
             SUM(CASE WHEN duration_months >= 3 THEN n_events ELSE 0 END)  AS n_standard,
             SUM(n_events)                                                  AS n_total
      FROM (
        SELECT rpfx, duration_months, COUNT(*) AS n_events
        FROM episodes
        GROUP BY rpfx, duration_months
      )
      GROUP BY rpfx
    """).fetchall()

    rows = []
    for rpfx, abbr, std, tot in summary:
        rate = (abbr / tot) if tot else 0
        rows.append({
            "rpfx": rpfx,
            "n_abbreviated": abbr,
            "n_standard": std,
            "n_total": tot,
            "abbrev_rate": rate,
        })
    out2 = DATA / "disconnect_summary.parquet"
    pq.write_table(pa.Table.from_pylist(rows), out2, compression="zstd")
    print(f"  wrote {out2.name} ({len(rows):,} rpfxs)")

    # Print top early-disconnectors
    rows_sorted = sorted(rows, key=lambda r: -r["abbrev_rate"])
    print("\nTop 15 early disconnectors (abbrev rate, n_total > 1000):")
    print(f"  {'rpfx':<4}  {'abbr%':>7}  {'abbreviated':>12}  {'standard':>10}  {'total':>10}")
    for r in [x for x in rows_sorted if x["n_total"] > 1000][:15]:
        print(f"  {r['rpfx']:<4}  {r['abbrev_rate']*100:>6.1f}%  "
              f"{r['n_abbreviated']:>12,}  {r['n_standard']:>10,}  {r['n_total']:>10,}")

    print(f"\nTotal runtime: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
