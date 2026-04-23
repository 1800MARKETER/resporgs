"""
Investigate the AL (Lumen/Allstream-Legacy) collapse:
  1) Month-by-month AL inventory trajectory
  2) Did the drop happen in one month (reseller close) or gradually?
  3) Where did the numbers GO the month of the drop? Transferred, or disconnected to spare?
  4) Did other Lumen-group prefixes absorb any of them?
"""

from __future__ import annotations
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
CACHE = ROOT / "cache"
CLEAN = ROOT / "clean"

TARGET = "AL"


def section(t): print(f"\n{'='*70}\n{t}\n{'='*70}")


def name_for(prefix: str) -> str:
    docs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    for d in docs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if code[:2] == prefix:
            return d.get("title", "?")
    return "?"


def lumen_group_prefixes() -> list[str]:
    """Find all prefixes in the Lumen group."""
    groups = json.loads((CLEAN / "resporgGroup.json").read_text(encoding="utf-8"))
    lumen_ids = [g["_id"].removeprefix("drafts.") for g in groups
                 if g.get("title", "").lower() == "lumen"]
    if not lumen_ids:
        return []
    resporgs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    pfxs = []
    for d in resporgs:
        for g in d.get("groups", []) or []:
            if g.get("_ref") in lumen_ids:
                code = (d.get("codeTwoDigit") or "").strip().upper()
                if len(code) >= 2:
                    pfxs.append(code[:2])
    return sorted(set(pfxs))


def trajectory(con):
    section(f"{TARGET} inventory trajectory across 16 months")
    rows = con.execute(f"""
        SELECT month, inventory, acquired, lost,
               harvested_cross_rpfx, appeared_from_spare, disappeared_to_spare,
               transfers_out
        FROM read_parquet('{(DATA/"resporg_month.parquet").as_posix()}')
        WHERE rpfx = '{TARGET}'
        ORDER BY month
    """).fetchall()
    print(f"{'month':<9} {'inventory':>12} {'acq':>8} {'lost':>8} {'harv':>8} {'fromSpare':>10} {'toSpare':>10} {'transOut':>10}")
    for r in rows:
        print(f"{r[0]:<9} {r[1]:>12,} {r[2]:>8,} {r[3]:>8,} {r[4]:>8,} {r[5]:>10,} {r[6]:>10,} {r[7]:>10,}")


def find_collapse_month(con) -> str:
    """Which month had the largest drop?"""
    rows = con.execute(f"""
        WITH t AS (
          SELECT month, inventory, LAG(inventory) OVER (ORDER BY month) AS prev
          FROM read_parquet('{(DATA/"resporg_month.parquet").as_posix()}')
          WHERE rpfx = '{TARGET}'
        )
        SELECT month, inventory, prev, prev - inventory AS drop
        FROM t WHERE prev IS NOT NULL
        ORDER BY drop DESC LIMIT 3
    """).fetchall()
    print(f"\nBiggest month-over-month drops:")
    for m, inv, prev, drop in rows:
        print(f"  {m}: {prev:,} -> {inv:,}  (drop of {drop:,})")
    return rows[0][0] if rows else None


def where_did_they_go(con, collapse_month: str, prev_month: str):
    section(f"Where did {TARGET}'s numbers go between {prev_month} and {collapse_month}?")
    rows = con.execute(f"""
        WITH prev AS (
          SELECT number, resporg, rpfx, status
          FROM read_parquet('{(CACHE/f"{prev_month}.parquet").as_posix()}')
          WHERE rpfx = '{TARGET}'
        ),
        curr AS (
          SELECT number, resporg, rpfx, status
          FROM read_parquet('{(CACHE/f"{collapse_month}.parquet").as_posix()}')
        ),
        fate AS (
          SELECT p.number, p.status AS prev_status,
                 c.rpfx AS new_rpfx, c.status AS new_status
          FROM prev p
          LEFT JOIN curr c USING(number)
        )
        SELECT
          CASE
            WHEN new_rpfx IS NULL THEN '(vanished - to SPARE)'
            WHEN new_rpfx = '{TARGET}' THEN '(still {TARGET})'
            ELSE new_rpfx
          END AS destination,
          COUNT(*) AS count,
          COUNT(*) FILTER (WHERE prev_status = 3) AS from_disconn,
          COUNT(*) FILTER (WHERE prev_status = 1) AS from_working,
          COUNT(*) FILTER (WHERE new_status = 3) AS to_disconn,
          COUNT(*) FILTER (WHERE new_status = 1) AS to_working
        FROM fate
        GROUP BY destination
        ORDER BY count DESC
        LIMIT 20
    """).fetchall()
    print(f"{'destination':<28} {'count':>10} {'fromDISC':>10} {'fromWORK':>10} {'toDISC':>10} {'toWORK':>10}")
    for dest, cnt, fd, fw, td, tw in rows:
        print(f"{dest:<28} {cnt:>10,} {fd:>10,} {fw:>10,} {td:>10,} {tw:>10,}")


def lumen_sibling_check(con, collapse_month: str, prev_month: str):
    section(f"Lumen group siblings trajectory ({prev_month} -> {collapse_month})")
    lumen = lumen_group_prefixes()
    print(f"Lumen group prefixes: {lumen}")
    for pfx in lumen:
        row = con.execute(f"""
            SELECT
              (SELECT inventory FROM read_parquet('{(DATA/"resporg_month.parquet").as_posix()}')
                WHERE rpfx = '{pfx}' AND month = '{prev_month}'),
              (SELECT inventory FROM read_parquet('{(DATA/"resporg_month.parquet").as_posix()}')
                WHERE rpfx = '{pfx}' AND month = '{collapse_month}')
        """).fetchone()
        prev_inv = row[0] or 0
        curr_inv = row[1] or 0
        delta = curr_inv - prev_inv
        name = name_for(pfx)
        print(f"  {pfx}  {prev_inv:>12,} -> {curr_inv:>12,}  delta {delta:>+12,}   {name}")


def main():
    con = duckdb.connect()
    trajectory(con)
    collapse_month = find_collapse_month(con)
    # Find the month immediately before the collapse
    months = sorted(p.stem for p in CACHE.glob("*.parquet"))
    idx = months.index(collapse_month)
    prev_month = months[idx - 1]
    where_did_they_go(con, collapse_month, prev_month)
    lumen_sibling_check(con, collapse_month, prev_month)


if __name__ == "__main__":
    main()
