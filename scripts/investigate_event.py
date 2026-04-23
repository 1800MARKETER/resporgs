"""
Investigate any month transition: who were the top gainers/losers, where did
the money go, what kind of event was it (Lumen-style carcass dump? Mass
first-assignment spike? Category-wide reshuffling?).

Usage:
    python scripts/investigate_event.py 2023-11 2023-12
    python scripts/investigate_event.py 2025-09 2025-10
"""

from __future__ import annotations
import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
CACHE = ROOT / "cache"
CLEAN = ROOT / "clean"


def name_map() -> dict[str, str]:
    docs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    m: dict[str, str] = {}
    for d in docs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) >= 2 and code[:2] not in m:
            m[code[:2]] = d.get("title") or d.get("alias") or "?"
    return m


def name(pfx: str, names: dict[str, str]) -> str:
    return names.get(pfx, "  (not in Sanity)")


def section(t: str):
    print(f"\n{'='*74}\n{t}\n{'='*74}")


def main():
    if len(sys.argv) < 3:
        print("Usage: investigate_event.py <prev_month> <curr_month>")
        sys.exit(1)
    prev_m, curr_m = sys.argv[1], sys.argv[2]
    prev_f = CACHE / f"{prev_m}.parquet"
    curr_f = CACHE / f"{curr_m}.parquet"
    if not (prev_f.exists() and curr_f.exists()):
        print(f"Missing cache file(s) for {prev_m} / {curr_m}")
        sys.exit(1)

    con = duckdb.connect()
    names = name_map()

    print(f"\n### EVENT INVESTIGATION: {prev_m} -> {curr_m} ###")

    # Top-level totals
    section("Totals")
    t = con.execute(
        f"""
        WITH j AS (
          SELECT a.rpfx AS a_pfx, a.status AS a_st, b.rpfx AS b_pfx, b.status AS b_st
          FROM read_parquet('{prev_f.as_posix()}') a
          FULL OUTER JOIN read_parquet('{curr_f.as_posix()}') b USING(number)
        )
        SELECT
          COUNT(*) FILTER (WHERE a_pfx IS NULL AND b_pfx IS NOT NULL) AS appeared,
          COUNT(*) FILTER (WHERE a_pfx IS NOT NULL AND b_pfx IS NULL) AS disappeared,
          COUNT(*) FILTER (WHERE a_pfx IS NOT NULL AND b_pfx IS NOT NULL AND a_pfx <> b_pfx) AS transfers,
          COUNT(*) FILTER (WHERE a_pfx IS NOT NULL AND b_pfx IS NOT NULL AND a_pfx <> b_pfx AND a_st = 3) AS landings
        FROM j
        """
    ).fetchone()
    print(f"  Appeared from spare:  {t[0]:>10,}")
    print(f"  Disappeared to spare: {t[1]:>10,}")
    print(f"  Transfers:            {t[2]:>10,}")
    print(f"  Disconnect landings:  {t[3]:>10,}")

    # Top gainers
    section("Top 20 GAINERS (delta inventory)")
    rows = con.execute(
        f"""
        WITH p AS (
          SELECT rpfx, COUNT(*) AS n FROM read_parquet('{prev_f.as_posix()}') GROUP BY rpfx
        ),
        c AS (
          SELECT rpfx, COUNT(*) AS n FROM read_parquet('{curr_f.as_posix()}') GROUP BY rpfx
        )
        SELECT
          COALESCE(c.rpfx, p.rpfx) AS rpfx,
          COALESCE(p.n, 0) AS prev_n,
          COALESCE(c.n, 0) AS curr_n,
          COALESCE(c.n, 0) - COALESCE(p.n, 0) AS delta
        FROM p FULL OUTER JOIN c USING(rpfx)
        ORDER BY delta DESC
        LIMIT 20
        """
    ).fetchall()
    print(f"  {'rpfx':<5} {'prev':>12} {'curr':>12} {'delta':>12}   name")
    for rpfx, p, c, d in rows:
        print(f"  {rpfx:<5} {p:>12,} {c:>12,} {d:>+12,}   {name(rpfx, names)}")

    # Top losers
    section("Top 20 LOSERS (delta inventory)")
    rows = con.execute(
        f"""
        WITH p AS (
          SELECT rpfx, COUNT(*) AS n FROM read_parquet('{prev_f.as_posix()}') GROUP BY rpfx
        ),
        c AS (
          SELECT rpfx, COUNT(*) AS n FROM read_parquet('{curr_f.as_posix()}') GROUP BY rpfx
        )
        SELECT
          COALESCE(c.rpfx, p.rpfx) AS rpfx,
          COALESCE(p.n, 0) AS prev_n,
          COALESCE(c.n, 0) AS curr_n,
          COALESCE(c.n, 0) - COALESCE(p.n, 0) AS delta
        FROM p FULL OUTER JOIN c USING(rpfx)
        ORDER BY delta ASC
        LIMIT 20
        """
    ).fetchall()
    print(f"  {'rpfx':<5} {'prev':>12} {'curr':>12} {'delta':>12}   name")
    for rpfx, p, c, d in rows:
        print(f"  {rpfx:<5} {p:>12,} {c:>12,} {d:>+12,}   {name(rpfx, names)}")

    # Source of "appeared" numbers: distributed over whom?
    section("Who acquired the NEWLY-APPEARED numbers (from spare)?")
    rows = con.execute(
        f"""
        SELECT
          b.rpfx,
          COUNT(*) AS appeared_count
        FROM read_parquet('{prev_f.as_posix()}') a
        RIGHT JOIN read_parquet('{curr_f.as_posix()}') b USING(number)
        WHERE a.number IS NULL
        GROUP BY b.rpfx
        ORDER BY appeared_count DESC
        LIMIT 15
        """
    ).fetchall()
    print(f"  {'rpfx':<5} {'appeared':>12}   name")
    for rpfx, n in rows:
        print(f"  {rpfx:<5} {n:>12,}   {name(rpfx, names)}")

    # Breakdown by prefix — was this concentrated in 800? new (833/844/855)?
    section("Appeared by NPA (toll-free prefix)")
    rows = con.execute(
        f"""
        SELECT b.prefix,
               COUNT(*) AS appeared_count
        FROM read_parquet('{prev_f.as_posix()}') a
        RIGHT JOIN read_parquet('{curr_f.as_posix()}') b USING(number)
        WHERE a.number IS NULL
        GROUP BY b.prefix
        ORDER BY b.prefix
        """
    ).fetchall()
    for p, n in rows:
        print(f"  {p}: {n:,}")

    # Biggest single-pair transfers (A -> B)
    section("Top 15 DIRECTED TRANSFERS (A -> B)")
    rows = con.execute(
        f"""
        SELECT a.rpfx AS from_rpfx, b.rpfx AS to_rpfx, COUNT(*) AS n,
               COUNT(*) FILTER (WHERE a.status = 3) AS from_disconn
        FROM read_parquet('{prev_f.as_posix()}') a
        JOIN read_parquet('{curr_f.as_posix()}') b USING(number)
        WHERE a.rpfx IS NOT NULL AND b.rpfx IS NOT NULL AND a.rpfx <> b.rpfx
        GROUP BY a.rpfx, b.rpfx
        ORDER BY n DESC
        LIMIT 15
        """
    ).fetchall()
    print(f"  {'from':<5} -> {'to':<5} {'count':>10} {'fromDISC':>10}   from -> to")
    for fr, to, n, fd in rows:
        print(
            f"  {fr:<5} -> {to:<5} {n:>10,} {fd:>10,}   "
            f"{name(fr, names)}  ->  {name(to, names)}"
        )


if __name__ == "__main__":
    main()
