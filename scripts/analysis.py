"""
Multi-month analysis over data/resporg_month.parquet.

Outputs four reports:
  1) Stable Opportunism Index   (15-month harvest ratio per resporg)
  2) Inventory trajectory       (start, end, delta, %change)
  3) Net flow leaderboard       (cumulative acquired/lost)
  4) Group validation           (do Sanity groups' member prefixes actually share flow?)
"""

from __future__ import annotations
import json
from collections import defaultdict
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
CLEAN = ROOT / "clean"

# Exclude pair 2025-02 -> 2025-03 (duplicate data)
EXCLUDED_TARGET_MONTHS = {"2025-03"}


def load_name_map():
    """Build 2-char prefix -> canonical title from Sanity."""
    docs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    by_prefix: dict[str, tuple[str, str]] = {}
    for d in docs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) < 2:
            continue
        pfx = code[:2]
        title = d.get("title") or d.get("alias") or "?"
        if pfx not in by_prefix:
            by_prefix[pfx] = (title, code)
    return by_prefix


def load_group_memberships() -> dict[str, list[str]]:
    """group-slug -> list of resporg 2-char prefixes."""
    resporgs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    groups = json.loads((CLEAN / "resporgGroup.json").read_text(encoding="utf-8"))

    id_to_group_slug = {}
    id_to_group_title = {}
    for g in groups:
        gid = g["_id"].removeprefix("drafts.")
        id_to_group_slug[gid] = (g.get("slug") or {}).get("current", "?")
        id_to_group_title[gid] = g.get("title", "?")

    group_members: dict[str, list[str]] = defaultdict(list)
    group_titles: dict[str, str] = {}
    for d in resporgs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) < 2:
            continue
        pfx = code[:2]
        for gref in d.get("groups", []) or []:
            gid = gref.get("_ref")
            if gid and gid in id_to_group_slug:
                slug = id_to_group_slug[gid]
                group_members[slug].append(pfx)
                group_titles[slug] = id_to_group_title[gid]
    return {
        slug: {"title": group_titles[slug], "prefixes": sorted(set(pfxs))}
        for slug, pfxs in group_members.items()
    }


def section(title: str):
    print(f"\n{'='*70}\n{title}\n{'='*70}")


def opportunism_leaderboard(con, names):
    section("STABLE OPPORTUNISM INDEX (14 valid month transitions)")
    excluded = "','".join(EXCLUDED_TARGET_MONTHS)
    rows = con.execute(f"""
        SELECT rpfx,
               SUM(acquired) AS total_acq,
               SUM(harvested_cross_rpfx) AS total_harv,
               SUM(lost) AS total_lost,
               SUM(appeared_from_spare) AS total_appeared
        FROM read_parquet('{(DATA/"resporg_month.parquet").as_posix()}')
        WHERE month NOT IN ('{excluded}') AND month != (SELECT MIN(month) FROM read_parquet('{(DATA/"resporg_month.parquet").as_posix()}'))
        GROUP BY rpfx
        HAVING total_acq > 10000
        ORDER BY total_harv / NULLIF(total_acq,0) DESC
        LIMIT 30
    """).fetchall()
    print(f"{'rpfx':<5} {'acquired':>10} {'harvest':>10} {'lost':>10} {'Opp.Idx':>9}   name")
    for rpfx, acq, harv, lost, app in rows:
        name = names.get(rpfx, (None,))[0] or "  (not in Sanity)"
        opp = harv / acq if acq else 0
        print(f"{rpfx:<5} {acq:>10,} {harv:>10,} {lost:>10,} {opp:>8.1%}   {name}")


def cleanest_carriers(con, names):
    section("CLEANEST CARRIERS (lowest Opp.Idx, acquired > 100K)")
    excluded = "','".join(EXCLUDED_TARGET_MONTHS)
    rows = con.execute(f"""
        SELECT rpfx,
               SUM(acquired) AS total_acq,
               SUM(harvested_cross_rpfx) AS total_harv,
               SUM(lost) AS total_lost
        FROM read_parquet('{(DATA/"resporg_month.parquet").as_posix()}')
        WHERE month NOT IN ('{excluded}') AND month != (SELECT MIN(month) FROM read_parquet('{(DATA/"resporg_month.parquet").as_posix()}'))
        GROUP BY rpfx
        HAVING total_acq > 100000
        ORDER BY total_harv / NULLIF(total_acq,0) ASC
        LIMIT 15
    """).fetchall()
    print(f"{'rpfx':<5} {'acquired':>10} {'harvest':>10} {'lost':>10} {'Opp.Idx':>9}   name")
    for rpfx, acq, harv, lost in rows:
        name = names.get(rpfx, (None,))[0] or "  (not in Sanity)"
        opp = harv / acq if acq else 0
        print(f"{rpfx:<5} {acq:>10,} {harv:>10,} {lost:>10,} {opp:>8.2%}   {name}")


def trajectory(con, names):
    section("INVENTORY TRAJECTORY (first vs last month of series)")
    rows = con.execute(f"""
        WITH m AS (
          SELECT rpfx, month, inventory,
                 ROW_NUMBER() OVER (PARTITION BY rpfx ORDER BY month ASC) AS rn_asc,
                 ROW_NUMBER() OVER (PARTITION BY rpfx ORDER BY month DESC) AS rn_desc
          FROM read_parquet('{(DATA/"resporg_month.parquet").as_posix()}')
          WHERE inventory > 0
        )
        SELECT
          f.rpfx,
          f.inventory AS start_inv,
          l.inventory AS end_inv,
          l.inventory - f.inventory AS delta,
          (l.inventory - f.inventory)::FLOAT / NULLIF(f.inventory,0) AS pct
        FROM (SELECT * FROM m WHERE rn_asc = 1) f
        JOIN (SELECT * FROM m WHERE rn_desc = 1) l USING (rpfx)
        WHERE f.inventory > 50000
        ORDER BY delta DESC
    """).fetchall()
    print("TOP 15 GROWERS (by absolute delta, start_inv > 50K)")
    print(f"{'rpfx':<5} {'start':>12} {'end':>12} {'delta':>12} {'%chg':>8}   name")
    for rpfx, s, e, d, p in rows[:15]:
        name = names.get(rpfx, (None,))[0] or "  (not in Sanity)"
        pct = (p or 0) * 100
        print(f"{rpfx:<5} {s:>12,} {e:>12,} {d:>+12,} {pct:>+7.1f}%   {name}")
    print("\nTOP 15 SHRINKERS (by absolute delta)")
    for rpfx, s, e, d, p in rows[-15:][::-1]:
        name = names.get(rpfx, (None,))[0] or "  (not in Sanity)"
        pct = (p or 0) * 100
        print(f"{rpfx:<5} {s:>12,} {e:>12,} {d:>+12,} {pct:>+7.1f}%   {name}")


def group_validation(con, names):
    section("GROUP VALIDATION (Sanity human-curated groups)")
    groups = load_group_memberships()
    multi_member = {s: g for s, g in groups.items() if len(g["prefixes"]) >= 2}
    print(f"{len(groups)} groups, {len(multi_member)} with 2+ member resporg prefixes.\n")

    rows = con.execute(f"""
        SELECT rpfx, SUM(inventory) AS inv_sum
        FROM read_parquet('{(DATA/"resporg_month.parquet").as_posix()}')
        WHERE month = (SELECT MAX(month) FROM read_parquet('{(DATA/"resporg_month.parquet").as_posix()}'))
        GROUP BY rpfx
    """).fetchall()
    inv_by_pfx = {r[0]: r[1] for r in rows}

    print(f"{'group':<24} {'members':>7} {'combined inventory':>20}   prefixes")
    print("-" * 90)
    for slug, g in sorted(multi_member.items(), key=lambda x: -sum(inv_by_pfx.get(p, 0) for p in x[1]["prefixes"])):
        total = sum(inv_by_pfx.get(p, 0) for p in g["prefixes"])
        pfxs = ", ".join(g["prefixes"])
        print(f"{g['title'][:23]:<24} {len(g['prefixes']):>7} {total:>20,}   {pfxs}")


def main():
    con = duckdb.connect()
    names = load_name_map()
    opportunism_leaderboard(con, names)
    cleanest_carriers(con, names)
    trajectory(con, names)
    group_validation(con, names)


if __name__ == "__main__":
    main()
