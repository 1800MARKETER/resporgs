"""
Build a 2-char-prefix -> Sanity resporg name lookup, and annotate
the top harvesters / net-flow leaders from the 2026-03 -> 2026-04 diff.
"""

from __future__ import annotations
import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_sanity_directory() -> dict[str, list[dict]]:
    """Return {2-char prefix: [resporg-docs]} from the deduped Sanity export."""
    docs = json.loads((ROOT / "clean" / "resporg.json").read_text(encoding="utf-8"))
    by_prefix: dict[str, list[dict]] = defaultdict(list)
    for d in docs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) >= 2:
            by_prefix[code[:2]].append(
                {
                    "title": d.get("title"),
                    "alias": d.get("alias"),
                    "code": code,
                    "slug": (d.get("slug") or {}).get("current"),
                }
            )
    return by_prefix


def load_group_directory() -> dict[str, dict]:
    """
    resporgGroup docs don't carry codes directly — groups are linked by references.
    For now, return {group-title: full-doc}; cross-resolution to codes is Phase 3 work.
    """
    docs = json.loads((ROOT / "clean" / "resporgGroup.json").read_text(encoding="utf-8"))
    return {d.get("title", ""): d for d in docs}


TOP_HARVESTERS = [
    ("MY", 108516, 199560, 45418, 0.5438),
    ("IU", 107297, 118390, 60846, 0.9063),
    ("JW", 45450, 49817, 748, 0.9123),
    ("GA", 35645, 72093, 22822, 0.4944),
    ("NA", 27020, 45794, 3093, 0.5900),
    ("FO", 20663, 22890, 425, 0.9027),
    ("SO", 16354, 23879, 44150, 0.6849),
    ("MT", 15166, 511375, 664237, 0.0297),
    ("CB", 14314, 24370, 520, 0.5874),
    ("ZX", 12452, 14101, 1384, 0.8831),
    ("PJ", 8340, 25154, 11503, 0.3316),
    ("QC", 5156, 504740, 259065, 0.0102),
    ("SS", 4821, 5756, 0, 0.8376),
    ("LQ", 4705, 115537, 38961, 0.0407),
    ("HV", 2801, 5530, 917, 0.5065),
    ("NJ", 1881, 22065, 34, 0.0852),
    ("TU", 1443, 132088, 4, 0.0109),
    ("HL", 1186, 5599, 728, 0.2118),
    ("EF", 835, 35148, 0, 0.0238),
    ("JY", 613, 79166, 181349, 0.0077),
]

TOP_GAINERS = [
    ("QC", 245675), ("FP", 207432), ("MY", 154142), ("TU", 132084),
    ("TW", 95745), ("LQ", 76576), ("HB", 70169), ("IU", 57544),
    ("YV", 49682), ("GA", 49271),
]

TOP_LOSERS = [
    ("AT", -11647), ("GI", -15657), ("SO", -20271), ("GD", -20421),
    ("LE", -38744), ("AU", -50930), ("JY", -102183), ("MT", -152862),
    ("VZ", -207568), ("AL", -330003),
]


def show(label: str, rows, name_col_width=50):
    by_prefix = load_sanity_directory()
    print(f"\n=== {label} ===")
    for row in rows:
        pfx = row[0]
        matches = by_prefix.get(pfx, [])
        if not matches:
            name = "  (no Sanity entry)"
        else:
            parts = []
            for m in matches[:3]:
                t = m["title"] or m["alias"] or "?"
                parts.append(f"{t} [{m['code']}]")
            name = "; ".join(parts)
            if len(matches) > 3:
                name += f"  (+{len(matches)-3} more sub-codes)"
        if isinstance(row[1], int) and len(row) == 2:
            print(f"  {pfx:<4} {row[1]:>+10,}  {name}")
        else:
            harvests, acq, lost, opp = row[1], row[2], row[3], row[4]
            print(f"  {pfx:<4} harvest={harvests:>7,} opp={opp:>6.1%} net={acq-lost:>+9,}  {name}")


def coverage_summary():
    """How many unique prefixes does Sanity cover?"""
    by_prefix = load_sanity_directory()
    total_entries = sum(len(v) for v in by_prefix.values())
    print(f"\n=== Sanity directory coverage ===")
    print(f"  {total_entries} resporg docs cover {len(by_prefix)} unique 2-char prefixes")
    multi = {p: v for p, v in by_prefix.items() if len(v) > 1}
    print(f"  {len(multi)} prefixes have multiple Sanity entries (sub-code listings)")
    if multi:
        top = sorted(multi.items(), key=lambda x: -len(x[1]))[:5]
        print(f"  Top prefixes by Sanity sub-entries:")
        for p, lst in top:
            print(f"    {p}: {len(lst)} entries — {', '.join(e['title'] or '?' for e in lst[:3])}")


def main():
    coverage_summary()
    show("TOP 20 HARVESTERS (Opportunism Index)", TOP_HARVESTERS)
    show("TOP 10 NET-GAINERS", TOP_GAINERS)
    show("TOP 10 NET-LOSERS (shrinking orgs)", TOP_LOSERS)


if __name__ == "__main__":
    main()
