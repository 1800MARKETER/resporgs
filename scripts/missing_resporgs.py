"""
Scan the latest monthly snapshot for all 2-char resporg prefixes,
cross-reference against the Sanity directory, and output the prefixes
with no Sanity entry — sorted by total inventory (biggest first).
"""

from __future__ import annotations
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from months import inventory, iter_records  # noqa: E402


def sanity_prefixes() -> set[str]:
    docs = json.loads((ROOT / "clean" / "resporg.json").read_text(encoding="utf-8"))
    pfxs = set()
    for d in docs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) >= 2:
            pfxs.add(code[:2])
    return pfxs


def main(month_name: str = "2026-04"):
    inv = {s.month: s for s in inventory()}
    if month_name not in inv:
        print(f"Month {month_name} not in inventory")
        sys.exit(1)
    src = inv[month_name]

    print(f"Scanning {month_name}...")
    counts_by_prefix: Counter = Counter()
    sample_codes_by_prefix: dict[str, set[str]] = defaultdict(set)
    status_by_prefix: dict[str, Counter] = defaultdict(Counter)

    for _p, _number, status, _date, resporg in iter_records(src):
        if len(resporg) < 2:
            continue
        pfx = resporg[:2]
        counts_by_prefix[pfx] += 1
        if len(sample_codes_by_prefix[pfx]) < 5:
            sample_codes_by_prefix[pfx].add(resporg)
        status_by_prefix[pfx][status] += 1

    covered = sanity_prefixes()
    missing = [
        (p, n) for p, n in counts_by_prefix.most_common() if p not in covered
    ]

    print(f"\nTotal distinct 2-char prefixes in {month_name}: {len(counts_by_prefix)}")
    print(f"Covered by Sanity: {len(covered & set(counts_by_prefix))}")
    print(f"Missing from Sanity: {len(missing)}")
    total_missing_inventory = sum(n for _, n in missing)
    total_inventory = sum(counts_by_prefix.values())
    pct = 100.0 * total_missing_inventory / total_inventory if total_inventory else 0
    print(
        f"Missing prefixes account for {total_missing_inventory:,} numbers "
        f"({pct:.1f}% of total active inventory {total_inventory:,})"
    )

    out_txt = ROOT / "data" / "missing_resporgs.txt"
    out_txt.parent.mkdir(exist_ok=True)
    with out_txt.open("w", encoding="utf-8") as f:
        f.write(
            f"Missing resporg prefixes from Sanity directory "
            f"(scanned {month_name}), sorted by total inventory.\n"
        )
        f.write(
            f"{'prefix':<6} {'inventory':>10} {'working':>10} {'transit':>8} "
            f"{'disconn':>8}   sample sub-codes\n"
        )
        f.write("-" * 90 + "\n")
        for p, n in missing:
            st = status_by_prefix[p]
            samples = ", ".join(sorted(sample_codes_by_prefix[p]))
            f.write(
                f"{p:<6} {n:>10,} {st.get('WORKING',0):>10,} "
                f"{st.get('TRANSIT',0):>8,} {st.get('DISCONN',0):>8,}   {samples}\n"
            )

    print(f"\nWrote full list to {out_txt}")
    print("\n=== TOP 40 MISSING PREFIXES (by total inventory) ===")
    print(
        f"{'prefix':<6} {'inventory':>10} {'working':>10} {'disconn':>8}   sample codes"
    )
    print("-" * 80)
    for p, n in missing[:40]:
        st = status_by_prefix[p]
        samples = ", ".join(sorted(sample_codes_by_prefix[p]))
        print(
            f"{p:<6} {n:>10,} {st.get('WORKING',0):>10,} "
            f"{st.get('DISCONN',0):>8,}   {samples}"
        )


if __name__ == "__main__":
    month = sys.argv[1] if len(sys.argv) > 1 else "2026-04"
    main(month)
