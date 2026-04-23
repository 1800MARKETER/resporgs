"""
First validation spike: diff two consecutive months and report:
  - transfer events (number changed resporg)
  - disconnect landings (from DISCONN with a DIFFERENT resporg prefix -> harvest)
  - reactivations (DISCONN -> WORKING with SAME resporg -> legit reclaim)
  - appearances (from spare)
  - disappearances (to spare)
  - per-resporg Opportunism Index on the one-month window

Processes one prefix at a time to keep memory bounded.
"""

from __future__ import annotations
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from months import inventory, iter_records, PREFIXES, MonthSource  # noqa: E402


def iter_prefix(source: MonthSource, prefix: str):
    """Yield (number, status, resporg) for one prefix of one month."""
    for p, number, status, _date, resporg in iter_records(source):
        if p == prefix:
            yield number, status, resporg


def diff_two_months(src_a: MonthSource, src_b: MonthSource):
    totals = dict(
        transfers=0,
        landings=0,          # transfer where prior status was DISCONN and resporg prefix differs
        reactivations=0,     # DISCONN -> WORKING with same resporg (filter out of harvest metric)
        appeared=0,          # in B, not in A  (came from spare)
        disappeared=0,       # in A, not in B  (went to spare)
        status_changed_same_resporg=0,
        unchanged=0,
    )
    status_pair_counter: Counter = Counter()
    acquired_by: Counter = Counter()   # resporg prefix -> all inventory additions
    harvested_by: Counter = Counter()  # resporg prefix -> disconnect-pool harvests (cross-resporg)
    lost_by: Counter = Counter()       # resporg prefix -> numbers that left them

    for prefix in PREFIXES:
        t0 = time.time()
        a: dict[str, tuple[str, str]] = {}
        for number, status, resporg in iter_prefix(src_a, prefix):
            a[number] = (status, resporg)
        load_s = time.time() - t0

        seen_in_b = 0
        t1 = time.time()
        for number, status_b, resporg_b in iter_prefix(src_b, prefix):
            seen_in_b += 1
            rpfx_b = resporg_b[:2]
            prev = a.pop(number, None)
            if prev is None:
                totals["appeared"] += 1
                acquired_by[rpfx_b] += 1
            else:
                status_a, resporg_a = prev
                rpfx_a = resporg_a[:2]
                if resporg_a != resporg_b:
                    totals["transfers"] += 1
                    acquired_by[rpfx_b] += 1
                    lost_by[rpfx_a] += 1
                    if status_a == "DISCONN" and rpfx_a != rpfx_b:
                        totals["landings"] += 1
                        harvested_by[rpfx_b] += 1
                else:
                    # Same resporg
                    if status_a == "DISCONN" and status_b == "WORKING":
                        totals["reactivations"] += 1
                    if status_a == status_b:
                        totals["unchanged"] += 1
                    else:
                        totals["status_changed_same_resporg"] += 1
                        status_pair_counter[(status_a, status_b)] += 1

        # a now contains only numbers that vanished in b
        for number, (status_a, resporg_a) in a.items():
            totals["disappeared"] += 1
            lost_by[resporg_a[:2]] += 1
        del a

        diff_s = time.time() - t1
        print(
            f"  prefix {prefix}: loaded {seen_in_b:>8,} in {load_s:>5.1f}s, "
            f"diffed in {diff_s:>5.1f}s"
        )

    return totals, status_pair_counter, acquired_by, harvested_by, lost_by


def main():
    month_a = sys.argv[1] if len(sys.argv) > 1 else "2026-03"
    month_b = sys.argv[2] if len(sys.argv) > 2 else "2026-04"

    inv = {s.month: s for s in inventory()}
    if month_a not in inv or month_b not in inv:
        print(f"Missing month. Have: {list(inv)}")
        sys.exit(1)

    print(f"Diffing {month_a} -> {month_b}\n")
    totals, status_pairs, acquired_by, harvested_by, lost_by = diff_two_months(
        inv[month_a], inv[month_b]
    )

    print("\n=== Event totals ===")
    for k, v in totals.items():
        print(f"  {k:<32} {v:>12,}")

    print("\n=== Top status transitions (same resporg) ===")
    for (a, b), n in status_pairs.most_common(10):
        print(f"  {a:<10} -> {b:<10} {n:>10,}")

    print("\n=== Top 20 resporg prefixes by HARVESTS from disconnect pool ===")
    print(f"  {'rpfx':<6} {'harvests':>10} {'acquired':>10} {'lost':>10} {'Opp.Idx':>10}")
    for rpfx, n in harvested_by.most_common(20):
        acq = acquired_by[rpfx]
        lost = lost_by[rpfx]
        opp = n / acq if acq else 0
        print(f"  {rpfx:<6} {n:>10,} {acq:>10,} {lost:>10,} {opp:>10.2%}")

    print("\n=== Top 20 resporg prefixes by NET FLOW (acquired - lost) ===")
    net = Counter()
    for r in set(list(acquired_by) + list(lost_by)):
        net[r] = acquired_by[r] - lost_by[r]
    for rpfx, delta in net.most_common(10):
        print(f"  {rpfx:<6}  +{delta:>8,}")
    print("  ...")
    for rpfx, delta in net.most_common()[-10:]:
        print(f"  {rpfx:<6}  {delta:>+9,}")


if __name__ == "__main__":
    main()
