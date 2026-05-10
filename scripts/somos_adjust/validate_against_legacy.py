"""Validate our pipeline's TFOINPUT/MikeOINPUT against Bud's known-good outputs.

Confirms byte-level correctness modulo the documented Individual-file fix:
    - Bud's MoDatProc.exe applies entries 1-2 of the 7,845-row Individual file
      and silently drops entries 3+. Our pipeline applies all 7,845.
    - Expected diff: ~7,843 rows where our output has GJK01 (correct) and
      Bud's output has the original (wrong) resporg.

Usage:
    # Validate Dec 2025 — Bud's last known-good month:
    python -m scripts.somos_adjust.validate_against_legacy --month 2025-12 \\
        --bud-dir "C:/Users/Bill/Downloads/2025-12"

The script:
    1. Reads our cache/legacy/<MM>-TFOINPUT.txt (must already be generated
       via output_legacy.py or make_month.py)
    2. Reads Bud's TFOINPUT.txt from the supplied --bud-dir
    3. Stream-diffs the two files line-by-line
    4. Prints a summary: total lines, equal lines, differing lines
    5. Bins the differing lines by (our_resporg, their_resporg) so we can
       confirm the diffs are GJK01 fixes (or surface unexpected drift).

Exit code 0 if differences are entirely the expected GJK pattern.
Exit code 1 if there are unexpected drifts (drift report printed).
"""
from __future__ import annotations
import argparse
import re
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OURS_DIR = ROOT / "cache" / "legacy"


def parse_tfo_line(line: str) -> tuple[str, str, str, str, str] | None:
    """Parse one TFOINPUT line. Returns (phone, status, date, fourth, resporg)
    or None if malformed."""
    line = line.rstrip("\r\n")
    if not line:
        return None
    parts = line.split(",")
    if len(parts) < 5:
        return None
    return tuple(p.strip() for p in parts[:5])


def diff_streams(ours_path: Path, bud_path: Path,
                 progress_every: int = 1_000_000) -> dict:
    """Stream-diff two TFOINPUT files. Returns a stats dict."""
    if not ours_path.exists():
        raise SystemExit(f"Our output not found: {ours_path}")
    if not bud_path.exists():
        raise SystemExit(f"Bud's output not found: {bud_path}")

    print(f"Diffing:")
    print(f"  ours: {ours_path} ({ours_path.stat().st_size / 1_048_576:.1f} MB)")
    print(f"  bud : {bud_path} ({bud_path.stat().st_size / 1_048_576:.1f} MB)")

    # Bin diffs by (our_resporg, their_resporg)
    diff_by_resporg_pair: Counter = Counter()
    # Bin diffs by which fields differ (status/date/fourth/resporg)
    diff_by_fields: Counter = Counter()
    # First N example diffs for human inspection
    examples: list[tuple[str, str]] = []
    EXAMPLES_MAX = 30

    total = 0
    equal = 0
    differing = 0
    only_ours = 0
    only_bud = 0
    parse_errors = 0

    with open(ours_path, encoding="ascii") as fa, \
         open(bud_path,  encoding="ascii") as fb:
        while True:
            la = fa.readline()
            lb = fb.readline()
            if not la and not lb:
                break
            if not la:
                only_bud += 1
                lb = lb  # consume rest implicitly via loop
                continue
            if not lb:
                only_ours += 1
                continue

            total += 1
            if la == lb:
                equal += 1
            else:
                differing += 1
                pa = parse_tfo_line(la)
                pb = parse_tfo_line(lb)
                if pa is None or pb is None:
                    parse_errors += 1
                    continue
                # The phone field SHOULD match (same line index = same number)
                # If it doesn't, there's bigger drift (e.g. one side missing rows).
                if pa[0] != pb[0]:
                    diff_by_fields["PHONE_MISMATCH"] += 1
                    if len(examples) < EXAMPLES_MAX:
                        examples.append((la.rstrip("\r\n"), lb.rstrip("\r\n")))
                    continue
                # Bin which other field(s) differ:
                fields = []
                if pa[1] != pb[1]: fields.append("status")
                if pa[2] != pb[2]: fields.append("date")
                if pa[3] != pb[3]: fields.append("fourth")
                if pa[4] != pb[4]: fields.append("resporg")
                diff_by_fields["+".join(fields) or "WHITESPACE_ONLY"] += 1
                # Track resporg pair changes
                if pa[4] != pb[4]:
                    diff_by_resporg_pair[(pb[4], pa[4])] += 1  # (their, ours)
                if len(examples) < EXAMPLES_MAX:
                    examples.append((la.rstrip("\r\n"), lb.rstrip("\r\n")))

            if total % progress_every == 0:
                print(f"  scanned {total:,}  equal {equal:,}  diff {differing:,}")

    return {
        "total": total,
        "equal": equal,
        "differing": differing,
        "only_ours": only_ours,
        "only_bud": only_bud,
        "parse_errors": parse_errors,
        "diff_by_fields": diff_by_fields,
        "diff_by_resporg_pair": diff_by_resporg_pair,
        "examples": examples,
    }


def print_report(stats: dict) -> int:
    """Pretty-print the diff stats. Returns exit code."""
    print("\n" + "=" * 64)
    print("  DIFF SUMMARY")
    print("=" * 64)
    total = stats["total"]
    equal = stats["equal"]
    differing = stats["differing"]
    print(f"  total lines compared   : {total:>12,}")
    print(f"  byte-equal             : {equal:>12,}  ({equal/max(total,1)*100:.4f}%)")
    print(f"  differing              : {differing:>12,}  ({differing/max(total,1)*100:.4f}%)")
    print(f"  only in ours           : {stats['only_ours']:>12,}")
    print(f"  only in bud's          : {stats['only_bud']:>12,}")
    print(f"  parse errors           : {stats['parse_errors']:>12,}")

    if stats["diff_by_fields"]:
        print("\n  diffs by field set:")
        for fields, n in stats["diff_by_fields"].most_common():
            print(f"    {fields:>40}  : {n:>10,}")

    pair_counter = stats["diff_by_resporg_pair"]
    if pair_counter:
        print("\n  diffs by (their_resporg -> our_resporg):")
        for (theirs, ours), n in pair_counter.most_common(20):
            arrow = "->" if theirs and ours else "  "
            print(f"    {theirs!r:>10} {arrow} {ours!r:>10}  : {n:>10,}")

    examples = stats["examples"]
    if examples:
        print(f"\n  first {len(examples)} differing-line examples (ours / bud's):")
        for la, lb in examples[:10]:
            print(f"    OURS: {la}")
            print(f"    BUD : {lb}")
            print()

    # Determine pass/fail. The expected diff is GJK01 fixes only.
    if differing == 0:
        print("RESULT: Byte-identical. (Including the Individual file - "
              "either Bud's bug isn't triggered for this fixture, or "
              "we're missing an override pass.)")
        return 0

    expected_only = True
    expected_count = 0
    unexpected_count = 0
    for (theirs, ours), n in pair_counter.items():
        if ours == "GJK01":
            expected_count += n
        else:
            expected_only = False
            unexpected_count += n

    # Plus consider field-set composition: only "resporg" is expected
    only_resporg_diffs = stats["diff_by_fields"].get("resporg", 0)
    other_diffs = differing - only_resporg_diffs
    if other_diffs:
        print(f"\n  WARNING: {other_diffs:,} diffs touch fields other than resporg.")

    if expected_only and other_diffs == 0:
        print(f"\nRESULT: PASS — all {expected_count:,} diffs are GJK01 fixes "
              "(the Individual-file bug correction). Pipeline matches Bud's "
              "output otherwise byte-for-byte.")
        return 0

    print(f"\nRESULT: UNEXPECTED DRIFT — {unexpected_count:,} diffs are not "
          f"GJK01 fixes; {other_diffs:,} touch non-resporg fields. "
          "Investigate the field-set table and resporg-pair table above.")
    return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--ours-dir", type=Path, default=DEFAULT_OURS_DIR,
                        help="Where our pipeline writes <MM>-TFOINPUT.txt "
                             "(default: cache/legacy/)")
    parser.add_argument("--bud-dir", type=Path, required=True,
                        help="Folder containing Bud's TFOINPUT.txt to diff against")
    parser.add_argument("--mike", action="store_true",
                        help="Diff MikeOINPUT.txt instead of TFOINPUT.txt")
    args = parser.parse_args()

    if not re.match(r"^\d{4}-\d{2}$", args.month):
        parser.error("--month must be YYYY-MM")

    if args.mike:
        ours_path = args.ours_dir / f"{args.month}-MikeOINPUT.txt"
        bud_path = args.bud_dir / "MikeOINPUT.txt"
    else:
        ours_path = args.ours_dir / f"{args.month}-TFOINPUT.txt"
        bud_path = args.bud_dir / "TFOINPUT.txt"

    stats = diff_streams(ours_path, bud_path)
    rc = print_report(stats)
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
