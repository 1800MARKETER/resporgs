"""Phase 5 step 4: emit byte-compat TFOINPUT.txt + MikeOINPUT.txt from the
adjusted parquet.

Goal: outputs structurally indistinguishable from Bud's MoDatProc.exe outputs,
EXCEPT for the ~7,843 GJK Individual rows that Bud's bug drops (we include
them, correctly remapped). That's the only legitimate diff.

Output formats (verified against Bud's 2026-05 outputs, byte-level):

TFOINPUT.txt — fixed-width, 41-char content + CRLF (43 bytes/record)
    AAA-EEE-NNNN  ,STATUS  ,YY/MM/DD,4T,RESPO\r\n
    │       12  │ 3│  7  │ 2│   8  │1│ 2│1│ 5 │
    Status is first 7 chars of (canonical_status + spaces).
    Date/4th/Resporg pad with spaces to fixed width.

MikeOINPUT.txt — variable-width CSV
    AAAEEENNNN,STATUS,YY/MM/DD,4T,RESPORG\r\n
    Status is the canonical (un-truncated, e.g. RESERVED stays RESERVED).
    Empty fields appear as adjacent commas.

Run from RESPORGS root:
    python -m scripts.somos_adjust.output_legacy --month 2026-05
"""
from __future__ import annotations
import argparse
import re
import time
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_INPUT_DIR = ROOT / "cache" / "adjusted"
DEFAULT_OUTPUT_DIR = ROOT / "cache" / "legacy"


def number_to_dashed(n: int) -> str:
    """8002000000 -> '800-200-0000'"""
    s = f"{n:010d}"
    return f"{s[0:3]}-{s[3:6]}-{s[6:10]}"


def make_tfo_line(num: int, status: str, date: str, fourth: str, resporg: str) -> str:
    """Bud's TFOINPUT format. Status truncated to 7 chars; other fields padded."""
    phone = number_to_dashed(num)
    s = (status + "        ")[:7]   # 7-char window, space-padded
    d = (date + "          ")[:8]
    f = (fourth + "          ")[:2]
    r = (resporg + "          ")[:5]
    return f"{phone}  ,{s} ,{d},{f},{r}"


def make_mike_line(num: int, status: str, date: str, fourth: str, resporg: str) -> str:
    """Bud's MikeOINPUT format. Variable-width CSV, full canonical status."""
    digits = f"{num:010d}"
    return f"{digits},{status},{date},{fourth},{resporg}"


def emit(month: str, input_dir: Path, output_dir: Path,
         emit_tfo: bool = True, emit_mike: bool = True, limit: int | None = None):
    src = input_dir / f"{month}.parquet"
    if not src.exists():
        raise SystemExit(f"Adjusted parquet not found: {src}")

    output_dir.mkdir(parents=True, exist_ok=True)
    tfo_path = output_dir / f"{month}-TFOINPUT.txt"
    mike_path = output_dir / f"{month}-MikeOINPUT.txt"

    print(f"Reading {src} ({src.stat().st_size / 1_048_576:.1f} MB)...")
    pf = pq.ParquetFile(str(src))

    tfo = open(tfo_path, "w", encoding="ascii", newline="\r\n") if emit_tfo else None
    mike = open(mike_path, "w", encoding="ascii", newline="\r\n") if emit_mike else None

    t0 = time.time()
    total = 0
    last_progress_t = t0
    last_progress_count = 0

    try:
        for batch in pf.iter_batches(batch_size=200_000,
                                     columns=["number", "status", "resporg", "date", "fourth"]):
            nums = batch.column("number").to_pylist()
            statuses = batch.column("status").to_pylist()
            resporgs = batch.column("resporg").to_pylist()
            dates = batch.column("date").to_pylist()
            fourths = batch.column("fourth").to_pylist()
            for i in range(len(nums)):
                if tfo is not None:
                    tfo.write(make_tfo_line(nums[i], statuses[i], dates[i],
                                             fourths[i], resporgs[i]) + "\n")
                if mike is not None:
                    mike.write(make_mike_line(nums[i], statuses[i], dates[i],
                                               fourths[i], resporgs[i]) + "\n")
                total += 1
                if limit and total >= limit:
                    raise StopIteration

            if total - last_progress_count >= 1_000_000:
                now = time.time()
                rate = (total - last_progress_count) / (now - last_progress_t)
                print(f"  {total:,} records ({rate/1000:.0f}K rec/s)")
                last_progress_count = total
                last_progress_t = now
    except StopIteration:
        pass
    finally:
        if tfo is not None:
            tfo.close()
        if mike is not None:
            mike.close()

    elapsed = time.time() - t0
    print(f"\nDone. {total:,} records in {elapsed:.1f}s.")
    if emit_tfo:
        print(f"  {tfo_path}: {tfo_path.stat().st_size / 1_048_576:.1f} MB")
    if emit_mike:
        print(f"  {mike_path}: {mike_path.stat().st_size / 1_048_576:.1f} MB")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, type=Path)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, type=Path)
    parser.add_argument("--no-tfo", action="store_true", help="Skip TFOINPUT.txt")
    parser.add_argument("--no-mike", action="store_true", help="Skip MikeOINPUT.txt")
    parser.add_argument("--limit", type=int, default=None, help="Limit records (testing)")
    args = parser.parse_args()

    # The leading YYYY-MM is the production form, but we accept any stem so
    # test fixtures (e.g. "2026-05-800-limit300000") work.
    if not re.match(r"^\d{4}-\d{2}", args.month):
        parser.error("--month must start with YYYY-MM")

    emit(args.month, args.input_dir, args.output_dir,
         emit_tfo=not args.no_tfo, emit_mike=not args.no_mike,
         limit=args.limit)


if __name__ == "__main__":
    main()
