"""Phase 5 step 3: orchestrator for the Somos monthly adjustment pipeline.

Reads the 7 per-AC CD-ROM files for a month, applies the four control passes
(RestrictedACExc, RO2RO, RO2Stat, Individual), fills AVAIL gap-fillers for
every missing number in 200-0000..999-9999, and writes a single canonical
adjusted parquet to RESPORGS/cache/adjusted/<YYYY-MM>.parquet.

Schema:
    number   uint64    10-digit phone as int (8002000000)
    prefix   uint16    area code (800, 833, ...)
    status   string    canonical status (WORKING, DISCONN, RESERVED, AVAIL, ...)
    resporg  string    5-char post-adjustment resporg/vendor code (or '')
    rpfx     string    first 2 chars of resporg
    date     string    'YY/MM/DD' or ''
    fourth   string    2-char template/age code or ''

Run from RESPORGS root:
    python -m scripts.somos_adjust.build_adjusted --month 2026-05 \\
        --input-dir "C:\\Users\\Bill\\Downloads\\2026-05"
"""
from __future__ import annotations
import argparse
import re
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .control_files import load_all, ControlBundle, DEFAULT_CTRL_DIR
from .adjustments import apply_all

ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = ROOT / "cache" / "adjusted"
PREFIXES = ("800", "833", "844", "855", "866", "877", "888")

SCHEMA = pa.schema([
    ("number", pa.uint64()),
    ("prefix", pa.uint16()),
    ("status", pa.string()),
    ("resporg", pa.string()),
    ("rpfx", pa.string()),
    ("date", pa.string()),
    ("fourth", pa.string()),
])


# ---------------------------------------------------------------------------
# Input file discovery + parsing
# ---------------------------------------------------------------------------

def find_cd_rom_files(input_dir: Path) -> dict[str, Path]:
    """{ac: path} for the 7 CD-ROM files. Per Bud's spec: filenames must
    start with 'CD-ROM_TFN_Report_' (18 chars) and chars 19-21 are the AC."""
    found: dict[str, Path] = {}
    if not input_dir.exists():
        return found
    for p in sorted(input_dir.iterdir()):
        name = p.name
        if len(name) < 21 or name[:18] != "CD-ROM_TFN_Report_":
            continue
        ac = name[18:21]
        if ac in PREFIXES and ac not in found:
            found[ac] = p
    return found


def parse_input_line(line: str) -> dict | None:
    """Parse one CD-ROM record. Returns None on malformed lines.

    Format: '800-200-0000  ,WORKING ,26/01/27,49,RBI01'
    Status field width is 8 (7-char status + space); we strip to canonical.
    """
    line = line.rstrip("\r\n")
    if not line:
        return None
    parts = line.split(",")
    if len(parts) < 5:
        return None
    digits = parts[0].strip().replace("-", "")
    if len(digits) != 10 or not digits.isdigit():
        return None
    return {
        "digits": digits,
        "status": parts[1].strip(),     # canonical: 'WORKING', 'RESERVED', etc.
        "date": parts[2].strip(),
        "fourth": parts[3].strip(),
        "resporg": parts[4].strip(),
    }


def make_avail(number_int: int) -> dict:
    """Synthetic AVAIL record for a gap-fill number."""
    return {
        "digits": f"{number_int:010d}",
        "status": "AVAIL",
        "date": "",
        "fourth": "",
        "resporg": "",
    }


# ---------------------------------------------------------------------------
# Per-AC processing
# ---------------------------------------------------------------------------

def process_ac_file(ac: str, path: Path, bundle: ControlBundle, limit: int | None = None):
    """Yield adjusted records for one AC, including AVAIL gap-fillers.

    Per Bud: each AC covers 200-0000..999-9999 inclusive (8M numbers).
    We yield one record per number, in ascending order.
    """
    expected = int(f"{ac}2000000")
    end_num = int(f"{ac}9999999")
    last_input_num = expected - 1

    yielded = 0

    with open(path, encoding="utf-8") as f:
        for raw in f:
            rec = parse_input_line(raw)
            if rec is None:
                continue
            input_num = int(rec["digits"])
            if input_num <= last_input_num:
                # Out-of-sequence or duplicate — skip per validation policy
                continue
            if input_num < expected:
                # Predates the AC's coverage window (shouldn't happen)
                last_input_num = input_num
                continue
            if input_num > end_num:
                # Past the end of this AC
                break

            # Fill AVAIL records up to but not including the input number
            while expected < input_num:
                yield apply_all(make_avail(expected), bundle.ac_exc, bundle.ro2ro,
                                bundle.ro2stat, bundle.individual)
                expected += 1
                yielded += 1
                if limit and yielded >= limit:
                    return

            # Process the input record
            yield apply_all(rec, bundle.ac_exc, bundle.ro2ro,
                            bundle.ro2stat, bundle.individual)
            expected = input_num + 1
            last_input_num = input_num
            yielded += 1
            if limit and yielded >= limit:
                return

    # Fill remaining AVAIL records to end_num
    while expected <= end_num:
        yield apply_all(make_avail(expected), bundle.ac_exc, bundle.ro2ro,
                        bundle.ro2stat, bundle.individual)
        expected += 1
        yielded += 1
        if limit and yielded >= limit:
            return


def write_ac_to_parquet(writer: pq.ParquetWriter, ac: str, path: Path,
                        bundle: ControlBundle, limit: int | None = None) -> int:
    """Process one AC, append batch to the writer. Returns row count.

    Builds columnar arrays in memory per-AC (~8M rows, ~500MB peak).
    """
    numbers: list[int] = []
    statuses: list[str] = []
    resporgs: list[str] = []
    rpfxs: list[str] = []
    dates: list[str] = []
    fourths: list[str] = []
    ac_int = int(ac)

    t0 = time.time()
    count = 0
    last_progress_count = 0
    last_progress_t = t0

    for rec in process_ac_file(ac, path, bundle, limit=limit):
        numbers.append(int(rec["digits"]))
        statuses.append(rec["status"])
        resp = rec["resporg"]
        resporgs.append(resp)
        rpfxs.append(resp[:2] if len(resp) >= 2 else "")
        dates.append(rec["date"])
        fourths.append(rec["fourth"])
        count += 1
        if count - last_progress_count >= 1_000_000:
            now = time.time()
            rate = (count - last_progress_count) / (now - last_progress_t)
            print(f"    {ac}: {count:,} records ({rate/1000:.0f}K rec/s)")
            last_progress_count = count
            last_progress_t = now

    n = len(numbers)
    table = pa.table({
        "number": pa.array(numbers, type=pa.uint64()),
        "prefix": pa.array([ac_int] * n, type=pa.uint16()),
        "status": pa.array(statuses, type=pa.string()),
        "resporg": pa.array(resporgs, type=pa.string()),
        "rpfx": pa.array(rpfxs, type=pa.string()),
        "date": pa.array(dates, type=pa.string()),
        "fourth": pa.array(fourths, type=pa.string()),
    }, schema=SCHEMA)
    writer.write_table(table)

    elapsed = time.time() - t0
    print(f"    {ac}: wrote {count:,} records in {elapsed:.1f}s")
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--input-dir", required=True, type=Path,
                        help="Directory containing 7 CD-ROM_TFN_Report_*.txt files")
    parser.add_argument("--ctrl-dir", default=DEFAULT_CTRL_DIR, type=Path,
                        help="Directory containing the 4 control files (default: C:\\MonthlyProcessing2)")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, type=Path,
                        help="Directory to write the per-month parquet (default: RESPORGS/cache/adjusted)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit records per AC (for testing)")
    parser.add_argument("--ac", default=None, choices=PREFIXES,
                        help="Process only one AC (for testing)")
    args = parser.parse_args()

    if not re.match(r"^\d{4}-\d{2}$", args.month):
        parser.error("--month must be YYYY-MM")

    print(f"Loading control files from {args.ctrl_dir}...")
    bundle = load_all(args.ctrl_dir)
    print(f"  {len(bundle.ac_exc)} AcExc | {len(bundle.ro2ro)} RO2RO | "
          f"{len(bundle.ro2stat)} RO2Stat | {len(bundle.individual)} Individual")

    print(f"Finding CD-ROM files in {args.input_dir}...")
    cd_rom = find_cd_rom_files(args.input_dir)
    if args.ac:
        if args.ac not in cd_rom:
            parser.error(f"No CD-ROM file found for AC {args.ac}")
        cd_rom = {args.ac: cd_rom[args.ac]}
        print(f"  Limiting to AC {args.ac}")
    else:
        missing = [p for p in PREFIXES if p not in cd_rom]
        if missing:
            parser.error(f"Missing CD-ROM files for area codes: {missing}")
    for ac in sorted(cd_rom):
        print(f"  {ac}: {cd_rom[ac].name}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    suffix = ""
    if args.ac:
        suffix = f"-{args.ac}"
    if args.limit:
        suffix += f"-limit{args.limit}"
    out_path = args.output_dir / f"{args.month}{suffix}.parquet"

    print(f"Writing to {out_path}...")
    t0 = time.time()
    writer = pq.ParquetWriter(str(out_path), SCHEMA, compression="zstd")
    total = 0
    try:
        for ac in PREFIXES:
            if ac not in cd_rom:
                continue
            print(f"  AC {ac}: {cd_rom[ac].name}")
            n = write_ac_to_parquet(writer, ac, cd_rom[ac], bundle, limit=args.limit)
            total += n
    finally:
        writer.close()

    size_mb = out_path.stat().st_size / 1_048_576
    elapsed = time.time() - t0
    print(f"\nDone. {total:,} records, {size_mb:.1f} MB, {elapsed:.1f}s.")
    print(f"Output: {out_path}")


if __name__ == "__main__":
    main()
