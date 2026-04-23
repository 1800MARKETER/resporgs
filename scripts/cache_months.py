"""
Cache each monthly snapshot as a compact Parquet file.
One file per month, all 7 prefix files unioned together.

Schema:
  number      uint64   (digits only, e.g. 8002000000)
  prefix      uint16   (800, 833, 844, 855, 866, 877, 888)
  status      uint8    (enum: see STATUS_CODES)
  resporg     str (5 chars)  -- full 5-char code
  rpfx        str (2 chars)  -- first 2 chars (resporg identity)
  change_yy   uint8    (last-change year, e.g. 25 for 2025)
  change_mm   uint8
  change_dd   uint8
"""

from __future__ import annotations
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from months import inventory, iter_records, MonthSource, PREFIXES  # noqa: E402

CACHE = ROOT / "cache"
CACHE.mkdir(exist_ok=True)

STATUS_CODES = {
    "WORKING": 1,
    "TRANSIT": 2,
    "DISCONN": 3,
    "RESERVED": 4,
    "UNAVAIL": 5,
    "ASSIGNED": 6,
    "SUSPEND": 7,
}


def parse_number(s: str) -> int:
    # "800-200-0000" -> 8002000000
    return int(s.replace("-", ""))


def parse_date(s: str) -> tuple[int, int, int]:
    # "25/04/15" -> (25, 4, 15)
    try:
        yy, mm, dd = s.split("/")
        return int(yy), int(mm), int(dd)
    except Exception:
        return (0, 0, 0)


def build_month(source: MonthSource, overwrite: bool = False):
    out = CACHE / f"{source.month}.parquet"
    if out.exists() and not overwrite:
        print(f"  {source.month}: already cached ({out.stat().st_size / 1e6:.1f} MB)")
        return
    if not source.complete():
        missing = [p for p in PREFIXES if p not in source.cd_rom_files]
        print(f"  {source.month}: SKIPPED — missing prefix(es) {missing}")
        return

    t0 = time.time()
    numbers: list[int] = []
    prefixes: list[int] = []
    statuses: list[int] = []
    resporgs: list[str] = []
    rpfxs: list[str] = []
    yys: list[int] = []
    mms: list[int] = []
    dds: list[int] = []

    for prefix, number, status, date, resporg in iter_records(source):
        try:
            numbers.append(parse_number(number))
            prefixes.append(int(prefix))
            statuses.append(STATUS_CODES.get(status, 0))
            resporgs.append(resporg)
            rpfxs.append(resporg[:2])
            yy, mm, dd = parse_date(date)
            yys.append(yy)
            mms.append(mm)
            dds.append(dd)
        except Exception:
            continue

    tbl = pa.table(
        {
            "number": pa.array(numbers, type=pa.uint64()),
            "prefix": pa.array(prefixes, type=pa.uint16()),
            "status": pa.array(statuses, type=pa.uint8()),
            "resporg": pa.array(resporgs, type=pa.string()),
            "rpfx": pa.array(rpfxs, type=pa.string()),
            "yy": pa.array(yys, type=pa.uint8()),
            "mm": pa.array(mms, type=pa.uint8()),
            "dd": pa.array(dds, type=pa.uint8()),
        }
    )
    pq.write_table(tbl, out, compression="zstd", compression_level=6)

    dur = time.time() - t0
    size_mb = out.stat().st_size / 1e6
    print(
        f"  {source.month}: {len(numbers):>10,} rows, "
        f"{size_mb:>6.1f} MB, {dur:>5.1f}s"
    )


def main():
    inv = inventory()
    print(f"Caching {len(inv)} months to {CACHE}")
    t0 = time.time()
    for src in inv:
        build_month(src)
    total = time.time() - t0
    print(f"\nDone in {total:.1f}s")
    print("\nCache contents:")
    for p in sorted(CACHE.glob("*.parquet")):
        print(f"  {p.name}: {p.stat().st_size / 1e6:>6.1f} MB")


if __name__ == "__main__":
    main()
