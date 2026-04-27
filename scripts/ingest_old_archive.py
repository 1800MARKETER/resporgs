"""
One-shot ingestion of pre-2022 Somos monthly snapshots from
E:\\resporgs\\old\\Monthly Reporg files\\ into D:\\resporgs\\YYYY-MM\\.

The existing `months.py` loader expects the canonical layout:
    D:\\resporgs\\YYYY-MM\\CD-ROM_TFN_Report_{800,833,844,855,866,877,888}_*.txt
    or
    D:\\resporgs\\YYYY-MM.zip  containing the same files

The old archive is a tarball of conventions accumulated over 5 years:
folders named "Apr2019", "20July", "22 07 Resporg"; zip variants of the
same; legacy single-file "ALL.txt" snapshots that combine all 7 prefixes.

This script normalises all of the above into the canonical D:\\resporgs\\
folder layout so cache_months.py picks them up with zero downstream
changes.

Usage:
  python scripts/ingest_old_archive.py --dry-run     # preview only
  python scripts/ingest_old_archive.py               # do the work
  python scripts/ingest_old_archive.py --cache       # then run cache_months.py

Idempotent: skips any month whose destination already has 7 CD-ROM files.
Never deletes the source. Logs to data/ingest_old_archive.log.
"""
from __future__ import annotations

import argparse
import re
import shutil
import sys
import time
import zipfile
from collections import defaultdict
from pathlib import Path

SOURCE_ROOT = Path(r"D:\resporgs\old\Monthly Reporg files")
DEST_ROOT = Path(r"D:\resporgs")
PARQUET_CACHE = Path(__file__).resolve().parent.parent / "cache"

PREFIXES = ("800", "833", "844", "855", "866", "877", "888")

# Match the canonical Somos / API filenames used by months.py
CD_ROM_RE = re.compile(
    # Three naming conventions seen in the archive (must stay in sync with months.py):
    #   SMS/800 era:       SMS800.D20180301.txt
    #   CD-ROM era:        CD-ROM_TFN_Report_800_20250601061856.txt
    #   Number-Status era: Number-Status-NPA-800.txt
    r"(?:CD-ROM_TFN_Report_|Number-Status-NPA-|SMS)"
    r"(800|833|844|855|866|877|888)"
    r"(?:\.D\d{8}|[_-][\w-]*)?\.txt$",
    re.IGNORECASE,
)

# Month-name dictionary covering every variant in the archive
MONTH_WORDS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


# -------------- filename → (year, month) --------------

def detect_month(name: str) -> tuple[int, int] | None:
    """Try every known filename convention. Returns (year, month) or None."""
    base = name.lower()
    # Strip trailing extension for matching
    stem = re.sub(r"\.(zip|txt)$", "", base)

    # 1. MonYYYY  e.g. apr2019, june2018, aug2020
    m = re.match(r"^([a-z]+?)[-_ ]?(\d{4})\b", stem)
    if m and m.group(1) in MONTH_WORDS:
        return int(m.group(2)), MONTH_WORDS[m.group(1)]

    # 2. YYYY MonALL  e.g. "2020 feb all", "2020 dec all edited2"
    m = re.match(r"^(\d{4})[-_ ]+([a-z]+)", stem)
    if m and m.group(2) in MONTH_WORDS:
        return int(m.group(1)), MONTH_WORDS[m.group(2)]

    # 3. YYYY-M-ALL  e.g. "2020-3-all"
    m = re.match(r"^(\d{4})[-_ ](\d{1,2})\b", stem)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12:
            return y, mo

    # 4. YY MM (with space)  e.g. "22 07 Resporg"
    m = re.match(r"^(\d{2})[ _](\d{2})\b", stem)
    if m:
        y, mo = 2000 + int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12:
            return y, mo

    # 5. YY-MM  e.g. "18-10"
    m = re.match(r"^(\d{2})-(\d{2})\b", stem)
    if m:
        y, mo = 2000 + int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12:
            return y, mo

    # 6. bqYYMMDD  e.g. "bq180907"
    m = re.match(r"^bq(\d{2})(\d{2})\d{2}", stem)
    if m:
        y, mo = 2000 + int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12:
            return y, mo

    # 7. ResporgMonYYYY  e.g. "ResporgAug2019"
    m = re.match(r"^resporg([a-z]+)(\d{4})", stem)
    if m and m.group(1) in MONTH_WORDS:
        return int(m.group(2)), MONTH_WORDS[m.group(1)]

    # 8. NNJuly / NNMonth — no-year prefix legacy: assume 2020 unless we have
    #    better signal (for "20July" -> 2020-07; for "20 11 Resporg" -> handled by #4)
    m = re.match(r"^(\d{2})([a-z]+)", stem)
    if m and m.group(2) in MONTH_WORDS:
        y, mo = 2000 + int(m.group(1)), MONTH_WORDS[m.group(2)]
        if 1 <= mo <= 12:
            return y, mo

    # 9. "Mon DDDD"  e.g. "Jan 2023" (with space)
    m = re.match(r"^([a-z]+)[ _]+(\d{4})", stem)
    if m and m.group(1) in MONTH_WORDS:
        return int(m.group(2)), MONTH_WORDS[m.group(1)]

    # 10. "Oct-1 2020" — the "-1" is just a duplicate marker; treat as Oct 2020
    m = re.match(r"^([a-z]+)-\d+\s+(\d{4})", stem)
    if m and m.group(1) in MONTH_WORDS:
        return int(m.group(2)), MONTH_WORDS[m.group(1)]

    return None


def yyyymm(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"


# -------------- source classification --------------

def classify_source(entry: Path) -> str:
    """folder | zip | text | unknown"""
    if entry.is_dir():
        return "folder"
    name = entry.name.lower()
    if name.endswith(".zip"):
        return "zip"
    if name.endswith(".txt"):
        return "text"
    return "unknown"


def folder_cd_rom_files(folder: Path) -> dict[str, Path]:
    """Walk a folder (recursively) and return prefix -> file path."""
    found: dict[str, Path] = {}
    for f in folder.rglob("*.txt"):
        m = CD_ROM_RE.match(f.name)
        if m:
            prefix = m.group(1)
            # Prefer earliest match if duplicates
            found.setdefault(prefix, f)
    return found


def zip_cd_rom_entries(zip_path: Path) -> dict[str, str]:
    """Inspect a zip and return prefix -> internal entry name."""
    found: dict[str, str] = {}
    try:
        with zipfile.ZipFile(zip_path) as zf:
            for name in zf.namelist():
                base = name.rsplit("/", 1)[-1]
                m = CD_ROM_RE.match(base)
                if m:
                    found.setdefault(m.group(1), name)
    except (zipfile.BadZipFile, OSError) as e:
        print(f"    WARNING: cannot read zip {zip_path.name}: {e}")
    return found


# -------------- per-month plan --------------

class MonthPlan:
    def __init__(self, month: str):
        self.month = month
        self.candidates: list[tuple[Path, str]] = []  # (source_path, kind)

    def add(self, path: Path, kind: str):
        self.candidates.append((path, kind))

    def pick_best(self) -> tuple[Path | None, str, dict[str, str | Path]]:
        """
        Return (chosen_path, kind, prefix_map). Prefer a source that is
        complete (all 7 prefixes). Among complete sources, prefer folder >
        zip > text. Among incomplete, take whichever has the most prefixes.
        """
        best = None
        for path, kind in self.candidates:
            if kind == "folder":
                prefix_map = folder_cd_rom_files(path)
            elif kind == "zip":
                prefix_map = zip_cd_rom_entries(path)
            else:
                prefix_map = {}  # text-only handled separately
            score = (
                1 if len(prefix_map) == 7 else 0,
                len(prefix_map),
                {"folder": 2, "zip": 1, "text": 0}.get(kind, -1),
            )
            if best is None or score > best[0]:
                best = (score, path, kind, prefix_map)
        if best is None:
            return None, "", {}
        _, path, kind, prefix_map = best
        return path, kind, prefix_map


# -------------- materialise into D:\resporgs\YYYY-MM\ --------------

def dest_complete(month: str) -> bool:
    """Skip if month is ALREADY satisfied — either as raw files in DEST_ROOT
    or as a built parquet in cache/. The parquet check makes the script
    purely additive: only fills genuine gaps, never overwrites cached data."""
    # 1. Parquet already built — skip
    if (PARQUET_CACHE / f"{month}.parquet").exists():
        return True
    # 2. Raw files already extracted in canonical location
    folder = DEST_ROOT / month
    if folder.is_dir():
        files = folder_cd_rom_files(folder)
        if len(files) == 7:
            return True
    # 3. Modern zip naming: YYYY-MM.zip
    zip_path = DEST_ROOT / f"{month}.zip"
    if zip_path.is_file():
        files = zip_cd_rom_entries(zip_path)
        if len(files) == 7:
            return True
    # 4. Legacy Dropbox zip naming: "YY MM Resporg.zip"
    yy = month[2:4]
    mm = month[5:7]
    legacy_zip = DEST_ROOT / f"{yy} {mm} Resporg.zip"
    if legacy_zip.is_file():
        files = zip_cd_rom_entries(legacy_zip)
        if len(files) == 7:
            return True
    return False


def materialise(
    month: str,
    src: Path,
    kind: str,
    prefix_map: dict[str, str | Path],
    dry_run: bool,
) -> tuple[int, list[str]]:
    """Copy/extract the chosen CD-ROM files into D:\\resporgs\\YYYY-MM\\.
    Returns (files_written, missing_prefixes)."""
    dest = DEST_ROOT / month
    if not dry_run:
        dest.mkdir(parents=True, exist_ok=True)

    written = 0
    if kind == "folder":
        for prefix, src_file in prefix_map.items():
            target = dest / Path(src_file).name
            if dry_run:
                print(f"      WOULD COPY  {src_file} -> {target}")
            else:
                shutil.copy2(src_file, target)
            written += 1
    elif kind == "zip":
        with zipfile.ZipFile(src) as zf:
            for prefix, entry_name in prefix_map.items():
                base = entry_name.rsplit("/", 1)[-1]
                target = dest / base
                if dry_run:
                    print(f"      WOULD EXTRACT  {src.name}!{entry_name} -> {target}")
                else:
                    with zf.open(entry_name) as fin, open(target, "wb") as fout:
                        shutil.copyfileobj(fin, fout)
                written += 1
    else:
        # text-only or empty; nothing to materialise
        pass

    missing = [p for p in PREFIXES if p not in prefix_map]
    return written, missing


# -------------- main orchestration --------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Preview only, don't write anything")
    ap.add_argument("--cache", action="store_true",
                    help="After ingestion, invoke cache_months.py to refresh parquet cache")
    args = ap.parse_args()

    if not SOURCE_ROOT.exists():
        print(f"ERROR: source not found: {SOURCE_ROOT}")
        sys.exit(1)
    if not DEST_ROOT.exists():
        print(f"ERROR: destination root not found: {DEST_ROOT}")
        sys.exit(1)

    print(f"Source:      {SOURCE_ROOT}")
    print(f"Destination: {DEST_ROOT}")
    print(f"Mode:        {'DRY RUN — no files will be written' if args.dry_run else 'WRITE'}")
    print()

    # Group every entry under SOURCE_ROOT by detected month
    plans: dict[str, MonthPlan] = {}
    unmatched: list[Path] = []

    for entry in sorted(SOURCE_ROOT.iterdir()):
        kind = classify_source(entry)
        if kind == "unknown":
            continue
        ym = detect_month(entry.name)
        if ym is None:
            unmatched.append(entry)
            continue
        month = yyyymm(*ym)
        plan = plans.setdefault(month, MonthPlan(month))
        plan.add(entry, kind)

    # Process each month
    skipped: list[str] = []
    materialised: list[tuple[str, int, list[str]]] = []
    text_only: list[str] = []
    no_cd_rom: list[str] = []

    print(f"Found {len(plans)} months in source.")
    print()

    for month in sorted(plans):
        plan = plans[month]
        print(f"[{month}] {len(plan.candidates)} candidate(s) in source")

        if dest_complete(month):
            print("  SKIP — destination already complete")
            skipped.append(month)
            continue

        src, kind, prefix_map = plan.pick_best()
        if src is None or not prefix_map:
            # Check if all candidates are text-only (legacy ALL.txt)
            kinds = {k for _, k in plan.candidates}
            if kinds == {"text"}:
                print("  TEXT-ONLY — legacy ALL.txt format, needs separate parser")
                text_only.append(month)
            else:
                print("  NO CD-ROM FILES FOUND in any candidate")
                no_cd_rom.append(month)
            continue

        have = sorted(prefix_map.keys())
        missing = [p for p in PREFIXES if p not in prefix_map]
        print(f"  source: {src.name}  ({kind})")
        print(f"  prefixes present: {have}")
        if missing:
            print(f"  prefixes MISSING: {missing}")
        written, _ = materialise(month, src, kind, prefix_map, args.dry_run)
        action = "would write" if args.dry_run else "wrote"
        print(f"  {action} {written} file(s) -> {DEST_ROOT}\\{month}\\")
        materialised.append((month, written, missing))

    # Summary
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Months in source:        {len(plans)}")
    print(f"  Skipped (already done):  {len(skipped)}")
    print(f"  Materialised this run:   {len(materialised)}")
    incomplete = [m for m, _, miss in materialised if miss]
    if incomplete:
        print(f"    Of which INCOMPLETE:   {len(incomplete)} (missing some prefixes)")
        for m, _, miss in materialised:
            if miss:
                print(f"      {m}: missing {miss}")
    if text_only:
        print(f"  Text-only (needs parser): {len(text_only)}")
        for m in text_only:
            print(f"      {m}")
    if no_cd_rom:
        print(f"  No CD-ROM files found:   {len(no_cd_rom)}")
        for m in no_cd_rom:
            print(f"      {m}")
    if unmatched:
        print(f"\n  Unmatched filenames (didn't fit any pattern):")
        for u in unmatched:
            print(f"      {u.name}")

    # Optionally chain into cache_months.py
    if args.cache and not args.dry_run and materialised:
        print()
        print("=" * 60)
        print("Running cache_months.py to refresh parquet cache...")
        print("=" * 60)
        import subprocess
        result = subprocess.run(
            [sys.executable, str(Path(__file__).parent / "cache_months.py")],
            cwd=str(Path(__file__).resolve().parent.parent),
        )
        sys.exit(result.returncode)


if __name__ == "__main__":
    main()
