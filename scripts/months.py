"""
Month inventory + loader for the Somos monthly snapshots on D:\\resporgs.

Each month is either:
  - A folder like D:\\resporgs\\2025-06\\ containing CD-ROM_TFN_Report_{prefix}_*.txt
  - A zip like D:\\resporgs\\2025-01.zip containing the same folder structure inside

A record in a CD-ROM file looks like:
  844-200-0000  ,WORKING ,25/04/15,83,RBI69

Fields (comma-separated, whitespace-padded):
  0: number (e.g. '844-200-0000')
  1: status  (WORKING | TRANSIT | DISCONN | RESERVED | UNAVAIL | ASSIGNED)
  2: last-change date (YY/MM/DD)
  3: template code (2 digits)
  4: full 5-char resporg code (first 2 chars = resporg prefix)
"""

from __future__ import annotations
import re
import os
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

ARCHIVE_ROOT = Path(r"D:\resporgs")
PREFIXES = ("800", "833", "844", "855", "866", "877", "888")

CD_ROM_RE = re.compile(
    # Three naming conventions seen in the archive:
    #   SMS/800 era:       SMS800.D20180301.txt                     (pre-Aug 2019)
    #   CD-ROM era:        CD-ROM_TFN_Report_800_20250601061856.txt (Aug 2019 - Mar 2025)
    #   Number-Status era: Number-Status-NPA-800.txt                (April 2025+)
    # Also allow post-timestamp suffixes like -UPDATED102323
    r"(?:CD-ROM_TFN_Report_|Number-Status-NPA-|SMS)"
    r"(800|833|844|855|866|877|888)"
    r"(?:\.D\d{8}|[_-][\w-]*)?\.txt$",
    re.IGNORECASE,
)
# Combined-file fallback formats — single file holding ALL prefixes for a month.
# Only used when per-prefix files aren't present (e.g. legacy 23 07/08 zips).
#   "<anything> All.txt" / "<anything>ALL.txt"   — clean snapshot, no spares
#   "MikeOINPUT.txt"                              — full dataset INCLUDING spares
# Same 5-field line format as CD-ROM files; prefix is derived from the number.
ALL_FILE_RE = re.compile(r"all\.txt$", re.IGNORECASE)
MIKE_FILE_RE = re.compile(r"mikeo?input\.txt$", re.IGNORECASE)

MONTH_FOLDER_RE = re.compile(r"^(\d{4}-\d{2})$")
# Two zip naming conventions in the archive:
#   2025-01.zip              (current convention)
#   22 06 Resporg.zip        (older Dropbox convention: YY MM Resporg)
MONTH_ZIP_RE = re.compile(
    r"^(?:(\d{4}-\d{2})|(\d{2}) (\d{2}) Resporg)\.zip$"
)

# Status filter — used when reading MikeOINPUT.txt to strip SPARE rows
# so combined-file months produce data parity with per-prefix months.
KNOWN_STATUSES = frozenset(("WORKING", "TRANSIT", "DISCONN", "RESERVED",
                             "UNAVAIL", "ASSIGNED", "SUSPEND"))


@dataclass
class MonthSource:
    month: str                    # e.g. "2025-06"
    kind: str                     # "folder" | "zip" | "combined_zip"
    path: Path                    # folder or zip path
    cd_rom_files: dict[str, str]  # prefix -> path-or-zip-entry  (empty if combined_zip)
    combined_entry: str = ""      # zip-internal path of ALL.txt / MikeOINPUT.txt
    combined_kind: str = ""       # "all" | "mike"

    def complete(self) -> bool:
        if self.kind == "combined_zip":
            return bool(self.combined_entry)
        return all(p in self.cd_rom_files for p in PREFIXES)


def inventory() -> list[MonthSource]:
    """Scan the archive root and return one MonthSource per recognised month."""
    by_month: dict[str, MonthSource] = {}

    for entry in sorted(ARCHIVE_ROOT.iterdir()):
        if entry.is_dir():
            m = MONTH_FOLDER_RE.match(entry.name)
            if not m:
                continue
            month = m.group(1)
            files: dict[str, str] = {}
            for f in entry.iterdir():
                cm = CD_ROM_RE.match(f.name)
                if cm:
                    files[cm.group(1)] = str(f)
            if files:
                by_month[month] = MonthSource(month, "folder", entry, files)
        elif entry.is_file():
            m = MONTH_ZIP_RE.match(entry.name)
            if not m:
                continue
            if m.group(1):
                month = m.group(1)                          # "2025-01"
            else:
                month = f"20{m.group(2)}-{m.group(3)}"      # "22 06" -> "2022-06"
            if month in by_month:
                # Duplicate download (e.g. "2024-03 (1).zip") — prefer already-seen
                continue
            files = {}
            all_entry = ""
            mike_entry = ""
            try:
                with zipfile.ZipFile(entry) as zf:
                    for name in zf.namelist():
                        base = name.rsplit("/", 1)[-1]
                        cm = CD_ROM_RE.match(base)
                        if cm:
                            files[cm.group(1)] = name  # zip entry name
                            continue
                        # Track combined-file fallbacks (only used if no per-prefix files found)
                        if not all_entry and ALL_FILE_RE.search(base):
                            all_entry = name
                        elif not mike_entry and MIKE_FILE_RE.search(base):
                            mike_entry = name
            except zipfile.BadZipFile:
                print(f"  SKIPPED {entry.name}: corrupt or incomplete zip")
                continue
            if files:
                by_month[month] = MonthSource(month, "zip", entry, files)
            elif all_entry or mike_entry:
                # Fallback: zip has no per-prefix files but has a combined snapshot.
                # Prefer ALL.txt (no spares) over MikeOINPUT.txt.
                if all_entry:
                    by_month[month] = MonthSource(
                        month, "combined_zip", entry, {},
                        combined_entry=all_entry, combined_kind="all"
                    )
                else:
                    by_month[month] = MonthSource(
                        month, "combined_zip", entry, {},
                        combined_entry=mike_entry, combined_kind="mike"
                    )

    return [by_month[k] for k in sorted(by_month)]


def iter_records(source: MonthSource) -> Iterator[tuple[str, str, str, str, str]]:
    """
    Yield (prefix, number, status, change_date_yymmdd, resporg_code)
    across all 7 prefix files for the given month. For combined_zip
    sources, opens the single ALL/MikeOINPUT file once and derives the
    prefix from each line's number.
    """
    if source.kind == "combined_zip":
        filter_spares = source.combined_kind == "mike"
        with zipfile.ZipFile(source.path) as zf, zf.open(source.combined_entry) as raw:
            for line in raw:
                try:
                    line = line.decode("ascii", errors="replace")
                except Exception:
                    continue
                parsed = _parse_combined_line(line, filter_spares=filter_spares)
                if parsed:
                    yield parsed
        return

    for prefix in PREFIXES:
        loc = source.cd_rom_files.get(prefix)
        if not loc:
            continue
        if source.kind == "folder":
            with open(loc, "r", encoding="ascii", errors="replace") as f:
                yield from _parse_lines(prefix, f)
        else:  # zip
            with zipfile.ZipFile(source.path) as zf, zf.open(loc) as raw:
                # decode line-by-line
                for line in raw:
                    try:
                        line = line.decode("ascii", errors="replace")
                    except Exception:
                        continue
                    parsed = _parse_line(prefix, line)
                    if parsed:
                        yield parsed


def _parse_line(prefix: str, line: str):
    parts = line.split(",")
    if len(parts) < 5:
        return None
    number = parts[0].strip()
    status = parts[1].strip()
    date = parts[2].strip()
    resporg = parts[4].strip()
    if not number or not resporg:
        return None
    return (prefix, number, status, date, resporg)


def _parse_lines(prefix: str, lines):
    for line in lines:
        parsed = _parse_line(prefix, line)
        if parsed:
            yield parsed


def _parse_combined_line(line: str, *, filter_spares: bool = False):
    """Parse a line from a combined ALL.txt or MikeOINPUT.txt file. The line
    holds all 7 prefixes interleaved, so we derive the prefix from the
    number's first 3 digits. ALL.txt format has dashes ('800-200-0000');
    MikeOINPUT.txt has no dashes ('8002000000'). Both work — _parse_line's
    fields are tolerant of either, and parse_number strips dashes."""
    parts = line.split(",")
    if len(parts) < 5:
        return None
    number = parts[0].strip()
    status = parts[1].strip()
    date = parts[2].strip()
    resporg = parts[4].strip()
    if not number or not resporg:
        return None
    digits = number.replace("-", "")
    if len(digits) < 3:
        return None
    prefix = digits[:3]
    if prefix not in PREFIXES:
        return None  # ignore non-toll-free numbers if any sneak in
    if filter_spares and status not in KNOWN_STATUSES:
        return None  # MikeOINPUT includes spare rows; drop them
    return (prefix, number, status, date, resporg)


if __name__ == "__main__":
    inv = inventory()
    print(f"Found {len(inv)} months in {ARCHIVE_ROOT}:")
    print(f"{'month':<10} {'kind':<8} {'prefixes':<10} complete")
    print("-" * 40)
    for src in inv:
        have = "".join("Y" if p in src.cd_rom_files else "-" for p in PREFIXES)
        flag = "OK" if src.complete() else "MISSING"
        print(f"{src.month:<10} {src.kind:<8} {have:<10} {flag}")
