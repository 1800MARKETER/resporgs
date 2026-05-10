"""Parsers for the 4 monthly control files used by MoDatProc.exe.

Files (all in `C:\\MonthlyProcessing2\\` by default):
    RestrictedACExc.txt                   - phone-pattern adjustments
    RO2RO.txt                             - resporg renames
    RO2Stat.txt                           - status overrides per resporg+status
    Individual number adjustment file-S.txt - per-number resporg overrides

All parsers strip leading-`<>` comment lines and `FILE END` markers (except the
Individual file, which has neither).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

DEFAULT_CTRL_DIR = Path(r"C:\MonthlyProcessing2")


# ---------------------------------------------------------------------------
# RestrictedACExc.txt
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AcExcRule:
    """One row of RestrictedACExc.txt.

    `phone_pattern` is the 10-digit pattern (dashes stripped) with `*` as
    wildcard for any single character. Length is exactly 10.

    Each replacement field can be:
      - "*"        keep the current value
      - ""         clear the field
      - any other  use as override
    """
    phone_pattern: str          # exactly 10 chars
    status_repl: str | None     # None = keep
    date_repl: str | None
    fourth_repl: str | None
    resporg_repl: str | None


def _strip_comment(line: str) -> str:
    """A `<>` token marks the start of a comment; everything from it to EOL is dropped."""
    idx = line.find("<>")
    if idx >= 0:
        return line[:idx]
    return line


def _normalise_replacement(s: str) -> str | None:
    """Convert a control-file replacement field to its semantic value:
        '*'    -> None  (keep)
        ''     -> ''    (clear)
        any X  -> X     (override)

    Strip first because trailing-comment lines like
        '800-271-****,UNAVAIL,*,*,*             <>...'
    leave whitespace on the last field after _strip_comment runs.
    """
    s = s.strip()
    if s == "*":
        return None
    return s  # '' for clear, otherwise literal value


def load_ac_exc(path: Path) -> list[AcExcRule]:
    rules: list[AcExcRule] = []
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = _strip_comment(raw).rstrip("\r\n")
            if not line.strip():
                continue
            if line.strip() == "FILE END":
                break
            parts = line.split(",")
            if len(parts) < 5:
                continue
            phone_raw = parts[0].strip().replace("-", "")
            if len(phone_raw) != 10:
                continue
            rules.append(AcExcRule(
                phone_pattern=phone_raw,
                status_repl=_normalise_replacement(parts[1]),
                date_repl=_normalise_replacement(parts[2]),
                fourth_repl=_normalise_replacement(parts[3]),
                resporg_repl=_normalise_replacement(parts[4]),
            ))
    return rules


# ---------------------------------------------------------------------------
# RO2RO.txt
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Ro2RoRule:
    """RespOrg rename rule. `from_pattern` is 5 chars with `*` as single-char
    wildcard. `to_resporg` is exactly 5 chars (no wildcards)."""
    from_pattern: str
    to_resporg: str


def load_ro2ro(path: Path) -> list[Ro2RoRule]:
    rules: list[Ro2RoRule] = []
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = _strip_comment(raw).rstrip("\r\n")
            if not line.strip():
                continue
            if line.strip() == "FILE END":
                break
            parts = line.split(",")
            if len(parts) < 2:
                continue
            old = parts[0].strip()
            new = parts[1].strip()
            if len(old) != 5 or len(new) != 5:
                continue
            rules.append(Ro2RoRule(from_pattern=old, to_resporg=new))
    return rules


# ---------------------------------------------------------------------------
# RO2Stat.txt
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Ro2StatRule:
    """Status override based on resporg + current status.

    `resporg_pattern` is 5 chars with `*` wildcard support.
    `status_match` is 7 chars (the field width in the input) or `*` for any.
    `new_status` is the replacement (typically 7 chars, padded)."""
    resporg_pattern: str
    status_match: str            # 7 chars or "*"
    new_status: str


def load_ro2stat(path: Path) -> list[Ro2StatRule]:
    rules: list[Ro2StatRule] = []
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = _strip_comment(raw).rstrip("\r\n")
            if not line.strip():
                continue
            if line.strip() == "FILE END":
                break
            parts = line.split(",")
            if len(parts) < 3:
                continue
            resporg = parts[0].strip()
            status = parts[1]  # may be padded to 7 chars; preserve verbatim
            new_status = parts[2]
            if len(resporg) != 5:
                continue
            rules.append(Ro2StatRule(
                resporg_pattern=resporg,
                status_match=status.strip() if status.strip() == "*" else status,
                new_status=new_status,
            ))
    return rules


# ---------------------------------------------------------------------------
# Individual number adjustment file-S.txt
# ---------------------------------------------------------------------------

def load_individual(path: Path) -> dict[str, str]:
    """Returns {phone_10_digit: target_resporg_5_char}.

    Bud's program uses a sequential-pointer scan over this file which has the
    documented bug of dropping entries 3+. We avoid the bug entirely by
    materialising into a dict for O(1) lookup per output row.

    The file format:
        - PHONENUMBER,RESPORG per line (10-digit phone, 5-char resporg)
        - sorted ascending, no duplicates, no FILE END marker
        - no comments

    Validation errors (per the spec):
        Code -1: line < 1 part OR phone < 10 chars
        Code -2: duplicate phone
        Code -3: out of sequence
        Code -4: resporg length != 5
    We log and skip invalid rows rather than raising.
    """
    overrides: dict[str, str] = {}
    last_phone = ""
    skipped_bad = 0
    skipped_dup = 0
    skipped_seq = 0
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            parts = line.split(",")
            if len(parts) < 2:
                skipped_bad += 1
                continue
            phone = parts[0].strip()
            resporg = parts[1].strip()
            if len(phone) < 10 or not phone.isdigit():
                skipped_bad += 1
                continue
            if len(resporg) != 5:
                skipped_bad += 1
                continue
            if phone in overrides:
                skipped_dup += 1
                continue
            if phone < last_phone:
                skipped_seq += 1
                continue
            overrides[phone] = resporg
            last_phone = phone
    if skipped_bad or skipped_dup or skipped_seq:
        print(f"[somos_adjust] Individual file: kept {len(overrides):,} entries; "
              f"skipped {skipped_bad} malformed, {skipped_dup} duplicate, {skipped_seq} out-of-sequence")
    return overrides


# ---------------------------------------------------------------------------
# Convenience: load all four
# ---------------------------------------------------------------------------

@dataclass
class ControlBundle:
    ac_exc: list[AcExcRule]
    ro2ro: list[Ro2RoRule]
    ro2stat: list[Ro2StatRule]
    individual: dict[str, str]


def load_all(ctrl_dir: Path = DEFAULT_CTRL_DIR) -> ControlBundle:
    ctrl_dir = Path(ctrl_dir)
    return ControlBundle(
        ac_exc=load_ac_exc(ctrl_dir / "RestrictedACExc.txt"),
        ro2ro=load_ro2ro(ctrl_dir / "RO2RO.txt"),
        ro2stat=load_ro2stat(ctrl_dir / "RO2Stat.txt"),
        individual=load_individual(ctrl_dir / "Individual number adjustment file-S.txt"),
    )


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    bundle = load_all()
    print(f"AC/Exch rules:           {len(bundle.ac_exc):>6}")
    print(f"RO2RO rules:             {len(bundle.ro2ro):>6}")
    print(f"RO2Stat rules:           {len(bundle.ro2stat):>6}")
    print(f"Individual overrides:    {len(bundle.individual):>6}")
    print()
    print("Sample RO2RO rules:")
    for r in bundle.ro2ro[:5]:
        print(f"  {r.from_pattern} -> {r.to_resporg}")
    print()
    # GJK canary - the bug Bud has
    gjk_count = sum(1 for v in bundle.individual.values() if v == "GJK01")
    print(f"Individual rows targeting GJK01: {gjk_count} (expect 7,845)")
