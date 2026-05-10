"""Pure-function adjustment passes for the Somos monthly data.

Each function takes a record (dict-like with the 5 fields) plus the relevant
control rules, and returns the adjusted record. The 4 passes are applied in
order by build_adjusted.py; each pass is independently unit-testable.

Field semantics (matches the input from CD-ROM_TFN_Report files):
    digits     - 10-digit phone (e.g. '8002000000')
    status     - 7-char status (e.g. 'WORKING', 'DISCONN', 'AVAIL  ')
    date       - 'YY/MM/DD' (8 chars) or ''
    fourth     - 2-char template/age code or ''
    resporg    - 5-char resporg ID, or ''

Mutates a passed-in dict in place for speed (this gets called 56M+ times in a
full run). Callers should either accept that or copy() the dict before each pass.
"""
from __future__ import annotations

from typing import Iterable

from .control_files import AcExcRule, Ro2RoRule, Ro2StatRule


# ---------------------------------------------------------------------------
# Pass 1: RestrictedACExc -- phone-pattern adjustments (status/date/4th/resporg)
# ---------------------------------------------------------------------------

def _phone_matches(pattern: str, digits: str) -> bool:
    """Both must be 10 chars. `*` in pattern matches any char in digits."""
    if len(pattern) != 10 or len(digits) != 10:
        return False
    for p, d in zip(pattern, digits):
        if p != "*" and p != d:
            return False
    return True


def apply_ac_exc(rec: dict, rules: Iterable[AcExcRule]) -> dict:
    """First-match-wins. Each replacement field semantics:
        None  -> keep current value
        ''    -> clear field
        any X -> override with X
    """
    digits = rec["digits"]
    for r in rules:
        if not _phone_matches(r.phone_pattern, digits):
            continue
        if r.status_repl is not None:
            rec["status"] = r.status_repl
        if r.date_repl is not None:
            rec["date"] = r.date_repl
        if r.fourth_repl is not None:
            rec["fourth"] = r.fourth_repl
        if r.resporg_repl is not None:
            rec["resporg"] = r.resporg_repl
        break
    return rec


# ---------------------------------------------------------------------------
# Pass 2: RO2RO -- resporg renames
# ---------------------------------------------------------------------------

def _resporg_matches(pattern: str, code: str) -> bool:
    """Both 5 chars. `*` matches any single char."""
    if len(pattern) != 5 or len(code) != 5:
        return False
    for p, c in zip(pattern, code):
        if p != "*" and p != c:
            return False
    return True


def apply_ro2ro(rec: dict, rules: Iterable[Ro2RoRule]) -> dict:
    """First-match-wins. Only runs if resporg is non-empty."""
    code = rec["resporg"]
    if not code:
        return rec
    for r in rules:
        if _resporg_matches(r.from_pattern, code):
            rec["resporg"] = r.to_resporg
            break
    return rec


# ---------------------------------------------------------------------------
# Pass 3: RO2Stat -- status overrides per resporg + current status
# ---------------------------------------------------------------------------

def _status_matches(rule_status: str, current_status: str) -> bool:
    """Per the spec, RO2Stat's STATUS field is either '*' (any status) or a
    literal status name (exact match). No partial-wildcards. Both sides are
    treated as stripped canonical status names."""
    rs = rule_status.strip()
    if rs == "*":
        return True
    return rs == current_status.strip()


def apply_ro2stat(rec: dict, rules: Iterable[Ro2StatRule]) -> dict:
    """First-match-wins. Only runs if resporg is non-empty.
    Compares post-RO2RO resporg (Bud applies RO2Stat AFTER RO2RO)."""
    code = rec["resporg"]
    if not code:
        return rec
    cur_status = rec["status"]
    for r in rules:
        if not _resporg_matches(r.resporg_pattern, code):
            continue
        if _status_matches(r.status_match, cur_status):
            rec["status"] = r.new_status.strip()
            break
    return rec


# ---------------------------------------------------------------------------
# Pass 4: Individual -- per-number resporg overrides (the buggy one in C#)
# ---------------------------------------------------------------------------

def apply_individual(rec: dict, overrides: dict[str, str]) -> dict:
    """O(1) dict lookup, sidestepping Bud's pointer-advance bug entirely."""
    target = overrides.get(rec["digits"])
    if target:
        rec["resporg"] = target
    return rec


# ---------------------------------------------------------------------------
# Combined: one record through all 4 passes (matches MoDatProc order)
# ---------------------------------------------------------------------------

def apply_all(rec: dict, ac_exc, ro2ro, ro2stat, individual) -> dict:
    """The full pipeline for one record. Mutates and returns the same dict."""
    apply_ac_exc(rec, ac_exc)
    apply_ro2ro(rec, ro2ro)
    apply_ro2stat(rec, ro2stat)
    apply_individual(rec, individual)
    return rec


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from pathlib import Path
    from .control_files import load_all

    bundle = load_all()

    print("--- Test 1: GJK Individual override (bug fix verification) ---")
    rec = {"digits": "8002222739", "status": "WORKING",
           "date": "25/12/15", "fourth": "43", "resporg": "NDB99"}
    apply_all(rec, bundle.ac_exc, bundle.ro2ro, bundle.ro2stat, bundle.individual)
    print(f"  after: resporg={rec['resporg']} (expected GJK01)")
    assert rec["resporg"] == "GJK01", "Individual pass failed"
    print("  OK")

    print("--- Test 2: RO2RO resporg rename ---")
    rec = {"digits": "8009999999", "status": "WORKING",
           "date": "25/01/01", "fourth": "00", "resporg": "HTC02"}
    apply_all(rec, bundle.ac_exc, bundle.ro2ro, bundle.ro2stat, bundle.individual)
    print(f"  after: resporg={rec['resporg']} (expected GJK01 from HTC02 rename)")
    assert rec["resporg"] == "GJK01", "RO2RO rename failed"
    print("  OK")

    print("--- Test 3: pass-through (no rules apply) ---")
    rec = {"digits": "8009999998", "status": "WORKING",
           "date": "25/01/01", "fourth": "00", "resporg": "ABC01"}
    apply_all(rec, bundle.ac_exc, bundle.ro2ro, bundle.ro2stat, bundle.individual)
    print(f"  after: resporg={rec['resporg']} (expected ABC01 unchanged)")
    assert rec["resporg"] == "ABC01"
    print("  OK")

    print("\nAll smoke tests passed.")
