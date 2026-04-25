"""
Detect commercial-mailbox / coworking addresses in resporg street1 fields.

Companies operating out of a UPS Store, iPostal, Regus, WeWork, etc. are
flagged as likely "virtual" operations. Real phone companies don't run
their business out of a mailbox rental, so this is a useful soft signal
on resporg profiles.

Reads:   clean/resporg.json
Writes:  data/mailbox_flags.parquet  (only flagged rows — unflagged omitted)

One-shot script. Re-run after Sanity address edits. Not on monthly rebuild.
"""

from __future__ import annotations
import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
CLEAN = ROOT / "clean"
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)


# (canonical_brand, confidence, compiled_regex) — tested against street1.lower()
# Order matters: first hit wins (so longer/more-specific patterns come first).
_PATTERNS: list[tuple[str, str, re.Pattern]] = [
    # Exact brand names — high confidence
    ("The UPS Store",          "high",   re.compile(r"\b(the\s+)?ups\s+store\b")),
    ("Mail Boxes Etc",         "high",   re.compile(r"\bmail\s*box(es)?\s*etc\.?\b|\bmbe\s*#")),
    ("PostNet",                "high",   re.compile(r"\bpost[\s\-]?net\b")),
    ("iPostal",                "high",   re.compile(r"\bipostal1?\b")),
    ("Earth Class Mail",       "high",   re.compile(r"\bearth\s+class\s+mail\b")),
    ("PostalAnnex",            "high",   re.compile(r"\bpostal\s*annex\b")),
    ("AIM Mail Center",        "high",   re.compile(r"\baim\s+mail\s+center\b")),
    ("Pak Mail",               "high",   re.compile(r"\bpak[\s\-]?mail\b")),
    ("Regus",                  "high",   re.compile(r"\bregus\b")),
    ("WeWork",                 "high",   re.compile(r"\bwe[\s\-]?work\b")),
    ("Davinci Virtual",        "high",   re.compile(r"\bdavinci\s+(virtual|meeting)\b")),
    ("Alliance Virtual",       "high",   re.compile(r"\balliance\s+virtual\b")),
    ("Opus Virtual Offices",   "high",   re.compile(r"\bopus\s+virtual\b")),
    ("Intelligent Office",     "high",   re.compile(r"\bintelligent\s+office\b")),
    ("Spaces (IWG)",           "high",   re.compile(r"\bspaces\s+(coworking|@|\-)")),
    ("Premier Workspaces",     "high",   re.compile(r"\bpremier\s+workspaces?\b")),
    ("Industrious",            "high",   re.compile(r"\bindustrious\s+(office|coworking)\b")),
    # Generic markers — medium confidence
    ("Private Mailbox (PMB)",  "medium", re.compile(r"\bpmb\s*#|\bprivate\s+mail\s*box\b|\s+pmb\s+\d")),
    ("Virtual Office (generic)", "medium", re.compile(r"\bvirtual\s+office\b")),
    ("Coworking (generic)",    "medium", re.compile(r"\bco[\s\-]?working\b")),
    ("P.O. Box",               "medium", re.compile(r"\bp\.?\s*o\.?\s*box\b|\bpost\s+office\s+box\b")),
]


def classify(street1: str) -> tuple[str, str, str] | None:
    """Return (brand, confidence, raw_hit) or None."""
    if not street1:
        return None
    text = street1.lower()
    for brand, conf, pat in _PATTERNS:
        m = pat.search(text)
        if m:
            # Grab a bit of context around the hit for raw_hit
            start = max(0, m.start() - 10)
            end = min(len(street1), m.end() + 10)
            snippet = street1[start:end].strip()
            return brand, conf, snippet
    return None


def _addr_key(addr: dict) -> str | None:
    """Normalized address key for shared-address detection."""
    s1 = (addr.get("street1") or "").strip().lower()
    city = (addr.get("city") or "").strip().lower()
    state = (addr.get("state") or "").strip().lower()
    if not s1 or not city:
        return None
    # Collapse whitespace, drop trailing punctuation
    s1 = re.sub(r"\s+", " ", s1).rstrip(" .,")
    return f"{s1}|{city}|{state}"


def main():
    docs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))

    # Pre-pass: count resporgs per address so we can flag shared ones.
    addr_counts: dict[str, list[str]] = {}
    for d in docs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) < 2:
            continue
        key = _addr_key(d.get("address") or {})
        if not key:
            continue
        addr_counts.setdefault(key, []).append(code[:2])

    shared_addrs = {k: v for k, v in addr_counts.items() if len(v) > 1}

    rows = []
    for d in docs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) < 2:
            continue
        rpfx = code[:2]
        addr = d.get("address") or {}
        street1 = addr.get("street1") or ""

        # 1. Brand / PO Box match
        hit = classify(street1)
        if hit:
            brand, conf, raw = hit
            rows.append(
                {
                    "rpfx": rpfx,
                    "doc_id": d["_id"].removeprefix("drafts."),
                    "brand": brand,
                    "confidence": conf,
                    "raw_hit": raw,
                    "street1": street1,
                }
            )
            continue

        # 2. Shared-address detection — flag if 2+ DISTINCT rpfxs at this address
        key = _addr_key(addr)
        if key and key in shared_addrs:
            peers = sorted({p for p in shared_addrs[key] if p != rpfx})
            if not peers:
                continue  # duplicate doc for the same rpfx, not a real shared address
            total = len(peers) + 1
            conf = "high" if total >= 3 else "medium"
            label = f"Shared address ({total} resporg{'s' if total > 1 else ''})"
            rows.append(
                {
                    "rpfx": rpfx,
                    "doc_id": d["_id"].removeprefix("drafts."),
                    "brand": label,
                    "confidence": conf,
                    "raw_hit": f"also: {', '.join(peers[:5])}",
                    "street1": street1,
                }
            )

    # Dedupe by rpfx (take the first / highest confidence). Sort so high > medium wins on tie.
    rows.sort(key=lambda r: (r["rpfx"], 0 if r["confidence"] == "high" else 1))
    seen: set[str] = set()
    deduped = []
    for r in rows:
        if r["rpfx"] in seen:
            continue
        seen.add(r["rpfx"])
        deduped.append(r)
    deduped.sort(key=lambda r: (r["brand"], r["rpfx"]))

    if not deduped:
        print("No mailbox flags detected.")
        return

    tbl = pa.Table.from_pylist(deduped)
    out = DATA / "mailbox_flags.parquet"
    pq.write_table(tbl, out, compression="zstd")
    print(f"Wrote {out.name}: {len(deduped)} flagged resporgs")
    print()
    print(f"{'rpfx':<5} {'brand':<24} {'conf':<7} raw")
    print("-" * 70)
    for r in deduped:
        print(f"{r['rpfx']:<5} {r['brand']:<24} {r['confidence']:<7} {r['raw_hit']}")


if __name__ == "__main__":
    main()
