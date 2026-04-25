"""
Build data/somos_contacts.parquet from the CSV exports we pulled from
the Somos portal Find Resp Org Contacts page.

Inputs:
  data/somos_contacts/suffix_*.csv  (one CSV per batch search like Key=01, 02, 99)

Output:
  data/somos_contacts.parquet  — one row per 3-char admin code (deduped)
  data/admin_networks.parquet  — derived: emails/phones that span 2+ resporgs

Schema (somos_contacts):
  rpfx                 VARCHAR  (2-char prefix matching our internal data)
  admin_code           VARCHAR  (3-char Somos admin prefix, e.g. QZA)
  sample_sub_code      VARCHAR  (the actual ID we got the row from, e.g. QZA01)
  company_name         VARCHAR
  street               VARCHAR
  city                 VARCHAR
  state                VARCHAR
  country              VARCHAR
  zip                  VARCHAR
  company_phone        VARCHAR
  primary_contact_name VARCHAR
  primary_contact_email VARCHAR
  primary_contact_phone VARCHAR
  primary_contact_fax  VARCHAR
  change_contact_name  VARCHAR
  change_contact_email VARCHAR
  change_contact_phone VARCHAR
  change_contact_fax   VARCHAR
  notes                VARCHAR
"""

from __future__ import annotations
import csv
import re
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
CSV_DIR = DATA / "somos_contacts"


def parse_address(field: str) -> dict:
    """The Company Address column is a single string concatenating street,
    city, state, country, zip, phone. Format observed:
        "33 Regency Drive  Poughkeepsie New York, USA 12603 800-627-5383"
    Heuristic: regex out the trailing zip + phone, then split on commas/state.
    """
    out = {"street": "", "city": "", "state": "", "country": "", "zip": "", "phone": ""}
    if not field:
        return out
    s = field.strip()
    # Trailing phone (xxx-xxx-xxxx or 10 digits)
    m = re.search(r'(\d{3}-\d{3}-\d{4}|\d{10})\s*$', s)
    if m:
        out["phone"] = m.group(1)
        s = s[:m.start()].strip()
    # Trailing zip (5 digits or 5+4 or alphanumeric for some countries)
    m = re.search(r'\s+([A-Z]?\d{5}(?:-?\d{4})?|[A-Z]\d[A-Z]\s?\d[A-Z]\d)\s*$', s)
    if m:
        out["zip"] = m.group(1)
        s = s[:m.start()].strip()
    # Country: split on last comma, country is after
    if "," in s:
        before, after = s.rsplit(",", 1)
        out["country"] = after.strip()
        s = before.strip()
    # Now s is "street city state". State is usually a 2-letter code or a
    # well-known state name at the end. Try to find common US states.
    US_STATES = {
        "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut",
        "Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
        "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan",
        "Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada",
        "New Hampshire","New Jersey","New Mexico","New York","North Carolina",
        "North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
        "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
        "Virginia","Washington","West Virginia","Wisconsin","Wyoming",
        "District Of Columbia","Puerto Rico",
    }
    for state in sorted(US_STATES, key=len, reverse=True):
        if s.endswith(" " + state):
            out["state"] = state
            s = s[: -len(state)].strip()
            break
    # State abbrev fallback
    if not out["state"]:
        m = re.search(r'\s+([A-Z]{2})\s*$', s)
        if m:
            out["state"] = m.group(1)
            s = s[:m.start()].strip()
    # City: last word(s) of remainder. Heuristic: last 1-3 capitalized words.
    if s:
        # If we have street + city, street is usually number-prefixed.
        # Find first non-numeric word boundary that splits street from city.
        # Simplest: street is before "  " (double space) if present in CSV
        if "  " in s:
            street, city = s.rsplit("  ", 1)
            out["street"] = street.strip()
            out["city"] = city.strip()
        else:
            # Take last 1-2 capitalized words as city, rest as street
            tokens = s.split()
            # Walk from end while tokens look like city words
            i = len(tokens)
            while i > 0 and tokens[i-1][:1].isupper() and not any(ch.isdigit() for ch in tokens[i-1]):
                i -= 1
                if len(tokens) - i >= 3:
                    break
            if i == len(tokens):
                out["street"] = s
            else:
                out["street"] = " ".join(tokens[:i])
                out["city"] = " ".join(tokens[i:])
    return out


def parse_contact(field: str) -> dict:
    """Contact column format observed:
        "Bill Quimby billquimby@billquimby.net P: 800-627-5383 x800 F: 800-329-0095"
    """
    out = {"name": "", "email": "", "phone": "", "fax": ""}
    if not field:
        return out
    s = field.strip()
    # Pull email
    m = re.search(r'\b([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})\b', s)
    if m:
        out["email"] = m.group(1)
        # Name is everything before email
        out["name"] = s[: m.start()].strip()
        s = s[m.end():].strip()
    # Pull phone (after "P:")
    m = re.search(r'P:\s*([\w\-\. ]+?)(?=F:|$)', s)
    if m:
        out["phone"] = m.group(1).strip()
    # Pull fax (after "F:")
    m = re.search(r'F:\s*([\w\-\. ]+)$', s)
    if m:
        out["fax"] = m.group(1).strip()
    if not out["name"] and not out["email"]:
        out["name"] = s  # whatever it is
    return out


def main():
    # Load each CSV; dedupe to one row per 3-char admin code, preferring the
    # earliest (smallest numeric) sub-code we saw (typically 01).
    by_3char: dict[str, dict] = {}
    sources = sorted(CSV_DIR.glob("suffix_*.csv"))
    print(f"Reading {len(sources)} CSVs...")
    for src in sources:
        with open(src, encoding="utf-8-sig", newline="") as fh:
            for r in csv.DictReader(fh):
                rid = (r.get("ID") or "").strip().upper()
                if len(rid) < 3:
                    continue
                three = rid[:3]
                # Prefer rows with smaller suffix (01 over 99)
                existing = by_3char.get(three)
                if existing:
                    if existing["sample_sub_code"] < rid:
                        continue
                by_3char[three] = {
                    "admin_code": three,
                    "rpfx": three[:2],
                    "sample_sub_code": rid,
                    "company_name": (r.get("Company Name") or "").strip(),
                    "_address_raw": (r.get("Company Address") or "").strip(),
                    "_primary_raw": (r.get("Primary Contact") or "").strip(),
                    "_change_raw": (r.get("Change Contact") or "").strip(),
                    "notes": (r.get("Notes") or "").strip(),
                }

    # Parse address + contacts
    rows = []
    for three, base in sorted(by_3char.items()):
        addr = parse_address(base["_address_raw"])
        primary = parse_contact(base["_primary_raw"])
        change = parse_contact(base["_change_raw"])
        rows.append({
            "rpfx": base["rpfx"],
            "admin_code": base["admin_code"],
            "sample_sub_code": base["sample_sub_code"],
            "company_name": base["company_name"],
            "street": addr["street"],
            "city": addr["city"],
            "state": addr["state"],
            "country": addr["country"],
            "zip": addr["zip"],
            "company_phone": addr["phone"],
            "primary_contact_name": primary["name"],
            "primary_contact_email": primary["email"],
            "primary_contact_phone": primary["phone"],
            "primary_contact_fax": primary["fax"],
            "change_contact_name": change["name"],
            "change_contact_email": change["email"],
            "change_contact_phone": change["phone"],
            "change_contact_fax": change["fax"],
            "notes": base["notes"],
        })

    out = DATA / "somos_contacts.parquet"
    pa_table = pa.Table.from_pylist(rows)
    pq.write_table(pa_table, out, compression="zstd")
    print(f"Wrote {out.name}: {len(rows):,} rows ({len(by_3char)} unique admin codes)")
    print(f"  with primary_email: {sum(1 for r in rows if r['primary_contact_email']):,}")

    # ---- Network detection ----
    by_email: dict[str, list[str]] = defaultdict(list)
    by_phone: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        em = (r["primary_contact_email"] or "").lower().strip()
        if em:
            by_email[em].append(r["admin_code"])
        ph = re.sub(r"\D", "", r["primary_contact_phone"] or "")
        if len(ph) >= 10:
            by_phone[ph[-10:]].append(r["admin_code"])

    networks = []
    for em, codes in by_email.items():
        codes = sorted(set(codes))
        if len(codes) >= 2:
            networks.append({
                "key": em,
                "key_type": "email",
                "n_admin_codes": len(codes),
                "admin_codes": ", ".join(codes),
                "rpfxs": ", ".join(sorted({c[:2] for c in codes})),
            })
    for ph, codes in by_phone.items():
        codes = sorted(set(codes))
        if len(codes) >= 2:
            networks.append({
                "key": ph,
                "key_type": "phone",
                "n_admin_codes": len(codes),
                "admin_codes": ", ".join(codes),
                "rpfxs": ", ".join(sorted({c[:2] for c in codes})),
            })

    networks.sort(key=lambda n: -n["n_admin_codes"])
    out2 = DATA / "admin_networks.parquet"
    pq.write_table(pa.Table.from_pylist(networks), out2, compression="zstd")
    print(f"Wrote {out2.name}: {len(networks):,} networks (email + phone clusters)")

    print()
    print("Top networks (≥3 resporgs):")
    print(f"  {'key_type':<7} {'count':<6} key  →  rpfxs")
    for n in networks[:20]:
        if n["n_admin_codes"] < 3:
            break
        key_short = n["key"][:50]
        rpfxs_short = n["rpfxs"][:80]
        print(f"  {n['key_type']:<7} {n['n_admin_codes']:<6} {key_short:<50}  {rpfxs_short}")


if __name__ == "__main__":
    main()
