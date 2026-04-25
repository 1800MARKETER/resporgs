"""
Create stub Sanity docs for resporgs that exist in the Somos contacts
parquet but not yet in our Sanity dataset. Each new doc lands in the
"Unknown" category so Bill can review and classify them in the editor.

Reads:
  data/somos_contacts.parquet  (built by scripts/build_somos_contacts.py)
  clean/resporg.json           (current Sanity state — to find gap)
  clean/resporgCategory.json   (to find Unknown category id)

Writes:
  Sanity Mutations API — `create` for each missing rpfx with:
    _type: "resporg"
    title: company_name from Somos
    slug: derived from title (uniquified per rpfx if collision)
    codeTwoDigit: full 5-char Somos sample sub-code (e.g. "EFP01")
    address: {street1, city, state (2-letter), country, zip}
    categories: [Unknown]

Default is DRY-RUN. Pass --apply to write.

Usage:
  python scripts/create_missing_resporgs.py
  python scripts/create_missing_resporgs.py --apply
  python scripts/create_missing_resporgs.py --only EF        # one specific rpfx
"""

from __future__ import annotations
import argparse
import json
import os
import re
import secrets
import sys
import urllib.request
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
CLEAN = ROOT / "clean"
DATA = ROOT / "data"

SANITY_PROJECT_ID = "52jbeh8g"
SANITY_DATASET = "blog"
SANITY_API_VERSION = "v2021-10-21"

# Map full state names (what Somos returns) to 2-letter codes (Sanity convention)
US_STATES_2 = {
    "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA",
    "Colorado":"CO","Connecticut":"CT","Delaware":"DE","Florida":"FL","Georgia":"GA",
    "Hawaii":"HI","Idaho":"ID","Illinois":"IL","Indiana":"IN","Iowa":"IA",
    "Kansas":"KS","Kentucky":"KY","Louisiana":"LA","Maine":"ME","Maryland":"MD",
    "Massachusetts":"MA","Michigan":"MI","Minnesota":"MN","Mississippi":"MS",
    "Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV","New Hampshire":"NH",
    "New Jersey":"NJ","New Mexico":"NM","New York":"NY","North Carolina":"NC",
    "North Dakota":"ND","Ohio":"OH","Oklahoma":"OK","Oregon":"OR","Pennsylvania":"PA",
    "Rhode Island":"RI","South Carolina":"SC","South Dakota":"SD","Tennessee":"TN",
    "Texas":"TX","Utah":"UT","Vermont":"VT","Virginia":"VA","Washington":"WA",
    "West Virginia":"WV","Wisconsin":"WI","Wyoming":"WY",
    "District Of Columbia":"DC","District of Columbia":"DC","Puerto Rico":"PR",
}


def _load_env():
    env_file = ROOT / "apikey.env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip().upper()
        v = v.strip().strip('"').strip("'")
        if k and v and k not in os.environ:
            os.environ[k] = v


_load_env()
TOKEN = os.environ.get("SANITY_API_TOKEN") or os.environ.get("SANITY_API_KEY") or ""


def slugify(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s[:80] or "resporg"


def sanity_mutate(mutations: list[dict]) -> tuple[bool, str]:
    if not TOKEN:
        return False, "no SANITY_API_TOKEN/SANITY_API_KEY"
    url = (
        f"https://{SANITY_PROJECT_ID}.api.sanity.io"
        f"/{SANITY_API_VERSION}/data/mutate/{SANITY_DATASET}"
    )
    body = {"mutations": mutations, "returnIds": True}
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {TOKEN}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return True, r.read().decode("utf-8")[:500]
    except Exception as e:
        detail = ""
        if hasattr(e, "read"):
            try:
                detail = e.read().decode("utf-8", errors="replace")[:400]
            except Exception:
                pass
        return False, f"{type(e).__name__}: {e} {detail}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually write to Sanity")
    ap.add_argument("--only", help="comma-separated 2-char rpfxs (e.g. EF or EF,HM)")
    args = ap.parse_args()

    # Find Unknown category id
    cats = json.loads((CLEAN / "resporgCategory.json").read_text(encoding="utf-8"))
    unknown = next(
        (c for c in cats if (c.get("slug") or {}).get("current") == "unknown"),
        None,
    )
    if not unknown:
        print("ERROR: no Sanity category with slug 'unknown'.", file=sys.stderr)
        sys.exit(1)
    unknown_id = unknown["_id"].removeprefix("drafts.")

    # Sanity 2-char rpfxs already covered, plus existing slugs (for collision avoidance)
    sanity_docs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    existing_2char: set[str] = set()
    existing_slugs: set[str] = set()
    for d in sanity_docs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) >= 2:
            existing_2char.add(code[:2])
        slug = (d.get("slug") or {}).get("current")
        if slug:
            existing_slugs.add(slug)

    # Read Somos contacts
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT rpfx, sample_sub_code, company_name, street, city, state,
               country, zip, primary_contact_email
        FROM read_parquet('{(DATA / "somos_contacts.parquet").as_posix()}')
        ORDER BY rpfx
    """).fetchall()
    cols = ["rpfx", "sample_sub_code", "company_name", "street", "city",
            "state", "country", "zip", "primary_contact_email"]

    # Pick one row per missing 2-char rpfx (smallest sample_sub_code wins)
    missing: dict[str, dict] = {}
    for r in rows:
        d = dict(zip(cols, r))
        pfx = d["rpfx"]
        if pfx in existing_2char:
            continue
        # First-seen wins (CSV is sorted by sample_sub_code already)
        if pfx not in missing:
            missing[pfx] = d

    if args.only:
        wanted = {p.strip().upper() for p in args.only.split(",")}
        missing = {k: v for k, v in missing.items() if k in wanted}

    if not missing:
        print("Nothing to create.")
        return

    print(f"Will create {len(missing)} stub resporg(s) in 'Unknown' category.")
    print()
    print(f"{'rpfx':<5} {'code':<8} {'title':<40} city/state")
    print("-" * 95)
    for pfx, d in sorted(missing.items()):
        st = US_STATES_2.get(d["state"] or "", d["state"] or "")
        print(f"{pfx:<5} {d['sample_sub_code']:<8} {(d['company_name'] or '')[:40]:<40} {(d['city'] or '')[:20]}, {st}")

    if not args.apply:
        print()
        print("Dry run. Re-run with --apply to write to Sanity.")
        return

    print()
    print("Creating in Sanity...")

    # Build mutations — one create per missing rpfx
    used_slugs = set(existing_slugs)
    mutations = []
    for pfx, d in sorted(missing.items()):
        title = (d["company_name"] or "").strip() or f"Unknown ({pfx})"
        base_slug = slugify(title)
        slug = base_slug
        n = 2
        while slug in used_slugs:
            slug = f"{base_slug}-{pfx.lower()}" if n == 2 else f"{base_slug}-{n}"
            n += 1
        used_slugs.add(slug)

        state2 = US_STATES_2.get((d["state"] or "").strip(), (d["state"] or "").strip())
        country = "United States" if d["country"] in (None, "", "USA", "United States") else d["country"]

        addr: dict = {"_type": "address"}
        if d["street"]: addr["street1"] = d["street"]
        if d["city"]:   addr["city"]    = d["city"]
        if state2:      addr["state"]   = state2
        if country:     addr["country"] = country
        if d["zip"]:    addr["zip"]     = d["zip"]

        doc: dict = {
            "_type": "resporg",
            "title": title,
            "slug": {"_type": "slug", "current": slug},
            "codeTwoDigit": d["sample_sub_code"],
            "address": addr,
            "categories": [
                {"_type": "reference", "_ref": unknown_id, "_key": secrets.token_hex(6)},
            ],
        }
        mutations.append({"create": doc})

    # Send in batches of 25 to keep request size reasonable
    BATCH = 25
    ok_total = fail_total = 0
    for i in range(0, len(mutations), BATCH):
        chunk = mutations[i:i + BATCH]
        ok, detail = sanity_mutate(chunk)
        if ok:
            ok_total += len(chunk)
            print(f"  batch {i//BATCH + 1}: OK ({len(chunk)} docs)")
        else:
            fail_total += len(chunk)
            print(f"  batch {i//BATCH + 1}: FAIL — {detail}")

    print()
    print(f"{ok_total} created, {fail_total} failed.")
    if ok_total:
        print()
        print("Next steps:")
        print("  1. python scripts/fetch_sanity_docs.py   (refresh local clean/)")
        print("  2. classify in editor at http://localhost:5179 (filter Category=Unknown)")
        print("  3. when done, deploy: scp clean/*.json + restart resporgs service")


if __name__ == "__main__":
    main()
