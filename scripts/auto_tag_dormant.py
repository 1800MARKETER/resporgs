"""
Auto-tag resporgs as Dormant when they've held ≤14 numbers for 2 consecutive
months. (14 is the "test numbers only" floor — every rpfx gets 14 default
UNAVAIL test numbers from Somos.)

Reads:   clean/resporg.json, clean/resporgCategory.json, data/resporg_month.parquet
Writes:  Sanity Mutations API — adds the 'dead' (Dormant) category ref to
         qualifying resporgs. Idempotent — skips rpfxs already tagged.

Default is DRY-RUN. Pass --apply to actually write.

Usage:
  python scripts/auto_tag_dormant.py              # show candidates
  python scripts/auto_tag_dormant.py --apply      # tag them in Sanity
  python scripts/auto_tag_dormant.py --months 3   # require 3 consecutive months
"""

from __future__ import annotations
import argparse
import json
import os
import secrets
import sys
import urllib.request
from collections import defaultdict
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
CLEAN = ROOT / "clean"
DATA = ROOT / "data"

SANITY_PROJECT_ID = "52jbeh8g"
SANITY_DATASET = "blog"
SANITY_API_VERSION = "v2021-10-21"

DORMANT_THRESHOLD = 14   # ≤ this counts as dormant
DORMANT_SLUG = "dead"    # Sanity slug of the Dormant category


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


def sanity_patch(doc_id: str, patch: dict) -> tuple[bool, str]:
    if not TOKEN:
        return False, "no SANITY_API_TOKEN/SANITY_API_KEY"
    url = (
        f"https://{SANITY_PROJECT_ID}.api.sanity.io"
        f"/{SANITY_API_VERSION}/data/mutate/{SANITY_DATASET}"
    )
    body = {"mutations": [{"patch": {"id": doc_id, **patch}}]}
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
            return True, r.read().decode("utf-8")[:200]
    except Exception as e:
        detail = ""
        if hasattr(e, "read"):
            try:
                detail = e.read().decode("utf-8", errors="replace")[:300]
            except Exception:
                pass
        return False, f"{type(e).__name__}: {e} {detail}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=2, help="consecutive months ≤14 (default 2)")
    ap.add_argument("--apply", action="store_true", help="actually write to Sanity")
    args = ap.parse_args()

    # Find the dormant category id
    cats = json.loads((CLEAN / "resporgCategory.json").read_text(encoding="utf-8"))
    dormant_cat = next(
        (c for c in cats if (c.get("slug") or {}).get("current") == DORMANT_SLUG),
        None,
    )
    if not dormant_cat:
        print(f"ERROR: no Sanity category with slug '{DORMANT_SLUG}'.", file=sys.stderr)
        print("Create the Dormant category in Sanity Studio first.", file=sys.stderr)
        sys.exit(1)
    dormant_cat_id = dormant_cat["_id"].removeprefix("drafts.")
    print(f"Dormant category: {dormant_cat.get('title','?').strip()} (id {dormant_cat_id})")

    # Get the latest N months of inventory per rpfx
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT rpfx, month, inventory
        FROM read_parquet('{(DATA / "resporg_month.parquet").as_posix()}')
        ORDER BY rpfx, month
    """).fetchall()
    months_seen = sorted({r[1] for r in rows})
    if len(months_seen) < args.months:
        print(f"ERROR: only {len(months_seen)} months of data; need {args.months}.", file=sys.stderr)
        sys.exit(1)
    target_months = set(months_seen[-args.months:])
    print(f"Checking last {args.months} months: {sorted(target_months)}")

    # Per-rpfx: collect inventory across target months
    by_rpfx: dict[str, dict[str, int]] = defaultdict(dict)
    for rpfx, month, inv in rows:
        if month in target_months:
            by_rpfx[rpfx][month] = inv

    # Candidates: ALL target months present AND every one ≤ THRESHOLD
    candidates: set[str] = set()
    for rpfx, m in by_rpfx.items():
        if len(m) != args.months:
            # Missing month entirely → that's actually a stronger signal (zero
            # inventory, didn't even appear in snapshot). Treat absent as dormant.
            present = len(m)
            if present == 0:
                continue  # rpfx isn't in any of the target months — no Sanity doc anyway?
            # If only some months present and the present ones are ≤threshold,
            # treat absent months as 0 (still ≤ threshold) and qualify.
            if all(v <= DORMANT_THRESHOLD for v in m.values()):
                candidates.add(rpfx)
        elif all(v <= DORMANT_THRESHOLD for v in m.values()):
            candidates.add(rpfx)

    # Also catch rpfxs in Sanity that have ZERO presence in target months
    # (absent from latest snapshot entirely → certainly ≤ 14)
    sanity_resporgs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    sanity_pfx_to_doc = {}
    for d in sanity_resporgs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) >= 2 and not code.startswith("1"):  # skip synthetic
            sanity_pfx_to_doc[code[:2]] = d
    absent_pfxs = set(sanity_pfx_to_doc) - set(by_rpfx)
    candidates |= absent_pfxs

    # Filter to rpfxs that actually exist in Sanity
    candidates &= set(sanity_pfx_to_doc)

    # Skip rpfxs already tagged dormant
    already_tagged = []
    to_tag = []
    for pfx in sorted(candidates):
        d = sanity_pfx_to_doc[pfx]
        already = any(
            cref.get("_ref") == dormant_cat_id
            for cref in (d.get("categories") or [])
        )
        if already:
            already_tagged.append(pfx)
        else:
            to_tag.append((pfx, d))

    print()
    print(f"Total candidates: {len(candidates)}")
    print(f"  already tagged dormant: {len(already_tagged)}")
    print(f"  to tag: {len(to_tag)}")
    print()

    if to_tag:
        print(f"{'rpfx':<5} {'inv (last months)':<25} title")
        print("-" * 70)
        for pfx, d in to_tag[:40]:
            invs = by_rpfx.get(pfx, {})
            inv_str = ", ".join(
                f"{m}:{invs.get(m, 'absent')}" for m in sorted(target_months)
            )
            print(f"{pfx:<5} {inv_str:<25} {d.get('title','?')[:45]}")
        if len(to_tag) > 40:
            print(f"  ... and {len(to_tag) - 40} more")

    if not args.apply:
        print()
        print(f"Dry run. Re-run with --apply to tag {len(to_tag)} resporgs.")
        return

    # Apply: PATCH each doc to ADD the dormant category to its categories array
    if not TOKEN:
        print("ERROR: cannot --apply without SANITY_API_TOKEN/SANITY_API_KEY", file=sys.stderr)
        sys.exit(1)

    print()
    print(f"Applying to {len(to_tag)} docs...")
    ok = fail = 0
    for pfx, d in to_tag:
        new_refs = []
        for cref in d.get("categories") or []:
            new_refs.append({
                "_type": cref.get("_type", "reference"),
                "_ref": cref["_ref"],
                "_key": cref.get("_key") or secrets.token_hex(6),
            })
        new_refs.append({
            "_type": "reference",
            "_ref": dormant_cat_id,
            "_key": secrets.token_hex(6),
        })
        doc_id = d["_id"].removeprefix("drafts.")
        ok_, detail = sanity_patch(doc_id, {"set": {"categories": new_refs}})
        if ok_:
            ok += 1
        else:
            fail += 1
            print(f"  FAIL {pfx} ({doc_id}): {detail}")

    print()
    print(f"{ok} tagged, {fail} failed.")
    if ok:
        print()
        print("Don't forget to:")
        print("  1. python scripts/fetch_sanity_docs.py   (refresh local clean/)")
        print("  2. restart the webapp to pick up DORMANT_RPFX changes")


if __name__ == "__main__":
    main()
