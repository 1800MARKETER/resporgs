"""One-shot: remove logoImage from the Primetel group + its 10 member resporgs.

The Primetel resporgs all shared the same hand-drawn gorilla SVG. The data we're
publishing about misdial-farm patterns is damaging enough on its own — Bill
doesn't want the cute/snarky image alongside it because it could read as
piling on. Pulling the image leaves the records using initials fallback.

Patches both the published doc and any drafts.<id> form to make sure the change
is visible immediately and not waiting on a "publish draft" workflow.

Reversible: only removes the reference. Asset stays in Sanity's library.
"""

from __future__ import annotations
import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

SANITY_PROJECT_ID = "52jbeh8g"
SANITY_DATASET = "blog"
SANITY_API_VERSION = "v2021-10-21"


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
if not TOKEN:
    print("ERROR: SANITY_API_TOKEN / SANITY_API_KEY not set in apikey.env", file=sys.stderr)
    sys.exit(1)


# (10 Primetel-group resporgs + the resporgGroup doc itself)
TARGET_IDS = [
    "142bfee7-add4-46f8-ada6-a3dc77679242",   # JWS01 WireStar
    "5b94be1d-b033-4569-9b4b-d9bb51cb5e25",   # HLK01 Unilink Telcom
    "9dd903ba-2b57-492e-b63c-264545b47f03",   # YLC01 Yorkshire Telecom
    "acceffd0-3315-480d-a6b9-0c93f3bc3f46",   # BUK01 Bluekey Communications
    "b0d008a9-1478-4f45-84f0-a364e973abc8",   # NTW01 Nextway Communications
    "cc9208ec-5a6b-4437-840b-afe3d8734396",   # MYR01 Mayfair Communication
    "d0d231da-54db-40c0-bd40-1dd22fd31046",   # ZPL01 Zipline
    "df91ccc0-cdcb-4bbd-a31c-2f1b8aa1a1f3",   # CBW01 Crossbow Telecom
    "e463835d-b83a-4fce-9aa9-248dcdff52e5",   # CRE01 Coore, Inc.
    "f56848fa-eab7-4ad9-9a42-6a04c448ee5c",   # BAM01 Beckham Telecom
    "c1d274ef-f548-41c1-ae50-84681803f420",   # Primetel resporgGroup
]


def groq_query(query: str) -> list[dict]:
    url = (f"https://{SANITY_PROJECT_ID}.api.sanity.io/{SANITY_API_VERSION}"
           f"/data/query/{SANITY_DATASET}?query={urllib.parse.quote(query)}")
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())["result"]


def find_existing_ids(target_ids: list[str]) -> tuple[list[str], list[str]]:
    """Return (existing_published, existing_drafts) by querying which IDs
    actually exist in Sanity."""
    quoted = ",".join(f'"{i}"' for i in target_ids)
    pub = groq_query(f'*[_id in [{quoted}]]{{ _id, _type, title, "hasLogo": defined(logoImage) }}')
    draft_ids_to_probe = [f"drafts.{i}" for i in target_ids]
    quoted_drafts = ",".join(f'"{i}"' for i in draft_ids_to_probe)
    drafts = groq_query(f'*[_id in [{quoted_drafts}]]{{ _id, _type, title, "hasLogo": defined(logoImage) }}')
    return pub, drafts


def mutate(patches: list[dict]) -> dict:
    body = json.dumps({"mutations": patches}).encode("utf-8")
    url = (f"https://{SANITY_PROJECT_ID}.api.sanity.io/{SANITY_API_VERSION}"
           f"/data/mutate/{SANITY_DATASET}")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def main():
    print("Querying Sanity for current state of all 11 target documents...")
    pub_docs, draft_docs = find_existing_ids(TARGET_IDS)
    print(f"  published versions found: {len(pub_docs)}")
    for d in pub_docs:
        print(f"    {d['_id']:<40} type={d['_type']:<14} hasLogo={d['hasLogo']}  {d.get('title','')}")
    print(f"  draft versions found: {len(draft_docs)}")
    for d in draft_docs:
        print(f"    {d['_id']:<40} type={d['_type']:<14} hasLogo={d['hasLogo']}  {d.get('title','')}")

    # Build patch list — only patch docs that exist AND currently have a logoImage
    to_patch = []
    for d in pub_docs + draft_docs:
        if d.get("hasLogo"):
            to_patch.append(d["_id"])

    if not to_patch:
        print("\nNothing to do — none of the targeted documents currently have a logoImage.")
        return

    print(f"\nWill issue {len(to_patch)} unset patches. IDs:")
    for i in to_patch:
        print(f"  {i}")

    if "--apply" not in sys.argv:
        print("\n(Dry run — re-run with --apply to actually push the mutations.)")
        return

    patches = [{"patch": {"id": i, "unset": ["logoImage"]}} for i in to_patch]
    print(f"\nApplying {len(patches)} mutations...")
    result = mutate(patches)
    print(f"  transactionId: {result.get('transactionId')}")
    print(f"  results:       {len(result.get('results') or [])} entries")
    # Verify
    print("\nRe-querying to verify...")
    pub_after, draft_after = find_existing_ids(TARGET_IDS)
    still_has = [d for d in (pub_after + draft_after) if d.get("hasLogo")]
    if still_has:
        print(f"  WARNING: {len(still_has)} doc(s) still have logoImage:")
        for d in still_has:
            print(f"    {d['_id']}  {d.get('title','')}")
    else:
        print("  All targeted documents now have no logoImage. Done.")


if __name__ == "__main__":
    main()
