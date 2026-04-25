"""
One-shot: strip the "unknown" category from any resporg that also has a
real category. Idempotent — safe to re-run.

"Real" means anything other than unknown / hidden / non-resporg.

Reads clean/resporg.json + clean/resporgCategory.json to find the offenders,
then PATCHes Sanity directly.

Dry-run by default. Pass --apply to actually write.
"""

from __future__ import annotations
import json
import os
import secrets
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLEAN = ROOT / "clean"

SANITY_PROJECT_ID = "52jbeh8g"
SANITY_DATASET = "blog"
SANITY_API_VERSION = "v2021-10-21"

META_SLUGS = {"unknown", "hidden", "non-resporg"}


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
    print("ERROR: SANITY_API_TOKEN / SANITY_API_KEY not set", file=sys.stderr)
    sys.exit(1)


def sanity_patch(doc_id: str, patch: dict) -> tuple[bool, str]:
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
    apply = "--apply" in sys.argv

    cats = json.loads((CLEAN / "resporgCategory.json").read_text(encoding="utf-8"))
    id_to_slug = {
        c["_id"].removeprefix("drafts."): (c.get("slug") or {}).get("current")
        for c in cats
    }
    unknown_id = next((cid for cid, s in id_to_slug.items() if s == "unknown"), None)
    if not unknown_id:
        print("No 'unknown' category found — nothing to do.")
        return

    resporgs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    offenders = []
    for d in resporgs:
        refs = [cref.get("_ref") for cref in (d.get("categories") or [])]
        slugs = {id_to_slug.get(r) for r in refs}
        if "unknown" in slugs and (slugs - META_SLUGS):
            offenders.append(d)

    print(f"{len(offenders)} resporgs have unknown + real category.")
    if not offenders:
        return

    for d in offenders[:10]:
        code = (d.get("codeTwoDigit") or "")[:2]
        slugs = sorted({id_to_slug.get(cref.get("_ref")) for cref in d.get("categories") or []})
        print(f"  {code}  {d.get('title', '?')[:40]:<42}  {slugs}")
    if len(offenders) > 10:
        print(f"  ... and {len(offenders) - 10} more")

    if not apply:
        print("\nDry run. Re-run with --apply to write to Sanity.")
        return

    print(f"\nApplying to {len(offenders)} docs...")
    ok_count = 0
    fail_count = 0
    for d in offenders:
        new_refs = []
        for cref in d.get("categories") or []:
            if cref.get("_ref") == unknown_id:
                continue  # drop it
            # Keep the original shape — _ref, _type, _key
            new_refs.append(
                {
                    "_type": cref.get("_type", "reference"),
                    "_ref": cref["_ref"],
                    "_key": cref.get("_key") or secrets.token_hex(6),
                }
            )
        doc_id = d["_id"].removeprefix("drafts.")
        ok, detail = sanity_patch(doc_id, {"set": {"categories": new_refs}})
        if ok:
            ok_count += 1
        else:
            fail_count += 1
            print(f"  FAIL {doc_id}: {detail}")

    print(f"\n{ok_count} patched, {fail_count} failed.")


if __name__ == "__main__":
    main()
