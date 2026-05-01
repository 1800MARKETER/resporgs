"""Push the AI-generated article drafts (data/article_drafts.json) into
Sanity as `drafts.<id>` post documents. They land unpublished — Bill
edits + publishes from Sanity Studio.

createOrReplace is used so re-running with the same JSON updates the
existing draft instead of duplicating.
"""

from __future__ import annotations
import json
import os
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

SANITY_PROJECT_ID = "52jbeh8g"
SANITY_DATASET    = "blog"
SANITY_API_VER    = "v2021-10-21"


def _load_env():
    env = ROOT / "apikey.env"
    if not env.exists(): return
    for line in env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line: continue
        k, _, v = line.partition("=")
        k = k.strip().upper(); v = v.strip().strip('"').strip("'")
        if k and v and k not in os.environ: os.environ[k] = v
_load_env()
TOKEN = os.environ.get("SANITY_API_TOKEN") or os.environ.get("SANITY_API_KEY") or ""
if not TOKEN: sys.exit("SANITY_API_TOKEN missing")


def mutate(patches):
    body = json.dumps({"mutations": patches}).encode("utf-8")
    url = f"https://{SANITY_PROJECT_ID}.api.sanity.io/{SANITY_API_VER}/data/mutate/{SANITY_DATASET}"
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type":"application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def main():
    src = ROOT / "data" / "article_drafts.json"
    if not src.exists(): sys.exit(f"missing {src}")
    payload = json.loads(src.read_text(encoding="utf-8"))
    drafts = payload.get("drafts", [])
    print(f"Drafts to push: {len(drafts)}")
    for d in drafts:
        print(f"  - {d['_id']}  {d.get('title','')[:80]}")

    if "--apply" not in sys.argv:
        print("\n(Dry-run — re-run with --apply to push.)")
        return

    patches = [{"createOrReplace": d} for d in drafts]
    result = mutate(patches)
    print(f"\nSanity transaction: {result.get('transactionId')}")
    print("Drafts visible at: https://resporgs.sanity.studio/desk/post (Drafts tab)")


if __name__ == "__main__":
    main()
