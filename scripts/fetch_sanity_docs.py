"""
Pull the latest resporg / resporgCategory / resporgGroup docs from Sanity
and overwrite clean/*.json. Used after edits via tools/editor so the
web app + precomputes see current data without a full dataset export.

Uses the Sanity query API with GROQ. Token must be in apikey.env
(SANITY_API_KEY or SANITY_API_TOKEN).

Prefers published over draft when both exist (mirrors dedupe_sanity.py).
"""

from __future__ import annotations
import json
import os
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLEAN = ROOT / "clean"
CLEAN.mkdir(exist_ok=True)

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


def groq(query: str) -> list[dict]:
    """Run a GROQ query, return the result array.

    Uses the query endpoint with pagination-free behavior — we fetch all docs
    of a type in one shot (max tens of MB, well inside Sanity's limit)."""
    url = (
        f"https://{SANITY_PROJECT_ID}.api.sanity.io/{SANITY_API_VERSION}"
        f"/data/query/{SANITY_DATASET}?query={urllib.parse.quote(query)}"
    )
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {TOKEN}"}
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        payload = json.loads(r.read().decode("utf-8"))
    return payload.get("result", [])


def dedupe_prefer_published(docs: list[dict]) -> list[dict]:
    by_base: dict[str, dict] = defaultdict(dict)
    for d in docs:
        _id = d["_id"]
        base = _id[len("drafts."):] if _id.startswith("drafts.") else _id
        key = "draft" if _id.startswith("drafts.") else "published"
        by_base[base][key] = d
    out = []
    for versions in by_base.values():
        out.append(versions.get("published") or versions["draft"])
    return out


def fetch_type(type_name: str) -> int:
    raw = groq(f'*[_type == "{type_name}"]')
    cleaned = dedupe_prefer_published(raw)
    dest = CLEAN / f"{type_name}.json"
    dest.write_text(
        json.dumps(cleaned, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"  {type_name:<20} raw={len(raw):>4}  unique={len(cleaned):>4}  -> {dest.name}")
    return len(cleaned)


def main():
    print("Fetching Sanity docs...")
    total = 0
    for t in ("resporg", "resporgCategory", "resporgGroup"):
        total += fetch_type(t)
    print(f"Done. {total:,} docs written to {CLEAN}")


if __name__ == "__main__":
    main()
