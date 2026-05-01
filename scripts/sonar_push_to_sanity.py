"""Push enriched resporg news data to Sanity + update local clean/resporg.json
so the webapp sees the changes immediately without needing a fetch_sanity_docs
re-pull.

Reads:  data/resporg_news_enrichment.json (output of sonar_enrich_resporgs.py)
Writes: Sanity mutate API (patches each resporg doc with aiOverview, aiAffiliations,
        aiTollFreeSpecific, aiOverviewConfidence, aiOverviewUpdated, recentNews)
        + local clean/resporg.json (mirror of the patch for instant frontend visibility)

Idempotent: re-running with the same JSON re-applies the same fields.
"""

from __future__ import annotations
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime
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


def mutate(patches: list[dict]) -> dict:
    body = json.dumps({"mutations": patches}).encode("utf-8")
    url = f"https://{SANITY_PROJECT_ID}.api.sanity.io/{SANITY_API_VER}/data/mutate/{SANITY_DATASET}"
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type":"application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())


def build_set_payload(parsed: dict, generated_at: str) -> dict:
    """Convert Sonar Pro's parsed output to the Sanity field shape."""
    overview   = (parsed.get("overview") or "").strip()
    affil      = (parsed.get("affiliations") or "").strip()
    tf_spec    = (parsed.get("toll_free_specific") or "").strip()
    confidence = (parsed.get("confidence") or "").strip().lower()
    rn_in = parsed.get("recent_news") or []

    rn_out = []
    for i, n in enumerate(rn_in):
        if not isinstance(n, dict): continue
        title = (n.get("title") or "").strip()
        if not title: continue
        rn_out.append({
            "_key":          f"rn{i}",      # Sanity arrays need _key per item
            "_type":         "newsItem",
            "date":          (n.get("date") or "").strip(),
            "title":         title,
            "snippet":       (n.get("snippet") or "").strip(),
            "sourceDomain":  (n.get("source_domain") or "").strip(),
            "sourceUrl":     (n.get("source_url") or "").strip(),
        })

    set_payload = {}
    if overview:   set_payload["aiOverview"]            = overview
    if affil:      set_payload["aiAffiliations"]        = affil
    if tf_spec and tf_spec.lower() != "no significant news found":
        set_payload["aiTollFreeSpecific"]               = tf_spec
    if confidence: set_payload["aiOverviewConfidence"]  = confidence
    set_payload["aiOverviewUpdated"]                    = generated_at
    set_payload["recentNews"]                           = rn_out
    return set_payload


def main():
    src = ROOT / "data" / "resporg_news_enrichment.json"
    if not src.exists():
        sys.exit(f"missing {src}")
    payload = json.loads(src.read_text(encoding="utf-8"))
    generated_at = payload.get("generated_at") or datetime.utcnow().isoformat() + "Z"
    records = payload.get("resporgs", [])
    print(f"loaded {len(records)} enrichment records")

    # Build patch list — only push where parse succeeded AND there's content
    patches = []
    local_updates: dict[str, dict] = {}     # _id -> set_payload (for local mirror)
    skipped_parse = 0
    skipped_empty = 0

    for r in records:
        if r.get("error"):
            continue
        parsed = r.get("parsed")
        if not parsed:
            # Try to rescue raw_content with a more aggressive cleaner
            raw = r.get("raw_content","")
            try:
                # Strip trailing junk after final closing brace
                end = raw.rfind("}")
                if end > 0:
                    parsed = json.loads(raw[:end+1])
            except Exception:
                pass
        if not parsed:
            skipped_parse += 1
            continue

        set_payload = build_set_payload(parsed, generated_at)
        # Skip if there's literally no useful content
        if not set_payload.get("aiOverview") and not set_payload.get("recentNews"):
            skipped_empty += 1
            continue

        rid = r["_id"]
        patches.append({"patch": {"id": rid, "set": set_payload}})
        local_updates[rid] = set_payload

    print(f"  patches to push: {len(patches)}")
    print(f"  skipped (parse-fail): {skipped_parse}")
    print(f"  skipped (no content): {skipped_empty}")

    if "--apply" not in sys.argv:
        print("\n(Dry-run — re-run with --apply to push to Sanity + update local clean/.)")
        return

    # Mutate in batches of 50 (Sanity tolerates larger but smaller is friendlier)
    BATCH = 50
    txn_ids = []
    for i in range(0, len(patches), BATCH):
        chunk = patches[i:i+BATCH]
        print(f"  pushing batch {i//BATCH + 1} ({len(chunk)} patches)...")
        result = mutate(chunk)
        txn_ids.append(result.get("transactionId"))
    print(f"  Sanity transactions: {txn_ids}")

    # Mirror to local clean/resporg.json so the webapp sees the changes now.
    clean_path = ROOT / "clean" / "resporg.json"
    docs = json.loads(clean_path.read_text(encoding="utf-8"))
    n_updated = 0
    for d in docs:
        if d["_id"] in local_updates:
            for k, v in local_updates[d["_id"]].items():
                d[k] = v
            n_updated += 1
    clean_path.write_text(
        json.dumps(docs, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  local clean/resporg.json updated: {n_updated} resporgs.")
    print("\nDone. Restart the webapp (or wait for next reload) to see the new sections.")


if __name__ == "__main__":
    main()
