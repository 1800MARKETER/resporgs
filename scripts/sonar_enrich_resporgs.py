"""Production: enrich resporgs (those with websites, not Unknown/NON/Dormant)
with company overview + recent news from Sonar Pro.

Saves results to data/resporg_news_enrichment.json. Sanity push is a
separate step — review JSON first, then run sonar_push_to_sanity.py
(separate script, not yet written) when ready.
"""

from __future__ import annotations
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

def _load_env():
    for p in [Path(r"C:\Users\Bill\claude code\local-prospector\.env"),
              ROOT / "apikey.env", ROOT / "apikey2.env"]:
        if not p.exists(): continue
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            k, _, v = line.partition("=")
            k = k.strip(); v = v.strip().strip('"').strip("'")
            if k and v and k not in os.environ: os.environ[k] = v
_load_env()

OR_KEY = os.environ.get("OPENROUTER_API_KEY")
if not OR_KEY: sys.exit("OPENROUTER_API_KEY missing")

OR_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL  = "perplexity/sonar-pro"

# Skip these category titles entirely
SKIP_CAT_TITLES = {"Unknown", "NON Resporg", "Dormant"}


SYSTEM = (
    "You are a research analyst for resporgs.com, a US toll-free industry "
    "publication. Write factual, citation-backed summaries of "
    "telecommunications RespOrgs. If sources are weak or absent for a "
    "given section, say so explicitly — do not fabricate. Prefer primary "
    "sources: somos.com, fcc.gov (especially ecfsapi.fcc.gov dockets), "
    "Federal Register, SEC EDGAR (10-K, 8-K, S-1), the company's own "
    "website, Light Reading, FierceTelecom, TR Daily, Communications "
    "Daily, telecompetitor.com, RCRWireless. Always include direct source "
    "URLs."
)


def build_prompt(resporg) -> str:
    name    = resporg["title"]
    code    = resporg.get("codeTwoDigit","?")
    website = resporg.get("website","(unknown)")
    addr    = resporg.get("address") or {}
    addr_line = ", ".join(filter(None, [addr.get("city"), addr.get("state"), addr.get("country")]))
    return (
        f"Research **{name}** (RespOrg code: {code}; website: {website}"
        f"{'; HQ: ' + addr_line if addr_line else ''}). Return a single JSON "
        "object — no commentary outside the JSON — with these keys:\n\n"
        '  "overview"          — 2-3 sentence factual summary: primary business, '
        'parent or holding company, year founded, HQ location, public/private, '
        'rough size if known\n'
        '  "recent_news"       — array of up to 5 news items from the past 36 '
        'months, each with: date (YYYY-MM-DD or YYYY-MM if exact day unknown), '
        'title, snippet (1-2 sentences), source_domain, source_url. Include '
        'acquisitions, funding rounds, FCC filings, lawsuits, RespOrg '
        'approvals, exec changes, product launches, partnerships, '
        'regulatory actions, earnings highlights for public companies, '
        'and any meaningful corporate developments. Even smaller items '
        'count — better to include 2-3 mid-relevance items than zero. '
        'Only return [] if you genuinely find nothing.\n'
        '  "toll_free_specific"— string: any news regarding their toll-free or '
        'RespOrg activity specifically. "" if none.\n'
        '  "affiliations"      — string: subsidiaries, parent groups, related '
        'operators sharing ownership. "" if none.\n'
        '  "confidence"        — one of "high" / "medium" / "low" indicating '
        'how confident you are in the overview based on source quality\n\n'
        "Output the raw JSON object only. No prose before or after. If you "
        "find no significant news, set recent_news to [] and say so in "
        "toll_free_specific or overview as appropriate."
    )


def sonar(prompt: str) -> dict:
    body = json.dumps({
        "model": MODEL,
        "messages": [{"role":"system","content":SYSTEM},
                     {"role":"user","content":prompt}],
        "temperature": 0.2,
    }).encode("utf-8")
    req = urllib.request.Request(OR_URL, data=body, method="POST", headers={
        "Authorization": f"Bearer {OR_KEY}", "Content-Type":"application/json",
        "HTTP-Referer": "https://resporgs.com", "X-Title":"Resporgs.com news",
    })
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode("utf-8", errors="replace")[:400]
        raise RuntimeError(f"OpenRouter HTTP {e.code}: {body_txt}") from e


def main():
    cats = {c["_id"]: c.get("title","").strip()
            for c in json.load(open(ROOT/"clean/resporgCategory.json", encoding="utf-8"))}
    skip_ids = {cid for cid, t in cats.items() if t in SKIP_CAT_TITLES}

    resporgs = json.load(open(ROOT/"clean/resporg.json", encoding="utf-8"))
    eligible = []
    for r in resporgs:
        if not r.get("website"): continue
        rcats = {c.get("_ref") for c in (r.get("categories") or [])}
        if rcats & skip_ids: continue
        eligible.append(r)

    print(f"Eligible resporgs: {len(eligible)}")

    # Allow --limit N for testing
    limit = None
    for i, a in enumerate(sys.argv):
        if a == "--limit" and i+1 < len(sys.argv):
            limit = int(sys.argv[i+1])
    if limit:
        eligible = eligible[:limit]
        print(f"  limited to first {limit}")

    out_path = ROOT / "data" / "resporg_news_enrichment.json"
    out_path.parent.mkdir(exist_ok=True)

    # Resume support: load existing results and skip those already processed
    existing = {}
    if out_path.exists():
        prev = json.load(open(out_path, encoding="utf-8"))
        existing = {r["_id"]: r for r in prev.get("resporgs", []) if r.get("_id")}
        print(f"  resuming — {len(existing)} already done")

    out = {"generated_at": datetime.utcnow().isoformat()+"Z",
           "model": MODEL, "resporgs": list(existing.values())}
    total_cost = 0.0

    for i, r in enumerate(eligible, 1):
        rid = r["_id"]
        if rid in existing:
            continue
        print(f"[{i}/{len(eligible)}] {r['title']} ({r.get('codeTwoDigit','?'):<5})  ", end="", flush=True)
        try:
            data = sonar(build_prompt(r))
            choice = (data.get("choices") or [{}])[0]
            msg = choice.get("message") or {}
            content = msg.get("content","").strip()
            usage = data.get("usage", {})
            cost = usage.get("cost", 0)
            total_cost += cost
            # Try to parse content as JSON. Sonar Pro occasionally emits
            # malformed JSON like ',""' between fields — patch those out.
            parsed = None
            err = None
            cleaned = content
            if cleaned.startswith("```"):
                cleaned = cleaned.strip("`")
                if cleaned.lstrip().startswith("json"):
                    cleaned = cleaned.lstrip()[4:].strip()
            # Common malformations Sonar Pro emits:
            #   ',""' (empty unnamed key inserted between fields)
            #   ',,'  (double comma)
            import re as _re
            cleaned2 = _re.sub(r',\s*"[^"]*"\s*,', ',', cleaned)   # remove ,"...":,
            cleaned2 = _re.sub(r',\s*""\s*,', ',', cleaned2)
            cleaned2 = _re.sub(r',\s*,', ',', cleaned2)
            for attempt in (cleaned, cleaned2):
                try:
                    parsed = json.loads(attempt)
                    break
                except Exception as e:
                    err = f"json-parse: {e}"
            if not parsed:
                err = f"json-parse failed after cleanup: {err}"
            print(f"cost=${cost:.4f}  {'OK' if parsed else 'parse-fail'}  ({usage.get('completion_tokens',0)} tok)")
            out["resporgs"].append({
                "_id": rid, "codeTwoDigit": r.get("codeTwoDigit"),
                "title": r["title"], "website": r.get("website"),
                "raw_content": content, "parsed": parsed,
                "parse_error": err, "usage": usage,
            })
        except Exception as e:
            print(f"ERROR: {e}")
            out["resporgs"].append({
                "_id": rid, "codeTwoDigit": r.get("codeTwoDigit"),
                "title": r["title"], "error": str(e),
            })
        # Persist after each call so we don't lose progress on interrupt
        out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False),
                            encoding="utf-8")
        # Light rate-limit
        time.sleep(0.3)

    print(f"\nDone. {len(out['resporgs'])} total records.")
    print(f"Total cost (this run + prior): ${total_cost:.4f}")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
