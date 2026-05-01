"""Rewrite every news-item snippet to exactly two sentences for consistency
on the /news index page and on per-resporg profile pages.

Uses Claude Haiku via OpenRouter — cheap, fast, summarization-focused.

Updates BOTH Sanity (so the source of truth has the new snippets) AND the
local clean/resporg.json mirror (so the live webapp picks them up).

Idempotent / resume-safe via a side-channel cache at
data/news_summaries_cache.json — keyed on (title + original snippet)
so re-running won't re-bill items already summarized.
"""

from __future__ import annotations
import json
import os
import sys
import time
import urllib.request
import urllib.error
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
OR_KEY = os.environ.get("OPENROUTER_API_KEY") or sys.exit("OPENROUTER_API_KEY missing")
SANITY_TOKEN = os.environ.get("SANITY_API_TOKEN") or os.environ.get("SANITY_API_KEY")
if not SANITY_TOKEN: sys.exit("SANITY_API_TOKEN missing")

OR_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL  = "anthropic/claude-3-haiku"

SANITY_PROJECT_ID = "52jbeh8g"
SANITY_DATASET    = "blog"
SANITY_API_VER    = "v2021-10-21"

CACHE_PATH = ROOT / "data" / "news_summaries_cache.json"


def haiku(title: str, snippet: str) -> str:
    prompt = (
        "You will be given a news headline and a brief existing summary. "
        "Rewrite the summary as exactly TWO complete sentences. The first "
        "sentence should state the news event with key facts (who, what, "
        "when, where as available). The second sentence should add context, "
        "consequence, or significance. Use only information present in the "
        "input — do NOT invent facts or add your own opinion. Plain prose, "
        "no bullets or lists. Return only the two sentences, nothing else.\n\n"
        f"Headline: {title}\n"
        f"Existing summary: {snippet}\n"
        "Two-sentence summary:"
    )
    body = json.dumps({
        "model": MODEL,
        "messages": [{"role":"user","content":prompt}],
        "temperature": 0.2,
        "max_tokens": 200,
    }).encode("utf-8")
    req = urllib.request.Request(OR_URL, data=body, method="POST", headers={
        "Authorization": f"Bearer {OR_KEY}", "Content-Type":"application/json",
        "HTTP-Referer": "https://resporgs.com", "X-Title": "Resporgs.com news summaries",
    })
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"OpenRouter HTTP {e.code}: {body_txt}") from e
    msg = (data.get("choices") or [{}])[0].get("message", {})
    return (msg.get("content") or "").strip()


def mutate(patches):
    body = json.dumps({"mutations": patches}).encode("utf-8")
    url = f"https://{SANITY_PROJECT_ID}.api.sanity.io/{SANITY_API_VER}/data/mutate/{SANITY_DATASET}"
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "Authorization": f"Bearer {SANITY_TOKEN}", "Content-Type":"application/json",
    })
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def main():
    cache = {}
    if CACHE_PATH.exists():
        try: cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except: cache = {}

    docs = json.loads((ROOT / "clean" / "resporg.json").read_text(encoding="utf-8"))
    n_total = sum(len(d.get("recentNews") or []) for d in docs)
    print(f"News items to consider: {n_total}")

    apply = "--apply" in sys.argv
    n_summarized = 0
    n_cached = 0
    patches = []

    for d in docs:
        rn = d.get("recentNews") or []
        if not rn: continue
        changed = False
        for n in rn:
            if not isinstance(n, dict): continue
            title = (n.get("title") or "").strip()
            orig = (n.get("snippet") or "").strip()
            if not title or not orig: continue
            cache_key = f"{title}||{orig}"
            if cache_key in cache:
                new_snip = cache[cache_key]
                n_cached += 1
            else:
                try:
                    new_snip = haiku(title, orig)
                    cache[cache_key] = new_snip
                    n_summarized += 1
                    if n_summarized % 10 == 0:
                        CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
                    time.sleep(0.2)
                except Exception as e:
                    print(f"  err on {title[:40]!r}: {e}")
                    continue
            if new_snip and new_snip != orig:
                n["snippet"] = new_snip
                changed = True
        if changed and apply:
            patches.append({"patch": {"id": d["_id"],
                                       "set": {"recentNews": rn}}})

    CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  Summarized this run: {n_summarized}")
    print(f"  Used cache:          {n_cached}")
    print(f"  Resporgs to patch:   {len(patches)}")

    if not apply:
        print("\n(Dry-run — re-run with --apply to push to Sanity + update local clean/.)")
        return

    # Push patches
    BATCH = 50
    for i in range(0, len(patches), BATCH):
        chunk = patches[i:i+BATCH]
        result = mutate(chunk)
        print(f"  pushed batch {i//BATCH + 1}: {result.get('transactionId')}")

    # Mirror to local clean/resporg.json
    (ROOT / "clean" / "resporg.json").write_text(
        json.dumps(docs, indent=2, ensure_ascii=False), encoding="utf-8")
    print("  local clean/resporg.json updated.")


if __name__ == "__main__":
    main()
