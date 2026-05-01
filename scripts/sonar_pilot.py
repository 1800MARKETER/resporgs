"""Perplexity Sonar Pro pilot: enrich 3 resporgs (one per size tier) with
news + a separate deep-dive on toll-free industry news as a whole.

Output: data/sonar_pilot_2026-04-28.json with raw responses + extracted summaries.
Goal: calibrate output quality before scaling to ~150 resporgs.

Tier 1 — major carrier        : Bandwidth (JYT01)
Tier 2 — mid / single-company : HelloSpoke (YPT01)
Tier 3 — long tail / misdial  : Mayfair Communication (MYR01)
Industry deep-dive            : toll-free / RespOrg / number-portability news
"""

from __future__ import annotations
import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Load OpenRouter key from local-prospector/.env (where Bill keeps it)
def _load_env():
    candidates = [
        Path(r"C:\Users\Bill\claude code\local-prospector\.env"),
        ROOT / "apikey.env",
        ROOT / "apikey2.env",
    ]
    for p in candidates:
        if not p.exists():
            continue
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and v and k not in os.environ:
                os.environ[k] = v

_load_env()
OR_KEY = os.environ.get("OPENROUTER_API_KEY")
if not OR_KEY:
    print("OPENROUTER_API_KEY not found", file=sys.stderr); sys.exit(1)

OR_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL = "perplexity/sonar-pro"


def sonar(prompt: str, system: str = None) -> dict:
    """Single Sonar Pro call. Returns a dict with content + citations."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    body = json.dumps({
        "model": MODEL,
        "messages": messages,
        "temperature": 0.2,
    }).encode("utf-8")
    req = urllib.request.Request(
        OR_URL,
        data=body, method="POST",
        headers={
            "Authorization": f"Bearer {OR_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://resporgs.com",
            "X-Title": "Resporgs.com news enrichment",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"OpenRouter HTTP {e.code}: {body_txt}") from e

    choice = (data.get("choices") or [{}])[0]
    msg = choice.get("message") or {}
    return {
        "content": msg.get("content", ""),
        "citations": data.get("citations") or msg.get("citations") or [],
        "usage": data.get("usage", {}),
        "model": data.get("model"),
        "raw": data,
    }


SYSTEM = (
    "You are a research analyst specialized in US telecommunications, "
    "particularly toll-free numbering, RespOrgs, and the Somos toll-free "
    "registry. Write concise, factual summaries with inline citations to "
    "sources. If you find no relevant news, say so explicitly rather than "
    "inventing content. Prefer sources from somos.com, fcc.gov, telecom "
    "trade publications (Light Reading, FierceTelecom, Comm Daily, "
    "telecompetitor.com), the company's own website, and SEC filings."
)

def resporg_prompt(name: str, website: str, code: str, tier_note: str) -> str:
    return f"""Research **{name}** (RespOrg code {code}, website: {website}). {tier_note}

Provide:
1. **Company overview** (1-2 sentences): primary business, parent/holding company, year founded, HQ location
2. **Recent news** (past 24 months): up to 8 items as a JSON-formatted array of objects with these exact keys: date, title, snippet, source_domain, source_url. Cover acquisitions, FCC filings, lawsuits, RespOrg approvals, exec changes, product launches, regulatory actions.
3. **Toll-free / RespOrg-specific developments**: anything regarding their toll-free or RespOrg activity in particular
4. **Notable affiliations**: subsidiaries, parent groups, related operators sharing ownership

If you find nothing meaningful for any section, say "No significant news found" — don't invent items.

Return everything as a single JSON object with keys: overview, recent_news, toll_free_specific, affiliations.
"""

INDUSTRY_PROMPT = """Conduct a deep-dive on the **US toll-free numbering industry** for the past 18 months. Cover:

1. **Somos / RespOrg policy changes** — any new rules, audits, FCC orders, RespOrg revocations or approvals
2. **Number-shortage / area-code-exhaustion issues** — current state of 8XX inventory, projections, new SAC openings
3. **Misdial-marketing crackdowns** — FCC enforcement actions, FTC cases, class-action suits against companies acquiring toll-free numbers to monetize misdialed calls
4. **Texting / 10DLC convergence** — toll-free messaging policy, vetting requirements, regulatory shifts
5. **Major industry M&A** — acquisitions of CLECs, RespOrgs, vanity-number brokers
6. **Notable customer-impact stories** — businesses losing famous toll-free numbers, disputes over reassignment

Return a JSON object with each section as a key, each containing an array of items with date, headline, summary (2-3 sentences), source_domain, source_url. Aim for ~5-10 items per section where the news supports it. If a section is sparse, say so explicitly.
"""


def main():
    targets = [
        {
            "tier": 1,
            "code": "JYT01",
            "name": "Bandwidth",
            "website": "https://www.bandwidth.com",
            "tier_note": "Tier 1 — publicly-traded major carrier (NASDAQ: BAND). Should yield rich coverage.",
        },
        {
            "tier": 2,
            "code": "YPT01",
            "name": "HelloSpoke",
            "website": "https://hellospoke.com",
            "tier_note": "Tier 2 — small/mid voice service provider, Louisville KY. Expect limited but real coverage.",
        },
        {
            "tier": 3,
            "code": "MYR01",
            "name": "Mayfair Communication",
            "website": "",   # Bill's data shows no website tagged
            "tier_note": "Tier 3 — long-tail RespOrg, suspected misdial-marketing operator. Expect minimal news; FCC complaints would be the most likely hit.",
        },
    ]

    results = {"generated_at": datetime.utcnow().isoformat() + "Z", "model": MODEL,
               "resporgs": [], "industry": None}

    print(f"Running Sonar Pro pilot — model: {MODEL}\n")

    for t in targets:
        print(f"=== Tier {t['tier']}: {t['name']} ({t['code']}) ===")
        prompt = resporg_prompt(t["name"], t["website"], t["code"], t["tier_note"])
        try:
            resp = sonar(prompt, SYSTEM)
            print(f"  tokens: {resp['usage']}")
            print(f"  citations: {len(resp['citations'])}")
            print(f"  content (first 600 chars):")
            print(f"    {resp['content'][:600]!r}")
            print()
            results["resporgs"].append({**t, "response": resp})
        except Exception as e:
            print(f"  ERROR: {e}")
            results["resporgs"].append({**t, "error": str(e)})

    print("=== INDUSTRY DEEP-DIVE ===")
    try:
        ind = sonar(INDUSTRY_PROMPT, SYSTEM)
        print(f"  tokens: {ind['usage']}")
        print(f"  citations: {len(ind['citations'])}")
        print(f"  content (first 800 chars):")
        print(f"    {ind['content'][:800]!r}")
        results["industry"] = ind
    except Exception as e:
        print(f"  ERROR: {e}")
        results["industry"] = {"error": str(e)}

    out_dir = ROOT / "data"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "sonar_pilot_2026-04-28.json"
    out_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nFull results saved to: {out_path}")


if __name__ == "__main__":
    main()
