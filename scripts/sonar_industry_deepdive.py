"""Multi-angle industry deep-dive via Sonar Pro.

Each query is narrowly scoped so Sonar Pro can find good sources for that
specific topic rather than returning 'no results' for a kitchen-sink prompt.

The output is intentionally article-shaped: each query corresponds to one
potential publishable piece on tollfreenumbers.com / resporgs.com.
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

SYSTEM = (
    "You are a research analyst for a US toll-free telecommunications "
    "publication. Write factual, citation-backed summaries. If sources "
    "are weak or absent, say so explicitly — never fabricate. Prefer "
    "primary sources: somos.com, fcc.gov, ecfsapi.fcc.gov (FCC dockets), "
    "Federal Register, SEC EDGAR, Light Reading, FierceTelecom, TR Daily, "
    "Communications Daily, telecompetitor.com, RCRWireless. Return clean "
    "Markdown unless the prompt requests JSON."
)


def sonar(prompt: str) -> dict:
    body = json.dumps({
        "model": MODEL,
        "messages": [{"role":"system","content":SYSTEM},
                     {"role":"user","content":prompt}],
        "temperature": 0.2,
    }).encode("utf-8")
    req = urllib.request.Request(OR_URL, data=body, method="POST", headers={
        "Authorization": f"Bearer {OR_KEY}", "Content-Type": "application/json",
        "HTTP-Referer": "https://resporgs.com", "X-Title": "Resporgs.com news",
    })
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"OpenRouter HTTP {e.code}: {body_txt}") from e


QUERIES = [
    {
        "id": "somos-administrator-renewal",
        "title": "Somos's role as Toll-Free Numbering Administrator — contract / renewal status",
        "prompt": (
            "Research the current status of **Somos, Inc.'s role as the federally-"
            "designated Toll-Free Numbering Administrator (TFNA)**, also referred "
            "to as the 'SMS/800 Number Administrator.' I'm specifically interested in:\n\n"
            "1. The current FCC contract / designation status — when was Somos's "
            "designation last renewed, when does it next come up for renewal, "
            "is there an active proceeding or RFP?\n"
            "2. Any FCC dockets, NPRMs, or orders concerning the TFNA role over "
            "the past 24 months\n"
            "3. Has any other entity proposed to take over the TFNA role? Has "
            "Somos's performance been challenged?\n"
            "4. The history of how Somos came to hold this role (DSMI / SMS/800 "
            "transition era).\n\n"
            "Cite FCC dockets, federal register notices, and trade press. "
            "If specific docket numbers exist, include them. If you cannot find "
            "concrete information, say so plainly."
        ),
    },
    {
        "id": "brn-toll-free-jan-2026",
        "title": "January 1, 2026 Business Registration Number requirement for toll-free verifications — full details",
        "prompt": (
            "Provide the complete, actionable detail on the **toll-free messaging "
            "verification rule that takes effect January 1, 2026**, requiring "
            "submissions to include a Business Registration Number (BRN), issuing "
            "country of registration, and legal entity type. Specifically:\n\n"
            "1. Who issued the rule — is it Somos, the FCC, both, an industry body?\n"
            "2. Which submissions are affected — only NEW verifications, or also "
            "renewals, or existing verified numbers?\n"
            "3. What counts as a valid BRN — EIN? state-business-license number? "
            "DUNS? what about non-US entities?\n"
            "4. Penalty for non-compliance — rejection, delay, suspension?\n"
            "5. Timeline / grace period if any\n"
            "6. How it relates to broader 10DLC vetting standards\n"
            "7. Critical reaction from industry — concerns about small businesses, "
            "international operators, etc.\n\n"
            "This is going to be turned into a published article. Provide specific "
            "source URLs for everything."
        ),
    },
    {
        "id": "fcc-dno-mandate-dec-2025",
        "title": "December 2025 FCC do-not-originate (DNO) call-blocking mandate",
        "prompt": (
            "Explain the **FCC's December 15, 2025 do-not-originate (DNO) call-"
            "blocking mandate** in detail. What does the order require voice "
            "providers to do? Which categories of numbers must be blocked "
            "(invalid, unallocated, do-not-originate, etc.)? How is this "
            "related to STIR/SHAKEN? What is the impact on toll-free numbers "
            "specifically? Cite FCC docket numbers and the order itself."
        ),
    },
    {
        "id": "tf-area-code-exhaustion-2026",
        "title": "Toll-free area code (8XX) exhaustion outlook in 2026",
        "prompt": (
            "What is the current state of toll-free numbering inventory in 2026? "
            "How many of the 8XX SACs (800, 888, 877, 866, 855, 844, 833, 822) "
            "are open vs nearly exhausted? Has the FCC scheduled the opening of "
            "the next code (e.g., 811, 880, 887)? What is the latest projection "
            "from Somos on when current inventory will be exhausted? Is there "
            "any policy debate about hoarding, warehousing, or RespOrg numbering "
            "behavior contributing to faster exhaustion?"
        ),
    },
    {
        "id": "misdial-fcc-enforcement",
        "title": "FCC / FTC enforcement against misdial-marketing operators",
        "prompt": (
            "Find any **FCC or FTC enforcement actions, complaints, lawsuits, or "
            "policy proposals targeting toll-free 'misdial marketing' operators** "
            "— companies that acquire toll-free vanity numbers solely to monetize "
            "callers who misdial popular numbers. The practice is sometimes called "
            "'traffic pumping' or 'misdial monetization.' I want news from the "
            "past 36 months specifically. If there are no public enforcement "
            "cases, that itself is interesting — say so."
        ),
    },
    {
        "id": "tf-industry-mna",
        "title": "Toll-free / RespOrg industry M&A activity",
        "prompt": (
            "What significant **mergers, acquisitions, or investments** have "
            "happened in the US toll-free / RespOrg / vanity-number-broker space "
            "in the past 24 months? Include both major carrier M&A (Bandwidth, "
            "Inteliquent, RingCentral, etc.) and smaller specialist deals. "
            "Include any reported sales or transfers of significant toll-free "
            "vanity number portfolios."
        ),
    },
]


def main():
    out_dir = ROOT / "data"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "sonar_industry_deepdive_2026-04-28.json"

    results = {"generated_at": datetime.utcnow().isoformat() + "Z",
               "model": MODEL, "queries": []}

    print(f"Running industry deep-dive — {len(QUERIES)} queries via {MODEL}\n")
    total_cost = 0.0
    for q in QUERIES:
        print(f"=== {q['id']}  {q['title']}")
        try:
            data = sonar(q["prompt"])
            choice = (data.get("choices") or [{}])[0]
            msg    = choice.get("message") or {}
            content = msg.get("content","")
            usage   = data.get("usage", {})
            cost    = usage.get("cost", 0)
            total_cost += cost
            print(f"  tokens in/out: {usage.get('prompt_tokens')}/{usage.get('completion_tokens')}  cost=${cost:.4f}")
            print(f"  content (head):")
            print(f"    {content[:300]!r}")
            print()
            results["queries"].append({**q, "content": content, "usage": usage,
                                       "annotations": msg.get("annotations") or [],
                                       "raw": data})
        except Exception as e:
            print(f"  ERROR: {e}\n")
            results["queries"].append({**q, "error": str(e)})

    out_path.write_text(json.dumps(results, indent=2, ensure_ascii=False),
                        encoding="utf-8")
    print(f"\nTotal cost: ${total_cost:.4f}")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
