"""Convert the strong industry-deep-dive query results into:
  (a) markdown files under data/article_drafts/  — for human review
  (b) a single push_post_drafts.json ready for Sanity import via mutate API

Each becomes a draft post so Bill can edit + publish from Sanity Studio.
"""

from __future__ import annotations
import json
import re
import secrets
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Subset of queries from sonar_industry_deepdive_2026-04-28.json that are
# article-quality. Tweak titles/slugs here.
PUBLISH = {
    "misdial-marketing-regulatory-vacuum": {
        "title": "The Toll-Free Misdial-Marketing Industry Operates in a Regulatory Vacuum",
        "slug":  "misdial-marketing-regulatory-vacuum",
        "excerpt": (
            "A 38% slice of America's most coveted toll-free vanity numbers "
            "is held by operators who don't run businesses on those lines — "
            "they monetize misdialed callers. There has been zero federal "
            "enforcement against this practice in the past three years."
        ),
        "source_md": "data/article_drafts/misdial-marketing-regulatory-vacuum.md",
    },
    "8xx-inventory-state-of-the-union": {
        "title": "State of the 8XX Inventory: A 2026 Look at Toll-Free Numbering Supply",
        "slug":  "8xx-inventory-state-of-the-union",
        "excerpt": (
            "Eight Service Access Codes hold roughly 56 million numbers "
            "in monthly circulation, but the vanity inventory has been "
            "exhausting since the 1990s. Here's the per-prefix picture."
        ),
        "source_md": "data/article_drafts/8xx-inventory-state-of-the-union.md",
    },
    "fcc-dno-mandate-dec-2025": {
        "title": "FCC Now Requires All Voice Providers to Block Calls From Invalid Numbers",
        "slug":  "fcc-dno-mandate-2025",
        "excerpt": (
            "As of December 15, 2025, every voice service provider in the call "
            "path — not just gateway carriers — must block calls from numbers "
            "on a 'do-not-originate' list. Penalties run up to $23,000 per "
            "day, and downstream carriers can refuse traffic from non-"
            "compliant providers. Here's what the rule says."
        ),
    },
    "brn-toll-free-jan-2026": {
        "title": "Big Changes for Toll-Free Messaging: The January 1, 2026 BRN Rule Explained",
        "slug":  "brn-toll-free-jan-2026",
        "excerpt": (
            "Starting January 1, 2026, every new toll-free A2P verification "
            "submission must include a Business Registration Number, country "
            "of registration, and legal entity type. Existing verified "
            "numbers are exempt — but new ones without these fields will be "
            "rejected. Here's what businesses need to know."
        ),
    },
    "somos-administrator-renewal": {
        "title": "Who Manages America's Toll-Free Numbers? A Primer on Somos and the FCC's TFNA Designation",
        "slug":  "somos-tfna-primer",
        "excerpt": (
            "Somos, Inc. has run the federally-designated Toll-Free Numbering "
            "database since 2013, managing more than 42 million toll-free "
            "numbers. But how did they get the role, when does their "
            "designation come up for renewal, and why isn't that calendar "
            "more public? Here's what we know — and what we don't."
        ),
    },
}


def short_key() -> str:
    return secrets.token_hex(6)


def md_to_portable_text(md: str) -> list[dict]:
    """Bare-minimum markdown → Sanity portable-text conversion.
    Handles: headings (# ## ###), normal paragraphs, blank-line separation.
    Inline links [text](url) become marks. Bullet lists not yet supported —
    they render as plain paragraphs. Bill can polish in Sanity Studio.
    """
    out = []
    paragraph_buf: list[str] = []

    def flush_paragraph():
        nonlocal paragraph_buf
        if not paragraph_buf:
            return
        text = " ".join(paragraph_buf).strip()
        paragraph_buf = []
        if not text: return
        out.append({
            "_type": "block", "_key": short_key(), "style": "normal",
            "markDefs": [],
            "children": [{"_type":"span","_key":short_key(),"text":text,"marks":[]}],
        })

    for raw in md.splitlines():
        line = raw.rstrip()
        if not line:
            flush_paragraph(); continue
        # Heading?
        m = re.match(r"^(#{1,6})\s+(.+)$", line)
        if m:
            flush_paragraph()
            level = len(m.group(1))
            style = {1:"h2", 2:"h2", 3:"h3", 4:"h4", 5:"h4", 6:"h4"}.get(level, "h2")
            out.append({
                "_type": "block", "_key": short_key(), "style": style,
                "markDefs": [],
                "children": [{"_type":"span","_key":short_key(),"text":m.group(2).strip(),"marks":[]}],
            })
            continue
        # Bullet list item?
        if re.match(r"^\s*[-*]\s+", line):
            flush_paragraph()
            text = re.sub(r"^\s*[-*]\s+", "", line)
            out.append({
                "_type": "block", "_key": short_key(), "style": "normal",
                "listItem": "bullet", "level": 1, "markDefs": [],
                "children": [{"_type":"span","_key":short_key(),"text":text,"marks":[]}],
            })
            continue
        # Numbered list item?
        if re.match(r"^\s*\d+\.\s+", line):
            flush_paragraph()
            text = re.sub(r"^\s*\d+\.\s+", "", line)
            out.append({
                "_type": "block", "_key": short_key(), "style": "normal",
                "listItem": "number", "level": 1, "markDefs": [],
                "children": [{"_type":"span","_key":short_key(),"text":text,"marks":[]}],
            })
            continue
        # Otherwise accumulate into paragraph buffer
        paragraph_buf.append(line)
    flush_paragraph()
    return out


def main():
    src = ROOT / "data" / "sonar_industry_deepdive_2026-04-28.json"
    if not src.exists():
        raise SystemExit(f"missing {src}")
    payload = json.loads(src.read_text(encoding="utf-8"))
    queries = {q["id"]: q for q in payload.get("queries", [])}

    out_md_dir = ROOT / "data" / "article_drafts"
    out_md_dir.mkdir(exist_ok=True, parents=True)

    drafts = []
    for qid, meta in PUBLISH.items():
        # Two sources of article content:
        #   1) source_md present -> read full markdown file authored elsewhere
        #   2) otherwise           -> Sonar Pro deep-dive content from industry json
        if meta.get("source_md"):
            md_path = ROOT / meta["source_md"]
            if not md_path.exists():
                print(f"  SKIP {qid} (markdown file missing: {md_path})"); continue
            md = md_path.read_text(encoding="utf-8")
            print(f"  using existing markdown: {md_path}")
        else:
            q = queries.get(qid)
            if not q or not q.get("content"):
                print(f"  SKIP {qid} (no Sonar content)"); continue
            # Save Sonar-derived markdown file
            md = (
                f"# {meta['title']}\n\n"
                f"_{meta['excerpt']}_\n\n"
                f"---\n\n"
                f"**Source query (for editor):** {qid}\n"
                f"**Generated:** {payload.get('generated_at')}\n"
                f"**Model:** {payload.get('model')}\n"
                f"**⚠ AI-generated draft. Verify all citations and dates against primary "
                f"sources (FCC dockets, somos.com, etc.) before publishing.**\n\n"
                f"---\n\n"
                f"{q['content']}\n"
            )
            md_path = out_md_dir / f"{meta['slug']}.md"
            md_path.write_text(md, encoding="utf-8")
            print(f"  wrote {md_path}")

        # Build portable-text body for Sanity. For source_md flow, the
        # markdown already includes the excerpt as italic lede; for Sonar
        # flow we prepend it explicitly.
        if meta.get("source_md"):
            full_md = md   # already shaped as full article
        else:
            full_md = f"{meta['excerpt']}\n\n{q['content']}"
        body = md_to_portable_text(full_md)

        # Build a draft post doc. Stable ID derived from slug so re-running
        # is idempotent (overwrites the same draft).
        doc_id = f"draft-article-{meta['slug']}"
        drafts.append({
            "_id":   f"drafts.{doc_id}",
            "_type": "post",
            "title": meta["title"],
            "slug":  {"_type":"slug","current": meta["slug"]},
            "noindex": True,           # keep out of search until Bill publishes
            "body":  body,
            # Keeping author/categories empty for Bill to set in Studio.
        })

    out_json = ROOT / "data" / "article_drafts.json"
    out_json.write_text(json.dumps({
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "drafts": drafts,
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  wrote {out_json}  ({len(drafts)} drafts)")


if __name__ == "__main__":
    main()
