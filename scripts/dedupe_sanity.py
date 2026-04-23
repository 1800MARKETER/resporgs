"""
Resolve draft/published duplicates in the Sanity export.

Sanity ships both versions: drafts have `_id` prefixed with `drafts.<uuid>`,
published have the bare `<uuid>`. When both exist, prefer published.

Writes cleaned JSON for each document type to RESPORGS/clean/.
"""

import json
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
BY_TYPE = ROOT / "by-type"
CLEAN = ROOT / "clean"
CLEAN.mkdir(exist_ok=True)


def base_id(doc_id: str) -> str:
    return doc_id[len("drafts."):] if doc_id.startswith("drafts.") else doc_id


def is_draft(doc_id: str) -> bool:
    return doc_id.startswith("drafts.")


def dedupe(docs):
    by_base = defaultdict(dict)  # base_id -> {"published": doc, "draft": doc}
    for d in docs:
        key = "draft" if is_draft(d["_id"]) else "published"
        by_base[base_id(d["_id"])][key] = d

    resolved = []
    draft_only = 0
    both = 0
    published_only = 0
    for base, versions in by_base.items():
        if "published" in versions:
            resolved.append(versions["published"])
            if "draft" in versions:
                both += 1
            else:
                published_only += 1
        else:
            resolved.append(versions["draft"])
            draft_only += 1
    return resolved, {
        "total_unique": len(resolved),
        "published_only": published_only,
        "draft_only": draft_only,
        "both_kept_published": both,
    }


def main():
    summary = {}
    for src in sorted(BY_TYPE.glob("*.json")):
        docs = json.loads(src.read_text(encoding="utf-8"))
        cleaned, stats = dedupe(docs)
        dest = CLEAN / src.name
        dest.write_text(
            json.dumps(cleaned, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        summary[src.stem] = {"raw": len(docs), **stats}

    print(f"{'type':<22} {'raw':>6} {'unique':>7} {'pub':>5} {'draft':>6} {'both':>5}")
    print("-" * 56)
    for t, s in sorted(summary.items(), key=lambda x: -x[1]["total_unique"]):
        print(
            f"{t:<22} {s['raw']:>6} {s['total_unique']:>7} "
            f"{s['published_only']:>5} {s['draft_only']:>6} {s['both_kept_published']:>5}"
        )

    (ROOT / "clean" / "_dedupe_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
