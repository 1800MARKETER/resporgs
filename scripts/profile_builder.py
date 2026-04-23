"""
Render a resporg profile page as Markdown (v0 — no flow graph yet; that layer
will be added when data/flow_graph.parquet exists).

Usage:
    python scripts/profile_builder.py MY       # one profile
    python scripts/profile_builder.py TW JW EF # several
"""

from __future__ import annotations
import json
import sys
from collections import defaultdict
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
CACHE = ROOT / "cache"
CLEAN = ROOT / "clean"
OUT_DIR = ROOT / "profiles"
OUT_DIR.mkdir(exist_ok=True)

FLOW_GRAPH = DATA / "flow_graph.parquet"
RESPORG_MONTH = DATA / "resporg_month.parquet"
ASSET_ROOT = ROOT / "sanity-export" / "blog-export-2026-04-21t16-03-52-563z"


def asset_path(image_field) -> str | None:
    """Extract local relative asset path from a Sanity image field."""
    if not image_field:
        return None
    ref = image_field.get("_sanityAsset") or ""
    # "image@file://./images/<hash>-<w>x<h>.<ext>"
    if "file://" in ref:
        rel = ref.split("file://", 1)[1].lstrip("./")
        return str(ASSET_ROOT / rel)
    return None


# ---------- Sanity loaders ----------
def _sanity_docs(name: str):
    return json.loads((CLEAN / f"{name}.json").read_text(encoding="utf-8"))


def resporg_by_prefix() -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for d in _sanity_docs("resporg"):
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) >= 2:
            out[code[:2]].append(d)
    return out


def category_lookup() -> tuple[dict[str, str], dict[str, str]]:
    """Return (id->slug, id->title) for categories."""
    id_slug: dict[str, str] = {}
    id_title: dict[str, str] = {}
    for c in _sanity_docs("resporgCategory"):
        cid = c["_id"].removeprefix("drafts.")
        id_slug[cid] = (c.get("slug") or {}).get("current", "unknown")
        id_title[cid] = c.get("title", "?")
    return id_slug, id_title


def group_lookup() -> tuple[dict[str, str], dict[str, str]]:
    """Return (id->slug, id->title) for resporgGroups."""
    id_slug: dict[str, str] = {}
    id_title: dict[str, str] = {}
    for g in _sanity_docs("resporgGroup"):
        gid = g["_id"].removeprefix("drafts.")
        id_slug[gid] = (g.get("slug") or {}).get("current", "?")
        id_title[gid] = g.get("title", "?")
    return id_slug, id_title


def portable_text_to_md(blocks) -> str:
    """Flatten a Sanity portable-text array to plain paragraphs."""
    if not blocks:
        return ""
    out = []
    for blk in blocks:
        if blk.get("_type") != "block":
            continue
        text = "".join(child.get("text", "") for child in blk.get("children", []))
        if text.strip():
            out.append(text)
    return "\n\n".join(out)


def fmt_int(n):
    return f"{n:,}" if n is not None else "—"


def fmt_pct(p):
    return f"{p*100:.2f}%" if p is not None else "—"


# ---------- Core profile renderer ----------
def render_profile(rpfx: str, con: duckdb.DuckDBPyConnection) -> str:
    rpfx = rpfx.upper()
    sanity_all = resporg_by_prefix()
    sanity_docs = sanity_all.get(rpfx, [])

    id_cat_slug, id_cat_title = category_lookup()
    id_grp_slug, id_grp_title = group_lookup()

    # Primary doc — prefer the one with title "TITLE" and codeTwoDigit matching
    primary = sanity_docs[0] if sanity_docs else None
    title = (primary.get("title") if primary else None) or f"Unknown RespOrg ({rpfx})"
    alias = primary.get("alias") if primary else None

    # Categories & groups across all Sanity records for this prefix
    cats: set[str] = set()
    grps: dict[str, str] = {}  # slug -> title
    for d in sanity_docs:
        for cref in d.get("categories", []) or []:
            cid = cref.get("_ref")
            if cid in id_cat_title:
                cats.add(id_cat_title[cid])
        for gref in d.get("groups", []) or []:
            gid = gref.get("_ref")
            if gid in id_grp_title:
                grps[id_grp_slug[gid]] = id_grp_title[gid]

    # --- Inventory snapshot from latest cached month ---
    latest_month = sorted(p.stem for p in CACHE.glob("*.parquet"))[-1]
    inv_by_prefix = {
        row[0]: row[1]
        for row in con.execute(
            f"""
            SELECT prefix, COUNT(*)
            FROM read_parquet('{(CACHE/f"{latest_month}.parquet").as_posix()}')
            WHERE rpfx = '{rpfx}'
            GROUP BY prefix
            """
        ).fetchall()
    }
    inv_by_status = {
        row[0]: row[1]
        for row in con.execute(
            f"""
            SELECT status, COUNT(*)
            FROM read_parquet('{(CACHE/f"{latest_month}.parquet").as_posix()}')
            WHERE rpfx = '{rpfx}'
            GROUP BY status
            """
        ).fetchall()
    }
    total_inv = sum(inv_by_prefix.values())

    # Sub-codes actually used this month
    subcodes = [
        row[0]
        for row in con.execute(
            f"""
            SELECT resporg, COUNT(*) AS n
            FROM read_parquet('{(CACHE/f"{latest_month}.parquet").as_posix()}')
            WHERE rpfx = '{rpfx}'
            GROUP BY resporg
            ORDER BY n DESC
            """
        ).fetchall()
    ]

    # --- Trajectory ---
    traj = con.execute(
        f"""
        SELECT month, inventory, acquired, lost,
               harvested_cross_rpfx, appeared_from_spare, disappeared_to_spare
        FROM read_parquet('{RESPORG_MONTH.as_posix()}')
        WHERE rpfx = '{rpfx}'
        ORDER BY month
        """
    ).fetchall()

    # Opportunism Index over full 42-mo window
    if traj:
        total_acq = sum(r[2] for r in traj)
        total_harv = sum(r[4] for r in traj)
        opp = (total_harv / total_acq) if total_acq else 0
        total_lost = sum(r[3] for r in traj)
        total_reac = 0  # placeholder if we add reactivations column
        first_m, first_inv = traj[0][0], traj[0][1]
        last_m, last_inv = traj[-1][0], traj[-1][1]
        delta = last_inv - first_inv
        pct = (delta / first_inv * 100) if first_inv else 0
    else:
        total_acq = total_harv = total_lost = 0
        opp = 0
        first_m = last_m = latest_month
        first_inv = last_inv = total_inv
        delta = pct = 0

    # Industry rank by inventory this month
    rank = con.execute(
        f"""
        WITH inv AS (
          SELECT rpfx, COUNT(*) AS n
          FROM read_parquet('{(CACHE/f"{latest_month}.parquet").as_posix()}')
          GROUP BY rpfx
        ),
        ranked AS (
          SELECT rpfx, n, RANK() OVER (ORDER BY n DESC) AS rk
          FROM inv
        )
        SELECT rk FROM ranked WHERE rpfx = '{rpfx}'
        """
    ).fetchone()
    rank = rank[0] if rank else None
    industry_count = con.execute(
        f"""
        SELECT COUNT(DISTINCT rpfx)
        FROM read_parquet('{(CACHE/f"{latest_month}.parquet").as_posix()}')
        """
    ).fetchone()[0]

    # ----- Flow summary (inbound / outbound) — only if flow graph exists -----
    flow_section_md = ""
    if FLOW_GRAPH.exists():
        # All-time totals by edge type
        inbound = con.execute(
            f"""
            SELECT edge_type, SUM(n) AS n
            FROM read_parquet('{FLOW_GRAPH.as_posix()}')
            WHERE to_node = '{rpfx}'
            GROUP BY edge_type
            """
        ).fetchall()
        outbound = con.execute(
            f"""
            SELECT edge_type, SUM(n) AS n
            FROM read_parquet('{FLOW_GRAPH.as_posix()}')
            WHERE from_node = '{rpfx}'
            GROUP BY edge_type
            """
        ).fetchall()
        # Top direct trading partners (TRANSFER only)
        top_sources = con.execute(
            f"""
            SELECT from_node, SUM(n) AS n
            FROM read_parquet('{FLOW_GRAPH.as_posix()}')
            WHERE to_node = '{rpfx}' AND edge_type = 'TRANSFER'
            GROUP BY from_node
            ORDER BY n DESC LIMIT 10
            """
        ).fetchall()
        top_dests = con.execute(
            f"""
            SELECT to_node, SUM(n) AS n
            FROM read_parquet('{FLOW_GRAPH.as_posix()}')
            WHERE from_node = '{rpfx}' AND edge_type = 'TRANSFER'
            GROUP BY to_node
            ORDER BY n DESC LIMIT 10
            """
        ).fetchall()
        # Top HARVEST sources (original prev_rpfx)
        top_harvest_from = con.execute(
            f"""
            SELECT prev_rpfx, SUM(n) AS n
            FROM read_parquet('{FLOW_GRAPH.as_posix()}')
            WHERE to_node = '{rpfx}' AND edge_type = 'HARVEST' AND prev_rpfx IS NOT NULL
            GROUP BY prev_rpfx
            ORDER BY n DESC LIMIT 10
            """
        ).fetchall()
        flow_section_md = render_flow_section(
            inbound, outbound, top_sources, top_dests, top_harvest_from, sanity_all
        )

    # --------- Build Markdown ---------
    lines: list[str] = []
    lines.append(f"# {title}")
    subtitle_bits = [f"**{rpfx}**"]
    if alias:
        subtitle_bits.append(f"alias _{alias}_")
    if primary and primary.get("codeTwoDigit"):
        subtitle_bits.append(f"primary code `{primary['codeTwoDigit']}`")
    lines.append("  ·  ".join(subtitle_bits))
    if cats:
        lines.append(f"**Category**: {', '.join(sorted(cats))}")
    if grps:
        lines.append(f"**Group**: {', '.join(sorted(grps.values()))}")
    lines.append("")

    # Logo
    if primary:
        logo_path = asset_path(primary.get("logoImage"))
        if logo_path:
            lines.append(f"![logo]({logo_path})")
            lines.append("")

    # Contact block
    if primary:
        addr = primary.get("address") or {}
        if primary.get("website"):
            lines.append(f"- Website: {primary['website']}")
        if primary.get("troubleNumber"):
            lines.append(f"- Support phone: {primary['troubleNumber']}")
        if primary.get("requestForm"):
            lines.append(f"- Request form: {primary['requestForm']}")
        addr_line = ", ".join(
            x
            for x in [
                addr.get("street1"),
                addr.get("street2"),
                addr.get("city"),
                addr.get("state"),
                addr.get("postalCode"),
                addr.get("country"),
            ]
            if x
        )
        if addr_line:
            lines.append(f"- Address: {addr_line}")
        lines.append("")

    # At-a-glance stats
    lines.append("## At a glance")
    lines.append(
        f"- **Inventory ({latest_month})**: {fmt_int(total_inv)} numbers"
        + (f"  (industry rank #{rank} of {industry_count})" if rank else "")
    )
    lines.append(
        f"- **4-year trend**: {fmt_int(first_inv)} ({first_m}) → "
        f"{fmt_int(last_inv)} ({last_m}), {delta:+,} ({pct:+.1f}%)"
    )
    lines.append(
        f"- **Opportunism Index (42-mo)**: {fmt_pct(opp)}  "
        f"(cumulative acquired {fmt_int(total_acq)}, harvested from disconnect {fmt_int(total_harv)})"
    )
    if subcodes:
        head = ", ".join(subcodes[:8])
        more = f" (+{len(subcodes)-8} more)" if len(subcodes) > 8 else ""
        lines.append(f"- **Sub-codes in use**: {len(subcodes)} — {head}{more}")
    lines.append("")

    # Inventory breakdown
    STATUS_NAMES = {1: "WORKING", 2: "TRANSIT", 3: "DISCONN", 4: "RESERVED", 5: "UNAVAIL", 6: "ASSIGNED", 7: "SUSPEND"}
    lines.append("## Inventory breakdown")
    lines.append("| Prefix | Count | | Status | Count |")
    lines.append("|---|---:|---|---|---:|")
    prefix_rows = sorted(inv_by_prefix.items())
    status_rows = sorted(inv_by_status.items(), key=lambda x: -x[1])
    for i in range(max(len(prefix_rows), len(status_rows))):
        lc = f"{prefix_rows[i][0]} | {prefix_rows[i][1]:,}" if i < len(prefix_rows) else " | "
        rc = (
            f"{STATUS_NAMES.get(status_rows[i][0], '?')} | {status_rows[i][1]:,}"
            if i < len(status_rows)
            else " | "
        )
        lines.append(f"| {lc} | | {rc} |")
    lines.append("")

    # Trajectory
    if traj:
        lines.append("## 42-month trajectory")
        lines.append("| Month | Inventory | Acquired | Harvested | Lost |")
        lines.append("|---|---:|---:|---:|---:|")
        # Compact: show every 3rd month + last
        step = max(1, len(traj) // 14)
        kept_indices = set(range(0, len(traj), step))
        kept_indices.add(len(traj) - 1)
        for idx, row in enumerate(traj):
            if idx not in kept_indices:
                continue
            month, inv, acq, lost, harv, *_ = row
            lines.append(
                f"| {month} | {fmt_int(inv)} | {fmt_int(acq)} | {fmt_int(harv)} | {fmt_int(lost)} |"
            )
        lines.append("")

    # Flow section if available
    if flow_section_md:
        lines.append(flow_section_md)

    # Sanity narrative
    if primary:
        summary = portable_text_to_md(primary.get("summary"))
        message = portable_text_to_md(primary.get("exactMatchMessage"))
        if summary:
            lines.append("## Summary")
            lines.append(summary)
            lines.append("")
        if message:
            lines.append("## Contact-form message")
            lines.append(message)
            lines.append("")
        if primary.get("topNumbers"):
            lines.append("## Notable numbers on file")
            lines.append("```")
            lines.append(primary["topNumbers"])
            lines.append("```")
            lines.append("")

    # Testimonials mentioning this resporg (by title/alias match in body)
    testi_hits = find_testimonials(title, alias, primary)
    if testi_hits:
        lines.append(f"## Testimonials mentioning this resporg ({len(testi_hits)})")
        for t in testi_hits[:5]:
            date = t.get("reviewDate", "?")
            author = t.get("author", "?")
            body = (t.get("body") or "").strip()
            lines.append(f"> {body}")
            lines.append(f">")
            lines.append(f"> — {author}, {date}")
            lines.append("")

    return "\n".join(lines)


_TESTIMONIALS_CACHE: list[dict] | None = None


def find_testimonials(title: str, alias: str | None, doc: dict | None) -> list[dict]:
    """Search testimonial bodies for word-boundary mentions of this resporg's
    name or alias. Strict — avoids false positives from common website roots.

    Testimonials in the Sanity corpus are mostly about TollFreeNumbers.com the
    service, not about individual resporgs, so most profiles will legitimately
    have zero matches.
    """
    import re as _re
    global _TESTIMONIALS_CACHE
    if _TESTIMONIALS_CACHE is None:
        _TESTIMONIALS_CACHE = _sanity_docs("testimonial")
    needles: list[str] = []
    for s in [title, alias]:
        if not s:
            continue
        s = s.strip()
        if len(s) < 5:
            continue
        low = s.lower()
        if low in {"?", "unknown", "secondary", "telecom", "communications",
                   "international", "messaging", "regional phone company"}:
            continue
        needles.append(low)
    if not needles:
        return []
    patterns = [_re.compile(r"\b" + _re.escape(n) + r"\b") for n in needles]
    hits = []
    for t in _TESTIMONIALS_CACHE:
        body = (t.get("body") or "").lower()
        if any(p.search(body) for p in patterns):
            hits.append(t)
    return hits


def render_flow_section(inbound, outbound, top_src, top_dst, top_harvest, sanity_all) -> str:
    def name_for(pfx: str) -> str:
        if pfx in {"DISC", "SPARE"}:
            return pfx
        docs = sanity_all.get(pfx, [])
        if docs:
            return docs[0].get("title") or pfx
        return pfx

    lines = ["## Flow patterns (42-month cumulative)"]
    inb = dict(inbound)
    out = dict(outbound)
    lines.append("### Inbound (numbers acquired)")
    lines.append("| Source type | Count |")
    lines.append("|---|---:|")
    for k in ("TRANSFER", "HARVEST", "FIRST_ASSIGN", "REACTIVATE"):
        lines.append(f"| {k} | {fmt_int(inb.get(k, 0))} |")
    lines.append("")
    lines.append("### Outbound (numbers lost)")
    lines.append("| Destination type | Count |")
    lines.append("|---|---:|")
    for k in ("TRANSFER", "DISCONNECT", "TO_SPARE"):
        lines.append(f"| {k} | {fmt_int(out.get(k, 0))} |")
    lines.append("")

    if top_src:
        lines.append("### Top direct-transfer sources (who handed numbers TO this resporg)")
        lines.append("| From | Count | Name |")
        lines.append("|---|---:|---|")
        for src, n in top_src:
            lines.append(f"| {src} | {fmt_int(n)} | {name_for(src)} |")
        lines.append("")
    if top_dst:
        lines.append("### Top direct-transfer destinations (who received numbers FROM this resporg)")
        lines.append("| To | Count | Name |")
        lines.append("|---|---:|---|")
        for dst, n in top_dst:
            lines.append(f"| {dst} | {fmt_int(n)} | {name_for(dst)} |")
        lines.append("")
    if top_harvest:
        lines.append(
            "### Top harvest-origin prefixes (numbers acquired from DISC pool, by prior owner)"
        )
        lines.append("| Prior owner | Count | Name |")
        lines.append("|---|---:|---|")
        for src, n in top_harvest:
            lines.append(f"| {src} | {fmt_int(n)} | {name_for(src)} |")
        lines.append("")
    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        print("Usage: profile_builder.py <RPFX> [<RPFX2> ...]")
        sys.exit(1)
    con = duckdb.connect()
    for rpfx in sys.argv[1:]:
        md = render_profile(rpfx, con)
        out_file = OUT_DIR / f"{rpfx.upper()}.md"
        out_file.write_text(md, encoding="utf-8")
        print(f"Wrote {out_file}")


if __name__ == "__main__":
    main()
