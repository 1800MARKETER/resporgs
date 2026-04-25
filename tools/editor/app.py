"""
Local resporg editor. Runs on http://localhost:5179 — never deployed.

Reads the most recent Sanity export from clean/resporg.json + related
docs. Writes directly to Sanity via the Mutations API (no override
files — Sanity is the source of truth).

Focus v1: category assignment, hidden flag, and local-only research notes.
Add other fields (title, alias, groups) later as needs arise.

Environment: reads SANITY_API_TOKEN from <repo>/apikey.env.
"""

from __future__ import annotations
import json
import os
import sys
from pathlib import Path

import urllib.request
import urllib.error

from flask import Flask, render_template, request, redirect, url_for, jsonify

ROOT = Path(__file__).resolve().parent.parent.parent  # repo root
CLEAN = ROOT / "clean"
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

SANITY_PROJECT_ID = "52jbeh8g"
SANITY_DATASET = "blog"
SANITY_API_VERSION = "v2021-10-21"
NOTES_FILE = DATA / "editor_notes.json"

app = Flask(__name__)


# ---------- env / secrets ----------

def _load_env():
    env_file = ROOT / "apikey.env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip().upper()  # normalize: Sanity_API_Key -> SANITY_API_KEY
            v = v.strip().strip('"').strip("'")
            if k and v and k not in os.environ:
                os.environ[k] = v


_load_env()
# Accept either SANITY_API_TOKEN (Sanity's own naming) or SANITY_API_KEY
# (matches GOOGLE_MAPS_API_KEY convention already in apikey.env).
SANITY_TOKEN = (
    os.environ.get("SANITY_API_TOKEN")
    or os.environ.get("SANITY_API_KEY")
    or ""
)
if not SANITY_TOKEN:
    print("WARNING: SANITY_API_TOKEN not set — saves will fail. "
          "Add it to apikey.env.", file=sys.stderr)


# ---------- Sanity data (read) ----------

def _load(name: str):
    return json.loads((CLEAN / f"{name}.json").read_text(encoding="utf-8"))


def load_state():
    """Reload the Sanity export from disk. Called on startup + on demand."""
    global RESPORG_DOCS, CATEGORY_DOCS, GROUP_DOCS
    RESPORG_DOCS = _load("resporg")
    CATEGORY_DOCS = _load("resporgCategory")
    GROUP_DOCS = _load("resporgGroup")


RESPORG_DOCS: list[dict] = []
CATEGORY_DOCS: list[dict] = []
GROUP_DOCS: list[dict] = []
load_state()


def all_categories():
    """(slug, title, id) list, alphabetized."""
    out = []
    for c in CATEGORY_DOCS:
        slug = (c.get("slug") or {}).get("current")
        if not slug:
            continue
        out.append(
            {
                "slug": slug,
                "title": c.get("title", slug),
                "id": c["_id"].removeprefix("drafts."),
            }
        )
    out.sort(key=lambda x: x["title"].lower())
    return out


def get_resporg(doc_id: str):
    """Find a resporg doc by its _id (with or without drafts. prefix)."""
    doc_id = doc_id.removeprefix("drafts.")
    for d in RESPORG_DOCS:
        if d["_id"].removeprefix("drafts.") == doc_id:
            return d
    return None


def _hidden_category_ids():
    """Sanity ids of the 'hidden' / 'non-resporg' categories if they exist."""
    ids = set()
    for c in CATEGORY_DOCS:
        slug = (c.get("slug") or {}).get("current")
        if slug in {"hidden", "non-resporg"}:
            ids.add(c["_id"].removeprefix("drafts."))
    return ids


# ---------- Local-only notes store ----------

def load_notes() -> dict:
    if NOTES_FILE.exists():
        try:
            return json.loads(NOTES_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_notes(notes: dict):
    NOTES_FILE.write_text(json.dumps(notes, indent=2, ensure_ascii=False), encoding="utf-8")


# ---------- Sanity Mutations ----------

def sanity_patch(doc_id: str, patch: dict) -> tuple[bool, str]:
    """PATCH a single doc. Returns (ok, detail)."""
    if not SANITY_TOKEN:
        return False, "SANITY_API_TOKEN not set"
    # Sanity mutation docs: https://www.sanity.io/docs/http-mutations
    url = (
        f"https://{SANITY_PROJECT_ID}.api.sanity.io"
        f"/{SANITY_API_VERSION}/data/mutate/{SANITY_DATASET}"
    )
    body = {
        "mutations": [
            {
                "patch": {
                    "id": doc_id.removeprefix("drafts."),
                    **patch,
                }
            }
        ]
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {SANITY_TOKEN}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            resp = json.loads(r.read().decode("utf-8"))
        return True, json.dumps(resp)[:300]
    except urllib.error.HTTPError as e:
        body_text = ""
        try:
            body_text = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            pass
        return False, f"HTTP {e.code}: {body_text}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


# ---------- Routes ----------

@app.route("/")
def index():
    """List resporgs with filter + search."""
    q_category = (request.args.get("category") or "").strip()
    q_hidden = request.args.get("hidden") or ""  # "yes" | "no" | ""
    q_search = (request.args.get("q") or "").strip().lower()

    hidden_ids = _hidden_category_ids()
    cats = all_categories()
    cat_id_to_slug = {c["id"]: c["slug"] for c in cats}
    cat_id_to_title = {c["id"]: c["title"] for c in cats}

    # One row per rpfx (not per Sanity doc — though they're mostly 1:1).
    # Collapse multiple Sanity entries for the same 2-char prefix into one.
    seen_pfx: set[str] = set()
    rows = []
    for d in RESPORG_DOCS:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) < 2:
            continue
        pfx = code[:2]
        if pfx in seen_pfx:
            continue
        seen_pfx.add(pfx)
        doc_cats = [
            (cat_id_to_slug.get(cref.get("_ref"), ""),
             cat_id_to_title.get(cref.get("_ref"), ""))
            for cref in (d.get("categories") or [])
            if cref.get("_ref") in cat_id_to_slug
        ]
        has_hidden_tag = any(
            cref.get("_ref") in hidden_ids for cref in (d.get("categories") or [])
        )
        is_prefix_hidden = pfx.startswith("1")
        row = {
            "doc_id": d["_id"].removeprefix("drafts."),
            "rpfx": pfx,
            "code": code,
            "title": d.get("title") or "?",
            "alias": d.get("alias") or "",
            "cats": doc_cats,  # list of (slug, title)
            "cat_slugs": {c[0] for c in doc_cats},
            "hidden_tag": has_hidden_tag,
            "hidden_prefix": is_prefix_hidden,
        }
        rows.append(row)

    # Apply filters
    def matches(r):
        if q_category and q_category not in r["cat_slugs"]:
            return False
        if q_hidden == "yes" and not (r["hidden_tag"] or r["hidden_prefix"]):
            return False
        if q_hidden == "no" and (r["hidden_tag"] or r["hidden_prefix"]):
            return False
        if q_search:
            blob = f"{r['title']} {r['alias']} {r['code']}".lower()
            if q_search not in blob:
                return False
        return True

    rows = [r for r in rows if matches(r)]
    rows.sort(key=lambda r: r["title"].lower())

    return render_template(
        "index.html",
        rows=rows,
        categories=cats,
        q_category=q_category,
        q_hidden=q_hidden,
        q_search=q_search,
        token_ok=bool(SANITY_TOKEN),
    )


@app.route("/edit/<doc_id>", methods=["GET", "POST"])
def edit(doc_id: str):
    doc = get_resporg(doc_id)
    if not doc:
        return f"No resporg with id {doc_id}", 404

    cats = all_categories()
    hidden_ids = _hidden_category_ids()
    notes = load_notes()
    rpfx = (doc.get("codeTwoDigit") or "")[:2].upper()

    if request.method == "POST":
        # Categories — multi-select
        selected_ids = set(request.form.getlist("category_ids"))
        toggle_hidden = request.form.get("hidden") == "on"

        # If hidden toggle is on, ensure a 'hidden' category ref is in the set;
        # if off, strip any hidden-style ids. We only control "hidden" slug — not
        # "non-resporg" which is structural.
        hidden_slug_id = next(
            (c["id"] for c in cats if c["slug"] == "hidden"), None
        )
        if toggle_hidden and hidden_slug_id:
            selected_ids.add(hidden_slug_id)
        elif not toggle_hidden and hidden_slug_id:
            selected_ids.discard(hidden_slug_id)

        # Auto-strip "unknown" when a real category is selected. Unknown
        # is a "we haven't classified this yet" bucket — if we've classified,
        # it shouldn't still be there. The meta slugs (hidden / non-resporg /
        # dead) don't count as real classifications — they describe state,
        # not industry, so they can coexist with any real category.
        META_SLUGS = {"unknown", "hidden", "non-resporg", "dead"}
        real_selected = any(
            c["id"] in selected_ids and c["slug"] not in META_SLUGS
            for c in cats
        )
        if real_selected:
            unknown_id = next(
                (c["id"] for c in cats if c["slug"] == "unknown"), None
            )
            if unknown_id:
                selected_ids.discard(unknown_id)

        # Preserve existing _key entries when possible; Sanity requires
        # unique _key for each array item.
        import secrets
        new_cats = []
        for cid in selected_ids:
            existing_key = None
            for cref in doc.get("categories") or []:
                if cref.get("_ref") == cid and cref.get("_key"):
                    existing_key = cref["_key"]
                    break
            new_cats.append(
                {
                    "_type": "reference",
                    "_ref": cid,
                    "_key": existing_key or secrets.token_hex(6),
                }
            )

        ok, detail = sanity_patch(
            doc["_id"], {"set": {"categories": new_cats}}
        )

        # Local-only notes
        note_text = (request.form.get("notes") or "").strip()
        if note_text:
            notes[rpfx] = note_text
        else:
            notes.pop(rpfx, None)
        save_notes(notes)

        # Update local doc in-memory so UI reflects the change immediately
        doc["categories"] = new_cats

        return render_template(
            "edit.html",
            doc=doc,
            rpfx=rpfx,
            categories=cats,
            current_cat_ids={c["_ref"] for c in new_cats},
            hidden_checked=(bool(hidden_slug_id) and hidden_slug_id in selected_ids),
            hidden_prefix=rpfx.startswith("1"),
            note=notes.get(rpfx, ""),
            saved_ok=ok,
            saved_detail=detail if not ok else "",
            token_ok=bool(SANITY_TOKEN),
        )

    current_cat_ids = {
        c.get("_ref") for c in doc.get("categories") or [] if c.get("_ref")
    }
    hidden_slug_id = next(
        (c["id"] for c in cats if c["slug"] == "hidden"), None
    )
    hidden_checked = bool(hidden_slug_id) and hidden_slug_id in current_cat_ids

    return render_template(
        "edit.html",
        doc=doc,
        rpfx=rpfx,
        categories=cats,
        current_cat_ids=current_cat_ids,
        hidden_checked=hidden_checked,
        hidden_prefix=rpfx.startswith("1"),
        note=notes.get(rpfx, ""),
        saved_ok=None,
        saved_detail="",
        token_ok=bool(SANITY_TOKEN),
    )


@app.route("/refresh")
def refresh():
    """Re-read clean/ JSONs from disk — useful after a fresh sanity dataset export."""
    load_state()
    return redirect(url_for("index"))


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5179
    print(f"Local resporg editor on http://localhost:{port}")
    print(f"  Sanity token: {'OK' if SANITY_TOKEN else 'MISSING — saves will fail'}")
    app.run(host="127.0.0.1", port=port, debug=True)
