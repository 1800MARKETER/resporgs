"""
Look up WHOIS creation date + registrar for each resporg's domain.

Output:  data/domain_age.parquet
Cache:   data/domain_whois_cache.json  (per-domain, persists between runs)

One-shot script. First run is slow (~500 domains × 1s rate-limit); subsequent
runs skip domains already cached within the TTL window.

Flow:
  clean/resporg.json  →  extract domains  →  dedupe
                      →  for each unmissed / stale domain:
                           python-whois → Archive.org CDX fallback
                      →  merge into cache (JSON, saved after each lookup)
                      →  fan back out to per-rpfx rows → parquet

Usage:
  python scripts/enrich_whois.py              # normal run (default TTL 90 days)
  python scripts/enrich_whois.py --ttl 999    # never re-check cached hits
  python scripts/enrich_whois.py --limit 20   # only look up 20 new domains
"""

from __future__ import annotations
import argparse
import datetime as dt
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

try:
    import whois  # python-whois
except ImportError:
    print("ERROR: pip install python-whois", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parent.parent
CLEAN = ROOT / "clean"
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

CACHE_FILE = DATA / "domain_whois_cache.json"
OUT_FILE = DATA / "domain_age.parquet"

SKIP_DOMAINS = {
    # Self-references or parents we don't want to score against any resporg
    "tollfreenumbers.com",
    "resporgs.com",
    "1cup.com",
    "vanitynumbers.com",
}


def extract_domain(raw: str) -> str | None:
    """Pull a hostname out of a URL or bare-domain string. Lowercases,
    strips 'www.'. Returns None if nothing looks like a domain."""
    if not raw:
        return None
    raw = raw.strip().lower()
    if not raw:
        return None
    # urlparse needs a scheme to populate netloc; add one if missing
    if "://" not in raw:
        raw = "http://" + raw
    try:
        host = urllib.parse.urlparse(raw).hostname or ""
    except ValueError:
        return None
    host = host.strip().lstrip(".")
    if host.startswith("www."):
        host = host[4:]
    # Must look like a domain: letters + a dot + TLD
    if "." not in host or " " in host or len(host) < 4:
        return None
    return host


def _first_dt(x):
    """WHOIS creation_date can be datetime, list of datetimes, str, or None."""
    if x is None:
        return None
    if isinstance(x, list):
        x = next((v for v in x if v is not None), None)
    if hasattr(x, "year"):
        return x
    if isinstance(x, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return dt.datetime.strptime(x[: len(fmt)], fmt)
            except ValueError:
                continue
    return None


def whois_lookup(domain: str) -> dict:
    """Return {'created': 'YYYY-MM-DD' | None, 'registrar': str|None, 'source': 'whois'|'none', 'error': str|None}."""
    try:
        r = whois.whois(domain)
    except Exception as e:
        return {"created": None, "registrar": None, "source": "none", "error": str(e)[:160]}
    created = _first_dt(getattr(r, "creation_date", None))
    registrar = getattr(r, "registrar", None)
    if isinstance(registrar, list):
        registrar = registrar[0] if registrar else None
    if created:
        return {
            "created": created.strftime("%Y-%m-%d"),
            "registrar": str(registrar)[:100] if registrar else None,
            "source": "whois",
            "error": None,
        }
    return {"created": None, "registrar": str(registrar)[:100] if registrar else None, "source": "none", "error": "no creation_date"}


def archive_org_fallback(domain: str) -> str | None:
    """First-seen timestamp via Archive.org CDX API. Returns 'YYYY-MM-DD' or None."""
    url = (
        f"https://web.archive.org/cdx/search/cdx?url={urllib.parse.quote(domain)}"
        f"&limit=1&output=json&fl=timestamp"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "resporgs-enrich/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8"))
        # Format: [["timestamp"], ["20050317150000"]]
        if len(data) >= 2 and data[1]:
            ts = data[1][0]
            return f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]}"
    except Exception:
        return None
    return None


def load_cache() -> dict:
    if not CACHE_FILE.exists():
        return {}
    try:
        return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache: dict):
    CACHE_FILE.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")


def is_fresh(entry: dict, ttl_days: int) -> bool:
    try:
        at = dt.datetime.fromisoformat(entry.get("looked_up_at", ""))
    except Exception:
        return False
    return (dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - at).days < ttl_days


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ttl", type=int, default=90, help="cache TTL in days (default 90)")
    ap.add_argument("--limit", type=int, default=0, help="cap new lookups this run (0=no cap)")
    ap.add_argument("--sleep", type=float, default=1.0, help="seconds between WHOIS calls")
    args = ap.parse_args()

    docs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    cache = load_cache()

    # Build rpfx -> domain map, and unique-domain set
    rpfx_domain: list[tuple[str, str, str]] = []  # (rpfx, doc_id, domain)
    unique_domains: set[str] = set()
    for d in docs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) < 2:
            continue
        rpfx = code[:2]
        doc_id = d["_id"].removeprefix("drafts.")
        candidates = [d.get("website"), d.get("alias")]
        dom = None
        for raw in candidates:
            dom = extract_domain(raw or "")
            if dom:
                break
        if not dom or dom in SKIP_DOMAINS:
            continue
        rpfx_domain.append((rpfx, doc_id, dom))
        unique_domains.add(dom)

    print(f"{len(rpfx_domain)} resporgs with a domain; {len(unique_domains)} unique domains")

    # Determine which domains need a lookup
    to_lookup = [
        d for d in sorted(unique_domains)
        if d not in cache or not is_fresh(cache[d], args.ttl)
    ]
    if args.limit:
        to_lookup = to_lookup[: args.limit]
    print(f"{len(to_lookup)} domains to look up (cache TTL {args.ttl} days)")

    t0 = time.time()
    for i, dom in enumerate(to_lookup, 1):
        res = whois_lookup(dom)
        if not res["created"]:
            # Try Archive.org
            ao = archive_org_fallback(dom)
            if ao:
                res["created"] = ao
                res["source"] = "archive.org"
                res["error"] = None
        res["looked_up_at"] = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).isoformat()
        cache[dom] = res

        # Save every 10 lookups + last one, to survive crashes
        if i % 10 == 0 or i == len(to_lookup):
            save_cache(cache)

        created = res["created"] or "—"
        reg = (res["registrar"] or "")[:30]
        src = res["source"]
        err = f" ERR:{res['error']}" if res.get("error") and src == "none" else ""
        elapsed = time.time() - t0
        eta = (elapsed / i) * (len(to_lookup) - i) if i else 0
        print(f"  [{i:>3}/{len(to_lookup)}] {dom:<40} {created:<12} {reg:<30} [{src}]{err}  (ETA {eta:.0f}s)")

        if i < len(to_lookup):
            time.sleep(args.sleep)

    # Assemble output: one row per rpfx (joined from cache)
    today = dt.date.today()
    rows = []
    for rpfx, doc_id, dom in rpfx_domain:
        entry = cache.get(dom)
        if not entry:
            continue
        created_str = entry.get("created")
        created_date = None
        age_years = None
        if created_str:
            try:
                created_date = dt.date.fromisoformat(created_str)
                age_years = (today - created_date).days / 365.25
            except ValueError:
                pass
        rows.append(
            {
                "rpfx": rpfx,
                "doc_id": doc_id,
                "domain": dom,
                "registrar": entry.get("registrar"),
                "created_date": created_date,
                "age_years": age_years,
                "source": entry.get("source", "none"),
                "looked_up_at": entry.get("looked_up_at"),
            }
        )

    if not rows:
        print("No rows to write.")
        return

    # pyarrow schema — explicit so nulls type correctly
    schema = pa.schema([
        ("rpfx", pa.string()),
        ("doc_id", pa.string()),
        ("domain", pa.string()),
        ("registrar", pa.string()),
        ("created_date", pa.date32()),
        ("age_years", pa.float64()),
        ("source", pa.string()),
        ("looked_up_at", pa.string()),
    ])
    tbl = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(tbl, OUT_FILE, compression="zstd")
    print(f"\nWrote {OUT_FILE.name}: {len(rows):,} rows")
    with_date = sum(1 for r in rows if r["created_date"])
    print(f"  with creation_date: {with_date:,}")
    print(f"  missing creation_date: {len(rows) - with_date:,}")


if __name__ == "__main__":
    main()
