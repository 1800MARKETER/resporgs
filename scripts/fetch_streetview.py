"""
Fetch a Google Maps Street View static image for each resporg's address
and cache it locally. Run once (or occasionally to refresh).

Requires an API key:
    export GOOGLE_MAPS_API_KEY=AIza...
    python scripts/fetch_streetview.py
    # optional: --force to re-fetch images that already exist

We only ever publish "Based in: City, State" — the street number is intentionally
NOT shown anywhere in the rendered site. The Street View image is the visual
cue; humans can read the location from the building's surroundings, and we
aren't typing the address into our HTML.

Output: webapp/static/streetview/<RPFX>.jpg + an index JSON.
"""

from __future__ import annotations
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLEAN = ROOT / "clean"
OUT_DIR = ROOT / "webapp" / "static" / "streetview"
OUT_DIR.mkdir(parents=True, exist_ok=True)
INDEX_FILE = OUT_DIR / "_index.json"


def load_env_files():
    """Load key=value pairs from .env / apikey.env into os.environ (if absent)."""
    for name in ("apikey.env", ".env", ".env.local"):
        f = ROOT / name
        if not f.exists():
            continue
        for line in f.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k and v and k not in os.environ:
                os.environ[k] = v

SIZE = "640x320"
FOV = 80          # field of view
PITCH = 0         # looking forward
HEADING = 235     # default heading; "outdoor" will often auto-correct


def build_query(address: dict) -> str | None:
    """Return a single-line query for the Street View API, or None if unusable."""
    street = (address.get("street1") or "").strip()
    city = (address.get("city") or "").strip()
    state = (address.get("state") or "").strip()
    postal = (address.get("postalCode") or "").strip()
    country = (address.get("country") or "").strip()
    if not (city and state):
        return None
    # Prefer full address; fall back to city+state+country if we have no street.
    parts = [p for p in (street, city, state, postal, country) if p]
    return ", ".join(parts)


def metadata_ok(query: str, api_key: str) -> bool:
    """Use the Street View metadata endpoint to check imagery exists (free)."""
    params = urllib.parse.urlencode(
        {"location": query, "source": "outdoor", "key": api_key}
    )
    url = f"https://maps.googleapis.com/maps/api/streetview/metadata?{params}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            body = json.loads(r.read().decode("utf-8"))
        return body.get("status") == "OK"
    except Exception as e:
        print(f"    metadata check failed: {e}")
        return False


def fetch_image(query: str, out_path: Path, api_key: str) -> bool:
    params = urllib.parse.urlencode(
        {
            "size": SIZE,
            "location": query,
            "source": "outdoor",
            "fov": FOV,
            "pitch": PITCH,
            "return_error_code": "true",
            "key": api_key,
        }
    )
    url = f"https://maps.googleapis.com/maps/api/streetview?{params}"
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            data = r.read()
        if len(data) < 4000:  # Google's "no imagery" placeholder is small
            return False
        out_path.write_bytes(data)
        return True
    except Exception as e:
        print(f"    fetch failed: {e}")
        return False


def fetch_satellite(query: str, out_path: Path, api_key: str, zoom: int = 19) -> bool:
    """Fallback: Maps Static satellite view when no Street View exists.

    Takes an aerial photo of the address at near-building resolution.
    Retries once on 403 after a short backoff — some requests intermittently
    hit Google's internal authorization path and need a second attempt.
    """
    params = urllib.parse.urlencode(
        {
            "center": query,
            "zoom": zoom,
            "size": SIZE,
            "maptype": "satellite",
            "key": api_key,
        }
    )
    url = f"https://maps.googleapis.com/maps/api/staticmap?{params}"
    for attempt in (1, 2, 3):
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                data = r.read()
            if len(data) < 4000:
                return False
            out_path.write_bytes(data)
            return True
        except urllib.error.HTTPError as e:
            if e.code == 403 and attempt < 3:
                time.sleep(1.0 * attempt)
                continue
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")[:200]
            except Exception:
                pass
            print(f"    satellite {e.code}: {body[:120]}")
            return False
        except Exception as e:
            if attempt < 3:
                time.sleep(0.5)
                continue
            print(f"    satellite failed: {e}")
            return False
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Re-fetch images that already exist")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N fetches (for testing)")
    parser.add_argument("--dry-run", action="store_true", help="List addresses that would be fetched")
    args = parser.parse_args()

    load_env_files()
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not api_key and not args.dry_run:
        print("ERROR: set GOOGLE_MAPS_API_KEY env var, or put it in apikey.env.")
        sys.exit(1)

    # Build rpfx -> best address (prefer the doc with the richest address)
    docs = json.loads((CLEAN / "resporg.json").read_text(encoding="utf-8"))
    by_pfx: dict[str, dict] = {}
    for d in docs:
        code = (d.get("codeTwoDigit") or "").strip().upper()
        if len(code) < 2:
            continue
        pfx = code[:2]
        addr = d.get("address") or {}
        if not addr.get("city"):
            continue
        existing = by_pfx.get(pfx)
        if not existing:
            by_pfx[pfx] = addr
            continue
        # prefer entry with a street1
        if addr.get("street1") and not existing.get("street1"):
            by_pfx[pfx] = addr

    index = {}
    if INDEX_FILE.exists():
        try:
            index = json.loads(INDEX_FILE.read_text(encoding="utf-8"))
        except Exception:
            index = {}

    street_saved = satellite_saved = both_missing = 0
    for rpfx, addr in sorted(by_pfx.items()):
        street_path = OUT_DIR / f"{rpfx}-street.jpg"
        satellite_path = OUT_DIR / f"{rpfx}-satellite.jpg"
        query = build_query(addr)
        if not query:
            continue

        # Skip entirely if both already exist (unless --force)
        if street_path.exists() and satellite_path.exists() and not args.force:
            continue

        record = index.get(rpfx, {
            "rpfx": rpfx,
            "city": addr.get("city"),
            "state": addr.get("state"),
        })

        if args.dry_run:
            need = []
            if not street_path.exists():
                need.append("street")
            if not satellite_path.exists():
                need.append("satellite")
            print(f"  {rpfx}  need={','.join(need)}  {query}")
            continue

        # Try street view (only if we don't already have it)
        if not street_path.exists() or args.force:
            if metadata_ok(query, api_key) and fetch_image(query, street_path, api_key):
                record["street"] = True
                street_saved += 1
                print(f"  {rpfx}  saved {street_path.name} ({street_path.stat().st_size // 1024} KB)")
            else:
                record["street"] = False
            time.sleep(0.15)

        # Try satellite (independent of whether street worked)
        if not satellite_path.exists() or args.force:
            if fetch_satellite(query, satellite_path, api_key):
                record["satellite"] = True
                satellite_saved += 1
                print(f"  {rpfx}  saved {satellite_path.name} ({satellite_path.stat().st_size // 1024} KB)")
            else:
                record["satellite"] = False
                print(f"  {rpfx}  satellite skipped")
            time.sleep(0.15)

        index[rpfx] = record
        if not record.get("street") and not record.get("satellite"):
            both_missing += 1

        if args.limit and (street_saved + satellite_saved) >= args.limit * 2:
            break

    fetched = street_saved + satellite_saved
    skipped = 0
    failed = both_missing

    INDEX_FILE.write_text(json.dumps(index, indent=2), encoding="utf-8")
    print(
        f"\nDone: street saved={street_saved}, satellite saved={satellite_saved}, "
        f"both missing={both_missing}"
    )


if __name__ == "__main__":
    main()
