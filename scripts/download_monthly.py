"""
Download the Somos monthly Number Status Report (the "CD-ROM" report).

Calls GET /v3/ip/reporting/numstatus/{npaId}, decodes the base64-encoded
zip from the response body, and writes it to D:\\resporgs\\YYYY-MM.zip
where months.py picks it up automatically.

Credentials are read from ../apikey.env:
    SOMOS_ACCESS_KEY=<URC Access Key>
    SOMOS_ACCESS_SECRET=<URC Access Secret>

Usage:
    python download_monthly.py                  # prod, current month, NPA=ALL
    python download_monthly.py --month 2026-05  # explicit month for the output filename
    python download_monthly.py --npa 800        # single prefix instead of ALL
    python download_monthly.py --env sandbox    # sandbox base URL (creds must be sandbox-issued)
    python download_monthly.py --dry-run        # auth only — verify token works without pulling the report
"""

from __future__ import annotations
import argparse
import base64
import datetime as dt
import json
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT / "apikey.env"
ARCHIVE_ROOT = Path(r"D:\resporgs")

ENVIRONMENTS = {
    "production": "https://api-tfnregistry.somos.com",
    "sandbox":    "https://sandbox-api-tfnregistry.somos.com",
}

# Spec ambiguity: basePath is /v3/ip but the path is listed as
# /ip/reporting/numstatus/{npaId} — likely a spec bug, but try both.
ENDPOINT_PATHS = (
    "/v3/ip/reporting/numstatus/{npa}",
    "/v3/ip/ip/reporting/numstatus/{npa}",
)

ZIP_MAGIC = b"PK\x03\x04"


def load_env(path: Path) -> dict[str, str]:
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip()
    return out


def get_access_token(base_url: str, key: str, secret: str) -> str:
    """OAuth 2.0 client_credentials grant. Returns the bearer access_token."""
    creds_b64 = base64.b64encode(f"{key}:{secret}".encode()).decode()
    resp = requests.post(
        f"{base_url}/token",
        headers={
            "Authorization": f"Basic {creds_b64}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    if resp.status_code != 200:
        sys.stderr.write(f"Token endpoint returned {resp.status_code}:\n{resp.text}\n")
        resp.raise_for_status()
    body = resp.json()
    token = body.get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in token response: {body}")
    print(f"[*] Token acquired (expires_in={body.get('expires_in')}s)")
    return token


def download_report(base_url: str, token: str, npa: str) -> tuple[str, bytes]:
    """Hit the numstatus endpoint and return (filename, decoded_bytes).

    Tries both candidate paths to handle the spec ambiguity.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Version": "3.30",
        "Accept": "application/json",
    }
    last_err = None
    for tmpl in ENDPOINT_PATHS:
        url = base_url + tmpl.format(npa=npa)
        print(f"[*] GET {url}")
        resp = requests.get(url, headers=headers, timeout=900)
        if resp.status_code == 404:
            print(f"    -> 404, trying next path")
            last_err = resp
            continue
        if resp.status_code != 200:
            sys.stderr.write(f"Endpoint returned {resp.status_code}:\n{resp.text[:1000]}\n")
            resp.raise_for_status()
        body = resp.json()
        if body.get("errList"):
            print(f"[!] errList present in response: {body['errList']}", file=sys.stderr)
        filename = body.get("fileName") or "unknown.bin"
        raw_b64 = body.get("fileContent") or ""
        if not raw_b64:
            raise RuntimeError(f"Response had no fileContent. Keys: {list(body)}")
        return filename, base64.b64decode(raw_b64)
    raise RuntimeError(f"All endpoint paths returned 404. Last status: {last_err.status_code if last_err else '?'}")


def main() -> int:
    p = argparse.ArgumentParser(description="Download Somos monthly Number Status Report")
    p.add_argument("--env", choices=ENVIRONMENTS, default="production")
    p.add_argument("--month", default=None,
                   help="Month string for output filename, e.g. 2026-05 (default: today's YYYY-MM)")
    p.add_argument("--npa", default="ALL",
                   choices=["800", "833", "844", "855", "866", "877", "888", "ALL"])
    p.add_argument("--dry-run", action="store_true",
                   help="Authenticate only — don't actually pull the report")
    args = p.parse_args()

    env = load_env(ENV_FILE)
    key = env.get("SOMOS_ACCESS_KEY")
    secret = env.get("SOMOS_ACCESS_SECRET")
    if not key or not secret:
        sys.exit(f"ERROR: Missing SOMOS_ACCESS_KEY or SOMOS_ACCESS_SECRET in {ENV_FILE}")

    base_url = ENVIRONMENTS[args.env]
    print(f"[*] Environment: {args.env} -> {base_url}")

    token = get_access_token(base_url, key, secret)
    if args.dry_run:
        print("[*] --dry-run: stopping after auth.")
        return 0

    filename, payload = download_report(base_url, token, args.npa)
    print(f"[*] Received: fileName={filename}, {len(payload):,} bytes")

    is_zip = payload.startswith(ZIP_MAGIC)
    print(f"[*] Magic bytes: {payload[:4]!r} (zip={is_zip})")

    month = args.month or dt.date.today().strftime("%Y-%m")
    if is_zip:
        out_path = ARCHIVE_ROOT / f"{month}.zip"
    else:
        ext = Path(filename).suffix or ".txt"
        out_path = ARCHIVE_ROOT / f"{month}_{args.npa}{ext}"

    ARCHIVE_ROOT.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(payload)
    print(f"[*] Saved: {out_path} ({len(payload):,} bytes)")

    if is_zip:
        print(f"[*] months.py inventory will pick up {month} on next run.")
    else:
        print(f"[!] Response was not a zip — file saved as-is. Inspect before feeding to pipeline.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
