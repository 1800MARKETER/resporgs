"""Extract Somos weekly Number Administration PDFs from Bill's mail archive zips.

Each zip from the Roundcube/IMAP export holds .eml files; each weekly Somos
notification has a NUM-YY-WW.pdf attached. This script walks the zips, parses
the MIME parts, and drops each PDF into RESPORGS/somos_pdfs/NUM-YY-WW.pdf.

Usage:
    python scripts/extract_somos_pdfs.py
    python scripts/extract_somos_pdfs.py --src "C:/Users/Bill/Downloads"

Idempotent — skips PDFs already on disk.
"""
from __future__ import annotations
import argparse
import email
import zipfile
from email import policy
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "somos_pdfs"
DEFAULT_SRC = Path("C:/Users/Bill/Downloads")
ZIP_GLOB = "bill@tollfreenumbers.com*.zip"


def extract_one_zip(zpath: Path, out_dir: Path) -> tuple[int, int]:
    """Return (extracted, skipped)."""
    extracted = skipped = 0
    with zipfile.ZipFile(zpath) as z:
        for name in z.namelist():
            if not name.lower().endswith(".eml"):
                continue
            with z.open(name) as f:
                msg = email.message_from_binary_file(f, policy=policy.default)
            for part in msg.walk():
                fn = part.get_filename()
                if not fn or not fn.lower().endswith(".pdf"):
                    continue
                # Only keep NUM-YY-WW.pdf — the weekly Number Administration ones.
                if not fn.upper().startswith("NUM-"):
                    continue
                out = out_dir / fn
                if out.exists():
                    skipped += 1
                    continue
                payload = part.get_payload(decode=True)
                if not payload:
                    continue
                out.write_bytes(payload)
                extracted += 1
    return extracted, skipped


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--src", default=str(DEFAULT_SRC),
                    help="Folder containing the bill@tollfreenumbers.com*.zip files")
    args = ap.parse_args()

    src = Path(args.src)
    if not src.exists():
        print(f"ERROR: source folder not found: {src}", file=sys.stderr)
        return 2

    OUT_DIR.mkdir(exist_ok=True)
    zips = sorted(src.glob(ZIP_GLOB))
    if not zips:
        print(f"No zips matched {src / ZIP_GLOB}")
        return 1

    total_e = total_s = 0
    for z in zips:
        e, s = extract_one_zip(z, OUT_DIR)
        total_e += e
        total_s += s
        print(f"  {z.name}: extracted {e}, skipped {s}")

    pdfs = sorted(OUT_DIR.glob("NUM-*.pdf"))
    print(f"\n  total extracted this run: {total_e}")
    print(f"  total skipped (already present): {total_s}")
    print(f"  total PDFs in {OUT_DIR.relative_to(ROOT)}: {len(pdfs)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
