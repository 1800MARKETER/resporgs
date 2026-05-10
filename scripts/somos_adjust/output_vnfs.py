"""Phase 5 step 6: build the VanityNumbersForSales SQLite from the adjusted parquet.

Replaces three hand-run scripts that were used for the May 2026 launch:
    extract_vendor_inventory.py  (filter MikeOINPUT to vendor codes + indiv overrides)
    categorize_inventory.py      (join digit_index for categorisation)
    build_inventory_db.py        (write SQLite)

With the adjusted parquet as input, the Individual-file overrides are already
baked in, so the filter is just `resporg IN <18 vendor codes>`. Plus the alias
for Sanity's 1RLIFF (6 chars) -> 1RLIF (5 chars in real data) is preserved.

Output schema matches what VanityNumbersForSales.com expects (do NOT change
without coordinating with the live app):

    rows(phone, vendor_code, area_code, status, age_months, first_used_date,
         source, term, category_code, category_label, category_code_2,
         category_label_2, relevance)

Run from RESPORGS root:
    python -m scripts.somos_adjust.output_vnfs --month 2026-05
"""
from __future__ import annotations
import argparse
import json
import re
import sqlite3
import time
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_PARQUET_DIR = ROOT / "cache" / "adjusted"
DEFAULT_VNFS_ROOT = ROOT.parent / "VanityNumbersForSales"
DEFAULT_TOLLFREE_DB = ROOT.parent / "local-prospector" / "data" / "tollfree.db"

# The statuses VanityNumbersForSales.com surfaces. Drops DISCONN/UNAVAIL/TRANSIT.
SALABLE_STATUSES = {"WORKING", "FEATURE", "REQUEST", "RESERVED"}


# ---------------------------------------------------------------------------
# Vendor manifest -> search set with 1RLIFF/1RLIF alias
# ---------------------------------------------------------------------------

def build_vendor_lookup(manifest_path: Path):
    """Returns (search_codes_set, alias_to_canonical_dict).
    alias_to_canonical maps the 5-char code we'd see in MikeOINPUT/parquet
    back to the canonical Sanity vendor_code (e.g. 1RLIF -> 1RLIFF)."""
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)
    search_codes: set[str] = set()
    alias_to_canonical: dict[str, str] = {}
    for v in manifest:
        code = v["vendor_code"]
        if len(code) == 5:
            search_codes.add(code)
            alias_to_canonical[code] = code
        elif len(code) > 5:
            # Sanity typo case (1RLIFF -> 1RLIF)
            truncated = code[:5]
            search_codes.add(truncated)
            alias_to_canonical[truncated] = code
    return search_codes, alias_to_canonical


# ---------------------------------------------------------------------------
# digit_index lookup from local-prospector
# ---------------------------------------------------------------------------

def load_digit_index(tollfree_db: Path) -> dict[str, list[dict]]:
    """{7-digit pattern: [{term, category_code, category_code_2, relevance, mike_rank}]}.
    Sorted within each bucket by best-match-first."""
    conn = sqlite3.connect(str(tollfree_db))
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT digits, term, category_code, category_code_2, relevance, mike_rank
        FROM digit_index
        WHERE term_length = 7 AND category_code IS NOT NULL
    """).fetchall()
    conn.close()

    idx: dict[str, list[dict]] = defaultdict(list)
    for digits, term, cat, cat2, rel, mike_rank in rows:
        idx[digits].append({
            "term": term,
            "category_code": cat,
            "category_code_2": cat2,
            "relevance": rel,
            "mike_rank": mike_rank,
        })
    for digits in idx:
        idx[digits].sort(key=lambda m: (
            -(m["relevance"] or 0),
            m["mike_rank"] or 1_000_000,
        ))
    return dict(idx)


def load_category_labels(tollfree_db: Path) -> dict[str, str]:
    conn = sqlite3.connect(str(tollfree_db))
    cur = conn.cursor()
    rows = cur.execute("SELECT code, label FROM categories").fetchall()
    conn.close()
    return {code: label for code, label in rows}


# ---------------------------------------------------------------------------
# Output SQLite construction
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE rows (
    phone            TEXT NOT NULL,
    vendor_code      TEXT NOT NULL,
    area_code        TEXT,
    status           TEXT,
    age_months       INTEGER,
    first_used_date  TEXT,
    source           TEXT,
    term             TEXT,
    category_code    TEXT,
    category_label   TEXT,
    category_code_2  TEXT,
    category_label_2 TEXT,
    relevance        INTEGER
);
"""

INDEXES = [
    "CREATE INDEX idx_rows_vendor ON rows(vendor_code, status)",
    "CREATE INDEX idx_rows_category ON rows(category_code, status)",
    "CREATE INDEX idx_rows_vendor_category ON rows(vendor_code, category_code)",
    "CREATE INDEX idx_rows_phone ON rows(phone)",
]


def build_vnfs_db(month: str, parquet_path: Path, manifest_path: Path,
                  tollfree_db: Path, out_path: Path):
    print(f"[VNFS] Loading vendor manifest from {manifest_path}")
    search_codes, alias_to_canonical = build_vendor_lookup(manifest_path)
    print(f"  {len(search_codes)} vendor search codes; aliases: "
          f"{', '.join(f'{a}->{c}' for a, c in alias_to_canonical.items() if a != c) or 'none'}")

    print(f"[VNFS] Loading digit_index from {tollfree_db}")
    digit_idx = load_digit_index(tollfree_db)
    print(f"  {len(digit_idx):,} 7-digit patterns")

    cat_labels = load_category_labels(tollfree_db)
    print(f"  {len(cat_labels)} categories")

    print(f"[VNFS] Reading {parquet_path} ({parquet_path.stat().st_size / 1_048_576:.0f} MB)")
    pf = pq.ParquetFile(str(parquet_path))

    if out_path.exists():
        out_path.unlink()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(out_path))
    conn.executescript("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;")
    conn.execute(SCHEMA)
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO rows (phone, vendor_code, area_code, status, age_months,
                          first_used_date, source, term, category_code,
                          category_label, category_code_2, category_label_2, relevance)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    t0 = time.time()
    scanned = 0
    written = 0
    last_progress_t = t0
    last_progress_scanned = 0

    cur.execute("BEGIN")

    for batch in pf.iter_batches(
        batch_size=200_000,
        columns=["number", "prefix", "status", "resporg", "date", "fourth"],
    ):
        nums = batch.column("number").to_pylist()
        prefixes = batch.column("prefix").to_pylist()
        statuses = batch.column("status").to_pylist()
        resporgs = batch.column("resporg").to_pylist()
        dates = batch.column("date").to_pylist()
        fourths = batch.column("fourth").to_pylist()

        rows_to_insert = []
        for i in range(len(nums)):
            scanned += 1
            resp = resporgs[i]
            if resp not in search_codes:
                continue
            status = statuses[i]
            if status not in SALABLE_STATUSES:
                continue
            phone = f"{nums[i]:010d}"
            vendor_code = alias_to_canonical[resp]
            area_code = phone[:3]
            try:
                age = int(fourths[i]) if fourths[i] else None
            except ValueError:
                age = None
            first_used = dates[i] or None

            last7 = phone[-7:]
            matches = digit_idx.get(last7)
            if matches:
                for m in matches:
                    cat = m["category_code"]
                    cat2 = m["category_code_2"]
                    rows_to_insert.append((
                        phone, vendor_code, area_code, status, age, first_used,
                        "adjusted",
                        m["term"],
                        cat,
                        cat_labels.get(cat, cat),
                        cat2,
                        cat_labels.get(cat2) if cat2 else None,
                        m["relevance"],
                    ))
                    written += 1
            else:
                rows_to_insert.append((
                    phone, vendor_code, area_code, status, age, first_used,
                    "adjusted",
                    None, None, None, None, None, None,
                ))
                written += 1

        cur.executemany(insert_sql, rows_to_insert)

        now = time.time()
        if now - last_progress_t >= 5:
            rate_in = (scanned - last_progress_scanned) / (now - last_progress_t)
            print(f"  scanned {scanned:>10,} | wrote {written:>9,} | "
                  f"{rate_in/1000:.0f}K rec/s")
            last_progress_t = now
            last_progress_scanned = scanned

    conn.commit()

    print(f"[VNFS] Building indexes...")
    for idx_sql in INDEXES:
        cur.execute(idx_sql)
    cur.execute("ANALYZE")
    conn.commit()
    conn.close()

    elapsed = time.time() - t0
    size_mb = out_path.stat().st_size / 1_048_576
    print(f"\n[VNFS] Done. Scanned {scanned:,} | wrote {written:,} | "
          f"{elapsed:.1f}s | {size_mb:.1f} MB")
    print(f"  -> {out_path}")
    return scanned, written


# ---------------------------------------------------------------------------
# Optional: deploy to droplet
# ---------------------------------------------------------------------------

def deploy_to_droplet(local_path: Path, droplet_ssh: str = "root@104.131.76.98",
                      remote_path: str = "/var/www/vanitynumbersforsales/data/inventory_2026-05.db",
                      service: str = "vanitynumbersforsales"):
    """gzip + scp the SQLite, decompress on droplet, restart service.
    Uses Python's built-in gzip (no system gzip needed). Requires scp + ssh on PATH."""
    import gzip as _gzip
    import shutil
    import subprocess

    print(f"[DEPLOY] Compressing {local_path} (Python gzip)...")
    gz_path = local_path.with_suffix(local_path.suffix + ".gz")
    if gz_path.exists():
        gz_path.unlink()
    src_size = local_path.stat().st_size
    with open(local_path, "rb") as f_in, _gzip.open(str(gz_path), "wb",
                                                    compresslevel=6) as f_out:
        shutil.copyfileobj(f_in, f_out, length=4 * 1024 * 1024)
    gz_size = gz_path.stat().st_size
    print(f"  {src_size/1_048_576:.1f} MB -> {gz_size/1_048_576:.1f} MB "
          f"({gz_size/src_size*100:.1f}%)")

    print(f"[DEPLOY] Uploading {gz_path.name} to {droplet_ssh}:{remote_path}.gz...")
    subprocess.run(["scp", str(gz_path), f"{droplet_ssh}:{remote_path}.gz"], check=True)
    print(f"[DEPLOY] Decompressing on droplet and restarting {service}...")
    subprocess.run([
        "ssh", droplet_ssh,
        f"cd $(dirname '{remote_path}') && gunzip -f '{remote_path}.gz' "
        f"&& systemctl restart {service} "
        f"&& sleep 8 && systemctl is-active {service}"
    ], check=True)
    gz_path.unlink()
    print("[DEPLOY] Done.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--parquet", type=Path, default=None,
                        help="Adjusted parquet path (default: cache/adjusted/<MM>.parquet)")
    parser.add_argument("--manifest", type=Path,
                        default=DEFAULT_VNFS_ROOT / "data" / "vendor_manifest.json")
    parser.add_argument("--tollfree-db", type=Path, default=DEFAULT_TOLLFREE_DB)
    parser.add_argument("--out", type=Path, default=None,
                        help="Output SQLite path (default: <VNFS_ROOT>/data/inventory_<MM>.db)")
    parser.add_argument("--deploy", action="store_true",
                        help="Push to droplet + restart service after build")
    args = parser.parse_args()

    if not re.match(r"^\d{4}-\d{2}$", args.month):
        parser.error("--month must be YYYY-MM")

    parquet_path = args.parquet or (DEFAULT_PARQUET_DIR / f"{args.month}.parquet")
    if not parquet_path.exists():
        parser.error(f"Adjusted parquet not found: {parquet_path}")

    out_path = args.out or (DEFAULT_VNFS_ROOT / "data" / f"inventory_{args.month}.db")

    build_vnfs_db(args.month, parquet_path, args.manifest, args.tollfree_db, out_path)

    if args.deploy:
        deploy_to_droplet(out_path)


if __name__ == "__main__":
    main()
