"""Build tollfree_intel.db from the canonical adjusted parquet + master_vanity.

This SQLite is what PhonePlatinumWire (the lookup engine behind
VanityNumbers.com search) queries to render the per-NPA "in use with X"
toll-free strip when a customer types a vanity word.

The existing /var/www/1cup/data/tollfree_intel.db on the NYC3 droplet
is rebuilt monthly from old MoDatProc.exe output. This rebuild replaces
it with a fresh pull from our canonical adjusted parquet — same schema,
same indexes, lookup code on the consumer site needs zero changes.

Output schema (must stay byte-compat — used by /var/www/PhonePlatinumWire/app/lookup.py):

    CREATE TABLE tf_monthly_scan (
        digit_number TEXT,    -- 10-digit phone, no formatting (e.g. "8002000000")
        vanity_form TEXT,     -- canonical vanity render (e.g. "800-TEXTGOD")
        status TEXT,          -- canonical UPPERCASE (e.g. "WORKING")
        change_date TEXT,     -- "YY/MM/DD" or ""
        resporg_id TEXT,      -- 5-char post-adjustment code
        scan_month TEXT,      -- "YYYY-MM"
        category_code TEXT,   -- from master_vanity (e.g. "TEL")
        category_label TEXT,
        keyword_rank INTEGER, -- mike_rank from master_vanity
        value_score REAL,     -- blended_score from master_vanity
        term TEXT,            -- the vanity word (e.g. "TEXTGOD")
        is_new_disconn INTEGER,
        is_new_transit INTEGER
    )
    + indexes idx_tms_month, idx_tms_substr4/5/6, idx_tms_exact

Filtering: include every TFN whose 7-digit suffix has a master_vanity match.
With ~1.26M unique 7-digit vanity patterns × 7 NPAs, that's up to ~8.8M rows
(less in practice — not every pattern exists in every NPA's working pool,
but we include ones that exist in the parquet for any status).

Run from RESPORGS root:
    python -m scripts.somos_adjust.build_tollfree_intel --month 2026-05

    # Build + scp to NYC3 + (no service restart needed; SQLite read-only):
    python -m scripts.somos_adjust.build_tollfree_intel --month 2026-05 --deploy
"""
from __future__ import annotations
import argparse
import re
import shutil
import sqlite3
import subprocess
import time
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_PARQUET_DIR = ROOT / "cache" / "adjusted"
DEFAULT_OUTPUT_DIR = ROOT / "cache" / "intel"
DEFAULT_VANITY_DB = Path("/var/www/local-prospector/data/master_vanity.db")

# Where the consumer site reads from on NYC3
DEPLOY_HOST = "root@104.131.76.98"
DEPLOY_PATH = "/var/www/1cup/data/tollfree_intel.db"
# SSH identity used to reach NYC3 from this box. Set up once during the
# 2026-05-08 RESPORGS migration; keeps cross-server pushes key-only.
DEPLOY_SSH_KEY = "/root/.ssh/migration_key"
SSH_OPTS = ["-i", DEPLOY_SSH_KEY, "-o", "StrictHostKeyChecking=no",
            "-o", "BatchMode=yes"]

SCHEMA = """
CREATE TABLE tf_monthly_scan (
    digit_number TEXT,
    vanity_form TEXT,
    status TEXT,
    change_date TEXT,
    resporg_id TEXT,
    scan_month TEXT,
    category_code TEXT,
    category_label TEXT,
    keyword_rank INTEGER,
    value_score REAL,
    term TEXT,
    is_new_disconn INTEGER,
    is_new_transit INTEGER
);
"""

INDEXES = [
    "CREATE INDEX idx_tms_month ON tf_monthly_scan(scan_month)",
    "CREATE INDEX idx_tms_exact ON tf_monthly_scan(scan_month, digit_number)",
    "CREATE INDEX idx_tms_substr4 ON tf_monthly_scan(scan_month, SUBSTR(digit_number, 7, 4))",
    "CREATE INDEX idx_tms_substr5 ON tf_monthly_scan(scan_month, SUBSTR(digit_number, 6, 5))",
    "CREATE INDEX idx_tms_substr6 ON tf_monthly_scan(scan_month, SUBSTR(digit_number, 5, 6))",
]


def load_vanity_lookup(vanity_db: Path) -> dict[str, tuple]:
    """Returns {digits_7: (word, category_code, category_label, mike_rank, blended_score)}.

    For digits with multiple vanity entries (different words spell the same digits),
    keeps the one with the highest blended_score. blended_score is what 1Cup uses
    as the canonical rank in their pipeline.
    """
    conn = sqlite3.connect(f"file:{vanity_db}?mode=ro", uri=True)
    cur = conn.execute("""
        SELECT digits, word, category_code, category_label, mike_rank, blended_score
        FROM vanity
    """)
    out: dict[str, tuple] = {}
    for digits, word, ccode, clabel, mike_rank, score in cur:
        cur_score = (out.get(digits) or (None, None, None, None, -1))[4] or -1
        if (score or 0) > cur_score:
            out[digits] = (word, ccode, clabel, mike_rank, score)
    conn.close()
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", required=True, help="YYYY-MM (must match a parquet stem)")
    parser.add_argument("--parquet", type=Path, default=None,
                        help="Adjusted parquet path (default: cache/adjusted/<MM>.parquet)")
    parser.add_argument("--vanity-db", type=Path, default=DEFAULT_VANITY_DB)
    parser.add_argument("--out", type=Path, default=None,
                        help="Output SQLite (default: cache/intel/tollfree_intel-<MM>.db)")
    parser.add_argument("--deploy", action="store_true",
                        help="scp to NYC3 droplet after build (overwrites the live "
                             f"file at {DEPLOY_HOST}:{DEPLOY_PATH})")
    parser.add_argument("--include-all-statuses", action="store_true", default=True,
                        help="(default) include all statuses incl. AVAIL")
    args = parser.parse_args()

    if not re.match(r"^\d{4}-\d{2}$", args.month):
        parser.error("--month must be YYYY-MM")

    parquet = args.parquet or (DEFAULT_PARQUET_DIR / f"{args.month}.parquet")
    if not parquet.exists():
        parser.error(f"Adjusted parquet not found: {parquet}")
    out = args.out or (DEFAULT_OUTPUT_DIR / f"tollfree_intel-{args.month}.db")

    print(f"[intel] Loading vanity lookup from {args.vanity_db}")
    t0 = time.time()
    vanity = load_vanity_lookup(args.vanity_db)
    print(f"  {len(vanity):,} unique 7-digit vanity patterns ({time.time()-t0:.1f}s)")

    print(f"[intel] Reading {parquet} ({parquet.stat().st_size / 1_048_576:.0f} MB)")
    pf = pq.ParquetFile(str(parquet))

    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        out.unlink()
    conn = sqlite3.connect(str(out))
    conn.executescript("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;")
    conn.executescript(SCHEMA)
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO tf_monthly_scan
        (digit_number, vanity_form, status, change_date, resporg_id, scan_month,
         category_code, category_label, keyword_rank, value_score, term,
         is_new_disconn, is_new_transit)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cur.execute("BEGIN")
    t0 = time.time()
    scanned = 0
    written = 0
    last_log = t0

    for batch in pf.iter_batches(batch_size=200_000,
                                  columns=["number", "status", "resporg", "date"]):
        nums = batch.column("number").to_pylist()
        statuses = batch.column("status").to_pylist()
        resporgs = batch.column("resporg").to_pylist()
        dates = batch.column("date").to_pylist()

        rows = []
        for i in range(len(nums)):
            scanned += 1
            digit10 = f"{nums[i]:010d}"
            suffix7 = digit10[3:]
            v = vanity.get(suffix7)
            if v is None:
                continue
            word, ccode, clabel, mike_rank, score = v
            npa = digit10[:3]
            vanity_form = f"{npa}-{word}"
            rows.append((
                digit10, vanity_form, statuses[i], dates[i] or "",
                (resporgs[i] or "").strip(), args.month,
                ccode, clabel, mike_rank, score, word,
                None, None,
            ))
        if rows:
            cur.executemany(insert_sql, rows)
            written += len(rows)

        now = time.time()
        if now - last_log >= 5:
            rate = scanned / (now - t0) / 1000
            print(f"  scanned {scanned:>11,} | wrote {written:>9,} | {rate:.0f}K/s")
            last_log = now

    print(f"[intel] Building indexes...")
    for sql in INDEXES:
        cur.execute(sql)
    cur.execute("ANALYZE")
    conn.commit()
    conn.close()

    elapsed = time.time() - t0
    size_mb = out.stat().st_size / 1_048_576
    print(f"\n[intel] Done. scanned {scanned:,} | wrote {written:,} | "
          f"{elapsed:.0f}s | {size_mb:.1f} MB")
    print(f"        -> {out}")

    if args.deploy:
        deploy(out)


def deploy(local_db: Path):
    """scp the new intel DB to NYC3, overwriting the live file.

    Compresses in flight (gzip) for ~3x faster transfer, then atomic rename
    on the remote so an in-flight request never sees a half-written file.
    """
    print(f"\n[deploy] Pushing {local_db.name} to {DEPLOY_HOST}:{DEPLOY_PATH}")
    src_size = local_db.stat().st_size
    print(f"  source: {src_size / 1_048_576:.1f} MB")

    # Compress + stream over ssh, decompress on far side. One pipe, no temp file.
    staging = DEPLOY_PATH + ".new"
    print(f"  streaming gzipped to {staging} then atomic-renaming...")
    t0 = time.time()
    cmd = (
        f"gzip -c '{local_db}' | "
        f"ssh {' '.join(SSH_OPTS)} {DEPLOY_HOST} "
        f"'gunzip -c > {staging} && chown www-data:www-data {staging} && "
        f"mv {staging} {DEPLOY_PATH} && ls -la {DEPLOY_PATH}'"
    )
    subprocess.run(cmd, shell=True, check=True)
    elapsed = time.time() - t0
    print(f"  upload done in {elapsed:.0f}s")

    # PhonePlatinumWire caches _latest_tf_scan_month at module load — must
    # restart it for the new month's data to be visible. SQLite reads are
    # otherwise live, so this is the only restart needed.
    print("[deploy] Restarting phoneplatinumwire on far side to clear scan_month cache...")
    subprocess.run([
        "ssh", *SSH_OPTS, DEPLOY_HOST,
        "systemctl restart phoneplatinumwire && "
        "sleep 2 && systemctl is-active phoneplatinumwire"
    ], check=True)
    print("[deploy] Done. Live site will show fresh data on next request.")


if __name__ == "__main__":
    main()
