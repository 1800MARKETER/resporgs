"""Single-driver orchestrator for one month's Somos pipeline.

Runs the full pipeline for one month with one command. Skips steps whose
outputs already exist (idempotent re-runs). Logs each step's timing.

Steps (each step is independently skippable):
    1. Verify CD-ROM input files exist (split ALL.txt first if needed; we
       don't auto-split here — split_all.py is intentionally separate so
       Bill can review the split before the long build).
    2. Build adjusted parquet: cache/adjusted/<MM>.parquet
    3. Emit legacy outputs: cache/legacy/<MM>-TFOINPUT.txt + <MM>-MikeOINPUT.txt
    4. Build VNFS inventory: <VNFS_ROOT>/data/inventory_<MM>.db
    5. Optional: deploy VNFS to droplet (--deploy)

Run:
    python -m scripts.somos_adjust.make_month --month 2026-05 \\
        --input-dir "C:/Users/Bill/Downloads/2026-05"

    # Skip steps that already have outputs:
    python -m scripts.somos_adjust.make_month --month 2026-05 \\
        --input-dir "C:/Users/Bill/Downloads/2026-05" --skip-existing

    # Just rebuild the legacy outputs:
    python -m scripts.somos_adjust.make_month --month 2026-05 --only legacy

    # Build + deploy VNFS to droplet:
    python -m scripts.somos_adjust.make_month --month 2026-05 \\
        --input-dir "C:/Users/Bill/Downloads/2026-05" --deploy
"""
from __future__ import annotations
import argparse
import re
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent

STEPS = ("adjusted", "legacy", "vnfs", "intel")


def _step_header(name: str, msg: str):
    bar = "=" * 64
    print(f"\n{bar}\n  STEP: {name} - {msg}\n{bar}")


def step_adjusted(month: str, input_dir: Path, ctrl_dir: Path | None,
                  skip_existing: bool) -> Path:
    out = ROOT / "cache" / "adjusted" / f"{month}.parquet"
    if skip_existing and out.exists():
        size_mb = out.stat().st_size / 1_048_576
        print(f"[adjusted] Already exists: {out} ({size_mb:.0f} MB) — skipping.")
        return out

    _step_header("adjusted", f"build canonical parquet for {month}")
    from .build_adjusted import (
        find_cd_rom_files, write_ac_to_parquet, SCHEMA, PREFIXES,
        DEFAULT_OUTPUT_DIR
    )
    from .control_files import load_all, DEFAULT_CTRL_DIR
    import pyarrow.parquet as pq

    bundle = load_all(ctrl_dir or DEFAULT_CTRL_DIR)
    print(f"[adjusted] Loaded controls: "
          f"{len(bundle.ac_exc)} AcExc | {len(bundle.ro2ro)} RO2RO | "
          f"{len(bundle.ro2stat)} RO2Stat | {len(bundle.individual)} Individual")

    cd_rom = find_cd_rom_files(input_dir)
    missing = [p for p in PREFIXES if p not in cd_rom]
    if missing:
        raise SystemExit(
            f"Missing CD-ROM files for ACs: {missing}\n"
            f"  If Somos sent ALL.txt, run split_all.py first."
        )

    out.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    writer = pq.ParquetWriter(str(out), SCHEMA, compression="zstd")
    total = 0
    try:
        for ac in PREFIXES:
            print(f"[adjusted]   AC {ac}: {cd_rom[ac].name}")
            total += write_ac_to_parquet(writer, ac, cd_rom[ac], bundle)
    finally:
        writer.close()
    size_mb = out.stat().st_size / 1_048_576
    print(f"[adjusted] {total:,} records, {size_mb:.0f} MB, "
          f"{time.time()-t0:.0f}s -> {out}")
    return out


def step_legacy(month: str, skip_existing: bool) -> tuple[Path, Path]:
    tfo = ROOT / "cache" / "legacy" / f"{month}-TFOINPUT.txt"
    mike = ROOT / "cache" / "legacy" / f"{month}-MikeOINPUT.txt"
    if skip_existing and tfo.exists() and mike.exists():
        print(f"[legacy] Already exist:\n  {tfo}\n  {mike}\n  — skipping.")
        return tfo, mike

    _step_header("legacy", f"emit TFOINPUT + MikeOINPUT for {month}")
    from .output_legacy import emit, DEFAULT_INPUT_DIR, DEFAULT_OUTPUT_DIR
    t0 = time.time()
    emit(month, DEFAULT_INPUT_DIR, DEFAULT_OUTPUT_DIR,
         emit_tfo=True, emit_mike=True, limit=None)
    print(f"[legacy] done in {time.time()-t0:.0f}s")
    return tfo, mike


def step_vnfs(month: str, skip_existing: bool, deploy: bool,
              tollfree_db: Path | None, manifest: Path | None) -> Path:
    from .output_vnfs import (
        DEFAULT_VNFS_ROOT, DEFAULT_TOLLFREE_DB, build_vnfs_db,
        deploy_to_droplet
    )
    out = (DEFAULT_VNFS_ROOT / "data" / f"inventory_{month}.db")
    if skip_existing and out.exists() and not deploy:
        size_mb = out.stat().st_size / 1_048_576
        print(f"[vnfs] Already exists: {out} ({size_mb:.0f} MB) — skipping.")
        return out

    _step_header("vnfs", f"build VanityNumbersForSales SQLite for {month}")
    parquet = ROOT / "cache" / "adjusted" / f"{month}.parquet"
    if not parquet.exists():
        raise SystemExit(f"[vnfs] Adjusted parquet missing: {parquet}")
    manifest_path = manifest or (DEFAULT_VNFS_ROOT / "data" / "vendor_manifest.json")
    if not manifest_path.exists():
        raise SystemExit(f"[vnfs] Vendor manifest missing: {manifest_path}")
    db = tollfree_db or DEFAULT_TOLLFREE_DB
    if not db.exists():
        raise SystemExit(f"[vnfs] tollfree.db missing: {db}")

    t0 = time.time()
    build_vnfs_db(month, parquet, manifest_path, db, out)
    print(f"[vnfs] build done in {time.time()-t0:.0f}s")

    if deploy:
        _step_header("vnfs deploy", f"upload {out.name} to droplet")
        deploy_to_droplet(
            out,
            remote_path=f"/var/www/vanitynumbersforsales/data/inventory_{month}.db",
        )
    return out


def step_intel(month: str, skip_existing: bool, deploy: bool) -> Path:
    out = ROOT / "cache" / "intel" / f"tollfree_intel-{month}.db"
    if skip_existing and out.exists() and not deploy:
        size_mb = out.stat().st_size / 1_048_576
        print(f"[intel] Already exists: {out} ({size_mb:.0f} MB) — skipping.")
        return out

    _step_header("intel", f"build VanityNumbers.com lookup SQLite for {month}")
    parquet = ROOT / "cache" / "adjusted" / f"{month}.parquet"
    if not parquet.exists():
        raise SystemExit(f"[intel] Adjusted parquet missing: {parquet}")

    from .build_tollfree_intel import (
        load_vanity_lookup, DEFAULT_VANITY_DB, SCHEMA, INDEXES, deploy as intel_deploy
    )
    import sqlite3 as _sqlite3
    import pyarrow.parquet as pq

    print(f"[intel] Loading vanity lookup from {DEFAULT_VANITY_DB}")
    vanity = load_vanity_lookup(DEFAULT_VANITY_DB)
    print(f"  {len(vanity):,} unique 7-digit vanity patterns")

    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        out.unlink()
    conn = _sqlite3.connect(str(out))
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

    pf = pq.ParquetFile(str(parquet))
    cur.execute("BEGIN")
    written = 0
    for batch in pf.iter_batches(batch_size=200_000,
                                  columns=["number", "status", "resporg", "date"]):
        nums = batch.column("number").to_pylist()
        statuses = batch.column("status").to_pylist()
        resporgs = batch.column("resporg").to_pylist()
        dates = batch.column("date").to_pylist()
        rows = []
        for i in range(len(nums)):
            digit10 = f"{nums[i]:010d}"
            v = vanity.get(digit10[3:])
            if v is None:
                continue
            word, ccode, clabel, mike_rank, score = v
            rows.append((
                digit10, f"{digit10[:3]}-{word}", statuses[i], dates[i] or "",
                (resporgs[i] or "").strip(), month,
                ccode, clabel, mike_rank, score, word, None, None,
            ))
        if rows:
            cur.executemany(insert_sql, rows)
            written += len(rows)
    print(f"[intel] Built indexes...")
    for sql in INDEXES:
        cur.execute(sql)
    cur.execute("ANALYZE")
    conn.commit()
    conn.close()
    print(f"[intel] {written:,} rows written, {out.stat().st_size / 1_048_576:.0f} MB")

    if deploy:
        _step_header("intel deploy", f"upload {out.name} to NYC3 + restart phoneplatinumwire")
        intel_deploy(out)
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--input-dir", type=Path, default=None,
                        help="Folder with the 7 CD-ROM files. Required for the adjusted step.")
    parser.add_argument("--ctrl-dir", type=Path, default=None,
                        help="Folder with the 4 control files (default: C:\\MonthlyProcessing2)")
    parser.add_argument("--only", choices=STEPS, default=None,
                        help="Run only this step")
    parser.add_argument("--skip", action="append", choices=STEPS, default=[],
                        help="Skip this step (repeatable)")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip a step if its output file(s) already exist")
    parser.add_argument("--deploy", action="store_true",
                        help="Deploy VNFS DB to droplet after building")
    parser.add_argument("--tollfree-db", type=Path, default=None)
    parser.add_argument("--manifest", type=Path, default=None)
    args = parser.parse_args()

    if not re.match(r"^\d{4}-\d{2}$", args.month):
        parser.error("--month must be YYYY-MM")

    if args.only:
        steps_to_run = [args.only]
    else:
        steps_to_run = [s for s in STEPS if s not in args.skip]

    print(f"Steps to run for {args.month}: {', '.join(steps_to_run)}")
    print(f"  skip-existing={args.skip_existing}  deploy={args.deploy}")

    overall_t0 = time.time()
    completed: list[tuple[str, float]] = []

    if "adjusted" in steps_to_run:
        if not args.input_dir:
            parser.error("--input-dir is required for the 'adjusted' step")
        t0 = time.time()
        step_adjusted(args.month, args.input_dir, args.ctrl_dir, args.skip_existing)
        completed.append(("adjusted", time.time() - t0))

    if "legacy" in steps_to_run:
        t0 = time.time()
        step_legacy(args.month, args.skip_existing)
        completed.append(("legacy", time.time() - t0))

    if "vnfs" in steps_to_run:
        t0 = time.time()
        step_vnfs(args.month, args.skip_existing, args.deploy,
                  args.tollfree_db, args.manifest)
        completed.append(("vnfs", time.time() - t0))

    if "intel" in steps_to_run:
        t0 = time.time()
        step_intel(args.month, args.skip_existing, args.deploy)
        completed.append(("intel", time.time() - t0))

    elapsed = time.time() - overall_t0
    print("\n" + "=" * 64)
    print(f"  ALL DONE in {elapsed:.0f}s")
    for name, dur in completed:
        print(f"    {name:>10}: {dur:6.1f}s")
    print("=" * 64)


if __name__ == "__main__":
    main()
