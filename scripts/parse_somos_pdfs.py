"""Parse Somos weekly Number Administration PDFs into three parquets.

Walks RESPORGS/somos_pdfs/NUM-*.pdf and extracts:

  data/somos_weekly_npa.parquet
      One row per (week_ending, npa). Columns:
        report_no, report_date, week_ending, npa,
        working, assigned, reserved, disconnect, transit, unavail, suspend,
        total_in_use, pct_in_use, spare, total_pool

  data/somos_weekly_pool.parquet
      One row per week_ending. Columns:
        week_ending, total_in_use, spare, growth_week,
        reserved_during_week, spared_from_disconnect,
        spared_from_reserved, spared_from_unavail, total_spared_week

  data/somos_exhaust_forecasts.parquet
      One row per (report_date, horizon). Columns:
        report_date, week_ending, horizon_months, observations,
        start_label, monthly_rate_of_change, months_to_exhaust,
        predicted_exhaust_date

The pool-trend table on page 2 of each PDF shows the trailing 6 weeks. We
parse every PDF and dedupe (week_ending, value) pairs by keeping the most
recent observation; that lets a sparse archive backfill historical weeks
correctly.
"""
from __future__ import annotations
import re
from datetime import datetime
from pathlib import Path

import pdfplumber
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
PDF_DIR = ROOT / "somos_pdfs"
OUT_DIR = ROOT / "data"

NPA_LABEL_RE = re.compile(r"NPA:\s*(\d{3})", re.I)
DATE_LINE_RE = re.compile(r"Date:\s*(\d{1,2}/\d{1,2}/\d{4})")
SUBJECT_DATE_RE = re.compile(r"for\s+\w+,\s*(\d{1,2}/\d{1,2}/\d{4})")
NOTIFICATION_RE = re.compile(r"Notification No:\s*(NUM-\d{2}-\d+)")
HORIZON_RE = re.compile(r"\((\d+)\)")


def _to_int(s: str | None) -> int | None:
    if s is None:
        return None
    s = s.replace(",", "").strip()
    if not s or s.upper() == "N/A":
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _to_pct(s: str | None) -> float | None:
    if s is None:
        return None
    s = s.replace("%", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _to_date(s: str | None) -> str | None:
    """mm/dd/yyyy -> yyyy-mm-dd."""
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None


def _parse_exhaust_date(s: str | None) -> str | None:
    """'Nov 2038' -> '2038-11-01'."""
    if not s or s.upper() == "N/A":
        return None
    try:
        return datetime.strptime(s.strip(), "%b %Y").date().isoformat()
    except ValueError:
        return None


def parse_one(pdf_path: Path) -> dict:
    """Return {'npa': [...], 'pool': [...], 'exhaust': [...]} for one PDF."""
    with pdfplumber.open(pdf_path) as pdf:
        page1_text = pdf.pages[0].extract_text() or ""
        page1_tables = pdf.pages[0].extract_tables()
        page2_tables = pdf.pages[1].extract_tables() if len(pdf.pages) >= 2 else []

    notif = NOTIFICATION_RE.search(page1_text)
    report_date = _to_date(DATE_LINE_RE.search(page1_text).group(1)) if DATE_LINE_RE.search(page1_text) else None
    week_ending = _to_date(SUBJECT_DATE_RE.search(page1_text).group(1)) if SUBJECT_DATE_RE.search(page1_text) else None
    # Some PDFs render the subject line with doubled characters (font glitch).
    # Try a "decompressed" version: collapse consecutive doubled characters.
    if week_ending is None:
        decompressed = re.sub(r"(.)\1", r"\1", page1_text)
        m = SUBJECT_DATE_RE.search(decompressed)
        if m:
            week_ending = _to_date(m.group(1))
    # Final fallback: report_date (the email date) is two days after the
    # Saturday week-ending. Walk back to the most recent Saturday.
    if week_ending is None and report_date is not None:
        from datetime import date as _date, timedelta
        d = _date.fromisoformat(report_date)
        # Saturday = weekday 5; back up to most recent Saturday on or before d.
        offset = (d.weekday() - 5) % 7
        week_ending = (d - timedelta(days=offset)).isoformat()
    report_no = notif.group(1) if notif else pdf_path.stem

    # ---- Page 1: NPA snapshot table ----
    npa_rows = []
    if page1_tables:
        for row in page1_tables[0]:
            if not row or not row[0]:
                continue
            m = NPA_LABEL_RE.search(row[0])
            if not m:
                continue
            npa = int(m.group(1))
            # Cols: WORKING, ASSIGNED, RESERVED, DISCONNECT, TRANSIT, UNAVAIL,
            #       SUSPEND, TOTAL_IN_USE, PERCENT, SPARE, TOTAL_POOL
            npa_rows.append({
                "report_no": report_no,
                "report_date": report_date,
                "week_ending": week_ending,
                "npa": npa,
                "working": _to_int(row[1]),
                "assigned": _to_int(row[2]),
                "reserved": _to_int(row[3]),
                "disconnect": _to_int(row[4]),
                "transit": _to_int(row[5]),
                "unavail": _to_int(row[6]),
                "suspend": _to_int(row[7]),
                "total_in_use": _to_int(row[8]),
                "pct_in_use": _to_pct(row[9]),
                "spare": _to_int(row[10]),
                "total_pool": _to_int(row[11]),
            })

    # ---- Page 2 Table 0: 6-week pool flow ----
    pool_rows = []
    if len(page2_tables) >= 1:
        flow_table = page2_tables[0]
        # Skip header row; data rows have a date in col 0.
        for row in flow_table[1:]:
            if not row or not row[0]:
                continue
            wend = _to_date(row[0])
            if not wend:
                continue
            # Some PDFs have a blank column index 4 (between GROWTH/WEEK and
            # NUMBERS RESERVED). Detect by column count.
            if len(row) >= 10:
                # Layout: date, in_use, spare, growth, '', reserved, spared_disc,
                #         spared_res, spared_unavail, total_spared
                pool_rows.append({
                    "week_ending": wend,
                    "total_in_use": _to_int(row[1]),
                    "spare": _to_int(row[2]),
                    "growth_week": _to_int(row[3]),
                    "reserved_during_week": _to_int(row[5]),
                    "spared_from_disconnect": _to_int(row[6]),
                    "spared_from_reserved": _to_int(row[7]),
                    "spared_from_unavail": _to_int(row[8]),
                    "total_spared_week": _to_int(row[9]),
                })
            else:
                # No blank column variant.
                pool_rows.append({
                    "week_ending": wend,
                    "total_in_use": _to_int(row[1]),
                    "spare": _to_int(row[2]),
                    "growth_week": _to_int(row[3]),
                    "reserved_during_week": _to_int(row[4]),
                    "spared_from_disconnect": _to_int(row[5]),
                    "spared_from_reserved": _to_int(row[6]),
                    "spared_from_unavail": _to_int(row[7]),
                    "total_spared_week": _to_int(row[8]),
                })

    # ---- Page 2 Table 1: exhaust forecasts ----
    exhaust_rows = []
    if len(page2_tables) >= 2:
        forecast_table = page2_tables[1]
        for row in forecast_table[1:]:
            if not row or not row[0] or "(" not in row[0]:
                continue
            obs_match = HORIZON_RE.search(row[0])
            if not obs_match:
                continue
            observations = int(obs_match.group(1))
            horizon_months = observations  # the regression-window length, in months
            start_label = row[0].split("(")[0].strip()
            exhaust_rows.append({
                "report_date": report_date,
                "week_ending": week_ending,
                "horizon_months": horizon_months,
                "observations": observations,
                "start_label": start_label,
                "monthly_rate_of_change": _to_int(row[1]),
                "months_to_exhaust": _to_int(row[2]),
                "predicted_exhaust_date": _parse_exhaust_date(row[3]),
            })

    return {"npa": npa_rows, "pool": pool_rows, "exhaust": exhaust_rows}


def main() -> int:
    pdfs = sorted(PDF_DIR.glob("NUM-*.pdf"))
    if not pdfs:
        print(f"No PDFs in {PDF_DIR}")
        return 1

    OUT_DIR.mkdir(exist_ok=True)

    npa_rows: list[dict] = []
    pool_rows_by_week: dict[str, dict] = {}  # dedupe by week_ending
    exhaust_rows: list[dict] = []
    failed: list[tuple[str, str]] = []

    for i, p in enumerate(pdfs, 1):
        try:
            out = parse_one(p)
        except Exception as e:
            failed.append((p.name, str(e)[:80]))
            continue
        npa_rows.extend(out["npa"])
        for r in out["pool"]:
            wend = r["week_ending"]
            if wend not in pool_rows_by_week:
                pool_rows_by_week[wend] = r
            else:
                # Prefer the row sourced from a more recent report (any later
                # PDF reporting this same week is by definition the same data).
                pass
        exhaust_rows.extend(out["exhaust"])
        if i % 50 == 0:
            print(f"  parsed {i}/{len(pdfs)}")

    print(f"\nParsed {len(pdfs) - len(failed)}/{len(pdfs)} PDFs")
    if failed:
        print(f"  Failures: {len(failed)}")
        for f in failed[:10]:
            print(f"    {f}")

    # ---- Write parquets ----
    npa_table = pa.Table.from_pylist(npa_rows)
    pool_rows = sorted(pool_rows_by_week.values(), key=lambda r: r["week_ending"])
    pool_table = pa.Table.from_pylist(pool_rows)
    exhaust_table = pa.Table.from_pylist(exhaust_rows)

    npa_path = OUT_DIR / "somos_weekly_npa.parquet"
    pool_path = OUT_DIR / "somos_weekly_pool.parquet"
    exhaust_path = OUT_DIR / "somos_exhaust_forecasts.parquet"

    pq.write_table(npa_table, npa_path)
    pq.write_table(pool_table, pool_path)
    pq.write_table(exhaust_table, exhaust_path)

    print(f"\n  wrote {npa_path.name}: {npa_table.num_rows:,} rows")
    print(f"  wrote {pool_path.name}: {pool_table.num_rows:,} rows")
    print(f"  wrote {exhaust_path.name}: {exhaust_table.num_rows:,} rows")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
