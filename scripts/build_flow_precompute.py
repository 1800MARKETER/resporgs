"""
Precompute per-rpfx flow summaries.

Profile pages currently hit flow_graph.parquet (166K rows) four times per
request — inbound edge-type totals, outbound edge-type totals, top 10
direct-transfer sources, top 10 direct-transfer destinations. Small file,
so each query is fast, but four of them add up.

Produces two tables:
  data/flow_totals.parquet  — (rpfx, inbound_transfer, inbound_harvest,
                               inbound_first_assign, inbound_reactivate,
                               outbound_transfer, outbound_disconnect,
                               outbound_to_spare)
  data/flow_top_partners.parquet — (rpfx, direction, ord, partner_rpfx, n)
                               direction = 'in' | 'out', ord = 1..10
"""

from __future__ import annotations
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"


def main():
    con = duckdb.connect()
    flow = (DATA / "flow_graph.parquet").as_posix()

    t0 = time.time()

    # Flow totals (one row per rpfx, with counts for each edge_type direction)
    out_totals = DATA / "flow_totals.parquet"
    con.execute(
        f"""
        COPY (
          WITH inbound AS (
            SELECT to_node AS rpfx, edge_type, SUM(n) AS n
            FROM read_parquet('{flow}')
            GROUP BY to_node, edge_type
          ),
          outbound AS (
            SELECT from_node AS rpfx, edge_type, SUM(n) AS n
            FROM read_parquet('{flow}')
            GROUP BY from_node, edge_type
          ),
          all_rpfxs AS (
            SELECT DISTINCT rpfx FROM (
              SELECT rpfx FROM inbound
              UNION
              SELECT rpfx FROM outbound
            )
          )
          SELECT
            r.rpfx,
            COALESCE(SUM(CASE WHEN i.edge_type = 'TRANSFER'     THEN i.n END), 0)  AS inbound_transfer,
            COALESCE(SUM(CASE WHEN i.edge_type = 'HARVEST'      THEN i.n END), 0)  AS inbound_harvest,
            COALESCE(SUM(CASE WHEN i.edge_type = 'FIRST_ASSIGN' THEN i.n END), 0)  AS inbound_first_assign,
            COALESCE(SUM(CASE WHEN i.edge_type = 'REACTIVATE'   THEN i.n END), 0)  AS inbound_reactivate,
            COALESCE(SUM(CASE WHEN o.edge_type = 'TRANSFER'     THEN o.n END), 0)  AS outbound_transfer,
            COALESCE(SUM(CASE WHEN o.edge_type = 'DISCONNECT'   THEN o.n END), 0)  AS outbound_disconnect,
            COALESCE(SUM(CASE WHEN o.edge_type = 'TO_SPARE'     THEN o.n END), 0)  AS outbound_to_spare
          FROM all_rpfxs r
          LEFT JOIN inbound  i ON r.rpfx = i.rpfx
          LEFT JOIN outbound o ON r.rpfx = o.rpfx
          GROUP BY r.rpfx
        ) TO '{out_totals.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)
        """
    )
    n_tot = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_totals.as_posix()}')").fetchone()[0]
    print(f"  {out_totals.name}: {n_tot} rpfxs — {time.time()-t0:.1f}s")

    # Top partners (direction = 'in' | 'out', top 10 per rpfx per direction)
    t1 = time.time()
    out_partners = DATA / "flow_top_partners.parquet"
    con.execute(
        f"""
        COPY (
          WITH inbound_pairs AS (
            SELECT to_node AS rpfx, from_node AS partner_rpfx, SUM(n) AS n
            FROM read_parquet('{flow}')
            WHERE edge_type = 'TRANSFER'
            GROUP BY to_node, from_node
          ),
          outbound_pairs AS (
            SELECT from_node AS rpfx, to_node AS partner_rpfx, SUM(n) AS n
            FROM read_parquet('{flow}')
            WHERE edge_type = 'TRANSFER'
            GROUP BY from_node, to_node
          ),
          ranked AS (
            SELECT 'in' AS direction, rpfx, partner_rpfx, n,
                   ROW_NUMBER() OVER (PARTITION BY rpfx ORDER BY n DESC) AS ord
            FROM inbound_pairs
            UNION ALL
            SELECT 'out' AS direction, rpfx, partner_rpfx, n,
                   ROW_NUMBER() OVER (PARTITION BY rpfx ORDER BY n DESC) AS ord
            FROM outbound_pairs
          )
          SELECT direction, rpfx, ord, partner_rpfx, n
          FROM ranked
          WHERE ord <= 10
          ORDER BY rpfx, direction, ord
        ) TO '{out_partners.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)
        """
    )
    n_part = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_partners.as_posix()}')").fetchone()[0]
    print(f"  {out_partners.name}: {n_part:,} rows — {time.time()-t1:.1f}s")

    print(f"Total: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
