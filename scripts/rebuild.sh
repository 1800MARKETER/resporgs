#!/bin/bash
# Rebuild Resporgs.com derived data after a new Somos monthly snapshot lands.
#
# Usage (on droplet):
#   sudo -u www-data HOME=/var/www/resporgs /var/www/resporgs/scripts/rebuild.sh
#
# Prerequisites:
#   - The raw monthly parquet must already be in /var/www/resporgs/cache/YYYY-MM.parquet
#     (run cache_months.py separately when uploading from laptop)
#
# What this script does (idempotent — rerun anytime):
#   1. Rebuild the event pipeline (pair_totals + resporg_month)
#   2. Rebuild the flow graph
#   3. Rebuild the enrichment (MM match %, age buckets)
#   4. Rebuild disconnect episodes
#   5. Rebuild precomputed ranks
#   6. Rebuild precomputed vanity holdings (per-rpfx — SLOW, 15-20 min)
#   7. Rebuild precomputed flow summaries
#   8. Restart the Flask service

set -euo pipefail

cd "$(dirname "$0")/.."
VENV=./venv/bin/python

echo ">> [1/8] events (pair_totals + resporg_month)"
$VENV scripts/build_events.py

echo ">> [2/8] flow graph"
$VENV scripts/build_flow_graph.py

echo ">> [3/8] enrichment"
$VENV scripts/enrich_profiles.py

echo ">> [4/8] disconnect episodes"
$VENV scripts/build_disconnect_episodes.py

echo ">> [5/8] ranks"
$VENV scripts/build_ranks.py

echo ">> [6/8] vanity precompute (15-20 min)"
$VENV scripts/build_vanity_precompute.py

echo ">> [7/9] flow summaries"
$VENV scripts/build_flow_precompute.py

echo ">> [8/9] rpfx snapshot (NPA/status/sub-codes)"
$VENV scripts/build_rpfx_snapshot.py

echo ">> [9/9] restart service"
sudo -n systemctl restart resporgs 2>/dev/null || \
  echo "  (need root to restart — run: systemctl restart resporgs)"

echo ""
echo "=== Rebuild complete ==="
