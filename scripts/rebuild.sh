#!/bin/bash
# Rebuild Resporgs.com derived data after a new Somos monthly snapshot lands.
#
# Two equally-valid rebuild paths exist (see plan.md section 11):
#   A. ON DROPLET (this script) — runs after `cache/YYYY-MM.parquet` is rsync'd
#      up. Requires ~4 GB of free RAM because build_events.py loads ~80 monthly
#      parquets and full-outer-joins consecutive pairs. Will refuse on a
#      memory-constrained host (see guard below).
#   B. LOCALLY — run each script individually on a workstation with plenty of
#      RAM, then scp the ~16 derived parquets in data/ to the droplet and
#      restart the service. This is the no-disruption option even on a fat
#      droplet, since rebuilds compete for CPU and memory with live traffic.
#
# Usage (on droplet, path A):
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

# Memory guard — build_events.py needs ~4 GB to FULL OUTER JOIN ~80 monthly
# parquets without OOM. The current droplet (1.9 GB, no swap) cannot do this;
# refuse with a useful message rather than die in the middle of step 1.
# Bumps the threshold up if the historical archive grows past ~120 months.
REQUIRED_MB=4096
if [ -r /proc/meminfo ]; then
    AVAIL_MB=$(awk '/MemAvailable:/ {print int($2/1024)}' /proc/meminfo)
    if [ "${AVAIL_MB:-0}" -lt "$REQUIRED_MB" ]; then
        cat <<EOF >&2
ERROR: Not enough memory to rebuild on this host.
  required: ~${REQUIRED_MB} MB available
  current : ${AVAIL_MB} MB available

build_events.py would be OOM-killed at step 1/11 against the current archive.

Use path B instead — rebuild on a workstation, then scp the derived parquets:
  1. Locally:  python scripts/build_events.py && python scripts/build_flow_graph.py
               && python scripts/enrich_profiles.py && ... (or run the
               equivalent build chain on a fat box with plenty of RAM)
  2. scp data/*.parquet root@<droplet>:/var/www/resporgs/data/
  3. ssh root@<droplet> 'systemctl restart resporgs'

See plan.md section 11 for the full hybrid path.
EOF
        exit 1
    fi
fi

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

echo ">> [8/10] rpfx snapshot (NPA/status/sub-codes)"
$VENV scripts/build_rpfx_snapshot.py

echo ">> [9/11] category trajectories"
$VENV scripts/build_category_trajectories.py

echo ">> [10/11] group trajectories"
$VENV scripts/build_group_trajectories.py

# NOTE: somos_weekly_*.parquet (the Pool section) are NOT rebuilt here.
# They have a weekly cadence driven by Bill's local /somos-weekly skill, and
# are rsync'd to the droplet directly. The droplet venv does not need
# pdfplumber.

echo ">> [11/11] restart service"
sudo -n systemctl restart resporgs 2>/dev/null || \
  echo "  (need root to restart — run: systemctl restart resporgs)"

echo ""
echo "=== Rebuild complete ==="
