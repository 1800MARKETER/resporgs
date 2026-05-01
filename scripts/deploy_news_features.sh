#!/bin/bash
# Deploy script for the resporgs.com news features (2026-04-28 → 2026-04-30 work).
# Run on the droplet at 104.131.76.98 to pull all new code + data live.
#
# Brings live:
#   - /news aggregated industry-news page
#   - "About {company}" + "Recent news" sections on /r/<rpfx> profile pages
#   - 310 resporgs with AI-enriched overviews + recent-news arrays
#   - Article post drafts in Sanity (ready for Bill to publish from Studio)

set -euo pipefail

REPO_DIR="${REPO_DIR:-/var/www/resporgs.com}"
SERVICE_NAME="${SERVICE_NAME:-resporgs}"

cd "$REPO_DIR"

echo "==> Pulling latest code from origin..."
git fetch --all
git pull --ff-only

echo "==> Refreshing Sanity content (resporg news + post fields)..."
python scripts/fetch_sanity_docs.py

echo "==> Restarting webapp service..."
sudo systemctl restart "$SERVICE_NAME"

echo "==> Health check..."
sleep 2
if curl -s -o /dev/null -w "%{http_code}" "http://localhost:5181/news" | grep -q "200"; then
  echo "    /news returned 200 OK"
else
  echo "    WARN: /news did not return 200 — check 'sudo systemctl status $SERVICE_NAME'"
  exit 1
fi

echo "==> Done."
echo "    Live: https://resporgs.com/news"
echo "    Live: https://resporgs.com/r/NX  (Verizon — sample profile with news)"
