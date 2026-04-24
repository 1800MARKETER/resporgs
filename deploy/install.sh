#!/bin/bash
# Resporgs.com — droplet bootstrap.
# Clones the repo, builds a Python venv, installs dependencies,
# writes systemd + nginx config, enables the service.
#
# Run once as root on the droplet:
#   curl -sSL https://raw.githubusercontent.com/1800MARKETER/resporgs/main/deploy/install.sh | bash
#
# Idempotent: safe to re-run after pulling new code.

set -euo pipefail

REPO_URL="https://github.com/1800MARKETER/resporgs"
APP_DIR="/var/www/resporgs"
APP_NAME="resporgs"
SERVICE_NAME="resporgs"
PORT=5178

echo ">> [1/5] Code + virtualenv"
if [ ! -d "$APP_DIR" ]; then
    cd /var/www && git clone "$REPO_URL" resporgs
else
    cd "$APP_DIR" && git pull --ff-only
fi

cd "$APP_DIR"
if [ ! -d venv ]; then
    python3 -m venv venv
fi
venv/bin/pip install --upgrade pip -q
venv/bin/pip install -r requirements.txt gunicorn -q

echo ">> [2/5] Data directories (populated separately via rsync)"
mkdir -p data cache sanity-export webapp/static/streetview clean
mkdir -p /var/www/local-prospector/data
chown -R www-data:www-data "$APP_DIR" /var/www/local-prospector

echo ">> [3/5] systemd unit"
cat > /etc/systemd/system/${SERVICE_NAME}.service << UNITEOF
[Unit]
Description=Resporgs.com Flask App (Gunicorn)
After=network.target

[Service]
User=www-data
Group=www-data
WorkingDirectory=${APP_DIR}/webapp
EnvironmentFile=-${APP_DIR}/apikey.env
ExecStart=${APP_DIR}/venv/bin/gunicorn --workers 2 --bind 127.0.0.1:${PORT} app:app
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNITEOF
systemctl daemon-reload

echo ">> [4/5] nginx site"
cat > /etc/nginx/sites-available/resporgs.com << NGINXEOF
server {
    listen 80;
    server_name resporgs.com www.resporgs.com;

    location /static/ {
        alias ${APP_DIR}/webapp/static/;
        expires 7d;
        access_log off;
    }

    location /assets/ {
        alias ${APP_DIR}/sanity-export/blog-export-2026-04-21t16-03-52-563z/;
        expires 7d;
        access_log off;
    }

    location / {
        proxy_pass http://127.0.0.1:${PORT};
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
NGINXEOF

ln -sf /etc/nginx/sites-available/resporgs.com /etc/nginx/sites-enabled/
nginx -t

echo ">> [5/5] Enable service (start deferred until data is synced)"
systemctl enable ${SERVICE_NAME} 2>&1 | grep -v "Created symlink" || true

echo ""
echo "================================================================"
echo "Done. Service is enabled but NOT yet started."
echo "Next steps:"
echo "  1. Rsync data up from your laptop (cache/, data/, sanity-export/,"
echo "     webapp/static/streetview/, clean/, apikey.env, and"
echo "     /var/www/local-prospector/data/master_vanity.db)."
echo "  2. systemctl start ${SERVICE_NAME}"
echo "  3. systemctl reload nginx"
echo "  4. certbot --nginx -d resporgs.com -d www.resporgs.com"
echo "================================================================"
