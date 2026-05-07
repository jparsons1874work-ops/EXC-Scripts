#!/usr/bin/env bash
set -euo pipefail

SERVICE_PATH="/etc/systemd/system/betfair-scripts.service"

cat > "$SERVICE_PATH" <<'SERVICE'
[Unit]
Description=Betfair Scripts Hub
After=network.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/opt/betfair-scripts
EnvironmentFile=/opt/betfair-scripts/.env
Environment=HOME=/home/ubuntu
Environment=USER=ubuntu
Environment=PATH=/opt/betfair-scripts/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
Environment=CHROME_BINARY=/usr/bin/google-chrome
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/betfair-scripts/.venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable betfair-scripts
systemctl restart betfair-scripts
systemctl status betfair-scripts --no-pager
