# Ubuntu EC2 Deployment Guide

Repository: `https://github.com/jparsons1874work-ops/EXC-Scripts.git`

Target server folder: `/opt/betfair-scripts`

This guide assumes an Ubuntu EC2 instance running the FastAPI Betfair Scripts Hub with Uvicorn. Nginx and HTTPS should be added before wider team access.

## 1. EC2 Setup Assumptions

- Ubuntu Server EC2 instance.
- SSH access restricted as tightly as possible, ideally to your own IP.
- Initial app test uses TCP `8000`.
- Do not open HTTP/HTTPS publicly until the app works locally on the VM.
- For team access, put Nginx and HTTPS in front of the app.
- Secrets live only in `/opt/betfair-scripts/.env` on the server.

Security group guidance:

- SSH `22`: restrict to trusted source IPs.
- Temporary Uvicorn test `8000`: restrict to trusted source IPs only, then close it after Nginx is working.
- HTTP `80` and HTTPS `443`: open only when ready for Nginx/Certbot.

## 2. Install System Packages

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip curl ca-certificates
```

Selenium/Chrome scripts, including Cricket - Time Check Today/Tomorrow, need Google Chrome and headless browser dependencies:

```bash
sudo apt install -y \
  fonts-liberation libasound2 libatk-bridge2.0-0 libatk1.0-0 libcairo2 \
  libcups2 libdbus-1-3 libdrm2 libgbm1 libgtk-3-0 libnspr4 libnss3 \
  libpango-1.0-0 libu2f-udev libvulkan1 libx11-6 libxcb1 libxcomposite1 \
  libxdamage1 libxext6 libxfixes3 libxkbcommon0 libxrandr2 wget xdg-utils

wget -q -O /tmp/google-chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install -y /tmp/google-chrome.deb
google-chrome --version
```

The cricket time checker first looks for `google-chrome`, then uses a configured or system ChromeDriver if available. If there is no `chromedriver` on `PATH`, Selenium Manager will try to manage the driver. To verify browser discovery without logging into Betfair or Decimal:

```bash
cd /opt/betfair-scripts
/opt/betfair-scripts/.venv/bin/python scripts/exc-cric-time-check/betfair_decimal_time_checker.py --debug-browser
```

If Selenium Manager cannot fetch or start a driver on the server, install a matching ChromeDriver and set `CHROMEDRIVER_PATH` in `/opt/betfair-scripts/.env`.

Install Nginx only when you are ready for the reverse proxy:

```bash
sudo apt install -y nginx
```

Equivalent helper:

```bash
sudo bash deploy/install_system_packages.sh
```

Run that helper only after the repo exists on the server.

## 3. Clone and Set Up the App

```bash
sudo mkdir -p /opt/betfair-scripts
sudo chown "$USER":"$USER" /opt/betfair-scripts
git clone https://github.com/jparsons1874work-ops/EXC-Scripts.git /opt/betfair-scripts
cd /opt/betfair-scripts
python3 -m venv .venv
/opt/betfair-scripts/.venv/bin/python -m pip install --upgrade pip
/opt/betfair-scripts/.venv/bin/python -m pip install -r requirements.txt
```

Create server-only environment config:

```bash
cp .env.example .env
nano .env
```

At minimum set:

```bash
APP_PASSWORD=use-a-strong-shared-password
```

Set operational secrets as needed:

```bash
BETFAIR_USERNAME=
BETFAIR_PASSWORD=
BETFAIR_APP_KEY=
DECIMAL_USERNAME=
DECIMAL_PASSWORD=
SLACK_BOT_TOKEN=
SLACK_CHANNEL=
TENNIS_INTEGRITY_SLACK_WEBHOOK_URL=
DUPE_MATCH_SLACK_WEBHOOK_URL=
SLACK_WEBHOOK_URL=
```

Slack webhook routing:

- Tennis - Integrity Check uses `TENNIS_INTEGRITY_SLACK_WEBHOOK_URL`, with `SLACK_WEBHOOK_URL` only as a backwards-compatible fallback.
- Betfair - Duplicate Match Check uses `DUPE_MATCH_SLACK_WEBHOOK_URL`, with `SLACK_WEBHOOK_URL` only as a backwards-compatible fallback.
- Betfair - Duplicate Market Check uses `DUPE_MATCH_SLACK_WEBHOOK_URL` for Slack alerts, with `SLACK_WEBHOOK_URL` only as a backwards-compatible fallback.
- Keep the Tennis Integrity and Duplicate Matches webhook values separate so alerts do not cross channels.

Betfair certificate options:

- File path option: upload cert/key files somewhere outside Git, then set `BETFAIR_CERT_FILE` and `BETFAIR_KEY_FILE`.
- B64 option: set `BETFAIR_CERT_B64` and `BETFAIR_KEY_B64`; the app writes temporary runtime cert files under ignored `runtime/secrets/`.

Example file path values:

```bash
BETFAIR_CERT_FILE=/opt/betfair-scripts/runtime/secrets/client-2048.crt
BETFAIR_KEY_FILE=/opt/betfair-scripts/runtime/secrets/client-2048.key
```

Do not commit `.env`, cert files, local JSON credentials, logs, output files, or browser profiles.

## 4. Manual Uvicorn Test

```bash
cd /opt/betfair-scripts
/opt/betfair-scripts/.venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Then test:

```bash
curl http://127.0.0.1:8000/health
```

From your browser, test `http://EC2_PUBLIC_IP:8000` only if the security group allows your IP.

Stop the manual command with `Ctrl+C`.

## 5. systemd Service

Example service file is provided at:

```text
deploy/betfair-scripts.service.example
```

Install it manually:

```bash
sudo cp deploy/betfair-scripts.service.example /etc/systemd/system/betfair-scripts.service
sudo systemctl daemon-reload
sudo systemctl enable betfair-scripts
sudo systemctl start betfair-scripts
sudo systemctl status betfair-scripts
```

Or use the helper after reviewing it:

```bash
sudo bash deploy/create_systemd_service.sh
```

The service:

- Uses working directory `/opt/betfair-scripts`.
- Loads environment from `/opt/betfair-scripts/.env`.
- Runs Uvicorn on `127.0.0.1:8000`.
- Restarts on failure.

Check logs:

```bash
journalctl -u betfair-scripts -f
```

## 6. Nginx Reverse Proxy

Install Nginx when the app works:

```bash
sudo apt install -y nginx
```

Example config is provided at:

```text
deploy/nginx_example.conf
```

Install it after replacing `YOUR_DOMAIN_OR_PUBLIC_IP`:

```bash
sudo cp deploy/nginx_example.conf /etc/nginx/sites-available/betfair-scripts
sudo nano /etc/nginx/sites-available/betfair-scripts
sudo ln -s /etc/nginx/sites-available/betfair-scripts /etc/nginx/sites-enabled/betfair-scripts
sudo nginx -t
sudo systemctl reload nginx
```

After Nginx is working, close direct inbound access to port `8000` in the EC2 security group.

## 7. HTTPS with Certbot

After DNS points to the EC2 instance and Nginx works on port `80`:

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d YOUR_DOMAIN
sudo certbot renew --dry-run
```

Keep `YOUR_DOMAIN` as a placeholder until you have the real DNS name.

## 8. Updating from Git

Use this safe update sequence:

```bash
cd /opt/betfair-scripts
sudo systemctl stop betfair-scripts
git status --short
git pull origin main
/opt/betfair-scripts/.venv/bin/python -m pip install -r requirements.txt
/opt/betfair-scripts/.venv/bin/python -m py_compile app/main.py
sudo systemctl start betfair-scripts
sudo systemctl status betfair-scripts
```

Follow logs:

```bash
journalctl -u betfair-scripts -f
```

## 9. Logs and Debugging

App health:

```bash
curl http://127.0.0.1:8000/health
```

Service status:

```bash
sudo systemctl status betfair-scripts
journalctl -u betfair-scripts -f
```

Nginx logs:

```bash
sudo tail -f /var/log/nginx/access.log
sudo tail -f /var/log/nginx/error.log
```

Runtime folders:

```bash
ls -la /opt/betfair-scripts/runtime/logs
ls -la /opt/betfair-scripts/runtime/output
```

Runtime files are ignored by Git.

## 10. Security Warnings

- Do not commit `.env`.
- Do not commit Betfair cert/key files.
- Do not commit real `credentials.json` or `*.local.json`.
- Do not commit runtime logs/output, browser profiles, or downloaded files.
- Use a strong shared `APP_PASSWORD`.
- Add Nginx and HTTPS before team access.
- Restrict EC2 security group source IPs where possible.
- Keep direct Uvicorn port `8000` private once Nginx is configured.

## 11. Windows Local Command Reference

For local Windows checks:

```powershell
cd C:\BetfairScripts
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```
