# Ubuntu EC2 Deployment Notes

Target server folder: `/opt/betfair-scripts`

## Install System Packages

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git curl
```

Selenium/Chrome scripts may also need browser packages:

```bash
sudo apt install -y chromium-browser chromium-chromedriver
```

Package names vary by Ubuntu image. If Chromium packages are unavailable, install Google Chrome stable and ensure a matching ChromeDriver is on `PATH`.

## Clone and Install

```bash
sudo mkdir -p /opt/betfair-scripts
sudo chown "$USER":"$USER" /opt/betfair-scripts
git clone https://github.com/jparsons1874work-ops/EXC-Scripts.git /opt/betfair-scripts
cd /opt/betfair-scripts
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Configure Environment

```bash
cp .env.example .env
nano .env
```

Add real values only on the server. Do not commit `.env`, cert files, local JSON credentials, or generated runtime files.

Betfair certificate options:

- Set `BETFAIR_CERT_FILE` and `BETFAIR_KEY_FILE` to server-local file paths.
- Or set `BETFAIR_CERT_B64` and `BETFAIR_KEY_B64`; the app writes temporary files under ignored `runtime/secrets/`.

## Run Manually

```bash
cd /opt/betfair-scripts
source .venv/bin/activate
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open the EC2 security group only to the required source IPs and port. For first testing this is usually TCP `8000`; for production, prefer Nginx with HTTPS on `443` and keep direct app ports private.

## systemd Service Example

Create `/etc/systemd/system/betfair-scripts.service`:

```ini
[Unit]
Description=Betfair Scripts Hub
After=network.target

[Service]
WorkingDirectory=/opt/betfair-scripts
Environment=PATH=/opt/betfair-scripts/.venv/bin
ExecStart=/opt/betfair-scripts/.venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=5
User=ubuntu

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable betfair-scripts
sudo systemctl start betfair-scripts
sudo systemctl status betfair-scripts
```

## Nginx and HTTPS

Use Nginx as a reverse proxy to `127.0.0.1:8000`, then add HTTPS with Certbot or put the app behind an AWS load balancer. Keep the app password enabled for the first version.

Example Nginx location:

```nginx
location / {
    proxy_pass http://127.0.0.1:8000;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

## Run Windows

Long-running scripts use a simple in-app run window, currently `07:00` to `23:00` in `Europe/London`. The app blocks starts outside the window and attempts to stop a running long-running script when the window ends. A fuller scheduler can be added later if needed.
