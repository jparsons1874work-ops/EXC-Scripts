# Betfair Scripts Hub

FastAPI web app for running operational Betfair, Decimal, SAMM, golf, cricket, and tennis integrity scripts from one protected interface.

The app uses:

- FastAPI and Jinja2 templates
- HTMX polling for live status/output
- Vanilla CSS
- Environment variables or `.env` for configuration
- Relative paths from the project root
- `sys.executable` when launching scripts

The old Streamlit entry point has been moved to `legacy_streamlit_app.py` and is not part of the supported deployment path.

## Local Windows Setup

From `C:\BetfairScripts`:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Create local configuration:

```powershell
Copy-Item .env.example .env
```

Edit `.env` with real values. Do not commit `.env`.

Required values depend on which scripts you run:

- `APP_PASSWORD` protects the app. If absent, local dev is allowed with a visible warning.
- Betfair scripts need `BETFAIR_USERNAME`, `BETFAIR_PASSWORD`, `BETFAIR_APP_KEY`, and cert/key file paths or B64 cert values.
- Decimal scripts need `DECIMAL_USERNAME` and `DECIMAL_PASSWORD`.
- Slack notifications need `SLACK_WEBHOOK_URL` or bot token/channel values, depending on the script.

## Run Locally

```powershell
cd C:\BetfairScripts
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Using the detected Python directly:

```powershell
& "C:\Users\jpa18\AppData\Local\Python\bin\python.exe" -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000`.

## Runtime Files

Runtime logs and generated files are written under:

- `runtime/logs/`
- `runtime/output/`
- `runtime/secrets/` for temporary cert files materialized from B64 secrets

These paths are ignored by Git. Logs are overwritten at the start of each run.

## Safety

Never commit secrets, certificates, `.env`, real `credentials.json`, local JSON config, browser profiles, logs, or generated output files.

The tennis integrity scanner needs an operational `integrity_list.xlsx` file in `scripts/Integrity-Scanner/data/`. This file is intentionally not included in the clean Git-ready repo.
