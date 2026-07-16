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
- Golf - Non-Runner Check needs `DG_API_KEY` for DataGolf field/schedule data.
- Golf - Non-Runner Check Slack notifications should use `GOLF_NR_SLACK_WEBHOOK_URL`, or `GOLF_NR_SLACK_BOT_TOKEN` plus `GOLF_NR_SLACK_CHANNEL`. If using bot token/channel, invite the bot to the target Slack channel and grant `chat:write`.
- Tennis integrity Slack notifications use `TENNIS_INTEGRITY_SLACK_WEBHOOK_URL`, falling back to `SLACK_WEBHOOK_URL` only if the tennis-specific value is missing.
- Betfair duplicate match and duplicate market Slack notifications use `DUPE_MATCH_SLACK_WEBHOOK_URL`, falling back to `SLACK_WEBHOOK_URL` only if the duplicate-specific value is missing.
- Betfair In-Play Start Checker uses `Slack_Webhook_TIP` exactly from the hub's server-side config/environment. It does not use `SLACK_WEBHOOK_URL`, Slack bot tokens, threads, or recovery messages.
- Other Slack integrations may use `SLACK_WEBHOOK_URL` or bot token/channel values, depending on the script.

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

## Decimal Cricket Fixture API

The Hub exposes cached Decimal cricket fixture JSON through a bearer-token protected API:

```text
GET /api/v1/cricket-fixtures/status
GET /api/v1/cricket-fixtures/all
GET /api/v1/cricket-fixtures/all/{event_id}
GET /api/v1/cricket-fixtures/today
GET /api/v1/cricket-fixtures/tomorrow
GET /api/v1/cricket-fixtures/YYYY-MM-DD
GET /api/v1/cricket-fixtures/YYYY-MM-DD/{event_id}
```

Set a long random `CRICKET_FIXTURE_API_KEY` in `.env`, then call an endpoint with:

```powershell
$headers = @{ Authorization = "Bearer YOUR_API_KEY" }
Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/v1/cricket-fixtures/today" -Headers $headers
```

The `/all` endpoint returns every fixture collected from Decimal's This Month and combined
Next Month and Beyond panels. It is cached in `runtime/output/decimal_cricket_fixtures_all.json` using
the same fixture structure as the daily responses. The full-date and all-fixtures endpoints
return their saved JSON payloads unchanged. Response headers include
`X-Fixture-Generated-At` and `X-Fixture-Data-Stale`. Missing files, invalid dates, and unknown
event IDs return structured JSON errors.

By default, the Hub refreshes today, tomorrow, and the combined all-upcoming file 30 seconds after
startup and every 180 minutes thereafter. The refresh opens both forward panels explicitly,
deduplicates their fixtures, and runs outside the web request. Each cache is
replaced atomically. Configure this with:

- `CRICKET_FIXTURE_REFRESH_ENABLED`
- `CRICKET_FIXTURE_REFRESH_INTERVAL_MINUTES`
- `CRICKET_FIXTURE_REFRESH_INITIAL_DELAY_SECONDS`
- `CRICKET_FIXTURE_REFRESH_TIMEOUT_SECONDS`
- `CRICKET_FIXTURE_API_MAX_AGE_MINUTES`

Automatic refresh requires `DECIMAL_USERNAME`, `DECIMAL_PASSWORD`, Chrome, and a working
ChromeDriver/Selenium Manager setup on the Hub server. Set `CRICKET_FIXTURE_REFRESH_ENABLED=false`
if another scheduler owns fixture generation.

## Runtime Files

Runtime logs and generated files are written under:

- `runtime/logs/`
- `runtime/output/`
- `runtime/secrets/` for temporary cert files materialized from B64 secrets

These paths are ignored by Git. Logs are overwritten at the start of each run.

## Safety

Never commit secrets, certificates, `.env`, real `credentials.json`, local JSON config, browser profiles, logs, or generated output files.

The tennis integrity scanner needs an operational `integrity_list.xlsx` file in `scripts/Integrity-Scanner/data/`. This file is intentionally not included in the clean Git-ready repo.
