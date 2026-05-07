from __future__ import annotations

import base64
import os
from pathlib import Path

from dotenv import load_dotenv


APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent
SCRIPTS_ROOT = PROJECT_ROOT / "scripts"
RUNTIME_DIR = PROJECT_ROOT / "runtime"
LOG_DIR = RUNTIME_DIR / "logs"
OUTPUT_DIR = RUNTIME_DIR / "output"
SECRET_RUNTIME_DIR = RUNTIME_DIR / "secrets"
STATIC_IMG_DIR = APP_DIR / "static" / "img"

SECRET_KEYS = (
    "BETFAIR_USERNAME",
    "BETFAIR_PASSWORD",
    "BETFAIR_APP_KEY",
    "BETFAIR_CERT_FILE",
    "BETFAIR_CERT_B64",
    "BETFAIR_KEY_FILE",
    "BETFAIR_KEY_B64",
    "DECIMAL_USERNAME",
    "DECIMAL_PASSWORD",
    "SLACK_BOT_TOKEN",
    "SLACK_CHANNEL",
    "TENNIS_INTEGRITY_SLACK_WEBHOOK_URL",
    "DUPE_MATCH_SLACK_WEBHOOK_URL",
    "SLACK_WEBHOOK_URL",
    "DG_API_KEY",
)


load_dotenv(PROJECT_ROOT / ".env")


def ensure_runtime_dirs() -> None:
    for path in (LOG_DIR, OUTPUT_DIR, SECRET_RUNTIME_DIR):
        path.mkdir(parents=True, exist_ok=True)


def get_setting(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def app_password() -> str:
    return get_setting("APP_PASSWORD")


def session_secret() -> str:
    return get_setting("SESSION_SECRET") or app_password() or "local-dev-session-secret"


def materialize_b64_secret(secret_name: str, file_name: str) -> str:
    encoded = get_setting(secret_name)
    if not encoded:
        return ""
    ensure_runtime_dirs()
    target = SECRET_RUNTIME_DIR / file_name
    target.write_bytes(base64.b64decode(encoded))
    return str(target)


def child_environment() -> dict[str, str]:
    env = os.environ.copy()
    for key in SECRET_KEYS:
        value = get_setting(key)
        if value:
            env[key] = value

    cert_file = env.get("BETFAIR_CERT_FILE") or materialize_b64_secret("BETFAIR_CERT_B64", "client-2048.crt")
    key_file = env.get("BETFAIR_KEY_FILE") or materialize_b64_secret("BETFAIR_KEY_B64", "client-2048.key")
    if cert_file:
        env["BETFAIR_CERT_FILE"] = cert_file
        env.setdefault("BETFAIR_CERTS_DIR", str(Path(cert_file).parent))
        env.setdefault("BF_CERTS_DIR", str(Path(cert_file).parent))
    if key_file:
        env["BETFAIR_KEY_FILE"] = key_file

    env.setdefault("SCRIPT_OUTPUT_DIR", str(OUTPUT_DIR))
    env.setdefault("CHROME_PROFILE_DIR", str(OUTPUT_DIR / "chrome_profile"))
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env


def branding_assets() -> dict[str, str]:
    """Return approved static branding asset URLs for files that exist locally."""
    assets: dict[str, str] = {}
    candidates = {
        "logo": ("betfair-logo.svg", "betfair-logo.png"),
        "arrows": ("betfair-arrows.svg", "betfair-arrows.png"),
        "hero_bg": ("hero-bg.png",),
        "favicon": ("favicon.ico", "favicon.png"),
    }
    for key, names in candidates.items():
        for name in names:
            if (STATIC_IMG_DIR / name).exists():
                assets[key] = f"/static/img/{name}"
                break
    return assets
