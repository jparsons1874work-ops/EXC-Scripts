#!/usr/bin/env python3
"""Compare Betfair and Decimal cricket fixture start times as TSV."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import ssl
import subprocess
import sys
import traceback
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from difflib import SequenceMatcher
from time import perf_counter
from typing import Iterable, Optional
from urllib.parse import urlencode

import pandas as pd
from betfairlightweight import APIClient
from betfairlightweight.filters import market_filter, price_projection
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from zoneinfo import ZoneInfo

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CERTS_DIR = os.getenv(
    "BETFAIR_CERTS_DIR",
    os.path.join(os.path.dirname(SCRIPT_DIR), "Integrity-Scanner", "certs"),
)
BETFAIR_USERNAME = os.getenv("BETFAIR_USERNAME", "")
BETFAIR_PASSWORD = os.getenv("BETFAIR_PASSWORD", "")
BETFAIR_APP_KEY = os.getenv("BETFAIR_APP_KEY", "")
BETFAIR_CERT_FILE = os.getenv("BETFAIR_CERT_FILE", os.path.join(DEFAULT_CERTS_DIR, "client-2048.crt"))
BETFAIR_KEY_FILE = os.getenv("BETFAIR_KEY_FILE", os.path.join(DEFAULT_CERTS_DIR, "client-2048.key"))

DECIMAL_USERNAME = os.getenv("DECIMAL_USERNAME", "")
DECIMAL_PASSWORD = os.getenv("DECIMAL_PASSWORD", "")

SIMILARITY_THRESHOLD = 0.80

# Decimal site settings. Adjust these to match the live site markup.
DECIMAL_LOGIN_URL = "https://www.decimalcricket.net/login"
DECIMAL_DEC_URL = "https://www.decimalcricket.net/dec"
DECIMAL_USERNAME_SELECTORS = [
    (By.CSS_SELECTOR, "input[type='email']"),
    (By.CSS_SELECTOR, "input[name='email']"),
    (By.CSS_SELECTOR, "input[name='username']"),
    (By.CSS_SELECTOR, "input[type='text']"),
]
DECIMAL_PASSWORD_SELECTORS = [
    (By.CSS_SELECTOR, "input[type='password']"),
    (By.CSS_SELECTOR, "input[name='password']"),
]
DECIMAL_SUBMIT_SELECTORS = [
    (By.CSS_SELECTOR, "button[type='submit']"),
    (By.CSS_SELECTOR, "input[type='submit']"),
    (By.XPATH, "//button[contains(., 'Login') or contains(., 'Log in') or contains(., 'Sign in')]"),
    (By.XPATH, "//button"),
]
DECIMAL_VIEWERS_SELECTORS = [
    (
        By.XPATH,
        "//*[self::a or self::button or self::div or self::span or self::li]"
        "[contains(translate(normalize-space(.), 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'VIEWERS')]",
    ),
    (By.CSS_SELECTOR, "[href*='viewer' i]"),
]
DECIMAL_LEGACY_DEVELOPER_SELECTORS = [
    (
        By.XPATH,
        "//*[self::a or self::button or self::div or self::span or self::li]"
        "[contains(translate(normalize-space(.), 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'LEGACY DEVELOPER')]",
    ),
    (
        By.XPATH,
        "//*[self::a or self::button or self::div or self::span or self::li]"
        "[contains(translate(normalize-space(.), 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'LEGACY')"
        " and contains(translate(normalize-space(.), 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'DEVELOPER')]",
    ),
    (By.CSS_SELECTOR, "[href*='legacy' i]"),
]
DECIMAL_LEGACY_ROW_SELECTOR = "a.list-group-item.list-group-item-action[data-id]"
DECIMAL_LEGACY_TODAY_ROW_SELECTORS = (
    f"#Today_container {DECIMAL_LEGACY_ROW_SELECTOR}",
    f"#Today {DECIMAL_LEGACY_ROW_SELECTOR}",
)
DECIMAL_LEGACY_IN_PLAY_ROW_SELECTORS = (
    f"#running_container {DECIMAL_LEGACY_ROW_SELECTOR}",
    f"#running {DECIMAL_LEGACY_ROW_SELECTOR}",
)
DECIMAL_LEGACY_READY_ROOT_SELECTORS = (
    "div.accordion#date",
    "#Today_container",
    "#running_container",
    "#Today",
    "#running",
)
DECIMAL_HEADLESS = True
DECIMAL_WAIT_SECONDS = 8
DECIMAL_QUICK_WAIT_SECONDS = 2
CHROME_BINARY_CANDIDATES = (
    "google-chrome",
    "google-chrome-stable",
    "chromium",
    "chromium-browser",
    "chrome",
)
CHROMEDRIVER_CANDIDATES = ("chromedriver",)
SUPPORTED_LINUX_MACHINES = {"x86_64", "amd64"}

UK_TZ = ZoneInfo("Europe/London")
UTC_TZ = ZoneInfo("UTC")
BETFAIR_CRICKET_EVENT_TYPE_ID = "4"
BETFAIR_MARKET_TYPE = "MATCH_ODDS"
BETFAIR_CERT_LOGIN_URL = "https://identitysso-cert.betfair.com/api/certlogin"
TIME_OUTPUT_FORMAT = "%Y-%m-%d %H:%M %Z"
PLACEHOLDER_PREFIXES = ("YOUR_", r"C:\path\to")
COMMON_WORDS = {
    "match",
    "odds",
    "the",
    "cricket",
}


@dataclass(frozen=True)
class Fixture:
    """Normalized fixture row used for matching and reporting."""

    match_name: str
    competition: str
    start_time: datetime
    source: str
    event_id: Optional[str] = None


@dataclass(frozen=True)
class MatchResult:
    """Best Decimal match for a Betfair fixture."""

    betfair_fixture: Fixture
    decimal_fixture: Optional[Fixture]
    score: float
    time_matches: bool


class DecimalScrapeError(RuntimeError):
    """Raised when Decimal fixtures cannot be scraped reliably."""


def validate_config() -> None:
    """Raise a friendly error if any required placeholder values remain."""
    config_values = {
        "BETFAIR_USERNAME": BETFAIR_USERNAME,
        "BETFAIR_PASSWORD": BETFAIR_PASSWORD,
        "BETFAIR_APP_KEY": BETFAIR_APP_KEY,
        "BETFAIR_CERT_FILE": BETFAIR_CERT_FILE,
        "BETFAIR_KEY_FILE": BETFAIR_KEY_FILE,
        "DECIMAL_USERNAME": DECIMAL_USERNAME,
        "DECIMAL_PASSWORD": DECIMAL_PASSWORD,
    }

    missing = [
        name
        for name, value in config_values.items()
        if not value or any(str(value).startswith(prefix) for prefix in PLACEHOLDER_PREFIXES)
    ]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(
            f"Missing required environment variables or cert files: {joined}"
        )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Compare Betfair Exchange and Decimal cricket fixture times."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--today", action="store_true", help="Check fixtures for today in UK time.")
    group.add_argument(
        "--tomorrow", action="store_true", help="Check fixtures for tomorrow in UK time."
    )
    parser.add_argument("--verbose", action="store_true", help="Write progress messages to stderr.")
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Write aligned columns for terminal viewing instead of TSV.",
    )
    parser.add_argument(
        "--debug-browser",
        "--print-browser-env",
        action="store_true",
        help="Print Chrome, ChromeDriver, OS, and architecture diagnostics without logging into Betfair or Decimal.",
    )
    args = parser.parse_args()
    if not args.debug_browser and not args.today and not args.tomorrow:
        parser.error("one of --today or --tomorrow is required unless --debug-browser is used")
    return args


def verbose_log(enabled: bool, message: str) -> None:
    """Write a diagnostic message to stderr when verbose mode is enabled."""
    if enabled:
        print(message, file=sys.stderr)


def run_command_text(command: list[str], timeout: int = 5) -> str:
    """Run a diagnostic command and return one line of output without raising."""
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except Exception as exc:
        return f"unavailable ({type(exc).__name__}: {exc})"

    output = (completed.stdout or completed.stderr or "").strip()
    if not output:
        return f"unavailable (exit code {completed.returncode})"
    return output.splitlines()[0]


def find_executable(candidates: Iterable[str]) -> str:
    """Return the first executable found on PATH."""
    for candidate in candidates:
        path = shutil.which(candidate)
        if path:
            return path
    return ""


def detect_chrome_binary() -> str:
    """Return the configured or discovered Chrome/Chromium binary path."""
    configured = os.getenv("CHROME_BINARY") or os.getenv("GOOGLE_CHROME_BIN") or os.getenv("CHROME_BIN")
    if configured:
        configured = configured.strip()
        if os.path.isfile(configured) or shutil.which(configured):
            return configured
        raise RuntimeError("Google Chrome is not installed or not found. Install Chrome on the server.")
    detected = find_executable(CHROME_BINARY_CANDIDATES)
    if detected:
        return detected
    if platform.system().lower() == "windows":
        windows_candidates = [
            os.path.join(os.getenv("PROGRAMFILES", ""), "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(os.getenv("PROGRAMFILES(X86)", ""), "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(os.getenv("LOCALAPPDATA", ""), "Google", "Chrome", "Application", "chrome.exe"),
        ]
        for candidate in windows_candidates:
            if candidate and os.path.isfile(candidate):
                return candidate
    raise RuntimeError("Google Chrome is not installed or not found. Install Chrome on the server.")


def detect_chromedriver_binary() -> str:
    """Return configured/system ChromeDriver path, or an empty string for Selenium Manager."""
    configured = os.getenv("CHROMEDRIVER_PATH", "").strip()
    if configured:
        if os.path.isfile(configured) or shutil.which(configured):
            return configured
        raise RuntimeError(f"CHROMEDRIVER_PATH is set but not a file or executable on PATH: {configured}")
    return find_executable(CHROMEDRIVER_CANDIDATES)


def chrome_version(chrome_binary: str) -> str:
    """Return detected Chrome version text."""
    if platform.system().lower() == "windows" and os.path.isfile(chrome_binary):
        escaped_path = chrome_binary.replace("'", "''")
        version = run_command_text(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"(Get-Item -LiteralPath '{escaped_path}').VersionInfo.ProductVersion",
            ]
        )
        if not version.startswith("unavailable"):
            return version
    return run_command_text([chrome_binary, "--version"])


def browser_diagnostics(chrome_binary: str = "", chromedriver_binary: str = "") -> dict[str, str]:
    """Collect non-secret browser and platform diagnostics."""
    if not chrome_binary:
        try:
            chrome_binary = detect_chrome_binary()
        except Exception as exc:
            chrome_binary = f"not found ({exc})"
    if not chromedriver_binary:
        try:
            chromedriver_binary = detect_chromedriver_binary() or "not found; Selenium Manager will be used"
        except Exception as exc:
            chromedriver_binary = f"not found ({exc})"

    version = ""
    if chrome_binary and not chrome_binary.startswith("not found"):
        version = chrome_version(chrome_binary)

    return {
        "os": platform.platform(),
        "system": platform.system(),
        "machine": platform.machine() or "unknown",
        "python": sys.version.replace("\n", " "),
        "chrome_binary": chrome_binary or "not found",
        "chrome_version": version or "unknown",
        "chromedriver_binary": chromedriver_binary or "not found; Selenium Manager will be used",
    }


def format_browser_diagnostics(diagnostics: dict[str, str]) -> str:
    """Format browser diagnostics for stderr/stdout."""
    return "\n".join(f"{key}: {value}" for key, value in diagnostics.items())


def print_browser_diagnostics() -> None:
    """Print browser diagnostics without logging into Betfair or Decimal."""
    print(format_browser_diagnostics(browser_diagnostics()))


def requested_day(args: argparse.Namespace) -> date:
    """Return the UK-local target day."""
    now_uk = datetime.now(UK_TZ)
    if args.today:
        return now_uk.date()
    return (now_uk + timedelta(days=1)).date()


def day_bounds(day: date) -> tuple[datetime, datetime]:
    """Return inclusive start and exclusive end datetimes in UK time."""
    start = datetime.combine(day, time.min, tzinfo=UK_TZ)
    end = start + timedelta(days=1)
    return start, end


def normalize_whitespace(value: str) -> str:
    """Collapse repeated whitespace."""
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_name(value: str) -> str:
    """Normalize fixture names for fuzzy matching."""
    cleaned = normalize_whitespace(value).lower()
    cleaned = cleaned.replace("&", " and ")

    replacements = [
        (r"\b(?:under[\s-]*19|u[\s-]*19)\b", "u19"),
        (r"\b(?:under[\s-]*17|u[\s-]*17)\b", "u17"),
        (r"\b(?:under[\s-]*21|u[\s-]*21)\b", "u21"),
        (r"\b(?:women|woman|ladies)\b", "women"),
        (r"\bw\b", "women"),
        (r"\bmen\b", "men"),
        (r"\bm\b", "men"),
        (r"\b(?:versus|vs|v)\b", "vs"),
    ]
    for pattern, replacement in replacements:
        cleaned = re.sub(pattern, replacement, cleaned)

    cleaned = re.sub(r"[^\w\s]", " ", cleaned)
    cleaned = normalize_whitespace(cleaned)

    # "South Africa W v India W" -> "south africa women vs india women"
    # "South Africa Women v India Women" -> "south africa women vs india women"
    # "Pakistan U19 vs India Under 19" -> "pakistan u19 vs india u19"
    parts = [part for part in cleaned.split() if part and part not in COMMON_WORDS]
    return " ".join(parts)


def normalize_competition(value: str) -> str:
    """Normalize competition names for fuzzy matching."""
    return normalize_name(value)


def strip_decimal_uk_timezone_suffix(value: str) -> tuple[str, Optional[str]]:
    """Strip a trailing UK timezone abbreviation and return the cleaned text plus token."""
    match = re.search(r"\s+(BST|GMT)\b", value, flags=re.IGNORECASE)
    if not match:
        return value, None
    cleaned = normalize_whitespace(re.sub(r"\s+(BST|GMT)\b", "", value, flags=re.IGNORECASE))
    return cleaned, match.group(1).upper()


def as_uk_local_decimal_datetime(parsed: datetime, target_day: date) -> datetime:
    """Attach the requested day and UK timezone without applying a second time shift."""
    # Decimal wall-clock times should stay exactly as shown on the site:
    # "20:00" on a BST date -> 2026-04-18 20:00 BST
    # "20:00 BST" -> 2026-04-18 20:00 BST
    # "20:00 GMT" on a GMT date -> 2026-12-18 20:00 GMT
    if parsed.year == 1900:
        parsed = datetime.combine(target_day, parsed.time())
    return parsed.replace(tzinfo=UK_TZ)


DECIMAL_PARSE_DEBUG = False
DECIMAL_ISO_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
)


def try_parse_decimal_iso_datetime(value: str) -> Optional[datetime]:
    """Parse ISO-like Decimal timestamps without hitting the generic pandas path."""
    candidate = normalize_whitespace(value)
    if not candidate or not DECIMAL_ISO_DATETIME_RE.fullmatch(candidate):
        return None

    normalized = candidate
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    elif re.search(r"[+-]\d{4}$", normalized):
        normalized = f"{normalized[:-5]}{normalized[-5:-2]}:{normalized[-2:]}"

    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def parse_decimal_datetime(value: str, target_day: date, verbose: bool = False) -> Optional[datetime]:
    """Parse a Decimal date/time string into a UK-local datetime."""
    raw = normalize_whitespace(value)
    if not raw:
        verbose_log(verbose, "Decimal time parse failed: empty value")
        return None

    parse_text, explicit_uk_tz = strip_decimal_uk_timezone_suffix(raw)
    debug_enabled = verbose and DECIMAL_PARSE_DEBUG
    if debug_enabled:
        verbose_log(verbose, f"Decimal time parse raw text: {raw!r}")
        if explicit_uk_tz:
            verbose_log(
                verbose,
                "Decimal time parse branch: explicit UK timezone suffix "
                f"{explicit_uk_tz} treated as UK wall-clock time",
            )
        else:
            verbose_log(verbose, "Decimal time parse branch: no explicit UK timezone suffix")

    parsed_iso = try_parse_decimal_iso_datetime(parse_text)
    if parsed_iso is not None:
        if parsed_iso.tzinfo is None:
            final_dt = as_uk_local_decimal_datetime(parsed_iso, target_day)
        else:
            final_dt = parsed_iso.astimezone(UK_TZ)
        if debug_enabled:
            verbose_log(verbose, "Decimal time parse branch: ISO-like timestamp")
            verbose_log(verbose, f"Decimal time parse final datetime: {final_dt.isoformat()}")
        return final_dt

    formats = [
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d %b %Y %H:%M",
        "%d %B %Y %H:%M",
        "%a %d %b %Y %H:%M",
        "%A %d %B %Y %H:%M",
        "%H:%M",
        "%I:%M %p",
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(parse_text, fmt)
        except ValueError:
            continue
        final_dt = as_uk_local_decimal_datetime(parsed, target_day)
        if debug_enabled:
            verbose_log(verbose, f"Decimal time parse branch: strptime format {fmt}")
            verbose_log(verbose, f"Decimal time parse final datetime: {final_dt.isoformat()}")
        return final_dt

    parsed_generic = pd.to_datetime(parse_text, errors="coerce", dayfirst=True)
    if pd.isna(parsed_generic):
        verbose_log(verbose, f"Decimal time parse failed for {raw!r}: pandas parse failed")
        return None

    parsed_dt = parsed_generic.to_pydatetime()
    if parsed_dt.tzinfo is None:
        final_dt = as_uk_local_decimal_datetime(parsed_dt, target_day)
        if debug_enabled:
            verbose_log(verbose, "Decimal time parse branch: pandas naive -> UK wall-clock")
            verbose_log(verbose, f"Decimal time parse final datetime: {final_dt.isoformat()}")
        return final_dt

    final_dt = parsed_dt.astimezone(UK_TZ)
    if debug_enabled:
        verbose_log(
            verbose,
            "Decimal time parse branch: pandas timezone-aware -> converted to UK "
            f"from {parsed_dt.tzinfo}",
        )
        verbose_log(verbose, f"Decimal time parse final datetime: {final_dt.isoformat()}")
    return final_dt


def filter_decimal_dataframe_for_target_day(
    dataframe: pd.DataFrame, target_day: date, verbose: bool
) -> pd.DataFrame:
    """Filter Decimal rows to the requested day before row parsing."""
    before_count = len(dataframe)
    verbose_log(verbose, f"Decimal rows before target-day filter: {before_count}")

    start_column = next(
        (column for column in dataframe.columns if str(column).strip().lower() == "start"),
        None,
    )
    if start_column is None:
        verbose_log(verbose, f"Decimal rows after target-day filter: {before_count}")
        return dataframe

    target_prefix = target_day.strftime("%d/%m/%Y")
    start_values = dataframe[start_column].astype(str).str.strip()
    filtered = dataframe[start_values.str.startswith(target_prefix, na=False)].copy()
    verbose_log(verbose, f"Decimal rows after target-day filter: {len(filtered)}")
    return filtered


def build_decimal_fixtures_fast_path(
    dataframe: pd.DataFrame,
    target_day: date,
    verbose: bool,
) -> Optional[list[Fixture]]:
    """Build Decimal fixtures directly when the expected columns are present."""
    column_map = {str(column).strip().lower(): column for column in dataframe.columns}
    start_column = column_map.get("start")
    name_column = column_map.get("name")
    if start_column is None or name_column is None:
        return None

    competition_column = column_map.get("competition")
    fixtures: list[Fixture] = []
    for _, row in dataframe.iterrows():
        time_text = str(row[start_column])
        match_name = normalize_whitespace(str(row[name_column]))
        if not match_name or match_name.lower() == "nan":
            continue

        competition = ""
        if competition_column is not None:
            competition = normalize_whitespace(str(row[competition_column]))
            if competition.lower() == "nan":
                competition = ""

        start_time = parse_decimal_datetime(time_text, target_day, verbose=verbose)
        if start_time is None or start_time.date() != target_day:
            continue

        fixtures.append(
            Fixture(
                match_name=match_name,
                competition=competition,
                start_time=start_time,
                source="decimal",
            )
        )

    return sorted(fixtures, key=lambda item: item.start_time)


def format_time(dt: Optional[datetime]) -> str:
    """Format a timezone-aware datetime for TSV output."""
    if dt is None:
        return "N/A"
    return dt.astimezone(UK_TZ).strftime(TIME_OUTPUT_FORMAT)


def get_betfair_certs_dir() -> str:
    """Return the shared directory containing the Betfair cert and key files."""
    cert_dir = os.path.dirname(os.path.abspath(BETFAIR_CERT_FILE))
    key_dir = os.path.dirname(os.path.abspath(BETFAIR_KEY_FILE))

    if not os.path.isfile(BETFAIR_CERT_FILE):
        raise ValueError(f"Betfair cert file not found at resolved path: {os.path.abspath(BETFAIR_CERT_FILE)}")
    if not os.path.isfile(BETFAIR_KEY_FILE):
        raise ValueError(f"Betfair key file not found at resolved path: {os.path.abspath(BETFAIR_KEY_FILE)}")
    if cert_dir != key_dir:
        raise ValueError(
            "BETFAIR_CERT_FILE and BETFAIR_KEY_FILE must be in the same folder for betfairlightweight."
        )
    return cert_dir


def direct_betfair_cert_login() -> str:
    """Perform raw Betfair certificate login with proxies disabled and return session token."""
    context = ssl.create_default_context()
    context.load_cert_chain(
        certfile=os.path.abspath(BETFAIR_CERT_FILE),
        keyfile=os.path.abspath(BETFAIR_KEY_FILE),
    )

    data = urlencode(
        {
            "username": BETFAIR_USERNAME,
            "password": BETFAIR_PASSWORD,
        }
    ).encode("utf-8")

    request = urllib.request.Request(
        BETFAIR_CERT_LOGIN_URL,
        data=data,
        method="POST",
        headers={
            "X-Application": BETFAIR_APP_KEY,
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )

    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        urllib.request.HTTPSHandler(context=context),
    )

    with opener.open(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))

    if payload.get("loginStatus") != "SUCCESS":
        raise RuntimeError(f"Direct Betfair cert login failed: {payload}")

    token = payload.get("sessionToken")
    if not token:
        raise RuntimeError("Direct Betfair cert login succeeded but returned no session token.")
    return token


def build_betfair_client() -> APIClient:
    """Create and log in a Betfair API client using certificate auth."""
    client = APIClient(
        username=BETFAIR_USERNAME,
        password=BETFAIR_PASSWORD,
        app_key=BETFAIR_APP_KEY,
        certs=get_betfair_certs_dir(),
    )
    try:
        client.login()
        return client
    except Exception as exc:
        try:
            token = direct_betfair_cert_login()
            client.set_session_token(token)
            return client
        except Exception as fallback_exc:
            raise RuntimeError(
                "Betfair login failed (including proxy-disabled cert fallback): "
                f"{exc}; fallback error: {fallback_exc}"
            ) from fallback_exc


def fetch_betfair_fixtures(target_day: date, verbose: bool) -> list[Fixture]:
    """Fetch unique Betfair cricket fixtures for the requested UK-local day."""
    start_uk, end_uk = day_bounds(target_day)
    verbose_log(verbose, f"Fetching Betfair fixtures for {target_day.isoformat()}")

    client = build_betfair_client()
    try:
        market_catalogues = client.betting.list_market_catalogue(
            filter=market_filter(
                event_type_ids=[BETFAIR_CRICKET_EVENT_TYPE_ID],
                market_type_codes=[BETFAIR_MARKET_TYPE],
                market_start_time={
                    "from": start_uk.astimezone(UTC_TZ).isoformat(),
                    "to": end_uk.astimezone(UTC_TZ).isoformat(),
                },
            ),
            market_projection=["EVENT", "COMPETITION", "MARKET_START_TIME"],
            sort="FIRST_TO_START",
            max_results=1000,
        )
    finally:
        client.logout()

    fixtures_by_event: dict[str, Fixture] = {}
    for market in market_catalogues:
        event = getattr(market, "event", None)
        if event is None or not getattr(event, "id", None):
            continue

        start_time = getattr(market, "market_start_time", None)
        if start_time is None:
            continue
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=UTC_TZ)
        start_uk_time = start_time.astimezone(UK_TZ)
        if not (start_uk <= start_uk_time < end_uk):
            continue

        event_id = str(event.id)
        if event_id in fixtures_by_event:
            continue

        competition = ""
        if getattr(market, "competition", None) is not None:
            competition = normalize_whitespace(getattr(market.competition, "name", ""))

        fixtures_by_event[event_id] = Fixture(
            match_name=normalize_whitespace(getattr(event, "name", "")),
            competition=competition,
            start_time=start_uk_time,
            source="betfair",
            event_id=event_id,
        )

    return sorted(fixtures_by_event.values(), key=lambda item: item.start_time)


def build_chrome_driver() -> WebDriver:
    """Create a Chrome WebDriver instance."""
    machine = (platform.machine() or "").lower()
    if platform.system().lower() == "linux" and machine and machine not in SUPPORTED_LINUX_MACHINES:
        raise RuntimeError(f"Unsupported Linux architecture for Chrome automation: {machine}")

    chrome_binary = detect_chrome_binary()
    chromedriver_binary = detect_chromedriver_binary()
    options = ChromeOptions()
    options.binary_location = chrome_binary
    if DECIMAL_HEADLESS:
        options.add_argument("--headless=new")
    options.page_load_strategy = "eager"
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-breakpad")
    options.add_argument("--disable-client-side-phishing-detection")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-sync")
    options.add_argument("--mute-audio")
    options.add_experimental_option(
        "prefs",
        {
            "profile.default_content_setting_values.images": 2,
        },
    )
    try:
        if chromedriver_binary:
            return webdriver.Chrome(service=Service(executable_path=chromedriver_binary), options=options)
        return webdriver.Chrome(options=options)
    except WebDriverException as exc:
        diagnostics = format_browser_diagnostics(browser_diagnostics(chrome_binary, chromedriver_binary))
        raise RuntimeError(
            "ChromeDriver/Selenium Manager failed to start Chrome.\n"
            f"{diagnostics}\n"
            f"Root error: {exc}"
        ) from exc


def wait_for_first_present(
    driver: WebDriver,
    selectors: Iterable[tuple[str, str]],
    timeout: int,
    step_name: str,
):
    """Wait until the first matching selector is present and return the element."""
    wait = WebDriverWait(driver, timeout)
    last_error: Optional[Exception] = None
    for by, value in selectors:
        try:
            return wait.until(EC.presence_of_element_located((by, value)))
        except TimeoutException as exc:
            last_error = exc
    raise RuntimeError(
        f"{step_name}: could not find any matching element for selectors: {list(selectors)}"
    ) from last_error


def wait_for_first_clickable(
    driver: WebDriver,
    selectors: Iterable[tuple[str, str]],
    timeout: int,
    step_name: str,
):
    """Wait until the first matching selector is clickable and return the element."""
    wait = WebDriverWait(driver, timeout)
    last_error: Optional[Exception] = None
    for by, value in selectors:
        try:
            return wait.until(EC.element_to_be_clickable((by, value)))
        except TimeoutException as exc:
            last_error = exc
    raise RuntimeError(
        f"{step_name}: could not find any clickable element for selectors: {list(selectors)}"
    ) from last_error


def find_first_present_immediate(
    driver: WebDriver,
    selectors: Iterable[tuple[str, str]],
):
    """Return the first immediately available matching element, if any."""
    for by, value in selectors:
        elements = driver.find_elements(by, value)
        if elements:
            return elements[0]
    return None


def wait_for_any_present(
    driver: WebDriver,
    selectors: Iterable[tuple[str, str]],
    timeout: int,
) -> bool:
    """Return True when any selector becomes present before timeout."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda current_driver: find_first_present_immediate(current_driver, selectors) is not None
        )
        return True
    except TimeoutException:
        return False


def decimal_login_ready(driver: WebDriver) -> bool:
    """Return True once Decimal has moved beyond login and exposed post-login UI."""
    if "/login" not in driver.current_url.lower():
        return True
    return find_first_present_immediate(driver, DECIMAL_USERNAME_SELECTORS) is None


def login_decimal(driver: WebDriver, verbose: bool) -> None:
    """Log into Decimal Cricket via Selenium."""
    verbose_log(verbose, f"Opening Decimal login page: {DECIMAL_LOGIN_URL}")
    try:
        driver.get(DECIMAL_LOGIN_URL)
    except Exception as exc:
        raise RuntimeError(f"Decimal login navigation failed: {DECIMAL_LOGIN_URL}") from exc
    wait = WebDriverWait(driver, DECIMAL_WAIT_SECONDS)

    try:
        verbose_log(verbose, "Locating Decimal username field")
        username_box = wait_for_first_present(
            driver,
            DECIMAL_USERNAME_SELECTORS,
            DECIMAL_WAIT_SECONDS,
            "Decimal login username field lookup",
        )
    except Exception as exc:
        raise RuntimeError("Decimal login failed while locating username field.") from exc

    try:
        verbose_log(verbose, "Locating Decimal password field")
        password_box = wait_for_first_present(
            driver,
            DECIMAL_PASSWORD_SELECTORS,
            DECIMAL_WAIT_SECONDS,
            "Decimal login password field lookup",
        )
    except Exception as exc:
        raise RuntimeError("Decimal login failed while locating password field.") from exc

    try:
        verbose_log(verbose, "Filling Decimal credentials")
        username_box.clear()
        username_box.send_keys(DECIMAL_USERNAME)
        password_box.clear()
        password_box.send_keys(DECIMAL_PASSWORD)
    except Exception as exc:
        raise RuntimeError("Decimal login failed while entering credentials.") from exc

    try:
        verbose_log(verbose, "Locating Decimal submit button")
        submit_button = wait_for_first_clickable(
            driver,
            DECIMAL_SUBMIT_SELECTORS,
            DECIMAL_WAIT_SECONDS,
            "Decimal login submit button lookup",
        )
    except Exception as exc:
        raise RuntimeError("Decimal login failed while locating submit button.") from exc

    try:
        verbose_log(verbose, "Submitting Decimal login form")
        driver.execute_script("arguments[0].click();", submit_button)
    except Exception as exc:
        raise RuntimeError("Decimal login failed while clicking submit button.") from exc

    try:
        verbose_log(verbose, "Waiting for Decimal login completion")
        wait.until(decimal_login_ready)
    except Exception as exc:
        raise RuntimeError(
            "Decimal login may have failed: page did not leave login screen or load fixtures area."
        ) from exc


def infer_column(columns: Iterable[str], patterns: Iterable[str]) -> Optional[str]:
    """Return the first dataframe column whose name matches one of the patterns."""
    lowered_columns = [(column, column.strip().lower()) for column in columns]
    for pattern in patterns:
        for original, lowered in lowered_columns:
            if pattern in lowered:
                return original
    return None


def looks_like_datetime_text(value: str) -> bool:
    """Return True when a value resembles a fixture date/time string."""
    text = normalize_whitespace(value)
    if not text:
        return False

    patterns = [
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
        r"\b\d{1,2}:\d{2}\b",
        r"\b(?:mon|tue|wed|thu|fri|sat|sun)\b",
        r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b",
    ]
    lowered = text.lower()
    return any(re.search(pattern, lowered) for pattern in patterns)


def is_rejected_fixture_text(value: str) -> bool:
    """Return True when text is clearly not a real fixture row value."""
    text = normalize_name(value)
    rejected_values = {
        "",
        "fixtures",
        "fixture",
        "date",
        "time",
        "competition",
        "match",
        "event",
        "name",
        "teams",
        "no data",
        "loading",
    }
    return text in rejected_values


def pick_fixture_fields_from_generic_row(
    row: pd.Series,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Pick match, competition, and time fields heuristically from a generic row."""
    values = [normalize_whitespace(str(value)) for value in row.tolist()]
    values = [value for value in values if value and value.lower() != "nan"]
    if not values:
        return None, None, None

    time_candidates = [value for value in values if looks_like_datetime_text(value)]
    time_text = time_candidates[0] if time_candidates else None

    non_time_values = [value for value in values if value != time_text and not is_rejected_fixture_text(value)]
    if not non_time_values:
        return None, None, time_text

    match_name = max(non_time_values, key=lambda value: len(normalize_whitespace(value)))
    competition_candidates = [value for value in non_time_values if value != match_name]
    competition = competition_candidates[0] if competition_candidates else ""
    return match_name, competition, time_text


def extract_table_rows_via_js(driver: WebDriver, table_element) -> tuple[list[str], list[list[str]]]:
    """Extract header + rows from the table in one JavaScript call."""
    payload = driver.execute_script(
        """
        const table = arguments[0];
        const clean = (value) => (value || "").replace(/\\s+/g, " ").trim();
        const rows = Array.from(table.querySelectorAll("tr"));
        let header = [];
        const dataRows = [];

        for (const row of rows) {
            const headerCells = Array.from(row.querySelectorAll("th"));
            const dataCells = Array.from(row.querySelectorAll("td"));

            if (headerCells.length && header.length === 0) {
                header = headerCells.map((cell) => clean(cell.innerText || cell.textContent));
                header = header.filter((value) => value);
                continue;
            }

            if (dataCells.length) {
                const values = dataCells.map((cell) => clean(cell.innerText || cell.textContent));
                if (values.some((value) => value)) {
                    dataRows.push(values);
                }
            }
        }

        return {header, rows: dataRows};
        """,
        table_element,
    )
    header = payload.get("header") or []
    rows = payload.get("rows") or []
    return header, rows


def extract_table_dataframe_from_element(driver: WebDriver, table_element, verbose: bool) -> pd.DataFrame:
    """Extract a DataFrame from a table-like Selenium element."""
    js_start = perf_counter()
    header_row, extracted_rows = extract_table_rows_via_js(driver, table_element)
    verbose_log(verbose, f"Decimal JS table extraction time: {perf_counter() - js_start:.2f}s")
    verbose_log(verbose, f"Decimal JS extraction row count: {len(extracted_rows)}")

    if not extracted_rows:
        raise RuntimeError("Decimal fixtures table element was found but contained no readable rows.")

    max_columns = max(len(row) for row in extracted_rows)
    normalized_rows = [row + [""] * (max_columns - len(row)) for row in extracted_rows]
    if header_row:
        columns = header_row + [f"col_{index}" for index in range(len(header_row), max_columns)]
    else:
        columns = [f"col_{index}" for index in range(max_columns)]

    dataframe = pd.DataFrame(normalized_rows, columns=columns[:max_columns])
    verbose_log(verbose, f"Decimal dataframe columns detected: {list(dataframe.columns)}")
    return dataframe


def extract_table_dataframe_via_selenium(table_element, verbose: bool) -> pd.DataFrame:
    """Extract a DataFrame from a table-like Selenium element via element calls."""
    rows = table_element.find_elements(By.CSS_SELECTOR, "tr")
    verbose_log(verbose, f"Decimal manual extraction row count: {len(rows)}")

    extracted_rows: list[list[str]] = []
    header_row: Optional[list[str]] = None
    for row in rows:
        header_cells = row.find_elements(By.CSS_SELECTOR, "th")
        data_cells = row.find_elements(By.CSS_SELECTOR, "td")

        if header_cells:
            values = [normalize_whitespace(cell.text) for cell in header_cells]
            values = [value for value in values if value]
            if values and header_row is None:
                header_row = values
            continue

        if data_cells:
            values = [normalize_whitespace(cell.text) for cell in data_cells]
            if any(values):
                extracted_rows.append(values)

    if not extracted_rows:
        raise RuntimeError("Decimal fixtures table element was found but contained no readable rows.")

    max_columns = max(len(row) for row in extracted_rows)
    normalized_rows = [row + [""] * (max_columns - len(row)) for row in extracted_rows]
    if header_row:
        columns = header_row + [f"col_{index}" for index in range(len(header_row), max_columns)]
    else:
        columns = [f"col_{index}" for index in range(max_columns)]

    dataframe = pd.DataFrame(normalized_rows, columns=columns[:max_columns])
    verbose_log(verbose, f"Decimal dataframe columns detected: {list(dataframe.columns)}")
    return dataframe


def extract_table_dataframe_with_fallback(driver: WebDriver, table_element, verbose: bool) -> pd.DataFrame:
    """Prefer JS extraction, then pandas, then Selenium element-walk fallback."""
    try:
        return extract_table_dataframe_from_element(driver, table_element, verbose)
    except Exception as exc:
        verbose_log(verbose, f"Decimal JS extraction failed, falling back to pandas: {exc}")

    outer_html = table_element.get_attribute("outerHTML") or ""
    try:
        tables = pd.read_html(outer_html)
        if tables:
            dataframe = tables[0].copy()
            verbose_log(verbose, f"Decimal dataframe columns detected: {list(dataframe.columns)}")
            return dataframe
    except Exception as exc:
        verbose_log(verbose, f"pandas.read_html failed for Decimal table: {exc}")

    try:
        return extract_table_dataframe_via_selenium(table_element, verbose)
    except Exception as exc:
        raise RuntimeError("Decimal fixtures table element was found but could not be parsed.") from exc


def click_decimal_element(driver: WebDriver, element, description: str, verbose: bool) -> None:
    """Click an element with normal, ActionChains, and JS fallbacks."""
    try:
        driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center', inline: 'center'});",
            element,
        )
    except Exception:
        pass

    click_errors: list[str] = []
    for strategy_name, action in (
        ("native", lambda: element.click()),
        ("action-chains", lambda: ActionChains(driver).move_to_element(element).pause(0.2).click().perform()),
        ("javascript", lambda: driver.execute_script("arguments[0].click();", element)),
    ):
        try:
            verbose_log(verbose, f"{description}: click via {strategy_name}")
            action()
            return
        except Exception as exc:
            click_errors.append(f"{strategy_name}={exc}")

    raise RuntimeError(f"{description}: all click strategies failed ({'; '.join(click_errors)})")


def open_decimal_menu_item(
    driver: WebDriver,
    selectors: Iterable[tuple[str, str]],
    description: str,
    verbose: bool,
) -> None:
    """Locate and click a Decimal menu item with hover retry support."""
    last_error: Optional[Exception] = None
    for attempt in range(2):
        for by, value in selectors:
            try:
                element = WebDriverWait(driver, DECIMAL_QUICK_WAIT_SECONDS).until(
                    EC.presence_of_element_located((by, value))
                )
                try:
                    ActionChains(driver).move_to_element(element).pause(0.2).perform()
                except Exception:
                    pass
                click_decimal_element(driver, element, description, verbose)
                return
            except Exception as exc:
                last_error = exc
        if attempt == 0:
            try:
                body = driver.find_element(By.TAG_NAME, "body")
                ActionChains(driver).move_to_element_with_offset(body, 40, 40).pause(0.2).perform()
            except Exception:
                pass
    raise RuntimeError(f"{description} could not be opened.") from last_error


def extract_decimal_legacy_rows_via_js(driver: WebDriver, verbose: bool) -> dict[str, object]:
    """Extract Decimal Legacy Developer fixtures using in-page JS."""
    payload = driver.execute_async_script(
        """
        const done = arguments[arguments.length - 1];
        const rowSelector = arguments[0];
        const todaySelectors = arguments[1];
        const inPlaySelectors = arguments[2];
        const readyRootSelectors = arguments[3];

        const clean = (value) => (value || "").replace(/\\s+/g, " ").trim();
        const isVisible = (el) => {
          if (!el) return false;
          try {
            const style = el.ownerDocument.defaultView.getComputedStyle(el);
            const rect = el.getBoundingClientRect();
            return style.display !== "none" && style.visibility !== "hidden" && rect.width > 0 && rect.height > 0;
          } catch (error) {
            return false;
          }
        };
        const textEquals = (el, text) => clean(el && (el.innerText || el.textContent || "")) === text;
        const pushUnique = (list, seen, value) => {
          if (value && !seen.has(value)) {
            seen.add(value);
            list.push(value);
          }
        };

        const collectRoots = () => {
          const roots = [];
          const seen = new Set();
          const visitRoot = (root) => {
            if (!root || seen.has(root)) return;
            seen.add(root);
            roots.push(root);
            let elements = [];
            try {
              if (root.querySelectorAll) {
                elements = Array.from(root.querySelectorAll("*"));
              }
            } catch (error) {}
            for (const el of elements) {
              if (el.shadowRoot) {
                visitRoot(el.shadowRoot);
              }
              const tag = (el.tagName || "").toLowerCase();
              if (tag !== "iframe" && tag !== "frame") continue;
              try {
                if (el.contentDocument) {
                  visitRoot(el.contentDocument);
                }
              } catch (error) {}
            }
          };
          visitRoot(document);
          return roots;
        };

        const queryAllDeep = (selector) => {
          const results = [];
          const seen = new Set();
          for (const root of collectRoots()) {
            let nodes = [];
            try {
              nodes = Array.from(root.querySelectorAll(selector));
            } catch (error) {
              continue;
            }
            for (const node of nodes) {
              if (!seen.has(node)) {
                seen.add(node);
                results.push(node);
              }
            }
          }
          return results;
        };

        const queryFirstDeep = (selector) => {
          for (const root of collectRoots()) {
            try {
              const node = root.querySelector(selector);
              if (node) return node;
            } catch (error) {}
          }
          return null;
        };

        const resolvePanelFromButton = (button) => {
          if (!button) return null;
          const target = clean(button.getAttribute("data-target") || button.getAttribute("data-bs-target") || "");
          if (target) {
            const panel = queryFirstDeep(target);
            if (panel) return panel;
          }
          const ariaControls = clean(button.getAttribute("aria-controls") || "");
          if (ariaControls) {
            const panel = queryFirstDeep(`#${ariaControls}`);
            if (panel) return panel;
          }
          return null;
        };

        const panelLooksExpanded = (panel) => {
          if (!panel) return false;
          const className = clean(panel.className || "");
          return /(^|\\s)show(\\s|$)/.test(className);
        };

        const findSectionButton = (kind) => {
          const exactText = kind === "today" ? "Today" : "In Play";
          const targetSelector = kind === "today"
            ? 'button[data-target="#Today"], button[data-bs-target="#Today"]'
            : 'button[data-target="#running"], button[data-bs-target="#running"]';
          const direct = queryFirstDeep(targetSelector);
          if (direct) return direct;
          return queryAllDeep("button").find((button) => textEquals(button, exactText)) || null;
        };

        const querySectionRows = (selectors) => {
          for (const selector of selectors) {
            const rows = queryAllDeep(selector).filter((row) => row && row.getAttribute("data-id"));
            if (rows.length) return rows;
          }
          return [];
        };

        const findContainer = (selectors) => {
          for (const selector of selectors) {
            const containerSelector = selector.replace(/\\s+a\\.list-group-item\\.list-group-item-action\\[data-id\\]$/, "");
            const node = queryFirstDeep(containerSelector);
            if (node) return node;
          }
          return null;
        };

        const inferSection = (row) => {
          let current = row;
          while (current) {
            const id = clean(current.id || "");
            if (id === "running" || id === "running_container") return "in_play";
            if (id === "Today" || id === "Today_container") return "today";
            if (current.parentElement) {
              current = current.parentElement;
              continue;
            }
            const root = current.getRootNode ? current.getRootNode() : null;
            if (root && root.host) {
              current = root.host;
              continue;
            }
            current = null;
          }
          return "other";
        };

        const serializeRow = (row) => {
          const teamA = clean(row.querySelector('div[data-id="A"]')?.innerText || row.querySelector('div[data-id="A"]')?.textContent || "");
          const teamB = clean(row.querySelector('div[data-id="B"]')?.innerText || row.querySelector('div[data-id="B"]')?.textContent || "");
          const dataName = clean(row.getAttribute("data-name") || "");
          return {
            match_id: clean(row.getAttribute("data-id") || ""),
            display_name: teamA && teamB ? `${teamA} v ${teamB}` : dataName,
            data_name: dataName,
            teamA,
            teamB,
            section: inferSection(row),
            start_time: clean(row.getAttribute("data-start") || ""),
            outer_html: row.outerHTML || "",
          };
        };

        const dedupeRows = (rows) => {
          const fixtures = [];
          const seen = new Set();
          for (const row of rows) {
            const fixture = serializeRow(row);
            const key = fixture.match_id || fixture.outer_html;
            if (!key || seen.has(key)) continue;
            seen.add(key);
            fixtures.push(fixture);
          }
          return fixtures;
        };

        const extractSnapshot = () => {
          const todayContainer = findContainer(todaySelectors);
          const inPlayContainer = findContainer(inPlaySelectors);
          const todayRows = querySectionRows(todaySelectors);
          const inPlayRows = querySectionRows(inPlaySelectors);
          const allRows = queryAllDeep(rowSelector).filter((row) => row && row.getAttribute("data-id"));
          const readyRootExists = readyRootSelectors.some((selector) => !!queryFirstDeep(selector));
          const inPlayButton = findSectionButton("in_play");
          const preferredRows = [...inPlayRows, ...todayRows];
          return {
            todayContainer,
            inPlayContainer,
            inPlayButton,
            todayRows,
            inPlayRows,
            allRows,
            ready: todayRows.length > 0 || inPlayRows.length > 0 || (readyRootExists && allRows.length > 0) || (!!inPlayButton && allRows.length > 0),
            readyRootExists,
          };
        };

        const maybeExpandSection = (kind, snapshot) => {
          const button = findSectionButton(kind);
          const panel = resolvePanelFromButton(button);
          const rows = kind === "today" ? snapshot.todayRows : snapshot.inPlayRows;
          if (!button || !panel || rows.length > 0 || panelLooksExpanded(panel)) return false;
          try {
            button.click();
            return true;
          } catch (error) {
            return false;
          }
        };

        const start = Date.now();
        let expanded = false;
        let snapshot = extractSnapshot();
        expanded = maybeExpandSection("today", snapshot) || expanded;
        expanded = maybeExpandSection("in_play", snapshot) || expanded;

        const trimFixture = (fixture) => ({
          match_id: fixture.match_id,
          display_name: fixture.display_name,
          data_name: fixture.data_name,
          teamA: fixture.teamA,
          teamB: fixture.teamB,
          section: fixture.section,
          start_time: fixture.start_time,
        });

        const finish = () => {
          snapshot = extractSnapshot();
          const preferredRows = [...snapshot.inPlayRows, ...snapshot.todayRows];
          const preferredFixtures = dedupeRows(preferredRows);
          const allFixtures = dedupeRows(snapshot.allRows);
          const source = preferredFixtures.length ? "today_inplay_sections" : "date_fallback";
          done({
            status: snapshot.ready || allFixtures.length ? "ok" : "waiting",
            preferred_fixtures: preferredFixtures.map(trimFixture),
            all_fixtures: allFixtures.map(trimFixture),
            todayRowsCount: snapshot.todayRows.length,
            inPlayRowsCount: snapshot.inPlayRows.length,
            allRowsCount: snapshot.allRows.length,
            foundTodayContainer: !!snapshot.todayContainer,
            foundInPlayContainer: !!snapshot.inPlayContainer,
            page_url: window.location.href,
            source,
          });
        };

        const tick = () => {
          snapshot = extractSnapshot();
          const waitedLongEnough = Date.now() - start >= 1500;
          if (snapshot.ready || !expanded || waitedLongEnough) {
            finish();
            return;
          }
          setTimeout(tick, 150);
        };

        tick();
        """,
        DECIMAL_LEGACY_ROW_SELECTOR,
        list(DECIMAL_LEGACY_TODAY_ROW_SELECTORS),
        list(DECIMAL_LEGACY_IN_PLAY_ROW_SELECTORS),
        list(DECIMAL_LEGACY_READY_ROOT_SELECTORS),
    )
    result = payload or {}
    if verbose:
        verbose_log(verbose, f"Decimal Legacy Developer page URL: {result.get('page_url', '')}")
    return result


def wait_for_decimal_legacy_rows_via_js(driver: WebDriver, timeout: int, verbose: bool) -> dict[str, object]:
    """Wait for Decimal Legacy Developer Today/In Play rows to become available."""
    payload: dict[str, object] = {}

    def poll(current_driver: WebDriver) -> bool:
        nonlocal payload
        payload = extract_decimal_legacy_rows_via_js(current_driver, verbose)
        return bool(payload.get("allRowsCount")) or bool(payload.get("todayRowsCount")) or bool(payload.get("inPlayRowsCount"))

    try:
        WebDriverWait(driver, timeout).until(poll)
    except TimeoutException as exc:
        raise RuntimeError("Decimal Legacy Developer Today/In Play sections could not be found.") from exc
    return payload


def open_decimal_legacy_developer(driver: WebDriver, verbose: bool) -> dict[str, object]:
    """Navigate Decimal to the Legacy Developer viewer and return JS extraction payload."""
    verbose_log(verbose, f"Opening Decimal dec page: {DECIMAL_DEC_URL}")
    try:
        driver.get(DECIMAL_DEC_URL)
    except Exception as exc:
        raise RuntimeError(f"Decimal navigation failed: {DECIMAL_DEC_URL}") from exc

    try:
        WebDriverWait(driver, DECIMAL_WAIT_SECONDS).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
    except Exception as exc:
        raise RuntimeError("Decimal dec page body did not load.") from exc

    try:
        verbose_log(verbose, "Locating/clicking Decimal VIEWERS menu")
        open_decimal_menu_item(driver, DECIMAL_VIEWERS_SELECTORS, "Decimal VIEWERS menu", verbose)
    except Exception as exc:
        raise RuntimeError("Decimal Legacy Developer menu could not be opened.") from exc

    try:
        verbose_log(verbose, "Locating/clicking Decimal LEGACY DEVELOPER submenu")
        open_decimal_menu_item(
            driver,
            DECIMAL_LEGACY_DEVELOPER_SELECTORS,
            "Decimal LEGACY DEVELOPER submenu",
            verbose,
        )
    except Exception as exc:
        raise RuntimeError("Decimal Legacy Developer menu could not be opened.") from exc

    payload = wait_for_decimal_legacy_rows_via_js(driver, DECIMAL_WAIT_SECONDS, verbose)
    verbose_log(verbose, f"Decimal Legacy Developer Today rows found: {int(payload.get('todayRowsCount', 0))}")
    verbose_log(verbose, f"Decimal Legacy Developer In Play rows found: {int(payload.get('inPlayRowsCount', 0))}")
    return payload


def parse_decimal_legacy_fixture_payload(
    row_payload: dict[str, object],
    target_day: date,
    verbose: bool,
) -> Optional[Fixture]:
    """Convert a JS-extracted Legacy Developer row into a Decimal fixture."""
    match_name = normalize_whitespace(str(row_payload.get("display_name", "")))
    if not match_name:
        match_name = normalize_whitespace(str(row_payload.get("data_name", "")))
    if not match_name:
        return None

    start_time_text = normalize_whitespace(str(row_payload.get("start_time", "")))
    if not start_time_text:
        return None

    start_time = parse_decimal_datetime(start_time_text, target_day, verbose=verbose)
    if start_time is None or start_time.date() != target_day:
        return None

    if is_rejected_fixture_text(match_name):
        return None
    if len(normalize_name(match_name)) < 4:
        return None

    return Fixture(
        match_name=match_name,
        competition="",
        start_time=start_time,
        source="decimal",
    )


def build_decimal_legacy_fixtures_from_rows(
    row_payloads: Iterable[dict[str, object]],
    target_day: date,
    verbose: bool,
) -> list[Fixture]:
    """Convert JS-extracted Legacy Developer rows into deduped target-day fixtures."""
    fixtures: list[Fixture] = []
    seen_keys: set[tuple[str, datetime]] = set()
    for row_payload in row_payloads:
        fixture = parse_decimal_legacy_fixture_payload(dict(row_payload), target_day, verbose)
        if fixture is None:
            continue
        dedupe_key = (normalize_name(fixture.match_name), fixture.start_time)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        fixtures.append(fixture)
    return sorted(fixtures, key=lambda item: item.start_time)


def extract_decimal_legacy_developer_fixtures(
    driver: WebDriver,
    target_day: date,
    verbose: bool,
) -> list[Fixture]:
    """Extract Decimal fixtures from the Legacy Developer Today/In Play sections."""
    payload = open_decimal_legacy_developer(driver, verbose)
    preferred_rows = payload.get("preferred_fixtures") or []
    all_rows = payload.get("all_fixtures") or []
    verbose_log(verbose, f"Decimal Legacy Developer Today container found: {bool(payload.get('foundTodayContainer'))}")
    verbose_log(verbose, f"Decimal Legacy Developer In Play container found: {bool(payload.get('foundInPlayContainer'))}")
    verbose_log(verbose, f"Decimal Legacy Developer today rows count: {int(payload.get('todayRowsCount', 0))}")
    verbose_log(verbose, f"Decimal Legacy Developer in-play rows count: {int(payload.get('inPlayRowsCount', 0))}")
    verbose_log(verbose, f"Decimal Legacy Developer preferred rows count: {len(preferred_rows)}")
    verbose_log(verbose, f"Decimal Legacy Developer all rows count: {len(all_rows)}")

    preferred_fixtures = build_decimal_legacy_fixtures_from_rows(preferred_rows, target_day, verbose)
    all_fixtures = build_decimal_legacy_fixtures_from_rows(all_rows, target_day, verbose)
    verbose_log(
        verbose,
        f"Decimal Legacy Developer preferred rows kept for target day: {len(preferred_fixtures)}",
    )
    verbose_log(verbose, f"Decimal Legacy Developer all rows kept for target day: {len(all_fixtures)}")
    if preferred_fixtures:
        fixtures = preferred_fixtures
        verbose_log(verbose, "Decimal Legacy Developer using Today/In Play rows for target day")
    else:
        if all_fixtures:
            fixtures = all_fixtures
            verbose_log(verbose, "Decimal Legacy Developer falling back to all rows for target day")
        else:
            raise RuntimeError("No valid Decimal Legacy Developer fixtures were parsed for target day.")

    if verbose and fixtures:
        samples = ", ".join(
            f"{fixture.match_name} @ {fixture.start_time.strftime('%d/%m/%Y %H:%M')}"
            for fixture in fixtures[:3]
        )
        verbose_log(verbose, f"Decimal Legacy Developer sample rows: {samples}")
    return fixtures


def fixture_row_to_fixture(row: pd.Series, target_day: date, verbose: bool = False) -> Optional[Fixture]:
    """Convert a dataframe row to a normalized Decimal fixture."""
    columns = list(row.index)
    match_col = infer_column(columns, ("match", "event", "name", "teams", "fixture"))
    time_col = infer_column(columns, ("start", "time", "date"))
    competition_col = infer_column(columns, ("competition", "league", "tournament", "series"))
    match_name: Optional[str] = None
    competition = ""
    time_text: Optional[str] = None

    if match_col is not None and time_col is not None:
        candidate_match = normalize_whitespace(str(row.get(match_col, "")))
        candidate_time = normalize_whitespace(str(row.get(time_col, "")))
        if candidate_match and candidate_match.lower() != "nan" and candidate_time:
            match_name = candidate_match
            time_text = candidate_time
            if competition_col:
                competition = normalize_whitespace(str(row.get(competition_col, ""))).replace("nan", "")

    if not match_name or not time_text:
        match_name, competition, time_text = pick_fixture_fields_from_generic_row(row)
        if not match_name or not time_text:
            return None

    if is_rejected_fixture_text(match_name):
        return None
    if len(normalize_name(match_name)) < 4:
        return None

    start_time = parse_decimal_datetime(time_text, target_day, verbose=verbose)
    if start_time is None:
        return None

    return Fixture(
        match_name=match_name,
        competition=competition,
        start_time=start_time,
        source="decimal",
    )


def fetch_decimal_fixtures(target_day: date, verbose: bool) -> list[Fixture]:
    """Log into Decimal and scrape Legacy Developer fixture rows for the requested UK-local day."""
    verbose_log(verbose, f"Fetching Decimal fixtures for {target_day.isoformat()}")
    login_start = perf_counter()
    try:
        driver = build_chrome_driver()
    except Exception as exc:
        raise DecimalScrapeError(f"Decimal browser startup failed: {exc}") from exc

    try:
        login_decimal(driver, verbose)
        verbose_log(verbose, f"Decimal login time: {perf_counter() - login_start:.2f}s")
        extraction_start = perf_counter()
        verbose_log(verbose, "Extracting Decimal Legacy Developer Today/In Play fixtures")
        fixtures = extract_decimal_legacy_developer_fixtures(driver, target_day, verbose)
        verbose_log(
            verbose,
            f"Decimal Legacy Developer extraction time: {perf_counter() - extraction_start:.2f}s",
        )
        if not fixtures:
            raise DecimalScrapeError("Decimal scrape returned zero fixtures for the requested day.")
        return fixtures
    except DecimalScrapeError:
        raise
    except Exception as exc:
        raise DecimalScrapeError(f"Decimal fixture scrape failed: {exc}") from exc
    finally:
        driver.quit()


def similarity(a: str, b: str) -> float:
    """Return a SequenceMatcher similarity score."""
    return SequenceMatcher(None, a, b).ratio()


def fixture_match_score(betfair_fixture: Fixture, decimal_fixture: Fixture) -> float:
    """Calculate a weighted fuzzy score using match name and competition."""
    name_score = similarity(
        normalize_name(betfair_fixture.match_name),
        normalize_name(decimal_fixture.match_name),
    )
    bf_comp = normalize_competition(betfair_fixture.competition)
    dec_comp = normalize_competition(decimal_fixture.competition)
    if bf_comp and dec_comp:
        competition_score = similarity(bf_comp, dec_comp)
        return (name_score * 0.8) + (competition_score * 0.2)
    return name_score


def match_fixtures(
    betfair_fixtures: list[Fixture],
    decimal_fixtures: list[Fixture],
    threshold: float,
) -> list[MatchResult]:
    """Match Betfair fixtures to the best unmatched Decimal fixtures."""
    remaining_decimal = set(range(len(decimal_fixtures)))
    results: list[MatchResult] = []

    for betfair_fixture in betfair_fixtures:
        best_index: Optional[int] = None
        best_score = 0.0

        for index in remaining_decimal:
            decimal_fixture = decimal_fixtures[index]
            score = fixture_match_score(betfair_fixture, decimal_fixture)
            if score > best_score:
                best_score = score
                best_index = index

        if best_index is None or best_score < threshold:
            results.append(
                MatchResult(
                    betfair_fixture=betfair_fixture,
                    decimal_fixture=None,
                    score=best_score,
                    time_matches=False,
                )
            )
            continue

        decimal_fixture = decimal_fixtures[best_index]
        remaining_decimal.remove(best_index)
        results.append(
            MatchResult(
                betfair_fixture=betfair_fixture,
                decimal_fixture=decimal_fixture,
                score=best_score,
                time_matches=betfair_fixture.start_time == decimal_fixture.start_time,
            )
        )

    return results


def comparison_counts(
    betfair_fixtures: list[Fixture],
    decimal_fixtures: list[Fixture],
    results: list[MatchResult],
) -> dict[str, int]:
    """Return summary counts for the fixture comparison."""
    matched_fixtures = sum(1 for result in results if result.decimal_fixture is not None)
    unmatched_betfair = sum(1 for result in results if result.decimal_fixture is None)
    return {
        "betfair_fixtures": len(betfair_fixtures),
        "decimal_fixtures": len(decimal_fixtures),
        "matched_fixtures": matched_fixtures,
        "unmatched_betfair_fixtures": unmatched_betfair,
        "unmatched_decimal_fixtures": max(len(decimal_fixtures) - matched_fixtures, 0),
    }


def print_summary_counts(counts: dict[str, int], pretty: bool) -> None:
    """Print comparison summary counts in TSV or aligned form."""
    rows = [
        ("Betfair Fixtures", counts["betfair_fixtures"]),
        ("Decimal Fixtures", counts["decimal_fixtures"]),
        ("Matched Fixtures", counts["matched_fixtures"]),
        ("Unmatched Betfair Fixtures", counts["unmatched_betfair_fixtures"]),
        ("Unmatched Decimal Fixtures", counts["unmatched_decimal_fixtures"]),
    ]
    if not pretty:
        print("Scrape Status\tBetfair\tOK")
        print("Scrape Status\tDecimal\tOK")
        for label, value in rows:
            print(f"{label}\t{value}")
        return
    print(f"{'Scrape Status':<26}  {'Betfair':<8}  OK")
    print(f"{'Scrape Status':<26}  {'Decimal':<8}  OK")
    for label, value in rows:
        print(f"{label:<26}  {value}")


def print_decimal_scrape_failure(
    betfair_fixtures: list[Fixture],
    root_error: Exception,
    pretty: bool,
) -> None:
    """Print a top-level Decimal scrape failure instead of false fixture mismatches."""
    root_message = str(root_error).replace("\n", " | ")
    if not pretty:
        print("Scrape Status\tBetfair\tOK")
        print("Scrape Status\tDecimal\tFAILED")
        print(f"Betfair Fixtures\t{len(betfair_fixtures)}")
        print("Decimal Fixtures\t0")
        print("Matched Fixtures\t0")
        print(f"Unmatched Betfair Fixtures\t{len(betfair_fixtures)}")
        print("Unmatched Decimal Fixtures\t0")
        print("Failure\tDecimal fixture scrape failed; comparison not reliable.")
        print(f"Root Error\t{root_message}")
        return

    print(f"{'Scrape Status':<26}  {'Betfair':<8}  OK")
    print(f"{'Scrape Status':<26}  {'Decimal':<8}  FAILED")
    print(f"{'Betfair Fixtures':<26}  {len(betfair_fixtures)}")
    print(f"{'Decimal Fixtures':<26}  0")
    print(f"{'Matched Fixtures':<26}  0")
    print(f"{'Unmatched Betfair Fixtures':<26}  {len(betfair_fixtures)}")
    print(f"{'Unmatched Decimal Fixtures':<26}  0")
    print()
    print("Decimal fixture scrape failed; comparison not reliable.")
    print(f"Root Error: {root_message}")


def print_results(
    results: list[MatchResult],
    betfair_fixtures: list[Fixture],
    decimal_fixtures: list[Fixture],
    pretty: bool = False,
) -> None:
    """Print results to stdout as TSV or aligned columns."""
    print_summary_counts(comparison_counts(betfair_fixtures, decimal_fixtures, results), pretty)
    not_matching_count = sum(
        1 for result in results
        if result.decimal_fixture is None or not result.time_matches
    )

    rows: list[tuple[str, str, str, str]] = []
    for result in results:
        if result.decimal_fixture is None:
            status = "NOT MATCHING"
            decimal_time = "N/A"
        elif not result.time_matches:
            status = "TIME DIFF"
            decimal_time = format_time(result.decimal_fixture.start_time)
        else:
            status = "OK"
            decimal_time = format_time(result.decimal_fixture.start_time)

        rows.append(
            (
                status,
                result.betfair_fixture.match_name,
                format_time(result.betfair_fixture.start_time),
                decimal_time,
            )
        )

    if not pretty:
        print(f"Not Matching\t{not_matching_count}")
        print("Status\tMatch\tBetfair Time\tDecimal Time")
        for status, match_name, betfair_time, decimal_time in rows:
            print(f"{status}\t{match_name}\t{betfair_time}\t{decimal_time}")
        return

    summary_label = "Not Matching"
    print(f"{summary_label:<12}  {not_matching_count}")

    headers = ("Status", "Match", "Betfair Time", "Decimal Time")
    status_width = max(len(headers[0]), *(len(row[0]) for row in rows))
    match_width = max(len(headers[1]), *(len(row[1]) for row in rows))
    betfair_width = max(len(headers[2]), *(len(row[2]) for row in rows))
    decimal_width = max(len(headers[3]), *(len(row[3]) for row in rows))

    print(
        f"{headers[0]:<{status_width}}  "
        f"{headers[1]:<{match_width}}  "
        f"{headers[2]:<{betfair_width}}  "
        f"{headers[3]:<{decimal_width}}"
    )
    for status, match_name, betfair_time, decimal_time in rows:
        print(
            f"{status:<{status_width}}  "
            f"{match_name:<{match_width}}  "
            f"{betfair_time:<{betfair_width}}  "
            f"{decimal_time:<{decimal_width}}"
        )


def exit_code_for_results(results: list[MatchResult]) -> int:
    """Return the process exit code based on match completeness and time equality."""
    for result in results:
        if result.decimal_fixture is None:
            return 1
        if not result.time_matches:
            return 1
    return 0


def main() -> int:
    """Entrypoint."""
    args = parse_args()
    total_start = perf_counter()
    try:
        if args.debug_browser:
            print_browser_diagnostics()
            return 0

        validate_config()
        target_day = requested_day(args)
        betfair_start = perf_counter()
        betfair_fixtures = fetch_betfair_fixtures(target_day, args.verbose)
        verbose_log(args.verbose, f"Betfair fetch time: {perf_counter() - betfair_start:.2f}s")
        try:
            decimal_fixtures = fetch_decimal_fixtures(target_day, args.verbose)
        except DecimalScrapeError as exc:
            print_decimal_scrape_failure(betfair_fixtures, exc, pretty=args.pretty)
            verbose_log(args.verbose, f"Total script runtime: {perf_counter() - total_start:.2f}s")
            if args.verbose:
                print(traceback.format_exc(limit=8), file=sys.stderr)
            return 2
        results = match_fixtures(betfair_fixtures, decimal_fixtures, SIMILARITY_THRESHOLD)
        print_results(results, betfair_fixtures, decimal_fixtures, pretty=args.pretty)
        verbose_log(args.verbose, f"Total script runtime: {perf_counter() - total_start:.2f}s")
        return exit_code_for_results(results)
    except Exception as exc:
        verbose_log(args.verbose, f"Total script runtime: {perf_counter() - total_start:.2f}s")
        print(f"Error type: {type(exc).__name__}", file=sys.stderr)
        print(f"Error: {exc}", file=sys.stderr)
        if args.verbose:
            print(traceback.format_exc(limit=8), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
