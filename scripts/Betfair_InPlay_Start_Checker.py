#!/usr/bin/env python3
"""Check Betfair MATCH_ODDS markets that missed their in-play start."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sqlite3
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable, Iterable
import unicodedata
from zoneinfo import ZoneInfo

import requests
from betfairlightweight import APIClient
from betfairlightweight.filters import market_filter
from dotenv import load_dotenv


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
RUNTIME_OUTPUT_DIR = Path(os.getenv("SCRIPT_OUTPUT_DIR", str(PROJECT_ROOT / "runtime" / "output")))
STATE_DB_PATH = RUNTIME_OUTPUT_DIR / "betfair_inplay_start_checker.sqlite3"

UK_TZ = ZoneInfo("Europe/London")
UTC_TZ = ZoneInfo("UTC")
PLACEHOLDER_PREFIXES = ("YOUR_", "PASTE_", "CHANGE_ME", "TODO")
EXCLUDED_SPORT_NAMES = {"tennis", "darts", "football", "soccer", "horse racing", "greyhound racing"}
ALERTABLE_STATUSES = {"OPEN", "SUSPENDED"}
DEFAULT_LOOKBACK_HOURS = 6.0
DEFAULT_LOOKAHEAD_HOURS = 24.0
DEFAULT_OVERDUE_MINUTES = 2.0
DEFAULT_MARKET_BOOK_BATCH_SIZE = 40
SLACK_WEBHOOK_ENV_NAME = "Slack_Webhook_TIP"
FLASHSCORE_SPORTS = ("Tennis", "Darts")
FLASHSCORE_URLS = {
    "Tennis": "https://www.flashscore.com/tennis/",
    "Darts": "https://www.flashscore.com/darts/",
}


load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Config:
    betfair_username: str
    betfair_password: str
    betfair_app_key: str
    betfair_certs_path: str
    slack_webhook_url: str
    slack_config_source: str


@dataclass(frozen=True)
class EventType:
    event_type_id: str
    sport_name: str


@dataclass(frozen=True)
class MarketCandidate:
    sport_name: str
    event_type_id: str
    event_id: str
    event_name: str
    competition_name: str
    market_id: str
    scheduled_start_utc: datetime | None


@dataclass(frozen=True)
class MarketBookSnapshot:
    market_id: str
    status: str
    inplay: bool


@dataclass(frozen=True)
class AlertDecision:
    should_alert: bool
    reason: str


@dataclass(frozen=True)
class FlashscoreMatch:
    sport_name: str
    match_name: str
    competition_name: str
    status_text: str
    score: str
    match_id: str
    url: str
    detected_live_at: datetime
    participants: tuple[str, str]


@dataclass(frozen=True)
class MatchConfidence:
    level: str
    reason: str
    score: float


@dataclass
class ScanStats:
    sports_discovered: int = 0
    included_sports_count: int = 0
    excluded_sports_count: int = 0
    markets_scanned: int = 0
    events_checked: int = 0
    flags_found: int = 0
    slack_alerts_sent: int = 0
    slack_alert_failures: int = 0
    skipped_events: int = 0
    api_errors: int = 0
    flashscore_live_matches_found: int = 0
    betfair_time_scan_status: str = "not_run"
    flashscore_scan_status: str = "not_run"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(UTC_TZ).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC_TZ)
        return value.astimezone(UTC_TZ)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC_TZ)
        return parsed.astimezone(UTC_TZ)
    return None


def normalize_match_name(value: str) -> str:
    without_accents = "".join(
        char for char in unicodedata.normalize("NFKD", value or "") if not unicodedata.combining(char)
    )
    without_brackets = re.sub(r"\([^)]*\)|\[[^]]*\]", " ", without_accents)
    lowered = without_brackets.casefold()
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def name_similarity(first: str, second: str) -> float:
    first_norm = normalize_match_name(first)
    second_norm = normalize_match_name(second)
    if not first_norm or not second_norm:
        return 0.0
    if first_norm == second_norm:
        return 1.0
    return SequenceMatcher(None, first_norm, second_norm).ratio()


def parse_participants(name: str) -> tuple[str, str] | None:
    parts = [
        part.strip()
        for part in re.split(r"\s+(?:v|vs|vs\.|@|-)\s+", name or "", maxsplit=1, flags=re.IGNORECASE)
        if part.strip()
    ]
    if len(parts) == 2:
        return parts[0], parts[1]
    return None


def participant_confidence(
    flashscore_participants: tuple[str, str],
    betfair_participants: tuple[str, str] | None,
    flashscore_competition: str,
    betfair_competition: str,
) -> MatchConfidence:
    if not betfair_participants:
        return MatchConfidence("Low", "Betfair participants could not be parsed", 0.0)
    direct = (
        name_similarity(flashscore_participants[0], betfair_participants[0])
        + name_similarity(flashscore_participants[1], betfair_participants[1])
    ) / 2
    reversed_score = (
        name_similarity(flashscore_participants[0], betfair_participants[1])
        + name_similarity(flashscore_participants[1], betfair_participants[0])
    ) / 2
    score = max(direct, reversed_score)
    if score >= 0.88:
        return MatchConfidence("High", "Both participant names match order-insensitively", score)

    one_name_score = max(
        name_similarity(flashscore_participants[0], betfair_participants[0]),
        name_similarity(flashscore_participants[0], betfair_participants[1]),
        name_similarity(flashscore_participants[1], betfair_participants[0]),
        name_similarity(flashscore_participants[1], betfair_participants[1]),
    )
    competition_score = name_similarity(flashscore_competition, betfair_competition)
    if one_name_score >= 0.90 and competition_score >= 0.82:
        return MatchConfidence("Medium", "One participant and competition are similar", (one_name_score + competition_score) / 2)
    if one_name_score >= 0.80:
        return MatchConfidence("Low", "Only one participant appears to match", one_name_score)
    return MatchConfidence("Low", "Participant names did not match confidently", score)


def format_betfair_time(value: datetime) -> str:
    return value.astimezone(UTC_TZ).strftime("%Y-%m-%dT%H:%M:%SZ")


def format_uk_datetime(value: datetime | None) -> str:
    if value is None:
        return "unknown"
    return value.astimezone(UK_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def format_slack_uk_time(value: datetime | None) -> str:
    if value is None:
        return "unknown"
    return value.astimezone(UK_TZ).strftime("%H:%M:%S UK")


def format_duration(delta: timedelta) -> str:
    total_seconds = max(0, int(delta.total_seconds()))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    return f"{minutes}m {seconds}s"


def log(message: str) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def object_get(obj: Any, name: str, default: Any = "") -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def is_placeholder(value: str) -> bool:
    stripped = value.strip()
    return not stripped or any(stripped.startswith(prefix) for prefix in PLACEHOLDER_PREFIXES)


def resolve_path(value: str, base_dir: Path) -> str:
    if not value:
        return ""
    path = Path(value)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return str(path)


def mask_webhook(value: str) -> str:
    if not value:
        return "not configured"
    if value.startswith("https://hooks.slack.com/services/"):
        return "https://hooks.slack.com/services/***"
    return "***"


def load_config() -> Config:
    cert_file = os.getenv("BETFAIR_CERT_FILE", "").strip()
    key_file = os.getenv("BETFAIR_KEY_FILE", "").strip()
    certs_dir = os.getenv("BETFAIR_CERTS_DIR", "").strip() or os.getenv("BF_CERTS_DIR", "").strip()
    if cert_file and not certs_dir:
        certs_dir = str(Path(resolve_path(cert_file, PROJECT_ROOT)).parent)
    if key_file and not certs_dir:
        certs_dir = str(Path(resolve_path(key_file, PROJECT_ROOT)).parent)
    if not certs_dir:
        certs_dir = str((SCRIPT_DIR / "Integrity-Scanner" / "certs").resolve())

    slack_webhook_url = os.getenv(SLACK_WEBHOOK_ENV_NAME, "").strip()
    slack_config_source = SLACK_WEBHOOK_ENV_NAME if slack_webhook_url else "not configured"

    return Config(
        betfair_username=os.getenv("BETFAIR_USERNAME", "").strip() or os.getenv("BF_USERNAME", "").strip(),
        betfair_password=os.getenv("BETFAIR_PASSWORD", "").strip() or os.getenv("BF_PASSWORD", "").strip(),
        betfair_app_key=os.getenv("BETFAIR_APP_KEY", "").strip() or os.getenv("BF_APP_KEY", "").strip(),
        betfair_certs_path=resolve_path(certs_dir, PROJECT_ROOT),
        slack_webhook_url=slack_webhook_url,
        slack_config_source=slack_config_source,
    )


def require_betfair_config(config: Config) -> None:
    missing = []
    if is_placeholder(config.betfair_username):
        missing.append("BETFAIR_USERNAME")
    if is_placeholder(config.betfair_password):
        missing.append("BETFAIR_PASSWORD")
    if is_placeholder(config.betfair_app_key):
        missing.append("BETFAIR_APP_KEY")
    if missing:
        raise RuntimeError(f"Missing Betfair config: {', '.join(missing)}")


def build_client(config: Config) -> APIClient:
    require_betfair_config(config)
    certs_dir = Path(config.betfair_certs_path).resolve()
    cert_file = Path(os.getenv("BETFAIR_CERT_FILE", str(certs_dir / "client-2048.crt")))
    key_file = Path(os.getenv("BETFAIR_KEY_FILE", str(certs_dir / "client-2048.key")))
    if not cert_file.is_absolute():
        cert_file = (PROJECT_ROOT / cert_file).resolve()
    if not key_file.is_absolute():
        key_file = (PROJECT_ROOT / key_file).resolve()

    log(f"Using Betfair certs directory: {certs_dir}")
    if not cert_file.exists():
        raise FileNotFoundError(f"Betfair cert file not found: {cert_file}")
    if not key_file.exists():
        raise FileNotFoundError(f"Betfair key file not found: {key_file}")

    client = APIClient(
        username=config.betfair_username,
        password=config.betfair_password,
        app_key=config.betfair_app_key,
        certs=str(certs_dir),
    )
    client.login()
    return client


def open_db(path: Path = STATE_DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    init_db(connection)
    return connection


def init_db(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS inplay_alert_state (
            event_id TEXT PRIMARY KEY,
            market_id TEXT,
            sport_name TEXT,
            competition_name TEXT,
            event_name TEXT,
            scheduled_start_utc TEXT,
            scheduled_start_uk TEXT,
            first_flagged_at TEXT,
            alert_sent_at TEXT,
            last_seen_status TEXT,
            last_seen_inplay INTEGER,
            recovered_at TEXT,
            last_checked_at TEXT,
            final_verification_at TEXT,
            final_verification_result TEXT,
            final_verification_reason TEXT,
            trigger_source TEXT,
            flashscore_match_id TEXT,
            flashscore_url TEXT,
            flashscore_match_name TEXT,
            flashscore_competition TEXT,
            flashscore_status TEXT,
            flashscore_score TEXT,
            flashscore_detected_live_at TEXT,
            match_confidence TEXT,
            match_reason TEXT,
            betfair_last_checked_at TEXT,
            betfair_last_seen_inplay INTEGER,
            betfair_last_seen_status TEXT,
            slack_alert_sent INTEGER,
            slack_error TEXT
        )
        """
    )
    ensure_column(connection, "inplay_alert_state", "final_verification_at", "TEXT")
    ensure_column(connection, "inplay_alert_state", "final_verification_result", "TEXT")
    ensure_column(connection, "inplay_alert_state", "final_verification_reason", "TEXT")
    ensure_column(connection, "inplay_alert_state", "trigger_source", "TEXT")
    ensure_column(connection, "inplay_alert_state", "flashscore_match_id", "TEXT")
    ensure_column(connection, "inplay_alert_state", "flashscore_url", "TEXT")
    ensure_column(connection, "inplay_alert_state", "flashscore_match_name", "TEXT")
    ensure_column(connection, "inplay_alert_state", "flashscore_competition", "TEXT")
    ensure_column(connection, "inplay_alert_state", "flashscore_status", "TEXT")
    ensure_column(connection, "inplay_alert_state", "flashscore_score", "TEXT")
    ensure_column(connection, "inplay_alert_state", "flashscore_detected_live_at", "TEXT")
    ensure_column(connection, "inplay_alert_state", "match_confidence", "TEXT")
    ensure_column(connection, "inplay_alert_state", "match_reason", "TEXT")
    ensure_column(connection, "inplay_alert_state", "betfair_last_checked_at", "TEXT")
    ensure_column(connection, "inplay_alert_state", "betfair_last_seen_inplay", "INTEGER")
    ensure_column(connection, "inplay_alert_state", "betfair_last_seen_status", "TEXT")
    ensure_column(connection, "inplay_alert_state", "slack_alert_sent", "INTEGER")
    ensure_column(connection, "inplay_alert_state", "slack_error", "TEXT")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS inplay_scan_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            level TEXT NOT NULL,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL,
            sport_name TEXT,
            event_id TEXT,
            market_id TEXT,
            details_json TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS inplay_scan_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_started_at TEXT NOT NULL,
            scan_completed_at TEXT,
            next_scan_at TEXT,
            status TEXT NOT NULL,
            dry_run INTEGER NOT NULL,
            sports_discovered INTEGER NOT NULL DEFAULT 0,
            included_sports_count INTEGER NOT NULL DEFAULT 0,
            excluded_sports_json TEXT NOT NULL DEFAULT '[]',
            markets_scanned INTEGER NOT NULL DEFAULT 0,
            events_checked INTEGER NOT NULL DEFAULT 0,
            flags_found INTEGER NOT NULL DEFAULT 0,
            slack_alerts_sent INTEGER NOT NULL DEFAULT 0,
            slack_alert_failures INTEGER NOT NULL DEFAULT 0,
            api_errors INTEGER NOT NULL DEFAULT 0,
            config_error TEXT NOT NULL DEFAULT ''
            ,
            betfair_time_scan_status TEXT NOT NULL DEFAULT 'not_run',
            flashscore_scan_status TEXT NOT NULL DEFAULT 'not_run',
            flashscore_live_matches_found INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    ensure_column(connection, "inplay_scan_runs", "betfair_time_scan_status", "TEXT NOT NULL DEFAULT 'not_run'")
    ensure_column(connection, "inplay_scan_runs", "flashscore_scan_status", "TEXT NOT NULL DEFAULT 'not_run'")
    ensure_column(connection, "inplay_scan_runs", "flashscore_live_matches_found", "INTEGER NOT NULL DEFAULT 0")
    connection.commit()


def ensure_column(connection: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    columns = {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in columns:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def db_log(
    connection: sqlite3.Connection,
    level: str,
    event_type: str,
    message: str,
    *,
    sport_name: str = "",
    event_id: str = "",
    market_id: str = "",
    event_name: str = "",
    details: dict[str, Any] | None = None,
) -> None:
    details_payload = dict(details or {})
    if event_name and "event_name" not in details_payload:
        details_payload["event_name"] = event_name
    connection.execute(
        """
        INSERT INTO inplay_scan_logs
            (timestamp, level, event_type, message, sport_name, event_id, market_id, details_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            iso_utc(utc_now()),
            level,
            event_type,
            message,
            sport_name,
            event_id,
            market_id,
            json.dumps(details_payload, sort_keys=True),
        ),
    )
    connection.commit()
    prefix = f"{level}: " if level not in {"INFO", "DEBUG"} else ""
    log(f"{prefix}{message}")


def start_scan_run(connection: sqlite3.Connection, args: argparse.Namespace) -> int:
    cursor = connection.execute(
        "INSERT INTO inplay_scan_runs (scan_started_at, status, dry_run) VALUES (?, ?, ?)",
        (iso_utc(utc_now()), "running", int(bool(args.dry_run))),
    )
    connection.commit()
    return int(cursor.lastrowid)


def finish_scan_run(
    connection: sqlite3.Connection,
    run_id: int,
    status: str,
    stats: ScanStats,
    excluded_sports: list[str],
    *,
    next_scan_at: datetime | None = None,
    config_error: str = "",
) -> None:
    connection.execute(
        """
        UPDATE inplay_scan_runs
        SET scan_completed_at = ?,
            next_scan_at = ?,
            status = ?,
            sports_discovered = ?,
            included_sports_count = ?,
            excluded_sports_json = ?,
            markets_scanned = ?,
            events_checked = ?,
            flags_found = ?,
            slack_alerts_sent = ?,
            slack_alert_failures = ?,
            api_errors = ?,
            config_error = ?,
            betfair_time_scan_status = ?,
            flashscore_scan_status = ?,
            flashscore_live_matches_found = ?
        WHERE id = ?
        """,
        (
            iso_utc(utc_now()),
            iso_utc(next_scan_at),
            status,
            stats.sports_discovered,
            stats.included_sports_count,
            json.dumps(excluded_sports),
            stats.markets_scanned,
            stats.events_checked,
            stats.flags_found,
            stats.slack_alerts_sent,
            stats.slack_alert_failures,
            stats.api_errors,
            config_error,
            stats.betfair_time_scan_status,
            stats.flashscore_scan_status,
            stats.flashscore_live_matches_found,
            run_id,
        ),
    )
    connection.commit()


def normalize_sport_name(value: str) -> str:
    return " ".join((value or "").casefold().split())


def is_excluded_sport(sport_name: str) -> bool:
    return normalize_sport_name(sport_name) in EXCLUDED_SPORT_NAMES


def sport_emoji(sport_name: str) -> str:
    name = normalize_sport_name(sport_name)
    emoji_map = {
        "cricket": ":cricket_bat_and_ball:",
        "tennis": ":tennis:",
        "basketball": ":basketball:",
        "rugby union": ":rugby_football:",
        "rugby league": ":rugby_football:",
        "darts": ":dart:",
        "snooker": ":8ball:",
        "pool": ":8ball:",
        "baseball": ":baseball:",
        "ice hockey": ":ice_hockey_stick_and_puck:",
        "hockey": ":ice_hockey_stick_and_puck:",
        "volleyball": ":volleyball:",
        "handball": ":handball:",
        "golf": ":golf:",
        "boxing": ":boxing_glove:",
        "mma": ":boxing_glove:",
        "mixed martial arts": ":boxing_glove:",
        "esports": ":video_game:",
        "esports ": ":video_game:",
    }
    return emoji_map.get(name, ":sports_medal:")


def list_event_types(client: APIClient, start_from: datetime, start_to: datetime) -> list[EventType]:
    event_filter = market_filter(
        market_start_time={
            "from": format_betfair_time(start_from),
            "to": format_betfair_time(start_to),
        }
    )
    results = client.betting.list_event_types(filter=event_filter)
    event_types: list[EventType] = []
    for result in results:
        event_type = object_get(result, "event_type", {})
        event_type_id = str(object_get(event_type, "id", "")).strip()
        sport_name = str(object_get(event_type, "name", "")).strip()
        if event_type_id and sport_name:
            event_types.append(EventType(event_type_id=event_type_id, sport_name=sport_name))
    return event_types


def list_market_catalogues(
    client: APIClient,
    event_type: EventType,
    start_from: datetime,
    start_to: datetime,
    max_results: int,
) -> list[Any]:
    event_filter = market_filter(
        event_type_ids=[event_type.event_type_id],
        market_type_codes=["MATCH_ODDS"],
        market_start_time={
            "from": format_betfair_time(start_from),
            "to": format_betfair_time(start_to),
        },
    )
    return client.betting.list_market_catalogue(
        filter=event_filter,
        market_projection=["EVENT", "EVENT_TYPE", "COMPETITION", "MARKET_START_TIME", "MARKET_DESCRIPTION"],
        sort="FIRST_TO_START",
        max_results=max_results,
    )


def flashscore_browser_matches(connection: sqlite3.Connection, timeout_seconds: int) -> list[FlashscoreMatch]:
    try:
        from selenium import webdriver
        from selenium.webdriver import ChromeOptions
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
    except Exception as exc:
        db_log(connection, "ERROR", "flashscore_scan_completed", f"Flashscore Selenium unavailable: {exc}")
        return []

    options = ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1400,1000")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_binary = os.getenv("CHROME_BINARY") or os.getenv("GOOGLE_CHROME_BIN") or os.getenv("CHROME_BIN")
    if chrome_binary:
        options.binary_location = chrome_binary

    chromedriver_path = os.getenv("CHROMEDRIVER_PATH", "").strip()
    driver = None
    matches: list[FlashscoreMatch] = []
    try:
        if chromedriver_path:
            driver = webdriver.Chrome(service=Service(executable_path=chromedriver_path), options=options)
        else:
            driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(timeout_seconds + 10)
        wait = WebDriverWait(driver, timeout_seconds)
        for sport_name, url in FLASHSCORE_URLS.items():
            try:
                driver.get(url)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[id^='g_'], .event__match")))
                time.sleep(2)
                rows = driver.execute_script(
                    """
                    const matchRows = Array.from(document.querySelectorAll("[id^='g_'], .event__match"));
                    function text(root, selector) {
                      const el = root.querySelector(selector);
                      return el ? el.innerText.trim() : "";
                    }
                    function previousCompetition(row) {
                      let node = row.previousElementSibling;
                      for (let i = 0; node && i < 30; i += 1, node = node.previousElementSibling) {
                        const title = node.querySelector(".event__title--name, .event__titleBox, .event__title");
                        if (title && title.innerText.trim()) return title.innerText.trim();
                        if (node.className && String(node.className).includes("event__header")) {
                          const txt = node.innerText.trim().replace(/\\n+/g, " - ");
                          if (txt) return txt;
                        }
                      }
                      return "";
                    }
                    return matchRows.map(row => {
                      const home = text(row, ".event__participant--home");
                      const away = text(row, ".event__participant--away");
                      const homeScore = text(row, ".event__score--home");
                      const awayScore = text(row, ".event__score--away");
                      const status = text(row, ".event__stage--block") || text(row, ".event__stage") || text(row, ".event__time");
                      const link = row.querySelector("a[href*='/match/']");
                      return {
                        id: row.id || row.getAttribute("data-event-id") || "",
                        home,
                        away,
                        status,
                        score: [homeScore, awayScore].filter(Boolean).join("-"),
                        url: link ? link.href : "",
                        competition: previousCompetition(row),
                        className: String(row.className || "")
                      };
                    }).filter(row => row.home && row.away);
                    """
                )
            except Exception as exc:
                db_log(connection, "ERROR", "flashscore_scan_completed", f"Flashscore {sport_name} fetch failed: {exc}", sport_name=sport_name)
                continue

            for row in rows or []:
                status = str(row.get("status") or "").strip()
                score = str(row.get("score") or "").strip()
                class_name = str(row.get("className") or "")
                if not flashscore_row_is_live(status, score, class_name):
                    continue
                home = str(row.get("home") or "").strip()
                away = str(row.get("away") or "").strip()
                match = FlashscoreMatch(
                    sport_name=sport_name,
                    match_name=f"{home} v {away}",
                    competition_name=str(row.get("competition") or "").strip(),
                    status_text=status or "Live",
                    score=score,
                    match_id=str(row.get("id") or "").strip(),
                    url=str(row.get("url") or "").strip(),
                    detected_live_at=utc_now(),
                    participants=(home, away),
                )
                matches.append(match)
                db_log(
                    connection,
                    "INFO",
                    "flashscore_live_match_found",
                    "Flashscore live match found",
                    sport_name=sport_name,
                    event_name=match.match_name,
                    details={
                        "flashscore_match_name": match.match_name,
                        "flashscore_competition": match.competition_name,
                        "flashscore_status": match.status_text,
                        "flashscore_score": match.score,
                        "flashscore_match_id": match.match_id,
                    },
                )
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass
    return matches


def flashscore_row_is_live(status: str, score: str, class_name: str) -> bool:
    text = f"{status} {class_name}".casefold()
    if any(marker in text for marker in ("finished", "after pen", "postponed", "cancelled", "walkover", "retired")):
        return False
    if any(marker in text for marker in ("live", "inplay", "in-play", "set", "leg", "break", "1st", "2nd", "3rd", "4th", "5th")):
        return True
    if score and re.search(r"\d", score) and not re.match(r"^\d{1,2}:\d{2}$", status.strip()):
        return True
    return False


def catalogue_to_candidate(catalogue: Any, fallback: EventType) -> MarketCandidate:
    event = object_get(catalogue, "event", {})
    event_type = object_get(catalogue, "event_type", {})
    competition = object_get(catalogue, "competition", {})
    return MarketCandidate(
        sport_name=str(object_get(event_type, "name", fallback.sport_name) or fallback.sport_name).strip(),
        event_type_id=str(object_get(event_type, "id", fallback.event_type_id) or fallback.event_type_id).strip(),
        event_id=str(object_get(event, "id", "")).strip(),
        event_name=str(object_get(event, "name", "") or object_get(catalogue, "market_name", "")).strip(),
        competition_name=str(object_get(competition, "name", "")).strip(),
        market_id=str(object_get(catalogue, "market_id", "")).strip(),
        scheduled_start_utc=parse_datetime(object_get(catalogue, "market_start_time", None)),
    )


def chunked(values: list[str], size: int) -> Iterable[list[str]]:
    size = max(1, size)
    for index in range(0, len(values), size):
        yield values[index : index + size]


def list_market_books(client: APIClient, market_ids: list[str]) -> dict[str, MarketBookSnapshot]:
    results = client.betting.list_market_book(market_ids=market_ids)
    snapshots: dict[str, MarketBookSnapshot] = {}
    for book in results:
        market_id = str(object_get(book, "market_id", "")).strip()
        if not market_id:
            continue
        status = str(object_get(book, "status", "")).strip().upper()
        inplay = bool(object_get(book, "inplay", False))
        snapshots[market_id] = MarketBookSnapshot(market_id=market_id, status=status, inplay=inplay)
    return snapshots


def alerted_event_ids(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute("SELECT event_id FROM inplay_alert_state WHERE alert_sent_at IS NOT NULL").fetchall()
    return {str(row["event_id"]) for row in rows}


def alert_decision(
    candidate: MarketCandidate,
    book: MarketBookSnapshot,
    now: datetime,
    already_alerted: set[str],
    overdue_minutes: float,
) -> AlertDecision:
    if not candidate.event_id:
        return AlertDecision(False, "missing event ID")
    if candidate.scheduled_start_utc is None:
        return AlertDecision(False, "missing scheduled start")
    if candidate.scheduled_start_utc > now - timedelta(minutes=overdue_minutes):
        return AlertDecision(False, "not overdue")
    if candidate.event_id in already_alerted:
        return AlertDecision(False, "already alerted")
    if book.status == "CLOSED":
        return AlertDecision(False, "closed")
    if book.inplay:
        return AlertDecision(False, "already in-play")
    if book.status not in ALERTABLE_STATUSES:
        return AlertDecision(False, f"status {book.status or 'unknown'}")
    return AlertDecision(True, "flagged")


def upsert_alert_state(
    connection: sqlite3.Connection,
    candidate: MarketCandidate,
    book: MarketBookSnapshot,
    *,
    now: datetime,
    alert_sent_at: datetime | None = None,
    final_verification_at: datetime | None = None,
    final_verification_result: str = "",
    final_verification_reason: str = "",
    trigger_source: str = "betfair_time",
    flashscore_match: FlashscoreMatch | None = None,
    match_confidence: MatchConfidence | None = None,
) -> None:
    first_flagged_at = iso_utc(now)
    existing = connection.execute(
        """
        SELECT first_flagged_at, alert_sent_at, recovered_at, final_verification_result, final_verification_reason
        FROM inplay_alert_state
        WHERE event_id = ?
        """,
        (candidate.event_id,),
    ).fetchone()
    recovered_at = existing["recovered_at"] if existing else None
    if book.inplay and not recovered_at:
        recovered_at = iso_utc(now)
    connection.execute(
        """
        INSERT INTO inplay_alert_state (
            event_id, market_id, sport_name, competition_name, event_name,
            scheduled_start_utc, scheduled_start_uk, first_flagged_at, alert_sent_at,
            last_seen_status, last_seen_inplay, recovered_at, last_checked_at,
            final_verification_at, final_verification_result, final_verification_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            market_id = excluded.market_id,
            sport_name = excluded.sport_name,
            competition_name = excluded.competition_name,
            event_name = excluded.event_name,
            scheduled_start_utc = excluded.scheduled_start_utc,
            scheduled_start_uk = excluded.scheduled_start_uk,
            first_flagged_at = COALESCE(inplay_alert_state.first_flagged_at, excluded.first_flagged_at),
            alert_sent_at = COALESCE(inplay_alert_state.alert_sent_at, excluded.alert_sent_at),
            last_seen_status = excluded.last_seen_status,
            last_seen_inplay = excluded.last_seen_inplay,
            recovered_at = COALESCE(inplay_alert_state.recovered_at, excluded.recovered_at),
            last_checked_at = excluded.last_checked_at,
            final_verification_at = COALESCE(excluded.final_verification_at, inplay_alert_state.final_verification_at),
            final_verification_result = CASE
                WHEN excluded.final_verification_result != '' THEN excluded.final_verification_result
                ELSE inplay_alert_state.final_verification_result
            END,
            final_verification_reason = CASE
                WHEN excluded.final_verification_reason != '' THEN excluded.final_verification_reason
                ELSE inplay_alert_state.final_verification_reason
            END
        """,
        (
            candidate.event_id,
            candidate.market_id,
            candidate.sport_name,
            candidate.competition_name,
            candidate.event_name,
            iso_utc(candidate.scheduled_start_utc),
            format_uk_datetime(candidate.scheduled_start_utc),
            existing["first_flagged_at"] if existing else first_flagged_at,
            iso_utc(alert_sent_at) if alert_sent_at else (existing["alert_sent_at"] if existing else None),
            book.status,
            int(book.inplay),
            recovered_at,
            iso_utc(now),
            iso_utc(final_verification_at) if final_verification_at else None,
            final_verification_result or (existing["final_verification_result"] if existing else ""),
            final_verification_reason or (existing["final_verification_reason"] if existing else ""),
        ),
    )
    connection.execute(
        """
        UPDATE inplay_alert_state
        SET trigger_source = COALESCE(trigger_source, ?),
            betfair_last_checked_at = ?,
            betfair_last_seen_inplay = ?,
            betfair_last_seen_status = ?
        WHERE event_id = ?
        """,
        (
            trigger_source,
            iso_utc(now),
            int(book.inplay),
            book.status,
            candidate.event_id,
        ),
    )
    if flashscore_match is not None:
        connection.execute(
            """
            UPDATE inplay_alert_state
            SET trigger_source = ?,
                flashscore_match_id = ?,
                flashscore_url = ?,
                flashscore_match_name = ?,
                flashscore_competition = ?,
                flashscore_status = ?,
                flashscore_score = ?,
                flashscore_detected_live_at = ?,
                match_confidence = ?,
                match_reason = ?
            WHERE event_id = ?
            """,
            (
                trigger_source,
                flashscore_match.match_id,
                flashscore_match.url,
                flashscore_match.match_name,
                flashscore_match.competition_name,
                flashscore_match.status_text,
                flashscore_match.score,
                iso_utc(flashscore_match.detected_live_at),
                match_confidence.level if match_confidence else "",
                match_confidence.reason if match_confidence else "",
                candidate.event_id,
            ),
        )
    if alert_sent_at is not None:
        connection.execute(
            "UPDATE inplay_alert_state SET slack_alert_sent = 1, slack_error = '' WHERE event_id = ?",
            (candidate.event_id,),
        )
    connection.commit()


def record_slack_error(connection: sqlite3.Connection, event_id: str, error: str) -> None:
    connection.execute(
        "UPDATE inplay_alert_state SET slack_alert_sent = 0, slack_error = ? WHERE event_id = ?",
        (error, event_id),
    )
    connection.commit()


def record_final_verification_failed(
    connection: sqlite3.Connection,
    candidate: MarketCandidate,
    initial_book: MarketBookSnapshot,
    *,
    now: datetime,
    reason: str,
    trigger_source: str = "betfair_time",
    flashscore_match: FlashscoreMatch | None = None,
    match_confidence: MatchConfidence | None = None,
) -> None:
    existing = connection.execute(
        """
        SELECT first_flagged_at, alert_sent_at, recovered_at
        FROM inplay_alert_state
        WHERE event_id = ?
        """,
        (candidate.event_id,),
    ).fetchone()
    connection.execute(
        """
        INSERT INTO inplay_alert_state (
            event_id, market_id, sport_name, competition_name, event_name,
            scheduled_start_utc, scheduled_start_uk, first_flagged_at, alert_sent_at,
            last_seen_status, last_seen_inplay, recovered_at, last_checked_at,
            final_verification_at, final_verification_result, final_verification_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            market_id = excluded.market_id,
            sport_name = excluded.sport_name,
            competition_name = excluded.competition_name,
            event_name = excluded.event_name,
            scheduled_start_utc = excluded.scheduled_start_utc,
            scheduled_start_uk = excluded.scheduled_start_uk,
            last_seen_status = excluded.last_seen_status,
            last_seen_inplay = NULL,
            last_checked_at = excluded.last_checked_at,
            final_verification_at = excluded.final_verification_at,
            final_verification_result = excluded.final_verification_result,
            final_verification_reason = excluded.final_verification_reason
        """,
        (
            candidate.event_id,
            candidate.market_id,
            candidate.sport_name,
            candidate.competition_name,
            candidate.event_name,
            iso_utc(candidate.scheduled_start_utc),
            format_uk_datetime(candidate.scheduled_start_utc),
            existing["first_flagged_at"] if existing else iso_utc(now),
            existing["alert_sent_at"] if existing else None,
            initial_book.status,
            None,
            existing["recovered_at"] if existing else None,
            iso_utc(now),
            iso_utc(now),
            "failed",
            reason,
        ),
    )
    connection.execute(
        """
        UPDATE inplay_alert_state
        SET trigger_source = ?,
            betfair_last_checked_at = ?,
            betfair_last_seen_inplay = NULL,
            betfair_last_seen_status = ?
        WHERE event_id = ?
        """,
        (trigger_source, iso_utc(now), initial_book.status, candidate.event_id),
    )
    if flashscore_match is not None:
        connection.execute(
            """
            UPDATE inplay_alert_state
            SET flashscore_match_id = ?,
                flashscore_url = ?,
                flashscore_match_name = ?,
                flashscore_competition = ?,
                flashscore_status = ?,
                flashscore_score = ?,
                flashscore_detected_live_at = ?,
                match_confidence = ?,
                match_reason = ?
            WHERE event_id = ?
            """,
            (
                flashscore_match.match_id,
                flashscore_match.url,
                flashscore_match.match_name,
                flashscore_match.competition_name,
                flashscore_match.status_text,
                flashscore_match.score,
                iso_utc(flashscore_match.detected_live_at),
                match_confidence.level if match_confidence else "",
                match_confidence.reason if match_confidence else "",
                candidate.event_id,
            ),
        )
    connection.commit()


def verify_candidate_before_alert(
    connection: sqlite3.Connection,
    client: APIClient,
    candidate: MarketCandidate,
    initial_book: MarketBookSnapshot,
    already_alerted: set[str],
    overdue_minutes: float,
) -> MarketBookSnapshot | None:
    db_log(
        connection,
        "INFO",
        "final_verification_started",
        "Final verification started",
        sport_name=candidate.sport_name,
        event_id=candidate.event_id,
        market_id=candidate.market_id,
        event_name=candidate.event_name,
    )
    try:
        final_books = list_market_books(client, [candidate.market_id])
        final_book = final_books.get(candidate.market_id)
        if final_book is None:
            raise RuntimeError("No MarketBook returned for final verification")
    except Exception as exc:
        now = utc_now()
        reason = str(exc)
        record_final_verification_failed(connection, candidate, initial_book, now=now, reason=reason)
        db_log(
            connection,
            "ERROR",
            "final_verification_failed",
            f"Final verification failed: {reason}",
            sport_name=candidate.sport_name,
            event_id=candidate.event_id,
            market_id=candidate.market_id,
            event_name=candidate.event_name,
            details={"reason": reason},
        )
        return None

    now = utc_now()
    latest_alerted = already_alerted | alerted_event_ids(connection)
    final_decision = alert_decision(candidate, final_book, now, latest_alerted, overdue_minutes)
    if final_book.inplay:
        upsert_alert_state(
            connection,
            candidate,
            final_book,
            now=now,
            final_verification_at=now,
            final_verification_result="suppressed",
            final_verification_reason="inplay",
        )
        db_log(
            connection,
            "INFO",
            "candidate_suppressed_final_check_inplay",
            "Candidate suppressed by final check: market is in-play",
            sport_name=candidate.sport_name,
            event_id=candidate.event_id,
            market_id=candidate.market_id,
            event_name=candidate.event_name,
            details={"status": final_book.status, "inplay": final_book.inplay},
        )
        return None
    if final_book.status == "CLOSED":
        upsert_alert_state(
            connection,
            candidate,
            final_book,
            now=now,
            final_verification_at=now,
            final_verification_result="suppressed",
            final_verification_reason="closed",
        )
        db_log(
            connection,
            "INFO",
            "candidate_suppressed_final_check_closed",
            "Candidate suppressed by final check: market is closed",
            sport_name=candidate.sport_name,
            event_id=candidate.event_id,
            market_id=candidate.market_id,
            event_name=candidate.event_name,
            details={"status": final_book.status, "inplay": final_book.inplay},
        )
        return None
    if not final_decision.should_alert:
        upsert_alert_state(
            connection,
            candidate,
            final_book,
            now=now,
            final_verification_at=now,
            final_verification_result="suppressed",
            final_verification_reason=final_decision.reason,
        )
        db_log(
            connection,
            "INFO",
            "skipped",
            f"Candidate suppressed by final check: {final_decision.reason}",
            sport_name=candidate.sport_name,
            event_id=candidate.event_id,
            market_id=candidate.market_id,
            event_name=candidate.event_name,
            details={"reason": final_decision.reason, "status": final_book.status, "inplay": final_book.inplay},
        )
        return None

    upsert_alert_state(
        connection,
        candidate,
        final_book,
        now=now,
        final_verification_at=now,
        final_verification_result="confirmed_not_inplay",
        final_verification_reason="not_inplay",
    )
    db_log(
        connection,
        "INFO",
        "final_verification_confirmed_not_inplay",
        "Final verification confirmed market is still not in-play",
        sport_name=candidate.sport_name,
        event_id=candidate.event_id,
        market_id=candidate.market_id,
        event_name=candidate.event_name,
        details={"status": final_book.status, "inplay": final_book.inplay},
    )
    return final_book


def build_slack_message(candidate: MarketCandidate, book: MarketBookSnapshot, now: datetime) -> str:
    emoji = sport_emoji(candidate.sport_name)
    overdue_by = format_duration(now - (candidate.scheduled_start_utc or now))
    suspended_note = ["", "This market is currently SUSPENDED."] if book.status == "SUSPENDED" else []
    return "\n".join(
        [
            f":warning: {emoji} Betfair In-Play Check",
            "",
            f"{candidate.event_name or 'Unknown event'} has a scheduled start time in the past but is not in-play.",
            *suspended_note,
            "",
            "Source: Betfair scheduled-time scan",
            f"Sport: {candidate.sport_name or 'unknown'}",
            f"Competition: {candidate.competition_name or 'unknown'}",
            f"Match ID: {candidate.event_id}",
            f"Market ID: {candidate.market_id}",
            f"Website start time: {format_slack_uk_time(candidate.scheduled_start_utc)}",
            f"Overdue by: {overdue_by}",
            f"Market status: {book.status or 'unknown'}",
            "In-play: false",
            "",
            "Please check whether this event should now be live/in-play. React with a tick once handled.",
        ]
    )


def send_slack_message(
    webhook_url: str,
    text: str,
    *,
    post_func: Callable[..., Any] = requests.post,
) -> None:
    if is_placeholder(webhook_url):
        raise RuntimeError(f"{SLACK_WEBHOOK_ENV_NAME} missing")
    response = post_func(webhook_url, json={"text": text}, timeout=15)
    status_code = int(getattr(response, "status_code", 0))
    body = str(getattr(response, "text", ""))
    if status_code >= 400:
        raise RuntimeError(f"Slack webhook failed: status={status_code}, body={body[:300]}")


def send_lifecycle_message(
    connection: sqlite3.Connection,
    config: Config,
    *,
    dry_run: bool,
    event_type: str,
    message: str,
) -> None:
    print(message, flush=True)
    if dry_run:
        db_log(connection, "INFO", event_type, "Dry-run: lifecycle Slack message not sent")
        return
    if is_placeholder(config.slack_webhook_url):
        db_log(connection, "ERROR", "config_error", f"{SLACK_WEBHOOK_ENV_NAME} missing")
        return
    try:
        send_slack_message(config.slack_webhook_url, message)
    except Exception as exc:
        db_log(connection, "ERROR", "slack_failure", f"Lifecycle Slack message failed: {exc}")
        return
    db_log(connection, "INFO", event_type, "Lifecycle Slack message sent")


def startup_message(repeat_minutes: float, dry_run: bool) -> str:
    cadence = f"Scanning every {repeat_minutes:g} minutes." if repeat_minutes else "Running one scan now."
    dry_run_note = " Dry-run is enabled; Slack alerts will not be sent." if dry_run else ""
    return (
        ":isittip: Betfair In-Play Start Checker online\n\n"
        f"{cadence} Monitoring MATCH_ODDS markets for overdue events that have not turned in-play."
        f"{dry_run_note}"
    )


def shutdown_message() -> str:
    return ":octagonal_sign: Betfair In-Play Start Checker stopped"


def fetch_overdue_candidates(
    connection: sqlite3.Connection,
    client: APIClient,
    args: argparse.Namespace,
    stats: ScanStats,
) -> tuple[list[MarketCandidate], list[str]]:
    now = utc_now()
    start_from = now - timedelta(hours=max(args.lookback_hours, 0))
    start_to = now + timedelta(hours=max(args.lookahead_hours, 0))
    event_types = list_event_types(client, start_from, start_to)
    stats.sports_discovered = len(event_types)
    excluded_sports: list[str] = []
    candidates: list[MarketCandidate] = []

    db_log(connection, "INFO", "sports_discovered", f"Sports discovered: {len(event_types)}")
    for event_type in event_types:
        if is_excluded_sport(event_type.sport_name):
            excluded_sports.append(event_type.sport_name)
            stats.excluded_sports_count += 1
            db_log(
                connection,
                "INFO",
                "skipped",
                "Skipped excluded sport",
                sport_name=event_type.sport_name,
                details={"reason": "excluded sport", "event_type_id": event_type.event_type_id},
            )
            continue

        stats.included_sports_count += 1
        try:
            catalogues = list_market_catalogues(client, event_type, start_from, start_to, args.max_results)
        except Exception as exc:
            stats.api_errors += 1
            record_final_verification_failed(
                connection,
                candidate,
                MarketBookSnapshot(candidate.market_id, "", False),
                now=utc_now(),
                reason=str(exc),
                trigger_source="flashscore_live",
                flashscore_match=flashscore_match,
                match_confidence=confidence,
            )
            db_log(
                connection,
                "ERROR",
                "api_error",
                f"Betfair listMarketCatalogue failed for {event_type.sport_name}: {exc}",
                sport_name=event_type.sport_name,
                details={"event_type_id": event_type.event_type_id},
            )
            continue

        stats.markets_scanned += len(catalogues)
        db_log(
            connection,
            "INFO",
            "markets_scanned",
            f"{event_type.sport_name}: {len(catalogues)} MATCH_ODDS markets",
            sport_name=event_type.sport_name,
            details={"markets_scanned": len(catalogues), "event_type_id": event_type.event_type_id},
        )
        for catalogue in catalogues:
            candidate = catalogue_to_candidate(catalogue, event_type)
            if not candidate.event_id:
                stats.skipped_events += 1
                db_log(
                    connection,
                    "INFO",
                    "skipped",
                    "Skipped market with missing event ID",
                    sport_name=candidate.sport_name,
                    market_id=candidate.market_id,
                    event_name=candidate.event_name,
                    details={"reason": "missing event ID"},
                )
                continue
            if candidate.scheduled_start_utc is None or candidate.scheduled_start_utc > now - timedelta(minutes=args.overdue_minutes):
                stats.skipped_events += 1
                db_log(
                    connection,
                    "DEBUG",
                    "skipped",
                    "Skipped event that is not overdue",
                    sport_name=candidate.sport_name,
                    event_id=candidate.event_id,
                    market_id=candidate.market_id,
                    event_name=candidate.event_name,
                    details={"reason": "not overdue", "scheduled_start_utc": iso_utc(candidate.scheduled_start_utc)},
                )
                continue
            candidates.append(candidate)
    return candidates, sorted(set(excluded_sports), key=str.casefold)


def process_candidates(
    connection: sqlite3.Connection,
    client: APIClient,
    config: Config,
    args: argparse.Namespace,
    candidates: list[MarketCandidate],
    stats: ScanStats,
) -> None:
    already_alerted = alerted_event_ids(connection)
    handled_this_scan: set[str] = set()
    by_market_id = {candidate.market_id: candidate for candidate in candidates if candidate.market_id}
    for batch in chunked(list(by_market_id), args.market_book_batch_size):
        try:
            books = list_market_books(client, batch)
        except Exception as exc:
            stats.api_errors += 1
            db_log(connection, "ERROR", "api_error", f"Betfair listMarketBook failed: {exc}", details={"market_ids": batch})
            continue

        for market_id in batch:
            candidate = by_market_id[market_id]
            book = books.get(market_id)
            if book is None:
                stats.skipped_events += 1
                db_log(
                    connection,
                    "ERROR",
                    "api_error",
                    "No MarketBook returned for market",
                    sport_name=candidate.sport_name,
                    event_id=candidate.event_id,
                    market_id=market_id,
                    event_name=candidate.event_name,
                    details={"reason": "API error"},
                )
                continue

            now = utc_now()
            stats.events_checked += 1
            if candidate.event_id in handled_this_scan:
                stats.skipped_events += 1
                db_log(
                    connection,
                    "INFO",
                    "skipped",
                    "Skipped event: already handled this scan",
                    sport_name=candidate.sport_name,
                    event_id=candidate.event_id,
                    market_id=market_id,
                    event_name=candidate.event_name,
                    details={"reason": "already handled this scan", "status": book.status, "inplay": book.inplay},
                )
                continue

            decision = alert_decision(candidate, book, now, already_alerted, args.overdue_minutes)
            if book.inplay or candidate.event_id in already_alerted:
                upsert_alert_state(connection, candidate, book, now=now)
            if not decision.should_alert:
                stats.skipped_events += 1
                db_log(
                    connection,
                    "INFO",
                    "skipped",
                    f"Skipped event: {decision.reason}",
                    sport_name=candidate.sport_name,
                    event_id=candidate.event_id,
                    market_id=market_id,
                    event_name=candidate.event_name,
                    details={"reason": decision.reason, "status": book.status, "inplay": book.inplay},
                )
                continue

            db_log(
                connection,
                "INFO",
                "candidate_found",
                "Candidate found for final verification",
                sport_name=candidate.sport_name,
                event_id=candidate.event_id,
                market_id=market_id,
                event_name=candidate.event_name,
                details={"status": book.status, "inplay": book.inplay},
            )
            handled_this_scan.add(candidate.event_id)
            final_book = verify_candidate_before_alert(
                connection,
                client,
                candidate,
                book,
                already_alerted,
                args.overdue_minutes,
            )
            if final_book is None:
                continue

            stats.flags_found += 1
            alert_now = utc_now()
            message = build_slack_message(candidate, final_book, alert_now)
            print("", flush=True)
            print(message, flush=True)
            if args.dry_run:
                db_log(
                    connection,
                    "INFO",
                    "dry_run_alert",
                    "Dry-run: would send Slack alert",
                    sport_name=candidate.sport_name,
                    event_id=candidate.event_id,
                    market_id=market_id,
                    event_name=candidate.event_name,
                )
                continue

            if is_placeholder(config.slack_webhook_url):
                stats.slack_alert_failures += 1
                db_log(
                    connection,
                    "ERROR",
                    "config_error",
                    f"{SLACK_WEBHOOK_ENV_NAME} missing",
                    sport_name=candidate.sport_name,
                    event_id=candidate.event_id,
                    market_id=market_id,
                    event_name=candidate.event_name,
                )
                upsert_alert_state(
                    connection,
                    candidate,
                    final_book,
                    now=alert_now,
                    final_verification_result="confirmed_not_inplay",
                    final_verification_reason=f"{SLACK_WEBHOOK_ENV_NAME} missing",
                )
                record_slack_error(connection, candidate.event_id, f"{SLACK_WEBHOOK_ENV_NAME} missing")
                continue

            try:
                send_slack_message(config.slack_webhook_url, message)
            except Exception as exc:
                stats.slack_alert_failures += 1
                db_log(
                    connection,
                    "ERROR",
                    "slack_alert_failed",
                    f"Slack alert failed: {exc}",
                    sport_name=candidate.sport_name,
                    event_id=candidate.event_id,
                    market_id=market_id,
                    event_name=candidate.event_name,
                )
                upsert_alert_state(
                    connection,
                    candidate,
                    final_book,
                    now=alert_now,
                    final_verification_result="confirmed_not_inplay",
                    final_verification_reason=f"slack_failed: {exc}",
                )
                record_slack_error(connection, candidate.event_id, str(exc))
                continue

            sent_at = utc_now()
            stats.slack_alerts_sent += 1
            already_alerted.add(candidate.event_id)
            upsert_alert_state(
                connection,
                candidate,
                final_book,
                now=alert_now,
                alert_sent_at=sent_at,
                final_verification_result="confirmed_not_inplay",
                final_verification_reason="alert_sent",
            )
            db_log(
                connection,
                "INFO",
                "slack_alert_sent",
                f"Sent Slack alert for event {candidate.event_id}",
                sport_name=candidate.sport_name,
                event_id=candidate.event_id,
                market_id=market_id,
                event_name=candidate.event_name,
            )


def event_types_by_name(client: APIClient, start_from: datetime, start_to: datetime) -> dict[str, EventType]:
    return {normalize_sport_name(event_type.sport_name): event_type for event_type in list_event_types(client, start_from, start_to)}


def betfair_flashscore_candidates(
    connection: sqlite3.Connection,
    client: APIClient,
    sports: Iterable[str],
    start_from: datetime,
    start_to: datetime,
) -> dict[str, list[MarketCandidate]]:
    by_sport: dict[str, list[MarketCandidate]] = {}
    event_types = event_types_by_name(client, start_from, start_to)
    for sport in sports:
        event_type = event_types.get(normalize_sport_name(sport))
        if event_type is None:
            db_log(connection, "ERROR", "flashscore_live_no_betfair_match", f"No Betfair event type found for {sport}", sport_name=sport)
            by_sport[sport] = []
            continue
        try:
            catalogues = list_market_catalogues(client, event_type, start_from, start_to, 1000)
        except Exception as exc:
            db_log(connection, "ERROR", "flashscore_live_no_betfair_match", f"Betfair catalogue fetch failed for {sport}: {exc}", sport_name=sport)
            by_sport[sport] = []
            continue
        by_sport[sport] = [catalogue_to_candidate(catalogue, event_type) for catalogue in catalogues]
    return by_sport


def best_betfair_match(
    flashscore_match: FlashscoreMatch,
    betfair_candidates: list[MarketCandidate],
) -> tuple[MarketCandidate | None, MatchConfidence]:
    best_candidate: MarketCandidate | None = None
    best_confidence = MatchConfidence("Low", "No Betfair match found", 0.0)
    for candidate in betfair_candidates:
        if normalize_sport_name(candidate.sport_name) != normalize_sport_name(flashscore_match.sport_name):
            continue
        confidence = participant_confidence(
            flashscore_match.participants,
            parse_participants(candidate.event_name),
            flashscore_match.competition_name,
            candidate.competition_name,
        )
        if confidence.score > best_confidence.score:
            best_candidate = candidate
            best_confidence = confidence
    return best_candidate, best_confidence


def build_flashscore_slack_message(
    flashscore_match: FlashscoreMatch,
    candidate: MarketCandidate,
    book: MarketBookSnapshot,
    confidence: MatchConfidence,
) -> str:
    emoji = sport_emoji(flashscore_match.sport_name)
    return "\n".join(
        [
            f":warning: {emoji} Flashscore Live / Betfair Not In-Play",
            "",
            "Flashscore shows this match as live, but the matched Betfair market is not in-play.",
            "",
            "Trigger source: Flashscore",
            f"Sport: {flashscore_match.sport_name}",
            f"Flashscore match: {flashscore_match.match_name}",
            f"Flashscore competition: {flashscore_match.competition_name or 'unknown'}",
            f"Flashscore status: {flashscore_match.status_text or 'Live'}",
            f"Flashscore score: {flashscore_match.score or 'unknown'}",
            "",
            f"Betfair event: {candidate.event_name}",
            f"Betfair Event ID: {candidate.event_id}",
            f"Betfair Market ID: {candidate.market_id}",
            f"Betfair market status: {book.status or 'unknown'}",
            "Betfair in-play: false",
            f"Match confidence: {confidence.level}",
            "",
            "Please check whether this Betfair event should now be turned in-play. React with a tick once handled.",
        ]
    )


def process_flashscore_live_matches(
    connection: sqlite3.Connection,
    client: APIClient,
    config: Config,
    args: argparse.Namespace,
    stats: ScanStats,
    flashscore_matches: list[FlashscoreMatch] | None = None,
) -> None:
    db_log(connection, "INFO", "flashscore_scan_started", "Flashscore live trigger scan started")
    try:
        live_matches = flashscore_matches if flashscore_matches is not None else flashscore_browser_matches(connection, args.flashscore_timeout_seconds)
    except Exception as exc:
        stats.flashscore_scan_status = "failed"
        db_log(connection, "ERROR", "flashscore_scan_completed", f"Flashscore scan failed: {exc}")
        return

    if not live_matches:
        stats.flashscore_scan_status = "complete"
        db_log(connection, "INFO", "flashscore_scan_completed", "Flashscore live trigger scan completed: 0 live matches")
        return

    stats.flashscore_live_matches_found += len(live_matches)
    now = utc_now()
    start_from = now - timedelta(hours=args.flashscore_lookback_hours)
    start_to = now + timedelta(hours=args.flashscore_lookahead_hours)
    candidates_by_sport = betfair_flashscore_candidates(
        connection,
        client,
        sorted({match.sport_name for match in live_matches}),
        start_from,
        start_to,
    )
    already_alerted = alerted_event_ids(connection)

    for flashscore_match in live_matches:
        candidate, confidence = best_betfair_match(
            flashscore_match,
            candidates_by_sport.get(flashscore_match.sport_name, []),
        )
        if candidate is None:
            db_log(
                connection,
                "INFO",
                "flashscore_live_no_betfair_match",
                "Flashscore live match has no Betfair match",
                sport_name=flashscore_match.sport_name,
                event_name=flashscore_match.match_name,
                details={
                    "flashscore_match_name": flashscore_match.match_name,
                    "flashscore_competition": flashscore_match.competition_name,
                    "flashscore_status": flashscore_match.status_text,
                    "reason": confidence.reason,
                },
            )
            continue
        if confidence.level != "High":
            db_log(
                connection,
                "INFO",
                "flashscore_betfair_match_low_confidence",
                "Flashscore Betfair match skipped due to low confidence",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={
                    "flashscore_match_name": flashscore_match.match_name,
                    "flashscore_status": flashscore_match.status_text,
                    "match_confidence": confidence.level,
                    "match_reason": confidence.reason,
                    "score": confidence.score,
                },
            )
            continue

        db_log(
            connection,
            "INFO",
            "flashscore_betfair_match_high_confidence",
            "Flashscore Betfair match high confidence",
            sport_name=flashscore_match.sport_name,
            event_id=candidate.event_id,
            market_id=candidate.market_id,
            event_name=candidate.event_name,
            details={
                "flashscore_match_name": flashscore_match.match_name,
                "flashscore_status": flashscore_match.status_text,
                "match_confidence": confidence.level,
                "match_reason": confidence.reason,
                "score": confidence.score,
            },
        )

        if candidate.event_id in already_alerted:
            upsert_alert_state(
                connection,
                candidate,
                MarketBookSnapshot(candidate.market_id, "", False),
                now=utc_now(),
                trigger_source="flashscore_live",
                flashscore_match=flashscore_match,
                match_confidence=confidence,
                final_verification_result="skipped",
                final_verification_reason="already_alerted",
            )
            db_log(
                connection,
                "INFO",
                "skipped",
                "Skipped Flashscore candidate: already alerted",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={"reason": "already alerted", "flashscore_match_name": flashscore_match.match_name},
            )
            continue

        db_log(
            connection,
            "INFO",
            "flashscore_final_betfair_check_started",
            "Flashscore final Betfair check started",
            sport_name=flashscore_match.sport_name,
            event_id=candidate.event_id,
            market_id=candidate.market_id,
            event_name=candidate.event_name,
            details={"flashscore_match_name": flashscore_match.match_name},
        )
        try:
            final_book = list_market_books(client, [candidate.market_id]).get(candidate.market_id)
            if final_book is None:
                raise RuntimeError("No MarketBook returned for Flashscore final check")
        except Exception as exc:
            stats.api_errors += 1
            db_log(
                connection,
                "ERROR",
                "final_verification_failed",
                f"Flashscore final Betfair check failed: {exc}",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={"reason": str(exc), "flashscore_match_name": flashscore_match.match_name},
            )
            continue

        checked_at = utc_now()
        if final_book.inplay:
            upsert_alert_state(
                connection,
                candidate,
                final_book,
                now=checked_at,
                trigger_source="flashscore_live",
                flashscore_match=flashscore_match,
                match_confidence=confidence,
                final_verification_at=checked_at,
                final_verification_result="suppressed",
                final_verification_reason="betfair_inplay",
            )
            db_log(
                connection,
                "INFO",
                "flashscore_candidate_suppressed_betfair_inplay",
                "Flashscore candidate suppressed: Betfair is in-play",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={"status": final_book.status, "inplay": final_book.inplay, "flashscore_match_name": flashscore_match.match_name},
            )
            continue
        if final_book.status == "CLOSED":
            upsert_alert_state(
                connection,
                candidate,
                final_book,
                now=checked_at,
                trigger_source="flashscore_live",
                flashscore_match=flashscore_match,
                match_confidence=confidence,
                final_verification_at=checked_at,
                final_verification_result="suppressed",
                final_verification_reason="betfair_closed",
            )
            db_log(
                connection,
                "INFO",
                "flashscore_candidate_suppressed_betfair_closed",
                "Flashscore candidate suppressed: Betfair market is closed",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={"status": final_book.status, "inplay": final_book.inplay, "flashscore_match_name": flashscore_match.match_name},
            )
            continue
        if final_book.status not in ALERTABLE_STATUSES:
            db_log(
                connection,
                "INFO",
                "skipped",
                f"Skipped Flashscore candidate: Betfair status {final_book.status or 'unknown'}",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={"reason": f"status {final_book.status or 'unknown'}", "flashscore_match_name": flashscore_match.match_name},
            )
            continue

        upsert_alert_state(
            connection,
            candidate,
            final_book,
            now=checked_at,
            trigger_source="flashscore_live",
            flashscore_match=flashscore_match,
            match_confidence=confidence,
            final_verification_at=checked_at,
            final_verification_result="confirmed_not_inplay",
            final_verification_reason="flashscore_live_betfair_not_inplay",
        )
        db_log(
            connection,
            "INFO",
            "flashscore_final_betfair_check_not_inplay",
            "Flashscore final Betfair check confirmed not in-play",
            sport_name=flashscore_match.sport_name,
            event_id=candidate.event_id,
            market_id=candidate.market_id,
            event_name=candidate.event_name,
            details={"status": final_book.status, "inplay": final_book.inplay, "flashscore_match_name": flashscore_match.match_name},
        )

        stats.flags_found += 1
        message = build_flashscore_slack_message(flashscore_match, candidate, final_book, confidence)
        print("", flush=True)
        print(message, flush=True)
        if args.dry_run:
            db_log(
                connection,
                "INFO",
                "dry_run_alert",
                "Dry-run: would send Flashscore Slack alert",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={"flashscore_match_name": flashscore_match.match_name},
            )
            continue
        if is_placeholder(config.slack_webhook_url):
            stats.slack_alert_failures += 1
            db_log(connection, "ERROR", "config_error", f"{SLACK_WEBHOOK_ENV_NAME} missing", sport_name=flashscore_match.sport_name, event_id=candidate.event_id, market_id=candidate.market_id, event_name=candidate.event_name)
            record_slack_error(connection, candidate.event_id, f"{SLACK_WEBHOOK_ENV_NAME} missing")
            continue
        try:
            send_slack_message(config.slack_webhook_url, message)
        except Exception as exc:
            stats.slack_alert_failures += 1
            db_log(
                connection,
                "ERROR",
                "slack_alert_failed",
                f"Flashscore Slack alert failed: {exc}",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
            )
            record_slack_error(connection, candidate.event_id, str(exc))
            continue
        sent_at = utc_now()
        stats.slack_alerts_sent += 1
        already_alerted.add(candidate.event_id)
        upsert_alert_state(
            connection,
            candidate,
            final_book,
            now=checked_at,
            alert_sent_at=sent_at,
            trigger_source="flashscore_live",
            flashscore_match=flashscore_match,
            match_confidence=confidence,
            final_verification_result="confirmed_not_inplay",
            final_verification_reason="flashscore_alert_sent",
        )
        db_log(
            connection,
            "INFO",
            "slack_alert_sent",
            f"Sent Flashscore Slack alert for event {candidate.event_id}",
            sport_name=flashscore_match.sport_name,
            event_id=candidate.event_id,
            market_id=candidate.market_id,
            event_name=candidate.event_name,
            details={"flashscore_match_name": flashscore_match.match_name},
        )

    stats.flashscore_scan_status = "complete"
    db_log(connection, "INFO", "flashscore_scan_completed", f"Flashscore live trigger scan completed: {len(live_matches)} live matches")


def run_scan(args: argparse.Namespace, config: Config, connection: sqlite3.Connection) -> int:
    run_id = start_scan_run(connection, args)
    stats = ScanStats()
    excluded_sports: list[str] = []
    status = "complete"
    config_error = ""
    db_log(connection, "INFO", "full_run_started", "Betfair In-Play Start Checker full run started")
    log(f"Slack webhook: {mask_webhook(config.slack_webhook_url)}")
    if args.dry_run:
        db_log(connection, "INFO", "dry_run", "Dry-run enabled: Slack alerts will not be sent")
    elif is_placeholder(config.slack_webhook_url):
        config_error = f"{SLACK_WEBHOOK_ENV_NAME} missing"
        db_log(connection, "ERROR", "config_error", f"{SLACK_WEBHOOK_ENV_NAME} missing")

    client: APIClient | None = None
    try:
        client = build_client(config)
    except Exception as exc:
        status = "failed"
        stats.api_errors += 1
        stats.betfair_time_scan_status = "failed"
        stats.flashscore_scan_status = "failed"
        db_log(connection, "ERROR", "api_error", f"Betfair API client setup failed: {exc}")
        traceback.print_exc(file=sys.stdout)
        finish_scan_run(connection, run_id, status, stats, excluded_sports, config_error=config_error)
        return 1 if not args.repeat_minutes else 0

    try:
        db_log(connection, "INFO", "betfair_time_scan_started", "Betfair scheduled-time scan started")
        try:
            candidates, excluded_sports = fetch_overdue_candidates(connection, client, args, stats)
            process_candidates(connection, client, config, args, candidates, stats)
            stats.betfair_time_scan_status = "complete"
            db_log(connection, "INFO", "betfair_time_scan_completed", "Betfair scheduled-time scan completed")
        except Exception as exc:
            status = "partial_failure"
            stats.api_errors += 1
            stats.betfair_time_scan_status = "failed"
            db_log(connection, "ERROR", "api_error", f"Betfair scheduled-time scan failed: {exc}")
            traceback.print_exc(file=sys.stdout)

        if args.disable_flashscore:
            stats.flashscore_scan_status = "disabled"
            db_log(connection, "INFO", "flashscore_scan_completed", "Flashscore live trigger scan disabled")
        else:
            try:
                process_flashscore_live_matches(connection, client, config, args, stats)
                if stats.flashscore_scan_status == "not_run":
                    stats.flashscore_scan_status = "complete"
            except Exception as exc:
                status = "partial_failure"
                stats.api_errors += 1
                stats.flashscore_scan_status = "failed"
                db_log(connection, "ERROR", "flashscore_scan_completed", f"Flashscore live trigger scan failed: {exc}")
                traceback.print_exc(file=sys.stdout)
    except Exception as exc:
        status = "failed"
        stats.api_errors += 1
        db_log(connection, "ERROR", "api_error", f"Full run failed: {exc}")
        traceback.print_exc(file=sys.stdout)
        if not args.repeat_minutes:
            finish_scan_run(connection, run_id, status, stats, excluded_sports, config_error=config_error)
            return 1
    finally:
        if client is not None:
            try:
                client.logout()
            except Exception:
                pass

    finish_scan_run(connection, run_id, status, stats, excluded_sports, config_error=config_error)
    db_log(
        connection,
        "INFO",
        "full_run_completed",
        (
            "Betfair In-Play Start Checker full run completed: "
            f"markets={stats.markets_scanned}, events_checked={stats.events_checked}, "
            f"flashscore_live={stats.flashscore_live_matches_found}, "
            f"flags={stats.flags_found}, slack_sent={stats.slack_alerts_sent}, "
            f"slack_failures={stats.slack_alert_failures}, api_errors={stats.api_errors}"
        ),
    )
    return 0 if status in {"complete", "partial_failure"} else 1


def mark_next_scan(connection: sqlite3.Connection, next_scan_at: datetime) -> None:
    row = connection.execute("SELECT id FROM inplay_scan_runs ORDER BY id DESC LIMIT 1").fetchone()
    if not row:
        return
    connection.execute("UPDATE inplay_scan_runs SET next_scan_at = ? WHERE id = ?", (iso_utc(next_scan_at), row["id"]))
    connection.commit()


def run_self_test() -> int:
    now = datetime(2026, 6, 19, 12, 5, tzinfo=timezone.utc)
    overdue = now - timedelta(minutes=4, seconds=22)
    future = now - timedelta(seconds=90)
    candidate = MarketCandidate("Cricket", "4", "123456789", "India v Australia", "ICC T20 World Cup", "1.234", overdue)
    suspended = MarketBookSnapshot("1.234", "SUSPENDED", False)
    open_book = MarketBookSnapshot("1.234", "OPEN", False)
    inplay_book = MarketBookSnapshot("1.234", "OPEN", True)
    closed_book = MarketBookSnapshot("1.234", "CLOSED", False)
    not_overdue_candidate = MarketCandidate("Cricket", "4", "123456789", "India v Australia", "", "1.234", future)

    assert alert_decision(candidate, open_book, now, set(), 2).should_alert
    assert alert_decision(candidate, suspended, now, set(), 2).should_alert
    assert "SUSPENDED" in build_slack_message(candidate, suspended, now)
    assert not alert_decision(candidate, inplay_book, now, set(), 2).should_alert
    assert alert_decision(candidate, inplay_book, now, set(), 2).reason == "already in-play"
    assert not alert_decision(not_overdue_candidate, open_book, now, set(), 2).should_alert
    assert alert_decision(not_overdue_candidate, open_book, now, set(), 2).reason == "not overdue"
    assert not alert_decision(candidate, closed_book, now, set(), 2).should_alert
    assert alert_decision(candidate, closed_book, now, set(), 2).reason == "closed"
    assert not alert_decision(candidate, open_book, now, {"123456789"}, 2).should_alert
    assert alert_decision(candidate, open_book, now, {"123456789"}, 2).reason == "already alerted"
    assert is_excluded_sport("Tennis")
    assert is_excluded_sport("Football")
    assert is_excluded_sport("Soccer")
    assert is_excluded_sport("Horse Racing")
    assert is_excluded_sport("Greyhound Racing")
    assert not is_excluded_sport("Cricket")

    try:
        send_slack_message("", "test")
        raise AssertionError("missing webhook should fail")
    except RuntimeError as exc:
        assert f"{SLACK_WEBHOOK_ENV_NAME} missing" in str(exc)

    class FailedResponse:
        status_code = 500
        text = "server error"

    try:
        send_slack_message("https://hooks.slack.com/services/test", "test", post_func=lambda *a, **k: FailedResponse())
        raise AssertionError("webhook failure should fail")
    except RuntimeError as exc:
        assert "Slack webhook failed" in str(exc)

    message = build_slack_message(candidate, open_book, now)
    assert ":cricket_bat_and_ball:" in message
    assert "Match ID: 123456789" in message
    assert "Overdue by: 4m 22s" in message
    assert "In-play: false" in message

    class FakeBetting:
        def list_market_book(self, market_ids: list[str]) -> list[dict[str, Any]]:
            return [{"market_id": market_id, "status": "OPEN", "inplay": False} for market_id in market_ids]

    class FakeClient:
        betting = FakeBetting()

    runtime_now = utc_now()
    runtime_candidate = MarketCandidate(
        "Cricket",
        "4",
        "runtime-event-1",
        "India v Australia",
        "ICC T20 World Cup",
        "1.runtime",
        runtime_now - timedelta(minutes=4, seconds=22),
    )

    dry_run_db = sqlite3.connect(":memory:")
    dry_run_db.row_factory = sqlite3.Row
    init_db(dry_run_db)
    dry_run_stats = ScanStats()
    dry_run_args = argparse.Namespace(dry_run=True, market_book_batch_size=40, overdue_minutes=2)
    process_candidates(
        dry_run_db,
        FakeClient(),  # type: ignore[arg-type]
        Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
        dry_run_args,
        [runtime_candidate],
        dry_run_stats,
    )
    assert dry_run_stats.flags_found == 1
    assert dry_run_stats.slack_alerts_sent == 0
    assert dry_run_db.execute("SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'dry_run_alert'").fetchone()[0] == 1
    dry_run_state = dry_run_db.execute(
        "SELECT final_verification_result, last_seen_inplay FROM inplay_alert_state WHERE event_id = ?",
        (runtime_candidate.event_id,),
    ).fetchone()
    assert dry_run_state["final_verification_result"] == "confirmed_not_inplay"
    assert dry_run_state["last_seen_inplay"] == 0
    dry_run_db.close()

    sent_messages: list[str] = []

    def fake_send_slack_message(webhook_url: str, text: str) -> None:
        sent_messages.append(text)

    real_send_slack_message = globals()["send_slack_message"]
    globals()["send_slack_message"] = fake_send_slack_message
    try:
        duplicate_db = sqlite3.connect(":memory:")
        duplicate_db.row_factory = sqlite3.Row
        init_db(duplicate_db)
        send_args = argparse.Namespace(dry_run=False, market_book_batch_size=40, overdue_minutes=2)
        first_stats = ScanStats()
        second_stats = ScanStats()
        process_candidates(
            duplicate_db,
            FakeClient(),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            send_args,
            [runtime_candidate],
            first_stats,
        )
        process_candidates(
            duplicate_db,
            FakeClient(),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            send_args,
            [runtime_candidate],
            second_stats,
        )
        assert len(sent_messages) == 1
        assert first_stats.slack_alerts_sent == 1
        assert second_stats.slack_alerts_sent == 0
        assert second_stats.skipped_events == 1
        duplicate_db.close()

        class FinalInplayBetting:
            def __init__(self) -> None:
                self.calls = 0

            def list_market_book(self, market_ids: list[str]) -> list[dict[str, Any]]:
                self.calls += 1
                inplay = self.calls == 2
                return [{"market_id": market_id, "status": "OPEN", "inplay": inplay} for market_id in market_ids]

        class FinalInplayClient:
            def __init__(self) -> None:
                self.betting = FinalInplayBetting()

        inplay_db = sqlite3.connect(":memory:")
        inplay_db.row_factory = sqlite3.Row
        init_db(inplay_db)
        inplay_stats = ScanStats()
        process_candidates(
            inplay_db,
            FinalInplayClient(),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            send_args,
            [runtime_candidate],
            inplay_stats,
        )
        inplay_state = inplay_db.execute(
            "SELECT last_seen_inplay, final_verification_result, final_verification_reason FROM inplay_alert_state WHERE event_id = ?",
            (runtime_candidate.event_id,),
        ).fetchone()
        assert inplay_stats.slack_alerts_sent == 0
        assert inplay_state["last_seen_inplay"] == 1
        assert inplay_state["final_verification_result"] == "suppressed"
        assert inplay_state["final_verification_reason"] == "inplay"
        assert inplay_db.execute(
            "SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'candidate_suppressed_final_check_inplay'"
        ).fetchone()[0] == 1
        inplay_db.close()

        class FinalFailureBetting:
            def __init__(self) -> None:
                self.calls = 0

            def list_market_book(self, market_ids: list[str]) -> list[dict[str, Any]]:
                self.calls += 1
                if self.calls == 2:
                    raise RuntimeError("final lookup failed")
                return [{"market_id": market_id, "status": "OPEN", "inplay": False} for market_id in market_ids]

        class FinalFailureClient:
            def __init__(self) -> None:
                self.betting = FinalFailureBetting()

        failure_db = sqlite3.connect(":memory:")
        failure_db.row_factory = sqlite3.Row
        init_db(failure_db)
        failure_stats = ScanStats()
        process_candidates(
            failure_db,
            FinalFailureClient(),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            send_args,
            [runtime_candidate],
            failure_stats,
        )
        failure_state = failure_db.execute(
            "SELECT last_seen_inplay, final_verification_result, final_verification_reason FROM inplay_alert_state WHERE event_id = ?",
            (runtime_candidate.event_id,),
        ).fetchone()
        assert failure_stats.slack_alerts_sent == 0
        assert failure_state["last_seen_inplay"] is None
        assert failure_state["final_verification_result"] == "failed"
        assert "final lookup failed" in failure_state["final_verification_reason"]
        assert failure_db.execute(
            "SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'final_verification_failed'"
        ).fetchone()[0] == 1
        failure_db.close()

        class FlashscoreFakeBetting:
            def __init__(self, status: str = "OPEN", inplay: bool = False, include_tennis: bool = True, include_darts: bool = True) -> None:
                self.status = status
                self.inplay = inplay
                self.include_tennis = include_tennis
                self.include_darts = include_darts

            def list_event_types(self, filter: Any) -> list[dict[str, Any]]:
                event_types = []
                if self.include_tennis:
                    event_types.append({"event_type": {"id": "2", "name": "Tennis"}})
                if self.include_darts:
                    event_types.append({"event_type": {"id": "15", "name": "Darts"}})
                return event_types

            def list_market_catalogue(self, filter: Any, market_projection: list[str], sort: str, max_results: int) -> list[dict[str, Any]]:
                event_type_ids = filter.get("eventTypeIds") or filter.get("event_type_ids") or []
                event_type_id = str(event_type_ids[0]) if event_type_ids else "2"
                if event_type_id == "15":
                    return [
                        {
                            "market_id": "1.darts",
                            "market_name": "Match Odds",
                            "event": {"id": "bf-darts-1", "name": "Luke Littler v Michael Smith"},
                            "event_type": {"id": "15", "name": "Darts"},
                            "competition": {"name": "Premier League Darts"},
                            "market_start_time": utc_now(),
                        }
                    ]
                return [
                    {
                        "market_id": "1.tennis",
                        "market_name": "Match Odds",
                        "event": {"id": "bf-tennis-1", "name": "Player A v Player B"},
                        "event_type": {"id": "2", "name": "Tennis"},
                        "competition": {"name": "ATP Challenger Example"},
                        "market_start_time": utc_now(),
                    }
                ]

            def list_market_book(self, market_ids: list[str]) -> list[dict[str, Any]]:
                return [{"market_id": market_id, "status": self.status, "inplay": self.inplay} for market_id in market_ids]

        class FlashscoreFakeClient:
            def __init__(self, betting: FlashscoreFakeBetting) -> None:
                self.betting = betting

        flash_args = argparse.Namespace(
            dry_run=False,
            flashscore_timeout_seconds=1,
            flashscore_lookback_hours=12.0,
            flashscore_lookahead_hours=24.0,
        )
        tennis_flash = FlashscoreMatch(
            "Tennis",
            "Player A v Player B",
            "ATP Challenger Example",
            "Live - Set 1",
            "1-0",
            "fs-tennis-1",
            "https://www.flashscore.com/match/fs-tennis-1/",
            utc_now(),
            ("Player A", "Player B"),
        )
        darts_flash = FlashscoreMatch(
            "Darts",
            "Luke Littler v Michael Smith",
            "Premier League Darts",
            "Live - Leg 1",
            "1-0",
            "fs-darts-1",
            "https://www.flashscore.com/match/fs-darts-1/",
            utc_now(),
            ("Luke Littler", "Michael Smith"),
        )

        flash_db = sqlite3.connect(":memory:")
        flash_db.row_factory = sqlite3.Row
        init_db(flash_db)
        flash_stats = ScanStats()
        process_flashscore_live_matches(
            flash_db,
            FlashscoreFakeClient(FlashscoreFakeBetting()),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            flash_stats,
            [tennis_flash],
        )
        assert flash_stats.slack_alerts_sent == 1
        assert any("Flashscore Live" in message for message in sent_messages)
        assert flash_db.execute("SELECT COUNT(*) FROM inplay_alert_state WHERE trigger_source = 'flashscore_live' AND alert_sent_at IS NOT NULL").fetchone()[0] == 1
        flash_db.close()

        darts_db = sqlite3.connect(":memory:")
        darts_db.row_factory = sqlite3.Row
        init_db(darts_db)
        darts_stats = ScanStats()
        process_flashscore_live_matches(
            darts_db,
            FlashscoreFakeClient(FlashscoreFakeBetting()),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            darts_stats,
            [darts_flash],
        )
        assert darts_stats.slack_alerts_sent == 1
        darts_db.close()

        suppress_db = sqlite3.connect(":memory:")
        suppress_db.row_factory = sqlite3.Row
        init_db(suppress_db)
        suppress_stats = ScanStats()
        process_flashscore_live_matches(
            suppress_db,
            FlashscoreFakeClient(FlashscoreFakeBetting(inplay=True)),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            suppress_stats,
            [tennis_flash],
        )
        assert suppress_stats.slack_alerts_sent == 0
        assert suppress_db.execute("SELECT last_seen_inplay FROM inplay_alert_state WHERE event_id = 'bf-tennis-1'").fetchone()[0] == 1
        suppress_db.close()

        closed_db = sqlite3.connect(":memory:")
        closed_db.row_factory = sqlite3.Row
        init_db(closed_db)
        closed_stats = ScanStats()
        process_flashscore_live_matches(
            closed_db,
            FlashscoreFakeClient(FlashscoreFakeBetting(status="CLOSED")),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            closed_stats,
            [tennis_flash],
        )
        assert closed_stats.slack_alerts_sent == 0
        assert closed_db.execute("SELECT last_seen_status FROM inplay_alert_state WHERE event_id = 'bf-tennis-1'").fetchone()[0] == "CLOSED"
        closed_db.close()

        no_match_db = sqlite3.connect(":memory:")
        no_match_db.row_factory = sqlite3.Row
        init_db(no_match_db)
        no_match_stats = ScanStats()
        process_flashscore_live_matches(
            no_match_db,
            FlashscoreFakeClient(FlashscoreFakeBetting(include_tennis=False, include_darts=False)),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            no_match_stats,
            [tennis_flash],
        )
        assert no_match_stats.slack_alerts_sent == 0
        assert no_match_db.execute("SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'flashscore_live_no_betfair_match'").fetchone()[0] >= 1
        no_match_db.close()
    finally:
        globals()["send_slack_message"] = real_send_slack_message

    log("Self-test passed.")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Betfair MATCH_ODDS markets that should be in-play.")
    parser.add_argument("--dry-run", action="store_true", help="Scan normally but do not send Slack alerts.")
    parser.add_argument("--self-test", action="store_true", help="Run fixture-based checks and exit.")
    parser.add_argument("--repeat-minutes", type=float, default=0, help="Repeat scans every N minutes until stopped.")
    parser.add_argument("--lookback-hours", type=float, default=DEFAULT_LOOKBACK_HOURS)
    parser.add_argument("--lookahead-hours", type=float, default=DEFAULT_LOOKAHEAD_HOURS)
    parser.add_argument("--overdue-minutes", type=float, default=DEFAULT_OVERDUE_MINUTES)
    parser.add_argument("--max-results", type=int, default=1000)
    parser.add_argument("--market-book-batch-size", type=int, default=DEFAULT_MARKET_BOOK_BATCH_SIZE)
    parser.add_argument("--disable-flashscore", action="store_true", help="Disable the Flashscore live-trigger scanner.")
    parser.add_argument("--flashscore-timeout-seconds", type=int, default=12)
    parser.add_argument("--flashscore-lookback-hours", type=float, default=12.0)
    parser.add_argument("--flashscore-lookahead-hours", type=float, default=24.0)
    parser.add_argument("--send-startup-message", action="store_true", help="Send a Slack message when the scanner starts.")
    parser.add_argument("--send-shutdown-message", action="store_true", help="Send a Slack message when the scanner stops.")
    parser.add_argument("--pause-on-exit", action="store_true", help="Wait for Enter before closing the console.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        return run_self_test()

    config = load_config()
    connection = open_db()
    repeat_minutes = max(args.repeat_minutes, 0)
    cycle = 1
    if args.send_startup_message:
        send_lifecycle_message(
            connection,
            config,
            dry_run=args.dry_run,
            event_type="startup_message",
            message=startup_message(repeat_minutes, args.dry_run),
        )
    try:
        while True:
            if repeat_minutes:
                log(f"Starting in-play start scan cycle {cycle}.")
            exit_code = run_scan(args, config, connection)
            if not repeat_minutes:
                return exit_code

            cycle += 1
            sleep_seconds = repeat_minutes * 60
            next_scan_at = utc_now() + timedelta(seconds=sleep_seconds)
            mark_next_scan(connection, next_scan_at)
            log(f"Next in-play start scan scheduled for {format_uk_datetime(next_scan_at)}.")
            time.sleep(sleep_seconds)
    except KeyboardInterrupt:
        log("Interrupted.")
        if args.send_shutdown_message:
            send_lifecycle_message(
                connection,
                config,
                dry_run=args.dry_run,
                event_type="shutdown_message",
                message=shutdown_message(),
            )
        return 130
    finally:
        connection.close()


def pause_before_exit() -> None:
    print("Betfair in-play start check finished. Press Enter to close...", flush=True)
    try:
        input()
    except EOFError:
        pass


def raise_keyboard_interrupt(signum: int, frame: Any) -> None:
    raise KeyboardInterrupt


if __name__ == "__main__":
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, raise_keyboard_interrupt)
    signal.signal(signal.SIGTERM, raise_keyboard_interrupt)

    exit_code = 0
    try:
        exit_code = main()
    except KeyboardInterrupt:
        log("Interrupted.")
        exit_code = 130
    except Exception as exc:
        log(f"ERROR: {exc}")
        traceback.print_exc()
        exit_code = 1
    finally:
        if "--pause-on-exit" in sys.argv:
            pause_before_exit()

    raise SystemExit(exit_code)
