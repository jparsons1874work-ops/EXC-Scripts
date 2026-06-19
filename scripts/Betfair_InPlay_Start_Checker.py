#!/usr/bin/env python3
"""Check Betfair MATCH_ODDS markets that missed their in-play start."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
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
EXCLUDED_SPORT_NAMES = {"tennis", "football", "soccer", "horse racing", "greyhound racing"}
ALERTABLE_STATUSES = {"OPEN", "SUSPENDED"}
DEFAULT_LOOKBACK_HOURS = 6.0
DEFAULT_LOOKAHEAD_HOURS = 24.0
DEFAULT_OVERDUE_MINUTES = 2.0
DEFAULT_MARKET_BOOK_BATCH_SIZE = 40
SLACK_WEBHOOK_ENV_NAME = "Slack_Webhook_TIP"


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
            last_checked_at TEXT
        )
        """
    )
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
        )
        """
    )
    connection.commit()


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
            config_error = ?
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
) -> None:
    first_flagged_at = iso_utc(now)
    existing = connection.execute(
        "SELECT first_flagged_at, alert_sent_at, recovered_at FROM inplay_alert_state WHERE event_id = ?",
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
            last_seen_status, last_seen_inplay, recovered_at, last_checked_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            last_checked_at = excluded.last_checked_at
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
        ),
    )
    connection.commit()


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
        db_log(connection, "INFO", "markets_scanned", f"{event_type.sport_name}: {len(catalogues)} MATCH_ODDS markets")
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

            stats.flags_found += 1
            handled_this_scan.add(candidate.event_id)
            message = build_slack_message(candidate, book, now)
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
                upsert_alert_state(connection, candidate, book, now=now)
                continue

            try:
                send_slack_message(config.slack_webhook_url, message)
            except Exception as exc:
                stats.slack_alert_failures += 1
                db_log(
                    connection,
                    "ERROR",
                    "slack_failure",
                    f"Slack alert failed: {exc}",
                    sport_name=candidate.sport_name,
                    event_id=candidate.event_id,
                    market_id=market_id,
                    event_name=candidate.event_name,
                )
                upsert_alert_state(connection, candidate, book, now=now)
                continue

            sent_at = utc_now()
            stats.slack_alerts_sent += 1
            already_alerted.add(candidate.event_id)
            upsert_alert_state(connection, candidate, book, now=now, alert_sent_at=sent_at)
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


def run_scan(args: argparse.Namespace, config: Config, connection: sqlite3.Connection) -> int:
    run_id = start_scan_run(connection, args)
    stats = ScanStats()
    excluded_sports: list[str] = []
    status = "complete"
    config_error = ""
    db_log(connection, "INFO", "scan_started", "Betfair in-play start scan started")
    log(f"Slack webhook: {mask_webhook(config.slack_webhook_url)}")
    if args.dry_run:
        db_log(connection, "INFO", "dry_run", "Dry-run enabled: Slack alerts will not be sent")
    elif is_placeholder(config.slack_webhook_url):
        config_error = f"{SLACK_WEBHOOK_ENV_NAME} missing"
        db_log(connection, "ERROR", "config_error", f"{SLACK_WEBHOOK_ENV_NAME} missing")

    client: APIClient | None = None
    try:
        client = build_client(config)
        candidates, excluded_sports = fetch_overdue_candidates(connection, client, args, stats)
        process_candidates(connection, client, config, args, candidates, stats)
    except Exception as exc:
        status = "failed"
        stats.api_errors += 1
        db_log(connection, "ERROR", "api_error", f"Scan failed: {exc}")
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
        "scan_completed",
        (
            "Betfair in-play start scan completed: "
            f"markets={stats.markets_scanned}, events_checked={stats.events_checked}, "
            f"flags={stats.flags_found}, slack_sent={stats.slack_alerts_sent}, "
            f"slack_failures={stats.slack_alert_failures}, api_errors={stats.api_errors}"
        ),
    )
    return 0 if status == "complete" else 1


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
    assert dry_run_db.execute("SELECT COUNT(*) FROM inplay_alert_state").fetchone()[0] == 0
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
