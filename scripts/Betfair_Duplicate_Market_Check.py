#!/usr/bin/env python3
"""Check Betfair Exchange events for duplicate market names."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import requests
from betfairlightweight import APIClient
from betfairlightweight.filters import market_filter


SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPT_MANAGER_ROOT = SCRIPT_DIR.parent

# ============================================================
# USER CONFIG PLACEHOLDERS
# ============================================================

BETFAIR_USERNAME_PLACEHOLDER = "YOUR_BETFAIR_USERNAME"
BETFAIR_PASSWORD_PLACEHOLDER = "YOUR_BETFAIR_PASSWORD"
BETFAIR_APP_KEY_PLACEHOLDER = "YOUR_BETFAIR_APP_KEY"

# Optional. Leave blank to use the default portable cert path:
# Scripts/Integrity-Scanner/certs
BETFAIR_CERTS_DIR_PLACEHOLDER = ""

# Optional. Leave blank to load from local JSON or environment:
SLACK_WEBHOOK_URL_PLACEHOLDER = ""

BETFAIR_CERTS_DIR = Path(
    os.environ.get(
        "BETFAIR_CERTS_DIR",
        BETFAIR_CERTS_DIR_PLACEHOLDER or SCRIPT_DIR / "Integrity-Scanner" / "certs",
    )
).resolve()
BETFAIR_CERT_FILE = BETFAIR_CERTS_DIR / "client-2048.crt"
BETFAIR_KEY_FILE = BETFAIR_CERTS_DIR / "client-2048.key"

UK_TZ = ZoneInfo("Europe/London")
UTC_TZ = ZoneInfo("UTC")
PLACEHOLDER_PREFIXES = ("YOUR_", "PASTE_", "CHANGE_ME", "TODO")
FOOTBALL_EVENT_TYPE_ID = "1"
FOOTBALL_SPORT_NAME = "Soccer"
DEFAULT_LOOKBACK_HOURS = 6
DEFAULT_LOOKAHEAD_HOURS = 48
DEFAULT_LOOKAHEAD_DAYS = 0
DEFAULT_EVENT_TYPE_IDS: tuple[str, ...] = (FOOTBALL_EVENT_TYPE_ID,)
DEFAULT_INITIAL_CHUNK_HOURS = 6
DEFAULT_MIN_CHUNK_MINUTES = 5
DEFAULT_MAX_RESULTS = 500
ALERT_STATE_PATH = SCRIPT_DIR / "Betfair_Duplicate_Market_Check.state.json"


@dataclass(frozen=True)
class Config:
    betfair_username: str
    betfair_password: str
    betfair_app_key: str
    betfair_certs_path: str
    slack_webhook_url: str
    betfair_credentials_source: str
    slack_config_source: str


@dataclass(frozen=True)
class Market:
    sport_name: str
    event_type_id: str
    event_id: str
    event_name: str
    competition_id: str
    competition_name: str
    market_id: str
    market_name: str
    normalised_market_name: str
    market_start_time_utc: datetime | None


@dataclass(frozen=True)
class FootballEvent:
    event_id: str
    event_name: str
    start_utc: datetime | None
    competition_id: str = ""
    competition_name: str = ""


@dataclass(frozen=True)
class DuplicateMarketGroup:
    sport_name: str
    event_type_id: str
    event_id: str
    event_name: str
    competition_id: str
    competition_name: str
    market_name: str
    normalised_market_name: str
    markets: tuple[Market, ...]


@dataclass
class CatalogueFetchStats:
    catalogue_chunks_requested: int = 0
    catalogue_chunks_split: int = 0
    catalogue_chunks_failed: int = 0
    catalogue_chunks_truncated_and_split: int = 0


def log(message: str) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            return json.load(handle)
    except Exception as exc:
        log(f"Could not read config {path}: {exc}")
        return {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def nested_get(data: dict[str, Any], *keys: str) -> str:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return ""
        current = current.get(key)
    return str(current or "").strip()


def first_config_value_with_source(
    config_sources: Iterable[tuple[str, dict[str, Any]]],
    paths: Iterable[tuple[str, ...]],
) -> tuple[str, str]:
    for source, config in config_sources:
        for path in paths:
            value = nested_get(config, *path)
            if value:
                return value, source
    return "", ""


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


def load_config() -> Config:
    market_config_paths = [
        SCRIPT_DIR / "Betfair_Duplicate_Market_Check.local.json",
        SCRIPT_MANAGER_ROOT / "betfair_duplicate_market_check.local.json",
    ]
    match_config_paths = [
        SCRIPT_DIR / "Betfair_Duplicate_Match_Check.local.json",
        SCRIPT_MANAGER_ROOT / "betfair_duplicate_match_check.local.json",
    ]
    market_config_sources = [(str(path), read_json(path)) for path in market_config_paths]
    match_config_sources = [(str(path), read_json(path)) for path in match_config_paths]

    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    slack_config_source = "SLACK_WEBHOOK_URL" if slack_webhook_url else ""
    if not slack_webhook_url:
        slack_webhook_url, slack_config_source = first_config_value_with_source(
            [*market_config_sources, *match_config_sources],
            [("slack", "webhook_url")],
        )
    if not slack_webhook_url:
        slack_webhook_url = SLACK_WEBHOOK_URL_PLACEHOLDER.strip()
        slack_config_source = "placeholders" if slack_webhook_url else "not configured"
    if is_placeholder(slack_webhook_url):
        slack_webhook_url = ""
        slack_config_source = "not configured"

    betfair_username = os.getenv("BETFAIR_USERNAME", "").strip()
    betfair_password = os.getenv("BETFAIR_PASSWORD", "").strip()
    betfair_app_key = os.getenv("BETFAIR_APP_KEY", "").strip()
    betfair_credentials_source = "environment" if betfair_username and betfair_password and betfair_app_key else ""
    if not betfair_credentials_source:
        betfair_username, username_source = first_config_value_with_source(
            market_config_sources,
            [("betfair", "username")],
        )
        betfair_password, password_source = first_config_value_with_source(
            market_config_sources,
            [("betfair", "password")],
        )
        betfair_app_key, app_key_source = first_config_value_with_source(
            market_config_sources,
            [("betfair", "app_key")],
        )
        if betfair_username and betfair_password and betfair_app_key:
            sources = {username_source, password_source, app_key_source}
            betfair_credentials_source = username_source if len(sources) == 1 else "local config"
    if not betfair_credentials_source:
        betfair_username = BETFAIR_USERNAME_PLACEHOLDER
        betfair_password = BETFAIR_PASSWORD_PLACEHOLDER
        betfair_app_key = BETFAIR_APP_KEY_PLACEHOLDER
        betfair_credentials_source = "placeholders"
    if is_placeholder(betfair_username) or is_placeholder(betfair_password) or is_placeholder(betfair_app_key):
        betfair_credentials_source = "placeholders"

    certs_dir_from_config, _certs_source = first_config_value_with_source(
        market_config_sources,
        [("betfair", "certs_dir"), ("betfair", "certs_path")],
    )
    certs_path = (
        os.getenv("BETFAIR_CERTS_DIR", "").strip()
        or certs_dir_from_config
        or BETFAIR_CERTS_DIR_PLACEHOLDER.strip()
        or str(BETFAIR_CERTS_DIR)
    )

    return Config(
        betfair_username=betfair_username,
        betfair_password=betfair_password,
        betfair_app_key=betfair_app_key,
        betfair_certs_path=resolve_path(certs_path, SCRIPT_DIR / "Integrity-Scanner"),
        slack_webhook_url=slack_webhook_url,
        betfair_credentials_source=betfair_credentials_source,
        slack_config_source=slack_config_source or "not configured",
    )


def require_betfair_config(config: Config) -> None:
    if (
        is_placeholder(config.betfair_username)
        or is_placeholder(config.betfair_password)
        or is_placeholder(config.betfair_app_key)
    ):
        raise RuntimeError(
            "Betfair credentials are not configured. Set BETFAIR_USERNAME, BETFAIR_PASSWORD, "
            "BETFAIR_APP_KEY, or create Betfair_Duplicate_Market_Check.local.json."
        )


def object_get(obj: Any, name: str, default: Any = "") -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC_TZ)
        return value.astimezone(UTC_TZ)
    if isinstance(value, str):
        cleaned = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(cleaned)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC_TZ)
        return parsed.astimezone(UTC_TZ)
    return None


def format_dt_utc(value: datetime | None) -> str:
    if value is None:
        return "unknown"
    return value.astimezone(UTC_TZ).strftime("%Y-%m-%d %H:%M:%S UTC")


def ordinal_suffix(day: int) -> str:
    if 11 <= day % 100 <= 13:
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")


def format_slack_uk_datetime(dt: datetime | None) -> str:
    if not dt:
        return "Unknown time"

    uk = dt.astimezone(UK_TZ)
    day = uk.day
    return f"{day}{ordinal_suffix(day)} {uk.strftime('%B %H:%M:%S %Z')}"


def normalise_market_name(name: str) -> str:
    return " ".join((name or "").strip().casefold().split())


def build_client(config: Config) -> APIClient:
    require_betfair_config(config)
    certs_dir = Path(config.betfair_certs_path).resolve()
    cert_file = certs_dir / "client-2048.crt"
    key_file = certs_dir / "client-2048.key"

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


def log_safe_config_sources(config: Config) -> None:
    log(f"Using Betfair credentials source: {config.betfair_credentials_source}")
    log(f"Using Slack config source: {config.slack_config_source}")
    log(f"Using Betfair certs directory: {Path(config.betfair_certs_path).resolve()}")
    log(f"Using state file: {ALERT_STATE_PATH.resolve()}")


def list_event_types(client: APIClient, start_from: datetime, start_to: datetime) -> list[Any]:
    event_filter = market_filter(
        market_start_time={
            "from": start_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": start_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    )
    return client.betting.list_event_types(filter=event_filter)


def list_football_events(
    client: APIClient,
    start_from: datetime,
    start_to: datetime,
    event_type_id: str = FOOTBALL_EVENT_TYPE_ID,
) -> list[Any]:
    event_filter = market_filter(
        event_type_ids=[event_type_id],
        market_start_time={
            "from": start_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": start_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    return client.betting.list_events(filter=event_filter)


def event_result_to_football_event(event_result: Any) -> FootballEvent:
    event = object_get(event_result, "event", {})
    return FootballEvent(
        event_id=str(object_get(event, "id", "")).strip(),
        event_name=str(object_get(event, "name", "")).strip(),
        start_utc=parse_datetime(object_get(event, "open_date", None)),
    )


def list_markets_for_event(client: APIClient, event_id: str, max_results: int = 1000) -> list[Any]:
    event_market_filter = market_filter(event_ids=[str(event_id)])
    return client.betting.list_market_catalogue(
        filter=event_market_filter,
        market_projection=[
            "EVENT",
            "EVENT_TYPE",
            "COMPETITION",
            "MARKET_START_TIME",
            "MARKET_DESCRIPTION",
        ],
        sort="FIRST_TO_START",
        max_results=max_results,
    )


def list_market_catalogues_once(
    client: APIClient,
    event_type_id: str,
    start_from: datetime,
    start_to: datetime,
    market_projection: list[str],
    max_results: int,
) -> list[Any]:
    duplicate_market_filter = market_filter(
        event_type_ids=[event_type_id],
        market_start_time={
            "from": start_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": start_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    return client.betting.list_market_catalogue(
        filter=duplicate_market_filter,
        market_projection=market_projection,
        max_results=max_results,
        sort="FIRST_TO_START",
    )


def is_too_much_data_error(exc: Exception) -> bool:
    text = str(exc)
    if "TOO_MUCH_DATA" in text or "too much data" in text.lower():
        return True
    response = object_get(exc, "response", None)
    if response and ("TOO_MUCH_DATA" in str(response) or "too much data" in str(response).lower()):
        return True
    return False


def dedupe_catalogues(catalogues: list[Any]) -> list[Any]:
    by_market_id: dict[str, Any] = {}
    without_market_id: list[Any] = []
    for catalogue in catalogues:
        market_id = str(object_get(catalogue, "market_id", "")).strip()
        if market_id:
            by_market_id[market_id] = catalogue
        else:
            without_market_id.append(catalogue)
    return [*by_market_id.values(), *without_market_id]


def format_chunk_time(value: datetime) -> str:
    return value.astimezone(UTC_TZ).strftime("%Y-%m-%d %H:%M")


def fetch_market_catalogue_window_adaptive(
    client: APIClient,
    event_type_id: str,
    sport_name: str,
    start_from: datetime,
    start_to: datetime,
    market_projection: list[str],
    max_results: int,
    min_chunk_minutes: float,
    stats: CatalogueFetchStats,
    depth: int = 0,
) -> list[Any]:
    window_seconds = (start_to - start_from).total_seconds()
    min_chunk_seconds = max(min_chunk_minutes, 0.1) * 60
    try:
        stats.catalogue_chunks_requested += 1
        catalogues = list_market_catalogues_once(
            client,
            event_type_id,
            start_from,
            start_to,
            market_projection,
            max_results,
        )
    except Exception as exc:
        if not is_too_much_data_error(exc):
            raise
        if window_seconds <= min_chunk_seconds:
            stats.catalogue_chunks_failed += 1
            log(
                f"{sport_name or event_type_id}: TOO_MUCH_DATA even at minimum chunk "
                f"{format_chunk_time(start_from)} to {format_chunk_time(start_to)}; skipping this chunk."
            )
            return []
        stats.catalogue_chunks_split += 1
        midpoint = start_from + timedelta(seconds=window_seconds / 2)
        log(
            f"{sport_name or event_type_id}: TOO_MUCH_DATA for "
            f"{format_chunk_time(start_from)} to {format_chunk_time(start_to)}, splitting smaller..."
        )
        left = fetch_market_catalogue_window_adaptive(
            client,
            event_type_id,
            sport_name,
            start_from,
            midpoint,
            market_projection,
            max_results,
            min_chunk_minutes,
            stats,
            depth + 1,
        )
        right = fetch_market_catalogue_window_adaptive(
            client,
            event_type_id,
            sport_name,
            midpoint,
            start_to,
            market_projection,
            max_results,
            min_chunk_minutes,
            stats,
            depth + 1,
        )
        return dedupe_catalogues(left + right)

    if len(catalogues) < max_results or window_seconds <= min_chunk_seconds:
        if len(catalogues) >= max_results:
            log(
                f"WARNING: {sport_name or event_type_id} chunk returned maxResults={max_results}; "
                "results may be truncated. Consider reducing --initial-chunk-hours or --min-chunk-minutes."
            )
        return catalogues

    stats.catalogue_chunks_truncated_and_split += 1
    midpoint = start_from + timedelta(seconds=window_seconds / 2)
    log(
        f"{sport_name or event_type_id}: chunk returned maxResults={max_results} for "
        f"{format_chunk_time(start_from)} to {format_chunk_time(start_to)}, splitting to avoid truncation..."
    )
    left = fetch_market_catalogue_window_adaptive(
        client,
        event_type_id,
        sport_name,
        start_from,
        midpoint,
        market_projection,
        max_results,
        min_chunk_minutes,
        stats,
        depth + 1,
    )
    right = fetch_market_catalogue_window_adaptive(
        client,
        event_type_id,
        sport_name,
        midpoint,
        start_to,
        market_projection,
        max_results,
        min_chunk_minutes,
        stats,
        depth + 1,
    )
    return dedupe_catalogues(left + right)


def iter_time_chunks(start_from: datetime, start_to: datetime, chunk_hours: float) -> Iterable[tuple[datetime, datetime]]:
    chunk_seconds = max(chunk_hours, 0.01) * 60 * 60
    current = start_from
    while current < start_to:
        next_end = min(current + timedelta(seconds=chunk_seconds), start_to)
        yield current, next_end
        current = next_end


def list_market_catalogues(
    client: APIClient,
    event_type_id: str,
    sport_name: str,
    start_from: datetime,
    start_to: datetime,
    initial_chunk_hours: float,
    min_chunk_minutes: float,
    max_results: int,
    stats: CatalogueFetchStats,
) -> list[Any]:
    market_projection = [
        "EVENT",
        "EVENT_TYPE",
        "COMPETITION",
        "MARKET_START_TIME",
        "MARKET_DESCRIPTION",
    ]
    catalogues: list[Any] = []
    for chunk_start, chunk_end in iter_time_chunks(start_from, start_to, initial_chunk_hours):
        catalogues.extend(
            fetch_market_catalogue_window_adaptive(
                client,
                event_type_id,
                sport_name,
                chunk_start,
                chunk_end,
                market_projection,
                max_results,
                min_chunk_minutes,
                stats,
            )
        )
    return dedupe_catalogues(catalogues)


def catalogue_to_market(catalogue: Any, fallback_sport_name: str, fallback_event_type_id: str) -> Market:
    event = object_get(catalogue, "event", {})
    event_type = object_get(catalogue, "event_type", {})
    competition = object_get(catalogue, "competition", {})
    market_name = str(object_get(catalogue, "market_name", "")).strip()
    return Market(
        sport_name=str(object_get(event_type, "name", fallback_sport_name) or fallback_sport_name),
        event_type_id=str(object_get(event_type, "id", fallback_event_type_id) or fallback_event_type_id),
        event_id=str(object_get(event, "id", "")).strip(),
        event_name=str(object_get(event, "name", "")).strip(),
        competition_id=str(object_get(competition, "id", "")).strip(),
        competition_name=str(object_get(competition, "name", "")).strip(),
        market_id=str(object_get(catalogue, "market_id", "")).strip(),
        market_name=market_name,
        normalised_market_name=normalise_market_name(market_name),
        market_start_time_utc=parse_datetime(object_get(catalogue, "market_start_time", None)),
    )


def find_duplicate_market_groups(markets: list[Market]) -> list[DuplicateMarketGroup]:
    grouped: dict[tuple[str, str, str], list[Market]] = {}
    for market in markets:
        if not market.event_type_id or not market.event_id or not market.normalised_market_name or not market.market_id:
            continue
        grouped.setdefault((market.event_type_id, market.event_id, market.normalised_market_name), []).append(market)

    duplicates: list[DuplicateMarketGroup] = []
    for (_event_type_id, _event_id, _normalised_name), grouped_markets in grouped.items():
        unique_by_market_id = {market.market_id: market for market in grouped_markets}
        if len(unique_by_market_id) < 2:
            continue

        sorted_markets = tuple(sorted(unique_by_market_id.values(), key=lambda market: market.market_id))
        first = sorted_markets[0]
        duplicates.append(
            DuplicateMarketGroup(
                sport_name=first.sport_name,
                event_type_id=first.event_type_id,
                event_id=first.event_id,
                event_name=first.event_name,
                competition_id=first.competition_id,
                competition_name=first.competition_name,
                market_name=first.market_name,
                normalised_market_name=first.normalised_market_name,
                markets=sorted_markets,
            )
        )

    return sorted(
        duplicates,
        key=lambda group: (
            group.sport_name.casefold(),
            group.event_name.casefold(),
            group.normalised_market_name,
            group.event_id,
        ),
    )


def duplicate_group_alert_key(group: DuplicateMarketGroup) -> str:
    market_ids = sorted(market.market_id for market in group.markets)
    return "|".join([group.event_type_id, group.event_id, group.normalised_market_name, *market_ids])


def sport_emoji(sport_name: str, event_type_id: str | None = None) -> str:
    name = (sport_name or "").strip().lower()
    emoji_map = {
        "soccer": ":soccer:",
        "football": ":soccer:",
        "tennis": ":tennis:",
        "basketball": ":basketball:",
        "golf": ":golf:",
        "ice hockey": ":ice_hockey_stick_and_puck:",
        "cricket": ":cricket_bat_and_ball:",
        "rugby league": ":rugby_football:",
        "rugby union": ":rugby_football:",
        "boxing": ":boxing_glove:",
        "horse racing": ":racehorse:",
        "motor sport": ":racing_car:",
        "esports": ":video_game:",
        "special bets": ":game_die:",
        "volleyball": ":volleyball:",
        "australian rules": ":rugby_football:",
        "darts": ":dart:",
        "gaelic games": ":stadium:",
        "mixed martial arts": ":martial_arts_uniform:",
        "greyhound racing": ":dog:",
        "politics": ":classical_building:",
        "snooker": ":8ball:",
        "baseball": ":baseball:",
        "american football": ":football:",
    }
    return emoji_map.get(name, ":warning:")


def event_start_time(group: DuplicateMarketGroup) -> datetime | None:
    times = [market.market_start_time_utc for market in group.markets if market.market_start_time_utc]
    return min(times) if times else None


def format_slack_message(group: DuplicateMarketGroup) -> str:
    emoji = sport_emoji(group.sport_name, group.event_type_id)
    competition_name = group.competition_name or "unknown"
    competition_id = group.competition_id or "unknown"
    market_lines = [f"- Market ID: {market.market_id}" for market in group.markets]
    return "\n".join(
        [
            f"*{emoji}Duplicate market detected :dupe-match-bot:*",
            f"*Match: {group.event_name or 'unknown'} - {format_slack_uk_datetime(event_start_time(group))}*",
            f"*Market Name: {group.market_name}*",
            "",
            "Match Details:",
            f"- Sport: {group.sport_name or 'unknown'} ({group.event_type_id})",
            f"- Event ID: {group.event_id}",
            f"- Competition: {competition_name} ({competition_id})",
            "",
            "Duplicate Markets:",
            *market_lines,
        ]
    )


def send_slack_message(webhook_url: str, text: str) -> None:
    if is_placeholder(webhook_url):
        raise RuntimeError(
            "Slack webhook is not configured. Set SLACK_WEBHOOK_URL, create "
            "Betfair_Duplicate_Market_Check.local.json, or configure the existing duplicate match checker local JSON."
        )
    response = requests.post(webhook_url, json={"text": text}, timeout=15)
    if response.status_code >= 400:
        raise RuntimeError(f"Slack webhook failed: status={response.status_code}, body={response.text}")


def run_test_slack(config: Config, dry_run: bool) -> int:
    message = ":warning: Betfair duplicate market checker Slack test"
    if dry_run:
        log("Dry run: Slack test message not sent.")
        print(message, flush=True)
        return 0
    send_slack_message(config.slack_webhook_url, message)
    log("Slack test message sent.")
    return 0


def load_alert_state(path: Path = ALERT_STATE_PATH) -> dict[str, Any]:
    state = read_json(path)
    sent_keys = state.get("sent_keys", [])
    if not isinstance(sent_keys, list):
        sent_keys = []
    return {"sent_keys": [str(key) for key in sent_keys]}


def save_alert_state(state: dict[str, Any], path: Path = ALERT_STATE_PATH) -> None:
    write_json(path, state)


def duplicate_group_to_dict(group: DuplicateMarketGroup) -> dict[str, Any]:
    market_start_times = sorted(
        {
            format_dt_utc(market.market_start_time_utc)
            for market in group.markets
            if market.market_start_time_utc
        }
    )
    return {
        "sport_name": group.sport_name,
        "event_type_id": group.event_type_id,
        "event_id": group.event_id,
        "event_name": group.event_name,
        "competition_name": group.competition_name,
        "competition_id": group.competition_id,
        "market_name": group.market_name,
        "normalised_market_name": group.normalised_market_name,
        "market_ids": [market.market_id for market in group.markets],
        "market_start_times_utc": market_start_times,
    }


def write_json_output(
    path: str,
    summary: dict[str, Any],
    duplicate_groups: list[DuplicateMarketGroup],
) -> None:
    output_path = Path(path)
    if not output_path.is_absolute():
        output_path = Path.cwd() / output_path
    payload = {
        "summary": summary,
        "duplicate_market_groups": [duplicate_group_to_dict(group) for group in duplicate_groups],
    }
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    log(f"Wrote JSON output: {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Betfair Soccer events for duplicate market names within each match.")
    parser.add_argument("--dry-run", action="store_true", help="Print findings and write JSON without sending Slack alerts.")
    parser.add_argument(
        "--json-output",
        default="",
        help="Optional path to write duplicate market debug JSON. Relative paths are resolved from the current working directory.",
    )
    parser.add_argument("--test-slack", action="store_true", help="Send a Slack test message and exit.")
    parser.add_argument("--pause-on-exit", action="store_true", help="Wait for Enter before closing the console.")
    parser.add_argument("--repeat-minutes", type=float, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--send-startup-message", action="store_true", help="Accepted for Script Manager compatibility.")
    parser.add_argument("--send-shutdown-message", action="store_true", help="Accepted for Script Manager compatibility.")
    parser.add_argument("--lookback-hours", type=float, default=DEFAULT_LOOKBACK_HOURS)
    parser.add_argument("--lookahead-hours", type=float, default=DEFAULT_LOOKAHEAD_HOURS)
    parser.add_argument("--lookahead-days", type=float, default=DEFAULT_LOOKAHEAD_DAYS)
    parser.add_argument(
        "--event-type-id",
        "--event-type-ids",
        action="append",
        dest="event_type_ids",
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--initial-chunk-hours", type=float, default=DEFAULT_INITIAL_CHUNK_HOURS, help=argparse.SUPPRESS)
    parser.add_argument("--min-chunk-minutes", type=float, default=DEFAULT_MIN_CHUNK_MINUTES, help=argparse.SUPPRESS)
    parser.add_argument("--max-results", type=int, default=1000, help=argparse.SUPPRESS)
    return parser.parse_args()


def run_scan(args: argparse.Namespace, config: Config) -> int:
    started = time.monotonic()
    start_from = datetime.now(UTC_TZ) - timedelta(hours=max(args.lookback_hours, 0))
    if args.lookahead_days and args.lookahead_days > 0:
        start_to = datetime.now(UTC_TZ) + timedelta(days=args.lookahead_days)
    else:
        start_to = datetime.now(UTC_TZ) + timedelta(hours=max(args.lookahead_hours, 0))
    requested_event_type_ids = tuple(
        str(value).strip()
        for value in (args.event_type_ids or DEFAULT_EVENT_TYPE_IDS)
        if str(value).strip()
    )
    event_type_id = requested_event_type_ids[0] if requested_event_type_ids else FOOTBALL_EVENT_TYPE_ID
    sport_name = FOOTBALL_SPORT_NAME if event_type_id == FOOTBALL_EVENT_TYPE_ID else f"Event Type {event_type_id}"
    max_results = max(1, int(args.max_results))

    log(f"Scanning Betfair {sport_name} ({event_type_id}) events from {format_dt_utc(start_from)} to {format_dt_utc(start_to)}")
    if args.dry_run:
        log("Dry run enabled: Slack alerts will not be sent.")

    client = build_client(config)
    duplicate_groups: list[DuplicateMarketGroup] = []
    football_events: list[FootballEvent] = []
    football_events_checked = 0
    markets_checked = 0
    failed_event_market_fetches = 0
    try:
        event_results = list_football_events(client, start_from, start_to, event_type_id)
        football_events = [
            event
            for event in (event_result_to_football_event(event_result) for event_result in event_results)
            if event.event_id
        ]
        log(f"Football events found: {len(football_events)}")

        for index, football_event in enumerate(football_events, start=1):
            log(f"Checking {index}/{len(football_events)}: {football_event.event_name} ({football_event.event_id})")
            try:
                catalogues = list_markets_for_event(client, football_event.event_id, max_results=max_results)
            except Exception as exc:
                failed_event_market_fetches += 1
                if is_too_much_data_error(exc):
                    log(f"{football_event.event_name} ({football_event.event_id}): TOO_MUCH_DATA fetching event markets; skipping this event.")
                else:
                    log(f"{football_event.event_name} ({football_event.event_id}): Betfair market fetch failed: {exc}")
                continue

            if len(catalogues) >= max_results:
                log(
                    f"WARNING: {football_event.event_name} ({football_event.event_id}) returned maxResults={max_results}; "
                    "event market results may be truncated."
                )

            event_markets = [
                catalogue_to_market(catalogue, sport_name, event_type_id)
                for catalogue in catalogues
            ]
            markets_checked += len(event_markets)
            football_events_checked += 1
            duplicate_groups.extend(find_duplicate_market_groups(event_markets))
    finally:
        try:
            client.logout()
        except Exception:
            pass

    alert_state = load_alert_state()
    sent_keys = set(alert_state["sent_keys"])
    skipped_already_alerted = 0
    slack_alerts_sent = 0

    for group in duplicate_groups:
        alert_key = duplicate_group_alert_key(group)
        if not args.dry_run and alert_key in sent_keys:
            skipped_already_alerted += 1
            log(f"Skipping already-alerted duplicate market group: {group.event_id} {group.market_name}")
            continue

        message = format_slack_message(group)
        print("", flush=True)
        print(message, flush=True)
        if args.dry_run:
            continue

        send_slack_message(config.slack_webhook_url, message)
        slack_alerts_sent += 1
        sent_keys.add(alert_key)
        alert_state["sent_keys"] = sorted(sent_keys)
        save_alert_state(alert_state)
        log(f"Sent Slack alert for event {group.event_id}, market name {group.market_name}")

    duration_seconds = round(time.monotonic() - started, 2)
    summary = {
        "sport_scanned": FOOTBALL_SPORT_NAME if event_type_id == FOOTBALL_EVENT_TYPE_ID else sport_name,
        "event_type_id": event_type_id,
        "football_events_found": len(football_events),
        "football_events_checked": football_events_checked,
        "markets_checked": markets_checked,
        "duplicate_market_groups_found": len(duplicate_groups),
        "slack_alerts_sent": slack_alerts_sent,
        "skipped_already_alerted_groups": skipped_already_alerted,
        "failed_event_market_fetches": failed_event_market_fetches,
        "dry_run": bool(args.dry_run),
        "duration_seconds": duration_seconds,
    }

    if args.json_output:
        write_json_output(args.json_output, summary, duplicate_groups)

    print("Duplicate market scan complete.", flush=True)
    print(f"Sport scanned: {summary['sport_scanned']} ({summary['event_type_id']})", flush=True)
    print(f"Football events found: {summary['football_events_found']}", flush=True)
    print(f"Football events checked: {summary['football_events_checked']}", flush=True)
    print(f"Markets checked: {summary['markets_checked']}", flush=True)
    print(f"Duplicate market groups found: {summary['duplicate_market_groups_found']}", flush=True)
    print(f"Slack alerts sent: {summary['slack_alerts_sent']}", flush=True)
    print(f"Skipped already-alerted groups: {summary['skipped_already_alerted_groups']}", flush=True)
    print(f"Failed event market fetches: {summary['failed_event_market_fetches']}", flush=True)
    print(f"Dry run: {str(summary['dry_run']).lower()}", flush=True)
    print(f"Duration seconds: {summary['duration_seconds']}", flush=True)
    return 0


def main() -> int:
    args = parse_args()
    config = load_config()
    log_safe_config_sources(config)

    if args.test_slack:
        return run_test_slack(config, args.dry_run)

    if args.repeat_minutes:
        log("Ignoring --repeat-minutes: duplicate market check runs once and exits.")
    try:
        return run_scan(args, config)
    except KeyboardInterrupt:
        raise
    except Exception as exc:
        log(f"ERROR: {exc}")
        traceback.print_exc(file=sys.stdout)
        return 1


def pause_before_exit() -> None:
    print("Duplicate market check finished. Press Enter to close...", flush=True)
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
