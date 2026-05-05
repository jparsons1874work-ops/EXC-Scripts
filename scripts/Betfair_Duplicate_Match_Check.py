#!/usr/bin/env python3
"""Check Betfair Exchange fixtures for duplicate match listings."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import requests
from betfairlightweight import APIClient
from betfairlightweight.filters import market_filter


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

# =============================================================================
# LOCAL CONFIG PLACEHOLDERS
# =============================================================================
# Safer options are still supported:
# - Environment variables: BETFAIR_USERNAME, BETFAIR_PASSWORD, BETFAIR_APP_KEY,
#   BETFAIR_CERTS_DIR, SLACK_WEBHOOK_URL
# - Local ignored file: Scripts/Betfair_Duplicate_Match_Check.local.json
#
# If you prefer editing this file directly, replace these placeholder strings.
BETFAIR_USERNAME_PLACEHOLDER = "YOUR_BETFAIR_USERNAME"
BETFAIR_PASSWORD_PLACEHOLDER = "YOUR_BETFAIR_PASSWORD"
BETFAIR_APP_KEY_PLACEHOLDER = "YOUR_BETFAIR_APP_KEY"
SLACK_WEBHOOK_URL_PLACEHOLDER = ""

BETFAIR_CERTS_DIR = Path(
    os.environ.get(
        "BETFAIR_CERTS_DIR",
        SCRIPT_DIR / "Integrity-Scanner" / "certs",
    )
).resolve()
BETFAIR_CERT_FILE = BETFAIR_CERTS_DIR / "client-2048.crt"
BETFAIR_KEY_FILE = BETFAIR_CERTS_DIR / "client-2048.key"

UK_TZ = ZoneInfo("Europe/London")
UTC_TZ = ZoneInfo("UTC")
PLACEHOLDER_PREFIXES = ("YOUR_", "PASTE_", "CHANGE_ME", "TODO")
DEFAULT_LOOKAHEAD_DAYS = 30
DEFAULT_MARKET_TYPES = ("MATCH_ODDS",)
ALERT_STATE_PATH = PROJECT_DIR / "betfair_duplicate_alert_state.json"
STRICT_DUPLICATE_REASON = (
    "Both parsed participants match within the same sport/event type. "
    "Reversed participant order is allowed."
)
SAME_TIME_PARTICIPANT_CONFLICT_REASON = (
    "One parsed participant appears in two different fixtures in the same sport/event type "
    "at the exact same scheduled start time."
)
STRICT_SAME_FIXTURE = "strict_same_fixture"
SAME_TIME_PARTICIPANT_CONFLICT = "same_time_participant_conflict"


@dataclass(frozen=True)
class Config:
    betfair_username: str
    betfair_password: str
    betfair_app_key: str
    betfair_certs_path: str
    slack_webhook_url: str


@dataclass(frozen=True)
class Fixture:
    sport_name: str
    event_type_id: str
    event_id: str
    market_id: str
    fixture_name: str
    competition_name: str
    competition_id: str
    parsed_names: tuple[str, ...]
    normalized_names: tuple[str, ...]
    participant_key: tuple[str, ...]
    genderless_participant_key: tuple[str, ...]
    start_utc: datetime | None

    @property
    def uk_date(self) -> str:
        if self.start_utc is None:
            return ""
        return self.start_utc.astimezone(UK_TZ).date().isoformat()


@dataclass(frozen=True)
class DuplicatePair:
    sport_name: str
    event_type_id: str
    match_type: str
    reason: str
    overlapping_participants: tuple[str, ...]
    start_time_key: str | None
    first: Fixture
    second: Fixture


@dataclass(frozen=True)
class DuplicateScanStats:
    candidate_pairs_checked: int = 0
    strict_duplicate_pairs_found: int = 0
    same_time_participant_conflicts_found: int = 0
    rejected_one_participant_overlap_count: int = 0
    rejected_gender_context_mismatch_count: int = 0


@dataclass(frozen=True)
class DuplicateScanResult:
    duplicates: list[DuplicatePair]
    stats: DuplicateScanStats


def log(message: str) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception as exc:
        log(f"Could not read config {path}: {exc}")
        return {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def today_uk_date() -> str:
    return datetime.now(UK_TZ).date().isoformat()


def load_daily_alert_state(path: Path = ALERT_STATE_PATH) -> dict[str, Any]:
    today = today_uk_date()
    state = read_json(path)
    if state.get("date") != today:
        return {"date": today, "sent_keys": []}
    sent_keys = state.get("sent_keys", [])
    if not isinstance(sent_keys, list):
        sent_keys = []
    return {"date": today, "sent_keys": [str(key) for key in sent_keys]}


def save_daily_alert_state(state: dict[str, Any], path: Path = ALERT_STATE_PATH) -> None:
    write_json(path, state)


def nested_get(data: dict[str, Any], *keys: str) -> str:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return ""
        current = current.get(key)
    return str(current or "").strip()


def first_config_value(configs: Iterable[dict[str, Any]], paths: Iterable[tuple[str, ...]]) -> str:
    for config in configs:
        for path in paths:
            value = nested_get(config, *path)
            if value:
                return value
    return ""


def is_placeholder(value: str) -> bool:
    stripped = value.strip()
    return not stripped or any(stripped.startswith(prefix) for prefix in PLACEHOLDER_PREFIXES)


def configured_value(value: str) -> str:
    stripped = value.strip()
    if is_placeholder(stripped):
        return ""
    return stripped


def resolve_path(value: str, base_dir: Path) -> str:
    if not value:
        return ""
    path = Path(value)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return str(path)


def load_config() -> Config:
    local_config_paths = [
        SCRIPT_DIR / "Betfair_Duplicate_Match_Check.local.json",
        PROJECT_DIR / "betfair_duplicate_match_check.local.json",
    ]
    existing_config_paths: list[Path] = []
    local_configs = [read_json(path) for path in local_config_paths]
    betfair_fallback_configs = [read_json(path) for path in existing_config_paths]
    betfair_configs = [*local_configs, *betfair_fallback_configs]

    username = (
        os.getenv("BETFAIR_USERNAME", "").strip()
        or os.getenv("BF_USERNAME", "").strip()
        or configured_value(BETFAIR_USERNAME_PLACEHOLDER)
        or first_config_value(betfair_configs, [("betfair", "username")])
    )
    password = (
        os.getenv("BETFAIR_PASSWORD", "").strip()
        or os.getenv("BF_PASSWORD", "").strip()
        or configured_value(BETFAIR_PASSWORD_PLACEHOLDER)
        or first_config_value(betfair_configs, [("betfair", "password")])
    )
    app_key = (
        os.getenv("BETFAIR_APP_KEY", "").strip()
        or os.getenv("BF_APP_KEY", "").strip()
        or configured_value(BETFAIR_APP_KEY_PLACEHOLDER)
        or first_config_value(betfair_configs, [("betfair", "app_key")])
    )
    certs_path = (
        os.getenv("BETFAIR_CERTS_DIR", "").strip()
        or os.getenv("BF_CERTS_DIR", "").strip()
        or first_config_value(betfair_configs, [("betfair", "certs_path")])
        or str(BETFAIR_CERTS_DIR)
    )
    slack_webhook_url = (
        os.getenv("SLACK_WEBHOOK_URL", "").strip()
        or configured_value(SLACK_WEBHOOK_URL_PLACEHOLDER)
        or first_config_value(local_configs, [("slack", "webhook_url")])
    )

    return Config(
        betfair_username=username,
        betfair_password=password,
        betfair_app_key=app_key,
        betfair_certs_path=resolve_path(certs_path, SCRIPT_DIR / "Integrity-Scanner"),
        slack_webhook_url=slack_webhook_url,
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


def normalize_name(value: str) -> str:
    return canonical_participant_name(value)


def canonical_participant_name(value: str) -> str:
    lowered = value.lower().strip()
    lowered = re.sub(r"\(\s*w\s*\)", " women ", lowered)
    lowered = re.sub(r"\[\s*w\s*\]", " women ", lowered)
    lowered = re.sub(r"\b(?:women|women's|womens|ladies|female)\b", " women ", lowered)
    lowered = re.sub(r"\([^)]*\)", " ", lowered)
    lowered = re.sub(r"\[[^]]*\]", " ", lowered)
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def remove_gender_context(value: str) -> str:
    return re.sub(r"\bwomen\b", " ", value).strip()


def participant_key(parsed_names: tuple[str, ...]) -> tuple[str, ...]:
    if len(parsed_names) != 2:
        return ()
    names = tuple(name for name in (canonical_participant_name(part) for part in parsed_names) if name)
    if len(names) != 2:
        return ()
    return tuple(sorted(names))


def genderless_participant_key(key: tuple[str, ...]) -> tuple[str, ...]:
    names = tuple(name for name in (remove_gender_context(part) for part in key) if name)
    if len(names) != 2:
        return ()
    return tuple(sorted(names))


def parse_participants(fixture_name: str) -> tuple[str, ...]:
    separators = [
        r"\s+v(?:s\.?)?\s+",
        r"\s+@\s+",
        r"\s+-\s+",
    ]
    for separator in separators:
        parts = [part.strip() for part in re.split(separator, fixture_name, maxsplit=1, flags=re.IGNORECASE)]
        if len(parts) == 2 and all(parts):
            return tuple(parts)
    return (fixture_name.strip(),)


def format_dt_utc(value: datetime | None) -> str:
    if value is None:
        return "unknown"
    return value.astimezone(UTC_TZ).strftime("%Y-%m-%d %H:%M:%S UTC")


def format_dt_uk(value: datetime | None) -> str:
    if value is None:
        return "unknown"
    return value.astimezone(UK_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


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


def start_time_key(dt: datetime | None) -> str | None:
    if not dt:
        return None
    return dt.astimezone(timezone.utc).replace(second=0, microsecond=0).isoformat()


def participant_set(fixture: Fixture) -> set[str]:
    if len(fixture.participant_key) != 2:
        return set()
    return set(fixture.participant_key)


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

    kwargs: dict[str, Any] = {
        "username": config.betfair_username,
        "password": config.betfair_password,
        "app_key": config.betfair_app_key,
        "certs": str(certs_dir),
    }

    client = APIClient(**kwargs)
    client.login()
    return client


def list_event_types(client: APIClient, start_from: datetime, start_to: datetime) -> list[Any]:
    event_filter = market_filter(
        market_start_time={
            "from": start_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": start_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    )
    return client.betting.list_event_types(filter=event_filter)


def list_fixtures_for_event_type(
    client: APIClient,
    event_type_id: str,
    start_from: datetime,
    start_to: datetime,
    market_types: tuple[str, ...],
) -> list[Any]:
    fixture_filter = market_filter(
        event_type_ids=[event_type_id],
        market_type_codes=list(market_types),
        market_start_time={
            "from": start_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": start_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    return client.betting.list_market_catalogue(
        filter=fixture_filter,
        market_projection=["EVENT", "EVENT_TYPE", "COMPETITION", "MARKET_START_TIME"],
        max_results=1000,
        sort="FIRST_TO_START",
    )


def catalogue_to_fixture(catalogue: Any, fallback_sport_name: str, fallback_event_type_id: str) -> Fixture:
    event = object_get(catalogue, "event", {})
    event_type = object_get(catalogue, "event_type", {})
    competition = object_get(catalogue, "competition", {})
    fixture_name = str(object_get(event, "name", "") or object_get(catalogue, "market_name", "")).strip()
    parsed_names = parse_participants(fixture_name)
    strict_participant_key = participant_key(parsed_names)
    normalized_names = strict_participant_key

    return Fixture(
        sport_name=str(object_get(event_type, "name", fallback_sport_name) or fallback_sport_name),
        event_type_id=str(object_get(event_type, "id", fallback_event_type_id) or fallback_event_type_id),
        event_id=str(object_get(event, "id", "")),
        market_id=str(object_get(catalogue, "market_id", "")),
        fixture_name=fixture_name,
        competition_name=str(object_get(competition, "name", "")),
        competition_id=str(object_get(competition, "id", "")),
        parsed_names=parsed_names,
        normalized_names=normalized_names,
        participant_key=strict_participant_key,
        genderless_participant_key=genderless_participant_key(strict_participant_key),
        start_utc=parse_datetime(object_get(catalogue, "market_start_time", None)),
    )


def duplicate_match_details(first: Fixture, second: Fixture) -> tuple[str, str, tuple[str, ...], str | None]:
    same_sport = first.event_type_id == second.event_type_id
    different_event = first.event_id != second.event_id
    if not same_sport or not different_event:
        return "", "", (), None

    overlapping_participants = tuple(sorted(participant_set(first).intersection(participant_set(second))))
    first_start_key = start_time_key(first.start_utc)
    second_start_key = start_time_key(second.start_utc)

    if first.participant_key and first.participant_key == second.participant_key:
        return STRICT_SAME_FIXTURE, STRICT_DUPLICATE_REASON, overlapping_participants, first_start_key

    if first_start_key is not None and first_start_key == second_start_key and overlapping_participants:
        return (
            SAME_TIME_PARTICIPANT_CONFLICT,
            SAME_TIME_PARTICIPANT_CONFLICT_REASON,
            overlapping_participants,
            first_start_key,
        )

    return "", "", (), None


def find_duplicates(fixtures: list[Fixture]) -> DuplicateScanResult:
    duplicates: list[DuplicatePair] = []
    candidate_pairs_checked = 0
    strict_duplicate_pairs_found = 0
    same_time_participant_conflicts_found = 0
    rejected_one_participant_overlap_count = 0
    rejected_gender_context_mismatch_count = 0
    by_event_type: dict[str, list[Fixture]] = {}
    for fixture in fixtures:
        by_event_type.setdefault(fixture.event_type_id, []).append(fixture)

    for event_type_id, sport_fixtures in by_event_type.items():
        for index, first in enumerate(sport_fixtures):
            for second in sport_fixtures[index + 1 :]:
                if not first.participant_key or not second.participant_key or first.event_id == second.event_id:
                    continue
                candidate_pairs_checked += 1

                match_type, reason, overlapping_participants, matched_start_time_key = duplicate_match_details(first, second)
                if reason:
                    if match_type == STRICT_SAME_FIXTURE:
                        strict_duplicate_pairs_found += 1
                    elif match_type == SAME_TIME_PARTICIPANT_CONFLICT:
                        same_time_participant_conflicts_found += 1
                    duplicates.append(
                        DuplicatePair(
                            sport_name=first.sport_name,
                            event_type_id=event_type_id,
                            match_type=match_type,
                            reason=reason,
                            overlapping_participants=overlapping_participants,
                            start_time_key=matched_start_time_key,
                            first=first,
                            second=second,
                        )
                    )
                    continue

                overlap_count = len(set(first.participant_key).intersection(second.participant_key))
                if overlap_count == 1:
                    rejected_one_participant_overlap_count += 1
                if (
                    first.genderless_participant_key
                    and first.genderless_participant_key == second.genderless_participant_key
                    and first.participant_key != second.participant_key
                ):
                    rejected_gender_context_mismatch_count += 1

    return DuplicateScanResult(
        duplicates=duplicates,
        stats=DuplicateScanStats(
            candidate_pairs_checked=candidate_pairs_checked,
            strict_duplicate_pairs_found=strict_duplicate_pairs_found,
            same_time_participant_conflicts_found=same_time_participant_conflicts_found,
            rejected_one_participant_overlap_count=rejected_one_participant_overlap_count,
            rejected_gender_context_mismatch_count=rejected_gender_context_mismatch_count,
        ),
    )


def duplicate_to_dict(pair: DuplicatePair) -> dict[str, Any]:
    payload = {
        "sport": pair.sport_name,
        "eventTypeId": pair.event_type_id,
        "match_type": pair.match_type,
        "reason": pair.reason,
        "overlapping_participants": list(pair.overlapping_participants),
        "start_time_key": pair.start_time_key,
        "event_ids": [pair.first.event_id, pair.second.event_id],
        "market_ids": [pair.first.market_id, pair.second.market_id],
        "fixture_names": [pair.first.fixture_name, pair.second.fixture_name],
        "competitions": [
            {"name": pair.first.competition_name, "id": pair.first.competition_id},
            {"name": pair.second.competition_name, "id": pair.second.competition_id},
        ],
        "parsed_participants": [list(pair.first.parsed_names), list(pair.second.parsed_names)],
        "canonical_participant_keys": [list(pair.first.participant_key), list(pair.second.participant_key)],
        "start_times": {
            "utc": [format_dt_utc(pair.first.start_utc), format_dt_utc(pair.second.start_utc)],
            "uk": [format_dt_uk(pair.first.start_utc), format_dt_uk(pair.second.start_utc)],
        },
        "first": asdict(pair.first),
        "second": asdict(pair.second),
    }
    for side in ("first", "second"):
        start_utc = getattr(pair, side).start_utc
        payload[side]["start_utc"] = format_dt_utc(start_utc)
        payload[side]["start_uk"] = format_dt_uk(start_utc)
    return payload


def duplicate_alert_key(pair: DuplicatePair) -> str:
    event_ids = tuple(sorted(event_id for event_id in (pair.first.event_id, pair.second.event_id) if event_id))
    market_ids = tuple(sorted(market_id for market_id in (pair.first.market_id, pair.second.market_id) if market_id))
    identity = event_ids or market_ids
    payload = {
        "event_type_id": pair.event_type_id,
        "identity": list(identity),
    }
    if pair.match_type == STRICT_SAME_FIXTURE:
        payload["participant_key"] = list(pair.first.participant_key)
    else:
        payload["match_type"] = pair.match_type
        payload["overlapping_participants"] = list(pair.overlapping_participants)
        payload["start_time_key"] = pair.start_time_key
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


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


def format_slack_message(pair: DuplicatePair) -> str:
    first = pair.first
    second = pair.second
    emoji = sport_emoji(first.sport_name, first.event_type_id)
    return "\n".join(
        [
            f"*{emoji}Duplicate fixture detected :dupe-match-bot:*",
            f"*Fixture 1: {first.fixture_name} - {format_slack_uk_datetime(first.start_utc)}*",
            f"*Fixture 2: {second.fixture_name} - {format_slack_uk_datetime(second.start_utc)}*",
            "",
            "Fixture 1:",
            f"- Event ID: {first.event_id}",
            f"- Market ID: {first.market_id}",
            f"- Name: {first.fixture_name}",
            f"-Competition: {first.competition_name or 'unknown'} ({first.competition_id or 'unknown'})",
            "Fixture 2:",
            f"- Event ID: {second.event_id}",
            f"- Market ID: {second.market_id}",
            f"- Name: {second.fixture_name}",
            f"-Competition: {second.competition_name or 'unknown'} ({second.competition_id or 'unknown'})",
        ]
    )


def send_slack_message(webhook_url: str, text: str) -> None:
    if is_placeholder(webhook_url):
        raise RuntimeError("Slack webhook is not configured. Set SLACK_WEBHOOK_URL or a local config value.")
    response = requests.post(webhook_url, json={"text": text}, timeout=15)
    if response.status_code >= 400:
        raise RuntimeError(f"Slack webhook failed: status={response.status_code}, body={response.text}")


def run_test_slack(config: Config, dry_run: bool) -> int:
    message = ":warning: Betfair duplicate checker Slack test"
    if dry_run:
        log("Dry run: Slack test message not sent.")
        print(message)
        return 0
    send_slack_message(config.slack_webhook_url, message)
    log("Slack test message sent.")
    return 0


def send_startup_message(config: Config, dry_run: bool, repeat_minutes: float) -> None:
    if repeat_minutes:
        cadence = f"I will scan every {repeat_minutes:g} minutes and post any genuine duplicate fixtures here."
    else:
        cadence = "I will run a scan now and post any genuine duplicate fixtures here."
    message = f":sunrise: Good morning team. Betfair duplicate fixture scanner active. {cadence}"
    log("Scanner active.")
    print(message, flush=True)
    if dry_run:
        log("Dry run: startup Slack message not sent.")
        return
    send_slack_message(config.slack_webhook_url, message)
    log("Startup Slack message sent.")


def send_shutdown_message(config: Config, dry_run: bool) -> None:
    message = ":crescent_moon: Goodnight team. Betfair duplicate fixture scanner stopped."
    log("Scanner stopped.")
    print(message, flush=True)
    if dry_run:
        log("Dry run: shutdown Slack message not sent.")
        return
    send_slack_message(config.slack_webhook_url, message)
    log("Shutdown Slack message sent.")


def write_json_output(
    path: str,
    duplicates: list[DuplicatePair],
    fixtures: list[Fixture],
    stats: DuplicateScanStats,
) -> None:
    output_path = Path(path)
    if not output_path.is_absolute():
        output_path = Path.cwd() / output_path
    payload = {
        "generated_at_utc": format_dt_utc(datetime.now(UTC_TZ)),
        "fixture_count": len(fixtures),
        "duplicate_count": len(duplicates),
        "duplicate_pairs_found": len(duplicates),
        "strict_duplicate_pairs_found": stats.strict_duplicate_pairs_found,
        "same_time_participant_conflicts_found": stats.same_time_participant_conflicts_found,
        "candidate_pairs_checked": stats.candidate_pairs_checked,
        "rejected_one_participant_overlap_count": stats.rejected_one_participant_overlap_count,
        "rejected_gender_context_mismatch_count": stats.rejected_gender_context_mismatch_count,
        "duplicates": [duplicate_to_dict(pair) for pair in duplicates],
    }
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    log(f"Wrote JSON output: {output_path}")


def make_test_fixture(
    fixture_name: str,
    event_id: str,
    start_utc: datetime | None,
    event_type_id: str = "1",
    sport_name: str = "Soccer",
) -> Fixture:
    parsed_names = parse_participants(fixture_name)
    strict_participant_key = participant_key(parsed_names)
    return Fixture(
        sport_name=sport_name,
        event_type_id=event_type_id,
        event_id=event_id,
        market_id=f"test-market-{event_id}",
        fixture_name=fixture_name,
        competition_name="Test Competition",
        competition_id="test-competition",
        parsed_names=parsed_names,
        normalized_names=strict_participant_key,
        participant_key=strict_participant_key,
        genderless_participant_key=genderless_participant_key(strict_participant_key),
        start_utc=start_utc,
    )


def run_self_test() -> int:
    at_1500 = datetime(2026, 1, 1, 15, 0, tzinfo=timezone.utc)
    at_1300 = datetime(2026, 1, 1, 13, 0, tzinfo=timezone.utc)

    same_time = find_duplicates(
        [
            make_test_fixture("Team A v Team B", "1", at_1500),
            make_test_fixture("Team A v Team C", "2", at_1500),
        ]
    ).duplicates
    assert len(same_time) == 1
    assert same_time[0].match_type == SAME_TIME_PARTICIPANT_CONFLICT
    assert same_time[0].overlapping_participants == ("team a",)

    different_time = find_duplicates(
        [
            make_test_fixture("Team A v Team B", "3", at_1300),
            make_test_fixture("Team A v Team C", "4", at_1500),
        ]
    ).duplicates
    assert not different_time

    strict_duplicate = find_duplicates(
        [
            make_test_fixture("Team A v Team B", "5", at_1300),
            make_test_fixture("Team B v Team A", "6", at_1500),
        ]
    ).duplicates
    assert len(strict_duplicate) == 1
    assert strict_duplicate[0].match_type == STRICT_SAME_FIXTURE

    assert canonical_participant_name("Arsenal") != canonical_participant_name("Arsenal (W)")
    assert canonical_participant_name("W") == "w"

    log("Self-test passed.")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check all Betfair Exchange sports for duplicate fixtures.")
    parser.add_argument("--dry-run", action="store_true", help="Print findings and write JSON without sending Slack alerts.")
    parser.add_argument("--json-output", default="", help="Optional path to write duplicate debug JSON.")
    parser.add_argument("--self-test", action="store_true", help="Run internal duplicate-rule checks and exit.")
    parser.add_argument("--test-slack", action="store_true", help="Send a Slack test message and exit.")
    parser.add_argument("--pause-on-exit", action="store_true", help="Wait for Enter before closing the console.")
    parser.add_argument("--repeat-minutes", type=float, default=0, help="Repeat scans every N minutes until stopped.")
    parser.add_argument("--send-startup-message", action="store_true", help="Send a one-time scanner active Slack message.")
    parser.add_argument("--send-shutdown-message", action="store_true", help="Send a scanner stopped Slack message on clean shutdown.")
    parser.add_argument("--lookahead-days", type=int, default=DEFAULT_LOOKAHEAD_DAYS)
    parser.add_argument("--market-type", action="append", dest="market_types", help="Betfair market type code to scan.")
    return parser.parse_args()


def run_scan(args: argparse.Namespace, config: Config) -> int:
    start_from = datetime.now(UTC_TZ) - timedelta(hours=6)
    start_to = datetime.now(UTC_TZ) + timedelta(days=args.lookahead_days)
    market_types = tuple(args.market_types or DEFAULT_MARKET_TYPES)

    log(f"Scanning Betfair fixtures from {format_dt_utc(start_from)} to {format_dt_utc(start_to)}")
    log(f"Market types: {', '.join(market_types)}")
    if args.dry_run:
        log("Dry run enabled: Slack alerts will not be sent.")

    client = build_client(config)
    fixtures: list[Fixture] = []
    try:
        event_types = list_event_types(client, start_from, start_to)
        log(f"Found {len(event_types)} Betfair event types with markets in date range.")

        for event_type_result in event_types:
            event_type = object_get(event_type_result, "event_type", {})
            event_type_id = str(object_get(event_type, "id", "")).strip()
            sport_name = str(object_get(event_type, "name", "")).strip()
            if not event_type_id:
                continue

            try:
                catalogues = list_fixtures_for_event_type(client, event_type_id, start_from, start_to, market_types)
            except Exception as exc:
                log(f"Skipping {sport_name or event_type_id}: Betfair fetch failed: {exc}")
                continue

            sport_fixtures = [
                catalogue_to_fixture(catalogue, sport_name, event_type_id)
                for catalogue in catalogues
            ]
            fixtures.extend(sport_fixtures)
            log(f"{sport_name or event_type_id}: {len(sport_fixtures)} fixtures")
    finally:
        try:
            client.logout()
        except Exception:
            pass

    scan_result = find_duplicates(fixtures)
    duplicates = scan_result.duplicates
    log(f"Duplicate pairs found: {len(duplicates)}")
    log(f"Strict same-fixture duplicates found: {scan_result.stats.strict_duplicate_pairs_found}")
    log(f"Same-time participant conflicts found: {scan_result.stats.same_time_participant_conflicts_found}")
    log(f"Candidate pairs checked: {scan_result.stats.candidate_pairs_checked}")
    log(f"Rejected one-participant overlaps: {scan_result.stats.rejected_one_participant_overlap_count}")
    log(f"Rejected gender-context mismatches: {scan_result.stats.rejected_gender_context_mismatch_count}")

    alert_state = load_daily_alert_state()
    sent_keys = set(alert_state["sent_keys"])
    skipped_already_alerted = 0

    for pair in duplicates:
        alert_key = duplicate_alert_key(pair)
        if not args.dry_run and alert_key in sent_keys:
            skipped_already_alerted += 1
            log(f"Skipping already-alerted duplicate pair for today: {pair.first.market_id} and {pair.second.market_id}")
            continue

        message = format_slack_message(pair)
        print("", flush=True)
        print(message, flush=True)
        if args.dry_run:
            continue
        send_slack_message(config.slack_webhook_url, message)
        sent_keys.add(alert_key)
        alert_state["sent_keys"] = sorted(sent_keys)
        save_daily_alert_state(alert_state)
        log(f"Sent Slack alert for markets {pair.first.market_id} and {pair.second.market_id}")

    if skipped_already_alerted:
        log(f"Skipped already-alerted duplicate pairs today: {skipped_already_alerted}")

    if args.json_output:
        write_json_output(args.json_output, duplicates, fixtures, scan_result.stats)

    log("Duplicate scan complete.")
    return 0


def main() -> int:
    args = parse_args()

    if args.self_test:
        return run_self_test()

    config = load_config()

    if args.test_slack:
        return run_test_slack(config, args.dry_run)

    if args.send_shutdown_message:
        signal.signal(signal.SIGTERM, raise_keyboard_interrupt)
        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, raise_keyboard_interrupt)

    repeat_minutes = max(args.repeat_minutes, 0)
    if args.send_startup_message:
        send_startup_message(config, args.dry_run, repeat_minutes)

    cycle = 1
    try:
        while True:
            if repeat_minutes:
                log(f"Starting duplicate scan cycle {cycle}.")
            try:
                run_scan(args, config)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                log(f"ERROR: {exc}")
                traceback.print_exc(file=sys.stdout)
                if not repeat_minutes:
                    return 1

            if not repeat_minutes:
                return 0

            cycle += 1
            sleep_seconds = repeat_minutes * 60
            next_scan = datetime.now().astimezone(UK_TZ) + timedelta(seconds=sleep_seconds)
            log(f"Next duplicate scan scheduled for {next_scan.strftime('%Y-%m-%d %H:%M:%S %Z')}.")
            time.sleep(sleep_seconds)
    except KeyboardInterrupt:
        if args.send_shutdown_message:
            send_shutdown_message(config, args.dry_run)
        return 130


def pause_before_exit() -> None:
    print("Duplicate match check finished. Press Enter to close...", flush=True)
    try:
        input()
    except EOFError:
        pass


def raise_keyboard_interrupt(signum: int, frame: Any) -> None:
    raise KeyboardInterrupt


if __name__ == "__main__":
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
