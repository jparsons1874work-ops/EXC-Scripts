#!/usr/bin/env python3
"""Schedule Slack reminders for selected Betfair Exchange events."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import traceback
from collections import Counter
from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import requests
from betfairlightweight import APIClient
from betfairlightweight.filters import market_filter
from dotenv import dotenv_values


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONFIG_DIR = PROJECT_ROOT / "config"
WINDOWS_ROOT = Path(r"C:\BetfairScripts")
EC2_ROOT = Path("/opt/betfair-scripts")
WINDOWS_ENV_PATH = WINDOWS_ROOT / ".env"
EC2_ENV_PATH = EC2_ROOT / ".env"
WINDOWS_CONFIG_PATH = WINDOWS_ROOT / "config" / "betfair_event_reminders_config.json"
EC2_CONFIG_PATH = EC2_ROOT / "config" / "betfair_event_reminders_config.json"
CONFIG_PATH = WINDOWS_CONFIG_PATH if os.name == "nt" else EC2_CONFIG_PATH
EXAMPLE_CONFIG_PATH = CONFIG_DIR / "betfair_event_reminders_config.example.json"
CONFIG_ENV_VAR = "BETFAIR_EVENT_REMINDERS_CONFIG"
STATE_PATH = PROJECT_ROOT / "data" / "betfair_event_reminders_sent.json"
LOG_DIR = PROJECT_ROOT / "logs"

UK_TZ = ZoneInfo("Europe/London")
UTC_TZ = ZoneInfo("UTC")
SCAN_START_TIME_UK = time(7, 0)
REMINDER_LEAD_MINUTES = 5
SLACK_SCHEDULE_LIMIT_PER_BUCKET = 30
SLACK_BUCKET_SECONDS = 5 * 60
SLACK_SCHEDULE_URL = "https://slack.com/api/chat.scheduleMessage"
MATCH_ODDS = "MATCH_ODDS"
WINNER = "WINNER"
OUTRIGHT_WINNER = "OUTRIGHT_WINNER"
STAGE_WINNER = "STAGE_WINNER"
DISALLOWED_COUNTRY_CODES = frozenset({"AU"})
GENERIC_OUTRIGHT_EMOJI = ":trophy:"
FIRST_TRY_SCORER = "FIRST_TRY_SCORER"
TO_WIN_THE_TOSS = "TO_WIN_THE_TOSS"
CRICKET_TOSS_LEAD_MINUTES = 40
CERT_FILE_NAME = "client-2048.crt"
KEY_FILE_NAME = "client-2048.key"
WINDOWS_DRIVE_PATH_RE = re.compile(r"^[A-Za-z]:[\\/]")

PLACEHOLDER_CONFIG: dict[str, str] = {
    "slack_bot_token": "xoxb-NEW-CHANNEL-BOT-TOKEN-PLACEHOLDER",
    "slack_channel_id": "C_NEW_CHANNEL_ID_PLACEHOLDER",
    "slack_channel_name": "#exc_sports_ops",
    "fallback_webhook_url": "https://hooks.slack.com/services/NEW/CHANNEL/WEBHOOK_PLACEHOLDER",
    "betfair_app_key": "BETFAIR_APP_KEY_PLACEHOLDER",
    "betfair_username": "BETFAIR_USERNAME_PLACEHOLDER",
    "betfair_password": "BETFAIR_PASSWORD_PLACEHOLDER",
    "certs_dir": r"C:\BetfairScripts\certs",
}

PLACEHOLDER_MARKERS = ("PLACEHOLDER", "CHANGE_ME", "YOUR_", "TODO", "PASTE_")

SPORT_RULE_ALL = "all"
SPORT_RULE_FIRST = "first"
SPORT_RULE_BOXING_BATCHES = "boxing_first_and_gap_batches"
SPORT_RULE_DARTS_GROUPS = "darts_first_competition_gap_and_new_date"
SPORT_RULE_POLITICS_MARKETS = "politics_markets"
SPORT_RULE_WINNER_MARKETS = "winner_markets"
SPORT_RULE_CRICKET_TOSS = "cricket_toss"
DARTS_GROUP_GAP = timedelta(hours=1)
BOXING_BATCH_GAP = timedelta(hours=2)
DEDUP_EVENT = "event"
DEDUP_MARKET = "market"

SPORTS: tuple[dict[str, str], ...] = (
    {"name": "American Football", "rule": SPORT_RULE_ALL, "emoji": ":football:"},
    {"name": "Boxing", "rule": SPORT_RULE_BOXING_BATCHES, "emoji": ":boxing_glove:"},
    {"name": "Cricket", "rule": SPORT_RULE_CRICKET_TOSS, "emoji": ":cricket:"},
    {"name": "Cycling", "rule": SPORT_RULE_WINNER_MARKETS, "emoji": ":bicyclist:"},
    {"name": "Darts", "rule": SPORT_RULE_DARTS_GROUPS, "emoji": ":dart:"},
    {"name": "Gaelic Games", "rule": SPORT_RULE_ALL, "emoji": ":flag-ie:"},
    {"name": "Golf", "rule": SPORT_RULE_WINNER_MARKETS, "emoji": ":golf:"},
    {"name": "Mixed Martial Arts", "rule": SPORT_RULE_FIRST, "emoji": ":martial_arts_uniform:"},
    {"name": "Politics", "rule": SPORT_RULE_POLITICS_MARKETS, "emoji": ":classical_building:"},
    {"name": "Rugby League", "rule": SPORT_RULE_ALL, "emoji": ":rugby_football:"},
    {"name": "Rugby Union", "rule": SPORT_RULE_ALL, "emoji": ":rugby_football:"},
    {"name": "Snooker", "rule": SPORT_RULE_FIRST, "emoji": ":8ball:"},
)

# Fallback IDs are deliberately easy to edit. The script first asks Betfair for
# event types in the scan window and only falls back to this mapping when needed.
FALLBACK_EVENT_TYPE_IDS: dict[str, str] = {
    "American Football": "6423",
    "Boxing": "6",
    "Cricket": "4",
    "Cycling": "11",
    "Darts": "3503",
    "Gaelic Games": "2152880",
    "Golf": "3",
    "Mixed Martial Arts": "26420387",
    "Politics": "2378961",
    "Rugby League": "1477",
    "Rugby Union": "5",
    "Snooker": "6422",
}


@dataclass(frozen=True)
class Config:
    slack_bot_token: str
    slack_channel_id: str
    slack_channel_name: str
    fallback_webhook_url: str
    betfair_app_key: str
    betfair_username: str
    betfair_password: str
    certs_dir: str
    certs_dir_aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class ConfigSource:
    kind: str
    path: Path


@dataclass(frozen=True)
class ScanWindow:
    start_uk: datetime
    end_uk: datetime
    start_utc: datetime
    end_utc: datetime


@dataclass(frozen=True)
class EventReminder:
    sport: str
    emoji: str
    event_type_id: str
    event_id: str
    event_name: str
    competition_id: str
    competition_name: str
    market_id: str
    market_name: str
    event_start_utc: datetime
    market_type_code: str = ""
    country_code: str = ""
    lead_minutes: int = REMINDER_LEAD_MINUTES
    duplicate_by: str = DEDUP_EVENT
    selection_reason: str = ""
    slack_message_override: str = ""
    slack_message_suffix: str = ""
    has_first_try_scorer: bool = False
    first_try_scorer_market_id: str = ""
    first_try_scorer_detection_reason: str = ""

    @property
    def event_start_uk(self) -> datetime:
        return self.event_start_utc.astimezone(UK_TZ)


@dataclass(frozen=True)
class SelectedReminder:
    reminder: EventReminder
    reasons: tuple[str, ...] = ()


@dataclass
class RunStats:
    sports_scanned: int = 0
    raw_markets_found: int = 0
    unique_events_found: int = 0
    reminders_selected: int = 0
    politics_markets_selected: int = 0
    cycling_winner_reminders_selected: int = 0
    golf_winner_reminders_selected: int = 0
    rugby_tip_stream_reminders: int = 0
    cricket_toss_reminders_selected: int = 0
    all_sports_outright_reminders_selected: int = 0
    aus_markets_excluded: int = 0
    scheduled_in_slack: int = 0
    skipped_duplicates: int = 0
    skipped_past_times: int = 0
    failures: int = 0


class ConfigMissing(RuntimeError):
    pass


class ConfigPlaceholderError(RuntimeError):
    pass


def log(message: str) -> None:
    logging.info(message)
    print(message, flush=True)


def object_get(obj: Any, name: str, default: Any = "") -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def object_get_any(obj: Any, names: Iterable[str], default: Any = "") -> Any:
    for name in names:
        value = object_get(obj, name, None)
        if value is not None and str(value).strip() != "":
            return value
    return default


def catalogue_market_description(catalogue: Any) -> Any:
    return object_get_any(catalogue, ("market_description", "marketDescription", "description"), {})


def catalogue_market_type_code(catalogue: Any, fallback: str = "") -> str:
    description = catalogue_market_description(catalogue)
    value = object_get_any(
        catalogue,
        ("market_type_code", "marketTypeCode", "market_type", "marketType"),
        "",
    )
    if not str(value or "").strip():
        value = object_get_any(description, ("market_type_code", "marketTypeCode", "market_type", "marketType"), "")
    return str(value or fallback or "").strip().upper()


def catalogue_market_name(catalogue: Any) -> str:
    return str(object_get(catalogue, "market_name", "") or object_get(catalogue, "marketName", "") or "").strip()


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.casefold())).strip()


OUTRIGHT_SIDE_MARKET_TERMS: tuple[tuple[str, str], ...] = (
    ("classification", "classification_market"),
    ("points", "points_market"),
    ("mountain", "mountains_market"),
    ("young rider", "young_rider_market"),
    ("jersey", "jersey_market"),
    ("stage", "stage_winner_market"),
    ("group winner", "group_winner_market"),
    ("heat winner", "heat_winner_market"),
    ("race winner", "race_winner_market"),
    ("set winner", "set_winner_market"),
    ("frame winner", "frame_winner_market"),
    ("map winner", "map_winner_market"),
    ("game winner", "game_winner_market"),
    ("match winner", "match_winner_market"),
    ("round winner", "round_winner_market"),
    ("period winner", "period_winner_market"),
    ("quarter winner", "quarter_winner_market"),
    ("half winner", "half_winner_market"),
    ("leg winner", "leg_winner_market"),
    ("hole winner", "hole_winner_market"),
    ("team winner", "team_market"),
    ("top 3", "top_finish_market"),
    ("top 5", "top_finish_market"),
    ("top 10", "top_finish_market"),
    ("without", "winner_without_market"),
    ("w o", "winner_without_market"),
    ("enhanced", "enhanced_market"),
    ("special", "special_market"),
    ("nationality", "nationality_market"),
    ("winning nation", "nationality_market"),
    ("winning team", "team_market"),
)


def is_disallowed_country(reminder: EventReminder) -> bool:
    return reminder.country_code.strip().upper() in DISALLOWED_COUNTRY_CODES


def is_winner_like_market_type(market_type_code: str) -> bool:
    market_type = str(market_type_code or "").strip().upper()
    return (
        market_type in {WINNER, OUTRIGHT_WINNER}
        or market_type.endswith("_WINNER")
        or market_type.startswith("WINNER_")
    )


def outright_market_selection(reminder: EventReminder) -> tuple[bool, str]:
    if is_disallowed_country(reminder):
        return False, f"disallowed_country={reminder.country_code.upper()}"

    market_type = reminder.market_type_code.upper()
    market_type_text = normalize_text(market_type)
    market_name = normalize_text(reminder.market_name)
    combined = f"{market_type_text} {market_name}".strip()
    for term, reason in OUTRIGHT_SIDE_MARKET_TERMS:
        if term in combined:
            return False, reason

    if is_winner_like_market_type(market_type):
        return True, f"market_type_code={market_type}"
    if market_name in {"winner", "winner regular", "tournament winner", "outright winner"}:
        return True, f"market_name={market_name}"
    return False, f"not_main_outright:{market_type or 'missing'}"


def is_winner_market(reminder: EventReminder) -> bool:
    is_selected, _reason = outright_market_selection(reminder)
    return is_selected


CYCLING_SIDE_MARKET_TERMS: tuple[tuple[str, str], ...] = (
    ("classification", "classification_market"),
    ("points", "points_market"),
    ("mountain", "mountains_market"),
    ("young rider", "young_rider_market"),
    ("jersey", "jersey_market"),
    ("head to head", "head_to_head_market"),
    ("match bet", "match_bet_market"),
    ("top 3", "top_finish_market"),
    ("top 10", "top_finish_market"),
    ("special", "special_market"),
    ("nationality", "nationality_market"),
    ("winning nation", "nationality_market"),
    ("winning team", "team_market"),
)


def cycling_market_selection(reminder: EventReminder) -> tuple[bool, str]:
    if is_disallowed_country(reminder):
        return False, f"disallowed_country={reminder.country_code.upper()}"
    market_type = reminder.market_type_code.upper()
    market_name = normalize_text(reminder.market_name)
    event_name = normalize_text(reminder.event_name)

    for term, reason in CYCLING_SIDE_MARKET_TERMS:
        if term in market_name:
            return False, reason

    if market_type == STAGE_WINNER:
        event_is_specific_stage = "stage" in event_name and (
            event_name in market_name or market_name in event_name or market_name == "winner"
        )
        if event_is_specific_stage:
            return True, "stage_winner_for_specific_stage_event"
        return False, "stage_winner_side_market"

    if market_type in {"MATCH_BET", "HEAD_TO_HEAD"}:
        return False, "match_bet_or_head_to_head_type"
    is_selected, reason = outright_market_selection(reminder)
    if is_selected:
        return True, reason
    return False, f"market_type_code_not_main_winner:{market_type or 'missing'}:{reason}"


def is_first_try_scorer_market(reminder: EventReminder) -> tuple[bool, str]:
    if reminder.market_type_code.upper() == FIRST_TRY_SCORER:
        return True, f"market_type_code={FIRST_TRY_SCORER}"
    if normalize_text(reminder.market_name) == "first try scorer":
        return True, "market_name=First Try Scorer"
    return False, ""


def is_cricket_toss_market(reminder: EventReminder) -> tuple[bool, str]:
    if reminder.market_type_code.upper() == TO_WIN_THE_TOSS:
        return True, f"market_type_code={TO_WIN_THE_TOSS}"
    if normalize_text(reminder.market_name) == "to win the toss":
        return True, "market_name=To Win the Toss"
    return False, ""


def cricket_event_display_name(event_name: str) -> str:
    name = re.sub(r"\s+", " ", event_name).strip()
    parts = re.split(r"\s+v(?:s\.?)?\s+", name, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return f"{parts[0].strip()} vs {parts[1].strip()}"
    return name


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC_TZ)
    return parsed.astimezone(UTC_TZ)


def format_uk(value: datetime) -> str:
    return value.astimezone(UK_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def format_utc(value: datetime) -> str:
    return value.astimezone(UTC_TZ).strftime("%Y-%m-%d %H:%M:%S UTC")


def betfair_time(value: datetime) -> str:
    return value.astimezone(UTC_TZ).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_placeholder(value: str, *, allow_empty: bool = False) -> bool:
    stripped = str(value or "").strip()
    if not stripped:
        return not allow_empty
    return any(marker in stripped for marker in PLACEHOLDER_MARKERS)


def build_scan_window(now_uk: datetime | None = None, lookahead_hours: float = 24, start_now: bool = False) -> ScanWindow:
    now = now_uk.astimezone(UK_TZ) if now_uk else datetime.now(UK_TZ)
    if start_now:
        start_uk = now
    else:
        start_uk = datetime.combine(now.date(), SCAN_START_TIME_UK, tzinfo=UK_TZ)
    end_uk = start_uk + timedelta(hours=lookahead_hours)
    return ScanWindow(start_uk, end_uk, start_uk.astimezone(UTC_TZ), end_uk.astimezone(UTC_TZ))


def reminder_time(event_start: datetime, lead_minutes: int = REMINDER_LEAD_MINUTES) -> datetime:
    return event_start.astimezone(UK_TZ) - timedelta(minutes=lead_minutes)


def duplicate_key(reminder: EventReminder, scheduled_post_epoch: int, slack_channel_id: str) -> str:
    identity = reminder.market_id if reminder.duplicate_by == DEDUP_MARKET and reminder.market_id else reminder.event_id
    return f"{reminder.sport}|{identity}|{scheduled_post_epoch}|{slack_channel_id}"


def slack_bucket_epoch(post_epoch: int) -> int:
    return post_epoch - (post_epoch % SLACK_BUCKET_SECONDS)


def slack_bucket_warnings(post_epochs: Iterable[int], limit: int = SLACK_SCHEDULE_LIMIT_PER_BUCKET) -> list[str]:
    counts = Counter(slack_bucket_epoch(epoch) for epoch in post_epochs)
    warnings: list[str] = []
    for bucket, count in sorted(counts.items()):
        if count > limit:
            bucket_dt = datetime.fromtimestamp(bucket, UTC_TZ).astimezone(UK_TZ)
            warnings.append(
                f"WARNING Slack scheduled-message bucket limit would be exceeded: "
                f"{count}/{limit} for {format_uk(bucket_dt)}"
            )
    return warnings


def catalogue_to_reminder(catalogue: Any, sport: str, emoji: str, fallback_event_type_id: str) -> EventReminder | None:
    event = object_get(catalogue, "event", {})
    event_type = object_get(catalogue, "event_type", {})
    competition = object_get(catalogue, "competition", {})
    start_utc = parse_datetime(object_get(catalogue, "market_start_time", None))
    event_id = str(object_get(event, "id", "") or "").strip()
    market_name = catalogue_market_name(catalogue)
    event_name = str(object_get(event, "name", "") or market_name or "").strip()
    if not event_id or start_utc is None:
        return None
    return EventReminder(
        sport=sport,
        emoji=emoji,
        event_type_id=str(object_get(event_type, "id", "") or fallback_event_type_id),
        event_id=event_id,
        event_name=event_name,
        competition_id=str(object_get(competition, "id", "") or ""),
        competition_name=str(object_get(competition, "name", "") or ""),
        market_id=str(object_get(catalogue, "market_id", "") or ""),
        market_name=market_name,
        event_start_utc=start_utc,
        market_type_code=catalogue_market_type_code(catalogue),
        country_code=str(object_get_any(event, ("country_code", "countryCode"), "") or "").strip().upper(),
    )


def dedupe_events(markets: Iterable[EventReminder]) -> list[EventReminder]:
    by_event: dict[str, EventReminder] = {}
    for reminder in sorted(markets, key=lambda item: (item.event_start_utc, item.market_id)):
        if reminder.event_id not in by_event:
            by_event[reminder.event_id] = reminder
    return list(by_event.values())


def append_selected(
    selected: list[SelectedReminder],
    selected_indexes: dict[str, int],
    event: EventReminder,
    reason: str,
) -> None:
    existing_index = selected_indexes.get(event.event_id)
    if existing_index is None:
        selected_indexes[event.event_id] = len(selected)
        selected.append(SelectedReminder(event, (reason,)))
        return

    existing = selected[existing_index]
    if reason not in existing.reasons:
        selected[existing_index] = SelectedReminder(existing.reminder, (*existing.reasons, reason))


def competition_key(event: EventReminder) -> str:
    return event.competition_id or event.competition_name or f"event:{event.event_id}"


def select_darts_reminders(events: list[EventReminder], scan_start_uk: datetime | None) -> list[SelectedReminder]:
    if not events:
        return []
    scan_date = (scan_start_uk or events[0].event_start_uk).astimezone(UK_TZ).date()
    selected: list[SelectedReminder] = []
    selected_indexes: dict[str, int] = {}
    by_competition: dict[str, list[EventReminder]] = {}
    for event in events:
        by_competition.setdefault(competition_key(event), []).append(event)

    for competition_events in by_competition.values():
        previous_event: EventReminder | None = None
        seen_dates: set[Any] = set()
        for event in competition_events:
            event_date = event.event_start_uk.date()
            if previous_event is None:
                append_selected(selected, selected_indexes, event, "first_in_competition")
                seen_dates.add(event_date)
                if event_date != scan_date:
                    append_selected(selected, selected_indexes, event, "first_on_new_scan_date")
                previous_event = event
                continue

            if event.event_start_uk - previous_event.event_start_uk > DARTS_GROUP_GAP:
                append_selected(selected, selected_indexes, event, "new_group_gap_gt_1h")
            if event_date != scan_date and event_date not in seen_dates:
                append_selected(selected, selected_indexes, event, "first_on_new_scan_date")
            seen_dates.add(event_date)
            previous_event = event

    return sorted(selected, key=lambda item: (item.reminder.event_start_utc, item.reminder.event_name.casefold(), item.reminder.event_id))


def select_boxing_reminders(events: list[EventReminder]) -> list[SelectedReminder]:
    selected: list[SelectedReminder] = []
    selected_indexes: dict[str, int] = {}
    previous_event: EventReminder | None = None
    for event in events:
        if previous_event is None:
            append_selected(selected, selected_indexes, event, "first_boxing_fight")
        elif event.event_start_uk - previous_event.event_start_uk > BOXING_BATCH_GAP:
            append_selected(selected, selected_indexes, event, "new_boxing_batch_gap_gt_2h")
        previous_event = event
    return selected


def select_reminders_with_reasons(
    events: Iterable[EventReminder],
    rule: str,
    scan_start_uk: datetime | None = None,
) -> list[SelectedReminder]:
    sorted_events = sorted(
        (event for event in events if not is_disallowed_country(event)),
        key=lambda item: (item.event_start_utc, item.event_name.casefold(), item.event_id),
    )
    if rule == SPORT_RULE_ALL:
        return [SelectedReminder(event) for event in sorted_events]
    if rule == SPORT_RULE_FIRST:
        return [SelectedReminder(event) for event in sorted_events[:1]]
    if rule == SPORT_RULE_BOXING_BATCHES:
        return select_boxing_reminders(sorted_events)
    if rule == SPORT_RULE_DARTS_GROUPS:
        return select_darts_reminders(sorted_events, scan_start_uk)
    raise ValueError(f"Unknown selection rule: {rule}")


def select_reminders(
    events: Iterable[EventReminder],
    rule: str,
    scan_start_uk: datetime | None = None,
) -> list[EventReminder]:
    return [selected.reminder for selected in select_reminders_with_reasons(events, rule, scan_start_uk)]


def in_scan_window(reminder: EventReminder, window: ScanWindow | None) -> bool:
    if window is None:
        return True
    start_utc = reminder.event_start_utc.astimezone(UTC_TZ)
    return window.start_utc <= start_utc < window.end_utc


def select_market_reminders(
    markets: Iterable[EventReminder],
    sport: str,
    window: ScanWindow | None = None,
) -> list[EventReminder]:
    selected: list[EventReminder] = []
    for market in sorted(markets, key=lambda item: (item.event_start_utc, item.event_name.casefold(), item.market_id)):
        if is_disallowed_country(market) or not in_scan_window(market, window):
            continue
        if sport == "Politics":
            selected.append(
                replace(
                    market,
                    duplicate_by=DEDUP_MARKET,
                    selection_reason="politics_market_in_window",
                    slack_message_override=(
                        f"{market.emoji} {market.event_name} - {market.market_name} "
                        f"(Market ID: {market.market_id})"
                    ),
                )
            )
        elif sport == "Cycling":
            is_selected, reason = cycling_market_selection(market)
            if is_selected:
                selected.append(
                    replace(
                        market,
                        duplicate_by=DEDUP_MARKET,
                        selection_reason=f"cycling_main_winner:{reason}",
                    )
                )
        elif sport == "Golf":
            is_selected, reason = outright_market_selection(market)
            if is_selected:
                selected.append(
                    replace(
                        market,
                        duplicate_by=DEDUP_MARKET,
                        selection_reason=f"golf_main_outright:{reason}",
                    )
                )
        elif sport == "Cricket":
            is_toss, _reason = is_cricket_toss_market(market)
            if is_toss:
                selected.append(
                    replace(
                        market,
                        duplicate_by=DEDUP_MARKET,
                        lead_minutes=CRICKET_TOSS_LEAD_MINUTES,
                        selection_reason="cricket_to_win_toss",
                        slack_message_override=(
                            f"{market.emoji} Suspend toss in {cricket_event_display_name(market.event_name)} "
                            f"- Market ID: {market.market_id}"
                        ),
                    )
                )
    return selected


def first_try_scorer_by_event(markets: Iterable[EventReminder]) -> dict[str, EventReminder]:
    matches: dict[str, EventReminder] = {}
    for market in sorted(markets, key=lambda item: (item.event_start_utc, item.market_id)):
        is_match, reason = is_first_try_scorer_market(market)
        if is_match and market.event_id not in matches:
            matches[market.event_id] = replace(market, first_try_scorer_detection_reason=reason)
    return matches


def apply_rugby_first_try_flags(
    selected: Iterable[SelectedReminder],
    first_try_markets: dict[str, EventReminder],
) -> list[SelectedReminder]:
    enriched: list[SelectedReminder] = []
    for item in selected:
        reminder = item.reminder
        first_try_market = first_try_markets.get(reminder.event_id)
        if first_try_market:
            reminder = replace(
                reminder,
                slack_message_suffix=" TIP with stream",
                has_first_try_scorer=True,
                first_try_scorer_market_id=first_try_market.market_id,
                first_try_scorer_detection_reason=first_try_market.first_try_scorer_detection_reason,
            )
        enriched.append(SelectedReminder(reminder, item.reasons))
    return enriched


def default_json_config_path() -> Path:
    return WINDOWS_CONFIG_PATH if os.name == "nt" else EC2_CONFIG_PATH


def resolve_config_source(cli_config_path: str = "") -> ConfigSource:
    if cli_config_path.strip():
        return ConfigSource("json", Path(cli_config_path).expanduser())
    env_config_path = os.getenv(CONFIG_ENV_VAR, "").strip()
    if env_config_path:
        return ConfigSource("json", Path(env_config_path).expanduser())
    if EC2_ENV_PATH.exists():
        return ConfigSource("env", EC2_ENV_PATH)
    if WINDOWS_ENV_PATH.exists():
        return ConfigSource("env", WINDOWS_ENV_PATH)
    return ConfigSource("json", default_json_config_path())


def resolve_config_path(cli_config_path: str = "") -> Path:
    return resolve_config_source(cli_config_path).path


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_env(path: Path) -> dict[str, str]:
    values = dotenv_values(path)
    return {str(key): str(value) for key, value in values.items() if value is not None}


def first_env_value(values: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = values.get(key, "")
        if value:
            return str(value).strip()
    return ""


def env_cert_values(values: dict[str, str]) -> tuple[str, ...]:
    cert_values = [
        first_env_value(values, "BETFAIR_CERTS_DIR"),
        first_env_value(values, "CERTS_DIR"),
    ]
    return tuple(value for value in cert_values if value)


def env_to_config(values: dict[str, str]) -> Config:
    cert_values = env_cert_values(values)
    return Config(
        slack_bot_token=first_env_value(values, "BETFAIR_EVENT_REMINDERS_SLACK_BOT_TOKEN", "SLACK_BOT_TOKEN"),
        slack_channel_id=first_env_value(values, "BETFAIR_EVENT_REMINDERS_SLACK_CHANNEL_ID", "SLACK_CHANNEL_ID"),
        slack_channel_name=first_env_value(
            values,
            "BETFAIR_EVENT_REMINDERS_SLACK_CHANNEL_NAME",
            "SLACK_CHANNEL_NAME",
        )
        or PLACEHOLDER_CONFIG["slack_channel_name"],
        fallback_webhook_url=first_env_value(
            values,
            "BETFAIR_EVENT_REMINDERS_FALLBACK_WEBHOOK_URL",
            "BETFAIR_EVENT_REMINDERS_WEBHOOK_URL",
            "SLACK_WEBHOOK_URL",
        ),
        betfair_app_key=first_env_value(values, "BETFAIR_APP_KEY"),
        betfair_username=first_env_value(values, "BETFAIR_USERNAME"),
        betfair_password=first_env_value(values, "BETFAIR_PASSWORD"),
        certs_dir=cert_values[0] if cert_values else PLACEHOLDER_CONFIG["certs_dir"],
        certs_dir_aliases=cert_values,
    )


def missing_config_message(source: ConfigSource) -> str:
    if source.kind == "env":
        return "\n".join(
            [
                f"Missing .env secrets file: {source.path}",
                "Create /opt/betfair-scripts/.env on EC2 with the Betfair Event Reminders values.",
                "Do not commit .env to Git.",
            ]
        )
    return "\n".join(
        [
            f"Missing real config file: {source.path}",
            "Preferred EC2 setup: store secrets in /opt/betfair-scripts/.env, then run --dry-run first.",
            "JSON is only needed when using --config or BETFAIR_EVENT_REMINDERS_CONFIG.",
            "Do not commit real config or .env files to Git.",
        ]
    )


def load_config(source: ConfigSource) -> Config:
    if not source.path.exists():
        raise ConfigMissing(
            missing_config_message(source)
        )
    if source.kind == "env":
        return env_to_config(read_env(source.path))
    data = read_json(source.path)
    if not isinstance(data, dict):
        raise RuntimeError(f"Config must be a JSON object: {source.path}")
    merged = {**PLACEHOLDER_CONFIG, **data}
    return Config(**{key: str(merged.get(key, "") or "") for key in PLACEHOLDER_CONFIG})


def placeholder_fields(config: Config) -> list[str]:
    missing: list[str] = []
    if is_placeholder(config.slack_bot_token):
        missing.append("slack_bot_token")
    if is_placeholder(config.slack_channel_id):
        missing.append("slack_channel_id")
    if is_placeholder(config.betfair_app_key):
        missing.append("betfair_app_key")
    if is_placeholder(config.betfair_username):
        missing.append("betfair_username")
    if is_placeholder(config.betfair_password):
        missing.append("betfair_password")
    return missing


def validate_config(config: Config, source: ConfigSource) -> None:
    missing = placeholder_fields(config)
    if missing:
        raise ConfigPlaceholderError(
            "\n".join(
                [
                    f"Config source is missing required values or still contains placeholders: {', '.join(missing)}",
                    f"Config source: {source.path}",
                    "Fill in the EC2-only real values, including the new Slack bot token and channel ID.",
                    "fallback_webhook_url is optional only and is not used for scheduled reminders.",
                    "No Betfair or Slack calls were made.",
                ]
            )
        )


def setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"betfair_event_reminders_{datetime.now(UK_TZ):%Y%m%d}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8")],
        force=True,
    )
    return log_path


def is_windows_drive_path(value: str) -> bool:
    return bool(WINDOWS_DRIVE_PATH_RE.match(value.strip()))


def missing_cert_files(certs_dir: Path) -> list[str]:
    return [name for name in (CERT_FILE_NAME, KEY_FILE_NAME) if not (certs_dir / name).exists()]


def resolve_betfair_certs_dir(
    raw_config_value: str | Iterable[str],
    repo_root: Path,
    *,
    is_windows_host: bool | None = None,
    ec2_certs_dir: Path | None = None,
) -> Path:
    is_windows = os.name == "nt" if is_windows_host is None else is_windows_host
    configured_values = (
        [raw_config_value]
        if isinstance(raw_config_value, str)
        else list(raw_config_value or [])
    )
    candidates: list[Path] = []

    for raw_value in configured_values:
        configured = str(raw_value or "").strip()
        if not configured:
            continue
        log(f"Configured Betfair cert path: {configured}")
        if not is_windows and is_windows_drive_path(configured):
            log(f"Ignoring Windows-style cert path on Linux: {configured}")
            continue
        configured_path = Path(configured).expanduser()
        if not is_windows and configured.startswith("/"):
            candidates.append(configured_path)
        elif configured_path.is_absolute():
            candidates.append(configured_path)
        else:
            candidates.append((repo_root / configured_path).resolve())

    candidates.extend(
        [
            ec2_certs_dir or (EC2_ROOT / "certs"),
            (repo_root / "certs").resolve(),
        ]
    )

    seen: set[str] = set()
    checked: list[str] = []
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        missing = missing_cert_files(candidate)
        if not missing:
            return candidate
        checked.append(f"{candidate} missing {', '.join(missing)}")

    raise FileNotFoundError("Betfair certificate files not found. Checked: " + "; ".join(checked))


def build_client(config: Config) -> APIClient:
    certs_dir = resolve_betfair_certs_dir(config.certs_dir_aliases or config.certs_dir, PROJECT_ROOT)
    cert_file = certs_dir / CERT_FILE_NAME
    key_file = certs_dir / KEY_FILE_NAME
    log(f"Resolved Betfair cert path: {certs_dir}")
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
    log("Betfair login success")
    return client


def discover_event_type_ids(client: APIClient, window: ScanWindow) -> dict[str, str]:
    event_filter = market_filter(
        market_start_time={"from": betfair_time(window.start_utc), "to": betfair_time(window.end_utc)}
    )
    results = client.betting.list_event_types(filter=event_filter)
    discovered: dict[str, str] = {}
    for result in results:
        event_type = object_get(result, "event_type", {})
        name = str(object_get(event_type, "name", "") or "").strip()
        event_type_id = str(object_get(event_type, "id", "") or "").strip()
        if name and event_type_id:
            discovered[name] = event_type_id
    return discovered


def list_event_type_ids(
    client: APIClient,
    window: ScanWindow,
    discovered_event_types: dict[str, str] | None = None,
) -> dict[str, str]:
    discovered = (
        discovered_event_types
        if discovered_event_types is not None
        else discover_event_type_ids(client, window)
    )
    discovered_by_name = {name.casefold(): event_type_id for name, event_type_id in discovered.items()}
    sport_ids: dict[str, str] = {}
    for sport in SPORTS:
        name = sport["name"]
        sport_ids[name] = discovered_by_name.get(name.casefold()) or FALLBACK_EVENT_TYPE_IDS[name]
    log(f"Sport event type mapping: {sport_ids}")
    return sport_ids


def list_market_catalogues(
    client: APIClient,
    event_type_id: str,
    window: ScanWindow,
    market_type_codes: Iterable[str] | None = (MATCH_ODDS,),
) -> list[Any]:
    filter_args: dict[str, Any] = {
        "event_type_ids": [event_type_id],
        "market_start_time": {"from": betfair_time(window.start_utc), "to": betfair_time(window.end_utc)},
    }
    if market_type_codes is not None:
        filter_args["market_type_codes"] = list(market_type_codes)
    fixture_filter = market_filter(**filter_args)
    return client.betting.list_market_catalogue(
        filter=fixture_filter,
        market_projection=["EVENT", "EVENT_TYPE", "COMPETITION", "MARKET_START_TIME", "MARKET_DESCRIPTION"],
        max_results=1000,
        sort="FIRST_TO_START",
    )


def api_market_type_filter_for_sport(sport_name: str, rule: str) -> tuple[str, ...] | None:
    if sport_name in {"Politics", "Cricket", "Cycling", "Golf"}:
        return None
    if rule == SPORT_RULE_WINNER_MARKETS:
        return (WINNER,)
    return (MATCH_ODDS,)


def list_market_type_codes(client: APIClient, event_type_id: str, window: ScanWindow) -> tuple[str, ...]:
    fixture_filter = market_filter(
        event_type_ids=[event_type_id],
        market_start_time={"from": betfair_time(window.start_utc), "to": betfair_time(window.end_utc)},
    )
    results = client.betting.list_market_types(filter=fixture_filter)
    values = {
        str(object_get_any(result, ("market_type", "marketType"), "") or "").strip().upper()
        for result in results
    }
    return tuple(sorted(value for value in values if value))


def sport_emoji(sport_name: str) -> str:
    for sport in SPORTS:
        if sport["name"].casefold() == sport_name.casefold():
            return sport["emoji"]
    return GENERIC_OUTRIGHT_EMOJI


def discover_all_sports_outright_reminders(
    client: APIClient,
    event_types: dict[str, str],
    window: ScanWindow,
) -> tuple[list[EventReminder], set[str]]:
    selected: list[EventReminder] = []
    aus_excluded: set[str] = set()
    for sport_name, event_type_id in sorted(event_types.items(), key=lambda item: item[0].casefold()):
        observed_types = list_market_type_codes(client, event_type_id, window)
        candidate_types = tuple(code for code in observed_types if is_winner_like_market_type(code))
        if not candidate_types:
            continue
        log(f"All-sports outright scan: sport={sport_name} candidate_market_types={', '.join(candidate_types)}")
        catalogues = list_market_catalogues(client, event_type_id, window, market_type_codes=candidate_types)
        for catalogue in catalogues:
            reminder = catalogue_to_reminder(catalogue, sport_name, sport_emoji(sport_name), event_type_id)
            if reminder is None:
                continue
            if is_disallowed_country(reminder):
                aus_excluded.add(reminder.market_id or f"{sport_name}|{reminder.event_id}")
                log(
                    f"Excluded AU outright market: sport={sport_name} event_id={reminder.event_id} "
                    f"market_id={reminder.market_id} market_type_code={reminder.market_type_code}"
                )
                continue
            is_selected, reason = outright_market_selection(reminder)
            if not is_selected:
                log(
                    f"Excluded all-sports winner-like market: sport={sport_name} event_id={reminder.event_id} "
                    f"market_id={reminder.market_id} market_type_code={reminder.market_type_code} "
                    f"market_name={reminder.market_name!r} reason={reason}"
                )
                continue
            selected.append(
                replace(
                    reminder,
                    duplicate_by=DEDUP_MARKET,
                    selection_reason=f"all_sports_main_outright:{reason}",
                    slack_message_override=(
                        f"{reminder.emoji} {sport_name}: {reminder.event_name} - {reminder.market_name} "
                        f"(Market ID: {reminder.market_id})"
                    ),
                )
            )
    return selected, aus_excluded


def dedupe_market_candidates(reminders: Iterable[EventReminder]) -> list[EventReminder]:
    selected: list[EventReminder] = []
    seen: set[str] = set()
    for reminder in reminders:
        identity = reminder.market_id or f"{reminder.sport}|{reminder.event_id}|{reminder.market_type_code}"
        if identity in seen:
            continue
        seen.add(identity)
        selected.append(reminder)
    return selected


def catalogue_total_matched(catalogue: Any) -> str:
    value = object_get_any(catalogue, ("total_matched", "totalMatched"), "")
    return str(value) if value != "" else "unavailable"


def log_cycling_market_diagnostic(catalogue: Any, event_type_id: str) -> None:
    reminder = catalogue_to_reminder(catalogue, "Cycling", ":bicyclist:", event_type_id)
    if reminder is None:
        log(f"Cycling diagnostic skipped malformed catalogue: {catalogue!r}")
        return
    log(
        "Cycling catalogue market: "
        f"event_name={reminder.event_name!r} event_id={reminder.event_id} "
        f"competition_name={reminder.competition_name!r} competition_id={reminder.competition_id or 'none'} "
        f"market_name={reminder.market_name!r} market_id={reminder.market_id} "
        f"market_type_code={reminder.market_type_code or 'none'} "
        f"market_start_utc={format_utc(reminder.event_start_utc)} "
        f"market_start_uk={format_uk(reminder.event_start_uk)} "
        f"total_matched={catalogue_total_matched(catalogue)}"
    )


def run_cycling_market_diagnostic(client: APIClient, window: ScanWindow, event_type_id: str) -> int:
    log("Cycling diagnostic mode: broad catalogue query; Slack scheduling and reminder state writes are disabled.")
    catalogues = list_market_catalogues(client, event_type_id, window, market_type_codes=None)
    log(f"Cycling catalogue markets before filter: {len(catalogues)}")
    market_types = sorted({catalogue_market_type_code(item) or "<none>" for item in catalogues})
    log(f"Cycling market types observed: {', '.join(market_types) if market_types else 'none'}")
    for catalogue in catalogues:
        log_cycling_market_diagnostic(catalogue, event_type_id)
    return 0


def chunked(values: list[str], size: int) -> Iterable[list[str]]:
    size = max(1, size)
    for index in range(0, len(values), size):
        yield values[index:index + size]


def list_market_catalogues_for_events(
    client: APIClient,
    event_ids: Iterable[str],
    *,
    batch_size: int = 40,
) -> list[Any]:
    catalogues: list[Any] = []
    unique_event_ids = sorted({event_id for event_id in event_ids if event_id})
    for event_id_batch in chunked(unique_event_ids, batch_size):
        fixture_filter = market_filter(event_ids=event_id_batch)
        catalogues.extend(
            client.betting.list_market_catalogue(
                filter=fixture_filter,
                market_projection=["EVENT", "EVENT_TYPE", "COMPETITION", "MARKET_START_TIME", "MARKET_DESCRIPTION"],
                max_results=1000,
                sort="FIRST_TO_START",
            )
        )
    return catalogues


def load_state(path: Path = STATE_PATH) -> dict[str, Any]:
    if not path.exists():
        return {"scheduled": []}
    try:
        data = read_json(path)
    except json.JSONDecodeError:
        backup = path.with_name(f"{path.name}.{datetime.now(UK_TZ):%Y%m%d%H%M%S}.bak")
        shutil.copy2(path, backup)
        log(f"State JSON was corrupt; backed up to {backup}")
        return {"scheduled": []}
    if not isinstance(data, dict):
        return {"scheduled": []}
    scheduled = data.get("scheduled", [])
    return {"scheduled": scheduled if isinstance(scheduled, list) else []}


def save_state(state: dict[str, Any], path: Path = STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(path.parent), suffix=".tmp") as handle:
        handle.write(payload)
        temp_path = Path(handle.name)
    temp_path.replace(path)


def scheduled_keys(state: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for record in state.get("scheduled", []):
        if isinstance(record, dict) and record.get("duplicate_key"):
            keys.add(str(record["duplicate_key"]))
    return keys


def format_slack_text(reminder: EventReminder) -> str:
    if reminder.slack_message_override:
        return reminder.slack_message_override
    return f"{reminder.emoji} {reminder.event_name} (Event ID: {reminder.event_id}){reminder.slack_message_suffix}"


def schedule_slack_message(config: Config, reminder: EventReminder, post_epoch: int) -> str:
    response = requests.post(
        SLACK_SCHEDULE_URL,
        headers={"Authorization": f"Bearer {config.slack_bot_token}", "Content-Type": "application/json"},
        json={"channel": config.slack_channel_id, "text": format_slack_text(reminder), "post_at": post_epoch},
        timeout=20,
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Slack returned non-JSON response: status={response.status_code} body={response.text}") from exc
    if response.status_code >= 400 or not payload.get("ok"):
        error = str(payload.get("error", "unknown_error"))
        if error == "restricted_too_many":
            raise RuntimeError("Slack chat.scheduleMessage failed: restricted_too_many")
        raise RuntimeError(f"Slack chat.scheduleMessage failed: {error}")
    return str(payload.get("scheduled_message_id") or "")


def state_record(reminder: EventReminder, key: str, post_epoch: int, scheduled_message_id: str) -> dict[str, Any]:
    return {
        "duplicate_key": key,
        "sport": reminder.sport,
        "event_id": reminder.event_id,
        "event_name": reminder.event_name,
        "competition_id": reminder.competition_id,
        "competition_name": reminder.competition_name,
        "market_id": reminder.market_id,
        "market_name": reminder.market_name,
        "market_type_code": reminder.market_type_code,
        "country_code": reminder.country_code,
        "event_start_uk": format_uk(reminder.event_start_uk),
        "scheduled_slack_post_uk": format_uk(datetime.fromtimestamp(post_epoch, UTC_TZ)),
        "scheduled_slack_post_epoch": post_epoch,
        "reminder_offset_minutes": reminder.lead_minutes,
        "selection_reason": reminder.selection_reason,
        "slack_text": format_slack_text(reminder),
        "has_first_try_scorer": reminder.has_first_try_scorer,
        "first_try_scorer_market_id": reminder.first_try_scorer_market_id,
        "first_try_scorer_detection_reason": reminder.first_try_scorer_detection_reason,
        "slack_scheduled_message_id": scheduled_message_id,
        "created_at_uk": format_uk(datetime.now(UK_TZ)),
    }


def run_scan(args: argparse.Namespace, config: Config, source: ConfigSource) -> int:
    log_path = setup_logging()
    log("Betfair Event Reminders starting")
    log(f"Config source: {source.path}")
    log(f"Log path: {log_path}")
    log(f"Slack channel: {config.slack_channel_name} ({config.slack_channel_id})")
    log("Scheduling method: Slack Web API chat.scheduleMessage")
    if config.fallback_webhook_url:
        log("Fallback webhook configured but not used for scheduled reminders.")
    if args.dry_run:
        log("Dry run enabled: no Slack messages will be scheduled and no state records will be written.")

    validate_config(config, source)
    window = build_scan_window(lookahead_hours=args.lookahead_hours, start_now=args.start_now)
    log(f"UK scan window: {format_uk(window.start_uk)} -> {format_uk(window.end_uk)}")
    log(f"UTC scan window: {format_utc(window.start_utc)} -> {format_utc(window.end_utc)}")

    stats = RunStats()
    all_selected: list[EventReminder] = []
    excluded_au_market_ids: set[str] = set()
    client = build_client(config)
    try:
        all_event_types = discover_event_type_ids(client, window)
        sport_ids = list_event_type_ids(client, window, all_event_types)
        if args.debug_cycling_markets:
            return run_cycling_market_diagnostic(client, window, sport_ids["Cycling"])
        for sport in SPORTS:
            sport_name = sport["name"]
            stats.sports_scanned += 1
            event_type_id = sport_ids[sport_name]
            log(f"Scanning {sport_name} eventTypeId={event_type_id}")
            market_type_filter = api_market_type_filter_for_sport(sport_name, sport["rule"])
            raw_catalogues = list_market_catalogues(
                client,
                event_type_id,
                window,
                market_type_codes=market_type_filter,
            )
            stats.raw_markets_found += len(raw_catalogues)
            reminders = [
                reminder
                for catalogue in raw_catalogues
                if (reminder := catalogue_to_reminder(catalogue, sport_name, sport["emoji"], event_type_id)) is not None
            ]
            aus_reminders = [reminder for reminder in reminders if is_disallowed_country(reminder)]
            for market in aus_reminders:
                excluded_au_market_ids.add(market.market_id or f"{sport_name}|{market.event_id}")
                log(
                    f"Excluded AU market: sport={sport_name} event_id={market.event_id} "
                    f"market_id={market.market_id} market_type_code={market.market_type_code}"
                )
            reminders = [reminder for reminder in reminders if not is_disallowed_country(reminder)]
            if sport["rule"] in {SPORT_RULE_POLITICS_MARKETS, SPORT_RULE_WINNER_MARKETS, SPORT_RULE_CRICKET_TOSS}:
                unique_events = dedupe_events(reminders)
                selected = select_market_reminders(reminders, sport_name, window)
                if sport_name == "Politics":
                    stats.politics_markets_selected += len(selected)
                elif sport_name == "Cycling":
                    stats.cycling_winner_reminders_selected += len(selected)
                    log(f"Cycling catalogue markets before filter: {len(reminders)}")
                    market_types = sorted({market.market_type_code or "<none>" for market in reminders})
                    log(f"Cycling market types observed: {', '.join(market_types) if market_types else 'none'}")
                    for market in reminders:
                        is_selected, reason = cycling_market_selection(market)
                        if is_selected:
                            log(
                                f"Selected Cycling main winner: event_name={market.event_name!r} "
                                f"event_id={market.event_id} market_name={market.market_name!r} "
                                f"market_id={market.market_id} market_type_code={market.market_type_code} "
                                f"reason={reason}"
                            )
                        else:
                            log(
                                f"Excluded Cycling market: event_id={market.event_id} market_id={market.market_id} "
                                f"market_type_code={market.market_type_code} market_name={market.market_name!r} "
                                f"reason={reason}"
                            )
                    log(f"Cycling selected after filter: {len(selected)}")
                elif sport_name == "Golf":
                    stats.golf_winner_reminders_selected += len(selected)
                    for market in reminders:
                        is_selected, reason = outright_market_selection(market)
                        if not is_selected:
                            log(
                                f"Excluded Golf market: event_id={market.event_id} market_id={market.market_id} "
                                f"market_type_code={market.market_type_code} market_name={market.market_name!r} "
                                f"reason={reason}"
                            )
                elif sport_name == "Cricket":
                    stats.cricket_toss_reminders_selected += len(selected)
                    for market in reminders:
                        is_toss, _reason = is_cricket_toss_market(market)
                        if not is_toss:
                            log(
                                f"Excluded Cricket market: event_id={market.event_id} market_id={market.market_id} "
                                f"market_type_code={market.market_type_code} market_name={market.market_name!r} "
                                f"reason=not_to_win_the_toss"
                            )
            else:
                unique_events = dedupe_events(reminders)
                selected_with_reasons = select_reminders_with_reasons(unique_events, sport["rule"], window.start_uk)
                if sport_name in {"Rugby League", "Rugby Union"}:
                    rugby_event_ids = [item.reminder.event_id for item in selected_with_reasons]
                    first_try_catalogues = list_market_catalogues_for_events(client, rugby_event_ids)
                    first_try_reminders = [
                        reminder
                        for catalogue in first_try_catalogues
                        if (
                            reminder := catalogue_to_reminder(catalogue, sport_name, sport["emoji"], event_type_id)
                        )
                        is not None
                    ]
                    selected_with_reasons = apply_rugby_first_try_flags(
                        selected_with_reasons,
                        first_try_scorer_by_event(first_try_reminders),
                    )
                selected = []
                for item in selected_with_reasons:
                    reason_label = ",".join(item.reasons)
                    selection_reason = reason_label or (
                        "rugby_match_odds_event" if sport_name in {"Rugby League", "Rugby Union"} else ""
                    )
                    selected.append(replace(item.reminder, selection_reason=selection_reason))
                if sport_name in {"Rugby League", "Rugby Union"}:
                    stats.rugby_tip_stream_reminders += sum(1 for item in selected if item.has_first_try_scorer)
                    for item in selected:
                        log(
                            f"Selected {sport_name} reminder: event_id={item.event_id} "
                            f"has_first_try_scorer={str(item.has_first_try_scorer).lower()} "
                            f"first_try_scorer_market_id={item.first_try_scorer_market_id or 'none'} "
                            f"detection={item.first_try_scorer_detection_reason or 'none'} "
                            f"text={format_slack_text(item)!r} reason={item.selection_reason}"
                        )
                else:
                    for item in selected:
                        if item.selection_reason:
                            log(
                                f"Selected {sport_name} reminder: event_id={item.event_id} "
                                f"reason={item.selection_reason}"
                            )
            stats.unique_events_found += len(unique_events)
            stats.reminders_selected += len(selected)
            all_selected.extend(selected)
            for item in selected:
                log(
                    f"Selected {sport_name}: event_name={item.event_name!r} event_id={item.event_id} "
                    f"market_name={item.market_name!r} market_type_code={item.market_type_code} "
                    f"market_id={item.market_id} country_code={item.country_code or 'unknown'} "
                    f"start={format_uk(item.event_start_uk)} "
                    f"reminder={format_uk(reminder_time(item.event_start_uk, item.lead_minutes))} "
                    f"offset_minutes={item.lead_minutes} reason={item.selection_reason or 'selected'}"
                )
            log(
                f"{sport_name}: raw markets={len(raw_catalogues)}, "
                f"unique events={len(unique_events)}, selected reminders={len(selected)}"
            )

        all_sports_outrights, all_sports_aus_excluded = discover_all_sports_outright_reminders(
            client,
            all_event_types,
            window,
        )
        excluded_au_market_ids.update(all_sports_aus_excluded)
        stats.aus_markets_excluded = len(excluded_au_market_ids)
        stats.all_sports_outright_reminders_selected = len(all_sports_outrights)
        all_selected.extend(all_sports_outrights)
        all_selected = dedupe_market_candidates(all_selected)
        stats.reminders_selected = len(all_selected)
    finally:
        try:
            client.logout()
        except Exception:
            pass

    post_epochs = [int(reminder_time(item.event_start_uk, item.lead_minutes).timestamp()) for item in all_selected]
    for warning in slack_bucket_warnings(post_epochs):
        log(warning)

    state = load_state()
    existing_keys = scheduled_keys(state)
    if args.force:
        log("Force mode enabled: duplicate record checks will be ignored.")

    now_uk = datetime.now(UK_TZ)
    for reminder in all_selected:
        if is_disallowed_country(reminder):
            log(
                f"SAFETY SKIP disallowed country before Slack scheduling: sport={reminder.sport} "
                f"event_id={reminder.event_id} market_id={reminder.market_id} country_code={reminder.country_code}"
            )
            continue
        post_time_uk = reminder_time(reminder.event_start_uk, reminder.lead_minutes)
        post_epoch = int(post_time_uk.timestamp())
        reason_suffix = f" reason={reminder.selection_reason}" if reminder.selection_reason else ""
        log(
            f"Reminder candidate: channel={config.slack_channel_name} post_at={format_uk(post_time_uk)} "
            f"text={format_slack_text(reminder)!r}{reason_suffix}"
        )
        if post_time_uk <= now_uk:
            stats.skipped_past_times += 1
            log(f"SKIP time in past: {reminder.sport} {reminder.event_name} post_at={format_uk(post_time_uk)}")
            continue
        key = duplicate_key(reminder, post_epoch, config.slack_channel_id)
        if not args.force and key in existing_keys:
            stats.skipped_duplicates += 1
            log(f"SKIP duplicate already scheduled: {key}")
            continue
        if args.dry_run:
            log(
                f"DRY RUN would schedule chat.scheduleMessage channel={config.slack_channel_id} "
                f"post_at={post_epoch} text={format_slack_text(reminder)!r}"
            )
            continue
        try:
            scheduled_message_id = schedule_slack_message(config, reminder, post_epoch)
        except Exception as exc:
            stats.failures += 1
            log(f"Slack scheduling failure for event {reminder.event_id}: {exc}")
            continue
        stats.scheduled_in_slack += 1
        log(f"Slack scheduling success: event={reminder.event_id} scheduled_message_id={scheduled_message_id}")
        record = state_record(reminder, key, post_epoch, scheduled_message_id)
        state.setdefault("scheduled", []).append(record)
        existing_keys.add(key)
        save_state(state)

    print_summary(window, stats)
    return 1 if stats.failures else 0


def print_summary(window: ScanWindow, stats: RunStats) -> None:
    lines = [
        "",
        "Betfair Event Reminders Summary",
        f"Scan window UK: {window.start_uk.strftime('%Y-%m-%d %H:%M')} -> {window.end_uk.strftime('%Y-%m-%d %H:%M')}",
        f"Sports scanned: {stats.sports_scanned}",
        f"Raw markets found: {stats.raw_markets_found}",
        f"Unique events found: {stats.unique_events_found}",
        f"Reminders selected: {stats.reminders_selected}",
        f"Politics markets selected: {stats.politics_markets_selected}",
        f"Cycling winner reminders selected: {stats.cycling_winner_reminders_selected}",
        f"Golf winner reminders selected: {stats.golf_winner_reminders_selected}",
        f"Rugby reminders with TIP with stream: {stats.rugby_tip_stream_reminders}",
        f"Cricket toss reminders selected: {stats.cricket_toss_reminders_selected}",
        f"All-sports outright reminders discovered: {stats.all_sports_outright_reminders_selected}",
        f"AU markets excluded: {stats.aus_markets_excluded}",
        f"Scheduled in Slack: {stats.scheduled_in_slack}",
        f"Skipped duplicates: {stats.skipped_duplicates}",
        f"Skipped past times: {stats.skipped_past_times}",
        f"Failures: {stats.failures}",
    ]
    for line in lines:
        log(line)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Schedule Slack reminders for selected Betfair Exchange events.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Production config guidance:\n"
            "  Store EC2 production secrets in /opt/betfair-scripts/.env.\n"
            "  Keep .env and real JSON config files out of Git, then run --dry-run first.\n"
            f"  JSON override: set {CONFIG_ENV_VAR} or pass --config /opt/betfair-scripts/config/betfair_event_reminders_config.json."
        ),
    )
    parser.add_argument("--config", default="", help="Path to an explicit real JSON config file.")
    parser.add_argument("--dry-run", action="store_true", help="Scan and print what would be scheduled without Slack/state writes.")
    parser.add_argument(
        "--debug-cycling-markets",
        action="store_true",
        help="Print a broad Cycling catalogue scan, without scheduling Slack or writing reminder state.",
    )
    parser.add_argument("--pause-on-exit", action="store_true", help="Wait for Enter before closing the console.")
    parser.add_argument("--lookahead-hours", type=float, default=24)
    parser.add_argument("--start-now", action="store_true", help="Use current UK time as scan start.")
    parser.add_argument("--force", action="store_true", help="Ignore duplicate record checks.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    return parser.parse_args()


def pause_before_exit() -> None:
    print("Betfair Event Reminders finished. Press Enter to close...", flush=True)
    try:
        input()
    except EOFError:
        pass


def main() -> int:
    args = parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    try:
        source = resolve_config_source(args.config)
        config = load_config(source)
        return run_scan(args, config, source)
    except (ConfigMissing, ConfigPlaceholderError) as exc:
        print(str(exc), flush=True)
        return 2


if __name__ == "__main__":
    exit_code = 0
    try:
        exit_code = main()
    except KeyboardInterrupt:
        print("Interrupted.", flush=True)
        exit_code = 130
    except Exception as exc:
        print(f"ERROR: {exc}", flush=True)
        traceback.print_exc()
        exit_code = 1
    finally:
        if "--pause-on-exit" in sys.argv:
            pause_before_exit()
    raise SystemExit(exit_code)
