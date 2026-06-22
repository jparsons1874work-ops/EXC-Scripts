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
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable, Iterable, Optional
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
RUN_LOCK_PATH = RUNTIME_OUTPUT_DIR / "betfair_inplay_start_checker.lock"

UK_TZ = ZoneInfo("Europe/London")
UTC_TZ = ZoneInfo("UTC")
PLACEHOLDER_PREFIXES = ("YOUR_", "PASTE_", "CHANGE_ME", "TODO")
EXCLUDED_SPORT_NAMES = {"tennis", "darts", "football", "soccer", "horse racing", "greyhound racing"}
ALERTABLE_STATUSES = {"OPEN", "SUSPENDED"}
DEFAULT_LOOKBACK_HOURS = 6.0
DEFAULT_LOOKAHEAD_HOURS = 24.0
DEFAULT_MARKET_BOOK_BATCH_SIZE = 40
BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS = 300
BETFAIR_TIME_ALERT_DELAY_SECONDS = 60
FLASHSCORE_ALERT_DELAY_SECONDS = 300
DEFAULT_RUN_LOCK_STALE_SECONDS = 300
SLACK_WEBHOOK_ENV_NAME = "Slack_Webhook_TIP"
FLASHSCORE_SPORTS = ("Tennis", "Darts")
FLASHSCORE_URLS = {
    "Tennis": "https://www.flashscore.com/tennis/",
    "Darts": "https://www.flashscore.com/darts/",
}
SURNAME_PARTICLES = {"van", "de", "del", "da", "di", "von", "la", "le", "du"}
COMMON_SURNAMES = {
    "smith",
    "jones",
    "williams",
    "brown",
    "taylor",
    "anderson",
    "thompson",
    "white",
    "martin",
    "lee",
    "wilson",
    "johnson",
    "roberts",
    "wright",
}
FLASHSCORE_REJECT_FINISHED_MARKERS = (
    "finished",
    "ended",
    "after pen",
    "after pen.",
    "aet",
    "walkover",
    "retired",
    "abandoned",
    "cancelled",
    "canceled",
    "postponed",
    "wo",
    "ret",
)
FLASHSCORE_REJECT_SCHEDULED_MARKERS = (
    "scheduled",
    "not started",
    "starts",
    "start time",
)
FLASHSCORE_LIVE_MARKERS = (
    "live",
    "inplay",
    "in-play",
    "in progress",
    "playing",
    "event__match--live",
    "event__stage--live",
)
FLASHSCORE_ACTIVE_STATUS_MARKERS = (
    "set 1",
    "set 2",
    "set 3",
    "set 4",
    "set 5",
    "1st set",
    "2nd set",
    "3rd set",
    "4th set",
    "5th set",
    "leg",
    "break",
)


load_dotenv(PROJECT_ROOT / ".env")

CURRENT_SCAN_RUN_ID: str = ""
CURRENT_SCAN_STARTED_AT: str = ""


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
class FinalMarketBookSnapshot:
    market_id: str
    status_raw: Any
    inplay_raw: Any
    status: str | None
    inplay: bool | None
    raw_book: Any


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
    match_format: str = "singles"
    side_1_player_1: str = ""
    side_1_player_2: str = ""
    side_2_player_1: str = ""
    side_2_player_2: str = ""


@dataclass(frozen=True)
class MatchConfidence:
    level: str
    reason: str
    score: float
    flashscore_participant_1: str = ""
    flashscore_participant_2: str = ""
    betfair_participant_1: str = ""
    betfair_participant_2: str = ""
    flashscore_surname_1: str = ""
    flashscore_surname_2: str = ""
    betfair_surname_1: str = ""
    betfair_surname_2: str = ""
    match_format: str = "singles"
    side_1_player_1: str = ""
    side_1_player_2: str = ""
    side_2_player_1: str = ""
    side_2_player_2: str = ""
    side_1_surnames: str = ""
    side_2_surnames: str = ""
    betfair_side_1_players: str = ""
    betfair_side_2_players: str = ""
    betfair_side_1_surnames: str = ""
    betfair_side_2_surnames: str = ""


@dataclass(frozen=True)
class MatchSides:
    match_format: str
    side_1: tuple[str, ...]
    side_2: tuple[str, ...]


@dataclass(frozen=True)
class PendingAlert:
    candidate: MarketCandidate
    initial_book: MarketBookSnapshot
    trigger_source: str
    flashscore_match: FlashscoreMatch | None = None
    match_confidence: MatchConfidence | None = None
    alert_delay_seconds: int = BETFAIR_TIME_ALERT_DELAY_SECONDS


@dataclass
class RunLock:
    path: Path
    handle: Any
    token: str


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
    pending_alerts: list[PendingAlert] = field(default_factory=list)


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


@dataclass(frozen=True)
class NameParts:
    original: str
    normalized: str
    tokens: tuple[str, ...]
    surname: str
    first_initial: str
    forename: str


def name_parts(name: str) -> NameParts:
    normalized = normalize_match_name(name)
    tokens = tuple(token for token in normalized.split() if token)
    if not tokens:
        return NameParts(name, normalized, (), "", "", "")

    trailing_initial = len(tokens[-1]) == 1
    if trailing_initial and len(tokens) >= 2:
        first_initial = tokens[-1]
        surname_tokens = list(tokens[:-1])
        forename = ""
    else:
        first_initial = tokens[0][0] if tokens[0] else ""
        forename = tokens[0] if len(tokens) >= 2 else ""
        surname_tokens = [tokens[-1]]
        index = len(tokens) - 2
        while index >= 0 and tokens[index] in SURNAME_PARTICLES:
            surname_tokens.insert(0, tokens[index])
            index -= 1
    return NameParts(name, normalized, tokens, " ".join(surname_tokens), first_initial, forename)


def clean_player_display(name: str) -> str:
    value = re.sub(r"\([^)]*\)|\[[^]]*\]", " ", name or "")
    value = re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip(" -")
    return value.strip()


def split_side_players(side: str) -> tuple[str, ...]:
    side = re.sub(r"\([^)]*\)|\[[^]]*\]", " ", side or "")
    side = re.sub(r"\s+(?:and)\s+", " / ", side, flags=re.IGNORECASE)
    side = re.sub(r"\s*(?:/|&|\+)\s*", " / ", side)
    raw_parts = re.split(r"\s*/\s*|\n+", side)
    players = tuple(clean_player_display(part) for part in raw_parts if clean_player_display(part))
    return players[:2]


def _split_match_sides_from_text(text: str) -> tuple[str, str] | None:
    value = re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()
    if not value:
        return None
    parts = [
        part.strip()
        for part in re.split(r"\s+(?:v|vs|vs\.|@|-)\s+", value, maxsplit=1, flags=re.IGNORECASE)
        if part.strip()
    ]
    if len(parts) == 2:
        return parts[0], parts[1]
    return None


def parse_match_sides(
    match_name: str,
    *,
    home: str = "",
    away: str = "",
    child_texts: Iterable[str] = (),
) -> MatchSides | None:
    if home.strip() and away.strip():
        side_1 = split_side_players(home)
        side_2 = split_side_players(away)
        if side_1 and side_2:
            return MatchSides("doubles" if len(side_1) == 2 or len(side_2) == 2 else "singles", side_1, side_2)

    for value in (match_name, *child_texts):
        split = _split_match_sides_from_text(value)
        if not split:
            continue
        side_1 = split_side_players(split[0])
        side_2 = split_side_players(split[1])
        if side_1 and side_2:
            return MatchSides("doubles" if len(side_1) == 2 or len(side_2) == 2 else "singles", side_1, side_2)

    lines: list[str] = []
    for value in (match_name, *child_texts):
        lines.extend(clean_player_display(part) for part in re.split(r"\n+", value or "") if clean_player_display(part))
    deduped: list[str] = []
    for line in lines:
        if line.casefold() in {"v", "vs", "vs.", "live"}:
            continue
        if line not in deduped and not re.search(r"^\d+(?:-\d+)?$", line):
            deduped.append(line)
    if len(deduped) >= 4:
        return MatchSides("doubles", tuple(deduped[:2]), tuple(deduped[2:4]))
    if len(deduped) >= 2:
        return MatchSides("singles", (deduped[0],), (deduped[1],))
    return None


def parse_participants(name: str) -> tuple[str, str] | None:
    sides = parse_match_sides(name)
    if sides:
        return " / ".join(sides.side_1), " / ".join(sides.side_2)
    return None


def surnames_for_players(players: Iterable[str]) -> tuple[str, ...]:
    return tuple(name_parts(player).surname for player in players if name_parts(player).surname)


def flashscore_with_sides(
    sport_name: str,
    home: str,
    away: str,
    full_text: str,
    child_texts: Iterable[str],
) -> tuple[str, tuple[str, str], str, str, str, str, str]:
    sides = parse_match_sides(f"{home} v {away}", home=home, away=away, child_texts=(full_text, *child_texts))
    if sides is None:
        sides = MatchSides("singles", (clean_player_display(home),), (clean_player_display(away),))
    side_1 = " / ".join(sides.side_1)
    side_2 = " / ".join(sides.side_2)
    match_format = "doubles" if sport_name == "Tennis" and (len(sides.side_1) == 2 or len(sides.side_2) == 2) else "singles"
    return (
        match_format,
        (side_1, side_2),
        sides.side_1[0] if len(sides.side_1) >= 1 else "",
        sides.side_1[1] if len(sides.side_1) >= 2 else "",
        sides.side_2[0] if len(sides.side_2) >= 1 else "",
        sides.side_2[1] if len(sides.side_2) >= 2 else "",
        f"{side_1} v {side_2}",
    )


def surname_matches(first: NameParts, second: NameParts) -> bool:
    if not first.surname or not second.surname:
        return False
    return first.surname == second.surname or SequenceMatcher(None, first.surname, second.surname).ratio() >= 0.92


def participant_pair_score(flashscore: NameParts, betfair: NameParts) -> tuple[int, list[str], bool]:
    score = 0
    reasons: list[str] = []
    support = False
    if surname_matches(flashscore, betfair):
        score += 45
        reasons.append(f"surname {flashscore.surname} matched")
    if flashscore.first_initial and betfair.first_initial and flashscore.first_initial == betfair.first_initial:
        score += 10
        support = True
        reasons.append("first initial matched")
    if flashscore.forename and betfair.forename and name_similarity(flashscore.forename, betfair.forename) >= 0.84:
        score += 10
        support = True
        reasons.append("forename similar")
    return score, reasons, support


def single_match_confidence(
    flashscore_sides: MatchSides,
    betfair_sides: MatchSides,
    flashscore_competition: str,
    betfair_competition: str,
) -> MatchConfidence:
    fs1 = name_parts(flashscore_sides.side_1[0])
    fs2 = name_parts(flashscore_sides.side_2[0])
    bf1 = name_parts(betfair_sides.side_1[0])
    bf2 = name_parts(betfair_sides.side_2[0])
    orders = ((fs1, fs2, bf1, bf2), (fs1, fs2, bf2, bf1))
    best_score = -1
    best_reasons: list[str] = []
    best_support = False
    best_bf1 = bf1
    best_bf2 = bf2
    both_surnames = False
    for left_fs, right_fs, left_bf, right_bf in orders:
        left_score, left_reasons, left_support = participant_pair_score(left_fs, left_bf)
        right_score, right_reasons, right_support = participant_pair_score(right_fs, right_bf)
        competition_score = name_similarity(flashscore_competition, betfair_competition)
        total = left_score + right_score
        support = left_support or right_support
        reasons = [*left_reasons, *right_reasons]
        if competition_score >= 0.82:
            total += 10
            support = True
            reasons.append("competition similar")
        surnames_ok = surname_matches(left_fs, left_bf) and surname_matches(right_fs, right_bf)
        if surnames_ok and competition_score >= 0.70:
            total += 5
            reasons.append("competition supports match context")
        if total > best_score:
            best_score = total
            best_reasons = reasons
            best_support = support
            best_bf1 = left_bf
            best_bf2 = right_bf
            both_surnames = surnames_ok

    common_surname_requires_support = any(
        surname in COMMON_SURNAMES for surname in (fs1.surname, fs2.surname, best_bf1.surname, best_bf2.surname)
    )
    if not both_surnames:
        level = "Low"
        best_reasons.append("both participant surnames did not match")
    elif common_surname_requires_support and not best_support:
        level = "Low"
        best_reasons.append("common surname match lacked supporting signal")
    elif best_score >= 90:
        level = "High"
    elif best_score >= 70:
        level = "Medium"
    else:
        level = "Low"

    return MatchConfidence(
        level,
        "; ".join(best_reasons) or "No strong matching evidence",
        float(best_score),
        flashscore_sides.side_1[0],
        flashscore_sides.side_2[0],
        best_bf1.original,
        best_bf2.original,
        fs1.surname,
        fs2.surname,
        best_bf1.surname,
        best_bf2.surname,
        "singles",
        flashscore_sides.side_1[0],
        "",
        flashscore_sides.side_2[0],
        "",
        fs1.surname,
        fs2.surname,
        best_bf1.original,
        best_bf2.original,
        best_bf1.surname,
        best_bf2.surname,
    )


def doubles_side_score(flashscore_players: tuple[str, ...], betfair_players: tuple[str, ...]) -> tuple[int, int, int, list[str]]:
    fs_parts = tuple(name_parts(player) for player in flashscore_players)
    bf_parts = tuple(name_parts(player) for player in betfair_players)
    best = (0, 0, 0, ["doubles side could not be paired"])
    if len(fs_parts) != 2 or len(bf_parts) != 2:
        return best
    for order in ((0, 1), (1, 0)):
        score = 0
        surname_count = 0
        initial_count = 0
        reasons: list[str] = []
        for fs, bf in zip(fs_parts, (bf_parts[order[0]], bf_parts[order[1]])):
            if surname_matches(fs, bf):
                score += 22
                surname_count += 1
                reasons.append(f"surname {fs.surname} matched")
            if fs.first_initial and bf.first_initial and fs.first_initial == bf.first_initial:
                score += 4
                initial_count += 1
                reasons.append(f"initial {fs.first_initial} matched")
        if score > best[0]:
            best = (score, surname_count, initial_count, reasons)
    return best


def doubles_match_confidence(
    flashscore_sides: MatchSides,
    betfair_sides: MatchSides,
    flashscore_competition: str,
    betfair_competition: str,
) -> MatchConfidence:
    if len(flashscore_sides.side_1) != 2 or len(flashscore_sides.side_2) != 2:
        return MatchConfidence("Low", "Flashscore doubles participants incomplete", 0.0, match_format="doubles")
    if len(betfair_sides.side_1) != 2 or len(betfair_sides.side_2) != 2:
        return MatchConfidence("Low", "Betfair doubles participants incomplete", 0.0, match_format="doubles")

    alignments = ((betfair_sides.side_1, betfair_sides.side_2), (betfair_sides.side_2, betfair_sides.side_1))
    best_score = -1
    best_reason: list[str] = []
    best_surname_count = 0
    best_initial_count = 0
    best_bf_side_1 = betfair_sides.side_1
    best_bf_side_2 = betfair_sides.side_2
    for bf_side_1, bf_side_2 in alignments:
        score_1, surnames_1, initials_1, reasons_1 = doubles_side_score(flashscore_sides.side_1, bf_side_1)
        score_2, surnames_2, initials_2, reasons_2 = doubles_side_score(flashscore_sides.side_2, bf_side_2)
        total = score_1 + score_2
        reasons = [*reasons_1, *reasons_2]
        competition_score = name_similarity(flashscore_competition, betfair_competition)
        if competition_score >= 0.82:
            total += 10
            reasons.append("competition similar")
        if total > best_score:
            best_score = total
            best_reason = reasons
            best_surname_count = surnames_1 + surnames_2
            best_initial_count = initials_1 + initials_2
            best_bf_side_1 = bf_side_1
            best_bf_side_2 = bf_side_2

    if best_surname_count == 4 and best_score >= 90:
        level = "High"
    elif best_surname_count >= 3 and best_score >= 75:
        level = "Medium"
        best_reason.append("three of four doubles surnames matched; no alert without full surname match")
    else:
        level = "Low"
        if best_surname_count <= 2:
            best_reason.append("fewer than three doubles surnames matched")

    fs_surnames_1 = surnames_for_players(flashscore_sides.side_1)
    fs_surnames_2 = surnames_for_players(flashscore_sides.side_2)
    bf_surnames_1 = surnames_for_players(best_bf_side_1)
    bf_surnames_2 = surnames_for_players(best_bf_side_2)
    return MatchConfidence(
        level,
        "; ".join(best_reason) or "No strong doubles matching evidence",
        float(best_score),
        " / ".join(flashscore_sides.side_1),
        " / ".join(flashscore_sides.side_2),
        " / ".join(best_bf_side_1),
        " / ".join(best_bf_side_2),
        " / ".join(fs_surnames_1),
        " / ".join(fs_surnames_2),
        " / ".join(bf_surnames_1),
        " / ".join(bf_surnames_2),
        "doubles",
        flashscore_sides.side_1[0],
        flashscore_sides.side_1[1],
        flashscore_sides.side_2[0],
        flashscore_sides.side_2[1],
        ", ".join(fs_surnames_1),
        ", ".join(fs_surnames_2),
        " / ".join(best_bf_side_1),
        " / ".join(best_bf_side_2),
        ", ".join(bf_surnames_1),
        ", ".join(bf_surnames_2),
    )


def participant_confidence(
    flashscore_participants: tuple[str, str],
    betfair_participants: tuple[str, str] | None,
    flashscore_competition: str,
    betfair_competition: str,
) -> MatchConfidence:
    if not betfair_participants:
        return MatchConfidence("Low", "Betfair participants could not be parsed", 0.0)
    flashscore_sides = parse_match_sides(" v ".join(flashscore_participants))
    betfair_sides = parse_match_sides(" v ".join(betfair_participants))
    if flashscore_sides is None or betfair_sides is None:
        return MatchConfidence("Low", "Participants could not be parsed", 0.0)
    if flashscore_sides.match_format != betfair_sides.match_format:
        return MatchConfidence(
            "Low",
            f"{flashscore_sides.match_format} Flashscore match cannot match {betfair_sides.match_format} Betfair event",
            0.0,
            flashscore_participants[0],
            flashscore_participants[1],
            betfair_participants[0],
            betfair_participants[1],
            match_format=flashscore_sides.match_format,
        )
    if flashscore_sides.match_format == "doubles":
        return doubles_match_confidence(flashscore_sides, betfair_sides, flashscore_competition, betfair_competition)
    return single_match_confidence(flashscore_sides, betfair_sides, flashscore_competition, betfair_competition)


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


def betfair_time_overdue_threshold_seconds(args: argparse.Namespace) -> int:
    explicit_seconds = getattr(args, "betfair_time_overdue_threshold_seconds", None)
    if explicit_seconds is not None:
        return max(0, int(explicit_seconds))
    legacy_minutes = getattr(args, "overdue_minutes", None)
    if legacy_minutes is not None:
        return max(0, int(float(legacy_minutes) * 60))
    return BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS


def is_betfair_time_overdue(candidate: MarketCandidate, now: datetime, overdue_threshold_seconds: int) -> bool:
    return candidate.scheduled_start_utc is not None and candidate.scheduled_start_utc < now - timedelta(seconds=overdue_threshold_seconds)


def log(message: str) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def object_get(obj: Any, name: str, default: Any = "") -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


_MISSING = object()


def object_get_any(obj: Any, names: Iterable[str], default: Any = _MISSING) -> Any:
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj.get(name)
        if not isinstance(obj, dict) and hasattr(obj, name):
            value = getattr(obj, name)
            if not callable(value):
                return value
    return default


def _market_book_shapes(market_book: Any) -> Iterable[Any]:
    seen: set[int] = set()
    stack = [market_book]
    while stack:
        current = stack.pop()
        if current is None:
            continue
        current_id = id(current)
        if current_id in seen:
            continue
        seen.add(current_id)
        yield current
        for raw_name in ("raw", "raw_data", "_data", "_json", "data", "market_book", "marketBook"):
            nested = object_get_any(current, (raw_name,), _MISSING)
            if nested is not _MISSING and not callable(nested):
                stack.append(nested)
        if not isinstance(current, dict) and hasattr(current, "__dict__"):
            stack.append(vars(current))


def _parse_market_book_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"true", "yes", "y", "1"}:
            return True
        if normalized in {"false", "no", "n", "0"}:
            return False
    return None


def extract_market_book_inplay(market_book) -> Optional[bool]:
    for shape in _market_book_shapes(market_book):
        value = object_get_any(shape, ("inplay", "inPlay", "in_play", "isInplay", "isInPlay"), _MISSING)
        if value is _MISSING:
            continue
        parsed = _parse_market_book_bool(value)
        if parsed is not None:
            return parsed
    return None


def extract_market_book_status(market_book) -> Optional[str]:
    for shape in _market_book_shapes(market_book):
        value = object_get_any(shape, ("status", "marketStatus"), _MISSING)
        if value is _MISSING or value is None:
            continue
        status = str(value).strip().upper()
        if status:
            return status
    return None


def raw_market_book_value(market_book: Any, names: Iterable[str]) -> Any:
    for shape in _market_book_shapes(market_book):
        value = object_get_any(shape, names, _MISSING)
        if value is not _MISSING:
            return value
    return None


def raw_value_for_log(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except TypeError:
        return str(value)


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
            slack_error TEXT,
            flashscore_participant_1 TEXT,
            flashscore_participant_2 TEXT,
            betfair_participant_1 TEXT,
            betfair_participant_2 TEXT,
            flashscore_surname_1 TEXT,
            flashscore_surname_2 TEXT,
            betfair_surname_1 TEXT,
            betfair_surname_2 TEXT,
            match_score REAL,
            match_format TEXT,
            side_1_player_1 TEXT,
            side_1_player_2 TEXT,
            side_2_player_1 TEXT,
            side_2_player_2 TEXT,
            side_1_surnames TEXT,
            side_2_surnames TEXT,
            betfair_side_1_players TEXT,
            betfair_side_2_players TEXT,
            betfair_side_1_surnames TEXT,
            betfair_side_2_surnames TEXT,
            pending_verification_at TEXT,
            verify_after TEXT,
            alert_delay_seconds INTEGER,
            candidate_first_seen_at TEXT,
            final_marketbook_status_raw TEXT,
            final_marketbook_inplay_raw TEXT,
            final_marketbook_inplay_parsed INTEGER,
            final_marketbook_status_parsed TEXT,
            visible_in_hub INTEGER NOT NULL DEFAULT 1,
            run_id TEXT,
            last_seen_run_id TEXT,
            last_seen_in_scan_at TEXT,
            hidden_reason TEXT,
            hidden_at TEXT
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
    ensure_column(connection, "inplay_alert_state", "flashscore_participant_1", "TEXT")
    ensure_column(connection, "inplay_alert_state", "flashscore_participant_2", "TEXT")
    ensure_column(connection, "inplay_alert_state", "betfair_participant_1", "TEXT")
    ensure_column(connection, "inplay_alert_state", "betfair_participant_2", "TEXT")
    ensure_column(connection, "inplay_alert_state", "flashscore_surname_1", "TEXT")
    ensure_column(connection, "inplay_alert_state", "flashscore_surname_2", "TEXT")
    ensure_column(connection, "inplay_alert_state", "betfair_surname_1", "TEXT")
    ensure_column(connection, "inplay_alert_state", "betfair_surname_2", "TEXT")
    ensure_column(connection, "inplay_alert_state", "match_score", "REAL")
    ensure_column(connection, "inplay_alert_state", "match_format", "TEXT")
    ensure_column(connection, "inplay_alert_state", "side_1_player_1", "TEXT")
    ensure_column(connection, "inplay_alert_state", "side_1_player_2", "TEXT")
    ensure_column(connection, "inplay_alert_state", "side_2_player_1", "TEXT")
    ensure_column(connection, "inplay_alert_state", "side_2_player_2", "TEXT")
    ensure_column(connection, "inplay_alert_state", "side_1_surnames", "TEXT")
    ensure_column(connection, "inplay_alert_state", "side_2_surnames", "TEXT")
    ensure_column(connection, "inplay_alert_state", "betfair_side_1_players", "TEXT")
    ensure_column(connection, "inplay_alert_state", "betfair_side_2_players", "TEXT")
    ensure_column(connection, "inplay_alert_state", "betfair_side_1_surnames", "TEXT")
    ensure_column(connection, "inplay_alert_state", "betfair_side_2_surnames", "TEXT")
    ensure_column(connection, "inplay_alert_state", "pending_verification_at", "TEXT")
    ensure_column(connection, "inplay_alert_state", "verify_after", "TEXT")
    ensure_column(connection, "inplay_alert_state", "alert_delay_seconds", "INTEGER")
    ensure_column(connection, "inplay_alert_state", "candidate_first_seen_at", "TEXT")
    ensure_column(connection, "inplay_alert_state", "final_marketbook_status_raw", "TEXT")
    ensure_column(connection, "inplay_alert_state", "final_marketbook_inplay_raw", "TEXT")
    ensure_column(connection, "inplay_alert_state", "final_marketbook_inplay_parsed", "INTEGER")
    ensure_column(connection, "inplay_alert_state", "final_marketbook_status_parsed", "TEXT")
    ensure_column(connection, "inplay_alert_state", "visible_in_hub", "INTEGER NOT NULL DEFAULT 1")
    ensure_column(connection, "inplay_alert_state", "run_id", "TEXT")
    ensure_column(connection, "inplay_alert_state", "last_seen_run_id", "TEXT")
    ensure_column(connection, "inplay_alert_state", "last_seen_in_scan_at", "TEXT")
    ensure_column(connection, "inplay_alert_state", "hidden_reason", "TEXT")
    ensure_column(connection, "inplay_alert_state", "hidden_at", "TEXT")
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
            details_json TEXT,
            run_id TEXT
        )
        """
    )
    ensure_column(connection, "inplay_scan_logs", "run_id", "TEXT")
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
            flashscore_live_matches_found INTEGER NOT NULL DEFAULT 0,
            run_id TEXT,
            current_run_started_at TEXT
        )
        """
    )
    ensure_column(connection, "inplay_scan_runs", "betfair_time_scan_status", "TEXT NOT NULL DEFAULT 'not_run'")
    ensure_column(connection, "inplay_scan_runs", "flashscore_scan_status", "TEXT NOT NULL DEFAULT 'not_run'")
    ensure_column(connection, "inplay_scan_runs", "flashscore_live_matches_found", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(connection, "inplay_scan_runs", "run_id", "TEXT")
    ensure_column(connection, "inplay_scan_runs", "current_run_started_at", "TEXT")
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
            (timestamp, level, event_type, message, sport_name, event_id, market_id, details_json, run_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            CURRENT_SCAN_RUN_ID,
        ),
    )
    connection.commit()
    prefix = f"{level}: " if level not in {"INFO", "DEBUG"} else ""
    log(f"{prefix}{message}")


def lock_file_metadata(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8") or "{}")
    except (OSError, json.JSONDecodeError):
        return {}


def lock_file_age_seconds(path: Path, now: datetime | None = None) -> float | None:
    try:
        modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC_TZ)
    except OSError:
        return None
    return max(0.0, ((now or utc_now()) - modified_at).total_seconds())


def try_lock_handle(handle: Any) -> bool:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False


def unlock_handle(handle: Any) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def acquire_run_lock(connection: sqlite3.Connection, run_id: int, stale_seconds: int = DEFAULT_RUN_LOCK_STALE_SECONDS) -> RunLock | None:
    RUN_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    handle = RUN_LOCK_PATH.open("a+", encoding="utf-8")
    handle.seek(0, os.SEEK_END)
    if handle.tell() == 0:
        handle.write(" ")
        handle.flush()
    if not try_lock_handle(handle):
        age_seconds = lock_file_age_seconds(RUN_LOCK_PATH)
        metadata = lock_file_metadata(RUN_LOCK_PATH)
        db_log(
            connection,
            "INFO",
            "run_skipped_existing_run_active",
            "Skipped run: existing checker run is active",
            details={
                "lock_path": str(RUN_LOCK_PATH),
                "lock_age_seconds": age_seconds,
                "stale_after_seconds": stale_seconds,
                "owner_pid": metadata.get("owner_pid", ""),
                "owner_run_id": metadata.get("run_id", ""),
                "owner_acquired_at": metadata.get("acquired_at", ""),
            },
        )
        handle.close()
        return None

    previous_age_seconds = lock_file_age_seconds(RUN_LOCK_PATH)
    previous_metadata = lock_file_metadata(RUN_LOCK_PATH)
    if (
        previous_age_seconds is not None
        and previous_age_seconds > stale_seconds
        and previous_metadata.get("owner_pid")
        and not previous_metadata.get("released_at")
    ):
        db_log(
            connection,
            "INFO",
            "stale_run_lock_recovered",
            "Recovered stale checker lock metadata",
            details={
                "lock_path": str(RUN_LOCK_PATH),
                "lock_age_seconds": previous_age_seconds,
                "stale_after_seconds": stale_seconds,
                "owner_pid": previous_metadata.get("owner_pid", ""),
                "owner_run_id": previous_metadata.get("run_id", ""),
                "owner_acquired_at": previous_metadata.get("acquired_at", ""),
            },
        )

    token = uuid.uuid4().hex
    payload = {
        "owner_pid": os.getpid(),
        "run_id": run_id,
        "token": token,
        "acquired_at": iso_utc(utc_now()),
        "stale_after_seconds": stale_seconds,
    }
    handle.seek(0)
    handle.truncate()
    handle.write(json.dumps(payload, sort_keys=True))
    handle.flush()
    os.fsync(handle.fileno())
    return RunLock(RUN_LOCK_PATH, handle, token)


def release_run_lock(lock: RunLock | None) -> None:
    if lock is None:
        return
    try:
        lock.handle.seek(0)
        lock.handle.truncate()
        lock.handle.write(json.dumps({"released_at": iso_utc(utc_now()), "token": lock.token}, sort_keys=True))
        lock.handle.flush()
        os.fsync(lock.handle.fileno())
    except OSError:
        pass
    try:
        unlock_handle(lock.handle)
    except OSError:
        pass
    lock.handle.close()


def start_scan_run(connection: sqlite3.Connection, args: argparse.Namespace) -> int:
    global CURRENT_SCAN_RUN_ID, CURRENT_SCAN_STARTED_AT
    run_uuid = str(uuid.uuid4())
    started_at = iso_utc(utc_now())
    cursor = connection.execute(
        """
        INSERT INTO inplay_scan_runs (scan_started_at, status, dry_run, run_id, current_run_started_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (started_at, "running", int(bool(args.dry_run)), run_uuid, started_at),
    )
    connection.commit()
    CURRENT_SCAN_RUN_ID = run_uuid
    CURRENT_SCAN_STARTED_AT = started_at
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


def list_match_odds_catalogues_for_event(client: APIClient, event_id: str, max_results: int = 100) -> list[Any]:
    event_filter = market_filter(
        event_ids=[event_id],
        market_type_codes=["MATCH_ODDS"],
    )
    return client.betting.list_market_catalogue(
        filter=event_filter,
        market_projection=["EVENT", "EVENT_TYPE", "COMPETITION", "MARKET_START_TIME", "MARKET_DESCRIPTION"],
        sort="FIRST_TO_START",
        max_results=max_results,
    )


def select_flashscore_live_filter(driver: Any, sport_name: str, connection: sqlite3.Connection) -> None:
    selected = False
    try:
        selected = bool(
            driver.execute_script(
                """
                const candidates = Array.from(document.querySelectorAll(
                  "button, a, [role='tab'], [class*='filter'], [class*='tab']"
                ));
                const live = candidates.find(el => {
                  const text = (el.innerText || el.textContent || "").trim().toLowerCase();
                  const title = (el.getAttribute("title") || "").trim().toLowerCase();
                  const aria = (el.getAttribute("aria-label") || "").trim().toLowerCase();
                  return text === "live" || text === "live now" || title === "live" || aria === "live";
                });
                if (!live) return false;
                live.click();
                return true;
                """
            )
        )
    except Exception:
        selected = False
    db_log(
        connection,
        "INFO",
        "flashscore_live_filter_selected",
        "Flashscore LIVE filter selected" if selected else "Flashscore LIVE filter not found",
        sport_name=sport_name,
        details={"selected": selected},
    )
    if selected:
        time.sleep(1.5)


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
                select_flashscore_live_filter(driver, sport_name, connection)
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
                      const participantTexts = Array.from(row.querySelectorAll(
                        ".event__participant, .event__participant--home, .event__participant--away, [class*='participant'], [class*='team'], [class*='name']"
                      )).map(el => el.innerText.trim()).filter(Boolean);
                      const homeScore = text(row, ".event__score--home");
                      const awayScore = text(row, ".event__score--away");
                      const status = text(row, ".event__stage--block") || text(row, ".event__stage") || text(row, ".event__time");
                      const link = row.querySelector("a[href*='/match/']");
                      const liveBadge = row.querySelector(
                        "[class*='live'], [class*='inplay'], [class*='stage--live'], [title*='Live'], [aria-label*='Live']"
                      );
                      const parentText = row.parentElement ? row.parentElement.innerText.slice(0, 500) : "";
                      return {
                        id: row.id || row.getAttribute("data-event-id") || "",
                        home,
                        away,
                        status,
                        score: [homeScore, awayScore].filter(Boolean).join("-"),
                        url: link ? link.href : "",
                        competition: previousCompetition(row),
                        className: String(row.className || ""),
                        liveBadgeText: liveBadge ? (liveBadge.innerText || liveBadge.textContent || liveBadge.getAttribute("title") || liveBadge.getAttribute("aria-label") || "") : "",
                        parentText,
                        fullText: row.innerText || "",
                        participantTexts
                      };
                    }).filter(row => row.fullText || (row.home && row.away));
                    """
                )
            except Exception as exc:
                db_log(connection, "ERROR", "flashscore_scan_completed", f"Flashscore {sport_name} fetch failed: {exc}", sport_name=sport_name)
                continue

            for row in rows or []:
                status = str(row.get("status") or "").strip()
                score = str(row.get("score") or "").strip()
                class_name = str(row.get("className") or "")
                full_text = str(row.get("fullText") or "").strip()
                live_badge_text = str(row.get("liveBadgeText") or "").strip()
                parent_text = str(row.get("parentText") or "").strip()
                live_decision = flashscore_row_live_decision(
                    status,
                    score,
                    class_name,
                    full_text=full_text,
                    sport_name=sport_name,
                    live_badge_text=live_badge_text,
                    parent_text=parent_text,
                )
                if not live_decision.is_live:
                    db_log(
                        connection,
                        "DEBUG",
                        live_decision.event_type,
                        "Flashscore row rejected: not live",
                        sport_name=sport_name,
                        details={
                            "row_text": full_text[:500],
                            "status": status,
                            "score": score,
                            "class_name": class_name,
                            "reason": live_decision.reason,
                        },
                    )
                    continue
                home = str(row.get("home") or "").strip()
                away = str(row.get("away") or "").strip()
                child_texts = tuple(str(text).strip() for text in (row.get("participantTexts") or []) if str(text).strip())
                match_format, participants, side_1_player_1, side_1_player_2, side_2_player_1, side_2_player_2, match_name = flashscore_with_sides(
                    sport_name,
                    home,
                    away,
                    full_text,
                    child_texts,
                )
                if sport_name == "Tennis" and match_format == "doubles":
                    db_log(
                        connection,
                        "INFO",
                        "flashscore_tennis_doubles_found",
                        "Flashscore tennis doubles match parsed",
                        sport_name=sport_name,
                        event_name=match_name,
                        details={"home": home, "away": away, "full_text": full_text[:500]},
                    )
                if sport_name == "Tennis" and match_format == "doubles" and (
                    not side_1_player_1 or not side_1_player_2 or not side_2_player_1 or not side_2_player_2
                ):
                    db_log(
                        connection,
                        "ERROR",
                        "flashscore_tennis_doubles_parse_failed",
                        "Flashscore tennis doubles parse failed",
                        sport_name=sport_name,
                        details={"home": home, "away": away, "full_text": full_text[:500]},
                    )
                    continue
                match = FlashscoreMatch(
                    sport_name=sport_name,
                    match_name=match_name,
                    competition_name=str(row.get("competition") or "").strip(),
                    status_text=status or "Live",
                    score=score,
                    match_id=str(row.get("id") or "").strip(),
                    url=str(row.get("url") or "").strip(),
                    detected_live_at=utc_now(),
                    participants=participants,
                    match_format=match_format,
                    side_1_player_1=side_1_player_1,
                    side_1_player_2=side_1_player_2,
                    side_2_player_1=side_2_player_1,
                    side_2_player_2=side_2_player_2,
                )
                matches.append(match)
                db_log(
                    connection,
                    "INFO",
                    "flashscore_row_accepted_live",
                    "Flashscore live match found",
                    sport_name=sport_name,
                    event_name=match.match_name,
                    details={
                        "flashscore_match_name": match.match_name,
                        "flashscore_competition": match.competition_name,
                        "flashscore_status": match.status_text,
                        "flashscore_score": match.score,
                        "flashscore_match_id": match.match_id,
                        "match_format": match.match_format,
                        "live_reason": live_decision.reason,
                    },
                )
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass
    return matches


@dataclass(frozen=True)
class FlashscoreLiveDecision:
    is_live: bool
    reason: str
    event_type: str


def _contains_marker(text: str, markers: Iterable[str]) -> bool:
    return any(marker in text for marker in markers)


def _contains_terminal_marker(text: str) -> bool:
    if _contains_marker(text, tuple(marker for marker in FLASHSCORE_REJECT_FINISHED_MARKERS if marker not in {"wo", "ret", "ft"})):
        return True
    return bool(re.search(r"\b(?:wo|ret|ft)\b", text))


def flashscore_row_live_decision(
    status: str,
    score: str,
    class_name: str,
    *,
    full_text: str = "",
    sport_name: str = "",
    live_badge_text: str = "",
    parent_text: str = "",
) -> FlashscoreLiveDecision:
    combined = " ".join(
        value.casefold()
        for value in (status, class_name, live_badge_text, full_text)
        if value
    )
    parent = parent_text.casefold()
    status_text = (status or "").casefold().strip()
    if _contains_terminal_marker(combined):
        return FlashscoreLiveDecision(False, "finished marker", "flashscore_row_rejected_finished")
    if _contains_marker(combined, FLASHSCORE_REJECT_SCHEDULED_MARKERS) or re.fullmatch(r"\d{1,2}:\d{2}", status_text or ""):
        return FlashscoreLiveDecision(False, "scheduled/start-time row", "flashscore_row_rejected_scheduled")
    if "yesterday" in parent and not _contains_marker(combined, FLASHSCORE_LIVE_MARKERS):
        return FlashscoreLiveDecision(False, "yesterday section without live marker", "flashscore_row_rejected_yesterday_not_live")
    if "tomorrow" in parent and not _contains_marker(combined, FLASHSCORE_LIVE_MARKERS):
        return FlashscoreLiveDecision(False, "tomorrow section without live marker", "flashscore_row_rejected_scheduled")

    has_live_marker = _contains_marker(combined, FLASHSCORE_LIVE_MARKERS)
    has_active_status = _contains_marker(combined, FLASHSCORE_ACTIVE_STATUS_MARKERS) or bool(
        re.search(r"\b(?:1st|2nd|3rd|4th|5th)\s+(?:set|leg)\b", combined)
    )
    if has_live_marker:
        return FlashscoreLiveDecision(True, "live marker", "flashscore_row_accepted_live")
    if has_active_status and not _contains_terminal_marker(combined):
        return FlashscoreLiveDecision(True, "active set/leg status", "flashscore_row_accepted_live")
    if score and re.search(r"\d", score):
        return FlashscoreLiveDecision(False, "score without live marker", "flashscore_row_rejected_not_live")
    return FlashscoreLiveDecision(False, "no live marker", "flashscore_row_rejected_not_live")


def flashscore_row_is_live(status: str, score: str, class_name: str) -> bool:
    return flashscore_row_live_decision(status, score, class_name).is_live


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
        market_id = str(object_get_any(book, ("market_id", "marketId"), "")).strip()
        if not market_id:
            continue
        status = extract_market_book_status(book) or ""
        inplay = extract_market_book_inplay(book)
        snapshots[market_id] = MarketBookSnapshot(market_id=market_id, status=status, inplay=inplay is True)
    return snapshots


def final_snapshot_from_market_book(book: Any) -> FinalMarketBookSnapshot | None:
    market_id = str(object_get_any(book, ("market_id", "marketId"), "")).strip()
    if not market_id:
        return None
    status_raw = raw_market_book_value(book, ("status", "marketStatus"))
    inplay_raw = raw_market_book_value(book, ("inplay", "inPlay", "in_play", "isInplay", "isInPlay"))
    return FinalMarketBookSnapshot(
        market_id=market_id,
        status_raw=status_raw,
        inplay_raw=inplay_raw,
        status=extract_market_book_status(book),
        inplay=extract_market_book_inplay(book),
        raw_book=book,
    )


def list_final_market_books(client: APIClient, market_ids: list[str]) -> dict[str, FinalMarketBookSnapshot]:
    results = client.betting.list_market_book(market_ids=market_ids)
    snapshots: dict[str, FinalMarketBookSnapshot] = {}
    for book in results:
        snapshot = final_snapshot_from_market_book(book)
        if snapshot is not None:
            snapshots[snapshot.market_id] = snapshot
    return snapshots


def final_snapshot_to_market_book(snapshot: FinalMarketBookSnapshot, fallback_market_id: str = "") -> MarketBookSnapshot:
    return MarketBookSnapshot(
        market_id=snapshot.market_id or fallback_market_id,
        status=snapshot.status or "",
        inplay=snapshot.inplay is True,
    )


def alerted_event_ids(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute("SELECT event_id FROM inplay_alert_state WHERE alert_sent_at IS NOT NULL").fetchall()
    return {str(row["event_id"]) for row in rows}


def alert_decision(
    candidate: MarketCandidate,
    book: MarketBookSnapshot,
    now: datetime,
    already_alerted: set[str],
    overdue_threshold_seconds: int,
) -> AlertDecision:
    if not candidate.event_id:
        return AlertDecision(False, "missing event ID")
    if candidate.scheduled_start_utc is None:
        return AlertDecision(False, "missing scheduled start")
    if not is_betfair_time_overdue(candidate, now, overdue_threshold_seconds):
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
                match_reason = ?,
                flashscore_participant_1 = ?,
                flashscore_participant_2 = ?,
                betfair_participant_1 = ?,
                betfair_participant_2 = ?,
                flashscore_surname_1 = ?,
                flashscore_surname_2 = ?,
                betfair_surname_1 = ?,
                betfair_surname_2 = ?,
                match_score = ?,
                match_format = ?,
                side_1_player_1 = ?,
                side_1_player_2 = ?,
                side_2_player_1 = ?,
                side_2_player_2 = ?,
                side_1_surnames = ?,
                side_2_surnames = ?,
                betfair_side_1_players = ?,
                betfair_side_2_players = ?,
                betfair_side_1_surnames = ?,
                betfair_side_2_surnames = ?
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
                match_confidence.flashscore_participant_1 if match_confidence else "",
                match_confidence.flashscore_participant_2 if match_confidence else "",
                match_confidence.betfair_participant_1 if match_confidence else "",
                match_confidence.betfair_participant_2 if match_confidence else "",
                match_confidence.flashscore_surname_1 if match_confidence else "",
                match_confidence.flashscore_surname_2 if match_confidence else "",
                match_confidence.betfair_surname_1 if match_confidence else "",
                match_confidence.betfair_surname_2 if match_confidence else "",
                match_confidence.score if match_confidence else None,
                match_confidence.match_format if match_confidence else flashscore_match.match_format,
                match_confidence.side_1_player_1 if match_confidence else flashscore_match.side_1_player_1,
                match_confidence.side_1_player_2 if match_confidence else flashscore_match.side_1_player_2,
                match_confidence.side_2_player_1 if match_confidence else flashscore_match.side_2_player_1,
                match_confidence.side_2_player_2 if match_confidence else flashscore_match.side_2_player_2,
                match_confidence.side_1_surnames if match_confidence else ", ".join(surnames_for_players((flashscore_match.side_1_player_1, flashscore_match.side_1_player_2))),
                match_confidence.side_2_surnames if match_confidence else ", ".join(surnames_for_players((flashscore_match.side_2_player_1, flashscore_match.side_2_player_2))),
                match_confidence.betfair_side_1_players if match_confidence else "",
                match_confidence.betfair_side_2_players if match_confidence else "",
                match_confidence.betfair_side_1_surnames if match_confidence else "",
                match_confidence.betfair_side_2_surnames if match_confidence else "",
                candidate.event_id,
            ),
        )
    if alert_sent_at is not None:
        connection.execute(
            "UPDATE inplay_alert_state SET slack_alert_sent = 1, slack_error = '' WHERE event_id = ?",
            (candidate.event_id,),
        )
    if trigger_source == "flashscore_live":
        visible = True
        if alert_sent_at is None and final_verification_result in {"suppressed_inplay", "suppressed_closed", "suppressed_status", "skipped"}:
            visible = False
        if alert_sent_at is None and book.inplay:
            visible = False
        if final_verification_result in {"pending_verification", "confirmed_not_inplay", "failed", "suppressed_unknown", "suppressed_ambiguous"}:
            visible = True
        connection.execute(
            "UPDATE inplay_alert_state SET visible_in_hub = ? WHERE event_id = ?",
            (1 if visible else 0, candidate.event_id),
        )
    if CURRENT_SCAN_RUN_ID:
        connection.execute(
            """
            UPDATE inplay_alert_state
            SET run_id = COALESCE(run_id, ?),
                last_seen_run_id = ?,
                last_seen_in_scan_at = ?,
                hidden_reason = '',
                hidden_at = NULL
            WHERE event_id = ?
            """,
            (CURRENT_SCAN_RUN_ID, CURRENT_SCAN_RUN_ID, iso_utc(now), candidate.event_id),
        )
    connection.commit()


def record_slack_error(connection: sqlite3.Connection, event_id: str, error: str) -> None:
    connection.execute(
        "UPDATE inplay_alert_state SET slack_alert_sent = 0, slack_error = ? WHERE event_id = ?",
        (error, event_id),
    )
    connection.commit()


def mark_state_seen_in_current_run(
    connection: sqlite3.Connection,
    event_id: str,
    *,
    seen_at: datetime | None = None,
    run_id: str | None = None,
    make_visible: bool = True,
) -> None:
    effective_run_id = run_id if run_id is not None else CURRENT_SCAN_RUN_ID
    if not event_id or not effective_run_id:
        return
    timestamp = iso_utc(seen_at or utc_now())
    connection.execute(
        """
        UPDATE inplay_alert_state
        SET run_id = COALESCE(run_id, ?),
            last_seen_run_id = ?,
            last_seen_in_scan_at = ?,
            hidden_reason = CASE WHEN ? = 1 THEN '' ELSE hidden_reason END,
            hidden_at = CASE WHEN ? = 1 THEN NULL ELSE hidden_at END,
            visible_in_hub = CASE WHEN ? = 1 THEN 1 ELSE visible_in_hub END
        WHERE event_id = ?
        """,
        (effective_run_id, effective_run_id, timestamp, 1 if make_visible else 0, 1 if make_visible else 0, 1 if make_visible else 0, event_id),
    )
    connection.commit()


def db_bool(value: bool | None) -> int | None:
    if value is None:
        return None
    return 1 if value else 0


def update_final_marketbook_audit(
    connection: sqlite3.Connection,
    event_id: str,
    snapshot: FinalMarketBookSnapshot | None,
    *,
    visible_in_hub: bool | None = None,
) -> None:
    connection.execute(
        """
        UPDATE inplay_alert_state
        SET final_marketbook_status_raw = ?,
            final_marketbook_inplay_raw = ?,
            final_marketbook_inplay_parsed = ?,
            final_marketbook_status_parsed = ?,
            last_seen_inplay = ?,
            betfair_last_seen_inplay = ?,
            last_seen_status = ?,
            betfair_last_seen_status = ?,
            visible_in_hub = COALESCE(?, visible_in_hub)
        WHERE event_id = ?
        """,
        (
            raw_value_for_log(snapshot.status_raw) if snapshot else "",
            raw_value_for_log(snapshot.inplay_raw) if snapshot else "",
            db_bool(snapshot.inplay) if snapshot else None,
            snapshot.status if snapshot and snapshot.status else "",
            db_bool(snapshot.inplay) if snapshot else None,
            db_bool(snapshot.inplay) if snapshot else None,
            snapshot.status if snapshot and snapshot.status else "",
            snapshot.status if snapshot and snapshot.status else "",
            db_bool(visible_in_hub),
            event_id,
        ),
    )
    connection.commit()


def set_visible_in_hub(connection: sqlite3.Connection, event_id: str, visible: bool) -> None:
    connection.execute("UPDATE inplay_alert_state SET visible_in_hub = ? WHERE event_id = ?", (1 if visible else 0, event_id))
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
            betfair_last_seen_status = ?,
            visible_in_hub = 1,
            run_id = COALESCE(run_id, ?),
            last_seen_run_id = COALESCE(NULLIF(?, ''), last_seen_run_id),
            last_seen_in_scan_at = COALESCE(NULLIF(?, ''), last_seen_in_scan_at),
            hidden_reason = '',
            hidden_at = NULL
        WHERE event_id = ?
        """,
        (
            trigger_source,
            iso_utc(now),
            initial_book.status,
            CURRENT_SCAN_RUN_ID,
            CURRENT_SCAN_RUN_ID,
            iso_utc(now) if CURRENT_SCAN_RUN_ID else "",
            candidate.event_id,
        ),
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
                match_reason = ?,
                flashscore_participant_1 = ?,
                flashscore_participant_2 = ?,
                betfair_participant_1 = ?,
                betfair_participant_2 = ?,
                flashscore_surname_1 = ?,
                flashscore_surname_2 = ?,
                betfair_surname_1 = ?,
                betfair_surname_2 = ?,
                match_score = ?,
                match_format = ?,
                side_1_player_1 = ?,
                side_1_player_2 = ?,
                side_2_player_1 = ?,
                side_2_player_2 = ?,
                side_1_surnames = ?,
                side_2_surnames = ?,
                betfair_side_1_players = ?,
                betfair_side_2_players = ?,
                betfair_side_1_surnames = ?,
                betfair_side_2_surnames = ?
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
                match_confidence.flashscore_participant_1 if match_confidence else "",
                match_confidence.flashscore_participant_2 if match_confidence else "",
                match_confidence.betfair_participant_1 if match_confidence else "",
                match_confidence.betfair_participant_2 if match_confidence else "",
                match_confidence.flashscore_surname_1 if match_confidence else "",
                match_confidence.flashscore_surname_2 if match_confidence else "",
                match_confidence.betfair_surname_1 if match_confidence else "",
                match_confidence.betfair_surname_2 if match_confidence else "",
                match_confidence.score if match_confidence else None,
                match_confidence.match_format if match_confidence else flashscore_match.match_format,
                match_confidence.side_1_player_1 if match_confidence else flashscore_match.side_1_player_1,
                match_confidence.side_1_player_2 if match_confidence else flashscore_match.side_1_player_2,
                match_confidence.side_2_player_1 if match_confidence else flashscore_match.side_2_player_1,
                match_confidence.side_2_player_2 if match_confidence else flashscore_match.side_2_player_2,
                match_confidence.side_1_surnames if match_confidence else ", ".join(surnames_for_players((flashscore_match.side_1_player_1, flashscore_match.side_1_player_2))),
                match_confidence.side_2_surnames if match_confidence else ", ".join(surnames_for_players((flashscore_match.side_2_player_1, flashscore_match.side_2_player_2))),
                match_confidence.betfair_side_1_players if match_confidence else "",
                match_confidence.betfair_side_2_players if match_confidence else "",
                match_confidence.betfair_side_1_surnames if match_confidence else "",
                match_confidence.betfair_side_2_surnames if match_confidence else "",
                candidate.event_id,
            ),
        )
    connection.commit()


def record_flashscore_match_diagnostic(
    connection: sqlite3.Connection,
    candidate: MarketCandidate,
    flashscore_match: FlashscoreMatch,
    confidence: MatchConfidence,
    *,
    now: datetime,
    result: str,
    reason: str,
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
            final_verification_at, final_verification_result, final_verification_reason,
            trigger_source, flashscore_match_id, flashscore_url, flashscore_match_name,
            flashscore_competition, flashscore_status, flashscore_score,
            flashscore_detected_live_at, match_confidence, match_reason,
            betfair_last_checked_at, betfair_last_seen_inplay, betfair_last_seen_status,
            flashscore_participant_1, flashscore_participant_2, betfair_participant_1,
            betfair_participant_2, flashscore_surname_1, flashscore_surname_2,
            betfair_surname_1, betfair_surname_2, match_score
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            market_id = excluded.market_id,
            sport_name = excluded.sport_name,
            competition_name = excluded.competition_name,
            event_name = excluded.event_name,
            scheduled_start_utc = excluded.scheduled_start_utc,
            scheduled_start_uk = excluded.scheduled_start_uk,
            first_flagged_at = COALESCE(inplay_alert_state.first_flagged_at, excluded.first_flagged_at),
            alert_sent_at = COALESCE(inplay_alert_state.alert_sent_at, excluded.alert_sent_at),
            last_seen_status = NULL,
            last_seen_inplay = NULL,
            last_checked_at = excluded.last_checked_at,
            final_verification_result = excluded.final_verification_result,
            final_verification_reason = excluded.final_verification_reason,
            trigger_source = excluded.trigger_source,
            flashscore_match_id = excluded.flashscore_match_id,
            flashscore_url = excluded.flashscore_url,
            flashscore_match_name = excluded.flashscore_match_name,
            flashscore_competition = excluded.flashscore_competition,
            flashscore_status = excluded.flashscore_status,
            flashscore_score = excluded.flashscore_score,
            flashscore_detected_live_at = excluded.flashscore_detected_live_at,
            match_confidence = excluded.match_confidence,
            match_reason = excluded.match_reason,
            betfair_last_checked_at = NULL,
            betfair_last_seen_inplay = NULL,
            betfair_last_seen_status = NULL,
            flashscore_participant_1 = excluded.flashscore_participant_1,
            flashscore_participant_2 = excluded.flashscore_participant_2,
            betfair_participant_1 = excluded.betfair_participant_1,
            betfair_participant_2 = excluded.betfair_participant_2,
            flashscore_surname_1 = excluded.flashscore_surname_1,
            flashscore_surname_2 = excluded.flashscore_surname_2,
            betfair_surname_1 = excluded.betfair_surname_1,
            betfair_surname_2 = excluded.betfair_surname_2,
            match_score = excluded.match_score
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
            existing["recovered_at"] if existing else None,
            iso_utc(now),
            result,
            reason,
            "flashscore_live",
            flashscore_match.match_id,
            flashscore_match.url,
            flashscore_match.match_name,
            flashscore_match.competition_name,
            flashscore_match.status_text,
            flashscore_match.score,
            iso_utc(flashscore_match.detected_live_at),
            confidence.level,
            confidence.reason,
            confidence.flashscore_participant_1,
            confidence.flashscore_participant_2,
            confidence.betfair_participant_1,
            confidence.betfair_participant_2,
            confidence.flashscore_surname_1,
            confidence.flashscore_surname_2,
            confidence.betfair_surname_1,
            confidence.betfair_surname_2,
            confidence.score,
        ),
    )
    connection.execute(
        """
        UPDATE inplay_alert_state
        SET match_format = ?,
            side_1_player_1 = ?,
            side_1_player_2 = ?,
            side_2_player_1 = ?,
            side_2_player_2 = ?,
            side_1_surnames = ?,
            side_2_surnames = ?,
            betfair_side_1_players = ?,
            betfair_side_2_players = ?,
            betfair_side_1_surnames = ?,
            betfair_side_2_surnames = ?,
            visible_in_hub = ?,
            run_id = COALESCE(run_id, ?),
            last_seen_run_id = COALESCE(NULLIF(?, ''), last_seen_run_id),
            last_seen_in_scan_at = COALESCE(NULLIF(?, ''), last_seen_in_scan_at),
            hidden_reason = CASE WHEN ? != '' THEN '' ELSE hidden_reason END,
            hidden_at = CASE WHEN ? != '' THEN NULL ELSE hidden_at END
        WHERE event_id = ?
        """,
        (
            confidence.match_format,
            confidence.side_1_player_1,
            confidence.side_1_player_2,
            confidence.side_2_player_1,
            confidence.side_2_player_2,
            confidence.side_1_surnames,
            confidence.side_2_surnames,
            confidence.betfair_side_1_players,
            confidence.betfair_side_2_players,
            confidence.betfair_side_1_surnames,
            confidence.betfair_side_2_surnames,
            1 if "ambiguous" in reason.casefold() else 0,
            CURRENT_SCAN_RUN_ID,
            CURRENT_SCAN_RUN_ID,
            iso_utc(now) if CURRENT_SCAN_RUN_ID else "",
            CURRENT_SCAN_RUN_ID,
            CURRENT_SCAN_RUN_ID,
            candidate.event_id,
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


def mark_candidate_pending(
    connection: sqlite3.Connection,
    pending: PendingAlert,
    *,
    now: datetime,
    alert_delay_seconds: int,
) -> None:
    existing = connection.execute(
        """
        SELECT candidate_first_seen_at, verify_after, final_verification_result, final_verification_reason
        FROM inplay_alert_state
        WHERE event_id = ?
        """,
        (pending.candidate.event_id,),
    ).fetchone()
    first_seen_at = parse_datetime(existing["candidate_first_seen_at"]) if existing else None
    if first_seen_at is None:
        first_seen_at = now
    verify_after = first_seen_at + timedelta(seconds=max(alert_delay_seconds, 0))
    existing_verify_after = parse_datetime(existing["verify_after"]) if existing else None
    if pending.trigger_source == "flashscore_live" and existing_verify_after and existing_verify_after > now:
        verify_after = existing_verify_after
        db_log(
            connection,
            "INFO",
            "flashscore_candidate_seen_before_verify_after",
            "Flashscore candidate seen again before delayed verification is due",
            sport_name=pending.candidate.sport_name,
            event_id=pending.candidate.event_id,
            market_id=pending.candidate.market_id,
            event_name=pending.candidate.event_name,
            details={
                "verify_after": iso_utc(verify_after),
                "candidate_first_seen_at": iso_utc(first_seen_at),
                "alert_delay_seconds": alert_delay_seconds,
            },
        )
    upsert_alert_state(
        connection,
        pending.candidate,
        pending.initial_book,
        now=now,
        trigger_source=pending.trigger_source,
        flashscore_match=pending.flashscore_match,
        match_confidence=pending.match_confidence,
        final_verification_result="pending_verification",
        final_verification_reason="pending_verification",
    )
    connection.execute(
        """
        UPDATE inplay_alert_state
        SET pending_verification_at = COALESCE(pending_verification_at, ?),
            verify_after = ?,
            alert_delay_seconds = ?,
            candidate_first_seen_at = COALESCE(candidate_first_seen_at, ?)
        WHERE event_id = ?
        """,
        (
            iso_utc(now),
            iso_utc(verify_after),
            alert_delay_seconds,
            iso_utc(now),
            pending.candidate.event_id,
        ),
    )
    connection.commit()
    event_type = (
        "flashscore_candidate_created_pending_5min"
        if pending.trigger_source == "flashscore_live"
        else "candidate_pending_verification"
    )
    message = (
        "Flashscore candidate stored pending 5-minute verification"
        if pending.trigger_source == "flashscore_live"
        else "Candidate stored pending delayed verification"
    )
    db_log(
        connection,
        "INFO",
        event_type,
        message,
        sport_name=pending.candidate.sport_name,
        event_id=pending.candidate.event_id,
        market_id=pending.candidate.market_id,
        event_name=pending.candidate.event_name,
        details={
            "trigger_source": pending.trigger_source,
            "verify_after": iso_utc(verify_after),
            "alert_delay_seconds": alert_delay_seconds,
            "candidate_first_seen_at": iso_utc(first_seen_at),
        },
    )
    if pending.trigger_source == "flashscore_live":
        db_log(
            connection,
            "INFO",
            "flashscore_candidate_pending_5min_verification",
            message,
            sport_name=pending.candidate.sport_name,
            event_id=pending.candidate.event_id,
            market_id=pending.candidate.market_id,
            event_name=pending.candidate.event_name,
            details={
                "trigger_source": pending.trigger_source,
                "verify_after": iso_utc(verify_after),
                "alert_delay_seconds": alert_delay_seconds,
                "candidate_first_seen_at": iso_utc(first_seen_at),
            },
        )


def send_verified_alert(
    connection: sqlite3.Connection,
    config: Config,
    args: argparse.Namespace,
    stats: ScanStats,
    already_alerted: set[str],
    pending: PendingAlert,
    final_book: MarketBookSnapshot,
) -> None:
    candidate = pending.candidate
    alert_now = utc_now()
    stats.flags_found += 1
    if pending.trigger_source == "flashscore_live" and pending.flashscore_match and pending.match_confidence:
        message = build_flashscore_slack_message(pending.flashscore_match, candidate, final_book, pending.match_confidence)
    else:
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
            market_id=candidate.market_id,
            event_name=candidate.event_name,
            details={"trigger_source": pending.trigger_source},
        )
        return

    if is_placeholder(config.slack_webhook_url):
        stats.slack_alert_failures += 1
        db_log(
            connection,
            "ERROR",
            "config_error",
            f"{SLACK_WEBHOOK_ENV_NAME} missing",
            sport_name=candidate.sport_name,
            event_id=candidate.event_id,
            market_id=candidate.market_id,
            event_name=candidate.event_name,
        )
        record_slack_error(connection, candidate.event_id, f"{SLACK_WEBHOOK_ENV_NAME} missing")
        return

    try:
        send_slack_message(config.slack_webhook_url, message)
    except Exception as exc:
        stats.slack_alert_failures += 1
        failed_event_type = "flashscore_slack_alert_failed" if pending.trigger_source == "flashscore_live" else "slack_alert_failed"
        db_log(
            connection,
            "ERROR",
            failed_event_type,
            f"Slack alert failed: {exc}",
            sport_name=candidate.sport_name,
            event_id=candidate.event_id,
            market_id=candidate.market_id,
            event_name=candidate.event_name,
            details={"trigger_source": pending.trigger_source},
        )
        record_slack_error(connection, candidate.event_id, str(exc))
        return

    sent_at = utc_now()
    stats.slack_alerts_sent += 1
    already_alerted.add(candidate.event_id)
    upsert_alert_state(
        connection,
        candidate,
        final_book,
        now=alert_now,
        alert_sent_at=sent_at,
        trigger_source=pending.trigger_source,
        flashscore_match=pending.flashscore_match,
        match_confidence=pending.match_confidence,
        final_verification_result="confirmed_not_inplay",
        final_verification_reason="alert_sent",
    )
    sent_event_type = "flashscore_slack_alert_sent" if pending.trigger_source == "flashscore_live" else "slack_alert_sent"
    db_log(
        connection,
        "INFO",
        sent_event_type,
        f"Sent Slack alert for event {candidate.event_id}",
        sport_name=candidate.sport_name,
        event_id=candidate.event_id,
        market_id=candidate.market_id,
        event_name=candidate.event_name,
        details={"trigger_source": pending.trigger_source},
    )


def flashscore_final_diagnostic_details(
    pending: PendingAlert,
    snapshot: FinalMarketBookSnapshot | None,
    *,
    decision: str,
    api_called_at: datetime,
    same_event_market_ids: list[str] | None = None,
    other_market_inplay_id: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate = pending.candidate
    flashscore_match = pending.flashscore_match
    confidence = pending.match_confidence
    details: dict[str, Any] = {
        "trigger_source": pending.trigger_source,
        "flashscore_match_name": flashscore_match.match_name if flashscore_match else "",
        "flashscore_status": flashscore_match.status_text if flashscore_match else "",
        "flashscore_score": flashscore_match.score if flashscore_match else "",
        "flashscore_participant_1": confidence.flashscore_participant_1 if confidence else "",
        "flashscore_participant_2": confidence.flashscore_participant_2 if confidence else "",
        "betfair_participant_1": confidence.betfair_participant_1 if confidence else "",
        "betfair_participant_2": confidence.betfair_participant_2 if confidence else "",
        "flashscore_surname_1": confidence.flashscore_surname_1 if confidence else "",
        "flashscore_surname_2": confidence.flashscore_surname_2 if confidence else "",
        "betfair_surname_1": confidence.betfair_surname_1 if confidence else "",
        "betfair_surname_2": confidence.betfair_surname_2 if confidence else "",
        "betfair_event_id": candidate.event_id,
        "betfair_event_name": candidate.event_name,
        "betfair_market_id": candidate.market_id,
        "betfair_market_name": "MATCH_ODDS",
        "final_marketbook_status_raw": raw_value_for_log(snapshot.status_raw) if snapshot else "",
        "final_marketbook_inplay_raw": raw_value_for_log(snapshot.inplay_raw) if snapshot else "",
        "final_marketbook_inplay_parsed": snapshot.inplay if snapshot else None,
        "final_marketbook_status_parsed": snapshot.status if snapshot else None,
        "final_api_called_at": iso_utc(api_called_at),
        "match_confidence": confidence.level if confidence else "",
        "match_score": confidence.score if confidence else None,
        "match_reason": confidence.reason if confidence else "",
        "same_event_match_odds_market_ids": same_event_market_ids or [],
        "same_event_other_market_inplay_id": other_market_inplay_id,
        "decision": decision,
    }
    details.update(extra or {})
    return details


def log_flashscore_final_decision(
    connection: sqlite3.Connection,
    pending: PendingAlert,
    snapshot: FinalMarketBookSnapshot | None,
    *,
    event_type: str,
    message: str,
    decision: str,
    api_called_at: datetime,
    level: str = "INFO",
    same_event_market_ids: list[str] | None = None,
    other_market_inplay_id: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    db_log(
        connection,
        level,
        "flashscore_final_betfair_diagnostic",
        "Flashscore final Betfair verification diagnostic",
        sport_name=pending.candidate.sport_name,
        event_id=pending.candidate.event_id,
        market_id=pending.candidate.market_id,
        event_name=pending.candidate.event_name,
        details=flashscore_final_diagnostic_details(
            pending,
            snapshot,
            decision=decision,
            api_called_at=api_called_at,
            same_event_market_ids=same_event_market_ids,
            other_market_inplay_id=other_market_inplay_id,
            extra=extra,
        ),
    )
    db_log(
        connection,
        level,
        event_type,
        message,
        sport_name=pending.candidate.sport_name,
        event_id=pending.candidate.event_id,
        market_id=pending.candidate.market_id,
        event_name=pending.candidate.event_name,
        details=flashscore_final_diagnostic_details(
            pending,
            snapshot,
            decision=decision,
            api_called_at=api_called_at,
            same_event_market_ids=same_event_market_ids,
            other_market_inplay_id=other_market_inplay_id,
            extra=extra,
        ),
    )


def record_flashscore_no_slack(
    connection: sqlite3.Connection,
    pending: PendingAlert,
    snapshot: FinalMarketBookSnapshot | None,
    *,
    now: datetime,
    result: str,
    reason: str,
    event_type: str,
    message: str,
    decision: str,
    api_called_at: datetime,
    same_event_market_ids: list[str] | None = None,
    other_market_inplay_id: str = "",
    level: str = "INFO",
    extra: dict[str, Any] | None = None,
) -> None:
    book = final_snapshot_to_market_book(snapshot, pending.candidate.market_id) if snapshot else pending.initial_book
    upsert_alert_state(
        connection,
        pending.candidate,
        book,
        now=now,
        trigger_source=pending.trigger_source,
        flashscore_match=pending.flashscore_match,
        match_confidence=pending.match_confidence,
        final_verification_at=now,
        final_verification_result=result,
        final_verification_reason=reason,
    )
    update_final_marketbook_audit(connection, pending.candidate.event_id, snapshot, visible_in_hub=result in {"failed", "suppressed_unknown", "suppressed_ambiguous"})
    log_flashscore_final_decision(
        connection,
        pending,
        snapshot,
        event_type=event_type,
        message=message,
        decision=decision,
        api_called_at=api_called_at,
        level=level,
        same_event_market_ids=same_event_market_ids,
        other_market_inplay_id=other_market_inplay_id,
        extra=extra,
    )
    db_log(
        connection,
        level,
        "flashscore_final_decision_no_slack",
        "Flashscore final decision: no Slack",
        sport_name=pending.candidate.sport_name,
        event_id=pending.candidate.event_id,
        market_id=pending.candidate.market_id,
        event_name=pending.candidate.event_name,
        details={"decision": decision, "reason": reason},
    )
    if result == "suppressed_flashscore_not_live":
        connection.execute(
            """
            UPDATE inplay_alert_state
            SET visible_in_hub = 0,
                hidden_reason = 'flashscore_not_live_or_not_seen_latest_live_scan',
                hidden_at = ?
            WHERE event_id = ?
            """,
            (iso_utc(now), pending.candidate.event_id),
        )
        connection.commit()
        db_log(
            connection,
            "INFO",
            "flashscore_visible_row_hidden_not_live",
            "Flashscore row hidden because match is no longer live",
            sport_name=pending.candidate.sport_name,
            event_id=pending.candidate.event_id,
            market_id=pending.candidate.market_id,
            event_name=pending.candidate.event_name,
            details={"reason": reason},
        )


def flashscore_pending_still_live(
    connection: sqlite3.Connection,
    pending: PendingAlert,
    args: argparse.Namespace,
) -> bool:
    verifier = getattr(args, "flashscore_live_verifier", None)
    if callable(verifier):
        return bool(verifier(pending))
    if pending.flashscore_match is None:
        return True
    match_id = normalize_match_name(pending.flashscore_match.match_id)
    match_url = str(pending.flashscore_match.url or "").strip().casefold()
    match_name = normalize_match_name(pending.flashscore_match.match_name)
    if not (match_id or match_url or match_name):
        return True
    try:
        live_matches = flashscore_browser_matches(connection, int(getattr(args, "flashscore_timeout_seconds", 12)))
    except Exception as exc:
        db_log(
            connection,
            "ERROR",
            "flashscore_pending_live_check_failed",
            "Flashscore pending live recheck failed before final Slack decision",
            sport_name=pending.candidate.sport_name,
            event_id=pending.candidate.event_id,
            market_id=pending.candidate.market_id,
            event_name=pending.candidate.event_name,
            details={"error": str(exc)},
        )
        return False
    for live_match in live_matches:
        if live_match.sport_name != pending.flashscore_match.sport_name:
            continue
        if match_id and normalize_match_name(live_match.match_id) == match_id:
            return True
        if match_url and str(live_match.url or "").strip().casefold() == match_url:
            return True
        if match_name and normalize_match_name(live_match.match_name) == match_name:
            return True
    return False


def verify_flashscore_same_event_markets(
    connection: sqlite3.Connection,
    client: APIClient,
    pending: PendingAlert,
    exact_snapshot: FinalMarketBookSnapshot,
    final_books: dict[str, FinalMarketBookSnapshot],
    *,
    api_called_at: datetime,
) -> tuple[bool, str, list[str], str]:
    try:
        catalogues = list_match_odds_catalogues_for_event(client, pending.candidate.event_id)
    except Exception as exc:
        return False, f"same_event_catalogue_api_error: {exc}", [], ""

    same_event_market_ids: list[str] = []
    exact_market_found = False
    for catalogue in catalogues:
        candidate = catalogue_to_candidate(catalogue, EventType(pending.candidate.event_type_id, pending.candidate.sport_name))
        if candidate.event_id and candidate.event_id != pending.candidate.event_id:
            continue
        if candidate.market_id:
            same_event_market_ids.append(candidate.market_id)
            if candidate.market_id == pending.candidate.market_id:
                exact_market_found = True

    if same_event_market_ids and not exact_market_found:
        return False, "exact_market_missing_from_same_event_catalogue", same_event_market_ids, ""
    if not same_event_market_ids:
        same_event_market_ids = [pending.candidate.market_id]

    missing_market_ids = [market_id for market_id in same_event_market_ids if market_id not in final_books]
    if missing_market_ids:
        try:
            final_books.update(list_final_market_books(client, missing_market_ids))
        except Exception as exc:
            return False, f"same_event_marketbook_api_error: {exc}", same_event_market_ids, ""

    for market_id in same_event_market_ids:
        snapshot = final_books.get(market_id)
        if snapshot is None:
            return False, f"same_event_marketbook_missing: {market_id}", same_event_market_ids, ""
        if snapshot.inplay is True:
            return False, "same_event_other_market_inplay", same_event_market_ids, market_id
        if snapshot.inplay is None:
            return False, f"same_event_marketbook_inplay_unknown: {market_id}", same_event_market_ids, ""
    return True, "", same_event_market_ids, ""


def process_flashscore_final_pending(
    connection: sqlite3.Connection,
    client: APIClient,
    config: Config,
    args: argparse.Namespace,
    stats: ScanStats,
    already_alerted: set[str],
    pending: PendingAlert,
    exact_snapshot: FinalMarketBookSnapshot | None,
    final_books: dict[str, FinalMarketBookSnapshot],
    api_called_at: datetime,
) -> None:
    now = utc_now()
    if exact_snapshot is None:
        stats.api_errors += 1
        record_final_verification_failed(
            connection,
            pending.candidate,
            pending.initial_book,
            now=now,
            reason="api_error: exact MarketBook missing",
            trigger_source=pending.trigger_source,
            flashscore_match=pending.flashscore_match,
            match_confidence=pending.match_confidence,
        )
        update_final_marketbook_audit(connection, pending.candidate.event_id, None, visible_in_hub=True)
        log_flashscore_final_decision(
            connection,
            pending,
            None,
            event_type="flashscore_final_inplay_unknown_no_alert",
            message="Flashscore final MarketBook missing; no Slack",
            decision="suppress_unknown",
            api_called_at=api_called_at,
            level="ERROR",
            extra={"reason": "exact MarketBook missing"},
        )
        db_log(connection, "ERROR", "flashscore_final_decision_no_slack", "Flashscore final decision: no Slack", event_id=pending.candidate.event_id, market_id=pending.candidate.market_id, details={"decision": "suppress_unknown", "reason": "exact MarketBook missing"})
        return

    latest_alerted = already_alerted | alerted_event_ids(connection)
    if pending.candidate.event_id in latest_alerted:
        record_flashscore_no_slack(
            connection,
            pending,
            exact_snapshot,
            now=now,
            result="suppressed_ambiguous",
            reason="already_alerted",
            event_type="flashscore_final_decision_no_slack",
            message="Flashscore final decision: already alerted",
            decision="suppress_ambiguous",
            api_called_at=api_called_at,
        )
        return

    if pending.match_confidence is None or pending.match_confidence.level != "High":
        record_flashscore_no_slack(
            connection,
            pending,
            exact_snapshot,
            now=now,
            result="suppressed_ambiguous",
            reason="match_confidence_not_high",
            event_type="flashscore_final_decision_no_slack",
            message="Flashscore final decision: match confidence not high",
            decision="suppress_ambiguous",
            api_called_at=api_called_at,
        )
        return

    if exact_snapshot.inplay is True:
        record_flashscore_no_slack(
            connection,
            pending,
            exact_snapshot,
            now=now,
            result="suppressed_inplay",
            reason="Betfair now in-play after 5-minute delay",
            event_type="flashscore_final_inplay_true_no_alert",
            message="Flashscore final MarketBook is in-play; no Slack",
            decision="suppress_inplay",
            api_called_at=api_called_at,
        )
        return

    if exact_snapshot.inplay is None:
        record_flashscore_no_slack(
            connection,
            pending,
            exact_snapshot,
            now=now,
            result="suppressed_unknown",
            reason="final MarketBook inplay unknown",
            event_type="flashscore_final_inplay_unknown_no_alert",
            message="Flashscore final MarketBook in-play value unknown; no Slack",
            decision="suppress_unknown",
            api_called_at=api_called_at,
        )
        return

    if exact_snapshot.status is None:
        record_flashscore_no_slack(
            connection,
            pending,
            exact_snapshot,
            now=now,
            result="suppressed_unknown",
            reason="final MarketBook status unknown",
            event_type="flashscore_final_inplay_unknown_no_alert",
            message="Flashscore final MarketBook status unknown; no Slack",
            decision="suppress_unknown",
            api_called_at=api_called_at,
        )
        return

    if exact_snapshot.status == "CLOSED":
        record_flashscore_no_slack(
            connection,
            pending,
            exact_snapshot,
            now=now,
            result="suppressed_closed",
            reason="Betfair market closed after 5-minute delay",
            event_type="flashscore_final_status_closed_no_alert",
            message="Flashscore final MarketBook is closed; no Slack",
            decision="suppress_closed",
            api_called_at=api_called_at,
        )
        return

    if exact_snapshot.status not in ALERTABLE_STATUSES:
        record_flashscore_no_slack(
            connection,
            pending,
            exact_snapshot,
            now=now,
            result="suppressed_unknown",
            reason=f"final MarketBook status {exact_snapshot.status}",
            event_type="flashscore_final_inplay_unknown_no_alert",
            message="Flashscore final MarketBook status is not alertable; no Slack",
            decision="suppress_unknown",
            api_called_at=api_called_at,
        )
        return

    if not flashscore_pending_still_live(connection, pending, args):
        record_flashscore_no_slack(
            connection,
            pending,
            exact_snapshot,
            now=now,
            result="suppressed_flashscore_not_live",
            reason="Flashscore match no longer live at verification time",
            event_type="flashscore_pending_suppressed_not_live_at_verification",
            message="Flashscore pending candidate suppressed because match is no longer live",
            decision="suppress_flashscore_not_live",
            api_called_at=api_called_at,
        )
        return

    db_log(
        connection,
        "INFO",
        "flashscore_final_inplay_false_candidate",
        "Flashscore final MarketBook is not in-play candidate",
        sport_name=pending.candidate.sport_name,
        event_id=pending.candidate.event_id,
        market_id=pending.candidate.market_id,
        event_name=pending.candidate.event_name,
        details=flashscore_final_diagnostic_details(pending, exact_snapshot, decision="send_slack_candidate", api_called_at=api_called_at),
    )

    same_event_ok, same_event_reason, same_event_market_ids, other_market_inplay_id = verify_flashscore_same_event_markets(
        connection,
        client,
        pending,
        exact_snapshot,
        final_books,
        api_called_at=api_called_at,
    )
    if not same_event_ok:
        event_type = "flashscore_same_event_other_market_inplay_no_alert" if same_event_reason == "same_event_other_market_inplay" else "flashscore_final_inplay_unknown_no_alert"
        decision = "suppress_same_event_other_market_inplay" if same_event_reason == "same_event_other_market_inplay" else "suppress_unknown"
        result = "suppressed_inplay" if same_event_reason == "same_event_other_market_inplay" else "suppressed_unknown"
        visible = same_event_reason != "same_event_other_market_inplay"
        book = final_snapshot_to_market_book(exact_snapshot, pending.candidate.market_id)
        upsert_alert_state(
            connection,
            pending.candidate,
            book,
            now=now,
            trigger_source=pending.trigger_source,
            flashscore_match=pending.flashscore_match,
            match_confidence=pending.match_confidence,
            final_verification_at=now,
            final_verification_result=result,
            final_verification_reason=same_event_reason,
        )
        update_final_marketbook_audit(connection, pending.candidate.event_id, exact_snapshot, visible_in_hub=visible)
        log_flashscore_final_decision(
            connection,
            pending,
            exact_snapshot,
            event_type=event_type,
            message="Flashscore same-event MATCH_ODDS check blocked Slack",
            decision=decision,
            api_called_at=api_called_at,
            same_event_market_ids=same_event_market_ids,
            other_market_inplay_id=other_market_inplay_id,
            level="INFO" if same_event_reason == "same_event_other_market_inplay" else "ERROR",
            extra={"reason": same_event_reason},
        )
        db_log(
            connection,
            "INFO",
            "flashscore_final_decision_no_slack",
            "Flashscore final decision: no Slack",
            sport_name=pending.candidate.sport_name,
            event_id=pending.candidate.event_id,
            market_id=pending.candidate.market_id,
            event_name=pending.candidate.event_name,
            details={"decision": decision, "reason": same_event_reason},
        )
        return

    final_book = final_snapshot_to_market_book(exact_snapshot, pending.candidate.market_id)
    upsert_alert_state(
        connection,
        pending.candidate,
        final_book,
        now=now,
        trigger_source=pending.trigger_source,
        flashscore_match=pending.flashscore_match,
        match_confidence=pending.match_confidence,
        final_verification_at=now,
        final_verification_result="confirmed_not_inplay",
        final_verification_reason="not_inplay",
    )
    update_final_marketbook_audit(connection, pending.candidate.event_id, exact_snapshot, visible_in_hub=True)
    log_flashscore_final_decision(
        connection,
        pending,
        exact_snapshot,
        event_type="flashscore_final_decision_send_slack",
        message="Flashscore final decision: send Slack",
        decision="send_slack",
        api_called_at=api_called_at,
        same_event_market_ids=same_event_market_ids,
    )
    send_verified_alert(connection, config, args, stats, already_alerted, pending, final_book)


def process_pending_alert_group(
    connection: sqlite3.Connection,
    client: APIClient,
    config: Config,
    args: argparse.Namespace,
    stats: ScanStats,
    pending_alerts: list[PendingAlert],
    delay_seconds: int,
    started_at: datetime,
) -> None:
    if not pending_alerts:
        return

    flashscore_group = all(pending.trigger_source == "flashscore_live" for pending in pending_alerts)
    elapsed_seconds = max(0.0, (utc_now() - started_at).total_seconds())
    wait_seconds = max(0.0, delay_seconds - elapsed_seconds)
    db_log(
        connection,
        "INFO",
        "alert_delay_started",
        f"Alert delay started for {len(pending_alerts)} candidate(s)",
        details={
            "alert_delay_seconds": delay_seconds,
            "wait_seconds": wait_seconds,
            "candidate_count": len(pending_alerts),
            "trigger_sources": sorted({pending.trigger_source for pending in pending_alerts}),
        },
    )
    if wait_seconds:
        time.sleep(wait_seconds)
    db_log(
        connection,
        "INFO",
        "alert_delay_completed",
        "Alert delay completed",
        details={"started_at": iso_utc(started_at), "completed_at": iso_utc(utc_now())},
    )

    already_alerted = alerted_event_ids(connection)
    by_market_id = {pending.candidate.market_id: pending for pending in pending_alerts if pending.candidate.market_id}
    for batch in chunked(list(by_market_id), getattr(args, "market_book_batch_size", DEFAULT_MARKET_BOOK_BATCH_SIZE)):
        db_log(
            connection,
            "INFO",
            "flashscore_delayed_verification_started" if flashscore_group else "final_verification_started",
            "Flashscore delayed verification started" if flashscore_group else "Delayed final verification started",
            details={"market_ids": batch},
        )
        if flashscore_group:
            db_log(
                connection,
                "INFO",
                "flashscore_final_betfair_api_call_started",
                "Flashscore final Betfair API call started",
                details={"market_ids": batch},
            )
        api_called_at = utc_now()
        try:
            final_books = list_final_market_books(client, batch) if flashscore_group else list_market_books(client, batch)
        except Exception as exc:
            stats.api_errors += 1
            for market_id in batch:
                pending = by_market_id[market_id]
                now = utc_now()
                record_final_verification_failed(
                    connection,
                    pending.candidate,
                    pending.initial_book,
                    now=now,
                    reason=f"api_error: {exc}",
                    trigger_source=pending.trigger_source,
                    flashscore_match=pending.flashscore_match,
                    match_confidence=pending.match_confidence,
                )
                db_log(
                    connection,
                    "ERROR",
                    "flashscore_suppressed_after_5min_delay_api_error" if pending.trigger_source == "flashscore_live" else "candidate_suppressed_after_delay_api_error",
                    f"Candidate suppressed after delay: API error {exc}",
                    sport_name=pending.candidate.sport_name,
                    event_id=pending.candidate.event_id,
                    market_id=market_id,
                    event_name=pending.candidate.event_name,
                )
            continue
        if flashscore_group:
            db_log(
                connection,
                "INFO",
                "flashscore_final_betfair_api_call_completed",
                "Flashscore final Betfair API call completed",
                details={"market_ids": batch, "returned_market_ids": sorted(final_books)},
            )

        for market_id in batch:
            pending = by_market_id[market_id]
            candidate = pending.candidate
            final_book = final_books.get(market_id)
            now = utc_now()
            if pending.trigger_source == "flashscore_live":
                process_flashscore_final_pending(
                    connection,
                    client,
                    config,
                    args,
                    stats,
                    already_alerted,
                    pending,
                    final_book,  # type: ignore[arg-type]
                    final_books,  # type: ignore[arg-type]
                    api_called_at,
                )
                continue
            if final_book is None:
                stats.api_errors += 1
                record_final_verification_failed(
                    connection,
                    candidate,
                    pending.initial_book,
                    now=now,
                    reason="api_error: no MarketBook returned",
                    trigger_source=pending.trigger_source,
                    flashscore_match=pending.flashscore_match,
                    match_confidence=pending.match_confidence,
                )
                db_log(
                    connection,
                    "ERROR",
                    "flashscore_suppressed_after_5min_delay_api_error" if pending.trigger_source == "flashscore_live" else "candidate_suppressed_after_delay_api_error",
                    "Candidate suppressed after delay: no MarketBook returned",
                    sport_name=candidate.sport_name,
                    event_id=candidate.event_id,
                    market_id=market_id,
                    event_name=candidate.event_name,
                )
                continue

            latest_alerted = already_alerted | alerted_event_ids(connection)
            if candidate.event_id in latest_alerted:
                upsert_alert_state(
                    connection,
                    candidate,
                    final_book,
                    now=now,
                    trigger_source=pending.trigger_source,
                    flashscore_match=pending.flashscore_match,
                    match_confidence=pending.match_confidence,
                    final_verification_at=now,
                    final_verification_result="suppressed",
                    final_verification_reason="already_alerted",
                )
                continue
            if final_book.inplay:
                final_result = "suppressed_inplay" if pending.trigger_source == "flashscore_live" else "suppressed"
                final_reason = "Betfair now in-play after 5-minute delay" if pending.trigger_source == "flashscore_live" else "inplay"
                upsert_alert_state(
                    connection,
                    candidate,
                    final_book,
                    now=now,
                    trigger_source=pending.trigger_source,
                    flashscore_match=pending.flashscore_match,
                    match_confidence=pending.match_confidence,
                    final_verification_at=now,
                    final_verification_result=final_result,
                    final_verification_reason=final_reason,
                )
                db_log(
                    connection,
                    "INFO",
                    "flashscore_suppressed_after_5min_delay_inplay" if pending.trigger_source == "flashscore_live" else "candidate_suppressed_after_delay_inplay",
                    "Candidate suppressed after delay: market is now in-play",
                    sport_name=candidate.sport_name,
                    event_id=candidate.event_id,
                    market_id=market_id,
                    event_name=candidate.event_name,
                    details={"status": final_book.status, "inplay": final_book.inplay},
                )
                continue
            if final_book.status == "CLOSED":
                final_result = "suppressed_closed" if pending.trigger_source == "flashscore_live" else "suppressed"
                final_reason = "Betfair market closed after 5-minute delay" if pending.trigger_source == "flashscore_live" else "closed"
                upsert_alert_state(
                    connection,
                    candidate,
                    final_book,
                    now=now,
                    trigger_source=pending.trigger_source,
                    flashscore_match=pending.flashscore_match,
                    match_confidence=pending.match_confidence,
                    final_verification_at=now,
                    final_verification_result=final_result,
                    final_verification_reason=final_reason,
                )
                db_log(
                    connection,
                    "INFO",
                    "flashscore_suppressed_after_5min_delay_closed" if pending.trigger_source == "flashscore_live" else "candidate_suppressed_after_delay_closed",
                    "Candidate suppressed after delay: market is closed",
                    sport_name=candidate.sport_name,
                    event_id=candidate.event_id,
                    market_id=market_id,
                    event_name=candidate.event_name,
                    details={"status": final_book.status, "inplay": final_book.inplay},
                )
                continue
            if final_book.status not in ALERTABLE_STATUSES:
                upsert_alert_state(
                    connection,
                    candidate,
                    final_book,
                    now=now,
                    trigger_source=pending.trigger_source,
                    flashscore_match=pending.flashscore_match,
                    match_confidence=pending.match_confidence,
                    final_verification_at=now,
                    final_verification_result="suppressed",
                    final_verification_reason=f"status {final_book.status or 'unknown'}",
                )
                continue

            upsert_alert_state(
                connection,
                candidate,
                final_book,
                now=now,
                trigger_source=pending.trigger_source,
                flashscore_match=pending.flashscore_match,
                match_confidence=pending.match_confidence,
                final_verification_at=now,
                final_verification_result="confirmed_not_inplay",
                final_verification_reason="not_inplay",
            )
            db_log(
                connection,
                "INFO",
                "flashscore_delayed_verification_confirmed_not_inplay" if pending.trigger_source == "flashscore_live" else "final_verification_confirmed_not_inplay",
                "Flashscore delayed verification confirmed market is still not in-play" if pending.trigger_source == "flashscore_live" else "Final verification confirmed market is still not in-play",
                sport_name=candidate.sport_name,
                event_id=candidate.event_id,
                market_id=market_id,
                event_name=candidate.event_name,
                details={"status": final_book.status, "inplay": final_book.inplay},
            )
            send_verified_alert(connection, config, args, stats, already_alerted, pending, final_book)


def process_pending_alerts(
    connection: sqlite3.Connection,
    client: APIClient,
    config: Config,
    args: argparse.Namespace,
    stats: ScanStats,
) -> None:
    pending_by_event: dict[str, PendingAlert] = {}
    for pending in stats.pending_alerts:
        if pending.trigger_source == "flashscore_live":
            continue
        if pending.candidate.event_id and pending.candidate.event_id not in pending_by_event:
            pending_by_event[pending.candidate.event_id] = pending
    if not pending_by_event:
        return

    started_at = utc_now()
    grouped: dict[int, list[PendingAlert]] = {}
    for pending in pending_by_event.values():
        grouped.setdefault(max(0, pending.alert_delay_seconds), []).append(pending)
    for delay_seconds in sorted(grouped):
        process_pending_alert_group(
            connection,
            client,
            config,
            args,
            stats,
            grouped[delay_seconds],
            delay_seconds,
            started_at,
        )


def pending_alert_from_state_row(row: sqlite3.Row) -> PendingAlert:
    candidate = MarketCandidate(
        sport_name=str(row["sport_name"] or ""),
        event_type_id="",
        event_id=str(row["event_id"] or ""),
        event_name=str(row["event_name"] or row["flashscore_match_name"] or ""),
        competition_name=str(row["competition_name"] or row["flashscore_competition"] or ""),
        market_id=str(row["market_id"] or ""),
        scheduled_start_utc=parse_datetime(row["scheduled_start_utc"]),
    )
    inplay_value = row["last_seen_inplay"] if row["last_seen_inplay"] is not None else row["betfair_last_seen_inplay"]
    initial_book = MarketBookSnapshot(
        candidate.market_id,
        str(row["last_seen_status"] or row["betfair_last_seen_status"] or "OPEN"),
        bool(int(inplay_value)) if inplay_value is not None else False,
    )
    flashscore_match = FlashscoreMatch(
        sport_name=str(row["sport_name"] or ""),
        match_name=str(row["flashscore_match_name"] or row["event_name"] or ""),
        competition_name=str(row["flashscore_competition"] or row["competition_name"] or ""),
        status_text=str(row["flashscore_status"] or ""),
        score=str(row["flashscore_score"] or ""),
        match_id=str(row["flashscore_match_id"] or ""),
        url=str(row["flashscore_url"] or ""),
        detected_live_at=parse_datetime(row["flashscore_detected_live_at"]) or parse_datetime(row["candidate_first_seen_at"]) or utc_now(),
        participants=(str(row["flashscore_participant_1"] or ""), str(row["flashscore_participant_2"] or "")),
        match_format=str(row["match_format"] or "singles"),
        side_1_player_1=str(row["side_1_player_1"] or ""),
        side_1_player_2=str(row["side_1_player_2"] or ""),
        side_2_player_1=str(row["side_2_player_1"] or ""),
        side_2_player_2=str(row["side_2_player_2"] or ""),
    )
    confidence = MatchConfidence(
        level=str(row["match_confidence"] or "High"),
        reason=str(row["match_reason"] or "delayed_flashscore_verification"),
        score=float(row["match_score"] or 0.0),
        flashscore_participant_1=str(row["flashscore_participant_1"] or ""),
        flashscore_participant_2=str(row["flashscore_participant_2"] or ""),
        betfair_participant_1=str(row["betfair_participant_1"] or ""),
        betfair_participant_2=str(row["betfair_participant_2"] or ""),
        flashscore_surname_1=str(row["flashscore_surname_1"] or ""),
        flashscore_surname_2=str(row["flashscore_surname_2"] or ""),
        betfair_surname_1=str(row["betfair_surname_1"] or ""),
        betfair_surname_2=str(row["betfair_surname_2"] or ""),
        match_format=str(row["match_format"] or "singles"),
        side_1_player_1=str(row["side_1_player_1"] or ""),
        side_1_player_2=str(row["side_1_player_2"] or ""),
        side_2_player_1=str(row["side_2_player_1"] or ""),
        side_2_player_2=str(row["side_2_player_2"] or ""),
        side_1_surnames=str(row["side_1_surnames"] or ""),
        side_2_surnames=str(row["side_2_surnames"] or ""),
        betfair_side_1_players=str(row["betfair_side_1_players"] or ""),
        betfair_side_2_players=str(row["betfair_side_2_players"] or ""),
        betfair_side_1_surnames=str(row["betfair_side_1_surnames"] or ""),
        betfair_side_2_surnames=str(row["betfair_side_2_surnames"] or ""),
    )
    return PendingAlert(
        candidate,
        initial_book,
        "flashscore_live",
        flashscore_match,
        confidence,
        int(row["alert_delay_seconds"] or FLASHSCORE_ALERT_DELAY_SECONDS),
    )


def due_flashscore_pending_alerts(connection: sqlite3.Connection, now: datetime) -> list[PendingAlert]:
    rows = connection.execute(
        """
        SELECT event_id, market_id, sport_name, competition_name, event_name,
               scheduled_start_utc, last_seen_status, last_seen_inplay, betfair_last_seen_status,
               betfair_last_seen_inplay, trigger_source, flashscore_match_id, flashscore_url,
               flashscore_match_name, flashscore_competition, flashscore_status, flashscore_score,
               flashscore_detected_live_at, match_confidence, match_reason, match_score,
               flashscore_participant_1, flashscore_participant_2, betfair_participant_1,
               betfair_participant_2, flashscore_surname_1, flashscore_surname_2,
               betfair_surname_1, betfair_surname_2, match_format, side_1_player_1,
               side_1_player_2, side_2_player_1, side_2_player_2, side_1_surnames,
               side_2_surnames, betfair_side_1_players, betfair_side_2_players,
               betfair_side_1_surnames, betfair_side_2_surnames, candidate_first_seen_at,
               pending_verification_at, verify_after, alert_delay_seconds,
               final_verification_result, final_verification_reason
        FROM inplay_alert_state
        WHERE trigger_source = 'flashscore_live'
          AND alert_sent_at IS NULL
          AND verify_after IS NOT NULL
          AND verify_after <= ?
          AND COALESCE(final_verification_result, '') IN ('pending_verification', 'failed')
        ORDER BY verify_after ASC
        LIMIT 100
        """,
        (iso_utc(now),),
    ).fetchall()
    pending_alerts = [pending_alert_from_state_row(row) for row in rows]
    for pending in pending_alerts:
        mark_state_seen_in_current_run(connection, pending.candidate.event_id)
        db_log(
            connection,
            "INFO",
            "flashscore_due_pending_loaded",
            "Flashscore due pending candidate loaded",
            sport_name=pending.candidate.sport_name,
            event_id=pending.candidate.event_id,
            market_id=pending.candidate.market_id,
            event_name=pending.candidate.event_name,
            details={"alert_delay_seconds": pending.alert_delay_seconds},
        )
        db_log(
            connection,
            "INFO",
            "flashscore_delayed_verification_due",
            "Flashscore delayed verification is due",
            sport_name=pending.candidate.sport_name,
            event_id=pending.candidate.event_id,
            market_id=pending.candidate.market_id,
            event_name=pending.candidate.event_name,
            details={"alert_delay_seconds": pending.alert_delay_seconds},
        )
    return pending_alerts


def process_due_flashscore_pending_alerts(
    connection: sqlite3.Connection,
    client: APIClient,
    config: Config,
    args: argparse.Namespace,
    stats: ScanStats,
) -> None:
    pending_alerts = due_flashscore_pending_alerts(connection, utc_now())
    if not pending_alerts:
        return
    process_pending_alert_group(
        connection,
        client,
        config,
        args,
        stats,
        pending_alerts,
        0,
        utc_now(),
    )


def cleanup_visible_rows_after_run(connection: sqlite3.Connection, current_run_id: str, now: datetime) -> None:
    now_iso = iso_utc(now)
    actionable_results = ("failed", "suppressed_unknown", "suppressed_ambiguous")
    connection.execute(
        """
        UPDATE inplay_alert_state
        SET visible_in_hub = 1,
            hidden_reason = '',
            hidden_at = NULL
        WHERE (
            last_seen_run_id = ?
            OR COALESCE(final_verification_result, '') = 'pending_verification'
            OR (verify_after IS NOT NULL AND verify_after > ?)
            OR COALESCE(slack_alert_sent, 0) = 1
            OR alert_sent_at IS NOT NULL
            OR COALESCE(final_verification_result, '') IN (?, ?, ?)
            OR COALESCE(final_verification_reason, '') LIKE '%ambiguous%'
            OR COALESCE(slack_error, '') != ''
        )
        AND NOT (
            COALESCE(slack_alert_sent, 0) = 0
            AND alert_sent_at IS NULL
            AND COALESCE(slack_error, '') = ''
            AND (
                COALESCE(final_verification_result, '') = 'suppressed_inplay'
                OR COALESCE(betfair_last_seen_inplay, last_seen_inplay, 0) = 1
            )
        )
        """,
        (current_run_id, now_iso, *actionable_results),
    )
    connection.execute(
        """
        UPDATE inplay_alert_state
        SET visible_in_hub = 0,
            hidden_reason = 'all_good_inplay',
            hidden_at = ?
        WHERE COALESCE(slack_alert_sent, 0) = 0
          AND alert_sent_at IS NULL
          AND COALESCE(slack_error, '') = ''
          AND (
              COALESCE(final_verification_result, '') = 'suppressed_inplay'
              OR COALESCE(betfair_last_seen_inplay, last_seen_inplay, 0) = 1
          )
        """,
        (now_iso,),
    )
    flashscore_rows_to_hide = connection.execute(
        """
        SELECT event_id, market_id, sport_name, event_name, flashscore_match_name
        FROM inplay_alert_state
        WHERE trigger_source = 'flashscore_live'
          AND COALESCE(last_seen_run_id, '') != ?
          AND COALESCE(final_verification_result, '') != 'pending_verification'
          AND (verify_after IS NULL OR verify_after <= ?)
          AND COALESCE(slack_alert_sent, 0) = 0
          AND alert_sent_at IS NULL
          AND COALESCE(final_verification_result, '') NOT IN (?, ?, ?)
          AND COALESCE(final_verification_reason, '') NOT LIKE '%ambiguous%'
          AND COALESCE(slack_error, '') = ''
        LIMIT 100
        """,
        (current_run_id, now_iso, *actionable_results),
    ).fetchall()
    connection.execute(
        """
        UPDATE inplay_alert_state
        SET visible_in_hub = 0,
            hidden_reason = CASE
                WHEN trigger_source = 'flashscore_live' THEN 'flashscore_not_live_or_not_seen_latest_live_scan'
                ELSE 'old_not_seen_in_latest_scan'
            END,
            hidden_at = ?
        WHERE COALESCE(last_seen_run_id, '') != ?
          AND COALESCE(final_verification_result, '') != 'pending_verification'
          AND (verify_after IS NULL OR verify_after <= ?)
          AND COALESCE(slack_alert_sent, 0) = 0
          AND alert_sent_at IS NULL
          AND COALESCE(final_verification_result, '') NOT IN (?, ?, ?)
          AND COALESCE(final_verification_reason, '') NOT LIKE '%ambiguous%'
          AND COALESCE(slack_error, '') = ''
        """,
        (now_iso, current_run_id, now_iso, *actionable_results),
    )
    connection.commit()
    for row in flashscore_rows_to_hide:
        db_log(
            connection,
            "INFO",
            "flashscore_visible_row_hidden_not_live",
            "Flashscore row hidden because it was not seen in latest live-only scan",
            sport_name=str(row["sport_name"] or ""),
            event_id=str(row["event_id"] or ""),
            market_id=str(row["market_id"] or ""),
            event_name=str(row["event_name"] or row["flashscore_match_name"] or ""),
            details={"reason": "flashscore_not_live_or_not_seen_latest_live_scan"},
        )


def cleanup_hub_visibility(connection: sqlite3.Connection) -> None:
    cleanup_visible_rows_after_run(connection, CURRENT_SCAN_RUN_ID, utc_now())


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


def alert_delay_seconds_for_source(args: argparse.Namespace, trigger_source: str) -> int:
    base_delay = max(0, int(getattr(args, "alert_delay_seconds", BETFAIR_TIME_ALERT_DELAY_SECONDS)))
    if trigger_source != "flashscore_live":
        return base_delay
    return max(0, int(getattr(args, "flashscore_alert_delay_seconds", FLASHSCORE_ALERT_DELAY_SECONDS)))


def fetch_overdue_candidates(
    connection: sqlite3.Connection,
    client: APIClient,
    args: argparse.Namespace,
    stats: ScanStats,
) -> tuple[list[MarketCandidate], list[str]]:
    now = utc_now()
    overdue_threshold_seconds = betfair_time_overdue_threshold_seconds(args)
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
            if candidate.scheduled_start_utc is None or not is_betfair_time_overdue(candidate, now, overdue_threshold_seconds):
                stats.skipped_events += 1
                db_log(
                    connection,
                    "DEBUG",
                    "not_overdue_yet",
                    "Skipped event that is not over 5 minutes past scheduled start",
                    sport_name=candidate.sport_name,
                    event_id=candidate.event_id,
                    market_id=candidate.market_id,
                    event_name=candidate.event_name,
                    details={
                        "reason": "not overdue",
                        "scheduled_start_utc": iso_utc(candidate.scheduled_start_utc),
                        "overdue_threshold_seconds": overdue_threshold_seconds,
                    },
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
    overdue_threshold_seconds = betfair_time_overdue_threshold_seconds(args)
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

            decision = alert_decision(candidate, book, now, already_alerted, overdue_threshold_seconds)
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
                    details={
                        "reason": decision.reason,
                        "status": book.status,
                        "inplay": book.inplay,
                        "overdue_threshold_seconds": overdue_threshold_seconds,
                    },
                )
                continue

            db_log(
                connection,
                "INFO",
                "candidate_found",
                "Candidate found for delayed verification",
                sport_name=candidate.sport_name,
                event_id=candidate.event_id,
                market_id=market_id,
                event_name=candidate.event_name,
                details={
                    "status": book.status,
                    "inplay": book.inplay,
                    "overdue_threshold_seconds": overdue_threshold_seconds,
                    "scheduled_start_utc": iso_utc(candidate.scheduled_start_utc),
                },
            )
            handled_this_scan.add(candidate.event_id)
            pending = PendingAlert(
                candidate,
                book,
                "betfair_time",
                alert_delay_seconds=alert_delay_seconds_for_source(args, "betfair_time"),
            )
            stats.pending_alerts.append(pending)
            mark_candidate_pending(
                connection,
                pending,
                now=now,
                alert_delay_seconds=pending.alert_delay_seconds,
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


def darts_event_names_materially_different(
    flashscore_match: FlashscoreMatch,
    candidate: MarketCandidate,
    confidence: MatchConfidence,
) -> bool:
    if confidence.score < 85:
        return False
    flashscore_surnames = {
        surname
        for surname in (confidence.flashscore_surname_1, confidence.flashscore_surname_2)
        if surname
    }
    betfair_surnames = {
        surname
        for surname in (confidence.betfair_surname_1, confidence.betfair_surname_2)
        if surname
    }
    if flashscore_surnames and betfair_surnames and flashscore_surnames == betfair_surnames:
        return False
    return name_similarity(flashscore_match.match_name, candidate.event_name) < 0.55


def best_betfair_match(
    connection: sqlite3.Connection,
    flashscore_match: FlashscoreMatch,
    betfair_candidates: list[MarketCandidate],
) -> tuple[MarketCandidate | None, MatchConfidence, bool]:
    doubles_log = flashscore_match.sport_name == "Tennis" and flashscore_match.match_format == "doubles"
    db_log(
        connection,
        "INFO",
        "doubles_name_match_started" if doubles_log else "name_match_started",
        "Flashscore Betfair name matching started",
        sport_name=flashscore_match.sport_name,
        event_name=flashscore_match.match_name,
        details={
            "flashscore_match_name": flashscore_match.match_name,
            "flashscore_competition": flashscore_match.competition_name,
            "match_format": flashscore_match.match_format,
        },
    )
    scored: list[tuple[MarketCandidate, MatchConfidence]] = []
    for candidate in betfair_candidates:
        if normalize_sport_name(candidate.sport_name) != normalize_sport_name(flashscore_match.sport_name):
            continue
        confidence = participant_confidence(
            flashscore_match.participants,
            parse_participants(candidate.event_name),
            flashscore_match.competition_name,
            candidate.competition_name,
        )
        if candidate.scheduled_start_utc and abs((flashscore_match.detected_live_at - candidate.scheduled_start_utc).total_seconds()) <= 3 * 3600:
            new_score = confidence.score + 5
            new_level = confidence.level
            if confidence.match_format == "doubles" and new_score >= 90:
                fs_surname_count = len([value for value in (confidence.side_1_surnames + "," + confidence.side_2_surnames).split(",") if value.strip()])
                bf_surname_count = len([value for value in (confidence.betfair_side_1_surnames + "," + confidence.betfair_side_2_surnames).split(",") if value.strip()])
                if fs_surname_count == 4 and bf_surname_count == 4 and "fewer than" not in confidence.reason and "three of four" not in confidence.reason:
                    new_level = "High"
            confidence = replace(
                confidence,
                score=new_score,
                level=new_level,
                reason=f"{confidence.reason}; scheduled time near live detection",
            )
        scored.append((candidate, confidence))
        db_log(
            connection,
            "DEBUG",
            "doubles_name_match_candidate_scored" if doubles_log else "name_match_candidate_scored",
            f"Name match candidate scored: {confidence.score:.0f} {confidence.level}",
            sport_name=flashscore_match.sport_name,
            event_id=candidate.event_id,
            market_id=candidate.market_id,
            event_name=candidate.event_name,
            details={
                "flashscore_match_name": flashscore_match.match_name,
                "betfair_event_name": candidate.event_name,
                "match_score": confidence.score,
                "match_confidence": confidence.level,
                "match_reason": confidence.reason,
                "flashscore_surname_1": confidence.flashscore_surname_1,
                "flashscore_surname_2": confidence.flashscore_surname_2,
                "betfair_surname_1": confidence.betfair_surname_1,
                "betfair_surname_2": confidence.betfair_surname_2,
                "match_format": confidence.match_format,
                "side_1_surnames": confidence.side_1_surnames,
                "side_2_surnames": confidence.side_2_surnames,
                "betfair_side_1_surnames": confidence.betfair_side_1_surnames,
                "betfair_side_2_surnames": confidence.betfair_side_2_surnames,
            },
        )
    if not scored:
        return None, MatchConfidence("Low", "No Betfair match found", 0.0), False
    scored.sort(key=lambda item: item[1].score, reverse=True)
    best_candidate, best_confidence = scored[0]
    high_confidence_candidates = [item for item in scored if item[1].level == "High"]
    if len(high_confidence_candidates) > 1 and high_confidence_candidates[1][1].score >= best_confidence.score - 8:
        return best_candidate, replace(
            best_confidence,
            level="Low",
            reason=f"ambiguous_match: multiple high-confidence candidates scored {best_confidence.score:.0f} and {high_confidence_candidates[1][1].score:.0f}",
        ), True
    if flashscore_match.sport_name == "Darts" and best_confidence.level == "High" and darts_event_names_materially_different(flashscore_match, best_candidate, best_confidence):
        return best_candidate, replace(
            best_confidence,
            level="Low",
            reason=f"ambiguous_match: Darts event names materially different; {best_confidence.reason}",
        ), True
    if len(scored) > 1 and best_confidence.level == "High" and scored[1][1].score >= best_confidence.score - 8:
        return best_candidate, MatchConfidence(
            "Low",
            f"ambiguous_match: top scores {best_confidence.score:.0f} and {scored[1][1].score:.0f}",
            best_confidence.score,
            best_confidence.flashscore_participant_1,
            best_confidence.flashscore_participant_2,
            best_confidence.betfair_participant_1,
            best_confidence.betfair_participant_2,
            best_confidence.flashscore_surname_1,
            best_confidence.flashscore_surname_2,
            best_confidence.betfair_surname_1,
            best_confidence.betfair_surname_2,
            best_confidence.match_format,
            best_confidence.side_1_player_1,
            best_confidence.side_1_player_2,
            best_confidence.side_2_player_1,
            best_confidence.side_2_player_2,
            best_confidence.side_1_surnames,
            best_confidence.side_2_surnames,
            best_confidence.betfair_side_1_players,
            best_confidence.betfair_side_2_players,
            best_confidence.betfair_side_1_surnames,
            best_confidence.betfair_side_2_surnames,
        ), True
    return best_candidate, best_confidence, False


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
        candidate, confidence, ambiguous = best_betfair_match(
            connection,
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
        if ambiguous:
            record_flashscore_match_diagnostic(
                connection,
                candidate,
                flashscore_match,
                confidence,
                now=utc_now(),
                result="skipped",
                reason="ambiguous_match",
            )
            db_log(
                connection,
                "INFO",
                "doubles_name_match_ambiguous_no_alert" if flashscore_match.match_format == "doubles" else "name_match_ambiguous_no_alert",
                "Flashscore Betfair match skipped as ambiguous",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={
                    "flashscore_match_name": flashscore_match.match_name,
                    "match_score": confidence.score,
                    "match_confidence": confidence.level,
                    "match_reason": confidence.reason,
                },
            )
            continue
        if confidence.level != "High":
            record_flashscore_match_diagnostic(
                connection,
                candidate,
                flashscore_match,
                confidence,
                now=utc_now(),
                result="skipped",
                reason=f"{confidence.level.casefold()}_confidence",
            )
            if flashscore_match.match_format == "doubles":
                log_type = "doubles_name_match_medium_confidence_no_alert" if confidence.level == "Medium" else "doubles_name_match_low_confidence_no_alert"
            else:
                log_type = "name_match_medium_confidence_no_alert" if confidence.level == "Medium" else "name_match_low_confidence_no_alert"
            db_log(
                connection,
                "INFO",
                log_type,
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
            "doubles_name_match_high_confidence" if flashscore_match.match_format == "doubles" else "name_match_high_confidence",
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

        existing_flashscore_state = connection.execute(
            """
            SELECT final_verification_result, final_verification_reason
            FROM inplay_alert_state
            WHERE event_id = ?
            """,
            (candidate.event_id,),
        ).fetchone()
        existing_flashscore_result = str(existing_flashscore_state["final_verification_result"] or "") if existing_flashscore_state else ""
        existing_flashscore_reason = str(existing_flashscore_state["final_verification_reason"] or "") if existing_flashscore_state else ""
        if existing_flashscore_result in {"suppressed_inplay", "suppressed_closed"} or (
            existing_flashscore_result == "suppressed" and existing_flashscore_reason in {"inplay", "closed", "already_alerted"}
        ):
            db_log(
                connection,
                "INFO",
                "skipped",
                "Skipped Flashscore candidate: already handled after delayed verification",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={
                    "reason": existing_flashscore_reason,
                    "flashscore_match_name": flashscore_match.match_name,
                },
            )
            continue

        try:
            current_books = list_market_books(client, [candidate.market_id])
        except Exception as exc:
            stats.api_errors += 1
            db_log(
                connection,
                "ERROR",
                "flashscore_suppressed_after_5min_delay_api_error",
                f"Flashscore candidate current Betfair check failed: {exc}",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={"flashscore_match_name": flashscore_match.match_name},
            )
            continue

        current_book = current_books.get(candidate.market_id)
        if current_book is None:
            stats.api_errors += 1
            db_log(
                connection,
                "ERROR",
                "flashscore_suppressed_after_5min_delay_api_error",
                "Flashscore candidate current Betfair check returned no MarketBook",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={"flashscore_match_name": flashscore_match.match_name},
            )
            continue

        if current_book.inplay:
            upsert_alert_state(
                connection,
                candidate,
                current_book,
                now=utc_now(),
                trigger_source="flashscore_live",
                flashscore_match=flashscore_match,
                match_confidence=confidence,
                final_verification_at=utc_now(),
                final_verification_result="suppressed_inplay",
                final_verification_reason="Betfair already in-play at Flashscore candidate creation",
            )
            db_log(
                connection,
                "INFO",
                "flashscore_candidate_suppressed_betfair_already_inplay",
                "Flashscore candidate suppressed: Betfair already in-play",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={
                    "flashscore_match_name": flashscore_match.match_name,
                    "status": current_book.status,
                    "inplay": current_book.inplay,
                },
            )
            continue

        if current_book.status == "CLOSED":
            upsert_alert_state(
                connection,
                candidate,
                current_book,
                now=utc_now(),
                trigger_source="flashscore_live",
                flashscore_match=flashscore_match,
                match_confidence=confidence,
                final_verification_at=utc_now(),
                final_verification_result="suppressed_closed",
                final_verification_reason="Betfair market closed at Flashscore candidate creation",
            )
            db_log(
                connection,
                "INFO",
                "flashscore_suppressed_after_5min_delay_closed",
                "Flashscore candidate suppressed: Betfair market is closed",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={"flashscore_match_name": flashscore_match.match_name, "status": current_book.status},
            )
            continue

        if current_book.status not in ALERTABLE_STATUSES:
            upsert_alert_state(
                connection,
                candidate,
                current_book,
                now=utc_now(),
                trigger_source="flashscore_live",
                flashscore_match=flashscore_match,
                match_confidence=confidence,
                final_verification_at=utc_now(),
                final_verification_result="suppressed_status",
                final_verification_reason=f"Betfair status {current_book.status or 'unknown'} at Flashscore candidate creation",
            )
            db_log(
                connection,
                "INFO",
                "skipped",
                "Skipped Flashscore candidate: Betfair status is not alertable",
                sport_name=flashscore_match.sport_name,
                event_id=candidate.event_id,
                market_id=candidate.market_id,
                event_name=candidate.event_name,
                details={"flashscore_match_name": flashscore_match.match_name, "status": current_book.status},
            )
            continue

        db_log(
            connection,
            "INFO",
            "candidate_found",
            "Flashscore candidate found for delayed verification",
            sport_name=flashscore_match.sport_name,
            event_id=candidate.event_id,
            market_id=candidate.market_id,
            event_name=candidate.event_name,
            details={"flashscore_match_name": flashscore_match.match_name},
        )
        pending = PendingAlert(
            candidate,
            current_book,
            "flashscore_live",
            flashscore_match,
            confidence,
            alert_delay_seconds_for_source(args, "flashscore_live"),
        )
        mark_candidate_pending(
            connection,
            pending,
            now=utc_now(),
            alert_delay_seconds=pending.alert_delay_seconds,
        )

    stats.flashscore_scan_status = "complete"
    db_log(connection, "INFO", "flashscore_scan_completed", f"Flashscore live trigger scan completed: {len(live_matches)} live matches")


def run_scan(args: argparse.Namespace, config: Config, connection: sqlite3.Connection) -> int:
    run_id = start_scan_run(connection, args)
    stats = ScanStats()
    excluded_sports: list[str] = []
    status = "complete"
    config_error = ""
    run_lock = acquire_run_lock(
        connection,
        run_id,
        max(1, int(getattr(args, "run_lock_stale_seconds", DEFAULT_RUN_LOCK_STALE_SECONDS))),
    )
    if run_lock is None:
        status = "skipped"
        stats.betfair_time_scan_status = "skipped"
        stats.flashscore_scan_status = "skipped"
        finish_scan_run(connection, run_id, status, stats, excluded_sports, config_error=config_error)
        return 0
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
        cleanup_hub_visibility(connection)
        finish_scan_run(connection, run_id, status, stats, excluded_sports, config_error=config_error)
        release_run_lock(run_lock)
        return 1 if not args.repeat_minutes else 0

    try:
        try:
            process_due_flashscore_pending_alerts(connection, client, config, args, stats)
        except Exception as exc:
            status = "partial_failure"
            stats.api_errors += 1
            db_log(connection, "ERROR", "api_error", f"Flashscore delayed verification failed: {exc}")
            traceback.print_exc(file=sys.stdout)

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

        try:
            process_pending_alerts(connection, client, config, args, stats)
        except Exception as exc:
            status = "partial_failure"
            stats.api_errors += 1
            db_log(connection, "ERROR", "api_error", f"Delayed alert verification failed: {exc}")
            traceback.print_exc(file=sys.stdout)
    except Exception as exc:
        status = "failed"
        stats.api_errors += 1
        db_log(connection, "ERROR", "api_error", f"Full run failed: {exc}")
        traceback.print_exc(file=sys.stdout)
        if not args.repeat_minutes:
            cleanup_hub_visibility(connection)
            finish_scan_run(connection, run_id, status, stats, excluded_sports, config_error=config_error)
            release_run_lock(run_lock)
            return 1
    finally:
        if client is not None:
            try:
                client.logout()
            except Exception:
                pass

    cleanup_hub_visibility(connection)
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
    release_run_lock(run_lock)
    return 0 if status in {"complete", "partial_failure"} else 1


def mark_next_scan(connection: sqlite3.Connection, next_scan_at: datetime) -> None:
    row = connection.execute("SELECT id FROM inplay_scan_runs ORDER BY id DESC LIMIT 1").fetchone()
    if not row:
        return
    connection.execute("UPDATE inplay_scan_runs SET next_scan_at = ? WHERE id = ?", (iso_utc(next_scan_at), row["id"]))
    connection.commit()


def run_self_test() -> int:
    now = datetime(2026, 6, 19, 12, 5, tzinfo=timezone.utc)
    overdue = now - timedelta(minutes=5, seconds=1)
    two_minutes_overdue = now - timedelta(minutes=2)
    almost_overdue = now - timedelta(minutes=4, seconds=59)
    future = now - timedelta(seconds=90)
    candidate = MarketCandidate("Cricket", "4", "123456789", "India v Australia", "ICC T20 World Cup", "1.234", overdue)
    two_minute_candidate = MarketCandidate("Cricket", "4", "123456790", "India v England", "ICC T20 World Cup", "1.235", two_minutes_overdue)
    almost_overdue_candidate = MarketCandidate("Cricket", "4", "123456791", "India v Pakistan", "ICC T20 World Cup", "1.236", almost_overdue)
    suspended = MarketBookSnapshot("1.234", "SUSPENDED", False)
    open_book = MarketBookSnapshot("1.234", "OPEN", False)
    inplay_book = MarketBookSnapshot("1.234", "OPEN", True)
    closed_book = MarketBookSnapshot("1.234", "CLOSED", False)
    not_overdue_candidate = MarketCandidate("Cricket", "4", "123456789", "India v Australia", "", "1.234", future)

    assert alert_decision(candidate, open_book, now, set(), BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS).should_alert
    assert alert_decision(candidate, suspended, now, set(), BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS).should_alert
    assert "SUSPENDED" in build_slack_message(candidate, suspended, now)
    assert not alert_decision(two_minute_candidate, open_book, now, set(), BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS).should_alert
    assert not alert_decision(almost_overdue_candidate, open_book, now, set(), BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS).should_alert
    assert alert_decision(almost_overdue_candidate, open_book, now, set(), BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS).reason == "not overdue"
    assert not alert_decision(candidate, inplay_book, now, set(), BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS).should_alert
    assert alert_decision(candidate, inplay_book, now, set(), BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS).reason == "already in-play"
    assert not alert_decision(not_overdue_candidate, open_book, now, set(), BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS).should_alert
    assert alert_decision(not_overdue_candidate, open_book, now, set(), BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS).reason == "not overdue"
    assert not alert_decision(candidate, closed_book, now, set(), BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS).should_alert
    assert alert_decision(candidate, closed_book, now, set(), BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS).reason == "closed"
    assert not alert_decision(candidate, open_book, now, {"123456789"}, BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS).should_alert
    assert alert_decision(candidate, open_book, now, {"123456789"}, BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS).reason == "already alerted"
    delay_args = argparse.Namespace(alert_delay_seconds=60, flashscore_alert_delay_seconds=300)
    assert alert_delay_seconds_for_source(delay_args, "betfair_time") == 60
    assert alert_delay_seconds_for_source(delay_args, "flashscore_live") == 300
    assert is_excluded_sport("Tennis")
    assert is_excluded_sport("Football")
    assert is_excluded_sport("Soccer")
    assert is_excluded_sport("Horse Racing")
    assert is_excluded_sport("Greyhound Racing")
    assert not is_excluded_sport("Cricket")
    assert flashscore_row_live_decision("Set 1", "", "", sport_name="Tennis").is_live
    assert not flashscore_row_live_decision("Finished", "6-4 6-4", "", sport_name="Tennis").is_live
    yesterday_finished = flashscore_row_live_decision(
        "Finished",
        "6-4 6-4",
        "",
        sport_name="Tennis",
        parent_text="Yesterday",
    )
    assert not yesterday_finished.is_live
    assert yesterday_finished.event_type == "flashscore_row_rejected_finished"
    assert not flashscore_row_live_decision("12:00", "", "", sport_name="Tennis").is_live
    assert not flashscore_row_live_decision("", "6-4 6-4", "", sport_name="Tennis").is_live
    assert flashscore_row_live_decision("Leg 1", "", "", sport_name="Darts").is_live
    assert not flashscore_row_live_decision("FT", "6-3", "", sport_name="Darts").is_live

    smith_match = participant_confidence(("Smith M.", "Jones D."), ("Michael Smith", "Dave Jones"), "", "")
    assert smith_match.level == "High"
    assert smith_match.flashscore_surname_1 == "smith"
    assert smith_match.betfair_surname_1 == "smith"
    djokovic_match = participant_confidence(("Djokovic N.", "Alcaraz C."), ("Novak Djokovic", "Carlos Alcaraz"), "", "")
    assert djokovic_match.level == "High"
    van_gerwen_match = participant_confidence(
        ("Van Gerwen M.", "Smith M."),
        ("Michael van Gerwen", "Michael Smith"),
        "Premier League Darts",
        "Premier League Darts",
    )
    assert van_gerwen_match.level == "High"
    assert van_gerwen_match.flashscore_surname_1 == "van gerwen"
    assert van_gerwen_match.betfair_surname_1 == "van gerwen"
    one_surname_match = participant_confidence(("Smith M.", "Brown D."), ("Michael Smith", "Dave Jones"), "", "")
    assert one_surname_match.level == "Low"
    assert "both participant surnames did not match" in one_surname_match.reason
    doubles_match = participant_confidence(
        ("Murray J. / Skupski N.", "Ram R. / Salisbury J."),
        ("Jamie Murray/Neal Skupski", "Joe Salisbury & Rajeev Ram"),
        "ATP Doubles",
        "ATP Doubles",
    )
    assert doubles_match.level == "High"
    assert doubles_match.match_format == "doubles"
    assert doubles_match.side_1_surnames == "murray, skupski"
    three_surname_doubles = participant_confidence(
        ("Murray J. / Skupski N.", "Ram R. / Salisbury J."),
        ("Jamie Murray/Neal Skupski", "Joe Salisbury & Max Purcell"),
        "ATP Doubles",
        "ATP Doubles",
    )
    assert three_surname_doubles.level == "Medium"

    lock_db = sqlite3.connect(":memory:")
    lock_db.row_factory = sqlite3.Row
    init_db(lock_db)
    original_lock_path = globals()["RUN_LOCK_PATH"]
    try:
        RUNTIME_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        globals()["RUN_LOCK_PATH"] = RUNTIME_OUTPUT_DIR / "betfair_inplay_start_checker_selftest.lock"
        first_lock = acquire_run_lock(lock_db, 1)
        assert first_lock is not None
        second_lock = acquire_run_lock(lock_db, 2)
        assert second_lock is None
        assert lock_db.execute(
            "SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'run_skipped_existing_run_active'"
        ).fetchone()[0] == 1
        release_run_lock(first_lock)
        third_lock = acquire_run_lock(lock_db, 3)
        assert third_lock is not None
        release_run_lock(third_lock)
    finally:
        globals()["RUN_LOCK_PATH"] = original_lock_path
        lock_db.close()

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
    assert "Overdue by: 5m 1s" in message
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
        runtime_now - timedelta(minutes=5, seconds=1),
    )

    dry_run_db = sqlite3.connect(":memory:")
    dry_run_db.row_factory = sqlite3.Row
    init_db(dry_run_db)
    dry_run_stats = ScanStats()
    dry_run_args = argparse.Namespace(
        dry_run=True,
        market_book_batch_size=40,
        betfair_time_overdue_threshold_seconds=BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS,
        alert_delay_seconds=0,
        flashscore_alert_delay_seconds=0,
    )
    process_candidates(
        dry_run_db,
        FakeClient(),  # type: ignore[arg-type]
        Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
        dry_run_args,
        [runtime_candidate],
        dry_run_stats,
    )
    process_pending_alerts(
        dry_run_db,
        FakeClient(),  # type: ignore[arg-type]
        Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
        dry_run_args,
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

    not_ready_db = sqlite3.connect(":memory:")
    not_ready_db.row_factory = sqlite3.Row
    init_db(not_ready_db)
    not_ready_stats = ScanStats()
    not_ready_candidate = MarketCandidate(
        "Cricket",
        "4",
        "runtime-event-not-ready",
        "India v Pakistan",
        "ICC T20 World Cup",
        "1.notready",
        utc_now() - timedelta(minutes=4, seconds=59),
    )
    process_candidates(
        not_ready_db,
        FakeClient(),  # type: ignore[arg-type]
        Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
        dry_run_args,
        [not_ready_candidate],
        not_ready_stats,
    )
    process_pending_alerts(
        not_ready_db,
        FakeClient(),  # type: ignore[arg-type]
        Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
        dry_run_args,
        not_ready_stats,
    )
    assert not_ready_stats.slack_alerts_sent == 0
    assert not_ready_db.execute("SELECT COUNT(*) FROM inplay_alert_state WHERE event_id = ?", (not_ready_candidate.event_id,)).fetchone()[0] == 0
    not_ready_db.close()

    sent_messages: list[str] = []

    def fake_send_slack_message(webhook_url: str, text: str) -> None:
        sent_messages.append(text)

    real_send_slack_message = globals()["send_slack_message"]
    globals()["send_slack_message"] = fake_send_slack_message
    try:
        duplicate_db = sqlite3.connect(":memory:")
        duplicate_db.row_factory = sqlite3.Row
        init_db(duplicate_db)
        send_args = argparse.Namespace(
            dry_run=False,
            market_book_batch_size=40,
            betfair_time_overdue_threshold_seconds=BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS,
            alert_delay_seconds=0,
            flashscore_alert_delay_seconds=0,
        )
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
        process_pending_alerts(
            duplicate_db,
            FakeClient(),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            send_args,
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
        process_pending_alerts(
            duplicate_db,
            FakeClient(),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            send_args,
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
        final_inplay_client = FinalInplayClient()
        process_candidates(
            inplay_db,
            final_inplay_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            send_args,
            [runtime_candidate],
            inplay_stats,
        )
        process_pending_alerts(
            inplay_db,
            final_inplay_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            send_args,
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
            "SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'candidate_suppressed_after_delay_inplay'"
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
        final_failure_client = FinalFailureClient()
        process_candidates(
            failure_db,
            final_failure_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            send_args,
            [runtime_candidate],
            failure_stats,
        )
        process_pending_alerts(
            failure_db,
            final_failure_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            send_args,
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
            "SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'candidate_suppressed_after_delay_api_error'"
        ).fetchone()[0] == 1
        failure_db.close()

        class FlashscoreFakeBetting:
            def __init__(
                self,
                status: str = "OPEN",
                inplay: bool = False,
                include_tennis: bool = True,
                include_darts: bool = True,
                market_book_sequence: list[Any] | None = None,
                same_event_extra_markets: list[dict[str, Any]] | None = None,
            ) -> None:
                self.status = status
                self.inplay = inplay
                self.include_tennis = include_tennis
                self.include_darts = include_darts
                self.market_book_sequence = market_book_sequence or []
                self.same_event_extra_markets = same_event_extra_markets or []
                self.market_book_calls: list[list[str]] = []

            def list_event_types(self, filter: Any) -> list[dict[str, Any]]:
                event_types = []
                if self.include_tennis:
                    event_types.append({"event_type": {"id": "2", "name": "Tennis"}})
                if self.include_darts:
                    event_types.append({"event_type": {"id": "15", "name": "Darts"}})
                return event_types

            def list_market_catalogue(self, filter: Any, market_projection: list[str], sort: str, max_results: int) -> list[dict[str, Any]]:
                event_type_ids = filter.get("eventTypeIds") or filter.get("event_type_ids") or []
                event_ids = filter.get("eventIds") or filter.get("event_ids") or []
                if event_ids:
                    event_id = str(event_ids[0])
                    if event_id == "bf-darts-1":
                        return [
                            {
                                "market_id": "1.darts",
                                "market_name": "Match Odds",
                                "event": {"id": "bf-darts-1", "name": "Luke Littler v Michael Smith"},
                                "event_type": {"id": "15", "name": "Darts"},
                                "competition": {"name": "Premier League Darts"},
                                "market_start_time": utc_now(),
                            },
                            *self.same_event_extra_markets,
                        ]
                    return [
                        {
                            "market_id": "1.tennis",
                            "market_name": "Match Odds",
                            "event": {"id": "bf-tennis-1", "name": "Player A v Player B"},
                            "event_type": {"id": "2", "name": "Tennis"},
                            "competition": {"name": "ATP Challenger Example"},
                            "market_start_time": utc_now(),
                        },
                        *self.same_event_extra_markets,
                    ]
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
                self.market_book_calls.append(list(market_ids))
                if self.market_book_sequence:
                    index = min(len(self.market_book_calls) - 1, len(self.market_book_sequence) - 1)
                    shape = self.market_book_sequence[index]
                else:
                    shape = (self.status, self.inplay)
                if callable(shape):
                    return [shape(market_id) for market_id in market_ids]
                if isinstance(shape, dict):
                    return [dict(shape, market_id=market_id) for market_id in market_ids]
                status, inplay = shape
                return [{"market_id": market_id, "status": status, "inplay": inplay} for market_id in market_ids]

        class FlashscoreFakeClient:
            def __init__(self, betting: FlashscoreFakeBetting) -> None:
                self.betting = betting

        flash_args = argparse.Namespace(
            dry_run=False,
            flashscore_timeout_seconds=1,
            flashscore_lookback_hours=12.0,
            flashscore_lookahead_hours=24.0,
            market_book_batch_size=40,
            alert_delay_seconds=0,
            flashscore_alert_delay_seconds=0,
            flashscore_live_verifier=lambda pending: True,
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

        class ObjectMarketBook:
            def __init__(self, market_id: str, status: str = "OPEN", inplay: bool = False) -> None:
                self.market_id = market_id
                self.status = status
                self.inplay = inplay

        def run_darts_flashscore_case(betting: FlashscoreFakeBetting, match: FlashscoreMatch = darts_flash) -> tuple[sqlite3.Connection, ScanStats, int]:
            case_db = sqlite3.connect(":memory:")
            case_db.row_factory = sqlite3.Row
            init_db(case_db)
            case_stats = ScanStats()
            before_messages = len(sent_messages)
            case_client = FlashscoreFakeClient(betting)
            process_flashscore_live_matches(
                case_db,
                case_client,  # type: ignore[arg-type]
                Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
                flash_args,
                case_stats,
                [match],
            )
            process_due_flashscore_pending_alerts(
                case_db,
                case_client,  # type: ignore[arg-type]
                Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
                flash_args,
                case_stats,
            )
            return case_db, case_stats, before_messages

        ambiguous_db = sqlite3.connect(":memory:")
        ambiguous_db.row_factory = sqlite3.Row
        init_db(ambiguous_db)
        ambiguous_candidates = [
            MarketCandidate("Darts", "15", "ambiguous-1", "Michael Smith v Dave Jones", "Premier League Darts", "1.amb1", utc_now()),
            MarketCandidate("Darts", "15", "ambiguous-2", "Michael Smith v David Jones", "Premier League Darts", "1.amb2", utc_now()),
        ]
        ambiguous_match = FlashscoreMatch(
            "Darts",
            "Smith M. v Jones D.",
            "Premier League Darts",
            "Live",
            "1-0",
            "fs-ambiguous",
            "https://www.flashscore.com/match/fs-ambiguous/",
            utc_now(),
            ("Smith M.", "Jones D."),
        )
        _, ambiguous_confidence, ambiguous = best_betfair_match(ambiguous_db, ambiguous_match, ambiguous_candidates)
        assert ambiguous
        assert ambiguous_confidence.level == "Low"
        assert ambiguous_db.execute("SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'name_match_candidate_scored'").fetchone()[0] == 2
        ambiguous_db.close()

        flash_db = sqlite3.connect(":memory:")
        flash_db.row_factory = sqlite3.Row
        init_db(flash_db)
        flash_stats = ScanStats()
        flash_betting = FlashscoreFakeBetting()
        flash_client = FlashscoreFakeClient(flash_betting)
        before_flash_messages = len(sent_messages)
        process_flashscore_live_matches(
            flash_db,
            flash_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            flash_stats,
            [tennis_flash],
        )
        process_due_flashscore_pending_alerts(
            flash_db,
            flash_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            flash_stats,
        )
        assert flash_stats.slack_alerts_sent == 1
        assert len(sent_messages) == before_flash_messages + 1
        assert any("Flashscore Live" in message for message in sent_messages[before_flash_messages:])
        assert flash_betting.market_book_calls == [["1.tennis"], ["1.tennis"]]
        assert flash_db.execute("SELECT COUNT(*) FROM inplay_alert_state WHERE trigger_source = 'flashscore_live' AND alert_sent_at IS NOT NULL").fetchone()[0] == 1
        flash_db.close()

        pending_delay_db = sqlite3.connect(":memory:")
        pending_delay_db.row_factory = sqlite3.Row
        init_db(pending_delay_db)
        pending_delay_stats = ScanStats()
        pending_delay_betting = FlashscoreFakeBetting()
        pending_delay_client = FlashscoreFakeClient(pending_delay_betting)
        before_pending_delay_messages = len(sent_messages)
        pending_delay_args = argparse.Namespace(
            dry_run=False,
            flashscore_timeout_seconds=1,
            flashscore_lookback_hours=12.0,
            flashscore_lookahead_hours=24.0,
            market_book_batch_size=40,
            alert_delay_seconds=60,
            flashscore_alert_delay_seconds=300,
        )
        process_flashscore_live_matches(
            pending_delay_db,
            pending_delay_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            pending_delay_args,
            pending_delay_stats,
            [tennis_flash],
        )
        process_due_flashscore_pending_alerts(
            pending_delay_db,
            pending_delay_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            pending_delay_args,
            pending_delay_stats,
        )
        pending_delay_state = pending_delay_db.execute(
            "SELECT alert_delay_seconds, final_verification_result, alert_sent_at FROM inplay_alert_state WHERE event_id = 'bf-tennis-1'"
        ).fetchone()
        assert pending_delay_stats.slack_alerts_sent == 0
        assert len(sent_messages) == before_pending_delay_messages
        assert pending_delay_betting.market_book_calls == [["1.tennis"]]
        assert pending_delay_state["alert_delay_seconds"] == 300
        assert pending_delay_state["final_verification_result"] == "pending_verification"
        assert pending_delay_state["alert_sent_at"] is None
        pending_delay_db.close()

        pending_visible_db = sqlite3.connect(":memory:")
        pending_visible_db.row_factory = sqlite3.Row
        init_db(pending_visible_db)
        pending_visible_stats = ScanStats()
        pending_visible_betting = FlashscoreFakeBetting()
        pending_visible_client = FlashscoreFakeClient(pending_visible_betting)
        process_flashscore_live_matches(
            pending_visible_db,
            pending_visible_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            pending_delay_args,
            pending_visible_stats,
            [tennis_flash],
        )
        pending_visible_state = pending_visible_db.execute(
            "SELECT final_verification_result, visible_in_hub, verify_after FROM inplay_alert_state WHERE event_id = 'bf-tennis-1'"
        ).fetchone()
        assert pending_visible_state["final_verification_result"] == "pending_verification"
        assert pending_visible_state["visible_in_hub"] == 1
        assert parse_datetime(pending_visible_state["verify_after"]) > utc_now()
        pending_visible_db.close()

        stale_inplay_db = sqlite3.connect(":memory:")
        stale_inplay_db.row_factory = sqlite3.Row
        init_db(stale_inplay_db)
        stale_inplay_stats = ScanStats()
        stale_inplay_betting = FlashscoreFakeBetting(market_book_sequence=[("OPEN", False), ("OPEN", True)])
        stale_inplay_client = FlashscoreFakeClient(stale_inplay_betting)
        before_stale_messages = len(sent_messages)
        process_flashscore_live_matches(
            stale_inplay_db,
            stale_inplay_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            stale_inplay_stats,
            [tennis_flash],
        )
        created_state = stale_inplay_db.execute(
            "SELECT last_seen_inplay, final_verification_result FROM inplay_alert_state WHERE event_id = 'bf-tennis-1'"
        ).fetchone()
        assert created_state["last_seen_inplay"] == 0
        assert created_state["final_verification_result"] == "pending_verification"
        process_due_flashscore_pending_alerts(
            stale_inplay_db,
            stale_inplay_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            stale_inplay_stats,
        )
        stale_inplay_state = stale_inplay_db.execute(
            "SELECT last_seen_inplay, last_seen_status, final_verification_result, final_verification_reason, alert_sent_at FROM inplay_alert_state WHERE event_id = 'bf-tennis-1'"
        ).fetchone()
        assert stale_inplay_stats.slack_alerts_sent == 0
        assert len(sent_messages) == before_stale_messages
        assert stale_inplay_betting.market_book_calls == [["1.tennis"], ["1.tennis"]]
        assert stale_inplay_state["last_seen_inplay"] == 1
        assert stale_inplay_state["last_seen_status"] == "OPEN"
        assert stale_inplay_state["final_verification_result"] == "suppressed_inplay"
        assert stale_inplay_state["final_verification_reason"] == "Betfair now in-play after 5-minute delay"
        assert stale_inplay_state["alert_sent_at"] is None
        stale_inplay_db.close()

        suspended_db = sqlite3.connect(":memory:")
        suspended_db.row_factory = sqlite3.Row
        init_db(suspended_db)
        suspended_stats = ScanStats()
        suspended_betting = FlashscoreFakeBetting(market_book_sequence=[("OPEN", False), ("SUSPENDED", False)])
        suspended_client = FlashscoreFakeClient(suspended_betting)
        before_suspended_messages = len(sent_messages)
        process_flashscore_live_matches(
            suspended_db,
            suspended_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            suspended_stats,
            [tennis_flash],
        )
        process_due_flashscore_pending_alerts(
            suspended_db,
            suspended_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            suspended_stats,
        )
        suspended_state = suspended_db.execute(
            "SELECT last_seen_inplay, last_seen_status, final_verification_result, alert_sent_at FROM inplay_alert_state WHERE event_id = 'bf-tennis-1'"
        ).fetchone()
        assert suspended_stats.slack_alerts_sent == 1
        assert len(sent_messages) == before_suspended_messages + 1
        assert suspended_state["last_seen_inplay"] == 0
        assert suspended_state["last_seen_status"] == "SUSPENDED"
        assert suspended_state["final_verification_result"] == "confirmed_not_inplay"
        assert suspended_state["alert_sent_at"] is not None
        suspended_db.close()

        not_live_at_verify_db = sqlite3.connect(":memory:")
        not_live_at_verify_db.row_factory = sqlite3.Row
        init_db(not_live_at_verify_db)
        not_live_at_verify_stats = ScanStats()
        not_live_at_verify_betting = FlashscoreFakeBetting()
        not_live_at_verify_client = FlashscoreFakeClient(not_live_at_verify_betting)
        not_live_at_verify_args = argparse.Namespace(
            dry_run=False,
            flashscore_timeout_seconds=1,
            flashscore_lookback_hours=12.0,
            flashscore_lookahead_hours=24.0,
            market_book_batch_size=40,
            alert_delay_seconds=0,
            flashscore_alert_delay_seconds=0,
            flashscore_live_verifier=lambda pending: False,
        )
        before_not_live_messages = len(sent_messages)
        process_flashscore_live_matches(
            not_live_at_verify_db,
            not_live_at_verify_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            not_live_at_verify_args,
            not_live_at_verify_stats,
            [tennis_flash],
        )
        process_due_flashscore_pending_alerts(
            not_live_at_verify_db,
            not_live_at_verify_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            not_live_at_verify_args,
            not_live_at_verify_stats,
        )
        not_live_at_verify_state = not_live_at_verify_db.execute(
            "SELECT final_verification_result, final_verification_reason, visible_in_hub, hidden_reason, alert_sent_at FROM inplay_alert_state WHERE event_id = 'bf-tennis-1'"
        ).fetchone()
        assert not_live_at_verify_stats.slack_alerts_sent == 0
        assert len(sent_messages) == before_not_live_messages
        assert not_live_at_verify_state["final_verification_result"] == "suppressed_flashscore_not_live"
        assert not_live_at_verify_state["final_verification_reason"] == "Flashscore match no longer live at verification time"
        assert not_live_at_verify_state["visible_in_hub"] == 0
        assert not_live_at_verify_state["hidden_reason"] == "flashscore_not_live_or_not_seen_latest_live_scan"
        assert not_live_at_verify_state["alert_sent_at"] is None
        assert not_live_at_verify_db.execute(
            "SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'flashscore_pending_suppressed_not_live_at_verification'"
        ).fetchone()[0] == 1
        not_live_at_verify_db.close()

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
        process_due_flashscore_pending_alerts(
            darts_db,
            FlashscoreFakeClient(FlashscoreFakeBetting()),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            darts_stats,
        )
        assert darts_stats.slack_alerts_sent == 1
        darts_db.close()

        darts_dict_inplay_db, darts_dict_inplay_stats, before_darts_dict_messages = run_darts_flashscore_case(
            FlashscoreFakeBetting(market_book_sequence=[("OPEN", False), {"status": "OPEN", "inplay": True}])
        )
        assert darts_dict_inplay_stats.slack_alerts_sent == 0
        assert len(sent_messages) == before_darts_dict_messages
        assert darts_dict_inplay_db.execute(
            "SELECT final_verification_result FROM inplay_alert_state WHERE event_id = 'bf-darts-1'"
        ).fetchone()["final_verification_result"] == "suppressed_inplay"
        assert darts_dict_inplay_db.execute(
            "SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'flashscore_final_inplay_true_no_alert'"
        ).fetchone()[0] == 1
        darts_dict_inplay_db.close()

        darts_object_inplay_db, darts_object_inplay_stats, before_darts_object_messages = run_darts_flashscore_case(
            FlashscoreFakeBetting(market_book_sequence=[("OPEN", False), lambda market_id: ObjectMarketBook(market_id, "OPEN", True)])
        )
        assert darts_object_inplay_stats.slack_alerts_sent == 0
        assert len(sent_messages) == before_darts_object_messages
        darts_object_inplay_db.close()

        darts_camel_inplay_db, darts_camel_inplay_stats, before_darts_camel_messages = run_darts_flashscore_case(
            FlashscoreFakeBetting(market_book_sequence=[("OPEN", False), {"status": "OPEN", "inPlay": True}])
        )
        assert darts_camel_inplay_stats.slack_alerts_sent == 0
        assert len(sent_messages) == before_darts_camel_messages
        darts_camel_inplay_db.close()

        darts_missing_inplay_db, darts_missing_inplay_stats, before_darts_missing_messages = run_darts_flashscore_case(
            FlashscoreFakeBetting(market_book_sequence=[("OPEN", False), {"status": "OPEN"}])
        )
        assert darts_missing_inplay_stats.slack_alerts_sent == 0
        assert len(sent_messages) == before_darts_missing_messages
        assert darts_missing_inplay_db.execute(
            "SELECT final_verification_result, last_seen_inplay FROM inplay_alert_state WHERE event_id = 'bf-darts-1'"
        ).fetchone()["final_verification_result"] == "suppressed_unknown"
        assert darts_missing_inplay_db.execute(
            "SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'flashscore_final_inplay_unknown_no_alert'"
        ).fetchone()[0] >= 1
        darts_missing_inplay_db.close()

        other_market = {
            "market_id": "1.darts.alt",
            "market_name": "Match Odds",
            "event": {"id": "bf-darts-1", "name": "Luke Littler v Michael Smith"},
            "event_type": {"id": "15", "name": "Darts"},
            "competition": {"name": "Premier League Darts"},
            "market_start_time": utc_now(),
        }
        darts_other_market_db, darts_other_market_stats, before_darts_other_messages = run_darts_flashscore_case(
            FlashscoreFakeBetting(
                market_book_sequence=[
                    ("OPEN", False),
                    {"status": "OPEN", "inplay": False},
                    lambda market_id: {"market_id": market_id, "status": "OPEN", "inplay": market_id == "1.darts.alt"},
                ],
                same_event_extra_markets=[other_market],
            )
        )
        assert darts_other_market_stats.slack_alerts_sent == 0
        assert len(sent_messages) == before_darts_other_messages
        assert darts_other_market_db.execute(
            "SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'flashscore_same_event_other_market_inplay_no_alert'"
        ).fetchone()[0] == 1
        darts_other_market_db.close()

        class AmbiguousDartsBetting(FlashscoreFakeBetting):
            def list_market_catalogue(self, filter: Any, market_projection: list[str], sort: str, max_results: int) -> list[dict[str, Any]]:
                event_type_ids = filter.get("eventTypeIds") or filter.get("event_type_ids") or []
                if str(event_type_ids[0]) == "15":
                    return [
                        {
                            "market_id": "1.amb-darts-1",
                            "market_name": "Match Odds",
                            "event": {"id": "bf-amb-darts-1", "name": "Luke Littler v Michael Smith"},
                            "event_type": {"id": "15", "name": "Darts"},
                            "competition": {"name": "Premier League Darts"},
                            "market_start_time": utc_now(),
                        },
                        {
                            "market_id": "1.amb-darts-2",
                            "market_name": "Match Odds",
                            "event": {"id": "bf-amb-darts-2", "name": "Luke Littler v Michael Smith"},
                            "event_type": {"id": "15", "name": "Darts"},
                            "competition": {"name": "Premier League Darts"},
                            "market_start_time": utc_now(),
                        },
                    ]
                return super().list_market_catalogue(filter, market_projection, sort, max_results)

        ambiguous_cycle_db, ambiguous_cycle_stats, before_ambiguous_cycle_messages = run_darts_flashscore_case(AmbiguousDartsBetting())
        assert ambiguous_cycle_stats.slack_alerts_sent == 0
        assert len(sent_messages) == before_ambiguous_cycle_messages
        assert ambiguous_cycle_db.execute(
            "SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'name_match_ambiguous_no_alert'"
        ).fetchone()[0] == 1
        ambiguous_cycle_db.close()

        suppress_db = sqlite3.connect(":memory:")
        suppress_db.row_factory = sqlite3.Row
        init_db(suppress_db)
        suppress_stats = ScanStats()
        suppress_betting = FlashscoreFakeBetting(inplay=True)
        suppress_client = FlashscoreFakeClient(suppress_betting)
        before_suppress_messages = len(sent_messages)
        process_flashscore_live_matches(
            suppress_db,
            suppress_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            suppress_stats,
            [tennis_flash],
        )
        process_due_flashscore_pending_alerts(
            suppress_db,
            suppress_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            suppress_stats,
        )
        assert suppress_stats.slack_alerts_sent == 0
        assert len(sent_messages) == before_suppress_messages
        suppress_state = suppress_db.execute(
            "SELECT last_seen_inplay, final_verification_result FROM inplay_alert_state WHERE event_id = 'bf-tennis-1'"
        ).fetchone()
        assert suppress_state["last_seen_inplay"] == 1
        assert suppress_state["final_verification_result"] == "suppressed_inplay"
        assert suppress_betting.market_book_calls == [["1.tennis"]]
        suppress_db.close()

        closed_db = sqlite3.connect(":memory:")
        closed_db.row_factory = sqlite3.Row
        init_db(closed_db)
        closed_stats = ScanStats()
        closed_betting = FlashscoreFakeBetting(market_book_sequence=[("OPEN", False), ("CLOSED", False)])
        closed_client = FlashscoreFakeClient(closed_betting)
        before_closed_messages = len(sent_messages)
        process_flashscore_live_matches(
            closed_db,
            closed_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            closed_stats,
            [tennis_flash],
        )
        process_due_flashscore_pending_alerts(
            closed_db,
            closed_client,  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            closed_stats,
        )
        assert closed_stats.slack_alerts_sent == 0
        assert len(sent_messages) == before_closed_messages
        closed_state = closed_db.execute(
            "SELECT last_seen_status, final_verification_result FROM inplay_alert_state WHERE event_id = 'bf-tennis-1'"
        ).fetchone()
        assert closed_state["last_seen_status"] == "CLOSED"
        assert closed_state["final_verification_result"] == "suppressed_closed"
        assert closed_betting.market_book_calls == [["1.tennis"], ["1.tennis"]]
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
        process_due_flashscore_pending_alerts(
            no_match_db,
            FlashscoreFakeClient(FlashscoreFakeBetting(include_tennis=False, include_darts=False)),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            no_match_stats,
        )
        assert no_match_stats.slack_alerts_sent == 0
        assert no_match_db.execute("SELECT COUNT(*) FROM inplay_scan_logs WHERE event_type = 'flashscore_live_no_betfair_match'").fetchone()[0] >= 1
        no_match_db.close()

        low_confidence_flash = FlashscoreMatch(
            "Darts",
            "Luke Littler v Peter Wright",
            "Premier League Darts",
            "Live - Set 1",
            "1-0",
            "fs-low-confidence",
            "https://www.flashscore.com/match/fs-low-confidence/",
            utc_now(),
            ("Luke Littler", "Peter Wright"),
        )
        low_confidence_db = sqlite3.connect(":memory:")
        low_confidence_db.row_factory = sqlite3.Row
        init_db(low_confidence_db)
        low_confidence_stats = ScanStats()
        process_flashscore_live_matches(
            low_confidence_db,
            FlashscoreFakeClient(FlashscoreFakeBetting()),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            low_confidence_stats,
            [low_confidence_flash],
        )
        process_due_flashscore_pending_alerts(
            low_confidence_db,
            FlashscoreFakeClient(FlashscoreFakeBetting()),  # type: ignore[arg-type]
            Config("", "", "", "", "https://hooks.slack.com/services/test", "test"),
            flash_args,
            low_confidence_stats,
        )
        low_confidence_state = low_confidence_db.execute(
            "SELECT last_seen_inplay, betfair_last_seen_inplay, match_confidence, final_verification_reason FROM inplay_alert_state WHERE event_id = 'bf-darts-1'"
        ).fetchone()
        assert low_confidence_stats.slack_alerts_sent == 0
        assert low_confidence_state["last_seen_inplay"] is None
        assert low_confidence_state["betfair_last_seen_inplay"] is None
        assert low_confidence_state["match_confidence"] == "Low"
        assert low_confidence_state["final_verification_reason"] == "low_confidence"
        low_confidence_db.close()
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
    parser.add_argument("--betfair-time-overdue-threshold-seconds", type=int, default=BETFAIR_TIME_OVERDUE_THRESHOLD_SECONDS)
    parser.add_argument("--overdue-minutes", type=float, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--max-results", type=int, default=1000)
    parser.add_argument("--market-book-batch-size", type=int, default=DEFAULT_MARKET_BOOK_BATCH_SIZE)
    parser.add_argument("--alert-delay-seconds", type=int, default=BETFAIR_TIME_ALERT_DELAY_SECONDS)
    parser.add_argument("--flashscore-alert-delay-seconds", type=int, default=FLASHSCORE_ALERT_DELAY_SECONDS)
    parser.add_argument("--run-lock-stale-seconds", type=int, default=DEFAULT_RUN_LOCK_STALE_SECONDS)
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
