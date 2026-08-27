from __future__ import annotations

import json
import logging
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import permutations
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse, urlunparse
from zoneinfo import ZoneInfo

import betfairlightweight
from betfairlightweight.filters import market_filter
from rapidfuzz import fuzz

from app.config import CONFIG_DIR, OUTPUT_DIR, child_environment, ensure_runtime_dirs
from app.golf_betfair_check import betfair_login


logger = logging.getLogger("uvicorn.error")
UK_TIMEZONE = ZoneInfo("Europe/London")
TENNIS_EVENT_TYPE_ID = "2"
SCRIPT_ID = "tennis-challenger-watcher"
CONFIG_PATH = CONFIG_DIR / "tennis_challenger_watcher.json"
STATE_PATH = OUTPUT_DIR / "tennis_challenger_watcher_state.json"
ALLOWED_CATEGORY_SEGMENTS = {"challenger-men-singles", "challenger-men-doubles"}


def utc_timestamp(value: datetime | None = None) -> str:
    return (value or datetime.now(timezone.utc)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path, fallback: Any) -> Any:
    try:
        if not path.exists():
            return fallback
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.warning("tennis_challenger_json_read_failed path=%s", path, exc_info=True)
        return fallback


def atomic_write_json(path: Path, data: Any) -> None:
    ensure_runtime_dirs()
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def normalize_tournament_url(value: str) -> str:
    raw = str(value or "").strip()
    parsed = urlparse(raw)
    hostname = (parsed.hostname or "").casefold()
    if parsed.scheme != "https" or hostname not in {"flashscore.com", "www.flashscore.com"}:
        raise ValueError("Tournament links must be HTTPS Flashscore links.")
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 3 or parts[0].casefold() != "tennis" or parts[1].casefold() not in ALLOWED_CATEGORY_SEGMENTS:
        raise ValueError("Only Flashscore ATP Challenger men's singles or doubles tournament links are supported.")
    clean_path = "/" + "/".join(parts[:3]) + "/"
    return urlunparse(("https", "www.flashscore.com", clean_path, "", "", ""))


def parse_tournament_links(text: str) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    for raw in re.split(r"[\r\n]+", str(text or "")):
        if not raw.strip():
            continue
        normalized = normalize_tournament_url(raw)
        if normalized not in seen:
            links.append(normalized)
            seen.add(normalized)
    return links


def tournament_label_from_url(url: str) -> str:
    parts = [part for part in urlparse(url).path.split("/") if part]
    if len(parts) < 3:
        return url
    event = parts[2].replace("-", " ").title()
    draw = "Doubles" if "doubles" in parts[1] else "Singles"
    return f"{event} ({draw})"


def read_config() -> dict[str, Any]:
    data = read_json(CONFIG_PATH, {})
    return data if isinstance(data, dict) else {}


def save_config(links: list[str]) -> dict[str, Any]:
    previous = read_config()
    data = {
        "tournament_links": links,
        "last_saved_at": utc_timestamp(),
        "revision": int(previous.get("revision", 0) or 0) + 1,
    }
    atomic_write_json(CONFIG_PATH, data)
    return data


def read_state() -> dict[str, Any]:
    data = read_json(STATE_PATH, {})
    return data if isinstance(data, dict) else {}


def uk_time_label(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(UK_TIMEZONE).strftime("%d %b %Y %H:%M:%S")
    except ValueError:
        return raw


def score_label(match: dict[str, Any]) -> str:
    sets = match.get("sets", []) if isinstance(match.get("sets"), list) else []
    set_parts = [f"{item.get('home', '')}-{item.get('away', '')}" for item in sets if item.get("home", "") != "" and item.get("away", "") != ""]
    aggregate = match.get("set_score", {}) if isinstance(match.get("set_score"), dict) else {}
    aggregate_text = ""
    if aggregate.get("home", "") != "" and aggregate.get("away", "") != "":
        aggregate_text = f"Sets {aggregate['home']}-{aggregate['away']}"
    detail = " ".join(set_parts)
    if aggregate_text and detail:
        return f"{aggregate_text} · {detail}"
    return aggregate_text or detail or "-"


def current_score_label(match: dict[str, Any]) -> str:
    base = score_label(match)
    game = match.get("current_game", {}) if isinstance(match.get("current_game"), dict) else {}
    points = match.get("current_points", {}) if isinstance(match.get("current_points"), dict) else {}
    additions: list[str] = []
    if game.get("home", "") != "" and game.get("away", "") != "":
        additions.append(f"Game {game['home']}-{game['away']}")
    if points.get("home", "") != "" and points.get("away", "") != "":
        additions.append(f"Points {points['home']}-{points['away']}")
    return " · ".join([part for part in [base if base != "-" else "", *additions] if part]) or "-"


def is_future_scheduled_match(match: dict[str, Any], now: datetime | None = None) -> bool:
    if str(match.get("status", "")) != "scheduled":
        return False
    reference = (now or datetime.now(timezone.utc)).astimezone(UK_TIMEZONE)
    scheduled_at = str(match.get("scheduled_at", "") or "").strip()
    if scheduled_at:
        try:
            parsed = datetime.fromisoformat(scheduled_at.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(UK_TIMEZONE).date() > reference.date()
        except ValueError:
            pass

    date_match = re.search(r"\b(\d{1,2})\.(\d{1,2})\.", str(match.get("start_time", "") or ""))
    if not date_match:
        return False
    day, month = map(int, date_match.groups())
    year = reference.year
    try:
        candidate = datetime(year, month, day, tzinfo=UK_TIMEZONE)
    except ValueError:
        return False
    if candidate.date() < reference.date() and reference.month == 12 and month == 1:
        candidate = candidate.replace(year=year + 1)
    return candidate.date() > reference.date()


def match_start_sort_key(match: dict[str, Any]) -> tuple[float, str]:
    player = str(match.get("player1", "")).casefold()
    scheduled_at = str(match.get("scheduled_at", "") or "").strip()
    if scheduled_at:
        try:
            parsed = datetime.fromisoformat(scheduled_at.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp(), player
        except ValueError:
            pass
    display_time = str(match.get("start_time", "") or match.get("display_status", ""))
    fallback = re.search(r"\b(\d{1,2})\.(\d{1,2})\.\s+(\d{1,2}):(\d{2})", display_time)
    if fallback:
        day, month, hour, minute = map(int, fallback.groups())
        try:
            parsed = datetime(datetime.now(UK_TIMEZONE).year, month, day, hour, minute, tzinfo=UK_TIMEZONE)
            return parsed.timestamp(), player
        except ValueError:
            pass
    return float("inf"), player


def group_matches_by_tournament(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for match in matches:
        tournament = str(match.get("tournament", "") or "ATP Challenger")
        grouped.setdefault(tournament, []).append(match)
    return [
        {"tournament": tournament, "matches": sorted(grouped[tournament], key=match_start_sort_key)}
        for tournament in sorted(grouped, key=str.casefold)
    ]


def watcher_context() -> dict[str, Any]:
    config = read_config()
    state = read_state()
    matches: list[dict[str, Any]] = []
    for raw in state.get("matches", []) or []:
        if not isinstance(raw, dict):
            continue
        item = dict(raw)
        item["score_label"] = current_score_label(item)
        item["last_checked_label"] = uk_time_label(item.get("last_checked_at"))
        matches.append(item)
    status_order = {"live": 0, "scheduled": 1, "suspended": 2, "error": 3, "finished": 4}
    matches.sort(
        key=lambda item: (
            status_order.get(str(item.get("status", "unknown")), 3),
            str(item.get("tournament", "")),
            str(item.get("start_time", "")),
            str(item.get("player1", "")),
        )
    )
    future_matches = [item for item in matches if is_future_scheduled_match(item)]
    future_ids = {str(item.get("id", "")) for item in future_matches}
    today_matches = [item for item in matches if str(item.get("id", "")) not in future_ids]
    alert_rows = []
    for alert in list(state.get("alerts", []) or [])[-50:][::-1]:
        if not isinstance(alert, dict):
            continue
        alert_rows.append({**alert, "timestamp": uk_time_label(alert.get("timestamp"))})
    return {
        "links": list(config.get("tournament_links", []) or []),
        "links_text": "\n".join(config.get("tournament_links", []) or []),
        "configured": bool(config.get("tournament_links")),
        "last_saved_at": uk_time_label(config.get("last_saved_at")),
        "watcher": state,
        "matches": matches,
        "today_matches": today_matches,
        "future_matches": future_matches,
        "today_match_groups": group_matches_by_tournament(today_matches),
        "future_match_groups": group_matches_by_tournament(future_matches),
        "alerts": alert_rows,
        "counts": {
            "total": len(matches),
            "scheduled": sum(item.get("status") == "scheduled" for item in matches),
            "live": sum(item.get("status") == "live" for item in matches),
            "finished": sum(item.get("status") == "finished" for item in matches),
            "errors": sum(item.get("status") == "error" for item in matches),
        },
        "last_sweep_label": uk_time_label(state.get("last_sweep_at")),
    }


def normalize_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    normalized = "".join(character for character in normalized if not unicodedata.combining(character))
    normalized = normalized.casefold().replace("&", " ").replace("/", " ")
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def participant_surnames(value: str) -> list[str]:
    surnames: list[str] = []
    for participant in re.split(r"\s*(?:/|&|\+|\band\b)\s*", str(value or ""), flags=re.IGNORECASE):
        tokens = normalize_name(participant).split()
        useful = [token for token in tokens if len(token) > 1]
        if useful:
            surnames.append(" ".join(useful))
    return surnames


def split_betfair_players(value: str) -> tuple[str, str] | None:
    parts = re.split(r"\s+(?:v|vs|versus)\.?\s+", str(value or ""), maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return None
    return parts[0].strip(), parts[1].strip()


def participant_match_score(flashscore_name: str, betfair_name: str) -> float:
    source_surnames = participant_surnames(flashscore_name)
    target_surnames = participant_surnames(betfair_name)
    if not source_surnames or len(source_surnames) != len(target_surnames):
        return 0.0

    def surname_score(source: str, target: str) -> float:
        if source == target:
            return 100.0
        source_tokens = set(source.split())
        target_tokens = set(target.split())
        if source_tokens == target_tokens:
            return 100.0
        if source_tokens and target_tokens and (source_tokens < target_tokens or target_tokens < source_tokens):
            return 97.0
        return float(fuzz.ratio(source, target))

    # A doubles pair is a two-person team, but Flashscore and Betfair do not
    # always list the partners in the same order. Score every one-to-one
    # assignment so one common surname cannot accidentally satisfy both names.
    best_score = 0.0
    for ordered_targets in permutations(target_surnames):
        score = sum(
            surname_score(source, target)
            for source, target in zip(source_surnames, ordered_targets)
        ) / len(source_surnames)
        best_score = max(best_score, score)
    return best_score


@dataclass(frozen=True)
class BetfairTennisEvent:
    event_id: str
    event_name: str
    open_date: datetime | None


def _object_value(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_betfair_events(results: Iterable[Any]) -> list[BetfairTennisEvent]:
    events: list[BetfairTennisEvent] = []
    for result in results or []:
        event = _object_value(result, "event", {})
        event_id = str(_object_value(event, "id", "") or "").strip()
        event_name = str(_object_value(event, "name", "") or "").strip()
        if event_id and event_name:
            events.append(
                BetfairTennisEvent(
                    event_id=event_id,
                    event_name=event_name,
                    open_date=_parse_datetime(_object_value(event, "open_date", None)),
                )
            )
    return events


def create_betfair_client() -> betfairlightweight.APIClient:
    return betfair_login(child_environment())


def fetch_upcoming_betfair_tennis_events(
    client: betfairlightweight.APIClient | None = None,
    attempts: int = 3,
) -> list[BetfairTennisEvent]:
    owns_client = client is None
    active_client = client or create_betfair_client()
    try:
        now = datetime.now(timezone.utc)
        event_filter = market_filter(
            event_type_ids=[TENNIS_EVENT_TYPE_ID],
            market_start_time={
                "from": (now - timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": (now + timedelta(days=8)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return parse_betfair_events(active_client.betting.list_events(filter=event_filter))
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "tennis_challenger_event_list_retry attempt=%s/%s error=%s",
                    attempt,
                    attempts,
                    exc,
                )
                if attempt < attempts:
                    time.sleep(0.5 * attempt)
        assert last_error is not None
        raise last_error
    finally:
        if owns_client:
            try:
                active_client.logout()
            except Exception:
                logger.warning("tennis_challenger_betfair_logout_failed", exc_info=True)


def match_betfair_event(match: dict[str, Any], events: Iterable[BetfairTennisEvent]) -> tuple[BetfairTennisEvent | None, float, str]:
    player1 = str(match.get("player1", "") or "")
    player2 = str(match.get("player2", "") or "")
    scored: list[tuple[float, BetfairTennisEvent]] = []
    for event in events:
        sides = split_betfair_players(event.event_name)
        if sides is None:
            continue
        forward = (participant_match_score(player1, sides[0]) + participant_match_score(player2, sides[1])) / 2
        reverse = (participant_match_score(player1, sides[1]) + participant_match_score(player2, sides[0])) / 2
        scored.append((max(forward, reverse), event))
    if not scored:
        return None, 0.0, "No upcoming Betfair tennis events were returned."
    scored.sort(key=lambda item: (-item[0], item[1].event_id))
    best_score, best = scored[0]
    if best_score < 86:
        return None, best_score, f"No confident Betfair event match (best score {best_score:.0f})."
    if len(scored) > 1 and scored[1][0] >= best_score - 4:
        return None, best_score, f"Betfair event match is ambiguous (best score {best_score:.0f})."
    return best, best_score, "Matched"
