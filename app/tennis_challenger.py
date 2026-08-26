from __future__ import annotations

import json
import logging
import re
import threading
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
GAME_BETTING_PATH = OUTPUT_DIR / "tennis_challenger_game_betting.json"
ALLOWED_CATEGORY_SEGMENTS = {"challenger-men-singles", "challenger-men-doubles"}
GAME_MARKET_PREFIX = "GAME_BY_GAME_"


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


def read_game_betting() -> dict[str, Any]:
    data = read_json(GAME_BETTING_PATH, {})
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


def watcher_context() -> dict[str, Any]:
    config = read_config()
    state = read_state()
    service = globals().get("game_betting_check_service")
    game_check = service.snapshot() if service is not None else read_game_betting()
    check_rows = {
        str(row.get("match_id", "")): row
        for row in game_check.get("rows", [])
        if isinstance(row, dict) and row.get("match_id")
    }
    matches: list[dict[str, Any]] = []
    for raw in state.get("matches", []) or []:
        if not isinstance(raw, dict):
            continue
        item = dict(raw)
        item["score_label"] = current_score_label(item)
        item["last_checked_label"] = uk_time_label(item.get("last_checked_at"))
        item["game_betting"] = check_rows.get(str(item.get("id", "")), {})
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
        "alerts": alert_rows,
        "counts": {
            "total": len(matches),
            "scheduled": sum(item.get("status") == "scheduled" for item in matches),
            "live": sum(item.get("status") == "live" for item in matches),
            "finished": sum(item.get("status") == "finished" for item in matches),
            "errors": sum(item.get("status") == "error" for item in matches),
        },
        "last_sweep_label": uk_time_label(state.get("last_sweep_at")),
        "game_check": {
            **game_check,
            "started_at_label": uk_time_label(game_check.get("started_at")),
            "completed_at_label": uk_time_label(game_check.get("completed_at")),
        },
    }


def normalize_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    normalized = "".join(character for character in normalized if not unicodedata.combining(character))
    normalized = normalized.casefold().replace("&", " ").replace("/", " ")
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def participant_surnames(value: str) -> list[str]:
    surnames: list[str] = []
    for participant in re.split(r"\s*(?:/|&)\s*", str(value or "")):
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
    target = normalize_name(betfair_name)
    if not source_surnames or not target:
        return 0.0
    token_scores = []
    for surname in source_surnames:
        if surname in target:
            token_scores.append(100.0)
        else:
            token_scores.append(float(fuzz.token_set_ratio(surname, target)))
    return sum(token_scores) / len(token_scores)


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


def market_type(catalogue: Any) -> str:
    description = _object_value(catalogue, "description", {})
    return str(_object_value(description, "market_type", "") or _object_value(description, "marketType", "") or "").strip().upper()


def is_game_market(catalogue: Any) -> bool:
    kind = market_type(catalogue)
    name = str(_object_value(catalogue, "market_name", "") or "").strip()
    return kind.startswith(GAME_MARKET_PREFIX) or bool(re.search(r"\b(?:set\s*\d+\s+)?game\s*\d+\b", name, re.IGNORECASE))


def perform_game_betting_check(matches: list[dict[str, Any]]) -> dict[str, Any]:
    started_at = utc_timestamp()
    scheduled = [match for match in matches if str(match.get("status", "")) == "scheduled"]
    if not scheduled:
        return {
            "status": "complete",
            "started_at": started_at,
            "completed_at": utc_timestamp(),
            "rows": [],
            "summary": "clear",
            "message": "There are no scheduled matches in scope.",
        }

    environment = child_environment()
    client = betfair_login(environment)
    try:
        now = datetime.now(timezone.utc)
        event_results = client.betting.list_events(
            filter=market_filter(
                event_type_ids=[TENNIS_EVENT_TYPE_ID],
                market_start_time={
                    "from": (now - timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "to": (now + timedelta(days=8)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
            )
        )
        events = parse_betfair_events(event_results)
        rows: list[dict[str, Any]] = []
        event_matches: dict[str, BetfairTennisEvent] = {}
        for match in scheduled:
            event, score, reason = match_betfair_event(match, events)
            base = {
                "match_id": str(match.get("id", "")),
                "match": f"{match.get('player1', '?')} v {match.get('player2', '?')}",
                "tournament": str(match.get("tournament", "")),
                "betfair_event_id": event.event_id if event else "",
                "betfair_event_name": event.event_name if event else "",
                "match_score": round(score, 1),
            }
            if event is None:
                rows.append({**base, "status": "event_not_found", "game_market_count": None, "message": reason})
            else:
                event_matches[event.event_id] = event
                rows.append({**base, "status": "pending", "game_market_count": None, "message": "Checking markets."})

        catalogues_by_event: dict[str, list[Any]] = {event_id: [] for event_id in event_matches}
        event_ids = list(event_matches)
        for index in range(0, len(event_ids), 10):
            chunk = event_ids[index : index + 10]
            catalogues = client.betting.list_market_catalogue(
                filter=market_filter(event_ids=chunk),
                market_projection=["EVENT", "MARKET_DESCRIPTION", "MARKET_START_TIME"],
                max_results=1000,
                sort="FIRST_TO_START",
            )
            for catalogue in catalogues or []:
                event = _object_value(catalogue, "event", {})
                event_id = str(_object_value(event, "id", "") or "")
                if event_id in catalogues_by_event:
                    catalogues_by_event[event_id].append(catalogue)

        for row in rows:
            event_id = row.get("betfair_event_id", "")
            if not event_id:
                continue
            game_markets = [catalogue for catalogue in catalogues_by_event.get(event_id, []) if is_game_market(catalogue)]
            names = sorted({str(_object_value(item, "market_name", "") or "") for item in game_markets})
            row.update(
                {
                    "status": "needs_action" if game_markets else "clear",
                    "game_market_count": len(game_markets),
                    "market_names": names[:20],
                    "message": f"{len(game_markets)} game market(s) still present." if game_markets else "Game betting deleted.",
                }
            )
    finally:
        try:
            client.logout()
        except Exception:
            logger.warning("tennis_challenger_betfair_logout_failed", exc_info=True)

    needs_action = sum(row.get("status") == "needs_action" for row in rows)
    unresolved = sum(row.get("status") == "event_not_found" for row in rows)
    return {
        "status": "complete",
        "started_at": started_at,
        "completed_at": utc_timestamp(),
        "summary": "needs_action" if needs_action else "attention" if unresolved else "clear",
        "needs_action_count": needs_action,
        "unresolved_count": unresolved,
        "rows": rows,
        "message": f"Checked {len(scheduled)} scheduled match(es).",
    }


class TennisGameBettingCheckService:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._running = False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            running = self._running
        data = read_game_betting()
        if running:
            data["status"] = "running"
        elif data.get("status") == "running":
            data.update({"status": "failed", "error": "The previous check was interrupted."})
        data.setdefault("status", "idle")
        data.setdefault("rows", [])
        return data

    def start(self) -> bool:
        with self._lock:
            if self._running:
                return False
            self._running = True
        atomic_write_json(
            GAME_BETTING_PATH,
            {"status": "running", "started_at": utc_timestamp(), "completed_at": "", "rows": [], "error": ""},
        )
        threading.Thread(target=self._run, name="tennis-challenger-game-betting-check", daemon=True).start()
        return True

    def _run(self) -> None:
        try:
            matches = list(read_state().get("matches", []) or [])
            result = perform_game_betting_check(matches)
        except Exception as exc:
            logger.exception("tennis_challenger_game_betting_check_failed")
            result = {
                "status": "failed",
                "started_at": read_game_betting().get("started_at", ""),
                "completed_at": utc_timestamp(),
                "rows": [],
                "error": str(exc),
            }
        try:
            atomic_write_json(GAME_BETTING_PATH, result)
        finally:
            with self._lock:
                self._running = False


game_betting_check_service = TennisGameBettingCheckService()
