from __future__ import annotations

import csv
import difflib
import json
import logging
import os
import re
import threading
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import betfairlightweight
import requests
from betfairlightweight.filters import market_filter

from app.config import CONFIG_DIR, OUTPUT_DIR, PROJECT_ROOT, child_environment, ensure_runtime_dirs


logger = logging.getLogger("uvicorn.error")
GOLF_EVENT_TYPE_ID = "3"
GOLF_MARKET_TYPES = ["OUTRIGHT", "WIN", "WINNER", "TOURNAMENT_WINNER", "OUTRIGHT_WINNER"]
GOLF_CONFIG_PATH = CONFIG_DIR / "golf_field_checker.json"
GOLF_STATE_DIR = OUTPUT_DIR / "golf_field_checker"
RESULT_PATH = OUTPUT_DIR / "golf_betfair_check.json"
NAME_OVERRIDES_PATH = PROJECT_ROOT / "scripts" / "name_overrides.csv"
EVENT_MATCH_THRESHOLD = 0.48
GENERIC_EVENT_WORDS = {
    "golf",
    "pga",
    "tour",
    "championship",
    "championships",
    "open",
    "presented",
    "by",
    "the",
    "2025",
    "2026",
    "2027",
}
IGNORED_RUNNER_NAMES = {
    "any other player",
    "any other golfer",
    "field",
    "the field",
}


@dataclass
class BetfairEvent:
    event_id: str
    event_name: str
    market_id: str
    market_name: str
    runners: list[Any]


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.exception("golf_betfair_json_read_failed path=%s", path)
        return {}
    return data if isinstance(data, dict) else {}


def _write_result(data: dict[str, Any]) -> None:
    ensure_runtime_dirs()
    temporary = RESULT_PATH.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(RESULT_PATH)


def competition_hint(url: str) -> str:
    segments = [unquote(segment).strip() for segment in urlparse(url).path.split("/") if segment.strip()]
    ignored = {"field", "entries", "entry-list", "entry_list"}
    for segment in reversed(segments):
        lowered = segment.lower()
        if lowered in ignored or re.fullmatch(r"20\d{2}", lowered):
            continue
        if re.fullmatch(r"[rs]\d{6,}", lowered):
            continue
        return re.sub(r"[-_]+", " ", segment).strip()
    return ""


def normalize_event_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(character for character in normalized if not unicodedata.combining(character))
    normalized = normalized.lower()
    normalized = re.sub(r"\b20\d{2}\b", " ", normalized)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def event_match_score(hint: str, event_name: str) -> float:
    normalized_hint = normalize_event_name(hint)
    normalized_event = normalize_event_name(event_name)
    if not normalized_hint or not normalized_event:
        return 0.0
    sequence_score = difflib.SequenceMatcher(None, normalized_hint, normalized_event).ratio()
    hint_tokens = set(normalized_hint.split()) - GENERIC_EVENT_WORDS
    event_tokens = set(normalized_event.split()) - GENERIC_EVENT_WORDS
    if hint_tokens:
        token_score = len(hint_tokens & event_tokens) / len(hint_tokens)
    else:
        token_score = len(set(normalized_hint.split()) & set(normalized_event.split())) / max(
            1, len(set(normalized_hint.split()))
        )
    containment_bonus = 0.12 if normalized_hint in normalized_event or normalized_event in normalized_hint else 0.0
    return min(1.0, (0.55 * sequence_score) + (0.45 * token_score) + containment_bonus)


def best_event_match(hint: str, events: list[BetfairEvent], used_event_ids: set[str]) -> tuple[BetfairEvent | None, float]:
    candidates = [
        (event_match_score(hint, event.event_name), event)
        for event in events
        if event.event_id not in used_event_ids
    ]
    if not candidates:
        return None, 0.0
    candidates.sort(key=lambda item: item[0], reverse=True)
    score, event = candidates[0]
    if score < EVENT_MATCH_THRESHOLD:
        return None, score
    if (
        len(candidates) > 1
        and normalize_event_name(hint) != normalize_event_name(event.event_name)
        and score - candidates[1][0] < 0.08
    ):
        return None, score
    return event, score


def _base_normalize_player(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(character for character in normalized if not unicodedata.combining(character))
    normalized = re.sub(r"\([^)]*\)", " ", normalized.lower())
    if "," in normalized:
        parts = [part.strip() for part in normalized.split(",", 1)]
        normalized = f"{parts[1]} {parts[0]}"
    normalized = re.sub(r"[-–—·.']", " ", normalized)
    tokens = [token for token in re.sub(r"\s+", " ", normalized).strip().split() if token]
    if tokens and tokens[-1] in {"jr", "jnr", "sr", "ii", "iii", "iv", "v"}:
        tokens.pop()
    tokens = [token for token in tokens if not (len(token) == 1 and token.isalpha())]
    return " ".join(tokens)


def load_name_aliases(path: Path = NAME_OVERRIDES_PATH) -> dict[str, str]:
    if not path.exists():
        return {}
    aliases: dict[str, str] = {}
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                betfair_name = str(row.get("Betfair", "") or "").strip()
                official_name = str(row.get("DataGolf", "") or "").strip()
                target = _base_normalize_player(betfair_name)
                if target:
                    aliases[target] = target
                    aliases[_base_normalize_player(official_name)] = target
    except OSError:
        logger.exception("golf_name_overrides_read_failed path=%s", path)
    return aliases


def player_key(value: str, aliases: dict[str, str] | None = None) -> str:
    normalized = _base_normalize_player(value)
    if aliases:
        normalized = aliases.get(normalized, normalized)
    tokens = normalized.split()
    if not tokens:
        return ""
    if len(tokens) == 1:
        return tokens[0]
    surname_prefixes = {"de", "del", "da", "di", "la", "le", "van", "von", "der"}
    surname = " ".join(tokens[-2:]) if len(tokens) >= 3 and tokens[-2] in surname_prefixes else tokens[-1]
    return f"{tokens[0][0]} {surname}"


def compare_player_lists(official_names: list[str], betfair_names: list[str]) -> dict[str, Any]:
    aliases = load_name_aliases()
    official: dict[str, str] = {}
    betfair: dict[str, str] = {}
    for name in official_names:
        key = player_key(name, aliases)
        if key:
            official.setdefault(key, name)
    for name in betfair_names:
        if _base_normalize_player(name) in IGNORED_RUNNER_NAMES:
            continue
        key = player_key(name, aliases)
        if key:
            betfair.setdefault(key, name)
    official_only = [official[key] for key in sorted(official.keys() - betfair.keys())]
    betfair_only = [betfair[key] for key in sorted(betfair.keys() - official.keys())]
    return {
        "matching": not official_only and not betfair_only,
        "official_only": official_only,
        "betfair_only": betfair_only,
        "official_count": len(official),
        "betfair_count": len(betfair),
    }


def _preferred_market_score(catalogue: Any) -> tuple[int, int]:
    name = normalize_event_name(str(getattr(catalogue, "market_name", "") or ""))
    preferred = 2 if name in {"winner", "tournament winner", "outright winner"} else 1 if "winner" in name else 0
    return preferred, len(getattr(catalogue, "runners", None) or [])


def catalogue_events(catalogues: list[Any]) -> list[BetfairEvent]:
    chosen: dict[str, Any] = {}
    for catalogue in catalogues:
        event = getattr(catalogue, "event", None)
        event_id = str(getattr(event, "id", "") or "")
        if not event_id:
            continue
        existing = chosen.get(event_id)
        if existing is None or _preferred_market_score(catalogue) > _preferred_market_score(existing):
            chosen[event_id] = catalogue
    return [
        BetfairEvent(
            event_id=event_id,
            event_name=str(getattr(catalogue.event, "name", "") or ""),
            market_id=str(getattr(catalogue, "market_id", "") or ""),
            market_name=str(getattr(catalogue, "market_name", "") or ""),
            runners=list(getattr(catalogue, "runners", None) or []),
        )
        for event_id, catalogue in chosen.items()
    ]


def list_betfair_events(client: betfairlightweight.APIClient) -> list[BetfairEvent]:
    now = datetime.now(timezone.utc)
    catalogues = client.betting.list_market_catalogue(
        filter=market_filter(
            event_type_ids=[GOLF_EVENT_TYPE_ID],
            market_type_codes=GOLF_MARKET_TYPES,
            market_start_time={
                "from": (now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": (now + timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        ),
        market_projection=["EVENT", "RUNNER_DESCRIPTION", "MARKET_START_TIME", "MARKET_DESCRIPTION"],
        max_results=200,
        sort="FIRST_TO_START",
    )
    return catalogue_events(list(catalogues or []))


def active_betfair_names(client: betfairlightweight.APIClient, event: BetfairEvent) -> list[str]:
    books = client.betting.list_market_book(market_ids=[event.market_id], price_projection=None)
    if not books:
        raise RuntimeError(f"Betfair returned no market book for {event.market_id}")
    active_ids = {
        int(getattr(runner, "selection_id"))
        for runner in (getattr(books[0], "runners", None) or [])
        if str(getattr(runner, "status", "") or "").upper() == "ACTIVE"
    }
    return [
        str(getattr(runner, "runner_name", "") or "")
        for runner in event.runners
        if int(getattr(runner, "selection_id")) in active_ids
    ]


def betfair_login(environment: dict[str, str]) -> betfairlightweight.APIClient:
    username = environment.get("BETFAIR_USERNAME") or environment.get("BF_USERNAME", "")
    password = environment.get("BETFAIR_PASSWORD") or environment.get("BF_PASSWORD", "")
    app_key = environment.get("BETFAIR_APP_KEY") or environment.get("BF_APP_KEY", "")
    certs_dir = environment.get("BETFAIR_CERTS_DIR") or environment.get("BF_CERTS_DIR", "")
    missing = [name for name, value in (("BETFAIR_USERNAME", username), ("BETFAIR_PASSWORD", password), ("BETFAIR_APP_KEY", app_key)) if not value]
    if missing:
        raise RuntimeError("Missing Betfair configuration: " + ", ".join(missing))
    if not certs_dir or not Path(certs_dir).is_dir():
        raise RuntimeError("Betfair certificate directory is not configured for server login")
    client = betfairlightweight.APIClient(username, password, app_key=app_key, certs=certs_dir)
    client.login()
    return client


def enabled_official_fields() -> list[dict[str, Any]]:
    config = _read_json(GOLF_CONFIG_PATH)
    rows: list[dict[str, Any]] = []
    for site in config.get("sites", []) or []:
        if not isinstance(site, dict) or not site.get("enabled"):
            continue
        site_id = str(site.get("id", "") or "")
        url = str(site.get("url", "") or "")
        config_saved_at = str(site.get("url_saved_at", "") or "")
        state = _read_json(GOLF_STATE_DIR / f"{site_id}.json")
        state_matches = bool(
            url
            and state.get("baseline_url") == url
            and str(state.get("config_saved_at", "") or "") == config_saved_at
        )
        label = {
            "pgatour": "PGA Tour",
            "pgachampions": "PGA Tour Champions",
            "dpworld": "DP World Tour",
            "lpga": "LPGA",
        }.get(site_id, site_id)
        rows.append(
            {
                "site_id": site_id,
                "competition": label,
                "url": url,
                "hint": competition_hint(url),
                "official_names": list(state.get("confirmed_field", []) or []) if state_matches else [],
                "ready": state_matches and bool(state.get("confirmed_field")),
            }
        )
    return rows


def slack_destination(environment: dict[str, str]) -> tuple[str, str, str]:
    return (
        (environment.get("GOLF_NR_SLACK_WEBHOOK_URL") or environment.get("SLACK_WEBHOOK_URL", "")).strip(),
        (environment.get("GOLF_NR_SLACK_BOT_TOKEN") or environment.get("SLACK_BOT_TOKEN", "")).strip(),
        (environment.get("GOLF_NR_SLACK_CHANNEL") or environment.get("SLACK_CHANNEL", "")).strip(),
    )


def discrepancy_slack_message(rows: list[dict[str, Any]]) -> str:
    lines = ["*Golf - Betfair Field Check: Discrepancy Found*"]
    for row in rows:
        if row.get("status") != "mismatch":
            continue
        lines.append(
            f"\n*{row['competition']}* ↔ *{row['betfair_event_name']}* "
            f"(official {row['official_count']} / Betfair {row['betfair_count']})"
        )
        for player in row.get("official_only", []):
            lines.append(f"• Official field only: {player}")
        for player in row.get("betfair_only", []):
            lines.append(f"• Betfair only: {player}")
    return "\n".join(lines)


def send_discrepancy_slack(environment: dict[str, str], rows: list[dict[str, Any]]) -> str:
    webhook, token, channel = slack_destination(environment)
    text = discrepancy_slack_message(rows)
    try:
        if webhook:
            response = requests.post(webhook, json={"text": text}, timeout=10)
            if response.status_code != 200:
                return f"Slack webhook returned status {response.status_code}"
            return "sent"
        if token and channel:
            response = requests.post(
                "https://slack.com/api/chat.postMessage",
                headers={"Authorization": f"Bearer {token}"},
                json={"channel": channel, "text": text},
                timeout=10,
            )
            payload = response.json() if response.content else {}
            if response.status_code != 200 or not payload.get("ok"):
                return f"Slack API rejected the message ({payload.get('error') or response.status_code})"
            return "sent"
    except Exception as exc:
        return f"Slack delivery failed: {exc}"
    return "Golf Slack destination is not configured"


def perform_check() -> dict[str, Any]:
    started_at = utc_timestamp()
    official_rows = enabled_official_fields()
    if not official_rows:
        raise RuntimeError("No enabled Golf competitions are configured")
    environment = child_environment()
    client = betfair_login(environment)
    try:
        events = list_betfair_events(client)
        if not events:
            raise RuntimeError("Betfair returned no upcoming Golf winner markets")
        rows: list[dict[str, Any]] = []
        used_event_ids: set[str] = set()
        for official in official_rows:
            base = {
                "site_id": official["site_id"],
                "competition": official["competition"],
                "official_count": len(official["official_names"]),
                "betfair_count": None,
                "betfair_event_name": "",
                "betfair_market_id": "",
                "official_only": [],
                "betfair_only": [],
            }
            if not official["ready"]:
                rows.append({**base, "status": "not_ready", "message": "Official field baseline is not ready yet."})
                continue
            event, score = best_event_match(official["hint"], events, used_event_ids)
            if event is None:
                rows.append(
                    {
                        **base,
                        "status": "event_not_found",
                        "message": f"No confident Betfair event match for '{official['hint']}' (best score {score:.2f}).",
                    }
                )
                continue
            used_event_ids.add(event.event_id)
            try:
                betfair_names = active_betfair_names(client, event)
                if not betfair_names:
                    raise RuntimeError("Betfair returned 0 active runners; skipped to avoid a false discrepancy")
                comparison = compare_player_lists(official["official_names"], betfair_names)
            except Exception as exc:
                rows.append(
                    {
                        **base,
                        "status": "error",
                        "betfair_event_name": event.event_name,
                        "betfair_market_id": event.market_id,
                        "message": str(exc),
                    }
                )
                continue
            rows.append(
                {
                    **base,
                    "status": "matching" if comparison["matching"] else "mismatch",
                    "message": "Fields match." if comparison["matching"] else "Field discrepancy found.",
                    "betfair_event_name": event.event_name,
                    "betfair_market_id": event.market_id,
                    "event_match_score": round(score, 3),
                    **comparison,
                }
            )
    finally:
        try:
            client.logout()
        except Exception:
            logger.warning("golf_betfair_logout_failed", exc_info=True)

    mismatches = [row for row in rows if row.get("status") == "mismatch"]
    slack_status = send_discrepancy_slack(environment, mismatches) if mismatches else "not needed"
    return {
        "status": "complete",
        "started_at": started_at,
        "completed_at": utc_timestamp(),
        "summary": "mismatch" if mismatches else "matching" if all(row.get("status") == "matching" for row in rows) else "attention",
        "rows": rows,
        "mismatch_count": len(mismatches),
        "slack_status": slack_status,
    }


class GolfBetfairCheckService:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._running = False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            running = self._running
        snapshot = _read_json(RESULT_PATH)
        if running:
            snapshot["status"] = "running"
        elif snapshot.get("status") == "running":
            snapshot["status"] = "failed"
            snapshot["error"] = "The previous check was interrupted before it completed."
        snapshot.setdefault("status", "idle")
        snapshot.setdefault("rows", [])
        return snapshot

    def start(self) -> bool:
        with self._lock:
            if self._running:
                return False
            self._running = True
            _write_result(
                {
                    "status": "running",
                    "started_at": utc_timestamp(),
                    "completed_at": "",
                    "rows": [],
                    "error": "",
                }
            )
            threading.Thread(target=self._run, name="golf-betfair-field-check", daemon=True).start()
            return True

    def _run(self) -> None:
        try:
            result = perform_check()
        except Exception as exc:
            logger.exception("golf_betfair_check_failed")
            result = {
                "status": "failed",
                "started_at": _read_json(RESULT_PATH).get("started_at", ""),
                "completed_at": utc_timestamp(),
                "rows": [],
                "error": str(exc),
            }
        try:
            _write_result(result)
        finally:
            with self._lock:
                self._running = False


golf_betfair_check_service = GolfBetfairCheckService()
