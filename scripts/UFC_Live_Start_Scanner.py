#!/usr/bin/env python3
"""Scan UFC.com for LIVE NOW fight starts and alert the in-play Slack channel."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
import threading
import time
import traceback
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlparse

import requests
from betfairlightweight import APIClient
from betfairlightweight.filters import market_filter
from dotenv import load_dotenv


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
RUNTIME_DIR = PROJECT_ROOT / "runtime"
CONFIG_DIR = RUNTIME_DIR / "config"
STATE_PATH = CONFIG_DIR / "ufc_live_start_scanner.json"

UTC_TZ = timezone.utc
DEFAULT_UFC_URL = "https://www.ufc.com/event/ufc-329"
UFC_CHECK_EVERY_SECONDS = 10
SLACK_WEBHOOK_ENV_NAME = "Slack_Webhook_TIP"
UFC_SLACK_WEBHOOK_ENV_NAME = "UFC_IS_IT_INPLAY_WEBHOOK_URL"
PLACEHOLDER_PREFIXES = ("YOUR_", "PASTE_", "CHANGE_ME", "TODO")
MMA_SPORT_MARKERS = ("mixed martial arts", "mma", "ufc")
MAX_DEBUG_BLOCK_CHARS = 600
STOP_EVENT = threading.Event()


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
class Fight:
    fighter_a: str
    fighter_b: str

    @property
    def display_name(self) -> str:
        return f"{self.fighter_a} v {self.fighter_b}"


@dataclass(frozen=True)
class BetfairCandidate:
    event_id: str
    event_name: str
    market_id: str
    market_name: str
    sport_name: str
    competition_name: str
    scheduled_start_utc: datetime | None


@dataclass(frozen=True)
class MatchResult:
    event_id: str
    market_id: str
    event_name: str
    confidence: str
    score: float
    reason: str

    @property
    def matched(self) -> bool:
        return bool(self.event_id)


def log(message: str) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def utc_now() -> datetime:
    return datetime.now(UTC_TZ)


def iso_utc(value: datetime | None = None) -> str:
    return (value or utc_now()).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_config_dir() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)


def read_json(path: Path = STATE_PATH) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log(f"Could not read state file: {exc}")
        return {}
    return data if isinstance(data, dict) else {}


def write_json(data: dict[str, Any], path: Path = STATE_PATH) -> None:
    ensure_config_dir()
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def update_state(**updates: Any) -> None:
    data = read_json()
    data.update(updates)
    write_json(data)


def resolve_ufc_url(cli_url: str) -> str:
    url = cli_url.strip()
    if url:
        return url
    stored = str(read_json().get("ufc_event_url", "") or "").strip()
    return stored


def save_ufc_url(url: str) -> None:
    data = read_json()
    previous = str(data.get("ufc_event_url", "") or "")
    data["ufc_event_url"] = url
    data["last_saved_at"] = iso_utc()
    if previous and previous != url:
        data["alerted_fight_keys"] = []
        data["alerted_fights"] = []
        data["last_detected_live_fight"] = ""
        data["last_slack_alert_sent"] = ""
    write_json(data)


def is_placeholder(value: str) -> bool:
    stripped = (value or "").strip()
    return not stripped or any(stripped.startswith(prefix) for prefix in PLACEHOLDER_PREFIXES)


def resolve_path(value: str, base_dir: Path) -> str:
    if not value:
        return ""
    path = Path(value)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return str(path)


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

    slack_webhook_url = os.getenv(UFC_SLACK_WEBHOOK_ENV_NAME, "").strip()
    slack_config_source = UFC_SLACK_WEBHOOK_ENV_NAME if slack_webhook_url else ""
    if not slack_webhook_url:
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


def object_get(obj: Any, name: str, default: Any = "") -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    if hasattr(obj, name):
        return getattr(obj, name)
    camel = re.sub(r"_([a-z])", lambda match: match.group(1).upper(), name)
    if hasattr(obj, camel):
        return getattr(obj, camel)
    return default


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC_TZ) if value.tzinfo is None else value.astimezone(UTC_TZ)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed.replace(tzinfo=UTC_TZ) if parsed.tzinfo is None else parsed.astimezone(UTC_TZ)
    return None


def format_betfair_time(value: datetime) -> str:
    return value.astimezone(UTC_TZ).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_name(value: str) -> str:
    without_accents = "".join(
        char for char in unicodedata.normalize("NFKD", value or "") if not unicodedata.combining(char)
    )
    lowered = without_accents.casefold()
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def surname(name: str) -> str:
    tokens = normalize_name(name).split()
    return tokens[-1] if tokens else ""


def clean_fighter_name(value: str) -> str:
    value = re.sub(r"\([^)]*\)|\[[^]]*\]", " ", value or "")
    value = re.sub(r"\b(?:live now|fight card|main card|prelims?)\b", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip(" -")
    return value


def split_match_sides(value: str) -> tuple[str, str] | None:
    text = re.sub(r"\s+", " ", (value or "").replace("\xa0", " ")).strip()
    if not text:
        return None
    parts = [
        clean_fighter_name(part)
        for part in re.split(r"\s+(?:v|vs|vs\.|@|-)\s+", text, maxsplit=1, flags=re.IGNORECASE)
    ]
    if len(parts) == 2 and parts[0] and parts[1]:
        return parts[0], parts[1]
    return None


def parse_fight_from_block_text(text: str) -> Fight | None:
    if "LIVE NOW" not in text.upper():
        return None
    lines = [clean_fighter_name(line) for line in re.split(r"[\r\n]+", text) if clean_fighter_name(line)]
    lines = [line for line in lines if line.casefold() not in {"live", "live now"}]
    for index, line in enumerate(lines):
        if re.fullmatch(r"v|vs|vs\.", line, flags=re.IGNORECASE):
            before = next((lines[pos] for pos in range(index - 1, -1, -1) if lines[pos]), "")
            after = next((lines[pos] for pos in range(index + 1, len(lines)) if lines[pos]), "")
            if before and after:
                return Fight(before, after)
    for line in lines:
        sides = split_match_sides(line)
        if sides:
            return Fight(*sides)
    return None


def parse_live_fights_from_block_texts(block_texts: Iterable[str]) -> list[Fight]:
    fights: list[Fight] = []
    seen: set[str] = set()
    for block_text in block_texts:
        fight = parse_fight_from_block_text(block_text)
        if not fight:
            continue
        key = fight_key(fight, "block")
        if key not in seen:
            fights.append(fight)
            seen.add(key)
    return fights


def validate_ufc_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("UFC event URL must be an absolute http(s) URL.")
    if "ufc.com" not in parsed.netloc.casefold():
        raise ValueError("UFC event URL must be on ufc.com.")


def fight_key(fight: Fight, ufc_url: str, date_value: str | None = None) -> str:
    names = sorted([normalize_name(fight.fighter_a), normalize_name(fight.fighter_b)])
    date_part = date_value or utc_now().date().isoformat()
    return "|".join([date_part, normalize_name(ufc_url), *names])


def format_slack_message(fight: Fight, match: MatchResult) -> str:
    betfair_id = match.event_id if match.matched else "Not matched"
    lines = [
        f"UFC - {fight.display_name} has started",
        f"Betfair ID: {betfair_id}",
    ]
    if not match.matched:
        lines.append("Please manually locate the Betfair event.")
    lines.append("Please ensure it is in play")
    return "\n".join(lines)


def send_slack_message(
    webhook_url: str,
    text: str,
    *,
    post_func: Callable[..., Any] = requests.post,
) -> None:
    if is_placeholder(webhook_url):
        raise RuntimeError(f"{UFC_SLACK_WEBHOOK_ENV_NAME} or {SLACK_WEBHOOK_ENV_NAME} missing")
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            response = post_func(webhook_url, json={"text": text}, timeout=15)
            status_code = int(getattr(response, "status_code", 0))
            body = str(getattr(response, "text", ""))
            if status_code < 400:
                return
            raise RuntimeError(f"Slack webhook failed: status={status_code}, body={body[:300]}")
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(1)
    raise RuntimeError(str(last_error))


def list_mma_match_odds_candidates(
    client: APIClient,
    *,
    start_from: datetime,
    start_to: datetime,
    max_results: int = 200,
) -> list[BetfairCandidate]:
    event_filter = market_filter(
        market_start_time={
            "from": format_betfair_time(start_from),
            "to": format_betfair_time(start_to),
        }
    )
    event_types = client.betting.list_event_types(filter=event_filter)
    mma_event_type_ids: list[str] = []
    sport_names: dict[str, str] = {}
    for result in event_types:
        event_type = object_get(result, "event_type", {})
        event_type_id = str(object_get(event_type, "id", "")).strip()
        sport_name = str(object_get(event_type, "name", "")).strip()
        if event_type_id and any(marker in sport_name.casefold() for marker in MMA_SPORT_MARKERS):
            mma_event_type_ids.append(event_type_id)
            sport_names[event_type_id] = sport_name
    candidates: list[BetfairCandidate] = []
    for event_type_id in mma_event_type_ids:
        catalogue_filter = market_filter(
            event_type_ids=[event_type_id],
            market_type_codes=["MATCH_ODDS"],
            market_start_time={
                "from": format_betfair_time(start_from),
                "to": format_betfair_time(start_to),
            },
        )
        catalogues = client.betting.list_market_catalogue(
            filter=catalogue_filter,
            market_projection=["EVENT", "EVENT_TYPE", "COMPETITION", "MARKET_START_TIME", "MARKET_DESCRIPTION"],
            sort="FIRST_TO_START",
            max_results=max_results,
        )
        for catalogue in catalogues:
            event = object_get(catalogue, "event", {})
            event_type = object_get(catalogue, "event_type", {})
            competition = object_get(catalogue, "competition", {})
            candidates.append(
                BetfairCandidate(
                    event_id=str(object_get(event, "id", "")).strip(),
                    event_name=str(object_get(event, "name", "")).strip(),
                    market_id=str(object_get(catalogue, "market_id", "")).strip(),
                    market_name=str(object_get(catalogue, "market_name", "")).strip(),
                    sport_name=str(object_get(event_type, "name", sport_names.get(event_type_id, ""))).strip(),
                    competition_name=str(object_get(competition, "name", "")).strip(),
                    scheduled_start_utc=parse_datetime(object_get(catalogue, "market_start_time", None)),
                )
            )
    return candidates


def candidate_sides(candidate: BetfairCandidate) -> tuple[str, str] | None:
    for value in (candidate.event_name, candidate.market_name):
        sides = split_match_sides(value)
        if sides:
            return sides
    return None


def score_candidate(fight: Fight, candidate: BetfairCandidate) -> tuple[float, str]:
    sides = candidate_sides(candidate)
    if not sides:
        return 0.0, "candidate has no parseable fighter sides"
    fight_names = (fight.fighter_a, fight.fighter_b)
    orders = ((fight_names[0], fight_names[1], sides[0], sides[1]), (fight_names[0], fight_names[1], sides[1], sides[0]))
    best_score = 0.0
    best_reason = "no strong participant match"
    for left_fight, right_fight, left_betfair, right_betfair in orders:
        left_norm = normalize_name(left_fight)
        right_norm = normalize_name(right_fight)
        left_bf_norm = normalize_name(left_betfair)
        right_bf_norm = normalize_name(right_betfair)
        left_ratio = SequenceMatcher(None, left_norm, left_bf_norm).ratio() if left_norm and left_bf_norm else 0.0
        right_ratio = SequenceMatcher(None, right_norm, right_bf_norm).ratio() if right_norm and right_bf_norm else 0.0
        score = (left_ratio + right_ratio) * 35
        reasons = [f"name_similarity={round((left_ratio + right_ratio) / 2, 3)}"]
        left_surname_match = surname(left_fight) and surname(left_fight) == surname(left_betfair)
        right_surname_match = surname(right_fight) and surname(right_fight) == surname(right_betfair)
        if left_surname_match:
            score += 15
            reasons.append(f"surname {surname(left_fight)} matched")
        if right_surname_match:
            score += 15
            reasons.append(f"surname {surname(right_fight)} matched")
        if "ufc" in normalize_name(candidate.competition_name + " " + candidate.event_name):
            score += 5
            reasons.append("UFC context")
        if score > best_score:
            best_score = score
            best_reason = "; ".join(reasons)
    return min(best_score, 100.0), best_reason


def match_fight_to_candidates(fight: Fight, candidates: Iterable[BetfairCandidate]) -> MatchResult:
    scored: list[tuple[float, str, BetfairCandidate]] = []
    for candidate in candidates:
        score, reason = score_candidate(fight, candidate)
        if score >= 75:
            scored.append((score, reason, candidate))
    scored.sort(key=lambda item: item[0], reverse=True)
    if not scored:
        return MatchResult("", "", "", "None", 0.0, "No high-confidence Betfair event found")
    if len(scored) > 1 and scored[0][0] - scored[1][0] < 12:
        top_names = ", ".join(f"{item[2].event_name} ({round(item[0], 1)})" for item in scored[:3])
        return MatchResult("", "", "", "Ambiguous", scored[0][0], f"Multiple close Betfair matches: {top_names}")
    score, reason, candidate = scored[0]
    return MatchResult(candidate.event_id, candidate.market_id, candidate.event_name, "High", score, reason)


def find_betfair_match(client: APIClient | None, fight: Fight) -> MatchResult:
    if client is None:
        return MatchResult("", "", "", "None", 0.0, "Betfair client unavailable")
    start_from = utc_now() - timedelta(hours=12)
    start_to = utc_now() + timedelta(days=2)
    candidates = list_mma_match_odds_candidates(client, start_from=start_from, start_to=start_to)
    log(f"betfair_candidates_scanned count={len(candidates)} fight_name={fight.display_name!r}")
    match = match_fight_to_candidates(fight, candidates)
    if match.matched:
        log(
            "matched_betfair_event_id=%s matched_betfair_market_id=%s fight_name=%r score=%.1f reason=%r"
            % (match.event_id, match.market_id, fight.display_name, match.score, match.reason)
        )
    else:
        log(f"betfair_match_not_found fight_name={fight.display_name!r} reason={match.reason!r}")
    return match


def fetch_ufc_block_texts(url: str, browser_holder: dict[str, Any]) -> list[str]:
    if browser_holder.get("playwright") is None:
        from playwright.sync_api import sync_playwright

        playwright = sync_playwright().start()
        chrome_binary = (
            os.getenv("CHROME_BINARY", "").strip()
            or os.getenv("GOOGLE_CHROME_BIN", "").strip()
            or os.getenv("CHROME_BIN", "").strip()
        )
        launch_options: dict[str, Any] = {
            "headless": True,
            "args": ["--no-sandbox", "--disable-dev-shm-usage"],
        }
        if chrome_binary and Path(chrome_binary).exists():
            launch_options["executable_path"] = chrome_binary
        browser = playwright.chromium.launch(**launch_options)
        browser_holder["playwright"] = playwright
        browser_holder["browser"] = browser
    browser = browser_holder["browser"]
    page = browser.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        try:
            page.wait_for_selector(".c-listing-fight", timeout=15000)
        except Exception:
            log("ufc_parse_warning no .c-listing-fight blocks found before timeout")
        texts = page.locator(".c-listing-fight").all_inner_texts()
        if not texts:
            body_text = page.locator("body").inner_text(timeout=5000)
            log(f"ufc_parse_error no fight blocks found body_preview={body_text[:MAX_DEBUG_BLOCK_CHARS]!r}")
        return [str(text) for text in texts]
    finally:
        page.close()


def close_browser(browser_holder: dict[str, Any]) -> None:
    browser = browser_holder.get("browser")
    playwright = browser_holder.get("playwright")
    if browser is not None:
        try:
            browser.close()
        except Exception as exc:
            log(f"Browser close failed: {exc}")
    if playwright is not None:
        try:
            playwright.stop()
        except Exception as exc:
            log(f"Playwright stop failed: {exc}")
    browser_holder.clear()


def load_alerted_keys_for_url(ufc_url: str) -> set[str]:
    data = read_json()
    if str(data.get("ufc_event_url", "") or "") != ufc_url:
        return set()
    return {str(key) for key in data.get("alerted_fight_keys", []) or []}


def mark_alerted(fight: Fight, match: MatchResult, ufc_url: str, key: str, *, dry_run: bool) -> None:
    data = read_json()
    keys = [str(existing) for existing in data.get("alerted_fight_keys", []) or []]
    if key not in keys:
        keys.append(key)
    alerted_fights = list(data.get("alerted_fights", []) or [])
    alerted_fights.append(
        {
            "fight_name": fight.display_name,
            "fighter_a": fight.fighter_a,
            "fighter_b": fight.fighter_b,
            "ufc_event_url": ufc_url,
            "betfair_event_id": match.event_id if match.matched else "Not matched",
            "betfair_market_id": match.market_id,
            "matched_betfair_event_name": match.event_name,
            "dry_run": bool(dry_run),
            "alerted_at": iso_utc(),
        }
    )
    data.update(
        {
            "ufc_event_url": ufc_url,
            "alerted_fight_keys": keys[-100:],
            "alerted_fights": alerted_fights[-100:],
            "last_detected_live_fight": fight.display_name,
            "last_slack_alert_sent": "Dry-run: not sent" if dry_run else f"{fight.display_name} at {iso_utc()}",
        }
    )
    write_json(data)


def process_live_fights(
    fights: Iterable[Fight],
    *,
    ufc_url: str,
    config: Config,
    betfair_client: APIClient | None,
    dry_run: bool,
) -> None:
    alerted_keys = load_alerted_keys_for_url(ufc_url)
    for fight in fights:
        key = fight_key(fight, ufc_url)
        update_state(last_detected_live_fight=fight.display_name)
        if key in alerted_keys:
            log(f"dedupe_skip fight_name={fight.display_name!r} ufc_event_url={ufc_url!r}")
            continue
        match = find_betfair_match(betfair_client, fight)
        message = format_slack_message(fight, match)
        log(
            "fight_live_detected fight_name=%r ufc_event_url=%r betfair_event_id=%r betfair_market_id=%r"
            % (fight.display_name, ufc_url, match.event_id or "Not matched", match.market_id)
        )
        if dry_run:
            log("DRY RUN Slack message follows:")
            print(message, flush=True)
            mark_alerted(fight, match, ufc_url, key, dry_run=True)
            alerted_keys.add(key)
            continue
        try:
            send_slack_message(config.slack_webhook_url, message)
        except Exception as exc:
            log(f"slack_failed fight_name={fight.display_name!r} error={exc}")
            continue
        log(f"slack_sent fight_name={fight.display_name!r}")
        mark_alerted(fight, match, ufc_url, key, dry_run=False)
        alerted_keys.add(key)


def run_scan_loop(args: argparse.Namespace) -> int:
    ufc_url = resolve_ufc_url(args.ufc_url)
    if not ufc_url:
        log("No UFC event URL configured.")
        return 2
    validate_ufc_url(ufc_url)
    save_ufc_url(ufc_url)

    config = load_config()
    log(f"Slack webhook source: {config.slack_config_source}")
    betfair_client: APIClient | None = None
    try:
        betfair_client = build_client(config)
    except Exception as exc:
        log(f"Betfair login unavailable; alerts will use Betfair ID: Not matched. error={exc}")

    browser_holder: dict[str, Any] = {}
    check_every = max(float(args.check_every_seconds), 2.0)
    log(f"Starting UFC live scanner url={ufc_url!r} cadence_seconds={check_every:g} dry_run={bool(args.dry_run)}")
    try:
        while not STOP_EVENT.is_set():
            update_state(last_check_time=iso_utc(), ufc_event_url=ufc_url)
            try:
                block_texts = fetch_ufc_block_texts(ufc_url, browser_holder)
                fights = parse_live_fights_from_block_texts(block_texts)
                log(f"ufc_check_complete live_fights={len(fights)} blocks={len(block_texts)}")
                process_live_fights(
                    fights,
                    ufc_url=ufc_url,
                    config=config,
                    betfair_client=betfair_client,
                    dry_run=bool(args.dry_run),
                )
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                log(f"ufc_check_failed error={exc}")
                close_browser(browser_holder)
            if args.once:
                return 0
            STOP_EVENT.wait(check_every)
    except KeyboardInterrupt:
        log("Interrupted.")
        return 130
    finally:
        close_browser(browser_holder)
        try:
            if betfair_client is not None:
                betfair_client.logout()
        except Exception:
            pass
    log("UFC live scanner stopped.")
    return 0


def run_self_test() -> int:
    assert normalize_name("  Jose Aldo Jr. ") == "jose aldo jr"
    block = """
    Early Prelims
    Fighter A
    VS
    Fighter B
    LIVE NOW
    """
    fights = parse_live_fights_from_block_texts([block])
    assert fights == [Fight("Fighter A", "Fighter B")]
    not_live = parse_live_fights_from_block_texts(["Fighter A\nVS\nFighter B\nUpcoming"])
    assert not_live == []

    fight = Fight("Jane Doe", "Mary Smith")
    key = fight_key(fight, DEFAULT_UFC_URL, "2026-07-08")
    assert "jane" in key and "smith" in key
    unmatched = MatchResult("", "", "", "None", 0.0, "")
    assert format_slack_message(fight, unmatched).splitlines() == [
        "UFC - Jane Doe v Mary Smith has started",
        "Betfair ID: Not matched",
        "Please manually locate the Betfair event.",
        "Please ensure it is in play",
    ]

    candidates = [
        BetfairCandidate("111", "Jane Doe v Mary Smith", "1.111", "Match Odds", "Mixed Martial Arts", "UFC 329", None),
        BetfairCandidate("222", "Other Fighter v Spare Name", "1.222", "Match Odds", "Mixed Martial Arts", "UFC 329", None),
    ]
    matched = match_fight_to_candidates(fight, candidates)
    assert matched.event_id == "111"
    reversed_match = match_fight_to_candidates(Fight("Mary Smith", "Jane Doe"), candidates)
    assert reversed_match.event_id == "111"

    test_config_dir = RUNTIME_DIR / "output"
    test_config_dir.mkdir(parents=True, exist_ok=True)
    path = test_config_dir / f"ufc_live_start_scanner_self_test_{os.getpid()}.json"
    try:
        write_json({"ufc_event_url": DEFAULT_UFC_URL}, path)
        assert read_json(path)["ufc_event_url"] == DEFAULT_UFC_URL
    finally:
        try:
            path.unlink()
        except (FileNotFoundError, PermissionError):
            pass

    source = Path(__file__).read_text(encoding="utf-8")
    for name in ("win" + "sound", "tkin" + "ter"):
        assert f"import {name}" not in source
        assert f"from {name}" not in source

    sent: list[str] = []

    def fake_post(url: str, json: dict[str, str], timeout: int) -> Any:
        sent.append(json["text"])

        class Response:
            status_code = 200
            text = "ok"

        return Response()

    send_slack_message("https://hooks.slack.com/services/test", "hello", post_func=fake_post)
    assert sent == ["hello"]
    log("Self-test passed.")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan UFC.com for LIVE NOW starts and alert Slack.")
    parser.add_argument("--self-test", action="store_true", help="Run fixture-based checks and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Do not send Slack; print what would be sent.")
    parser.add_argument("--ufc-url", default="", help="Current UFC event URL, for example https://www.ufc.com/event/ufc-329.")
    parser.add_argument("--check-every-seconds", type=float, default=UFC_CHECK_EVERY_SECONDS)
    parser.add_argument("--once", action="store_true", help="Run one check cycle and exit.")
    return parser.parse_args()


def handle_stop_signal(signum: int, frame: Any) -> None:
    STOP_EVENT.set()
    raise KeyboardInterrupt


def main() -> int:
    args = parse_args()
    if args.self_test:
        return run_self_test()
    return run_scan_loop(args)


if __name__ == "__main__":
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handle_stop_signal)
    signal.signal(signal.SIGTERM, handle_stop_signal)
    signal.signal(signal.SIGINT, handle_stop_signal)
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        log("Interrupted.")
        raise SystemExit(130)
    except Exception as exc:
        log(f"ERROR: {exc}")
        traceback.print_exc()
        raise SystemExit(1)
