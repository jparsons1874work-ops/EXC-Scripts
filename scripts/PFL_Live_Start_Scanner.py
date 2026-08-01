#!/usr/bin/env python3
"""Monitor a PFL event page and alert when a Betfair-matched fight goes live."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import threading
import time
import traceback
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from rapidfuzz import fuzz


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
STATE_PATH = PROJECT_ROOT / "runtime" / "config" / "pfl_live_start_scanner.json"

CHECK_EVERY_SECONDS = 3
BETFAIR_MMA_EVENT_TYPE_ID = "26420387"
BETFAIR_LOOKBACK_HOURS = 12
BETFAIR_LOOKAHEAD_HOURS = 36
BETFAIR_MATCH_THRESHOLD = 82.0
BETFAIR_AMBIGUITY_GAP = 4.0
SLACK_WEBHOOK_ENV_NAME = "Slack_Webhook_TIP"
UFC_SLACK_WEBHOOK_ENV_NAME = "UFC_IS_IT_INPLAY_WEBHOOK_URL"
PLACEHOLDER_PREFIXES = ("YOUR_", "PASTE_", "CHANGE_ME", "TODO")
STOP_EVENT = threading.Event()
UTC_TZ = timezone.utc


load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class BetfairEvent:
    event_id: str
    event_name: str
    open_date: datetime | None = None


@dataclass(frozen=True)
class MatchResult:
    event: BetfairEvent | None
    score: float
    reason: str


def log(message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


def iso_utc(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC_TZ)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize(text: str) -> str:
    return " ".join((text or "").casefold().split())


def fighter_key(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text or "")
    ascii_text = "".join(character for character in decomposed if not unicodedata.combining(character))
    words = re.findall(r"[a-z0-9]+", ascii_text.casefold())
    while words and words[-1] in {"jr", "sr"}:
        words.pop()
    return " ".join(words)


def split_fight_name(text: str) -> tuple[str, str] | None:
    parts = re.split(
        r"\s+(?:v(?:s)?\.?|versus)\s+",
        " ".join((text or "").split()),
        maxsplit=1,
        flags=re.IGNORECASE,
    )
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None
    return parts[0], parts[1]


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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def update_state(**updates: Any) -> None:
    data = read_json()
    data.update(updates)
    write_json(data)


def resolve_pfl_url(cli_url: str) -> str:
    return cli_url.strip() or str(read_json().get("pfl_event_url", "") or "").strip()


def validate_pfl_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("PFL event URL must be an absolute http(s) URL.")
    hostname = (parsed.hostname or "").casefold()
    if hostname != "pflmma.com" and not hostname.endswith(".pflmma.com"):
        raise ValueError("PFL event URL must be on pflmma.com.")


def save_pfl_url(url: str) -> None:
    data = read_json()
    previous_url = str(data.get("pfl_event_url", "") or "")
    data["pfl_event_url"] = url
    data["last_saved_at"] = iso_utc()
    if previous_url and previous_url != url:
        data["alerted_fight_keys"] = []
        data["alerted_fights"] = []
        data["last_detected_live_fight"] = ""
        data["last_slack_alert_sent"] = ""
    write_json(data)


def is_placeholder(value: str) -> bool:
    stripped = (value or "").strip()
    return not stripped or any(stripped.startswith(prefix) for prefix in PLACEHOLDER_PREFIXES)


def slack_webhook_url() -> tuple[str, str]:
    """Use exactly the same Slack webhook priority as the UFC scanner."""
    url = os.getenv(UFC_SLACK_WEBHOOK_ENV_NAME, "").strip()
    if url:
        return url, UFC_SLACK_WEBHOOK_ENV_NAME
    url = os.getenv(SLACK_WEBHOOK_ENV_NAME, "").strip()
    return url, SLACK_WEBHOOK_ENV_NAME if url else "not configured"


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
            if int(getattr(response, "status_code", 0)) < 400:
                return
            body = str(getattr(response, "text", ""))
            raise RuntimeError(
                f"Slack webhook failed: status={getattr(response, 'status_code', 0)}, body={body[:300]}"
            )
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(1)
    raise RuntimeError(str(last_error))


def alerted_keys() -> set[str]:
    return {str(key) for key in read_json().get("alerted_fight_keys", []) or []}


def record_alert(*, key: str, fight_name: str, event_id: str, pfl_url: str, dry_run: bool) -> None:
    data = read_json()
    keys = [str(existing) for existing in data.get("alerted_fight_keys", []) or []]
    if key not in keys:
        keys.append(key)
    fights = list(data.get("alerted_fights", []) or [])
    fights.append(
        {
            "fight_name": fight_name,
            "pfl_event_url": pfl_url,
            "betfair_event_id": event_id,
            "dry_run": bool(dry_run),
            "alerted_at": iso_utc(),
        }
    )
    data.update(
        {
            "pfl_event_url": pfl_url,
            "alerted_fight_keys": keys[-150:],
            "alerted_fights": fights[-150:],
            "last_detected_live_fight": fight_name,
            "last_slack_alert_sent": (
                f"Dry-run: {fight_name} ({event_id})"
                if dry_run
                else f"{fight_name} ({event_id}) at {iso_utc()}"
            ),
        }
    )
    write_json(data)


def format_slack_message(fight_name: str, event_id: str) -> str:
    participants = split_fight_name(fight_name)
    display = f"{participants[0]} v {participants[1]}" if participants else fight_name
    return f":pfl: {display} - Live now (Betfair Event ID: {event_id})"


def deliver_live_alert(
    *,
    fight_name: str,
    event_id: str,
    pfl_url: str,
    webhook_url: str,
    dry_run: bool,
) -> bool:
    message = format_slack_message(fight_name, event_id)
    log(f"PFL Fight Started: {fight_name} (Betfair Event ID: {event_id})")
    if dry_run:
        log(f"DRY RUN Slack message: {message!r}")
    else:
        try:
            send_slack_message(webhook_url, message)
        except Exception as exc:
            log(f"slack_failed fight_name={fight_name!r} event_id={event_id!r} error={exc}")
            return False
        log(f"slack_sent fight_name={fight_name!r} event_id={event_id!r}")
    key = f"live|{normalize(pfl_url)}|{event_id}"
    record_alert(key=key, fight_name=fight_name, event_id=event_id, pfl_url=pfl_url, dry_run=dry_run)
    return True


def object_value(value: Any, *names: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        for name in names:
            if name in value:
                return value[name]
        return default
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return default


def parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC_TZ)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC_TZ)


def parse_betfair_events(results: Iterable[Any]) -> list[BetfairEvent]:
    events: dict[str, BetfairEvent] = {}
    for result in results:
        event = object_value(result, "event", default=result)
        event_id = str(object_value(event, "id", default="") or "").strip()
        event_name = str(object_value(event, "name", default="") or "").strip()
        if not event_id or not event_name or split_fight_name(event_name) is None:
            continue
        events[event_id] = BetfairEvent(
            event_id=event_id,
            event_name=event_name,
            open_date=parse_datetime(object_value(event, "open_date", "openDate")),
        )
    return sorted(events.values(), key=lambda item: (item.open_date or datetime.max.replace(tzinfo=UTC_TZ), item.event_name))


def resolve_betfair_certs_dir() -> str:
    cert_file_value = os.getenv("BETFAIR_CERT_FILE", "").strip()
    key_file_value = os.getenv("BETFAIR_KEY_FILE", "").strip()
    if cert_file_value or key_file_value:
        if not cert_file_value or not key_file_value:
            raise RuntimeError("BETFAIR_CERT_FILE and BETFAIR_KEY_FILE must both be configured.")
        cert_file = Path(cert_file_value).expanduser().resolve()
        key_file = Path(key_file_value).expanduser().resolve()
        if not cert_file.is_file() or not key_file.is_file():
            raise RuntimeError("Configured Betfair certificate or key file was not found.")
        if cert_file.parent != key_file.parent:
            raise RuntimeError("BETFAIR_CERT_FILE and BETFAIR_KEY_FILE must be in the same folder.")
        return str(cert_file.parent)

    candidates = [
        Path(os.getenv("BETFAIR_CERTS_DIR", "")).expanduser() if os.getenv("BETFAIR_CERTS_DIR", "").strip() else None,
        PROJECT_ROOT / "certs",
    ]
    for candidate in candidates:
        if candidate and (candidate / "client-2048.crt").is_file() and (candidate / "client-2048.key").is_file():
            return str(candidate.resolve())
    raise RuntimeError("Betfair certificate files were not found.")


def fetch_betfair_events(now: datetime | None = None) -> list[BetfairEvent]:
    from betfairlightweight import APIClient, filters

    username = os.getenv("BETFAIR_USERNAME", "").strip()
    password = os.getenv("BETFAIR_PASSWORD", "").strip()
    app_key = os.getenv("BETFAIR_APP_KEY", "").strip()
    missing = [
        name
        for name, value in (
            ("BETFAIR_USERNAME", username),
            ("BETFAIR_PASSWORD", password),
            ("BETFAIR_APP_KEY", app_key),
        )
        if is_placeholder(value)
    ]
    if missing:
        raise RuntimeError("Missing Betfair configuration: " + ", ".join(missing))

    anchor = (now or datetime.now(UTC_TZ)).astimezone(UTC_TZ)
    start = anchor - timedelta(hours=BETFAIR_LOOKBACK_HOURS)
    end = anchor + timedelta(hours=BETFAIR_LOOKAHEAD_HOURS)
    client = APIClient(username=username, password=password, app_key=app_key, certs=resolve_betfair_certs_dir())
    try:
        client.login()
        log("Betfair login success; caching MMA events before PFL scanning starts.")
        results = client.betting.list_events(
            filter=filters.market_filter(
                event_type_ids=[BETFAIR_MMA_EVENT_TYPE_ID],
                market_start_time={"from": iso_utc(start), "to": iso_utc(end)},
            )
        )
    finally:
        try:
            client.logout()
        except Exception:
            pass
    events = parse_betfair_events(results)
    log(f"Cached {len(events)} Betfair MMA fights; no further Betfair calls will be made during scanning.")
    return events


def match_betfair_event(fight_name: str, events: Iterable[BetfairEvent]) -> MatchResult:
    site_parts = split_fight_name(fight_name)
    if site_parts is None:
        return MatchResult(None, 0.0, "PFL fight name could not be split into two fighters")
    site_a, site_b = (fighter_key(part) for part in site_parts)
    if not site_a or not site_b:
        return MatchResult(None, 0.0, "PFL fighter name was empty after normalization")

    scored: list[tuple[float, BetfairEvent]] = []
    for event in events:
        betfair_parts = split_fight_name(event.event_name)
        if betfair_parts is None:
            continue
        betfair_a, betfair_b = (fighter_key(part) for part in betfair_parts)
        if sorted((site_a, site_b)) == sorted((betfair_a, betfair_b)):
            scored.append((100.0, event))
            continue
        forward = (fuzz.ratio(site_a, betfair_a) + fuzz.ratio(site_b, betfair_b)) / 2
        reverse = (fuzz.ratio(site_a, betfair_b) + fuzz.ratio(site_b, betfair_a)) / 2
        scored.append((max(forward, reverse), event))

    if not scored:
        return MatchResult(None, 0.0, "no cached Betfair MMA fights")
    scored.sort(key=lambda item: (-item[0], item[1].event_id))
    best_score, best_event = scored[0]
    if best_score < BETFAIR_MATCH_THRESHOLD:
        return MatchResult(None, best_score, f"best score below {BETFAIR_MATCH_THRESHOLD:g}")
    if len(scored) > 1 and scored[1][0] > best_score - BETFAIR_AMBIGUITY_GAP:
        return MatchResult(None, best_score, "ambiguous cached Betfair fight match")
    return MatchResult(best_event, best_score, "exact" if best_score == 100 else "fuzzy")


def pfl_wrapper_fighter_names(wrapper: Any) -> list[str]:
    full_name_headings = wrapper.locator("h4:not(.mb-0)")
    if full_name_headings.count() >= 2:
        name_headings = full_name_headings
    else:
        name_headings = wrapper.locator("h4")
    names = name_headings.evaluate_all(
        """
        els => els.map(el => Array.from(el.childNodes).map(node =>
            node.nodeName === 'BR' ? ' ' : (node.textContent || '')
        ).join('').trim())
        """
    )
    unique_names: list[str] = []
    for name in names:
        cleaned = " ".join(str(name).split())
        if cleaned and cleaned not in unique_names:
            unique_names.append(cleaned)
    return unique_names


def get_pfl_card_fights(page: Any) -> list[str]:
    wrappers = page.locator('[id^="fightCardWrapper"]')
    fights: list[str] = []
    for index in range(wrappers.count()):
        names = pfl_wrapper_fighter_names(wrappers.nth(index))
        if len(names) >= 2:
            fight = f"{names[0]} v {names[1]}"
            if fight not in fights:
                fights.append(fight)
    return fights


def prepare_card_matches(page: Any, events: Iterable[BetfairEvent]) -> dict[str, BetfairEvent]:
    card_fights = get_pfl_card_fights(page)
    matches: dict[str, BetfairEvent] = {}
    log(f"PFL card fights found for startup matching: {len(card_fights)}")
    for fight in card_fights:
        result = match_betfair_event(fight, events)
        if result.event is None:
            log(f"Startup Betfair match withheld for {fight!r}: {result.reason} (score={result.score:.1f})")
            continue
        matches[normalize(fight)] = result.event
        log(
            f"Startup Betfair match: {fight} -> {result.event.event_name} "
            f"(Event ID: {result.event.event_id}, score={result.score:.1f})"
        )
    return matches


def get_pfl_live_fight(page: Any) -> str | None:
    try:
        page.reload(wait_until="commit", timeout=8000)
    except Exception as exc:
        log(f"PFL reload warning: {exc}")
    page.wait_for_timeout(3500)

    live_boxes = page.locator(".live-now")
    for index in range(live_boxes.count()):
        box = live_boxes.nth(index)
        box_id = box.get_attribute("id")
        style = (box.get_attribute("style") or "").casefold()
        if "display: none" in style or "display:none" in style or not box_id:
            continue
        fight_number = box_id.replace("liveNow_", "")
        wrapper = page.locator(f"#fightCardWrapper{fight_number}")
        if wrapper.count() == 0:
            continue
        names = pfl_wrapper_fighter_names(wrapper)
        if len(names) >= 2:
            return f"{names[0]} v {names[1]}"
    return None


def open_pfl_page(pfl_url: str, browser_holder: dict[str, Any]) -> Any:
    from playwright.sync_api import sync_playwright

    playwright = sync_playwright().start()
    chrome_binary = (
        os.getenv("CHROME_BINARY", "").strip()
        or os.getenv("GOOGLE_CHROME_BIN", "").strip()
        or os.getenv("CHROME_BIN", "").strip()
    )
    launch_options: dict[str, Any] = {"headless": True, "args": ["--no-sandbox", "--disable-dev-shm-usage"]}
    if chrome_binary and Path(chrome_binary).exists():
        launch_options["executable_path"] = chrome_binary
    browser = playwright.chromium.launch(**launch_options)
    context = browser.new_context(ignore_https_errors=True)
    page = context.new_page()
    browser_holder.update({"playwright": playwright, "browser": browser, "context": context, "page": page})
    log("Opening PFL...")
    try:
        page.goto(pfl_url, wait_until="commit", timeout=8000)
    except Exception as exc:
        log(f"Initial PFL navigation warning: {exc}")
    page.wait_for_timeout(1000)
    return page


def close_browser(browser_holder: dict[str, Any]) -> None:
    for name in ("page", "context", "browser"):
        resource = browser_holder.get(name)
        if resource is not None:
            try:
                resource.close()
            except Exception as exc:
                log(f"{name.capitalize()} close warning: {exc}")
    playwright = browser_holder.get("playwright")
    if playwright is not None:
        try:
            playwright.stop()
        except Exception as exc:
            log(f"Playwright close warning: {exc}")
    browser_holder.clear()


def run_scan_loop(args: argparse.Namespace) -> int:
    pfl_url = resolve_pfl_url(args.pfl_url)
    if not pfl_url:
        log("No PFL event URL configured.")
        return 2
    validate_pfl_url(pfl_url)
    save_pfl_url(pfl_url)

    webhook_url, webhook_source = slack_webhook_url()
    log(f"Slack webhook source: {webhook_source}")
    if not args.dry_run and is_placeholder(webhook_url):
        raise RuntimeError(f"{UFC_SLACK_WEBHOOK_ENV_NAME} or {SLACK_WEBHOOK_ENV_NAME} missing")

    cached_events = fetch_betfair_events()
    if not cached_events:
        raise RuntimeError("No Betfair MMA fights were found in the startup cache window.")
    cache_time = iso_utc()
    update_state(
        betfair_events_cached=len(cached_events),
        betfair_cache_loaded_at=cache_time,
        cached_betfair_events=[
            {"event_id": event.event_id, "event_name": event.event_name, "open_date": iso_utc(event.open_date) if event.open_date else ""}
            for event in cached_events
        ],
    )

    browser_holder: dict[str, Any] = {}
    page: Any | None = None
    prepared_matches: dict[str, BetfairEvent] = {}
    known_alerts = alerted_keys()
    last_unmatched_key = ""
    check_every = max(float(args.check_every_seconds), 2.0)
    log(f"Starting PFL scanner url={pfl_url!r} cadence_seconds={check_every:g} dry_run={bool(args.dry_run)}")

    try:
        page = open_pfl_page(pfl_url, browser_holder)
        page.wait_for_timeout(2500)
        prepared_matches = prepare_card_matches(page, cached_events)
        update_state(
            cached_pfl_fight_matches=[
                {
                    "pfl_fight_name": fight_name,
                    "betfair_event_id": event.event_id,
                    "betfair_event_name": event.event_name,
                }
                for fight_name, event in sorted(prepared_matches.items())
            ]
        )
        log(f"Prepared {len(prepared_matches)} PFL-to-Betfair fight matches before live scanning.")
        while not STOP_EVENT.is_set():
            update_state(last_check_time=iso_utc(), pfl_event_url=pfl_url)
            try:
                if page is None:
                    page = open_pfl_page(pfl_url, browser_holder)
                live_display = get_pfl_live_fight(page)
                if live_display:
                    update_state(last_detected_live_fight=live_display)
                    matched_event = prepared_matches.get(normalize(live_display))
                    match = (
                        MatchResult(matched_event, 100.0, "startup cache")
                        if matched_event
                        else match_betfair_event(live_display, cached_events)
                    )
                    if match.event is None:
                        unmatched_key = normalize(live_display)
                        if unmatched_key != last_unmatched_key:
                            log(
                                f"No safe cached Betfair match for {live_display!r}: "
                                f"{match.reason} (score={match.score:.1f}); Slack alert withheld."
                            )
                            last_unmatched_key = unmatched_key
                    else:
                        last_unmatched_key = ""
                        live_key = f"live|{normalize(pfl_url)}|{match.event.event_id}"
                        if live_key not in known_alerts and deliver_live_alert(
                            fight_name=live_display,
                            event_id=match.event.event_id,
                            pfl_url=pfl_url,
                            webhook_url=webhook_url,
                            dry_run=bool(args.dry_run),
                        ):
                            known_alerts.add(live_key)
            except Exception as exc:
                log(f"PFL page error: {exc}")
                close_browser(browser_holder)
                page = None

            if args.once:
                return 0
            STOP_EVENT.wait(check_every)
    except KeyboardInterrupt:
        log("Interrupted.")
        return 130
    finally:
        close_browser(browser_holder)
    log("PFL scanner stopped.")
    return 0


def run_self_test() -> int:
    validate_pfl_url("https://pflmma.com/event/pfl-example")
    assert split_fight_name("Fighter A v Fighter B") == ("Fighter A", "Fighter B")
    assert split_fight_name("Fighter A vs. Fighter B") == ("Fighter A", "Fighter B")
    events = [
        BetfairEvent("37826791", "Fighter B v Fighter A"),
        BetfairEvent("111", "Someone Else v Another Fighter"),
    ]
    exact = match_betfair_event("Fighter A v Fighter B", events)
    assert exact.event and exact.event.event_id == "37826791" and exact.score == 100
    assert format_slack_message("Fighter A v Fighter B", "37826791") == (
        ":pfl: Fighter A v Fighter B - Live now (Betfair Event ID: 37826791)"
    )
    parsed = parse_betfair_events(
        [{"event": {"id": "37826791", "name": "Fighter A v Fighter B", "openDate": "2026-08-01T20:00:00Z"}}]
    )
    assert parsed[0].event_id == "37826791"
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
    parser = argparse.ArgumentParser(description="Scan a PFL event page for live fights and send Betfair-matched alerts.")
    parser.add_argument("--self-test", action="store_true", help="Run fixture-based checks and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Print Slack alerts without sending them.")
    parser.add_argument("--pfl-url", default="", help="Current PFL event URL.")
    parser.add_argument("--check-every-seconds", type=float, default=CHECK_EVERY_SECONDS)
    parser.add_argument("--once", action="store_true", help="Run one check cycle and exit.")
    return parser.parse_args()


def handle_stop_signal(signum: int, frame: Any) -> None:
    STOP_EVENT.set()


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
