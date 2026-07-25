#!/usr/bin/env python3
"""Monitor ESPN and UFC.com for UFC walkouts, live starts, and fight endings."""

from __future__ import annotations

import argparse
import json
import os
import signal
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import requests
import urllib3
from dotenv import load_dotenv


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
RUNTIME_DIR = PROJECT_ROOT / "runtime"
CONFIG_DIR = RUNTIME_DIR / "config"
STATE_PATH = CONFIG_DIR / "ufc_live_start_scanner.json"

ESPN_URL = (
    "https://site.web.api.espn.com/apis/personalized/v2/scoreboard/header"
    "?sport=mma&league=ufc"
)
CHECK_EVERY_SECONDS = 3
SLACK_WEBHOOK_ENV_NAME = "Slack_Webhook_TIP"
UFC_SLACK_WEBHOOK_ENV_NAME = "UFC_IS_IT_INPLAY_WEBHOOK_URL"
PLACEHOLDER_PREFIXES = ("YOUR_", "PASTE_", "CHANGE_ME", "TODO")
STOP_EVENT = threading.Event()
UTC_TZ = timezone.utc


load_dotenv(PROJECT_ROOT / ".env")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def log(message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


def iso_utc(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC_TZ)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize(text: str) -> str:
    return " ".join((text or "").casefold().split())


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


def resolve_ufc_url(cli_url: str) -> str:
    return cli_url.strip() or str(read_json().get("ufc_event_url", "") or "").strip()


def validate_ufc_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("UFC event URL must be an absolute http(s) URL.")
    if parsed.netloc.casefold() != "ufc.com" and not parsed.netloc.casefold().endswith(".ufc.com"):
        raise ValueError("UFC event URL must be on ufc.com.")


def save_ufc_url(url: str) -> None:
    data = read_json()
    previous_url = str(data.get("ufc_event_url", "") or "")
    data["ufc_event_url"] = url
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


def record_alert(
    *,
    key: str,
    alert_type: str,
    fight_name: str,
    ufc_url: str,
    dry_run: bool,
) -> None:
    data = read_json()
    keys = [str(existing) for existing in data.get("alerted_fight_keys", []) or []]
    if key not in keys:
        keys.append(key)
    fights = list(data.get("alerted_fights", []) or [])
    fights.append(
        {
            "fight_name": fight_name,
            "alert_type": alert_type,
            "ufc_event_url": ufc_url,
            "betfair_event_id": "Not matched",
            "betfair_market_id": "",
            "dry_run": bool(dry_run),
            "alerted_at": iso_utc(),
        }
    )
    updates: dict[str, Any] = {
        "ufc_event_url": ufc_url,
        "alerted_fight_keys": keys[-150:],
        "alerted_fights": fights[-150:],
        "last_slack_alert_sent": (
            f"Dry-run: {alert_type} for {fight_name}"
            if dry_run
            else f"{alert_type} for {fight_name} at {iso_utc()}"
        ),
    }
    if alert_type == "Fight started":
        updates["last_detected_live_fight"] = fight_name
    data.update(updates)
    write_json(data)


def deliver_alert(
    *,
    title: str,
    message: str,
    key: str,
    alert_type: str,
    fight_name: str,
    ufc_url: str,
    webhook_url: str,
    dry_run: bool,
) -> bool:
    log(f"{title}: {fight_name}")
    if dry_run:
        log(f"DRY RUN Slack message: {message!r}")
    else:
        try:
            send_slack_message(webhook_url, message)
        except Exception as exc:
            log(f"slack_failed alert_type={alert_type!r} fight_name={fight_name!r} error={exc}")
            return False
        log(f"slack_sent alert_type={alert_type!r} fight_name={fight_name!r}")
    record_alert(
        key=key,
        alert_type=alert_type,
        fight_name=fight_name,
        ufc_url=ufc_url,
        dry_run=dry_run,
    )
    return True


def espn_fight_name(fight: dict[str, Any]) -> str:
    names = [str(competitor.get("displayName", "TBD")) for competitor in fight.get("competitors", [])]
    return " vs ".join(names)


def espn_status_text(fight: dict[str, Any]) -> str:
    full_status = fight.get("fullStatus", {}) or {}
    parts = [
        fight.get("status", ""),
        fight.get("summary", ""),
        fight.get("description", ""),
        full_status.get("name", ""),
        full_status.get("description", ""),
        full_status.get("detail", ""),
        full_status.get("shortDetail", ""),
    ]
    return " | ".join(str(part) for part in parts if part)


def espn_fight_ended(fight: dict[str, Any]) -> bool:
    full_status = fight.get("fullStatus", {}) or {}
    state = str(full_status.get("state", "")).casefold()
    text = espn_status_text(fight).casefold()
    return bool(full_status.get("completed", False) or state == "post" or "final" in text)


def espn_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        events = payload["sports"][0]["leagues"][0]["events"]
    except (KeyError, IndexError, TypeError):
        return []
    return sorted(events, key=lambda item: item.get("matchNumber", 999), reverse=True)


def check_espn(
    *,
    ufc_url: str,
    webhook_url: str,
    known_alerts: set[str],
    ended_on_first_check: set[str],
    first_check: bool,
    dry_run: bool,
) -> None:
    response = requests.get(ESPN_URL, verify=False, timeout=8)
    response.raise_for_status()
    fights = espn_events(response.json())
    log(f"ESPN fights found: {len(fights)}")

    for fight in fights:
        name = espn_fight_name(fight)
        normalized_name = normalize(name)
        status_text = espn_status_text(fight).casefold()
        ended = espn_fight_ended(fight)

        if first_check and ended:
            ended_on_first_check.add(normalized_name)

        walkout_key = f"walkout|{normalized_name}"
        if "walkout" in status_text and walkout_key not in known_alerts:
            if deliver_alert(
                title="UFC Walkout Alert",
                message=f"🚶 WALKOUTS\n\n{name}",
                key=walkout_key,
                alert_type="Walkouts",
                fight_name=name,
                ufc_url=ufc_url,
                webhook_url=webhook_url,
                dry_run=dry_run,
            ):
                known_alerts.add(walkout_key)

        ended_key = f"ended|{normalized_name}"
        if (
            not first_check
            and ended
            and normalized_name not in ended_on_first_check
            and ended_key not in known_alerts
        ):
            if deliver_alert(
                title="UFC Fight Ended",
                message=f"✅ FIGHT ENDED\n\n{name}",
                key=ended_key,
                alert_type="Fight ended",
                fight_name=name,
                ufc_url=ufc_url,
                webhook_url=webhook_url,
                dry_run=dry_run,
            ):
                known_alerts.add(ended_key)


def extract_live_fight(lines: list[str]) -> str | None:
    cleaned = [line.strip() for line in lines if line.strip()]
    for index, line in enumerate(cleaned):
        if line.upper() == "VS" and index > 0 and index + 1 < len(cleaned):
            return f"{cleaned[index - 1]} vs {cleaned[index + 1]}"
    return None


def get_ufc_live_fight(page: Any) -> str | None:
    try:
        page.reload(wait_until="commit", timeout=8000)
    except Exception as exc:
        log(f"UFC reload warning: {exc}")
    page.wait_for_timeout(3000)

    fights = page.locator(".c-listing-fight")
    fight_count = fights.count()
    log(f"UFC fight blocks found: {fight_count}")
    if fight_count == 0:
        log("UFC parse warning: no .c-listing-fight blocks found")
        return None

    for index in range(fight_count):
        fight = fights.nth(index)
        try:
            live_banner = fight.locator(".c-listing-fight__banner--live")
            if live_banner.count() == 0 or not live_banner.first.is_visible():
                continue
            display = extract_live_fight(fight.inner_text(timeout=1500).splitlines())
            if display:
                return display
        except Exception as exc:
            log(f"UFC fight block warning index={index} error={exc}")
    return None


def open_ufc_page(ufc_url: str, browser_holder: dict[str, Any]) -> Any:
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
    context = browser.new_context(ignore_https_errors=True)
    page = context.new_page()
    browser_holder.update(
        {
            "playwright": playwright,
            "browser": browser,
            "context": context,
            "page": page,
        }
    )
    log("Opening UFC.com...")
    try:
        page.goto(ufc_url, wait_until="commit", timeout=8000)
    except Exception as exc:
        log(f"Initial UFC navigation warning: {exc}")
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
    ufc_url = resolve_ufc_url(args.ufc_url)
    if not ufc_url:
        log("No UFC event URL configured.")
        return 2
    validate_ufc_url(ufc_url)
    save_ufc_url(ufc_url)

    webhook_url, webhook_source = slack_webhook_url()
    log(f"Slack webhook source: {webhook_source}")
    if not args.dry_run and is_placeholder(webhook_url):
        raise RuntimeError(f"{UFC_SLACK_WEBHOOK_ENV_NAME} or {SLACK_WEBHOOK_ENV_NAME} missing")

    known_alerts = alerted_keys()
    ended_on_first_check: set[str] = set()
    first_espn_check = True
    browser_holder: dict[str, Any] = {}
    page: Any | None = None
    check_every = max(float(args.check_every_seconds), 2.0)
    log(
        f"Starting UFC scanner url={ufc_url!r} "
        f"cadence_seconds={check_every:g} dry_run={bool(args.dry_run)}"
    )

    try:
        while not STOP_EVENT.is_set():
            update_state(last_check_time=iso_utc(), ufc_event_url=ufc_url)
            try:
                check_espn(
                    ufc_url=ufc_url,
                    webhook_url=webhook_url,
                    known_alerts=known_alerts,
                    ended_on_first_check=ended_on_first_check,
                    first_check=first_espn_check,
                    dry_run=bool(args.dry_run),
                )
                first_espn_check = False
            except Exception as exc:
                log(f"ESPN error: {exc}")

            try:
                if page is None:
                    page = open_ufc_page(ufc_url, browser_holder)
                live_display = get_ufc_live_fight(page)
                if live_display:
                    live_key = f"live|{normalize(ufc_url)}|{normalize(live_display)}"
                    update_state(last_detected_live_fight=live_display)
                    if live_key not in known_alerts and deliver_alert(
                        title="UFC Fight Started",
                        message=f":ufc: FIGHT STARTED / LIVE NOW\n\n{live_display}",
                        key=live_key,
                        alert_type="Fight started",
                        fight_name=live_display,
                        ufc_url=ufc_url,
                        webhook_url=webhook_url,
                        dry_run=bool(args.dry_run),
                    ):
                        known_alerts.add(live_key)
            except Exception as exc:
                log(f"UFC.com error: {exc}")
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
    log("UFC scanner stopped.")
    return 0


def run_self_test() -> int:
    assert normalize("  Zhang   WEILI ") == "zhang weili"
    assert extract_live_fight(["Main card", "Fighter A", "VS", "Fighter B", "LIVE NOW"]) == (
        "Fighter A vs Fighter B"
    )
    assert extract_live_fight(["Fighter A", "Fighter B"]) is None
    validate_ufc_url("https://www.ufc.com/event/example")

    event = {
        "competitors": [{"displayName": "Fighter A"}, {"displayName": "Fighter B"}],
        "fullStatus": {"state": "post", "completed": True, "shortDetail": "Final"},
    }
    assert espn_fight_name(event) == "Fighter A vs Fighter B"
    assert espn_fight_ended(event)
    assert espn_events({"sports": [{"leagues": [{"events": [event]}]}]}) == [event]

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
    parser = argparse.ArgumentParser(
        description="Scan ESPN and UFC.com for walkouts, live starts, and fight endings."
    )
    parser.add_argument("--self-test", action="store_true", help="Run fixture-based checks and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Print Slack alerts without sending them.")
    parser.add_argument("--ufc-url", default="", help="Current UFC event URL.")
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
