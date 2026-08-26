#!/usr/bin/env python3
"""Monitor configured Flashscore ATP Challenger tournaments and alert Slack."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from app.tennis_challenger import (
    CONFIG_PATH,
    STATE_PATH,
    atomic_write_json,
    normalize_tournament_url,
    read_config,
    read_state,
    tournament_label_from_url,
    utc_timestamp,
)


try:
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)
except AttributeError:
    pass


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

STOP_EVENT = threading.Event()
COMPLETED_STATUSES = {"finished", "retired", "walkover", "abandoned", "cancelled", "canceled", "awarded"}
ALERT_FLAG_BY_TYPE = {
    "serve_detected": "serve_detected",
    "match_started": "match_started",
    "set_1_complete": "set_1_complete",
    "set_2_complete": "set_2_complete",
    "match_complete": "match_complete",
}
DEFAULT_POLL_SECONDS = 3.0
DEFAULT_RELOAD_MINUTES = 15.0
MAX_ALERT_HISTORY = 500


ROW_EXTRACTOR = r"""
rows => rows.map(row => {
  const text = selector => {
    const node = row.querySelector(selector);
    return node ? String(node.textContent || '').replace(/\s+/g, ' ').trim() : '';
  };
  const value = (side, index) => text(`.event__part--${side}.event__part--${index}`);
  const link = row.querySelector('a.eventRowLink[href*="/match/tennis/"]');
  const serve = row.querySelector('svg.serve-ico');
  const classes = String(row.className || '');
  const id = String(row.id || '').replace(/^.*_/, '');
  return {
    id,
    url: link ? link.href : '',
    player1: text('.event__homeParticipant'),
    player2: text('.event__awayParticipant'),
    raw_status: text('.event__stageTime'),
    row_classes: classes,
    set_score: {
      home: text('.event__score--home'),
      away: text('.event__score--away')
    },
    set_parts: [1, 2, 3, 4, 5].map(index => ({
      home: value('home', index),
      away: value('away', index)
    })),
    current_points: {
      home: value('home', 6),
      away: value('away', 6)
    },
    server_side: serve && String(serve.className.baseVal || serve.className).includes('serveHome')
      ? 'home'
      : serve && String(serve.className.baseVal || serve.className).includes('serveAway')
        ? 'away'
        : '',
    is_live: classes.includes('event__match--live'),
    is_scheduled: classes.includes('event__match--scheduled')
  };
}).filter(item => item.id && item.url && item.player1 && item.player2)
"""


def log(message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


def clean_score_value(value: Any) -> str | int:
    raw = str(value or "").strip()
    if raw.upper() in {"A", "AD"}:
        return "A"
    match = re.match(r"^-?\d+", raw)
    return int(match.group(0)) if match else ""


def normalize_status(raw_status: str, row_classes: str = "", is_live: bool = False, is_scheduled: bool = False) -> str:
    text = " ".join(str(raw_status or "").casefold().split())
    if re.search(r"\b(retired|walkover|abandoned|cancelled|canceled|awarded)\b", text):
        return re.search(r"\b(retired|walkover|abandoned|cancelled|canceled|awarded)\b", text).group(1)  # type: ignore[union-attr]
    if re.search(r"\b(finished|final|ended)\b", text):
        return "finished"
    if re.search(r"\b(suspended|interrupted|delayed)\b", text):
        return "suspended"
    if is_live or "event__match--live" in row_classes or re.search(r"\b(set\s*\d+|live|tiebreak)\b", text):
        return "live"
    if is_scheduled or "event__match--scheduled" in row_classes or re.search(r"\b\d{1,2}:\d{2}\b", text):
        return "scheduled"
    return "unknown"


def current_set_number(raw_status: str, status: str) -> int | None:
    match = re.search(r"\bset\s*([1-5])\b", str(raw_status or ""), re.IGNORECASE)
    if match:
        return int(match.group(1))
    if status == "live":
        return 1
    return None


def normalize_scraped_match(raw: dict[str, Any], source_url: str, tournament: str) -> dict[str, Any]:
    status = normalize_status(
        str(raw.get("raw_status", "")),
        str(raw.get("row_classes", "")),
        bool(raw.get("is_live")),
        bool(raw.get("is_scheduled")),
    )
    current_set = current_set_number(str(raw.get("raw_status", "")), status)
    sets: list[dict[str, Any]] = []
    for pair in raw.get("set_parts", []) or []:
        if not isinstance(pair, dict):
            continue
        home = clean_score_value(pair.get("home"))
        away = clean_score_value(pair.get("away"))
        if home != "" and away != "":
            sets.append({"home": home, "away": away})
    current_game = {"home": "", "away": ""}
    if status == "live" and current_set and len(sets) >= current_set:
        current_game = dict(sets[current_set - 1])
    server_side = str(raw.get("server_side", "") or "")
    player1 = str(raw.get("player1", "") or "").strip()
    player2 = str(raw.get("player2", "") or "").strip()
    server = player1 if server_side == "home" else player2 if server_side == "away" else ""
    return {
        "id": str(raw.get("id", "") or ""),
        "url": str(raw.get("url", "") or ""),
        "source_url": source_url,
        "tournament": tournament,
        "player1": player1,
        "player2": player2,
        "status": "finished" if status in COMPLETED_STATUSES else status,
        "finish_reason": status if status in COMPLETED_STATUSES and status != "finished" else "",
        "display_status": str(raw.get("raw_status", "") or status.title()),
        "start_time": str(raw.get("raw_status", "") or "") if status == "scheduled" else "",
        "set_score": {
            "home": clean_score_value((raw.get("set_score") or {}).get("home")),
            "away": clean_score_value((raw.get("set_score") or {}).get("away")),
        },
        "sets": sets,
        "current_set_number": current_set,
        "current_game": current_game,
        "current_points": {
            "home": clean_score_value((raw.get("current_points") or {}).get("home")),
            "away": clean_score_value((raw.get("current_points") or {}).get("away")),
        },
        "server": server,
        "server_side": server_side,
    }


def satisfied_alerts(match: dict[str, Any]) -> dict[str, bool]:
    status = str(match.get("status", ""))
    current_set = int(match.get("current_set_number") or 0)
    completed = status == "finished"
    set_count = len(match.get("sets", []) or [])
    return {
        "serve_detected": bool(status == "scheduled" and match.get("server")),
        "match_started": status == "live" or completed,
        "set_1_complete": current_set >= 2 or (completed and set_count >= 1),
        "set_2_complete": current_set >= 3 or (completed and set_count >= 2),
        "match_complete": completed,
    }


def pending_alerts(previous: dict[str, Any] | None, match: dict[str, Any]) -> list[str]:
    sent = dict((previous or {}).get("alerts_sent", {}) or {})
    satisfied = satisfied_alerts(match)
    if previous is None:
        if match.get("status") == "finished":
            return []
        if satisfied["serve_detected"]:
            return ["serve_detected"]
        if match.get("status") == "live":
            return ["match_started"]
        return []
    order = ["serve_detected", "match_started", "set_1_complete", "set_2_complete", "match_complete"]
    alerts: list[str] = []
    for alert_type in order:
        if alert_type == "serve_detected" and match.get("status") != "scheduled":
            continue
        if satisfied[alert_type] and not sent.get(ALERT_FLAG_BY_TYPE[alert_type]):
            alerts.append(alert_type)
    return alerts


def hydrate_initial_alert_flags(match: dict[str, Any]) -> dict[str, bool]:
    satisfied = satisfied_alerts(match)
    if match.get("status") == "finished":
        return satisfied
    if match.get("status") == "live":
        satisfied["serve_detected"] = True
        satisfied["set_1_complete"] = False
        satisfied["set_2_complete"] = False
        satisfied["match_complete"] = False
        satisfied["match_started"] = False
        return satisfied
    return {key: False for key in satisfied}


def score_text(match: dict[str, Any]) -> str:
    parts = [f"{item['home']}-{item['away']}" for item in match.get("sets", []) or []]
    aggregate = match.get("set_score", {}) or {}
    aggregate_text = ""
    if aggregate.get("home", "") != "" and aggregate.get("away", "") != "":
        aggregate_text = f"sets {aggregate['home']}-{aggregate['away']}"
    if aggregate_text and parts:
        return f"{aggregate_text} ({', '.join(parts)})"
    return aggregate_text or ", ".join(parts) or "score unavailable"


def slack_message(alert_type: str, match: dict[str, Any]) -> str:
    players = f"{match.get('player1', '?')} v {match.get('player2', '?')}"
    tournament = str(match.get("tournament", "ATP Challenger"))
    url = str(match.get("url", ""))
    common = [f"*Match:* {players}", f"*Tournament:* {tournament}"]
    if alert_type == "serve_detected":
        lines = ["🎾 *ATP Challenger — toss decided*", *common]
        lines.append(f"*First server:* {match.get('server', 'Detected')}")
        if match.get("start_time"):
            lines.append(f"*Scheduled:* {match['start_time']}")
        lines.append("Prepare to turn the match in play when play begins.")
    elif alert_type == "match_started":
        lines = ["🟢 *ATP Challenger — match started*", *common, f"*Score:* {score_text(match)}", "*Action:* TIP required."]
    elif alert_type == "set_1_complete":
        lines = ["✅ *ATP Challenger — Set 1 complete*", *common, f"*Score:* {score_text(match)}", "*Action:* Set 1 settlement required."]
    elif alert_type == "set_2_complete":
        lines = ["✅ *ATP Challenger — Set 2 complete*", *common, f"*Score:* {score_text(match)}", "*Action:* Set 2 settlement required."]
    else:
        reason = str(match.get("finish_reason", "") or "")
        heading = "🏁 *ATP Challenger — match complete*" if not reason else f"⚠️ *ATP Challenger — match ended ({reason.upper()})*"
        action = "*Action:* Match settlement required." if not reason else "*Action:* Manual review and match settlement required."
        lines = [heading, *common, f"*Final score:* {score_text(match)}", action]
    if url:
        lines.append(f"<{url}|Open Flashscore match>")
    return "\n".join(lines)


def send_slack(webhook_url: str, message: str) -> None:
    response = requests.post(webhook_url, json={"text": message}, timeout=15)
    if response.status_code != 200:
        raise RuntimeError(f"Slack webhook returned status {response.status_code}")


def slack_webhook_url() -> str:
    return os.getenv("Webhook_Challenger", "").strip()


def configured_links() -> list[str]:
    links: list[str] = []
    for value in read_config().get("tournament_links", []) or []:
        try:
            links.append(normalize_tournament_url(str(value)))
        except ValueError as exc:
            log(f"Ignoring invalid saved tournament link: {exc}")
    return list(dict.fromkeys(links))


def extract_page_rows(page: Any) -> list[dict[str, Any]]:
    rows = page.eval_on_selector_all('[data-event-row="true"]', ROW_EXTRACTOR)
    return [row for row in rows or [] if isinstance(row, dict)]


def _handle_stop(_signum: int, _frame: Any) -> None:
    STOP_EVENT.set()


def write_runtime_state(
    matches: dict[str, dict[str, Any]],
    alerts: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    *,
    last_error: str = "",
) -> None:
    atomic_write_json(
        STATE_PATH,
        {
            "status": "running" if not STOP_EVENT.is_set() else "stopped",
            "last_sweep_at": utc_timestamp(),
            "last_error": last_error,
            "sources": sources,
            "matches": list(matches.values()),
            "alerts": alerts[-MAX_ALERT_HISTORY:],
        },
    )


def run_watcher(poll_seconds: float, reload_minutes: float) -> int:
    webhook = slack_webhook_url()
    if not webhook:
        raise RuntimeError(
            "Slack is not configured. Set Webhook_Challenger in the Hub environment."
        )
    if not configured_links():
        raise RuntimeError("No ATP Challenger tournament links are saved in the Hub.")

    try:
        from playwright.sync_api import Page, sync_playwright
    except ImportError as exc:
        raise RuntimeError("Playwright is not installed. Install requirements.txt and Playwright Chromium.") from exc

    prior = read_state()
    matches = {
        str(item.get("id", "")): item
        for item in prior.get("matches", []) or []
        if isinstance(item, dict) and item.get("id")
    }
    alerts = [item for item in prior.get("alerts", []) or [] if isinstance(item, dict)]
    pages: dict[str, Page] = {}
    loaded_at: dict[str, float] = {}
    source_errors: dict[str, str] = {}

    with sync_playwright() as playwright:
        launch_options: dict[str, Any] = {
            "headless": True,
            "args": ["--disable-dev-shm-usage", "--no-sandbox"],
        }
        chrome_binary = os.getenv("CHROME_BINARY", "").strip()
        if chrome_binary and Path(chrome_binary).is_file():
            launch_options["executable_path"] = chrome_binary
        browser = playwright.chromium.launch(**launch_options)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-GB",
            timezone_id="Europe/London",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
            ),
        )
        log(f"Watcher started. Config: {CONFIG_PATH}. State: {STATE_PATH}.")

        try:
            while not STOP_EVENT.is_set():
                sweep_started = time.monotonic()
                links = configured_links()
                if not links:
                    write_runtime_state(matches, alerts, [], last_error="No tournament links are configured.")
                    STOP_EVENT.wait(max(1.0, poll_seconds))
                    continue

                for old_url in set(pages) - set(links):
                    pages.pop(old_url).close()
                    loaded_at.pop(old_url, None)
                    source_errors.pop(old_url, None)

                for url in links:
                    page = pages.get(url)
                    needs_reload = page is None or time.monotonic() - loaded_at.get(url, 0) >= reload_minutes * 60
                    try:
                        if page is None:
                            page = context.new_page()
                            page.set_default_timeout(15000)
                            pages[url] = page
                        if needs_reload:
                            log(f"Loading {tournament_label_from_url(url)}")
                            page.goto(url, wait_until="domcontentloaded", timeout=30000)
                            page.wait_for_selector('[data-event-row="true"]', timeout=20000)
                            loaded_at[url] = time.monotonic()
                        raw_matches = extract_page_rows(page)
                        if not raw_matches:
                            raise RuntimeError("Tournament page returned no match rows")
                        source_errors.pop(url, None)
                    except Exception as exc:
                        source_errors[url] = str(exc)
                        loaded_at[url] = 0
                        log(f"Tournament read failed for {tournament_label_from_url(url)}: {exc}")
                        continue

                    tournament = tournament_label_from_url(url)
                    for raw in raw_matches:
                        scraped = normalize_scraped_match(raw, url, tournament)
                        match_id = scraped["id"]
                        previous = matches.get(match_id)
                        now = utc_timestamp()
                        next_match = {
                            **(previous or {}),
                            **scraped,
                            "first_seen_at": (previous or {}).get("first_seen_at", now),
                            "last_checked_at": now,
                            "last_error": "",
                        }
                        if previous is None:
                            next_match["alerts_sent"] = hydrate_initial_alert_flags(next_match)
                        else:
                            next_match["alerts_sent"] = dict(previous.get("alerts_sent", {}) or {})

                        for alert_type in pending_alerts(previous, next_match):
                            try:
                                message = slack_message(alert_type, next_match)
                                send_slack(webhook, message)
                            except Exception as exc:
                                next_match["last_error"] = f"Slack alert failed: {exc}"
                                log(f"Slack alert failed for {next_match['player1']} v {next_match['player2']}: {exc}")
                                continue
                            next_match["alerts_sent"][ALERT_FLAG_BY_TYPE[alert_type]] = True
                            alert = {
                                "timestamp": now,
                                "type": alert_type,
                                "match_id": match_id,
                                "match": f"{next_match['player1']} v {next_match['player2']}",
                                "tournament": tournament,
                                "message": message,
                            }
                            alerts.append(alert)
                            next_match["last_alert_at"] = now
                            log(f"Slack alert sent: {alert_type} — {alert['match']}")
                        matches[match_id] = next_match

                active_urls = set(links)
                for match in matches.values():
                    if match.get("source_url") not in active_urls:
                        match["out_of_scope"] = True
                    else:
                        match.pop("out_of_scope", None)
                matches = {key: value for key, value in matches.items() if not value.get("out_of_scope")}
                sources = [
                    {
                        "url": url,
                        "label": tournament_label_from_url(url),
                        "status": "error" if url in source_errors else "ok",
                        "error": source_errors.get(url, ""),
                    }
                    for url in links
                ]
                error_message = "; ".join(f"{tournament_label_from_url(url)}: {error}" for url, error in source_errors.items())
                write_runtime_state(matches, alerts, sources, last_error=error_message)
                elapsed = time.monotonic() - sweep_started
                STOP_EVENT.wait(max(0.25, poll_seconds - elapsed))
        finally:
            for page in pages.values():
                try:
                    page.close()
                except Exception:
                    pass
            context.close()
            browser.close()

    write_runtime_state(matches, alerts, [], last_error="")
    log("Watcher stopped cleanly.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--poll-seconds", type=float, default=DEFAULT_POLL_SECONDS)
    parser.add_argument("--reload-minutes", type=float, default=DEFAULT_RELOAD_MINUTES)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)
    try:
        return run_watcher(max(1.0, args.poll_seconds), max(1.0, args.reload_minutes))
    except Exception as exc:
        previous = read_state()
        atomic_write_json(
            STATE_PATH,
            {
                **previous,
                "status": "failed",
                "last_sweep_at": utc_timestamp(),
                "last_error": str(exc),
            },
        )
        log(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
