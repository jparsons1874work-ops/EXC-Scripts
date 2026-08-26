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
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.tennis_challenger import (
    CONFIG_PATH,
    STATE_PATH,
    atomic_write_json,
    create_betfair_client,
    fetch_upcoming_betfair_tennis_events,
    match_betfair_event,
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
BETFAIR_REFRESH_SECONDS = 5 * 60
FIXTURES_REFRESH_SECONDS = 60
EVENT_ROW_SELECTOR = ".event__match[id]"
UK_TIMEZONE = ZoneInfo("Europe/London")
FLASHSCORE_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome Safari/537.36"
)


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
        "scheduled_at": str(raw.get("scheduled_at", "") or ""),
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
    if satisfied["match_complete"] and not sent.get(ALERT_FLAG_BY_TYPE["match_complete"]):
        return ["match_complete"]
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
    betfair_event_id = str(match.get("betfair_event_id", "") or "")
    common = [
        f"*Match:* {players}",
        f"*Tournament:* {tournament}",
        f"*Betfair event ID:* `{betfair_event_id}`" if betfair_event_id else "*Betfair event ID:* Not matched",
    ]
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


def extract_feed_config(html: str, source_url: str) -> dict[str, str]:
    country_match = re.search(r"country_id\s*=\s*(\d+)", html)
    tournament_match = re.search(r'tournament_id\s*=\s*"([A-Za-z0-9]+)"', html)
    sport_match = re.search(r"sport_id\s*:\s*(\d+)", html)
    feed_sign_match = re.search(r'feed_sign\\?"\s*:\s*\\?"([^"\\]+)', html)
    if not country_match or not tournament_match or not feed_sign_match:
        raise RuntimeError("Flashscore page did not expose its tournament feed configuration")
    offset = datetime.now(UK_TIMEZONE).utcoffset()
    timezone_hour = int((offset.total_seconds() if offset else 0) // 3600)
    sport_id = sport_match.group(1) if sport_match else "2"
    tournament_id = tournament_match.group(1)
    country_id = country_match.group(1)
    feed_name = f"t_{sport_id}_{country_id}_{tournament_id}_{timezone_hour}_en_1"
    return {
        "source_url": source_url,
        "feed_url": f"https://global.flashscore.ninja/2/x/feed/{feed_name}",
        "feed_sign": feed_sign_match.group(1),
        "tournament_id": tournament_id,
        "country_id": country_id,
    }


def load_feed_config(session: requests.Session, source_url: str, label: str) -> dict[str, str]:
    log(f"{label}: loading tournament feed configuration")
    response = session.get(source_url, timeout=25)
    response.raise_for_status()
    config = extract_feed_config(response.text, source_url)
    log(
        f"{label}: feed configured (tournament {config['tournament_id']}, "
        f"country {config['country_id']})"
    )
    return config


def feed_fields(chunk: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for item in chunk.split("¬"):
        key, separator, value = item.partition("÷")
        if separator and key and key not in fields:
            fields[key] = value
    return fields


def feed_start_time(value: str) -> str:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return "Scheduled"
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone(UK_TIMEZONE).strftime("%d.%m. %H:%M")


def feed_start_datetime(value: str) -> str:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return ""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def parse_tournament_feed(payload: str, source_url: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for chunk in str(payload or "").split("¬~"):
        if not chunk.startswith("AA÷"):
            continue
        fields = feed_fields(chunk)
        match_id = fields.get("AA", "").strip()
        player1 = (fields.get("AE") or fields.get("CX") or "").strip()
        player2 = (fields.get("AF") or "").strip()
        if not match_id or not player1 or not player2:
            continue

        status_code = fields.get("AB", "")
        if status_code == "2":
            status = "live"
        elif status_code == "3":
            status = "finished"
        else:
            status = "scheduled"
        set_score = {
            "home": clean_score_value(fields.get("AG")),
            "away": clean_score_value(fields.get("AH")),
        }
        completed_sets = int(set_score["home"] or 0) + int(set_score["away"] or 0)
        raw_status = (
            f"Set {completed_sets + 1}"
            if status == "live"
            else "Finished"
            if status == "finished"
            else feed_start_time(fields.get("AD", ""))
        )
        set_keys = [("BA", "BB"), ("BC", "BD"), ("BE", "BF"), ("BG", "BH"), ("BI", "BJ")]
        set_parts = [
            {"home": fields.get(home_key, ""), "away": fields.get(away_key, "")}
            for home_key, away_key in set_keys
        ]
        server_side = "home" if fields.get("AI") == "y" else "away" if fields.get("AJ") == "y" else ""
        rows.append(
            {
                "id": match_id,
                "url": f"https://www.flashscore.com/match/tennis/{match_id}/",
                "player1": player1,
                "player2": player2,
                "scheduled_at": feed_start_datetime(fields.get("AD", "")),
                "raw_status": raw_status,
                "row_classes": f"event__match event__match--{status}",
                "set_score": set_score,
                "set_parts": set_parts,
                "current_points": {
                    "home": fields.get("WA", ""),
                    "away": fields.get("WB", ""),
                },
                "server_side": server_side,
                "is_live": status == "live",
                "is_scheduled": status == "scheduled",
            }
        )
    return rows


def fetch_tournament_feed(
    session: requests.Session,
    config: dict[str, str],
    label: str,
) -> list[dict[str, Any]]:
    response = session.get(
        config["feed_url"],
        headers={
            "x-fsign": config["feed_sign"],
            "Referer": config["source_url"],
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
        params={"_": int(time.time() * 1000)},
        timeout=20,
    )
    response.raise_for_status()
    rows = parse_tournament_feed(response.text, config["source_url"])
    if not rows:
        raise RuntimeError(
            f"Flashscore feed returned no matches (HTTP {response.status_code}, "
            f"{len(response.text)} characters)"
        )
    age = response.headers.get("Age", "unknown")
    log(f"{label}: feed returned {len(rows)} match(es), cache age {age}s")
    return rows


def extract_fixtures_feed(html: str) -> str:
    match = re.search(
        r"cjs\.initialFeeds\[\s*['\"]fixtures['\"]\s*\]\s*=\s*\{\s*data\s*:\s*`(.*?)`",
        str(html or ""),
        re.DOTALL,
    )
    if not match:
        raise RuntimeError("Flashscore fixtures page did not expose its fixtures data")
    return match.group(1)


def fetch_tournament_fixtures(
    session: requests.Session,
    config: dict[str, str],
    label: str,
) -> list[dict[str, Any]]:
    fixtures_url = config["source_url"].rstrip("/") + "/fixtures/"
    response = session.get(
        fixtures_url,
        headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
        params={"_": int(time.time() * 1000)},
        timeout=25,
    )
    response.raise_for_status()
    rows = parse_tournament_feed(extract_fixtures_feed(response.text), config["source_url"])
    log(f"{label}: fixtures page returned {len(rows)} named future match(es)")
    return rows


def extract_page_rows(page: Any) -> list[dict[str, Any]]:
    rows = page.eval_on_selector_all(EVENT_ROW_SELECTOR, ROW_EXTRACTOR)
    return [row for row in rows or [] if isinstance(row, dict)]


def page_diagnostic(page: Any) -> str:
    details: list[str] = []
    try:
        details.append(f"url={page.url}")
    except Exception:
        pass
    try:
        details.append(f"title={page.title()!r}")
    except Exception:
        pass
    try:
        body = " ".join(page.locator("body").inner_text(timeout=2000).split())[:300]
        if body:
            details.append(f"body={body!r}")
    except Exception:
        pass
    return "; ".join(details) or "page diagnostics unavailable"


def load_tournament_rows(page: Any, url: str, label: str = "Tournament") -> list[dict[str, Any]]:
    log(f"{label}: requesting page (wait mode: commit)")
    try:
        response = page.goto(url, wait_until="commit", timeout=30000)
        response_status = getattr(response, "status", None)
        log(
            f"{label}: navigation committed"
            + (f" with HTTP {response_status}" if response_status is not None else "")
            + f"; current URL {page.url}"
        )
    except Exception as exc:
        # Some Flashscore pages keep navigation open while their live-data
        # requests continue. The useful DOM may still arrive, so do not abandon
        # the page solely because the navigation promise timed out.
        log(f"{label}: navigation did not commit cleanly ({exc}); checking the DOM anyway")
    # Fresh server profiles can leave the event list behind a first-visit layer.
    # The score data is usable as soon as the rows are attached to the DOM; it
    # does not need to be visually unobscured for extraction.
    log(f"{label}: waiting for match rows to attach")
    page.wait_for_selector(EVENT_ROW_SELECTOR, state="attached", timeout=15000)
    attached_count = page.locator(EVENT_ROW_SELECTOR).count()
    log(f"{label}: {attached_count} match row(s) attached; extracting scores")
    rows = extract_page_rows(page)
    if not rows:
        raise RuntimeError(f"Tournament page returned no match rows ({page_diagnostic(page)})")
    log(f"{label}: extracted {len(rows)} match row(s)")
    return rows


class FlashscoreLivePageProbe:
    """Keep live tournament pages open so short DOM transitions are not lost to feed caching."""

    def __init__(self) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError("Playwright is not installed") from exc
        self._manager = sync_playwright().start()
        try:
            self._browser = self._manager.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage", "--no-sandbox"],
            )
            self._context = self._browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=FLASHSCORE_USER_AGENT,
            )
        except Exception:
            browser = getattr(self, "_browser", None)
            if browser is not None:
                try:
                    browser.close()
                except Exception:
                    pass
            self._manager.stop()
            raise
        self._pages: dict[str, Any] = {}

    def rows(self, url: str, label: str) -> list[dict[str, Any]]:
        page = self._pages.get(url)
        if page is None:
            page = self._context.new_page()
            page.set_default_timeout(8000)
            self._pages[url] = page
            rows = load_tournament_rows(page, url, f"{label} live page")
            log(f"{label}: live page connected for toss and start detection")
            return rows
        rows = extract_page_rows(page)
        if not rows:
            raise RuntimeError(f"Live page returned no rows ({page_diagnostic(page)})")
        return rows

    def retain(self, urls: set[str]) -> None:
        for old_url in set(self._pages) - urls:
            page = self._pages.pop(old_url)
            try:
                page.close()
            except Exception:
                pass

    def discard(self, url: str) -> None:
        page = self._pages.pop(url, None)
        if page is not None:
            try:
                page.close()
            except Exception:
                pass

    def close(self) -> None:
        for page in self._pages.values():
            try:
                page.close()
            except Exception:
                pass
        self._pages.clear()
        try:
            self._context.close()
        finally:
            try:
                self._browser.close()
            finally:
                self._manager.stop()


def overlay_live_rows(
    base_rows: dict[str, dict[str, Any]],
    live_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    merged = dict(base_rows)
    for live_row in live_rows:
        match_id = str(live_row.get("id", "") or "")
        if match_id in merged:
            merged[match_id] = {**merged[match_id], **live_row}
    return merged


def _handle_stop(_signum: int, _frame: Any) -> None:
    STOP_EVENT.set()


def write_runtime_state(
    matches: dict[str, dict[str, Any]],
    alerts: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    *,
    last_error: str = "",
    phase: str = "",
) -> None:
    atomic_write_json(
        STATE_PATH,
        {
            "status": "running" if not STOP_EVENT.is_set() else "stopped",
            "last_sweep_at": utc_timestamp(),
            "last_error": last_error,
            "phase": phase,
            "sources": sources,
            "matches": list(matches.values()),
            "alerts": alerts[-MAX_ALERT_HISTORY:],
        },
    )


def run_browser_watcher(poll_seconds: float, reload_minutes: float) -> int:
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

    log(f"Starting browser for {len(configured_links())} configured tournament(s)")
    with sync_playwright() as playwright:
        launch_options: dict[str, Any] = {
            "headless": True,
            "args": ["--disable-dev-shm-usage", "--no-sandbox"],
        }
        # The Hub's global CHROME_BINARY is intended for other scanners. This
        # watcher deliberately mirrors the working FlashMultiviewer by using
        # Playwright's own Chromium unless a Challenger-specific override is set.
        chrome_binary = os.getenv("CHALLENGER_CHROME_BINARY", "").strip()
        if chrome_binary and Path(chrome_binary).is_file():
            launch_options["executable_path"] = chrome_binary
            log(f"Using Challenger Chrome override: {chrome_binary}")
        else:
            log("Using Playwright bundled Chromium (Multiviewer-compatible mode)")
        browser = playwright.chromium.launch(**launch_options)
        log(f"Browser process launched: Chromium {browser.version}")
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome Safari/537.36"
            ),
        )
        log(f"Watcher started. Config: {CONFIG_PATH}. State: {STATE_PATH}.")
        write_runtime_state(matches, alerts, [], last_error="", phase="Starting first scan")

        try:
            sweep_number = 0
            while not STOP_EVENT.is_set():
                sweep_number += 1
                sweep_started = time.monotonic()
                links = configured_links()
                if sweep_number == 1 or sweep_number % 10 == 0:
                    log(
                        f"Sweep {sweep_number} started: {len(links)} tournament(s), "
                        f"{len(matches)} known match(es)"
                    )
                if not links:
                    write_runtime_state(
                        matches,
                        alerts,
                        [],
                        last_error="No tournament links are configured.",
                        phase="Waiting for tournament links",
                    )
                    STOP_EVENT.wait(max(1.0, poll_seconds))
                    continue

                for old_url in set(pages) - set(links):
                    pages.pop(old_url).close()
                    loaded_at.pop(old_url, None)
                    source_errors.pop(old_url, None)

                for url in links:
                    tournament = tournament_label_from_url(url)
                    page = pages.get(url)
                    needs_reload = page is None or time.monotonic() - loaded_at.get(url, 0) >= reload_minutes * 60
                    try:
                        if page is None:
                            log(f"{tournament}: creating browser page")
                            page = context.new_page()
                            page.set_default_timeout(15000)
                            pages[url] = page
                        if needs_reload:
                            log(f"Loading {tournament}")
                            write_runtime_state(
                                matches,
                                alerts,
                                [],
                                last_error="",
                                phase=f"Loading {tournament}",
                            )
                            raw_matches = load_tournament_rows(page, url, tournament)
                            loaded_at[url] = time.monotonic()
                        else:
                            raw_matches = extract_page_rows(page)
                        if not raw_matches:
                            raise RuntimeError(
                                f"Tournament page returned no match rows ({page_diagnostic(page)})"
                            )
                        source_errors.pop(url, None)
                    except Exception as exc:
                        diagnostic = page_diagnostic(page) if page is not None else "page was not created"
                        detailed_error = f"{exc} [{diagnostic}]"
                        source_errors[url] = detailed_error
                        loaded_at[url] = 0
                        log(f"Tournament read failed for {tournament}: {detailed_error}")
                        continue

                    log(f"{tournament}: processing {len(raw_matches)} row(s)")
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
                phase = (
                    f"Watching {len(links)} tournament(s)"
                    if not error_message
                    else f"Scan completed with {len(source_errors)} source error(s)"
                )
                write_runtime_state(matches, alerts, sources, last_error=error_message, phase=phase)
                if sweep_number == 1 or sweep_number % 10 == 0 or error_message:
                    live_count = sum(item.get("status") == "live" for item in matches.values())
                    scheduled_count = sum(item.get("status") == "scheduled" for item in matches.values())
                    log(
                        f"Sweep {sweep_number} complete in {time.monotonic() - sweep_started:.1f}s: "
                        f"{len(matches)} match(es), {scheduled_count} scheduled, {live_count} live, "
                        f"{len(source_errors)} source error(s)"
                    )
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


def run_watcher(poll_seconds: float, reload_minutes: float) -> int:
    webhook = slack_webhook_url()
    if not webhook:
        raise RuntimeError("Slack is not configured. Set Webhook_Challenger in the Hub environment.")
    links = configured_links()
    if not links:
        raise RuntimeError("No ATP Challenger tournament links are saved in the Hub.")

    prior = read_state()
    matches = {
        str(item.get("id", "")): item
        for item in prior.get("matches", []) or []
        if isinstance(item, dict) and item.get("id")
    }
    alerts = [item for item in prior.get("alerts", []) or [] if isinstance(item, dict)]
    feed_configs: dict[str, dict[str, Any]] = {}
    fixture_rows: dict[str, list[dict[str, Any]]] = {}
    fixtures_loaded_at: dict[str, float] = {}
    betfair_events: list[Any] = []
    betfair_loaded_at = 0.0
    betfair_client: Any = None
    session = requests.Session()
    session.headers.update({"User-Agent": FLASHSCORE_USER_AGENT, "Accept": "*/*"})
    live_probe: FlashscoreLivePageProbe | None = None
    live_probe_errors: dict[str, str] = {}
    live_probe_retry_at: dict[str, float] = {}

    log(f"Starting direct Flashscore feed watcher for {len(links)} configured tournament(s)")
    log("Reading the tournament feed with a live-page overlay for toss and start transitions")
    try:
        live_probe = FlashscoreLivePageProbe()
        log("Live-page browser launched")
    except Exception as exc:
        log(f"Live-page overlay unavailable; feed monitoring will continue: {exc}")
    write_runtime_state(matches, alerts, [], last_error="", phase="Starting first feed scan")

    sweep_number = 0
    try:
        while not STOP_EVENT.is_set():
            sweep_number += 1
            sweep_started = time.monotonic()
            links = configured_links()
            source_errors: dict[str, str] = {}
            if sweep_number == 1 or sweep_number % 10 == 0:
                log(
                    f"Feed sweep {sweep_number} started: {len(links)} tournament(s), "
                    f"{len(matches)} known match(es)"
                )

            if not links:
                write_runtime_state(
                    matches,
                    alerts,
                    [],
                    last_error="No tournament links are configured.",
                    phase="Waiting for tournament links",
                )
                STOP_EVENT.wait(max(1.0, poll_seconds))
                continue

            unmatched_scheduled = any(
                item.get("status") == "scheduled" and not item.get("betfair_event_id")
                for item in matches.values()
            )
            should_refresh_betfair = (
                betfair_loaded_at == 0.0
                or unmatched_scheduled
                or time.monotonic() - betfair_loaded_at >= BETFAIR_REFRESH_SECONDS
            )
            if should_refresh_betfair:
                try:
                    if betfair_client is None:
                        log("Connecting to Betfair for event matching")
                        betfair_client = create_betfair_client()
                    reason = "unmatched scheduled match(es)" if unmatched_scheduled else "scheduled refresh"
                    log(f"Refreshing upcoming Betfair tennis events ({reason})")
                    betfair_events = fetch_upcoming_betfair_tennis_events(betfair_client)
                    betfair_loaded_at = time.monotonic()
                    log(f"Betfair event cache loaded: {len(betfair_events)} upcoming tennis event(s)")
                except Exception as exc:
                    betfair_loaded_at = time.monotonic()
                    if betfair_client is not None:
                        try:
                            betfair_client.logout()
                        except Exception:
                            pass
                        betfair_client = None
                    log(f"Betfair event refresh failed; Flashscore monitoring will continue: {exc}")

            for old_url in set(feed_configs) - set(links):
                feed_configs.pop(old_url, None)
                fixture_rows.pop(old_url, None)
                fixtures_loaded_at.pop(old_url, None)
                live_probe_errors.pop(old_url, None)
                live_probe_retry_at.pop(old_url, None)
            if live_probe is not None:
                live_probe.retain(set(links))

            for url in links:
                tournament = tournament_label_from_url(url)
                config = feed_configs.get(url)
                needs_reload = (
                    config is None
                    or time.monotonic() - float(config.get("loaded_at", 0)) >= reload_minutes * 60
                )
                try:
                    if needs_reload:
                        write_runtime_state(
                            matches,
                            alerts,
                            [],
                            last_error="",
                            phase=f"Configuring {tournament}",
                        )
                        config = load_feed_config(session, url, tournament)
                        config["loaded_at"] = time.monotonic()
                        feed_configs[url] = config
                    summary_matches = fetch_tournament_feed(session, config, tournament)
                    source_errors.pop(url, None)
                except Exception as exc:
                    source_errors[url] = str(exc)
                    feed_configs.pop(url, None)
                    log(f"Tournament feed failed for {tournament}: {exc}")
                    continue

                if (
                    url not in fixture_rows
                    or time.monotonic() - fixtures_loaded_at.get(url, 0.0) >= FIXTURES_REFRESH_SECONDS
                ):
                    try:
                        fixture_rows[url] = fetch_tournament_fixtures(session, config, tournament)
                        fixtures_loaded_at[url] = time.monotonic()
                    except Exception as exc:
                        fixtures_loaded_at[url] = time.monotonic()
                        log(f"Future fixture refresh failed for {tournament}; using prior rows: {exc}")

                combined_matches = {
                    str(item.get("id", "")): item
                    for item in fixture_rows.get(url, [])
                    if item.get("id")
                }
                combined_matches.update(
                    {
                        str(item.get("id", "")): item
                        for item in summary_matches
                        if item.get("id")
                    }
                )
                if live_probe is not None and time.monotonic() >= live_probe_retry_at.get(url, 0.0):
                    try:
                        live_rows = live_probe.rows(url, tournament)
                        combined_matches = overlay_live_rows(combined_matches, live_rows)
                        live_probe_retry_at.pop(url, None)
                        if url in live_probe_errors:
                            log(f"{tournament}: live-page overlay recovered")
                            live_probe_errors.pop(url, None)
                    except Exception as exc:
                        error_text = str(exc)
                        if live_probe_errors.get(url) != error_text:
                            log(f"{tournament}: live-page overlay failed; using feed data: {error_text}")
                        live_probe_errors[url] = error_text
                        live_probe_retry_at[url] = time.monotonic() + 60
                        live_probe.discard(url)
                raw_matches = list(combined_matches.values())

                now = utc_timestamp()
                for raw in raw_matches:
                    scraped = normalize_scraped_match(raw, url, tournament)
                    match_id = scraped["id"]
                    previous = matches.get(match_id)
                    next_match = {
                        **(previous or {}),
                        **scraped,
                        "first_seen_at": (previous or {}).get("first_seen_at", now),
                        "last_checked_at": now,
                        "last_error": "",
                    }
                    should_recheck_betfair = (
                        next_match.get("status") == "scheduled"
                        or not next_match.get("betfair_event_id")
                    )
                    if betfair_events and should_recheck_betfair:
                        betfair_event, betfair_score, betfair_reason = match_betfair_event(
                            next_match,
                            betfair_events,
                        )
                        if betfair_event is not None:
                            next_match.update(
                                {
                                    "betfair_event_id": betfair_event.event_id,
                                    "betfair_event_name": betfair_event.event_name,
                                    "betfair_match_score": round(betfair_score, 1),
                                    "betfair_match_message": "Matched",
                                }
                            )
                            if str((previous or {}).get("betfair_event_id", "")) != betfair_event.event_id:
                                log(
                                    f"Betfair matched: {next_match['player1']} v {next_match['player2']} "
                                    f"-> {betfair_event.event_id} ({betfair_event.event_name})"
                                )
                        else:
                            next_match.update(
                                {
                                    "betfair_event_id": "",
                                    "betfair_event_name": "",
                                    "betfair_match_score": round(betfair_score, 1),
                                    "betfair_match_message": betfair_reason,
                                }
                            )
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
            matches = {
                key: value
                for key, value in matches.items()
                if value.get("source_url") in active_urls
            }
            sources = [
                {
                    "url": url,
                    "label": tournament_label_from_url(url),
                    "status": "error" if url in source_errors else "ok",
                    "error": source_errors.get(url, ""),
                }
                for url in links
            ]
            error_message = "; ".join(
                f"{tournament_label_from_url(url)}: {error}"
                for url, error in source_errors.items()
            )
            phase = (
                f"Watching {len(links)} tournament feed(s)"
                if not error_message
                else f"Feed scan completed with {len(source_errors)} source error(s)"
            )
            write_runtime_state(matches, alerts, sources, last_error=error_message, phase=phase)
            if sweep_number == 1 or sweep_number % 10 == 0 or error_message:
                live_count = sum(item.get("status") == "live" for item in matches.values())
                scheduled_count = sum(item.get("status") == "scheduled" for item in matches.values())
                log(
                    f"Feed sweep {sweep_number} complete in {time.monotonic() - sweep_started:.1f}s: "
                    f"{len(matches)} match(es), {scheduled_count} scheduled, {live_count} live, "
                    f"{len(source_errors)} source error(s)"
                )
            STOP_EVENT.wait(max(0.5, poll_seconds - (time.monotonic() - sweep_started)))
    finally:
        session.close()
        if live_probe is not None:
            try:
                live_probe.close()
            except Exception as exc:
                log(f"Live-page browser cleanup failed: {exc}")
        if betfair_client is not None:
            try:
                betfair_client.logout()
            except Exception:
                log("Betfair logout failed while stopping watcher")

    write_runtime_state(matches, alerts, [], last_error="", phase="Stopped")
    log("Direct feed watcher stopped cleanly.")
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
