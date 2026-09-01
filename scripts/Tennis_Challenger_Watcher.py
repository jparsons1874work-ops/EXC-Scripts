#!/usr/bin/env python3
"""Monitor configured Flashscore tennis tournaments and matches and alert Slack."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
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
from urllib.parse import urlparse
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
    flashscore_match_id,
    is_single_match_url,
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
DEFAULT_POLL_SECONDS = 10.0
DEFAULT_RELOAD_MINUTES = 15.0
DEFAULT_RESET_HOURS = 2.0
WATCHER_RESTART_EXIT_CODE = 75
MAX_ALERT_HISTORY = 100
BETFAIR_REFRESH_SECONDS = 10 * 60
BETFAIR_UNMATCHED_REFRESH_SECONDS = BETFAIR_REFRESH_SECONDS
TOURNAMENT_DISCOVERY_REFRESH_SECONDS = 10 * 60
FIXTURES_REFRESH_SECONDS = TOURNAMENT_DISCOVERY_REFRESH_SECONDS
FINISHED_RETENTION_SECONDS = 60 * 60
LIVE_PAGE_POLL_SECONDS = 5.0
LIVE_PAGE_MAX_AGE_SECONDS = 20
LIVE_PAGE_RECYCLE_SECONDS = 5 * 60
DETAIL_PAGE_POLL_SECONDS = 0.25
DETAIL_PAGE_MAX_AGE_SECONDS = 60
DETAIL_PREMATCH_WINDOW_SECONDS = 12 * 60 * 60
DETAIL_PAST_WINDOW_SECONDS = 8 * 60 * 60
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
    server_side: serve && /(?:serveHome|icon--serveHome)/.test(String(serve.className.baseVal || serve.className))
      ? 'home'
      : serve && /(?:serveAway|icon--serveAway)/.test(String(serve.className.baseVal || serve.className))
        ? 'away'
        : '',
    is_live: classes.includes('event__match--live'),
    is_scheduled: classes.includes('event__match--scheduled')
  };
}).filter(item => item.id && item.url && item.player1 && item.player2)
"""


DETAIL_EXTRACTOR = r"""
base => {
  const text = selector => {
    const node = document.querySelector(selector);
    return node ? String(node.textContent || '').replace(/\s+/g, ' ').trim() : '';
  };
  const value = (side, suffix) => text(`.smh__${side}.smh__part--${suffix}`);
  const observedRawStatus = text('.detailScore__status')
    || text('.fixedHeaderDuel__detailStatus')
    || text('[class*="detailStatus"]');
  const rawStatus = observedRawStatus || String(base.raw_status || '');
  const statusText = rawStatus.toLowerCase();
  const currentSetMatch = rawStatus.match(/\bset\s*([1-5])/i);
  const statusLabel = currentSetMatch ? `Set ${currentSetMatch[1]}` : rawStatus;
  const table = document.querySelector('.smh__template.tennis, .smh__template[class*="tennis"]');
  const setParts = table ? [1, 2, 3, 4, 5].map(index => ({
    home: value('home', index),
    away: value('away', index)
  })) : (base.set_parts || []);
  const completedSetWinner = pair => {
    const home = Number.parseInt(String(pair.home || ''), 10);
    const away = Number.parseInt(String(pair.away || ''), 10);
    if (!Number.isFinite(home) || !Number.isFinite(away)) return '';
    if (home === 7 && away === 6) return 'home';
    if (away === 7 && home === 6) return 'away';
    if ((home === 6 || home === 7) && home - away >= 2) return 'home';
    if ((away === 6 || away === 7) && away - home >= 2) return 'away';
    if (home >= 10 && home - away >= 2) return 'home';
    if (away >= 10 && away - home >= 2) return 'away';
    return '';
  };
  const isPaused = /\b(suspended|interrupted|delayed)\b/.test(statusText);
  const isFinished = !isPaused
    && /\b(finished|final|retired|walkover|abandoned|cancelled|canceled|awarded)\b/.test(statusText);
  const isLive = !isPaused && !isFinished && Boolean(
    table && (document.querySelector('.smh__live') || /\b(set\s*\d+|live|tiebreak)\b/.test(statusText))
  );
  const homeServe = document.querySelector(
    '.smh__service.smh__home [title*="Serving"], .duelParticipant__home [title*="Serving"]'
  );
  const awayServe = document.querySelector(
    '.smh__service.smh__away [title*="Serving"], .duelParticipant__away [title*="Serving"]'
  );
  return {
    ...base,
    raw_status: isPaused ? rawStatus : isFinished ? rawStatus || 'Finished' : isLive ? statusLabel || 'Set 1' : String(base.raw_status || rawStatus),
    row_classes: `event__match event__match--${isPaused ? 'suspended' : isFinished ? 'finished' : isLive ? 'live' : 'scheduled'}`,
    set_score: table ? {
      home: value('home', 'current'),
      away: value('away', 'current')
    } : (base.set_score || {home: '', away: ''}),
    set_parts: setParts,
    current_points: table ? {
      home: value('home', 'game'),
      away: value('away', 'game')
    } : (base.current_points || {home: '', away: ''}),
    server_side: homeServe ? 'home' : awayServe ? 'away' : '',
    is_live: isLive,
    is_scheduled: !isFinished && !isLive,
    _detail_source: true
  };
}
"""


SINGLE_MATCH_BASE_EXTRACTOR = r"""
input => {
  const text = selector => {
    const node = document.querySelector(selector);
    return node ? String(node.textContent || '').replace(/\s+/g, ' ').trim() : '';
  };
  const participant = side => {
    for (const selector of [
      `.smh__participantName.smh__${side} .participant__participantName`,
      `.duelParticipant__${side} .participant__participantName`,
      `.duelParticipant__${side} [class*="participantName"]`
    ]) {
      const names = Array.from(document.querySelectorAll(selector))
        .map(node => String(node.textContent || '').replace(/\s+/g, ' ').trim())
        .filter(Boolean);
      if (names.length) return Array.from(new Set(names)).join(' / ');
    }
    return '';
  };
  const rawStatus = text('.detailScore__status')
    || text('.fixedHeaderDuel__detailStatus')
    || text('[class*="detailStatus"]')
    || text('.duelParticipant__startTime')
    || text('[class*="startTime"]');
  const tournament = text('.tournamentHeader__country')
    || text('.detail__breadcrumbs')
    || text('[data-testid*="tournament"]')
    || text('[data-testid*="breadcrumb"]')
    || `Single match ${input.id}`;
  return {
    id: input.id,
    url: input.url,
    player1: participant('home'),
    player2: participant('away'),
    tournament,
    raw_status: rawStatus,
    row_classes: 'event__match event__match--scheduled',
    set_score: {home: '', away: ''},
    set_parts: [],
    current_points: {home: '', away: ''},
    server_side: '',
    is_live: false,
    is_scheduled: true
  };
}
"""


def log(message: str) -> None:
    try:
        print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)
    except BrokenPipeError:
        # A dead Hub log reader must not prevent resource cleanup or a planned
        # process recycle.
        pass


def is_broken_pipe_error(value: Any) -> bool:
    text = str(value or "").casefold()
    return "broken pipe" in text or "errno 32" in text


def restart_watcher_process() -> None:
    """Replace this process after all watcher resources have been closed."""
    executable = sys.executable
    argv = [executable, str(Path(__file__).resolve()), *sys.argv[1:]]
    log("Starting a fresh watcher process now")
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except (BrokenPipeError, OSError):
        pass
    os.execv(executable, argv)


def clean_score_value(value: Any) -> str | int:
    raw = str(value or "").strip()
    if raw.upper() in {"A", "AD"}:
        return "A"
    match = re.match(r"^-?\d+", raw)
    return int(match.group(0)) if match else ""


def normalize_status(raw_status: str, row_classes: str = "", is_live: bool = False, is_scheduled: bool = False) -> str:
    text = " ".join(str(raw_status or "").casefold().split())
    if re.search(r"\b(suspended|interrupted|delayed)\b", text):
        return "suspended"
    if re.search(r"\b(retired|walkover|abandoned|cancelled|canceled|awarded)\b", text):
        return re.search(r"\b(retired|walkover|abandoned|cancelled|canceled|awarded)\b", text).group(1)  # type: ignore[union-attr]
    if re.search(r"\b(finished|final|ended)\b", text):
        return "finished"
    if is_live or "event__match--live" in row_classes or re.search(r"\b(set\s*\d+|live|tiebreak)\b", text):
        return "live"
    if is_scheduled or "event__match--scheduled" in row_classes or re.search(r"\b\d{1,2}:\d{2}\b", text):
        return "scheduled"
    return "unknown"


def completed_set_winner(pair: dict[str, Any]) -> str:
    home = clean_score_value(pair.get("home"))
    away = clean_score_value(pair.get("away"))
    if not isinstance(home, int) or not isinstance(away, int):
        return ""
    if home == 7 and away == 6:
        return "home"
    if away == 7 and home == 6:
        return "away"
    if home in {6, 7} and home - away >= 2:
        return "home"
    if away in {6, 7} and away - home >= 2:
        return "away"
    if home >= 10 and home - away >= 2:
        return "home"
    if away >= 10 and away - home >= 2:
        return "away"
    return ""


def score_shows_match_complete(set_parts: Any) -> bool:
    wins = {"home": 0, "away": 0}
    for pair in set_parts or []:
        if not isinstance(pair, dict):
            continue
        winner = completed_set_winner(pair)
        if winner:
            wins[winner] += 1
    return max(wins.values()) >= 2


def status_from_raw_match(raw: dict[str, Any]) -> str:
    status = normalize_status(
        str(raw.get("raw_status", "")),
        str(raw.get("row_classes", "")),
        bool(raw.get("is_live")),
        bool(raw.get("is_scheduled")),
    )
    # Flashscore can briefly show a score that resembles a completed match
    # while its authoritative status is Interrupted/Suspended. A pause must
    # always win over score inference or we will send a false completion alert.
    if status in {"live", "unknown"} and score_shows_match_complete(raw.get("set_parts")):
        return "finished"
    return status


def current_set_number(raw_status: str, status: str) -> int | None:
    match = re.search(r"\bset\s*([1-5])\b", str(raw_status or ""), re.IGNORECASE)
    if match:
        return int(match.group(1))
    if status == "live":
        return 1
    return None


def normalize_scraped_match(raw: dict[str, Any], source_url: str, tournament: str) -> dict[str, Any]:
    observed_status = normalize_status(
        str(raw.get("raw_status", "")),
        str(raw.get("row_classes", "")),
        bool(raw.get("is_live")),
        bool(raw.get("is_scheduled")),
    )
    status = status_from_raw_match(raw)
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
        "completion_confirmed": observed_status in COMPLETED_STATUSES,
        "finish_reason": status if status in COMPLETED_STATUSES and status != "finished" else "",
        "display_status": str(raw.get("raw_status", "") or status.title()),
        "start_time": str(raw.get("raw_status", "") or "") if status == "scheduled" else "",
        "scheduled_at": str(raw.get("scheduled_at", "") or ""),
        "finished_at": str(raw.get("finished_at", "") or "") if status == "finished" else "",
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


def should_scan_match_detail(
    row: dict[str, Any],
    now: datetime | None = None,
) -> bool:
    if bool(row.get("is_live")) or "event__match--live" in str(row.get("row_classes", "")):
        return True
    if not (bool(row.get("is_scheduled")) or "event__match--scheduled" in str(row.get("row_classes", ""))):
        return False

    reference = (now or datetime.now(timezone.utc)).astimezone(UK_TIMEZONE)
    scheduled: datetime | None = None
    scheduled_at = str(row.get("scheduled_at", "") or "").strip()
    if scheduled_at:
        try:
            scheduled = datetime.fromisoformat(scheduled_at.replace("Z", "+00:00"))
            if scheduled.tzinfo is None:
                scheduled = scheduled.replace(tzinfo=timezone.utc)
            scheduled = scheduled.astimezone(UK_TIMEZONE)
        except ValueError:
            scheduled = None

    raw_status = str(row.get("raw_status", "") or "")
    dated = re.search(r"\b(\d{1,2})\.(\d{1,2})\.\s+(\d{1,2}):(\d{2})\b", raw_status)
    timed = re.search(r"\b(\d{1,2}):(\d{2})\b", raw_status)
    if scheduled is None and dated:
        day, month, hour, minute = map(int, dated.groups())
        try:
            scheduled = datetime(reference.year, month, day, hour, minute, tzinfo=UK_TIMEZONE)
            if scheduled < reference and reference.month == 12 and month == 1:
                scheduled = scheduled.replace(year=reference.year + 1)
        except ValueError:
            scheduled = None
    elif scheduled is None and timed:
        hour, minute = map(int, timed.groups())
        scheduled = reference.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if scheduled is None:
        return False
    seconds_until_start = (scheduled - reference).total_seconds()
    return -DETAIL_PAST_WINDOW_SECONDS <= seconds_until_start <= DETAIL_PREMATCH_WINDOW_SECONDS


def satisfied_alerts(match: dict[str, Any]) -> dict[str, bool]:
    status = str(match.get("status", ""))
    provisional_finish = status == "finished" and not match.get("completion_confirmed")
    if status == "suspended" or provisional_finish:
        return {
            "serve_detected": False,
            "match_started": False,
            "set_1_complete": False,
            "set_2_complete": False,
            "match_complete": False,
        }
    current_set = int(match.get("current_set_number") or 0)
    completed = status == "finished" and bool(match.get("completion_confirmed"))
    set_count = len(match.get("sets", []) or [])
    return {
        "serve_detected": bool(status == "scheduled" and match.get("server")),
        "match_started": status == "live" or completed,
        "set_1_complete": current_set >= 2 or (completed and set_count >= 1),
        "set_2_complete": current_set >= 3 or (completed and set_count >= 2),
        "match_complete": completed,
    }


def set_alerts_enabled(match: dict[str, Any]) -> bool:
    competition = " ".join(
        [
            str(match.get("tournament", "") or ""),
            str(match.get("source_url", "") or ""),
        ]
    )
    participants = " ".join(
        [
            str(match.get("player1", "") or ""),
            str(match.get("player2", "") or ""),
        ]
    )
    is_doubles_pair = "/" in participants or bool(re.search(r"\s(?:&|\+)\s", participants))
    excluded_competition = bool(
        re.search(r"\b(?:itf|doubles?)\b", competition, re.IGNORECASE)
    )
    return not excluded_competition and not is_doubles_pair


def pending_alerts(previous: dict[str, Any] | None, match: dict[str, Any]) -> list[str]:
    if match.get("status") == "suspended":
        return []
    sent = dict((previous or {}).get("alerts_sent", {}) or {})
    satisfied = satisfied_alerts(match)
    previously_satisfied = satisfied_alerts(previous or {})
    if previous is None:
        if match.get("status") == "finished":
            return []
        if satisfied["serve_detected"]:
            return ["serve_detected"]
        if match.get("status") == "live":
            return ["match_started"]
        return []
    if satisfied["match_complete"]:
        if (
            previously_satisfied["match_complete"]
            and not sent.get(ALERT_FLAG_BY_TYPE["match_complete"])
        ):
            return ["match_complete"]
        return []
    order = ["serve_detected", "match_started", "set_1_complete", "set_2_complete", "match_complete"]
    alerts: list[str] = []
    for alert_type in order:
        if alert_type == "serve_detected" and match.get("status") != "scheduled":
            continue
        if alert_type in {"set_1_complete", "set_2_complete"} and not set_alerts_enabled(match):
            continue
        if alert_type in {"set_1_complete", "set_2_complete"} and not previously_satisfied[alert_type]:
            continue
        if satisfied[alert_type] and not sent.get(ALERT_FLAG_BY_TYPE[alert_type]):
            alerts.append(alert_type)
    return alerts


def carry_alert_flags(
    previous: dict[str, Any] | None,
    match: dict[str, Any],
) -> dict[str, bool]:
    if previous is None:
        return hydrate_initial_alert_flags(match)
    flags = dict(previous.get("alerts_sent", {}) or {})
    # A genuinely completed match cannot return to live or suspended. If it
    # does, the completion flag came from a provisional/incorrect observation;
    # clear it so the eventual real finish can still alert.
    if match.get("status") in {"live", "suspended"} or not match.get("completion_confirmed"):
        flags[ALERT_FLAG_BY_TYPE["match_complete"]] = False
    return flags


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


def live_state_fingerprint(match: dict[str, Any] | None) -> str:
    value = match or {}
    return json.dumps(
        {
            "status": value.get("status"),
            "set_score": value.get("set_score"),
            "sets": value.get("sets"),
            "current_game": value.get("current_game"),
            "current_points": value.get("current_points"),
            "server_side": value.get("server_side"),
        },
        sort_keys=True,
        default=str,
    )


def match_scan_log_line(match: dict[str, Any], change: str = "same") -> str:
    sets = ",".join(
        f"{item.get('home', '')}-{item.get('away', '')}"
        for item in match.get("sets", []) or []
    ) or "-"
    aggregate = match.get("set_score", {}) or {}
    current_game = match.get("current_game", {}) or {}
    points = match.get("current_points", {}) or {}
    source = str(match.get("score_source", "unknown") or "unknown")
    age = match.get("score_age_seconds")
    age_text = f"{float(age):.1f}s" if isinstance(age, (int, float)) else "-"
    payload_hash = str(match.get("source_payload_hash", "") or "-")
    return (
        f"Match processed [{match.get('tournament', 'Manual tennis')}] "
        f"{match.get('player1', '?')} v {match.get('player2', '?')} "
        f"({match.get('id', '-')}) -> {match.get('status', 'unknown')} | "
        f"sets {aggregate.get('home', '')}-{aggregate.get('away', '')} "
        f"[{sets}] | game {current_game.get('home', '')}-{current_game.get('away', '')} | "
        f"points {points.get('home', '')}-{points.get('away', '')} | "
        f"server {match.get('server_side') or '-'} | source {source} age {age_text} "
        f"payload {payload_hash} | {change}"
    )


def slack_message(alert_type: str, match: dict[str, Any]) -> str:
    players = f"{match.get('player1', '?')} v {match.get('player2', '?')}"
    tournament = str(match.get("tournament", "Manual tennis"))
    url = str(match.get("url", ""))
    betfair_event_id = str(match.get("betfair_event_id", "") or "")
    common = [
        f"*Match:* {players}",
        f"*Tournament:* {tournament}",
        f"*Betfair event ID:* `{betfair_event_id}`" if betfair_event_id else "*Betfair event ID:* Not matched",
    ]
    if alert_type == "serve_detected":
        lines = ["🎾 *Manual tennis — toss decided*", *common]
        lines.append(f"*First server:* {match.get('server', 'Detected')}")
        if match.get("start_time"):
            lines.append(f"*Scheduled:* {match['start_time']}")
        lines.append("Prepare to turn the match in play when play begins.")
    elif alert_type == "match_started":
        lines = ["🟢 *Manual tennis — match started*", *common, f"*Score:* {score_text(match)}", "*Action:* TIP required."]
    elif alert_type == "set_1_complete":
        lines = ["✅ *Manual tennis — Set 1 complete*", *common, f"*Score:* {score_text(match)}", "*Action:* Set 1 settlement required."]
    elif alert_type == "set_2_complete":
        lines = ["✅ *Manual tennis — Set 2 complete*", *common, f"*Score:* {score_text(match)}", "*Action:* Set 2 settlement required."]
    else:
        reason = str(match.get("finish_reason", "") or "")
        heading = "🏁 *Manual tennis — match complete*" if not reason else f"⚠️ *Manual tennis — match ended ({reason.upper()})*"
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
            log(f"Ignoring invalid saved Flashscore link: {exc}")
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
        # The ninja CDN can lag several games behind the website during live
        # tennis. Flashscore's own x/feed route is the low-latency source used
        # to render its scoreboard and exposes the same compact payload.
        "feed_url": f"https://www.flashscore.com/x/feed/{feed_name}",
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
            "Origin": "https://www.flashscore.com",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
        params={"_": int(time.time() * 1000)},
        timeout=(3.0, 5.0),
    )
    response.raise_for_status()
    rows = parse_tournament_feed(response.text, config["source_url"])
    if not rows:
        raise RuntimeError(
            f"Flashscore feed returned no matches (HTTP {response.status_code}, "
            f"{len(response.text)} characters)"
        )
    payload_hash = hashlib.sha256(response.text.encode("utf-8", errors="replace")).hexdigest()[:12]
    response_date = str(response.headers.get("Date", "") or "")
    for row in rows:
        row["_feed_payload_hash"] = payload_hash
        row["_feed_response_date"] = response_date
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
    return rows


def extract_page_rows(page: Any) -> list[dict[str, Any]]:
    rows = page.eval_on_selector_all(EVENT_ROW_SELECTOR, ROW_EXTRACTOR)
    return [row for row in rows or [] if isinstance(row, dict)]


def extract_single_match_row(page: Any, url: str) -> dict[str, Any]:
    match_id = flashscore_match_id(url)
    if not match_id:
        raise RuntimeError("Single-match source has no Flashscore match ID")
    base = page.evaluate(SINGLE_MATCH_BASE_EXTRACTOR, {"id": match_id, "url": url})
    if not isinstance(base, dict) or not base.get("player1") or not base.get("player2"):
        raise RuntimeError(f"Single-match page returned no participants ({page_diagnostic(page)})")
    row = page.evaluate(DETAIL_EXTRACTOR, base)
    if not isinstance(row, dict):
        raise RuntimeError("Single-match page returned no match data")
    row["_tournament"] = str(base.get("tournament", "") or f"Single match {match_id}")
    return row


def load_single_match_rows(page: Any, url: str, label: str = "Single match") -> list[dict[str, Any]]:
    log(f"{label}: requesting direct match page (wait mode: commit)")
    try:
        response = page.goto(url, wait_until="commit", timeout=15000)
        response_status = getattr(response, "status", None)
        log(
            f"{label}: direct page navigation committed"
            + (f" with HTTP {response_status}" if response_status is not None else "")
        )
    except Exception as exc:
        log(f"{label}: direct page navigation did not commit cleanly ({exc}); checking the DOM")
    page.wait_for_selector(
        ".smh__participantName, .duelParticipant__home .participant__participantName",
        state="attached",
        timeout=15000,
    )
    row = extract_single_match_row(page, url)
    log(f"{label}: direct match connected: {row['player1']} v {row['player2']}")
    return [row]


def extract_source_rows(page: Any, url: str) -> list[dict[str, Any]]:
    if is_single_match_url(url):
        return [extract_single_match_row(page, url)]
    return extract_page_rows(page)


def load_source_rows(page: Any, url: str, label: str) -> list[dict[str, Any]]:
    if is_single_match_url(url):
        return load_single_match_rows(page, url, label)
    return load_tournament_rows(page, url, label)


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
                service_workers="block",
                extra_http_headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
            )

            def trim_heavy_resources(route: Any) -> None:
                request = route.request
                hostname = (urlparse(request.url).hostname or "").casefold()
                allowed_host = any(
                    hostname == suffix or hostname.endswith(f".{suffix}")
                    for suffix in (
                        "flashscore.com",
                        "flashscore.ninja",
                        "fsdatacentre.com",
                        "livesport.services",
                        "lsapp.eu",
                    )
                )
                if request.resource_type in {"image", "media", "font", "stylesheet"} or not allowed_host:
                    route.abort()
                else:
                    route.continue_()

            self._context.route("**/*", trim_heavy_resources)
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
        self._opened_at: dict[str, float] = {}

    def _clear_page_cache(self, page: Any) -> None:
        session = None
        try:
            session = self._context.new_cdp_session(page)
            session.send("Network.clearBrowserCache")
        except Exception:
            pass
        finally:
            if session is not None:
                try:
                    session.detach()
                except Exception:
                    pass

    def rows(self, url: str, label: str) -> list[dict[str, Any]]:
        page = self._pages.get(url)
        opened_at = self._opened_at.get(url, 0.0)
        if page is not None and time.monotonic() - opened_at >= LIVE_PAGE_RECYCLE_SECONDS:
            log(f"{label}: recycling live push page after five minutes")
            self.discard(url)
            page = None
        if page is None:
            page = self._context.new_page()
            page.set_default_timeout(8000)
            self._pages[url] = page
            self._opened_at[url] = time.monotonic()
            rows = load_source_rows(page, url, f"{label} live page")
            log(f"{label}: live page connected for current scores, toss and start detection")
            return rows
        rows = extract_source_rows(page, url)
        if not rows:
            raise RuntimeError(f"Live page returned no rows ({page_diagnostic(page)})")
        return rows

    def retain(self, urls: set[str]) -> None:
        for old_url in set(self._pages) - urls:
            self.discard(old_url)

    def discard(self, url: str) -> None:
        page = self._pages.pop(url, None)
        self._opened_at.pop(url, None)
        if page is not None:
            self._clear_page_cache(page)
            try:
                page.close()
            except Exception:
                pass

    def close(self) -> None:
        for url in list(self._pages):
            self.discard(url)
        try:
            self._context.close()
        finally:
            try:
                self._browser.close()
            finally:
                self._manager.stop()


class FlashscoreLivePageMonitor:
    """Scan individual match pages without blocking the feed-scanning thread."""

    def __init__(
        self,
        probe_factory: Any | None = None,
        poll_seconds: float = LIVE_PAGE_POLL_SECONDS,
        max_age_seconds: float = LIVE_PAGE_MAX_AGE_SECONDS,
    ) -> None:
        self._probe_factory = probe_factory
        self._poll_seconds = poll_seconds
        self._max_age_seconds = max_age_seconds
        self._lock = threading.Lock()
        self._targets: dict[str, str] = {}
        self._rows: dict[str, list[dict[str, Any]]] = {}
        self._updated_at: dict[str, float] = {}
        self._errors: dict[str, str] = {}
        self._detail_targets_by_source: dict[str, dict[str, dict[str, Any]]] = {}
        self._detail_rows: dict[str, dict[str, Any]] = {}
        self._detail_updated_at: dict[str, float] = {}
        self._detail_errors: dict[str, str] = {}
        self._fatal_error = ""
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="challenger-live-page-monitor",
            daemon=True,
        )
        self._thread.start()

    def set_targets(self, targets: dict[str, str]) -> None:
        with self._lock:
            if targets == self._targets:
                return
            self._targets = dict(targets)
            for old_url in set(self._rows) - set(targets):
                self._rows.pop(old_url, None)
                self._updated_at.pop(old_url, None)
                self._errors.pop(old_url, None)
                self._detail_targets_by_source.pop(old_url, None)
            valid_detail_ids = {
                match_id
                for source_targets in self._detail_targets_by_source.values()
                for match_id in source_targets
            }
            for match_id in set(self._detail_rows) - valid_detail_ids:
                self._detail_rows.pop(match_id, None)
                self._detail_updated_at.pop(match_id, None)
                self._detail_errors.pop(match_id, None)
        self._wake.set()

    def snapshot(self, url: str) -> tuple[list[dict[str, Any]], str, float | None]:
        with self._lock:
            updated_at = self._updated_at.get(url)
            age = time.monotonic() - updated_at if updated_at is not None else None
            rows = list(self._rows.get(url, [])) if age is not None and age <= self._max_age_seconds else []
            source_targets = self._detail_targets_by_source.get(url, {})
            detail_rows: list[dict[str, Any]] = []
            detail_ages: list[float] = []
            now = time.monotonic()
            for match_id in source_targets:
                detail_updated_at = self._detail_updated_at.get(match_id)
                detail_age = now - detail_updated_at if detail_updated_at is not None else None
                detail_row = self._detail_rows.get(match_id)
                max_detail_age = (
                    FINISHED_RETENTION_SECONDS
                    if detail_row and status_from_raw_match(detail_row) in COMPLETED_STATUSES
                    else DETAIL_PAGE_MAX_AGE_SECONDS
                )
                if detail_age is None or detail_age > max_detail_age or not detail_row:
                    continue
                detail_rows.append(dict(detail_row))
                detail_ages.append(detail_age)
            if detail_rows:
                rows = list(overlay_live_rows(
                    {str(row.get("id", "")): row for row in rows if row.get("id")},
                    detail_rows,
                ).values())
                age = min([value for value in [age, *detail_ages] if value is not None])
            return rows, self._errors.get(url, "") or getattr(self, "_fatal_error", ""), age

    def snapshot_after(
        self,
        url: str,
        observed_after: float,
        timeout: float = 3.0,
    ) -> tuple[list[dict[str, Any]], str, float | None]:
        """Wait briefly for this sweep's live DOM read, then return the latest safe snapshot."""
        deadline = time.monotonic() + max(0.0, timeout)
        self._wake.set()
        while not self._stop.is_set():
            with self._lock:
                updated_at = self._updated_at.get(url)
                error = self._errors.get(url, "") or getattr(self, "_fatal_error", "")
            if updated_at is not None and updated_at >= observed_after:
                break
            if error and updated_at is None:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            self._stop.wait(min(0.05, remaining))
        return self.snapshot(url)

    def set_detail_targets(self, source_url: str, rows: list[dict[str, Any]]) -> None:
        """Seed the sequential match-page queue from the successful feed scan."""
        self._set_detail_targets(source_url, rows)

    def _set_detail_targets(self, source_url: str, rows: list[dict[str, Any]]) -> None:
        candidates = [
            {**row, "_detail_source_url": source_url}
            for row in rows
            if row.get("id") and row.get("url")
        ]
        candidates.sort(key=lambda row: 0 if row.get("is_live") else 1)
        with self._lock:
            self._detail_targets_by_source[source_url] = {
                str(row["id"]): row for row in candidates
            }
            valid_detail_ids = {
                match_id
                for source_targets in self._detail_targets_by_source.values()
                for match_id in source_targets
            }
            for match_id in set(self._detail_rows) - valid_detail_ids:
                self._detail_rows.pop(match_id, None)
                self._detail_updated_at.pop(match_id, None)
                self._detail_errors.pop(match_id, None)

    def _run(self) -> None:
        if self._probe_factory is not None:
            self._run_test_probe()
            return
        try:
            asyncio.run(self._run_async())
        except Exception as exc:
            with self._lock:
                self._fatal_error = str(exc) or type(exc).__name__
            log(f"Live-page overlay unavailable; feed monitoring continues: {exc}")

    def _run_test_probe(self) -> None:
        probe: FlashscoreLivePageProbe | None = None
        try:
            probe = self._probe_factory()
            log("Lightweight live push-page browser launched")
            while not self._stop.is_set():
                self._wake.clear()
                with self._lock:
                    targets = dict(self._targets)
                probe.retain(set(targets))
                for url, label in targets.items():
                    if self._stop.is_set():
                        break
                    try:
                        rows = probe.rows(url, label)
                    except Exception as exc:
                        error_text = str(exc)
                        with self._lock:
                            prior_error = self._errors.get(url, "")
                            self._errors[url] = error_text
                        if error_text != prior_error:
                            log(f"{label}: live-page overlay failed; feed monitoring continues: {error_text}")
                        probe.discard(url)
                        continue
                    with self._lock:
                        recovered = bool(self._errors.pop(url, ""))
                        self._rows[url] = rows
                        self._updated_at[url] = time.monotonic()
                    if recovered:
                        log(f"{label}: live-page overlay recovered")
                self._wake.wait(self._poll_seconds)
                self._wake.clear()
        except Exception as exc:
            with self._lock:
                self._fatal_error = str(exc) or type(exc).__name__
            log(f"Live-page overlay unavailable; feed monitoring continues: {exc}")
        finally:
            if probe is not None:
                try:
                    probe.close()
                except Exception as exc:
                    log(f"Live-page browser cleanup failed: {exc}")

    async def _scan_target(self, context: Any, url: str, label: str) -> list[dict[str, Any]]:
        page = await context.new_page()
        try:
            try:
                await page.goto(url, wait_until="commit", timeout=15000)
            except Exception:
                # Flashscore can keep navigation requests open after the useful
                # document has arrived, so inspect the DOM before treating the
                # navigation timeout as a source failure.
                pass
            await page.wait_for_selector(EVENT_ROW_SELECTOR, state="attached", timeout=10000)
            rows = await page.eval_on_selector_all(EVENT_ROW_SELECTOR, ROW_EXTRACTOR)
            clean_rows = [row for row in rows or [] if isinstance(row, dict)]
            if not clean_rows:
                raise RuntimeError(f"{label} live page returned no match rows")
            self._set_detail_targets(url, clean_rows)
            return clean_rows
        finally:
            try:
                await asyncio.wait_for(page.close(), timeout=3)
            except Exception:
                pass

    async def _run_detail_pages(self, browser: Any) -> None:
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=FLASHSCORE_USER_AGENT,
            timezone_id="Europe/London",
        )

        async def trim_heavy_resources(route: Any) -> None:
            if route.request.resource_type in {"image", "media", "font"}:
                await route.abort()
            else:
                await route.continue_()

        await context.route("**/*", trim_heavy_resources)
        detail_sweep_number = 0
        try:
            while not self._stop.is_set():
                with self._lock:
                    targets = [
                        row
                        for source_targets in self._detail_targets_by_source.values()
                        for row in source_targets.values()
                        if should_scan_match_detail(row)
                    ]
                # Live matches are checked first. Upcoming matches retain the
                # chronological order supplied by their tournament page.
                targets.sort(key=lambda row: 0 if row.get("is_live") else 1)

                if not targets:
                    await asyncio.sleep(1)
                    continue

                detail_sweep_number += 1
                detail_sweep_started = time.monotonic()
                successful_checks = 0
                cache_cleared = False
                for target_index, row in enumerate(targets):
                    if self._stop.is_set():
                        break
                    match_id = str(row.get("id", "") or "")
                    page = None
                    try:
                        page = await context.new_page()
                        async def read_match_page() -> Any:
                            try:
                                await page.goto(
                                    str(row.get("url", "")),
                                    wait_until="commit",
                                    timeout=3500,
                                )
                            except Exception:
                                pass
                            await page.wait_for_selector(
                                ".smh__template.tennis, .duelParticipant",
                                state="attached",
                                timeout=3500,
                            )
                            try:
                                await page.wait_for_function(
                                    """
                                    () => {
                                      const status = document.querySelector(
                                        '.detailScore__status, .fixedHeaderDuel__detailStatus, [class*="detailStatus"]'
                                      );
                                      const statusText = String(status?.textContent || '').trim();
                                      const score = document.querySelector(
                                        '.smh__template.tennis .smh__part--1, .smh__template.tennis .smh__part--current'
                                      );
                                      const serving = document.querySelector('[title*="Serving"]');
                                      return Boolean(statusText || score || serving);
                                    }
                                    """,
                                    timeout=2000,
                                )
                            except Exception:
                                # Scheduled pages can legitimately have no status,
                                # score or server until the toss is published.
                                pass
                            await asyncio.sleep(0.25)
                            return await page.evaluate(DETAIL_EXTRACTOR, row)

                        result = await read_match_page()
                        if not isinstance(result, dict) or not result.get("id"):
                            raise RuntimeError("Direct match page returned no match data")
                    except Exception as exc:
                        error_text = str(exc) or type(exc).__name__
                        with self._lock:
                            prior_error = self._detail_errors.get(match_id, "")
                            self._detail_errors[match_id] = error_text
                            if is_broken_pipe_error(error_text):
                                self._fatal_error = error_text
                        if error_text != prior_error:
                            players = f"{row.get('player1', '?')} v {row.get('player2', '?')}"
                            log(f"{players}: direct match page failed: {error_text}")
                        if is_broken_pipe_error(error_text):
                            log("Direct match browser pipe failed; ending its worker for a full reset")
                            return
                    else:
                        successful_checks += 1
                        with self._lock:
                            prior_result = self._detail_rows.get(match_id)
                            recovered = bool(self._detail_errors.pop(match_id, ""))
                            self._detail_rows[match_id] = result
                            self._detail_updated_at[match_id] = time.monotonic()
                        if recovered:
                            players = f"{row.get('player1', '?')} v {row.get('player2', '?')}"
                            log(f"{players}: direct match page recovered")
                        prior_status = status_from_raw_match(prior_result) if prior_result else ""
                        current_status = status_from_raw_match(result)
                        if current_status != prior_status:
                            players = f"{row.get('player1', '?')} v {row.get('player2', '?')}"
                            score = result.get("set_score") or {}
                            score_text = f"{score.get('home', '')}-{score.get('away', '')}".strip("-")
                            log(
                                f"Direct match update: {players} -> {current_status}"
                                + (f" ({score_text})" if score_text else "")
                            )
                    finally:
                        if page is not None:
                            if target_index == len(targets) - 1:
                                try:
                                    cache_session = await context.new_cdp_session(page)
                                    try:
                                        await cache_session.send("Network.clearBrowserCache")
                                        cache_cleared = True
                                    finally:
                                        await cache_session.detach()
                                except Exception as exc:
                                    log(f"Direct match sweep cache clear failed: {exc}")
                            try:
                                await asyncio.wait_for(page.close(), timeout=3)
                            except Exception:
                                pass
                log(
                    f"Direct match sweep {detail_sweep_number} complete in "
                    f"{time.monotonic() - detail_sweep_started:.1f}s: "
                    f"{successful_checks}/{len(targets)} match page(s) updated; "
                    f"cache {'cleared' if cache_cleared else 'not cleared'}"
                )
                await asyncio.sleep(DETAIL_PAGE_POLL_SECONDS)
        finally:
            try:
                await context.close()
            except Exception:
                pass

    async def _run_async(self) -> None:
        try:
            from playwright.async_api import async_playwright
        except ImportError as exc:
            raise RuntimeError("Playwright is not installed") from exc

        async with async_playwright() as manager:
            browser = await manager.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage", "--no-sandbox"],
            )
            log("Sequential direct match-page sweeps launched")
            try:
                await self._run_detail_pages(browser)
            finally:
                await browser.close()

    def close(self, timeout: float = 5.0) -> bool:
        self._stop.set()
        self._wake.set()
        self._thread.join(timeout=max(0.0, timeout))
        return not self._thread.is_alive()


class FlashscoreFixtureMonitor:
    """Refresh future fixtures without delaying the live-score loop."""

    def __init__(self, poll_seconds: float = FIXTURES_REFRESH_SECONDS) -> None:
        self._poll_seconds = poll_seconds
        self._lock = threading.Lock()
        self._targets: dict[str, str] = {}
        self._rows: dict[str, list[dict[str, Any]]] = {}
        self._errors: dict[str, str] = {}
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="challenger-fixture-monitor",
            daemon=True,
        )
        self._thread.start()

    def set_targets(self, targets: dict[str, str]) -> None:
        with self._lock:
            if targets == self._targets:
                return
            self._targets = dict(targets)
            for old_url in set(self._rows) - set(targets):
                self._rows.pop(old_url, None)
                self._errors.pop(old_url, None)
        self._wake.set()

    def snapshot(self, url: str) -> tuple[list[dict[str, Any]], str]:
        with self._lock:
            return list(self._rows.get(url, [])), self._errors.get(url, "")

    def _run(self) -> None:
        session = requests.Session()
        session.headers.update({"User-Agent": FLASHSCORE_USER_AGENT, "Accept": "*/*"})
        try:
            while not self._stop.is_set():
                self._wake.clear()
                with self._lock:
                    targets = dict(self._targets)
                for url, label in targets.items():
                    if self._stop.is_set():
                        break
                    try:
                        rows = fetch_tournament_fixtures(
                            session,
                            {"source_url": url},
                            label,
                        )
                    except Exception as exc:
                        error_text = str(exc)
                        with self._lock:
                            prior_error = self._errors.get(url, "")
                            self._errors[url] = error_text
                        if error_text != prior_error:
                            log(f"{label}: future fixture refresh failed: {error_text}")
                        continue
                    with self._lock:
                        recovered = bool(self._errors.pop(url, ""))
                        self._rows[url] = rows
                    if recovered:
                        log(f"{label}: future fixture refresh recovered")
                self._wake.wait(self._poll_seconds)
                self._wake.clear()
        finally:
            session.close()

    def close(self, timeout: float = 5.0) -> bool:
        self._stop.set()
        self._wake.set()
        self._thread.join(timeout=max(0.0, timeout))
        return not self._thread.is_alive()


def overlay_live_rows(
    base_rows: dict[str, dict[str, Any]],
    live_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    merged = dict(base_rows)
    for live_row in live_rows:
        match_id = str(live_row.get("id", "") or "")
        if not match_id:
            continue
        # A new live row can reach the browser before the cached direct feed.
        merged[match_id] = {**merged.get(match_id, {}), **live_row}
    return merged


def should_refresh_betfair_events(
    loaded_at: float,
    unmatched_scheduled: bool,
    now: float | None = None,
) -> bool:
    if loaded_at <= 0:
        return True
    age = (time.monotonic() if now is None else now) - loaded_at
    interval = BETFAIR_UNMATCHED_REFRESH_SECONDS if unmatched_scheduled else BETFAIR_REFRESH_SECONDS
    return age >= interval


def timestamp_is_at_least_seconds_old(
    value: Any,
    seconds: float,
    now: datetime | None = None,
) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    reference = now or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return (reference.astimezone(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds() >= seconds


def prune_expired_finished_matches(
    matches: dict[str, dict[str, Any]],
    expired_finished_matches: dict[str, str],
    now: datetime | None = None,
) -> list[str]:
    removed: list[str] = []
    for match_id, match in list(matches.items()):
        if str(match.get("status", "")) != "finished":
            continue
        finished_at = str(match.get("finished_at", "") or "")
        if timestamp_is_at_least_seconds_old(finished_at, FINISHED_RETENTION_SECONDS, now):
            expired_finished_matches[match_id] = finished_at
            matches.pop(match_id, None)
            removed.append(match_id)
    return removed


def _handle_stop(_signum: int, _frame: Any) -> None:
    STOP_EVENT.set()


def write_runtime_state(
    matches: dict[str, dict[str, Any]],
    alerts: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    *,
    expired_finished_matches: dict[str, str] | None = None,
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
            "expired_finished_matches": dict(expired_finished_matches or {}),
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
        raise RuntimeError("No Flashscore tennis tournament or match links are saved in the Hub.")

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
    alerts = [
        item for item in prior.get("alerts", []) or [] if isinstance(item, dict)
    ][-MAX_ALERT_HISTORY:]
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
                        next_match["alerts_sent"] = carry_alert_flags(previous, next_match)

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


def run_watcher(
    poll_seconds: float,
    reload_minutes: float,
    reset_hours: float = DEFAULT_RESET_HOURS,
) -> int:
    webhook = slack_webhook_url()
    if not webhook:
        raise RuntimeError("Slack is not configured. Set Webhook_Challenger in the Hub environment.")
    links = configured_links()
    if not links:
        raise RuntimeError("No Flashscore tennis tournament or match links are saved in the Hub.")

    prior = read_state()
    prior_matches = {
        str(item.get("id", "")): item
        for item in prior.get("matches", []) or []
        if isinstance(item, dict) and item.get("id")
    }
    # Keep alert and Betfair metadata for continuity, but do not republish a
    # saved score while the first clean scan is still starting.
    matches: dict[str, dict[str, Any]] = {}
    alerts = [
        item for item in prior.get("alerts", []) or [] if isinstance(item, dict)
    ][-MAX_ALERT_HISTORY:]
    expired_finished_matches = {
        str(match_id): str(finished_at)
        for match_id, finished_at in dict(prior.get("expired_finished_matches", {}) or {}).items()
        if match_id and finished_at
    }
    for prior_match in prior_matches.values():
        if prior_match.get("status") == "finished" and not prior_match.get("finished_at"):
            prior_match["finished_at"] = (
                prior_match.get("last_alert_at")
                or prior_match.get("last_checked_at")
                or utc_timestamp()
            )
    feed_configs: dict[str, dict[str, Any]] = {}
    feed_failures: dict[str, str] = {}
    betfair_events: list[Any] = []
    betfair_loaded_at = 0.0
    betfair_client: Any = None
    session = requests.Session()
    session.headers.update({"User-Agent": FLASHSCORE_USER_AGENT, "Accept": "*/*"})
    fixture_monitor = FlashscoreFixtureMonitor()
    fixture_monitor.set_targets(
        {
            url: tournament_label_from_url(url)
            for url in links
            if not is_single_match_url(url)
        }
    )
    live_page_monitor = FlashscoreLivePageMonitor(
        probe_factory=FlashscoreLivePageProbe,
        poll_seconds=LIVE_PAGE_POLL_SECONDS,
        max_age_seconds=LIVE_PAGE_MAX_AGE_SECONDS,
    )
    live_page_monitor.set_targets(
        {url: tournament_label_from_url(url) for url in links}
    )
    reset_seconds = max(60.0, reset_hours * 60 * 60)
    reset_deadline = time.monotonic() + reset_seconds
    restart_reason = ""

    log(f"Starting Flashscore push-backed watcher for {len(links)} configured source(s)")
    log(
        f"Every {poll_seconds:g} seconds: read each live push page, publish the hub state, then wait; "
        "refreshing tournament discovery and Betfair matching every 10 minutes"
    )
    log(f"Full watcher process recycle scheduled every {reset_hours:g} hour(s)")
    write_runtime_state(
        matches,
        alerts,
        [],
        expired_finished_matches=expired_finished_matches,
        last_error="",
        phase="Starting first feed scan",
    )

    sweep_number = 0
    try:
        while not STOP_EVENT.is_set():
            if time.monotonic() >= reset_deadline:
                restart_reason = f"scheduled {reset_hours:g}-hour process recycle"
                log(
                    f"Scheduled {reset_hours:g}-hour reset reached; closing all watcher resources"
                )
                break
            sweep_number += 1
            sweep_started = time.monotonic()
            links = configured_links()
            source_errors: dict[str, str] = {}
            fresh_feed_urls: set[str] = set()
            fresh_live_page_urls: set[str] = set()
            changed_match_count = 0
            authoritative_match_ids: dict[str, set[str]] = {}
            match_scan_lines: list[str] = []
            if sweep_number == 1 or sweep_number % 10 == 0:
                log(
                    f"State sweep {sweep_number} started: {len(links)} source(s), "
                    f"{len(matches)} known match(es)"
                )

            if not links:
                write_runtime_state(
                    matches,
                    alerts,
                    [],
                    expired_finished_matches=expired_finished_matches,
                    last_error="No tournament or match links are configured.",
                    phase="Waiting for Flashscore links",
                )
                STOP_EVENT.wait(max(1.0, poll_seconds))
                continue

            unmatched_scheduled = any(
                item.get("status") == "scheduled" and not item.get("betfair_event_id")
                for item in matches.values()
            )
            should_refresh_betfair = should_refresh_betfair_events(
                betfair_loaded_at,
                unmatched_scheduled,
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

            tournament_links = [url for url in links if not is_single_match_url(url)]
            for old_url in set(feed_configs) - set(tournament_links):
                feed_configs.pop(old_url, None)
                feed_failures.pop(old_url, None)
            monitor_targets = {url: tournament_label_from_url(url) for url in links}
            fixture_monitor.set_targets(
                {url: monitor_targets[url] for url in tournament_links}
            )
            live_page_monitor.set_targets(monitor_targets)

            for url in links:
                tournament = tournament_label_from_url(url)
                live_rows, live_page_error, live_page_age = live_page_monitor.snapshot_after(
                    url,
                    sweep_started,
                    timeout=min(3.0, max(1.0, poll_seconds / 2)),
                )
                if is_broken_pipe_error(live_page_error):
                    restart_reason = (
                        f"{tournament} live browser reported {live_page_error}"
                    )
                    log(
                        f"{tournament}: broken browser pipe detected; "
                        "closing all watcher resources for an immediate reset"
                    )
                    break

                if is_single_match_url(url):
                    if not live_rows:
                        error_text = live_page_error or "Direct match page is still loading."
                        source_errors[url] = error_text
                        prior_error = feed_failures.get(url, "")
                        feed_failures[url] = error_text
                        if live_page_error and error_text != prior_error:
                            log(f"{tournament}: direct match source failed: {error_text}")
                        continue
                    fresh_live_page_urls.add(url)
                    if feed_failures.pop(url, ""):
                        log(f"{tournament}: direct match source recovered")
                    combined_matches = {
                        str(item.get("id", "")): {
                            **item,
                            "_score_source": "flashscore_direct_match_page",
                            "_score_age_seconds": float(live_page_age or 0.0),
                        }
                        for item in live_rows
                        if item.get("id")
                    }
                else:
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
                                expired_finished_matches=expired_finished_matches,
                                last_error="",
                                phase=f"Configuring {tournament}",
                            )
                            config = load_feed_config(session, url, tournament)
                            config["loaded_at"] = time.monotonic()
                            feed_configs[url] = config
                        summary_matches = fetch_tournament_feed(session, config, tournament)
                        fresh_feed_urls.add(url)
                        recovered = bool(feed_failures.pop(url, ""))
                        if recovered:
                            log(f"{tournament}: fresh score feed recovered")
                    except Exception as exc:
                        error_text = str(exc)
                        source_errors[url] = error_text
                        prior_error = feed_failures.get(url, "")
                        feed_failures[url] = error_text
                        if "401" in error_text or "403" in error_text:
                            feed_configs.pop(url, None)
                        if error_text != prior_error:
                            log(f"{tournament}: fresh score feed failed: {error_text}")
                        continue

                    future_rows, _fixture_error = fixture_monitor.snapshot(url)
                    future_rows = [
                        {**item, "_score_source": "flashscore_fixture_page"}
                        for item in future_rows
                    ]
                    combined_matches = {
                        str(item.get("id", "")): item
                        for item in future_rows
                        if item.get("id")
                    }
                    combined_matches.update(
                        {
                            str(item.get("id", "")): item
                            for item in summary_matches
                            if item.get("id")
                        }
                    )
                    if live_rows:
                        fresh_live_page_urls.add(url)
                    relevant_live_rows: list[dict[str, Any]] = []
                    for live_row in live_rows:
                        live_match_id = str(live_row.get("id", "") or "")
                        live_status = status_from_raw_match(live_row)
                        is_toss_row = live_status == "scheduled" and bool(live_row.get("server_side"))
                        if live_match_id not in combined_matches and live_status != "live" and not is_toss_row:
                            continue
                        relevant_live_rows.append(
                            {
                                **live_row,
                                "_score_source": "flashscore_live_push_page",
                                "_score_age_seconds": float(live_page_age or 0.0),
                            }
                        )
                    combined_matches = overlay_live_rows(combined_matches, relevant_live_rows)
                authoritative_match_ids[url] = set(combined_matches)
                raw_matches = list(combined_matches.values())

                now = utc_timestamp()
                for raw in raw_matches:
                    match_tournament = str(raw.get("_tournament", "") or tournament)
                    scraped = normalize_scraped_match(raw, url, match_tournament)
                    match_id = scraped["id"]
                    previous = matches.get(match_id) or prior_matches.get(match_id)
                    next_match = {
                        **(previous or {}),
                        **scraped,
                        "first_seen_at": (previous or {}).get("first_seen_at", now),
                        "last_checked_at": now,
                        "score_source": str(
                            raw.get("_score_source", "") or "flashscore_snapshot"
                        ),
                        "score_age_seconds": float(raw.get("_score_age_seconds", 0.0) or 0.0),
                        "source_payload_hash": str(raw.get("_feed_payload_hash", "") or ""),
                        "source_response_date": str(raw.get("_feed_response_date", "") or ""),
                        "last_error": "",
                    }
                    state_changed = (
                        previous is not None
                        and live_state_fingerprint(previous) != live_state_fingerprint(next_match)
                    )
                    if state_changed:
                        changed_match_count += 1
                    if next_match.get("status") == "finished":
                        if match_id in expired_finished_matches:
                            matches.pop(match_id, None)
                            continue
                        next_match["finished_at"] = (
                            str((previous or {}).get("finished_at", "") or "")
                            or now
                        )
                    else:
                        next_match.pop("finished_at", None)
                        expired_finished_matches.pop(match_id, None)
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
                    next_match["alerts_sent"] = carry_alert_flags(previous, next_match)

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
                            "tournament": match_tournament,
                            "message": message,
                        }
                        alerts.append(alert)
                        next_match["last_alert_at"] = now
                        log(f"Slack alert sent: {alert_type} — {alert['match']}")
                    matches[match_id] = next_match
                    prior_matches[match_id] = next_match
                    match_scan_lines.append(
                        match_scan_log_line(
                            next_match,
                            "new" if previous is None else "changed" if state_changed else "same",
                        )
                    )

            if restart_reason:
                break

            active_urls = set(links)
            matches = {
                key: value
                for key, value in matches.items()
                if value.get("source_url") in active_urls
            }
            # A successful fresh feed fully replaces that tournament's prior
            # live-score cache. Do not carry vanished scheduled/live rows
            # forward with an old score.
            matches = {
                key: value
                for key, value in matches.items()
                if str(value.get("source_url", "")) not in authoritative_match_ids
                or key in authoritative_match_ids[str(value.get("source_url", ""))]
                or value.get("status") == "finished"
            }
            removed_finished = prune_expired_finished_matches(matches, expired_finished_matches)
            if removed_finished:
                log(f"Cleared {len(removed_finished)} match(es) finished for at least one hour")
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
            overlay_status = (
                f"live page {len(fresh_live_page_urls)}/{len(links)} · "
                f"tournament feed {len(fresh_feed_urls)}/{len(tournament_links)} · "
                f"{len(links) - len(tournament_links)} direct match link(s) · "
                "discovery/Betfair every 10m"
            )
            phase = (
                f"Watching {len(links)} source(s) · {overlay_status}"
                if not error_message
                else (
                    f"Feed scan completed with {len(source_errors)} source error(s) · "
                    f"{overlay_status}"
                )
            )
            write_runtime_state(
                matches,
                alerts,
                sources,
                expired_finished_matches=expired_finished_matches,
                last_error=error_message,
                phase=phase,
            )
            for match_line in match_scan_lines:
                log(match_line)
            live_count = sum(item.get("status") == "live" for item in matches.values())
            scheduled_count = sum(item.get("status") == "scheduled" for item in matches.values())
            log(
                f"Published sweep {sweep_number} to the hub in {time.monotonic() - sweep_started:.1f}s: "
                f"{len(fresh_live_page_urls)}/{len(links)} live push page(s), "
                f"{len(fresh_feed_urls)}/{len(tournament_links)} tournament feed(s), "
                f"{len(links) - len(tournament_links)} direct link(s), {len(matches)} match(es), "
                f"{scheduled_count} scheduled, {live_count} live, "
                f"{changed_match_count} changed, {len(source_errors)} error(s)"
            )
            STOP_EVENT.wait(max(0.5, poll_seconds))
    finally:
        session.close()
        if not live_page_monitor.close(10):
            log("Live push-page monitor is still stopping; watcher shutdown will clean up its thread")
        if not fixture_monitor.close():
            log("Future-fixture monitor is still stopping; watcher shutdown will clean up its thread")
        if betfair_client is not None:
            try:
                betfair_client.logout()
            except Exception:
                log("Betfair logout failed while stopping watcher")

    if restart_reason and not STOP_EVENT.is_set():
        write_runtime_state(
            matches,
            alerts,
            [],
            expired_finished_matches=expired_finished_matches,
            last_error="",
            phase=f"Restarting watcher · {restart_reason}",
        )
        log("Watcher reset cleanup complete")
        return WATCHER_RESTART_EXIT_CODE

    write_runtime_state(
        matches,
        alerts,
        [],
        expired_finished_matches=expired_finished_matches,
        last_error="",
        phase="Stopped",
    )
    log("Direct feed watcher stopped cleanly.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--poll-seconds", type=float, default=DEFAULT_POLL_SECONDS)
    parser.add_argument("--reload-minutes", type=float, default=DEFAULT_RELOAD_MINUTES)
    parser.add_argument("--reset-hours", type=float, default=DEFAULT_RESET_HOURS)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)
    try:
        result = run_watcher(
            max(1.0, args.poll_seconds),
            max(1.0, args.reload_minutes),
            max(1 / 60, args.reset_hours),
        )
        if result == WATCHER_RESTART_EXIT_CODE and not STOP_EVENT.is_set():
            restart_watcher_process()
        return result
    except Exception as exc:
        if is_broken_pipe_error(exc) and not STOP_EVENT.is_set():
            log(f"Broken pipe reached the main watcher; forcing an immediate reset: {exc}")
            restart_watcher_process()
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
