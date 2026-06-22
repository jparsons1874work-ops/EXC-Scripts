#!/usr/bin/env python3
"""Self-test the Betfair In-Play hub table parser/template contract."""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app import parsers


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def init_fixture_connection(*, include_visible_row: bool) -> sqlite3.Connection:
    now = iso_now()
    connection = sqlite3.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE inplay_scan_runs (
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
            config_error TEXT NOT NULL DEFAULT '',
            betfair_time_scan_status TEXT NOT NULL DEFAULT 'complete',
            flashscore_scan_status TEXT NOT NULL DEFAULT 'complete',
            flashscore_live_matches_found INTEGER NOT NULL DEFAULT 0,
            run_id TEXT,
            current_run_started_at TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE inplay_alert_state (
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
            final_marketbook_inplay_parsed INTEGER,
            final_marketbook_status_parsed TEXT,
            visible_in_hub INTEGER,
            last_seen_in_scan_at TEXT
        )
        """
    )
    # Deliberately omit run_id from logs to cover older DBs that have not been migrated yet.
    connection.execute(
        """
        CREATE TABLE inplay_scan_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            level TEXT NOT NULL,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL,
            sport_name TEXT,
            event_id TEXT,
            market_id TEXT,
            details_json TEXT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO inplay_scan_runs (
            scan_started_at, scan_completed_at, status, dry_run, excluded_sports_json,
            markets_scanned, run_id, current_run_started_at
        )
        VALUES (?, ?, 'complete', 0, '[]', 1, 'run-current', ?)
        """,
        (now, now, now),
    )
    if include_visible_row:
        connection.execute(
            """
            INSERT INTO inplay_alert_state (
                event_id, market_id, sport_name, event_name, last_seen_status,
                last_seen_inplay, last_checked_at, final_verification_result,
                final_verification_reason, trigger_source, flashscore_match_name,
                flashscore_status, betfair_last_checked_at, betfair_last_seen_inplay,
                betfair_last_seen_status, slack_alert_sent, final_marketbook_inplay_parsed,
                final_marketbook_status_parsed, visible_in_hub, last_seen_in_scan_at
            )
            VALUES (
                'event-visible', '1.visible', 'Tennis', 'Player A v Player B', 'OPEN',
                0, ?, 'pending_verification', 'diagnostic detail hidden', 'flashscore_live',
                'Player A v Player B', 'Live - Set 1', ?, 0, 'OPEN', 0, 0, 'OPEN', 1, ?
            )
            """,
            (now, now, now),
        )
        connection.execute(
            """
            INSERT INTO inplay_alert_state (
                event_id, market_id, sport_name, event_name, last_seen_status,
                last_seen_inplay, last_checked_at, final_verification_result,
                trigger_source, visible_in_hub, last_seen_in_scan_at
            )
            VALUES ('event-hidden', '1.hidden', 'Darts', 'Old Event', 'OPEN', 0, ?, '', 'flashscore_live', 0, ?)
            """,
            (now, now),
        )
    connection.commit()
    return connection


def render_panel(inplay) -> str:
    env = Environment(
        loader=FileSystemLoader(PROJECT_ROOT / "app" / "templates"),
        autoescape=select_autoescape(("html", "xml")),
    )
    template = env.get_template("partials/inplay_checker_panel.html")
    return template.render(inplay=inplay)


def parse_from_connection(connection: sqlite3.Connection):
    marker = PROJECT_ROOT / "runtime" / "output" / "inplay_hub_display_self_test.marker"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("fixture", encoding="utf-8")
    original_connect = parsers.sqlite3.connect
    parsers.sqlite3.connect = lambda _: connection
    try:
        return parsers.parse_inplay_checker_state(marker)
    finally:
        parsers.sqlite3.connect = original_connect


def main() -> int:
    visible_connection = init_fixture_connection(include_visible_row=True)
    parsed = parse_from_connection(visible_connection)
    assert parsed is not None
    assert len(parsed.active_flags) == 1
    row = parsed.active_flags[0]
    assert row["sport_name"] == "Tennis"
    assert row["display_event_name"] == "Player A v Player B"
    assert row["flashscore_status_display"] == "Live - Set 1"
    assert row["betfair_compact_status"] == "OPEN / Not In-Play"
    assert row["timer_until_slack"]
    assert row["final_verification_reason"] == "diagnostic detail hidden"
    html = render_panel(parsed)
    for heading in ("Sport", "Event Name", "Time of Scan", "Flashscore Status", "Betfair Status", "Timer Until Slack"):
        assert heading in html
    for hidden_heading in ("Betfair Event ID", "Final Verification Result", "Final Verification Reason", "Match Reason"):
        assert hidden_heading not in html
    assert "diagnostic detail hidden" not in html
    assert "Old Event" not in html

    empty_connection = init_fixture_connection(include_visible_row=False)
    parsed = parse_from_connection(empty_connection)
    assert parsed is not None
    assert parsed.active_flags == []
    html = render_panel(parsed)
    assert "No active in-play checks. Pending or flagged matches will appear here." in html
    assert "<table>" in html

    print("In-play hub display self-test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
