from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from app.config import OUTPUT_DIR


@dataclass(frozen=True)
class CricketTimeResult:
    mismatch_count: int
    rows: list[dict[str, str]]
    summary: dict[str, str]
    failure_message: str = ""
    root_error: str = ""


@dataclass(frozen=True)
class InPlayCheckerResult:
    summary: dict[str, str]
    sport_breakdown: list[dict[str, str]]
    active_flags: list[dict[str, str]]
    recovered_events: list[dict[str, str]]
    recent_alerts: list[dict[str, str]]
    recent_skipped_events: list[dict[str, str]]
    recent_logs: list[dict[str, str]]
    config_error: str = ""


UK_TZ = ZoneInfo("Europe/London")
INPLAY_DB_PATH = OUTPUT_DIR / "betfair_inplay_start_checker.sqlite3"


SUMMARY_LABELS = (
    "Scrape Status",
    "Betfair Fixtures",
    "Decimal Fixtures",
    "Matched Fixtures",
    "Unmatched Betfair Fixtures",
    "Unmatched Decimal Fixtures",
)


def _parse_summary_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    for label in SUMMARY_LABELS:
        if not stripped.startswith(label):
            continue
        remainder = stripped[len(label) :].strip()
        if not remainder:
            return None
        return label, re.sub(r"\s{2,}|\t+", " ", remainder)
    return None


def parse_cricket_time_check_output(output_lines: list[str]) -> CricketTimeResult:
    summary: dict[str, str] = {}
    for line in output_lines:
        if line.startswith("STDERR:"):
            continue
        parsed = _parse_summary_line(line)
        if parsed is None:
            continue
        label, value = parsed
        if label == "Scrape Status":
            parts = value.split(maxsplit=1)
            if len(parts) == 2:
                summary[f"{parts[0].lower()}_status"] = parts[1]
            continue
        summary[label.lower().replace(" ", "_")] = value

    failure_line = next(
        (
            line.strip()
            for line in output_lines
            if line.strip() == "Decimal fixture scrape failed; comparison not reliable."
            or line.strip().startswith("Failure")
        ),
        "",
    )
    root_error_line = next((line.strip() for line in output_lines if line.strip().startswith("Root Error")), "")
    if failure_line:
        root_error = re.sub(r"^Root Error:?\s*", "", root_error_line).strip()
        failure_message = "Decimal fixture scrape failed; comparison not reliable."
        return CricketTimeResult(
            mismatch_count=0,
            rows=[],
            summary=summary,
            failure_message=failure_message,
            root_error=root_error,
        )

    summary_line = next((line.strip() for line in output_lines if line.strip().startswith("Not Matching")), None)
    if summary_line is None:
        raise ValueError("Could not find mismatch summary line.")

    match = re.match(r"^Not Matching\s+(\d+)$", summary_line)
    if not match:
        raise ValueError("Could not parse mismatch summary line.")

    header_index = next(
        (index for index, line in enumerate(output_lines) if line.strip().startswith("Status")),
        None,
    )
    if header_index is None:
        raise ValueError("Could not find cricket output header.")

    rows: list[dict[str, str]] = []
    for line in output_lines[header_index + 1 :]:
        stripped = line.strip()
        if not stripped or stripped.startswith("STDERR:"):
            continue
        parts = re.split(r"\s{2,}", stripped, maxsplit=3)
        if len(parts) == 4:
            rows.append(
                {
                    "status": parts[0],
                    "match": parts[1],
                    "betfair_time": parts[2],
                    "decimal_time": parts[3],
                }
            )

    return CricketTimeResult(mismatch_count=int(match.group(1)), rows=rows, summary=summary)


def _format_db_time(value: str | None) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    return parsed.astimezone(UK_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def _rows(connection: sqlite3.Connection, query: str, params: tuple = ()) -> list[dict[str, str]]:
    connection.row_factory = sqlite3.Row
    return [dict(row) for row in connection.execute(query, params).fetchall()]


def _log_details(row: dict[str, str]) -> dict[str, str]:
    try:
        details = json.loads(row.get("details_json") or "{}")
    except json.JSONDecodeError:
        return {}
    if not isinstance(details, dict):
        return {}
    return details


def _decorate_log_rows(rows: list[dict[str, str]]) -> None:
    for row in rows:
        details = _log_details(row)
        row["event_name"] = str(details.get("event_name") or "")
        row["reason"] = str(details.get("reason") or "")
        for key in ("first_flagged_at", "alert_sent_at", "recovered_at", "last_checked_at", "timestamp"):
            if key in row:
                row[key] = _format_db_time(row.get(key))


def _decorate_state_rows(rows: list[dict[str, str]]) -> None:
    now = datetime.now(UK_TZ)
    for row in rows:
        inplay = row.get("betfair_last_seen_inplay", row.get("last_seen_inplay"))
        if inplay is None or inplay == "":
            row["inplay_label"] = "Unknown"
        elif int(inplay) == 1:
            row["inplay_label"] = "Yes"
        else:
            row["inplay_label"] = "No"

        start_value = row.get("scheduled_start_utc")
        try:
            start_dt = datetime.fromisoformat(str(start_value or "").replace("Z", "+00:00")).astimezone(UK_TZ)
        except ValueError:
            start_dt = None
        row["overdue_by"] = _format_duration(now - start_dt) if start_dt else ""
        row["slack_alert_sent"] = "Yes" if row.get("alert_sent_at") or row.get("slack_alert_sent") else "No"
        source = str(row.get("trigger_source") or "betfair_time")
        row["source_label"] = "Flashscore" if source == "flashscore_live" else "Betfair time"
        row["display_event_name"] = str(row.get("flashscore_match_name") or row.get("event_name") or "")
        row["display_competition"] = str(row.get("flashscore_competition") or row.get("competition_name") or "")
        row["betfair_status_label"] = str(row.get("betfair_last_seen_status") or row.get("last_seen_status") or "")
        row["match_confidence_label"] = str(row.get("match_confidence") or "")
        row["last_log_reason"] = str(
            row.get("slack_error")
            or row.get("match_reason")
            or row.get("final_verification_reason")
            or row.get("final_verification_result")
            or ""
        )
        for key in ("first_flagged_at", "alert_sent_at", "recovered_at", "last_checked_at", "final_verification_at", "betfair_last_checked_at", "flashscore_detected_live_at"):
            if key in row:
                row[key] = _format_db_time(row.get(key))


def _format_duration(delta) -> str:
    total_seconds = max(0, int(delta.total_seconds()))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    return f"{minutes}m {seconds}s"


def _latest_scan_sport_breakdown(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    breakdown: dict[str, dict[str, int | str]] = {}

    def entry_for(sport_name: str) -> dict[str, int | str]:
        sport = sport_name or "unknown"
        if sport not in breakdown:
            breakdown[sport] = {
                "sport_name": sport,
                "markets_scanned": 0,
                "not_overdue": 0,
                "already_in_play": 0,
                "closed": 0,
                "already_alerted": 0,
                "missing_event_id": 0,
                "flags": 0,
                "slack_alerts": 0,
                "api_errors": 0,
                "other_skips": 0,
            }
        return breakdown[sport]

    for row in rows:
        details = _log_details(row)
        sport_name = str(row.get("sport_name") or "")
        if not sport_name and row.get("event_type") == "markets_scanned":
            message = str(row.get("message") or "")
            sport_name = message.split(":", 1)[0].strip()
        entry = entry_for(sport_name)

        event_type = str(row.get("event_type") or "")
        reason = str(details.get("reason") or "")
        if event_type == "markets_scanned":
            try:
                entry["markets_scanned"] = int(entry["markets_scanned"]) + int(details.get("markets_scanned") or 0)
            except (TypeError, ValueError):
                pass
        elif event_type == "skipped":
            if reason == "not overdue":
                entry["not_overdue"] = int(entry["not_overdue"]) + 1
            elif reason == "already in-play":
                entry["already_in_play"] = int(entry["already_in_play"]) + 1
            elif reason == "closed":
                entry["closed"] = int(entry["closed"]) + 1
            elif reason == "already alerted":
                entry["already_alerted"] = int(entry["already_alerted"]) + 1
            elif reason == "missing event ID":
                entry["missing_event_id"] = int(entry["missing_event_id"]) + 1
            else:
                entry["other_skips"] = int(entry["other_skips"]) + 1
        elif event_type == "dry_run_alert":
            entry["flags"] = int(entry["flags"]) + 1
        elif event_type == "slack_alert_sent":
            entry["flags"] = int(entry["flags"]) + 1
            entry["slack_alerts"] = int(entry["slack_alerts"]) + 1
        elif event_type in {"api_error", "final_verification_failed"}:
            entry["api_errors"] = int(entry["api_errors"]) + 1

    return [
        {key: str(value) for key, value in row.items()}
        for row in sorted(breakdown.values(), key=lambda item: str(item["sport_name"]).casefold())
        if any(int(row.get(metric, "0")) for metric in ("markets_scanned", "not_overdue", "already_in_play", "closed", "flags", "api_errors"))
    ]


def parse_inplay_checker_state(db_path: Path = INPLAY_DB_PATH) -> InPlayCheckerResult | None:
    if not db_path.exists():
        return None

    connection = sqlite3.connect(db_path)
    try:
        latest = connection.execute(
            "SELECT * FROM inplay_scan_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if latest is None:
            return None
        columns = [description[0] for description in connection.execute("SELECT * FROM inplay_scan_runs LIMIT 1").description]
        latest_run = dict(zip(columns, latest))
        excluded_sports = json.loads(latest_run.get("excluded_sports_json") or "[]")
        active_flags = _rows(
            connection,
            """
            SELECT event_id, market_id, sport_name, competition_name, event_name,
                   scheduled_start_utc, scheduled_start_uk, first_flagged_at, alert_sent_at,
                   last_seen_status, last_seen_inplay, recovered_at, last_checked_at,
                   final_verification_at, final_verification_result, final_verification_reason,
                   trigger_source, flashscore_match_id, flashscore_url, flashscore_match_name,
                   flashscore_competition, flashscore_status, flashscore_score,
                   flashscore_detected_live_at, match_confidence, match_reason,
                   betfair_last_checked_at, betfair_last_seen_inplay, betfair_last_seen_status,
                   slack_alert_sent, slack_error
            FROM inplay_alert_state
            ORDER BY COALESCE(betfair_last_checked_at, last_checked_at, first_flagged_at) DESC
            LIMIT 50
            """,
        )
        recovered_events = _rows(
            connection,
            """
            SELECT event_id, market_id, sport_name, event_name, recovered_at, last_seen_status, last_checked_at
            FROM inplay_alert_state
            WHERE recovered_at IS NOT NULL
            ORDER BY recovered_at DESC
            LIMIT 10
            """,
        )
        recent_alerts = _rows(
            connection,
            """
            SELECT event_id, market_id, sport_name, competition_name, event_name,
                   scheduled_start_utc, scheduled_start_uk, alert_sent_at, last_seen_status,
                   last_seen_inplay, last_checked_at, final_verification_result, final_verification_reason,
                   trigger_source, flashscore_match_name, flashscore_competition, flashscore_status,
                   flashscore_score, match_confidence, match_reason, betfair_last_checked_at,
                   betfair_last_seen_inplay, betfair_last_seen_status, slack_alert_sent, slack_error
            FROM inplay_alert_state
            WHERE alert_sent_at IS NOT NULL
            ORDER BY alert_sent_at DESC
            LIMIT 10
            """,
        )
        recent_logs = _rows(
            connection,
            """
            SELECT timestamp, level, event_type, message, sport_name, event_id, market_id, details_json
            FROM inplay_scan_logs
            ORDER BY id DESC
            LIMIT 80
            """,
        )
        recent_skipped_events = _rows(
            connection,
            """
            SELECT timestamp, level, event_type, message, sport_name, event_id, market_id, details_json
            FROM inplay_scan_logs
            WHERE event_type = 'skipped'
            ORDER BY id DESC
            LIMIT 200
            """,
        )
        latest_scan_logs = _rows(
            connection,
            """
            SELECT timestamp, level, event_type, message, sport_name, event_id, market_id, details_json
            FROM inplay_scan_logs
            WHERE timestamp >= ?
            ORDER BY id ASC
            LIMIT 5000
            """,
            (latest_run.get("scan_started_at") or "",),
        )
        sport_breakdown = _latest_scan_sport_breakdown(latest_scan_logs)

        _decorate_state_rows(active_flags)
        _decorate_state_rows(recent_alerts)
        for rows in (recovered_events,):
            for row in rows:
                for key in ("first_flagged_at", "alert_sent_at", "recovered_at", "last_checked_at", "timestamp"):
                    if key in row:
                        row[key] = _format_db_time(row.get(key))
        _decorate_log_rows(recent_logs)
        _decorate_log_rows(recent_skipped_events)

        summary = {
            "last_scan_time": _format_db_time(latest_run.get("scan_completed_at") or latest_run.get("scan_started_at")),
            "next_scheduled_scan": _format_db_time(latest_run.get("next_scan_at")),
            "included_sports_count": str(latest_run.get("included_sports_count", 0)),
            "excluded_sports": ", ".join(excluded_sports) if excluded_sports else "None",
            "markets_scanned": str(latest_run.get("markets_scanned", 0)),
            "flags_found": str(latest_run.get("flags_found", 0)),
            "slack_alerts_sent": str(latest_run.get("slack_alerts_sent", 0)),
            "slack_alert_failures": str(latest_run.get("slack_alert_failures", 0)),
            "api_errors": str(latest_run.get("api_errors", 0)),
            "betfair_time_scan_status": str(latest_run.get("betfair_time_scan_status", "")),
            "flashscore_scan_status": str(latest_run.get("flashscore_scan_status", "")),
            "flashscore_live_matches_found": str(latest_run.get("flashscore_live_matches_found", 0)),
            "dry_run": "yes" if latest_run.get("dry_run") else "no",
            "status": str(latest_run.get("status", "")),
        }
        return InPlayCheckerResult(
            summary=summary,
            sport_breakdown=sport_breakdown,
            active_flags=active_flags,
            recovered_events=recovered_events,
            recent_alerts=recent_alerts,
            recent_skipped_events=recent_skipped_events,
            recent_logs=recent_logs,
            config_error=str(latest_run.get("config_error") or ""),
        )
    finally:
        connection.close()
