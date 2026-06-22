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
    active_groups: list[dict[str, object]]
    active_flags: list[dict[str, str]]
    recovered_events: list[dict[str, str]]
    recent_alerts: list[dict[str, str]]
    recent_skipped_events: list[dict[str, str]]
    recent_logs: list[dict[str, str]]
    config_error: str = ""


UK_TZ = ZoneInfo("Europe/London")
INPLAY_DB_PATH = OUTPUT_DIR / "betfair_inplay_start_checker.sqlite3"
PREFERRED_SPORT_ORDER = {
    "Tennis": 1,
    "Darts": 2,
    "Cricket": 3,
    "Basketball": 4,
    "Rugby Union": 5,
    "Rugby League": 6,
    "Snooker": 7,
}


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


def _table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()}


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
        row["scheduled_start_utc_raw"] = row.get("scheduled_start_utc")
        row["flashscore_detected_live_at_raw"] = row.get("flashscore_detected_live_at")
        row["last_checked_at_raw"] = row.get("last_checked_at")
        row["betfair_last_checked_at_raw"] = row.get("betfair_last_checked_at")
        row["verify_after_raw"] = row.get("verify_after")
        final_inplay = row.get("final_marketbook_inplay_parsed")
        inplay = final_inplay if final_inplay not in (None, "") else row.get("betfair_last_seen_inplay", row.get("last_seen_inplay"))
        if inplay is None or inplay == "":
            row["inplay_label"] = "Unknown"
        elif int(inplay) == 1:
            row["inplay_label"] = "Yes"
        else:
            row["inplay_label"] = "No"
        raw_inplay = row.get("final_marketbook_inplay_raw")
        row["final_inplay_raw_label"] = str(raw_inplay) if raw_inplay not in (None, "") else ""
        row["final_inplay_parsed_label"] = row["inplay_label"] if final_inplay not in (None, "") else ""

        start_value = row.get("scheduled_start_utc")
        try:
            start_dt = datetime.fromisoformat(str(start_value or "").replace("Z", "+00:00")).astimezone(UK_TZ)
        except ValueError:
            start_dt = None
        row["overdue_by"] = _format_duration(now - start_dt) if start_dt else ""
        verify_after = None
        try:
            verify_after = datetime.fromisoformat(str(row.get("verify_after_raw") or "").replace("Z", "+00:00")).astimezone(UK_TZ)
        except ValueError:
            verify_after = None
        if verify_after and verify_after > now:
            row["time_remaining"] = _format_duration(verify_after - now)
        elif verify_after and str(row.get("final_verification_result") or "") == "pending_verification":
            row["time_remaining"] = "Due"
        else:
            row["time_remaining"] = ""
        row["slack_alert_sent"] = "Yes" if row.get("alert_sent_at") or row.get("slack_alert_sent") else "No"
        source = str(row.get("trigger_source") or "betfair_time")
        row["source_label"] = "Flashscore" if source == "flashscore_live" else "Betfair time"
        row["format_label"] = str(row.get("match_format") or "singles").title()
        row["display_event_name"] = str(row.get("flashscore_match_name") or row.get("event_name") or "")
        row["display_competition"] = str(row.get("flashscore_competition") or row.get("competition_name") or "")
        row["betfair_status_label"] = str(row.get("betfair_last_seen_status") or row.get("last_seen_status") or "")
        confidence = str(row.get("match_confidence") or "")
        score = row.get("match_score")
        try:
            score_value = float(score) if score not in (None, "") else None
        except (TypeError, ValueError):
            score_value = None
        row["match_score_label"] = f"{score_value:.0f}" if score_value is not None else ""
        row["match_confidence_label"] = f"{confidence} ({score_value:.0f})" if confidence and score_value is not None else confidence
        row["last_log_reason"] = str(
            row.get("slack_error")
            or row.get("match_reason")
            or row.get("final_verification_reason")
            or row.get("final_verification_result")
            or ""
        )
        result = str(row.get("final_verification_result") or "")
        reason = str(row.get("final_verification_reason") or "")
        if result == "pending_verification":
            row["verification_label"] = "Pending"
        elif result == "confirmed_not_inplay":
            row["verification_label"] = "Confirmed Not In-Play"
        elif result == "failed":
            row["verification_label"] = "Failed - API Error"
        elif result == "suppressed_inplay" or (result == "suppressed" and reason in {"inplay", "betfair_inplay"}):
            row["verification_label"] = "Suppressed - Now In-Play"
        elif result == "suppressed_closed" or (result == "suppressed" and reason in {"closed", "betfair_closed"}):
            row["verification_label"] = "Suppressed - Closed"
        else:
            row["verification_label"] = result.replace("_", " ").title() if result else ""
        for key in ("first_flagged_at", "alert_sent_at", "recovered_at", "last_checked_at", "final_verification_at", "betfair_last_checked_at", "flashscore_detected_live_at", "verify_after", "pending_verification_at"):
            if key in row:
                row[key] = _format_db_time(row.get(key))


def _format_duration(delta) -> str:
    total_seconds = max(0, int(delta.total_seconds()))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    return f"{minutes}m {seconds}s"


def _sort_time(row: dict[str, str]) -> datetime:
    candidates = (
        row.get("flashscore_detected_live_at_raw"),
        row.get("scheduled_start_utc_raw"),
        row.get("last_checked_at_raw"),
        row.get("betfair_last_checked_at_raw"),
    )
    for value in candidates:
        if not value:
            continue
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UK_TZ)
        except ValueError:
            continue
    return datetime.max.replace(tzinfo=UK_TZ)


def _group_active_rows(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        row["display_time_uk"] = _format_db_time(row.get("flashscore_detected_live_at_raw") or row.get("scheduled_start_utc_raw"))
        grouped.setdefault(str(row.get("sport_name") or "Other"), []).append(row)

    groups: list[dict[str, object]] = []
    for sport_name, sport_rows in grouped.items():
        sport_rows.sort(key=lambda row: (_sort_time(row), str(row.get("display_event_name") or row.get("event_name") or "")))
        groups.append({"sport_name": sport_name, "rows": sport_rows})
    groups.sort(key=lambda group: (PREFERRED_SPORT_ORDER.get(str(group["sport_name"]), 99), str(group["sport_name"]).casefold()))
    return groups


def _sort_rows_by_sport_time(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return sorted(
        rows,
        key=lambda row: (
            PREFERRED_SPORT_ORDER.get(str(row.get("sport_name") or ""), 99),
            str(row.get("sport_name") or "").casefold(),
            _sort_time(row),
            str(row.get("display_event_name") or row.get("event_name") or "").casefold(),
        ),
    )


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
        elif event_type == "not_overdue_yet":
            entry["not_overdue"] = int(entry["not_overdue"]) + 1
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
        elif event_type in {"slack_alert_sent", "flashscore_slack_alert_sent"}:
            entry["flags"] = int(entry["flags"]) + 1
            entry["slack_alerts"] = int(entry["slack_alerts"]) + 1
        elif event_type in {"api_error", "final_verification_failed", "flashscore_suppressed_after_5min_delay_api_error"}:
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
        state_columns = _table_columns(connection, "inplay_alert_state")

        def state_column(name: str, default: str = "''") -> str:
            return name if name in state_columns else f"{default} AS {name}"

        visible_filter = """
            (
                COALESCE({visible_column}, 0) = 1
                OR COALESCE(final_verification_result, '') IN ('pending_verification', 'confirmed_not_inplay', 'failed', 'suppressed_unknown', 'suppressed_ambiguous')
                OR alert_sent_at IS NOT NULL
                OR COALESCE(slack_alert_sent, 0) = 1
                OR COALESCE(slack_error, '') != ''
                OR COALESCE(final_verification_reason, '') LIKE '%ambiguous%'
            )
            AND NOT (
                COALESCE(final_verification_result, '') = 'suppressed_inplay'
                AND COALESCE(slack_alert_sent, 0) = 0
                AND alert_sent_at IS NULL
                AND COALESCE(slack_error, '') = ''
            )
            AND NOT (
                trigger_source = 'flashscore_live'
                AND COALESCE(betfair_last_seen_inplay, last_seen_inplay, 0) = 1
                AND COALESCE(slack_alert_sent, 0) = 0
                AND alert_sent_at IS NULL
                AND COALESCE(slack_error, '') = ''
            )
        """.format(visible_column="visible_in_hub" if "visible_in_hub" in state_columns else "0")

        active_flags = _rows(
            connection,
            f"""
            SELECT event_id, market_id, sport_name, competition_name, event_name,
                   scheduled_start_utc, scheduled_start_uk, first_flagged_at, alert_sent_at,
                   last_seen_status, last_seen_inplay, recovered_at, last_checked_at,
                   final_verification_at, final_verification_result, final_verification_reason,
                   trigger_source, flashscore_match_id, flashscore_url, flashscore_match_name,
                   flashscore_competition, flashscore_status, flashscore_score,
                   flashscore_detected_live_at, match_confidence, match_reason,
                   betfair_last_checked_at, betfair_last_seen_inplay, betfair_last_seen_status,
                   slack_alert_sent, slack_error, {state_column("flashscore_participant_1")},
                   {state_column("flashscore_participant_2")}, {state_column("betfair_participant_1")},
                   {state_column("betfair_participant_2")}, {state_column("flashscore_surname_1")},
                   {state_column("flashscore_surname_2")}, {state_column("betfair_surname_1")},
                   {state_column("betfair_surname_2")}, {state_column("match_score", "NULL")},
                   {state_column("match_format")}, {state_column("side_1_player_1")},
                   {state_column("side_1_player_2")}, {state_column("side_2_player_1")},
                   {state_column("side_2_player_2")}, {state_column("side_1_surnames")},
                   {state_column("side_2_surnames")}, {state_column("betfair_side_1_players")},
                   {state_column("betfair_side_2_players")}, {state_column("betfair_side_1_surnames")},
                   {state_column("betfair_side_2_surnames")}, {state_column("pending_verification_at")},
                   {state_column("verify_after")}, {state_column("alert_delay_seconds", "NULL")},
                   {state_column("candidate_first_seen_at")}, {state_column("final_marketbook_status_raw")},
                   {state_column("final_marketbook_inplay_raw")},
                   {state_column("final_marketbook_inplay_parsed", "NULL")},
                   {state_column("final_marketbook_status_parsed")},
                   {state_column("visible_in_hub", "1")}
            FROM inplay_alert_state
            WHERE {visible_filter}
            ORDER BY COALESCE(flashscore_detected_live_at, scheduled_start_utc, betfair_last_checked_at, last_checked_at, first_flagged_at) ASC
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
            f"""
            SELECT event_id, market_id, sport_name, competition_name, event_name,
                   scheduled_start_utc, scheduled_start_uk, alert_sent_at, last_seen_status,
                   last_seen_inplay, last_checked_at, final_verification_result, final_verification_reason,
                   trigger_source, flashscore_match_name, flashscore_competition, flashscore_status,
                   flashscore_score, match_confidence, match_reason, betfair_last_checked_at,
                   betfair_last_seen_inplay, betfair_last_seen_status, slack_alert_sent, slack_error,
                   {state_column("match_score", "NULL")}, {state_column("match_format")},
                   {state_column("verify_after")}, {state_column("pending_verification_at")},
                   {state_column("final_marketbook_status_raw")},
                   {state_column("final_marketbook_inplay_raw")},
                   {state_column("final_marketbook_inplay_parsed", "NULL")},
                   {state_column("final_marketbook_status_parsed")}
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
            WHERE level = 'ERROR'
               OR event_type IN (
                    'slack_alert_sent',
                    'slack_alert_failed',
                    'flashscore_slack_alert_sent',
                    'flashscore_slack_alert_failed',
                    'flashscore_candidate_created_pending_5min',
                    'flashscore_candidate_pending_5min_verification',
                    'flashscore_delayed_verification_due',
                    'flashscore_delayed_verification_confirmed_not_inplay',
                    'flashscore_final_decision_send_slack',
                    'flashscore_final_inplay_unknown_no_alert',
                    'flashscore_same_event_other_market_inplay_no_alert',
                    'api_error',
                    'final_verification_failed',
                    'flashscore_suppressed_after_5min_delay_api_error',
                    'name_match_ambiguous_no_alert',
                    'doubles_name_match_ambiguous_no_alert',
                    'flashscore_live_no_betfair_match',
                    'flashscore_tennis_doubles_parse_failed',
                    'run_skipped_existing_run_active'
               )
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
        active_flags = _sort_rows_by_sport_time(active_flags)
        active_groups = _group_active_rows(active_flags)
        for rows in (recovered_events,):
            for row in rows:
                for key in ("first_flagged_at", "alert_sent_at", "recovered_at", "last_checked_at", "timestamp"):
                    if key in row:
                        row[key] = _format_db_time(row.get(key))
        _decorate_log_rows(recent_logs)
        _decorate_log_rows(recent_skipped_events)

        ambiguous_matches = sum(1 for row in latest_scan_logs if row.get("event_type") in {"name_match_ambiguous_no_alert", "doubles_name_match_ambiguous_no_alert"})
        active_not_inplay_flags = sum(1 for row in active_flags if row.get("inplay_label") == "No")
        last_error_row = next((row for row in recent_logs if row.get("level") == "ERROR"), None)

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
            "active_not_inplay_flags": str(active_not_inplay_flags),
            "ambiguous_matches_skipped": str(ambiguous_matches),
            "last_error": str(last_error_row.get("message") if last_error_row else ""),
            "dry_run": "yes" if latest_run.get("dry_run") else "no",
            "status": str(latest_run.get("status", "")),
        }
        return InPlayCheckerResult(
            summary=summary,
            sport_breakdown=sport_breakdown,
            active_groups=active_groups,
            active_flags=active_flags,
            recovered_events=recovered_events,
            recent_alerts=recent_alerts,
            recent_skipped_events=recent_skipped_events,
            recent_logs=recent_logs,
            config_error=str(latest_run.get("config_error") or ""),
        )
    finally:
        connection.close()
