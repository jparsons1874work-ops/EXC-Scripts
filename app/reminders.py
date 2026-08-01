from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from app.config import PROJECT_ROOT, get_setting


logger = logging.getLogger("uvicorn.error")
UK_TZ = ZoneInfo("Europe/London")
REMINDER_STATE_PATH = PROJECT_ROOT / "data" / "betfair_event_reminders_sent.json"
REMINDER_SLACK_CHANNEL_ID = (
    get_setting("BETFAIR_EVENT_REMINDERS_SLACK_CHANNEL_ID")
    or get_setting("SLACK_CHANNEL_ID")
    or "C07FXG95GQ6"
)


def _record_channel_id(record: dict[str, Any]) -> str:
    channel_id = str(record.get("slack_channel_id", "") or "")
    if channel_id:
        return channel_id
    duplicate_key = str(record.get("duplicate_key", "") or "")
    return duplicate_key.rsplit("|", 1)[-1] if "|" in duplicate_key else ""


def daily_reminders_context(
    now_uk: datetime | None = None,
    state_path: Path = REMINDER_STATE_PATH,
) -> dict[str, Any]:
    now = now_uk.astimezone(UK_TZ) if now_uk else datetime.now(UK_TZ)
    rows: list[dict[str, str]] = []
    try:
        data = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("reminder_state_read_failed path=%s error=%r", state_path, exc)
        data = {}

    records = data.get("scheduled", []) if isinstance(data, dict) else []
    if isinstance(records, list):
        for record in records:
            if not isinstance(record, dict) or str(record.get("status", "scheduled")) != "scheduled":
                continue
            if _record_channel_id(record) != REMINDER_SLACK_CHANNEL_ID:
                continue
            try:
                post_at = datetime.fromtimestamp(int(record.get("scheduled_slack_post_epoch", 0)), UK_TZ)
            except (TypeError, ValueError, OSError, OverflowError):
                continue
            if post_at.date() != now.date():
                continue
            rows.append(
                {
                    "time": post_at.strftime("%H:%M %Z"),
                    "sport": str(record.get("sport", "") or "Unknown"),
                    "message": str(record.get("slack_text", "") or ""),
                    "sort_key": post_at.isoformat(),
                }
            )

    rows.sort(key=lambda row: (row["sort_key"], row["sport"], row["message"]))
    return {
        "date_label": now.strftime("%A %d %B %Y").replace(" 0", " "),
        "rows": rows,
    }
