from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo

from app.registry import ScriptSpec


def parse_hhmm(value: str) -> time:
    hour, minute = value.split(":", 1)
    return time(int(hour), int(minute))


def local_schedule_time(spec: ScriptSpec, at: datetime | None = None) -> datetime:
    tz = ZoneInfo(spec.automation_timezone)
    if at is None:
        return datetime.now(tz)
    if at.tzinfo is None:
        return at.replace(tzinfo=tz)
    return at.astimezone(tz)


def window_status(spec: ScriptSpec, at: datetime | None = None) -> tuple[bool, str]:
    if not spec.allowed_window:
        return True, ""

    tz = ZoneInfo(spec.allowed_window.timezone)
    if at is None:
        local_now = datetime.now(tz)
    elif at.tzinfo is None:
        local_now = at.replace(tzinfo=tz)
    else:
        local_now = at.astimezone(tz)
    now = local_now.time().replace(second=0, microsecond=0)
    start = parse_hhmm(spec.allowed_window.start)
    end = parse_hhmm(spec.allowed_window.end)

    if start == end:
        allowed = True
    elif start < end:
        allowed = start <= now < end
    else:
        allowed = now >= start or now < end

    label = f"{spec.allowed_window.start}-{spec.allowed_window.end} {spec.allowed_window.timezone}"
    return allowed, label
