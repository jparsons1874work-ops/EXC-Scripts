from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo

from app.registry import ScriptSpec


def parse_hhmm(value: str) -> time:
    hour, minute = value.split(":", 1)
    return time(int(hour), int(minute))


def window_status(spec: ScriptSpec) -> tuple[bool, str]:
    if not spec.allowed_window:
        return True, ""

    tz = ZoneInfo(spec.allowed_window.timezone)
    now = datetime.now(tz).time().replace(second=0, microsecond=0)
    start = parse_hhmm(spec.allowed_window.start)
    end = parse_hhmm(spec.allowed_window.end)

    if start <= end:
        allowed = start <= now <= end
    else:
        allowed = now >= start or now <= end

    label = f"{spec.allowed_window.start}-{spec.allowed_window.end} {spec.allowed_window.timezone}"
    return allowed, label
