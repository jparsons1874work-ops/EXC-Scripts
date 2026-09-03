#!/usr/bin/env python3
"""Supervise the manual tennis watcher, resetting at :25 and :55 UK time."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.tennis_challenger import STATE_PATH, atomic_write_json, read_state


STOP_EVENT = threading.Event()
RESTART_PAUSE_SECONDS = 30
# Leave time for process discovery and forced cleanup within the Hub's 8-second
# stop grace, so its Stop button does not kill the supervisor mid-cleanup.
STOP_GRACE_SECONDS = 3
UK_TIMEZONE = ZoneInfo("Europe/London")


def log(message: str) -> None:
    try:
        print(f"[Tennis supervisor] {message}", flush=True)
    except BrokenPipeError:
        pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def next_reset_at(now: datetime) -> datetime:
    """Continue the 12:55 UK anchor every half hour, including overnight."""
    # London changes by whole hours for daylight saving, so these UTC minute
    # marks also remain :25 and :55 locally through both clock changes.
    now = now.astimezone(timezone.utc)
    candidate = now.replace(minute=25, second=0, microsecond=0)
    while candidate <= now:
        candidate += timedelta(minutes=30)
    return candidate


def publish_phase(phase: str, *, status: str = "running", error: str = "") -> None:
    # Only write after the worker has exited so its state writer cannot race us.
    atomic_write_json(
        STATE_PATH,
        {**read_state(), "phase": phase, "status": status, "last_error": error},
    )


def start_worker(args: list[str]) -> subprocess.Popen:
    options = (
        {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
        if os.name == "nt"
        else {"start_new_session": True}
    )
    return subprocess.Popen(
        [sys.executable, "-u", str(PROJECT_ROOT / "scripts" / "Tennis_Challenger_Watcher.py"), *args],
        cwd=str(PROJECT_ROOT),
        **options,
    )


def stop_worker(process: subprocess.Popen) -> None:
    """Stop the worker and its browsers before allowing any replacement."""
    if os.name == "nt":
        if process.poll() is None:
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
                check=False,
            )
            if process.poll() is None:
                process.kill()
    else:
        groups = worker_process_groups(process.pid)
        # The worker is a session leader. Keep its group ID even if the worker
        # exits first, so abandoned Playwright/Chromium children are also killed.
        for group in groups:
            try:
                os.killpg(group, signal.SIGTERM)
            except ProcessLookupError:
                pass
        try:
            process.wait(timeout=STOP_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            log("Watcher did not stop cleanly; forcing its process group to exit")
        finally:
            for group in groups:
                try:
                    os.killpg(group, signal.SIGKILL)
                except ProcessLookupError:
                    pass
    # If termination cannot be confirmed, fail instead of creating duplicates.
    process.wait(timeout=2)


def worker_process_groups(pid: int) -> set[int]:
    """Include browser descendants that create their own process groups."""
    try:
        completed = subprocess.run(
            ["ps", "-eo", "pid=,ppid=,pgid="],
            capture_output=True,
            text=True,
            timeout=2,
            check=True,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        log(f"Could not inspect browser groups; stopping the watcher's own group: {exc}")
        return {pid}
    rows = [tuple(map(int, line.split())) for line in completed.stdout.splitlines() if line.strip()]
    descendants = {pid}
    groups = {pid}
    while True:
        children = {child for child, parent, _group in rows if parent in descendants}
        if children <= descendants:
            break
        descendants.update(children)
    groups.update(group for child, _parent, group in rows if child in descendants)
    return groups


def supervise(args: list[str]) -> int:
    log("Hard resets at :25 and :55 UK time, with a 30-second pause before restarting")
    while not STOP_EVENT.is_set():
        reset_at = next_reset_at(utc_now())
        reset_label = reset_at.astimezone(UK_TIMEZONE).strftime("%H:%M %Z")
        process = None
        try:
            try:
                process = start_worker(args)
            except OSError as exc:
                reason = f"Watcher could not start: {exc}"
            else:
                log(f"Started watcher PID {process.pid}; next hard reset {reset_label}")
                while not STOP_EVENT.is_set():
                    if utc_now() >= reset_at:
                        reason = f"Scheduled {reset_label} hard reset"
                        break
                    return_code = process.poll()
                    if return_code is not None:
                        reason = f"Watcher exited with code {return_code}"
                        break
                    STOP_EVENT.wait(0.5)
        finally:
            if process is not None:
                stop_worker(process)

        if STOP_EVENT.is_set():
            break
        log(f"{reason}; stopped completely, waiting 30 seconds before a fresh start")
        publish_phase(f"{reason} · Restarting in 30 seconds")
        if STOP_EVENT.wait(RESTART_PAUSE_SECONDS):
            break

    publish_phase("Stopped", status="stopped")
    log("Supervisor stopped; automatic restarts cancelled")
    return 0


def _handle_stop(_signum: int, _frame) -> None:
    STOP_EVENT.set()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--poll-seconds", type=float, default=10.0)
    parser.add_argument("--reload-minutes", type=float, default=15.0)
    args = parser.parse_args()
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)
    try:
        return supervise([
            "--poll-seconds", str(args.poll_seconds),
            "--reload-minutes", str(args.reload_minutes),
        ])
    except Exception as exc:
        log(f"Supervisor failed: {exc}")
        publish_phase("Supervisor failed", status="failed", error=str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
