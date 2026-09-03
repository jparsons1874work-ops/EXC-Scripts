from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class RunWindow:
    start: str
    end: str
    timezone: str = "Europe/London"


@dataclass(frozen=True)
class ScriptSpec:
    id: str
    name: str
    category: str
    description: str
    relative_path: str
    default_args: tuple[str, ...] = ()
    long_running: bool = False
    needs_parameters: bool = False
    parsed_output: bool = False
    allowed_window: RunWindow | None = None
    timeout_seconds: int = 600
    allow_concurrent: bool = False
    auto_start_times: tuple[str, ...] = ()
    auto_stop_times: tuple[str, ...] = ()
    auto_start_on_hub_start: bool = False
    automation_timezone: str = "Europe/London"


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def script(
    name: str,
    category: str,
    description: str,
    relative_path: str,
    default_args: tuple[str, ...] = (),
    long_running: bool = False,
    needs_parameters: bool = False,
    parsed_output: bool = False,
    allowed_window: RunWindow | None = None,
    timeout_seconds: int = 600,
    allow_concurrent: bool = False,
    auto_start_times: tuple[str, ...] = (),
    auto_stop_times: tuple[str, ...] = (),
    auto_start_on_hub_start: bool = False,
    automation_timezone: str = "Europe/London",
    id_override: str | None = None,
) -> ScriptSpec:
    return ScriptSpec(
        id=id_override or slugify(name),
        name=name,
        category=category,
        description=description,
        relative_path=relative_path,
        default_args=default_args,
        long_running=long_running,
        needs_parameters=needs_parameters,
        parsed_output=parsed_output,
        allowed_window=allowed_window,
        timeout_seconds=timeout_seconds,
        allow_concurrent=allow_concurrent,
        auto_start_times=auto_start_times,
        auto_stop_times=auto_stop_times,
        auto_start_on_hub_start=auto_start_on_hub_start,
        automation_timezone=automation_timezone,
    )


SCRIPT_REGISTRY: tuple[ScriptSpec, ...] = (
    script(
        "Golf - Non-Runner Check",
        "Golf",
        "Monitors official tour field pages and alerts confirmed additions or withdrawals.",
        "scripts/Golf_Exchange_NR_Checks.py",
        ("--repeat-minutes", "5"),
        long_running=True,
        timeout_seconds=10 * 365 * 24 * 60 * 60,
        auto_start_on_hub_start=True,
    ),
    script(
        "Cricket - Decimal Fixture Scrape",
        "Cricket",
        "Scrapes Decimal cricket fixtures and writes the latest output workbook.",
        "scripts/Decimal_Cricket_Scrape_Auto.py",
        timeout_seconds=20 * 60,
    ),
    script(
        "SAMM - Selection Name Check",
        "SAMM",
        "Extracts selection names and probabilities from Polymarket or Kalshi.",
        "scripts/Signal_Selection_Extractor.py",
        needs_parameters=True,
    ),
    script(
        "Betfair - Duplicate Match Check",
        "Betfair",
        "Monitors Betfair Exchange fixtures for duplicate match listings.",
        "scripts/Betfair_Duplicate_Match_Check.py",
        ("--repeat-minutes", "30", "--send-startup-message", "--send-shutdown-message"),
        long_running=True,
        allowed_window=RunWindow("07:00", "23:00"),
        timeout_seconds=17 * 60 * 60,
        auto_start_times=("07:00",),
        auto_stop_times=("23:00",),
        auto_start_on_hub_start=True,
    ),
    script(
        "Betfair - Duplicate Market Check",
        "Betfair",
        "Checks Betfair Exchange football events for duplicate market names.",
        "scripts/Betfair_Duplicate_Market_Check.py",
        timeout_seconds=16 * 60 * 60,
        auto_start_times=("07:00", "15:00", "23:00"),
    ),
    script(
        "Betfair In-Play Start Checker",
        "Betfair",
        "Flags MATCH_ODDS markets whose scheduled start is overdue but not in-play.",
        "scripts/Betfair_InPlay_Start_Checker.py",
        ("--repeat-minutes", "2", "--send-startup-message", "--send-shutdown-message"),
        long_running=True,
        parsed_output=True,
        allowed_window=RunWindow("23:05", "23:00"),
        timeout_seconds=25 * 60 * 60,
        auto_start_times=("07:00", "23:05"),
        auto_stop_times=("23:00",),
        auto_start_on_hub_start=True,
    ),
    script(
        "Betfair Event Reminders",
        "Reminders",
        "Runs hourly and schedules Slack reminders for selected Betfair sports events.",
        "scripts/Betfair_Event_Reminders.py",
        ("--pause-on-exit",),
        timeout_seconds=30 * 60,
    ),
    ScriptSpec(
        id="ufc-live-start-scanner",
        name="UFC - Live Start Scanner",
        category="UFC",
        description="Scans the current UFC card for LIVE NOW fights and alerts the in-play Slack channel.",
        relative_path="scripts/UFC_Live_Start_Scanner.py",
        long_running=True,
        timeout_seconds=6 * 60 * 60,
    ),
    ScriptSpec(
        id="pfl-live-start-scanner",
        name="PFL - Live Start Scanner",
        category="PFL",
        description="Scans the current PFL card for live fights and alerts the UFC in-play Slack channel.",
        relative_path="scripts/PFL_Live_Start_Scanner.py",
        long_running=True,
        timeout_seconds=6 * 60 * 60,
    ),
    script(
        "Tennis - Integrity Check",
        "Tennis",
        "Runs the integrity scanner against Betfair tennis markets.",
        "scripts/Integrity-Scanner/start_scanner.py",
        long_running=True,
        allowed_window=RunWindow("07:00", "23:00"),
        timeout_seconds=17 * 60 * 60,
        auto_start_times=("07:00",),
        auto_stop_times=("23:00",),
        auto_start_on_hub_start=True,
    ),
    script(
        "Tennis - Manual Coverage Watcher",
        "Tennis",
        "Monitors selected Flashscore tennis tournaments or matches and sends operational Slack alerts.",
        "scripts/Tennis_Manual_Watcher_Supervisor.py",
        ("--poll-seconds", "10", "--reload-minutes", "15"),
        long_running=True,
        timeout_seconds=10 * 365 * 24 * 60 * 60,
        id_override="tennis-challenger-watcher",
    ),
    script(
        "Cricket - Time Check Today",
        "Cricket",
        "Compares today's Betfair and Decimal cricket fixture start times.",
        "scripts/exc-cric-time-check/web_time_check_runner.py",
        ("--today", "--pretty"),
        parsed_output=True,
    ),
    script(
        "Cricket - Time Check Tomorrow",
        "Cricket",
        "Compares tomorrow's Betfair and Decimal cricket fixture start times.",
        "scripts/exc-cric-time-check/web_time_check_runner.py",
        ("--tomorrow", "--pretty"),
        parsed_output=True,
    ),
)

SCRIPTS_BY_ID = {spec.id: spec for spec in SCRIPT_REGISTRY}
CATEGORIES = sorted({spec.category for spec in SCRIPT_REGISTRY})
