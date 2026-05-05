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
) -> ScriptSpec:
    return ScriptSpec(
        id=slugify(name),
        name=name,
        category=category,
        description=description,
        relative_path=relative_path,
        default_args=default_args,
        long_running=long_running,
        needs_parameters=needs_parameters,
        parsed_output=parsed_output,
        allowed_window=allowed_window,
    )


SCRIPT_REGISTRY: tuple[ScriptSpec, ...] = (
    script(
        "Golf - Non-Runner Check",
        "Golf",
        "Checks Betfair Exchange golf markets for potential non-runner issues.",
        "scripts/Golf_Exchange_NR_Checks.py",
    ),
    script(
        "Cricket - Decimal Fixture Scrape",
        "Cricket",
        "Scrapes Decimal cricket fixtures and writes the latest output workbook.",
        "scripts/Decimal_Cricket_Scrape_Auto.py",
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
    ),
    script(
        "Betfair - Duplicate Market Check",
        "Betfair",
        "Checks Betfair Exchange football events for duplicate market names.",
        "scripts/Betfair_Duplicate_Market_Check.py",
        long_running=True,
        allowed_window=RunWindow("07:00", "23:00"),
    ),
    script(
        "Tennis - Integrity Check",
        "Tennis",
        "Runs the integrity scanner against Betfair tennis markets.",
        "scripts/Integrity-Scanner/start_scanner.py",
        long_running=True,
        allowed_window=RunWindow("07:00", "23:00"),
    ),
    script(
        "Cricket - Time Check Today",
        "Cricket",
        "Compares today's Betfair and Decimal cricket fixture start times.",
        "scripts/exc-cric-time-check/betfair_decimal_time_checker.py",
        ("--today", "--pretty"),
        parsed_output=True,
    ),
    script(
        "Cricket - Time Check Tomorrow",
        "Cricket",
        "Compares tomorrow's Betfair and Decimal cricket fixture start times.",
        "scripts/exc-cric-time-check/betfair_decimal_time_checker.py",
        ("--tomorrow", "--pretty"),
        parsed_output=True,
    ),
)

SCRIPTS_BY_ID = {spec.id: spec for spec in SCRIPT_REGISTRY}
CATEGORIES = sorted({spec.category for spec in SCRIPT_REGISTRY})
