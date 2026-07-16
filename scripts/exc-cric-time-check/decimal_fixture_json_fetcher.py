#!/usr/bin/env python3
"""Fetch Decimal cricket fixtures and write them as JSON."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]
TIME_CHECKER_PATH = SCRIPT_DIR / "betfair_decimal_time_checker.py"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "runtime" / "output"


def load_time_checker() -> ModuleType:
    """Load the existing cricket time checker so this file can reuse its Decimal scraper."""
    spec = importlib.util.spec_from_file_location("betfair_decimal_time_checker", TIME_CHECKER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load Decimal scraper from {TIME_CHECKER_PATH}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Decimal cricket fixtures only and save the result as JSON."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--today", action="store_true", help="Fetch today's Decimal fixtures in UK time.")
    group.add_argument("--tomorrow", action="store_true", help="Fetch tomorrow's Decimal fixtures in UK time.")
    group.add_argument("--date", help="Fetch a specific UK-local date, formatted as YYYY-MM-DD.")
    group.add_argument(
        "--all-upcoming",
        action="store_true",
        help="Fetch every fixture in Decimal's This Month and Next Month and Beyond panels.",
    )
    parser.add_argument(
        "--output",
        help=(
            "JSON output path. Defaults to "
            "runtime/output/decimal_cricket_fixtures_<target-date>.json, or "
            "runtime/output/decimal_cricket_fixtures_all.json with --all-upcoming."
        ),
    )
    parser.add_argument("--stdout", action="store_true", help="Also print the JSON payload to stdout.")
    parser.add_argument("--verbose", action="store_true", help="Write progress messages to stderr.")
    parser.add_argument(
        "--debug-decimal",
        action="store_true",
        help="Write Decimal scrape debug artifacts under runtime/output.",
    )
    parser.add_argument(
        "--debug-browser",
        "--print-browser-env",
        action="store_true",
        help="Print Chrome, ChromeDriver, OS, and architecture diagnostics without logging into Decimal.",
    )
    return parser.parse_args()


def resolve_target_day(args: argparse.Namespace, checker: ModuleType) -> date:
    now_uk = datetime.now(checker.UK_TZ)
    if args.date:
        try:
            return date.fromisoformat(args.date)
        except ValueError as exc:
            raise ValueError("--date must be formatted as YYYY-MM-DD") from exc
    if args.tomorrow:
        return (now_uk + timedelta(days=1)).date()
    return now_uk.date()


def validate_decimal_config(checker: ModuleType) -> None:
    missing = [
        name
        for name in ("DECIMAL_USERNAME", "DECIMAL_PASSWORD")
        if not getattr(checker, name, "")
    ]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"Missing required Decimal environment variables: {joined}")


def fixture_to_json(fixture: Any, checker: ModuleType) -> dict[str, Any]:
    start_time = fixture.start_time.astimezone(checker.UK_TZ)
    payload = {
        "match_name": fixture.match_name,
        "competition": fixture.competition,
        "venue": getattr(fixture, "venue", ""),
        "start_time": start_time.isoformat(),
        "start_time_uk": start_time.strftime(checker.TIME_OUTPUT_FORMAT),
        "source": fixture.source,
        "event_id": fixture.event_id,
    }
    metadata = getattr(fixture, "metadata", None)
    if metadata:
        payload["decimal_row"] = metadata
    return payload


def build_payload(target_day: date, fixtures: list[Any], checker: ModuleType) -> dict[str, Any]:
    generated_at = datetime.now(checker.UK_TZ)
    return {
        "source": "decimal",
        "target_date": target_day.isoformat(),
        "timezone": "Europe/London",
        "generated_at": generated_at.isoformat(),
        "fixture_count": len(fixtures),
        "fixtures": [fixture_to_json(fixture, checker) for fixture in fixtures],
    }


def build_upcoming_payload(fixtures: list[Any], checker: ModuleType) -> dict[str, Any]:
    generated_at = datetime.now(checker.UK_TZ)
    serialized_fixtures = [fixture_to_json(fixture, checker) for fixture in fixtures]
    detected_sections = {
        str((fixture.get("decimal_row") or {}).get("section", ""))
        for fixture in serialized_fixtures
        if isinstance(fixture.get("decimal_row"), dict)
    }
    section_order = ("this_month", "next_month", "next_month_and_beyond", "beyond")
    return {
        "source": "decimal",
        "scope": "all_upcoming",
        "sections": [section for section in section_order if section in detected_sections],
        "timezone": "Europe/London",
        "generated_at": generated_at.isoformat(),
        "fixture_count": len(serialized_fixtures),
        "fixtures": serialized_fixtures,
    }


def output_path_for(args: argparse.Namespace, target_day: date | None) -> Path:
    if args.output:
        return Path(args.output).expanduser()
    if args.all_upcoming:
        return DEFAULT_OUTPUT_DIR / "decimal_cricket_fixtures_all.json"
    if target_day is None:
        raise ValueError("A target date is required for a daily fixture output file.")
    return DEFAULT_OUTPUT_DIR / f"decimal_cricket_fixtures_{target_day.isoformat()}.json"


def main() -> int:
    args = parse_args()
    checker = load_time_checker()

    if args.debug_browser:
        checker.print_browser_diagnostics()
        return 0

    try:
        validate_decimal_config(checker)
        if args.all_upcoming:
            target_day = None
            fixtures = checker.fetch_decimal_upcoming_fixtures(args.verbose, args.debug_decimal)
            payload = build_upcoming_payload(fixtures, checker)
        else:
            target_day = resolve_target_day(args, checker)
            fixtures = checker.fetch_decimal_fixtures(target_day, args.verbose, args.debug_decimal)
            payload = build_payload(target_day, fixtures, checker)
        output_path = output_path_for(args, target_day)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_text = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
        temporary_path = output_path.with_suffix(output_path.suffix + f".{os.getpid()}.tmp")
        try:
            temporary_path.write_text(output_text, encoding="utf-8")
            temporary_path.replace(output_path)
        finally:
            temporary_path.unlink(missing_ok=True)
        print(f"Wrote Decimal fixture JSON: {output_path}")
        if args.stdout:
            print(output_text, end="")
        return 0
    except Exception as exc:
        print(f"Error type: {type(exc).__name__}", file=sys.stderr)
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
