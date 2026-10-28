from __future__ import annotations

import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "cricket_decimal_loading_checker",
    ROOT / "scripts" / "exc-cric-time-check" / "betfair_decimal_time_checker.py",
)
checker = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = checker
spec.loader.exec_module(checker)


def fixture_row(day: str) -> dict:
    return {
        "match_id": "12345",
        "display_name": "Zimbabwe v Bangladesh",
        "start_time": f"{day}T12:00:00Z",
        "section": "tomorrow",
    }


class DecimalFixtureLoadingTests(unittest.TestCase):
    def wait_for_snapshots(self, snapshots, *, upcoming=False):
        # Exercise Selenium's polling contract without real delays or logins.
        class SnapshotWait:
            def __init__(self, driver, timeout):
                self.driver = driver

            def until(self, predicate):
                for _ in snapshots:
                    if predicate(self.driver):
                        return True
                raise checker.TimeoutException("fixture loading timed out")

        with patch.object(checker, "WebDriverWait", SnapshotWait):
            with patch.object(checker, "extract_decimal_legacy_rows_via_js", side_effect=snapshots) as extract:
                payload = checker.wait_for_decimal_legacy_rows_via_js(
                    MagicMock(), date(2026, 10, 29), 60, False,
                    "upcoming" if upcoming else None,
                )
        return payload, extract.call_count

    def test_toolbar_partial_rows_and_other_dates_do_not_end_wait(self):
        toolbar = {
            "allRowsCount": 1,
            "candidateRowsCount": 2,
            "candidate_fixtures": [{"raw_text": "Markets 0 INPLAY STATUS PRIORITY MATCH SESSION"}],
        }
        partial = {
            "tomorrowRowsCount": 1,
            "preferred_fixtures": [{"match_id": "12345", "display_name": "Zimbabwe v Bangladesh"}],
        }
        other_date = {"all_fixtures": [fixture_row("2026-10-28")]}
        loaded = {"preferred_fixtures": [fixture_row("2026-10-29")]}
        payload, polls = self.wait_for_snapshots([toolbar, partial, other_date, loaded])
        self.assertIs(payload, loaded)
        self.assertEqual(polls, 4)

    def test_loaded_fixtures_return_without_waiting_for_timeout(self):
        loaded = {"preferred_fixtures": [fixture_row("2026-10-29")]}
        payload, polls = self.wait_for_snapshots([loaded, {}])
        self.assertIs(payload, loaded)
        self.assertEqual(polls, 1)

    def test_empty_page_still_fails_and_keeps_debug_artifacts_after_timeout(self):
        empty = {"candidateRowsCount": 2, "candidate_fixtures": [{"raw_text": "Markets 0"}]}
        payload, polls = self.wait_for_snapshots([empty, empty])
        self.assertEqual(polls, 2)
        driver = MagicMock()
        with patch.object(checker, "open_decimal_legacy_developer", return_value=payload):
            with patch.object(checker, "write_decimal_debug_artifacts") as debug:
                with self.assertRaisesRegex(RuntimeError, "No valid Decimal Legacy Developer fixtures"):
                    checker.extract_decimal_legacy_developer_fixtures(
                        driver, date(2026, 10, 29), False, True,
                    )
        self.assertIs(debug.call_args.args[0], driver)
        self.assertEqual(debug.call_args.args[3], [])
        self.assertTrue(debug.call_args.args[4])

    def test_upcoming_wait_accepts_future_dates_but_not_past_dates(self):
        past = {"preferred_fixtures": [fixture_row("2026-10-28")]}
        future = {"preferred_fixtures": [fixture_row("2026-11-15")]}
        payload, polls = self.wait_for_snapshots([past, future], upcoming=True)
        self.assertIs(payload, future)
        self.assertEqual(polls, 2)

    def test_navigation_uses_longer_fixture_timeout_without_changing_login_timeout(self):
        driver = MagicMock()
        with patch.object(checker, "open_decimal_menu_item"):
            with patch.object(checker, "WebDriverWait"):
                with patch.object(checker, "wait_for_decimal_legacy_rows_via_js", return_value={}) as wait:
                    checker.open_decimal_legacy_developer(driver, date(2026, 10, 29), False)
        self.assertEqual(wait.call_args.args[2], 60)
        self.assertEqual(checker.DECIMAL_WAIT_SECONDS, 8)


if __name__ == "__main__":
    unittest.main()
