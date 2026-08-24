from __future__ import annotations

import threading
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import app.runner as runner_module
from app.registry import RunWindow, SCRIPTS_BY_ID, script
from app.runner import ScriptRunner
from app.scheduler import window_status


UK_TZ = ZoneInfo("Europe/London")


def automation_runner() -> ScriptRunner:
    runner = ScriptRunner.__new__(ScriptRunner)
    runner._automation_lock = threading.RLock()
    runner._handled_automation_triggers = set()
    runner.start = Mock()
    runner.stop = Mock()
    runner.stop_expired_windows = Mock()
    runner.get_state = Mock(return_value=SimpleNamespace(status="idle"))
    return runner


class ScriptAutomationTests(unittest.TestCase):
    def test_registry_has_requested_uk_schedules(self) -> None:
        golf = SCRIPTS_BY_ID["golf-non-runner-check"]
        duplicate_match = SCRIPTS_BY_ID["betfair-duplicate-match-check"]
        duplicate_market = SCRIPTS_BY_ID["betfair-duplicate-market-check"]
        tennis = SCRIPTS_BY_ID["tennis-integrity-check"]
        inplay = SCRIPTS_BY_ID["betfair-in-play-start-checker"]

        self.assertEqual(golf.default_args, ("--repeat-minutes", "5"))
        self.assertEqual(golf.auto_start_times, ("07:00",))
        self.assertEqual(golf.auto_stop_times, ("23:00",))
        self.assertTrue(golf.auto_start_on_hub_start)
        self.assertEqual(duplicate_match.auto_start_times, ("07:00",))
        self.assertEqual(duplicate_match.auto_stop_times, ("23:00",))
        self.assertEqual(tennis.auto_start_times, ("07:00",))
        self.assertEqual(tennis.auto_stop_times, ("23:00",))
        self.assertFalse(duplicate_market.long_running)
        self.assertEqual(duplicate_market.auto_start_times, ("07:00", "15:00", "23:00"))
        self.assertEqual(inplay.auto_start_times, ("07:00", "23:05"))
        self.assertEqual(inplay.auto_stop_times, ("23:00",))

    def test_window_end_is_exclusive(self) -> None:
        spec = script(
            "Window Test",
            "Test",
            "Test",
            "test.py",
            allowed_window=RunWindow("07:00", "23:00"),
        )
        self.assertTrue(window_status(spec, datetime(2026, 8, 1, 22, 59, tzinfo=UK_TZ))[0])
        self.assertFalse(window_status(spec, datetime(2026, 8, 1, 23, 0, tzinfo=UK_TZ))[0])

    def test_scheduled_start_runs_once_during_matching_minute(self) -> None:
        spec = script(
            "Scheduled Test",
            "Test",
            "Test",
            "test.py",
            auto_start_times=("15:00",),
        )
        runner = automation_runner()
        at = datetime(2026, 8, 1, 15, 0, 30, tzinfo=UK_TZ)

        with patch.dict(runner_module.SCRIPTS_BY_ID, {spec.id: spec}, clear=True):
            runner.run_automations(at=at)
            runner.run_automations(at=at.replace(second=45))

        runner.start.assert_called_once_with(spec.id, [])

    def test_hub_start_catches_up_continuous_window_job(self) -> None:
        spec = script(
            "Catch-up Test",
            "Test",
            "Test",
            "test.py",
            long_running=True,
            allowed_window=RunWindow("07:00", "23:00"),
            auto_start_times=("07:00",),
            auto_start_on_hub_start=True,
        )
        runner = automation_runner()

        with patch.dict(runner_module.SCRIPTS_BY_ID, {spec.id: spec}, clear=True):
            runner.run_automations(catch_up=True, at=datetime(2026, 8, 1, 12, 0, tzinfo=UK_TZ))

        runner.start.assert_called_once_with(spec.id, [])

    def test_inplay_restart_break_stops_before_catch_up(self) -> None:
        spec = SCRIPTS_BY_ID["betfair-in-play-start-checker"]
        self.assertFalse(window_status(spec, datetime(2026, 8, 1, 23, 0, tzinfo=UK_TZ))[0])
        self.assertFalse(window_status(spec, datetime(2026, 8, 1, 23, 4, tzinfo=UK_TZ))[0])
        self.assertTrue(window_status(spec, datetime(2026, 8, 1, 23, 5, tzinfo=UK_TZ))[0])
        self.assertTrue(window_status(spec, datetime(2026, 8, 2, 7, 0, tzinfo=UK_TZ))[0])


if __name__ == "__main__":
    unittest.main()
