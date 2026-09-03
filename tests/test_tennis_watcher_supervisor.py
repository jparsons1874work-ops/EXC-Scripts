from __future__ import annotations

import signal
import subprocess
import sys
import threading
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, call, patch
from zoneinfo import ZoneInfo

from scripts import Tennis_Manual_Watcher_Supervisor as supervisor


class TennisWatcherSupervisorTests(unittest.TestCase):
    def test_clock_schedule_uses_1255_anchor_and_continues_overnight(self) -> None:
        uk = ZoneInfo("Europe/London")
        for current, expected in [
            ("2026-09-03T12:54:59", "2026-09-03T12:55:00"),
            ("2026-09-03T12:55:00", "2026-09-03T13:25:00"),
            ("2026-09-03T13:25:30", "2026-09-03T13:55:00"),
            ("2026-09-03T23:55:00", "2026-09-04T00:25:00"),
        ]:
            with self.subTest(current=current):
                now = datetime.fromisoformat(current).replace(tzinfo=uk)
                result = supervisor.next_reset_at(now).astimezone(uk)
                self.assertEqual(result, datetime.fromisoformat(expected).replace(tzinfo=uk))

    def test_clock_changes_still_reset_every_half_hour(self) -> None:
        for start in ["2026-03-29T00:55:00+00:00", "2026-10-25T00:55:00+00:00"]:
            now = datetime.fromisoformat(start)
            result = supervisor.next_reset_at(now)
            self.assertEqual(result - now, timedelta(minutes=30))
            self.assertEqual(result.astimezone(supervisor.UK_TIMEZONE).minute, 25)

    def run_supervisor_cycle(self, *, cancel_during_pause=False, exit_code=None):
        stopped = threading.Event()
        events = []
        now = datetime(2026, 9, 3, 11, 54, 59, tzinfo=timezone.utc)
        processes = []
        phases = []

        def start(args):
            events.append(("start", tuple(args)))
            process = Mock(pid=100 + len(processes))
            process.poll.return_value = exit_code
            processes.append(process)
            if len(processes) == 2:
                stopped.set()
            return process

        def wait(seconds):
            nonlocal now
            events.append(("wait", seconds))
            now += timedelta(seconds=seconds)
            if seconds == 30 and cancel_during_pause:
                stopped.set()
            return stopped.is_set()

        event = SimpleNamespace(is_set=stopped.is_set, wait=wait)
        with (
            patch.object(supervisor, "STOP_EVENT", event),
            patch.object(supervisor, "utc_now", side_effect=lambda: now),
            patch.object(supervisor, "start_worker", side_effect=start),
            patch.object(supervisor, "stop_worker", side_effect=lambda proc: events.append(("stop", proc.pid))),
            patch.object(supervisor, "publish_phase", side_effect=lambda phase, **kwargs: phases.append((phase, kwargs))),
            patch.object(supervisor, "log"),
        ):
            self.assertEqual(supervisor.supervise(["--poll-seconds", "10"]), 0)
        return events, processes, phases

    def test_scheduled_reset_stops_then_waits_30_seconds_then_starts_fresh(self) -> None:
        events, processes, phases = self.run_supervisor_cycle()
        relevant = [event for event in events if event != ("wait", 0.5)]
        self.assertEqual(relevant, [
            ("start", ("--poll-seconds", "10")),
            ("stop", 100),
            ("wait", 30),
            ("start", ("--poll-seconds", "10")),
            ("stop", 101),
        ])
        self.assertEqual(len(processes), 2)
        self.assertIn("Scheduled 12:55 BST hard reset", phases[0][0])

    def test_manual_stop_during_pause_prevents_restart(self) -> None:
        events, processes, phases = self.run_supervisor_cycle(cancel_during_pause=True)
        self.assertEqual(len(processes), 1)
        self.assertEqual(events[-1], ("wait", 30))
        self.assertEqual(phases[-1], ("Stopped", {"status": "stopped"}))

    def test_early_exit_recovers_with_the_same_pause(self) -> None:
        events, processes, phases = self.run_supervisor_cycle(exit_code=1)
        self.assertEqual(len(processes), 2)
        self.assertEqual(events[1:3], [("stop", 100), ("wait", 30)])
        self.assertIn("Watcher exited with code 1", phases[0][0])

    def test_manual_stop_while_running_cleans_up_without_restart(self) -> None:
        event = threading.Event()
        process = Mock(pid=100)

        def wait(_seconds):
            event.set()
            return True

        process.poll.return_value = None
        with (
            patch.object(supervisor, "STOP_EVENT", SimpleNamespace(is_set=event.is_set, wait=wait)),
            patch.object(supervisor, "start_worker", return_value=process) as start,
            patch.object(supervisor, "stop_worker") as stop,
            patch.object(supervisor, "publish_phase"),
            patch.object(supervisor, "log"),
        ):
            supervisor.supervise([])
        start.assert_called_once_with([])
        stop.assert_called_once_with(process)

    def test_stop_failure_does_not_start_an_overlapping_worker(self) -> None:
        process = Mock(pid=100)
        process.poll.return_value = 1
        with (
            patch.object(supervisor, "STOP_EVENT", threading.Event()),
            patch.object(supervisor, "start_worker", return_value=process) as start,
            patch.object(supervisor, "stop_worker", side_effect=subprocess.TimeoutExpired("worker", 2)),
            patch.object(supervisor, "log"),
        ):
            with self.assertRaises(subprocess.TimeoutExpired):
                supervisor.supervise([])
        start.assert_called_once_with([])

    def test_linux_cleanup_includes_browser_groups_and_forces_hung_worker(self) -> None:
        process = Mock(pid=100)
        process.wait.side_effect = [subprocess.TimeoutExpired("worker", 3), 0]
        with (
            patch.object(supervisor.os, "name", "posix"),
            patch.object(supervisor, "worker_process_groups", return_value={100, 200}),
            patch.object(supervisor.os, "killpg", create=True) as killpg,
            patch.object(supervisor.signal, "SIGKILL", 9, create=True),
            patch.object(supervisor, "log"),
        ):
            supervisor.stop_worker(process)
        killpg.assert_has_calls([
            call(100, signal.SIGTERM), call(200, signal.SIGTERM),
            call(100, 9), call(200, 9),
        ], any_order=True)
        self.assertEqual(process.wait.call_args_list, [call(timeout=3), call(timeout=2)])

    def test_linux_cleanup_kills_leftovers_even_when_worker_already_exited(self) -> None:
        process = Mock(pid=100)
        process.poll.return_value = 1
        process.wait.return_value = 1
        with (
            patch.object(supervisor.os, "name", "posix"),
            patch.object(supervisor, "worker_process_groups", return_value={100}),
            patch.object(supervisor.os, "killpg", create=True) as killpg,
            patch.object(supervisor.signal, "SIGKILL", 9, create=True),
        ):
            supervisor.stop_worker(process)
        self.assertEqual(killpg.call_args_list, [call(100, signal.SIGTERM), call(100, 9)])

    def test_browser_group_discovery_excludes_unrelated_processes(self) -> None:
        rows = "100 50 100\n101 100 100\n200 101 200\n201 200 200\n300 50 300\n"
        with patch.object(supervisor.subprocess, "run", return_value=SimpleNamespace(stdout=rows)):
            self.assertEqual(supervisor.worker_process_groups(100), {100, 200})

    def test_failed_process_discovery_still_allows_worker_group_cleanup(self) -> None:
        with (
            patch.object(supervisor.subprocess, "run", side_effect=subprocess.TimeoutExpired("ps", 2)),
            patch.object(supervisor, "log"),
        ):
            self.assertEqual(supervisor.worker_process_groups(100), {100})

    def test_phase_updates_preserve_saved_alerts_and_matches(self) -> None:
        previous = {"matches": [{"id": "match1"}], "alerts": [{"type": "match_started"}], "phase": "Watching"}
        with (
            patch.object(supervisor, "read_state", return_value=previous),
            patch.object(supervisor, "atomic_write_json") as write,
        ):
            supervisor.publish_phase("Restarting in 30 seconds")
        saved = write.call_args.args[1]
        self.assertEqual(saved["matches"], previous["matches"])
        self.assertEqual(saved["alerts"], previous["alerts"])

    def test_supervisor_can_start_from_its_script_path(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(supervisor.PROJECT_ROOT / "scripts" / "Tennis_Manual_Watcher_Supervisor.py"), "--help"],
            cwd=supervisor.PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn(":25 and :55 UK time", completed.stdout)


if __name__ == "__main__":
    unittest.main()
