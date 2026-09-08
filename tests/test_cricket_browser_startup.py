from __future__ import annotations

import importlib.util
import io
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = ROOT / "scripts" / "exc-cric-time-check"
spec = importlib.util.spec_from_file_location(
    "cricket_browser_test_checker", SCRIPT_DIR / "betfair_decimal_time_checker.py"
)
checker = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = checker
spec.loader.exec_module(checker)


class CricketBrowserStartupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=ROOT / "runtime" / "output")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.profile_root = self.root / "hub profile"
        self.profile_root.mkdir()
        self.sentinel = self.profile_root / "existing-profile-data"
        self.sentinel.write_text("keep", encoding="utf-8")
        self.env = patch.dict(os.environ, {"CHROME_PROFILE_DIR": str(self.profile_root)})
        self.env.start()
        self.addCleanup(self.env.stop)
        for name, value in (
            ("detect_chrome_binary", "/test/chrome"),
            ("detect_chromedriver_binary", ""),
            ("browser_diagnostics", {"chrome_version": "test"}),
        ):
            patcher = patch.object(checker, name, return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch.object(checker, "DECIMAL_DEBUG_DIR", self.root / "debug")
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_hub_profile_is_used_and_pipe_session_is_cleaned_on_quit(self):
        with patch.object(checker.webdriver.Chrome, "__init__", return_value=None) as start:
            driver = checker.build_chrome_driver()
        self.addCleanup(driver._decimal_profile.cleanup)
        options = start.call_args.kwargs["options"]
        profile = Path(driver._decimal_profile.name)
        self.assertEqual(profile.parent, self.profile_root)
        self.assertIn(f"--user-data-dir={profile}", options.arguments)
        self.assertIn("--remote-debugging-pipe", options.arguments)
        self.assertIn("--headless=new", options.arguments)
        self.assertNotIn("--remote-debugging-port=0", options.arguments)
        self.assertEqual(options.page_load_strategy, "eager")
        service = start.call_args.kwargs["service"]
        service.stop()
        with patch.object(checker.webdriver.Chrome, "quit"):
            driver.quit()
        self.assertFalse(profile.exists())
        self.assertTrue(self.sentinel.exists())

    def test_unreachable_browser_retries_once_with_fresh_profile_and_port(self):
        profiles = []
        options_seen = []
        services = []

        def start(**kwargs):
            profiles.append(Path(kwargs["profile"].name))
            options_seen.append(kwargs["options"])
            services.append(kwargs["service"])
            kwargs["service"].stop = MagicMock(wraps=kwargs["service"].stop)
            if len(profiles) == 1:
                raise checker.WebDriverException("session not created: chrome not reachable")
            self.assertFalse(profiles[0].exists())
            services[0].stop.assert_called_once()
            result = MagicMock()
            result.profile = kwargs["profile"]
            return result

        with patch.object(checker, "DecimalChromeDriver", side_effect=start) as constructor:
            with redirect_stderr(io.StringIO()) as output:
                driver = checker.build_chrome_driver()
        self.addCleanup(driver.profile.cleanup)
        self.addCleanup(services[1].stop)
        self.assertEqual(constructor.call_count, 2)
        self.assertNotEqual(*profiles)
        self.assertIn("--remote-debugging-port=0", options_seen[1].arguments)
        self.assertNotIn("--remote-debugging-pipe", options_seen[1].arguments)
        self.assertIn("retrying once", output.getvalue())
        self.assertTrue(self.sentinel.exists())

    def test_both_failures_are_reported_and_profiles_are_removed(self):
        errors = [
            checker.WebDriverException("chrome not reachable"),
            checker.WebDriverException("Chrome instance exited"),
        ]
        with patch.object(checker, "DecimalChromeDriver", side_effect=errors) as constructor:
            with redirect_stderr(io.StringIO()):
                with self.assertRaises(RuntimeError) as context:
                    checker.build_chrome_driver()
        self.assertEqual(constructor.call_count, 2)
        self.assertIs(context.exception.__cause__, errors[1])
        self.assertIn("Attempt 1 (pipe)", str(context.exception))
        self.assertIn("Attempt 2 (port)", str(context.exception))
        self.assertIn("ChromeDriver log:", str(context.exception))
        self.assertEqual(list(self.profile_root.iterdir()), [self.sentinel])

    def test_driver_version_and_discovery_errors_are_not_retried(self):
        for message in (
            "session not created: This version of ChromeDriver only supports Chrome version 140",
            "Unable to obtain driver for chrome",
        ):
            with self.subTest(message=message):
                with patch.object(
                    checker, "DecimalChromeDriver", side_effect=checker.WebDriverException(message)
                ) as constructor:
                    with self.assertRaisesRegex(RuntimeError, message):
                        checker.build_chrome_driver()
                self.assertEqual(constructor.call_count, 1)
                self.assertEqual(list(self.profile_root.iterdir()), [self.sentinel])

    def test_explicit_driver_is_preserved_and_command_logging_is_disabled(self):
        with patch.object(checker, "detect_chromedriver_binary", return_value="/test/chromedriver"):
            with patch.object(checker, "DecimalChromeDriver") as constructor:
                checker.build_chrome_driver()
        kwargs = constructor.call_args.kwargs
        self.addCleanup(kwargs["profile"].cleanup)
        self.addCleanup(kwargs["service"].stop)
        self.assertEqual(kwargs["service"].path, "/test/chromedriver")
        self.assertIn("--log-level=WARNING", kwargs["service"].command_line_args())

    def test_no_hub_profile_uses_project_output_and_is_unique_per_session(self):
        with patch.dict(os.environ, {"CHROME_PROFILE_DIR": ""}):
            with patch.object(checker, "PROJECT_ROOT", self.root):
                with patch.object(checker, "DecimalChromeDriver") as constructor:
                    checker.build_chrome_driver()
                    checker.build_chrome_driver()
        profiles = []
        for call in constructor.call_args_list:
            self.addCleanup(call.kwargs["profile"].cleanup)
            self.addCleanup(call.kwargs["service"].stop)
            profiles.append(Path(call.kwargs["profile"].name))
        self.assertNotEqual(*profiles)
        self.assertEqual(profiles[0].parent, self.root / "runtime" / "output" / "chrome_profiles")

    def test_profile_cleanup_runs_even_if_quit_raises(self):
        with patch.object(checker.webdriver.Chrome, "__init__", return_value=None):
            profile = tempfile.TemporaryDirectory(dir=self.profile_root)
            driver = checker.DecimalChromeDriver(profile=profile)
        with patch.object(checker.webdriver.Chrome, "quit", side_effect=RuntimeError("quit failed")):
            with self.assertRaisesRegex(RuntimeError, "quit failed"):
                driver.quit()
        self.assertFalse(Path(profile.name).exists())
        self.assertTrue(self.sentinel.exists())

    def test_browser_check_requires_no_credentials_and_closes_browser(self):
        driver = MagicMock()
        driver.execute_script.return_value = "complete"
        with patch.object(sys, "argv", ["checker", "--check-browser"]):
            with patch.object(checker, "build_chrome_driver", return_value=driver):
                with patch.object(checker, "validate_config") as validate:
                    with patch.object(checker, "fetch_betfair_fixtures") as fetch:
                        with redirect_stdout(io.StringIO()) as output:
                            self.assertEqual(checker.main(), 0)
        validate.assert_not_called()
        fetch.assert_not_called()
        driver.get.assert_called_once_with("about:blank")
        driver.quit.assert_called_once()
        self.assertIn("Chrome startup check: OK", output.getvalue())

    def test_browser_check_failure_returns_error_and_closes_browser(self):
        driver = MagicMock()
        driver.get.side_effect = checker.WebDriverException("page failed")
        with patch.object(sys, "argv", ["checker", "--check-browser"]):
            with patch.object(checker, "build_chrome_driver", return_value=driver):
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    self.assertEqual(checker.main(), 2)
        driver.quit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
