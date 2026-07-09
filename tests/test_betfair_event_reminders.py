from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
import sys
import uuid
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import Betfair_Event_Reminders as reminders  # noqa: E402
from Betfair_Event_Reminders import (  # noqa: E402
    ConfigSource,
    EventReminder,
    UK_TZ,
    build_scan_window,
    dedupe_events,
    duplicate_key,
    load_config,
    missing_config_message,
    reminder_time,
    resolve_config_path,
    resolve_config_source,
    select_reminders,
    slack_bucket_warnings,
    ConfigMissing,
    ConfigPlaceholderError,
    validate_config,
)


def reminder(
    sport: str,
    event_id: str,
    start_utc: datetime,
    *,
    competition_id: str = "",
    competition_name: str = "",
    market_id: str = "",
) -> EventReminder:
    return EventReminder(
        sport=sport,
        emoji=":test:",
        event_type_id="1",
        event_id=event_id,
        event_name=f"Event {event_id}",
        competition_id=competition_id,
        competition_name=competition_name,
        market_id=market_id or f"market-{event_id}",
        market_name="Match Odds",
        event_start_utc=start_utc,
    )


class BetfairEventReminderTests(unittest.TestCase):
    def test_0700_uk_scan_window(self) -> None:
        now = datetime(2026, 7, 9, 12, 0, tzinfo=UK_TZ)
        window = build_scan_window(now_uk=now)
        self.assertEqual(window.start_uk.strftime("%Y-%m-%d %H:%M %Z"), "2026-07-09 07:00 BST")
        self.assertEqual(window.end_uk.strftime("%Y-%m-%d %H:%M %Z"), "2026-07-10 07:00 BST")

    def test_uk_scan_window_converts_to_utc(self) -> None:
        now = datetime(2026, 7, 9, 12, 0, tzinfo=UK_TZ)
        window = build_scan_window(now_uk=now)
        self.assertEqual(window.start_utc, datetime(2026, 7, 9, 6, 0, tzinfo=timezone.utc))
        self.assertEqual(window.end_utc, datetime(2026, 7, 10, 6, 0, tzinfo=timezone.utc))

    def test_event_start_minus_five_minutes(self) -> None:
        event_start = datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)
        self.assertEqual(reminder_time(event_start).strftime("%H:%M %Z"), "14:55 BST")

    def test_event_dedupe_keeps_first_market_per_event(self) -> None:
        first = reminder("Rugby Union", "event-1", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc), market_id="m1")
        second = reminder("Rugby Union", "event-1", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc), market_id="m2")
        unique = dedupe_events([second, first])
        self.assertEqual(len(unique), 1)
        self.assertEqual(unique[0].market_id, "m1")

    def test_first_event_per_sport_logic(self) -> None:
        later = reminder("Boxing", "later", datetime(2026, 7, 9, 20, 0, tzinfo=timezone.utc))
        earlier = reminder("Boxing", "earlier", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc))
        selected = select_reminders([later, earlier], "first")
        self.assertEqual([item.event_id for item in selected], ["earlier"])

    def test_darts_first_event_per_competition_logic(self) -> None:
        events = [
            reminder("Darts", "a-late", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc), competition_id="a"),
            reminder("Darts", "a-early", datetime(2026, 7, 9, 16, 0, tzinfo=timezone.utc), competition_id="a"),
            reminder("Darts", "b-only", datetime(2026, 7, 9, 17, 0, tzinfo=timezone.utc), competition_id="b"),
        ]
        selected = select_reminders(events, "darts_first_per_competition")
        self.assertEqual([item.event_id for item in selected], ["a-early", "b-only"])

    def test_duplicate_key_generation(self) -> None:
        item = reminder("Snooker", "123", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc))
        self.assertEqual(duplicate_key(item, 1783605300, "C123"), "Snooker|123|1783605300|C123")

    def test_slack_30_per_5_minute_bucket_warning(self) -> None:
        warnings = slack_bucket_warnings([1783605300] * 31)
        self.assertEqual(len(warnings), 1)
        self.assertIn("31/30", warnings[0])

    def test_config_resolution_order(self) -> None:
        with patch.dict("os.environ", {"BETFAIR_EVENT_REMINDERS_CONFIG": "/tmp/ec2-config.json"}):
            self.assertEqual(resolve_config_path("C:/explicit/config.json"), Path("C:/explicit/config.json"))
            self.assertEqual(resolve_config_path(), Path("/tmp/ec2-config.json"))

    def test_missing_config_does_not_create_real_placeholder(self) -> None:
        missing_path = ROOT / "runtime" / "output" / f"missing-{uuid.uuid4()}.json"
        with self.assertRaises(ConfigMissing) as context:
            load_config(ConfigSource("json", missing_path))
        self.assertFalse(missing_path.exists())
        self.assertIn("Do not commit real config or .env files to Git.", str(context.exception))
        self.assertEqual(str(context.exception), missing_config_message(ConfigSource("json", missing_path)))

    def test_loads_from_env_file(self) -> None:
        env_path = ROOT / "runtime" / "output" / f"reminders-{uuid.uuid4()}.env"
        env_path.parent.mkdir(parents=True, exist_ok=True)
        env_path.write_text(
            "\n".join(
                [
                    "BETFAIR_EVENT_REMINDERS_SLACK_BOT_TOKEN=SAFE_TEST_SLACK_BOT_TOKEN",
                    "BETFAIR_EVENT_REMINDERS_SLACK_CHANNEL_ID=C123456789",
                    "BETFAIR_EVENT_REMINDERS_SLACK_CHANNEL_NAME=#exc_sports_ops",
                    "BETFAIR_EVENT_REMINDERS_FALLBACK_WEBHOOK_URL=",
                    "BETFAIR_APP_KEY=test-app-key",
                    "BETFAIR_USERNAME=test-user",
                    "BETFAIR_PASSWORD='test password'",
                    "BETFAIR_CERTS_DIR=/opt/betfair-scripts/certs",
                ]
            ),
            encoding="utf-8",
        )
        config = load_config(ConfigSource("env", env_path))
        self.assertEqual(config.slack_channel_id, "C123456789")
        self.assertEqual(config.slack_channel_name, "#exc_sports_ops")
        self.assertEqual(config.betfair_password, "test password")

    def test_env_source_is_used_when_json_is_missing(self) -> None:
        env_path = ROOT / "runtime" / "output" / f"reminders-{uuid.uuid4()}.env"
        json_path = ROOT / "runtime" / "output" / f"missing-{uuid.uuid4()}.json"
        env_path.parent.mkdir(parents=True, exist_ok=True)
        env_path.write_text(
            "BETFAIR_EVENT_REMINDERS_SLACK_BOT_TOKEN=SAFE_TEST_SLACK_BOT_TOKEN\n"
            "BETFAIR_EVENT_REMINDERS_SLACK_CHANNEL_ID=C123456789\n"
            "BETFAIR_APP_KEY=test-app-key\n"
            "BETFAIR_USERNAME=test-user\n"
            "BETFAIR_PASSWORD=test-password\n",
            encoding="utf-8",
        )
        with patch.dict("os.environ", {}, clear=True):
            with patch.object(reminders, "EC2_ENV_PATH", env_path), patch.object(reminders, "WINDOWS_ENV_PATH", json_path):
                source = resolve_config_source()
        self.assertEqual(source, ConfigSource("env", env_path))
        config = load_config(source)
        validate_config(config, source)

    def test_required_field_validation_from_env(self) -> None:
        config = reminders.env_to_config({"BETFAIR_EVENT_REMINDERS_SLACK_BOT_TOKEN": "SAFE_TEST_SLACK_BOT_TOKEN"})
        with self.assertRaises(ConfigPlaceholderError) as context:
            validate_config(config, ConfigSource("env", Path("/opt/betfair-scripts/.env")))
        message = str(context.exception)
        self.assertIn("slack_channel_id", message)
        self.assertIn("betfair_app_key", message)
        self.assertIn("betfair_username", message)
        self.assertIn("betfair_password", message)

    def test_placeholder_detection_from_env(self) -> None:
        config = reminders.env_to_config(
            {
                "BETFAIR_EVENT_REMINDERS_SLACK_BOT_TOKEN": "xoxb-NEW-CHANNEL-BOT-TOKEN-PLACEHOLDER",
                "BETFAIR_EVENT_REMINDERS_SLACK_CHANNEL_ID": "C123456789",
                "BETFAIR_APP_KEY": "test-app-key",
                "BETFAIR_USERNAME": "test-user",
                "BETFAIR_PASSWORD": "test-password",
            }
        )
        with self.assertRaises(ConfigPlaceholderError) as context:
            validate_config(config, ConfigSource("env", Path("/opt/betfair-scripts/.env")))
        self.assertIn("slack_bot_token", str(context.exception))

    def test_json_config_path_still_works(self) -> None:
        json_path = ROOT / "runtime" / "output" / f"reminders-{uuid.uuid4()}.json"
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(
            """{
  "slack_bot_token": "SAFE_TEST_SLACK_BOT_TOKEN",
  "slack_channel_id": "C123456789",
  "slack_channel_name": "#exc_sports_ops",
  "fallback_webhook_url": "",
  "betfair_app_key": "test-app-key",
  "betfair_username": "test-user",
  "betfair_password": "test-password",
  "certs_dir": "/opt/betfair-scripts/certs"
}
""",
            encoding="utf-8",
        )
        source = ConfigSource("json", json_path)
        config = load_config(source)
        validate_config(config, source)
        self.assertEqual(config.slack_channel_id, "C123456789")


if __name__ == "__main__":
    unittest.main()
