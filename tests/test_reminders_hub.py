import json
import unittest
import uuid
from datetime import datetime
from pathlib import Path

from app.reminders import UK_TZ, daily_reminders_context


class ReminderHubTests(unittest.TestCase):
    def test_daily_table_shows_only_active_target_channel_reminders_for_today(self) -> None:
        now = datetime(2026, 8, 1, 12, 0, tzinfo=UK_TZ)
        today_epoch = int(datetime(2026, 8, 1, 14, 30, tzinfo=UK_TZ).timestamp())
        tomorrow_epoch = int(datetime(2026, 8, 2, 10, 0, tzinfo=UK_TZ).timestamp())
        data = {
            "scheduled": [
                {
                    "duplicate_key": f"Boxing|event-1|{today_epoch}|C07FXG95GQ6",
                    "sport": "Boxing",
                    "slack_text": ":boxing_glove: Fight starting in 30 mins",
                    "scheduled_slack_post_epoch": today_epoch,
                },
                {
                    "duplicate_key": f"Boxing|event-1|{today_epoch - 3600}|C07FXG95GQ6",
                    "sport": "Boxing",
                    "slack_text": "Outdated reminder",
                    "scheduled_slack_post_epoch": today_epoch - 3600,
                    "status": "superseded",
                },
                {
                    "duplicate_key": f"Darts|event-2|{tomorrow_epoch}|C07FXG95GQ6",
                    "sport": "Darts",
                    "slack_text": "Tomorrow",
                    "scheduled_slack_post_epoch": tomorrow_epoch,
                },
                {
                    "duplicate_key": f"Rugby Union|event-3|{today_epoch}|C_OLD_CHANNEL",
                    "sport": "Rugby Union",
                    "slack_text": "Wrong channel",
                    "scheduled_slack_post_epoch": today_epoch,
                },
            ]
        }
        state_path = Path(__file__).resolve().parents[1] / "runtime" / "output" / f"reminder-hub-{uuid.uuid4()}.json"
        try:
            state_path.write_text(json.dumps(data), encoding="utf-8")
            context = daily_reminders_context(now, state_path)
        finally:
            state_path.unlink(missing_ok=True)

        self.assertEqual(context["date_label"], "Saturday 1 August 2026")
        self.assertEqual(
            context["rows"],
            [
                {
                    "time": "14:30 BST",
                    "sport": "Boxing",
                    "message": ":boxing_glove: Fight starting in 30 mins",
                    "sort_key": "2026-08-01T14:30:00+01:00",
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
