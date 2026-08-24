from __future__ import annotations

import unittest
from unittest.mock import patch

from starlette.requests import Request

import app.main as hub
from app.registry import SCRIPTS_BY_ID


PGA_URL = "https://www.pgatour.com/tournaments/2026/test/R2026001/field"


class GolfHubTests(unittest.TestCase):
    def golf_config(self):
        return {
            "sites": [
                {
                    "id": "pgatour",
                    "enabled": True,
                    "url": PGA_URL,
                    "url_saved_at": "2026-08-24T12:00:00Z",
                }
            ],
            "last_saved_at": "2026-08-24T12:00:00Z",
        }

    def golf_state(self, site_id):
        if site_id != "pgatour":
            return {}
        return {
            "baseline_url": PGA_URL,
            "config_saved_at": "2026-08-24T12:00:00Z",
            "confirmed_field": ["Alice Player", "Bob Golfer"],
            "confirmed_alternates": ["Reserve Person"],
            "last_checked_at": "2026-08-24T13:30:00Z",
            "change_history": [
                {
                    "timestamp": "2026-08-24T13:15:00Z",
                    "change": "Withdrawal",
                    "player": "Cara Golfer",
                    "note": "",
                }
            ],
        }

    def test_context_lists_competitions_counts_and_changes(self) -> None:
        with (
            patch.object(hub, "_read_golf_config", return_value=self.golf_config()),
            patch.object(hub, "_read_golf_state", side_effect=self.golf_state),
        ):
            context = hub._golf_context()

        pga = context["sites"][0]
        self.assertEqual(pga["field_count"], 2)
        self.assertEqual(pga["reserve_count"], 1)
        self.assertEqual(pga["tracking_since"], "24 Aug 2026 13:00:00")
        self.assertEqual(pga["last_checked"], "24 Aug 2026 14:30:00")
        self.assertEqual(context["changes"][0]["player"], "Cara Golfer")
        self.assertEqual(context["changes"][0]["timestamp"], "24 Aug 2026 14:15:00")

    def test_old_tournament_history_is_hidden_after_url_change(self) -> None:
        state = self.golf_state("pgatour")
        state["baseline_url"] = "https://www.pgatour.com/tournaments/2026/old/R2026000/field"
        with (
            patch.object(hub, "_read_golf_config", return_value=self.golf_config()),
            patch.object(hub, "_read_golf_state", return_value=state),
        ):
            context = hub._golf_context()

        self.assertEqual(context["changes"], [])
        self.assertIsNone(context["sites"][0]["field_count"])

    def test_golf_page_renders_both_tables(self) -> None:
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/scripts/golf-non-runner-check",
                "root_path": "",
                "scheme": "http",
                "server": ("test", 80),
                "headers": [],
                "query_string": b"",
                "app": hub.app,
                "router": hub.app.router,
            }
        )
        spec = SCRIPTS_BY_ID["golf-non-runner-check"]
        with (
            patch.object(hub, "_read_golf_config", return_value=self.golf_config()),
            patch.object(hub, "_read_golf_state", side_effect=self.golf_state),
        ):
            golf_context = hub._golf_context()
        context = hub.template_context(
            request,
            spec=spec,
            state=hub.runner.get_state(spec.id),
            allowed=True,
            window_label="07:00-23:00 Europe/London",
            cricket=None,
            inplay=None,
            parsed_output_message="",
            golf=golf_context,
            golf_betfair={
                "status": "complete",
                "summary": "mismatch",
                "rows": [
                    {
                        "competition": "PGA Tour",
                        "status": "mismatch",
                        "official_count": 2,
                        "betfair_count": 1,
                        "betfair_event_name": "Test Championship",
                        "betfair_market_id": "1.234",
                        "official_only": ["Cara Golfer"],
                        "betfair_only": [],
                        "message": "Field discrepancy found.",
                    }
                ],
                "mismatch_count": 1,
                "slack_status": "sent",
                "started_at_label": "24 Aug 2026 14:00:00",
                "completed_at_label": "24 Aug 2026 14:01:00",
            },
            ufc=None,
            pfl=None,
            reminders=None,
        )

        html = hub.templates.get_template("script_detail.html").render(context)

        self.assertIn("Tracked Competitions", html)
        self.assertIn("Confirmed Field Changes", html)
        self.assertIn("Check with Betfair", html)
        self.assertIn("Betfair Exchange Field Comparison", html)
        self.assertIn("Cara Golfer", html)


if __name__ == "__main__":
    unittest.main()
