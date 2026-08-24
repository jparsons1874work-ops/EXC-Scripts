from __future__ import annotations

import unittest
from copy import deepcopy
from unittest.mock import patch

from scripts import Golf_Exchange_NR_Checks as golf


class GolfFieldCheckerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.states = {}
        self.load_patch = patch.object(
            golf,
            "load_state",
            side_effect=lambda tour_id: deepcopy(self.states.get(tour_id, {})),
        )
        self.save_patch = patch.object(
            golf,
            "save_state",
            side_effect=lambda tour_id, state: self.states.__setitem__(tour_id, deepcopy(state)),
        )
        self.load_patch.start()
        self.save_patch.start()
        self.site = golf.SITE_DEFINITIONS["pgatour"]
        self.url = "https://www.pgatour.com/tournaments/2026/test/R2026001/field"

    def tearDown(self) -> None:
        self.save_patch.stop()
        self.load_patch.stop()

    def evaluate_and_commit(self, field, alternates=None):
        result = golf.evaluate_reading(
            "pgatour",
            self.site,
            self.url,
            list(field),
            list(alternates or []),
        )
        if result.proposed_state is not None:
            golf.save_state("pgatour", result.proposed_state)
        return result

    def test_first_read_is_a_silent_baseline(self) -> None:
        result = self.evaluate_and_commit(["Alice Player", "Bob Golfer"])

        self.assertFalse(result.slack_needed)
        self.assertEqual(golf.load_state("pgatour")["confirmed_field"], ["Alice Player", "Bob Golfer"])

    def test_change_requires_two_consecutive_reads(self) -> None:
        self.evaluate_and_commit(["Alice Player", "Bob Golfer"])

        first = self.evaluate_and_commit(["Alice Player", "Cara Golfer"])
        second = golf.evaluate_reading(
            "pgatour",
            self.site,
            self.url,
            ["Alice Player", "Cara Golfer"],
            [],
        )

        self.assertFalse(first.slack_needed)
        self.assertTrue(second.slack_needed)
        self.assertEqual(second.removed, ["Bob Golfer"])
        self.assertEqual(second.added, [{"name": "Cara Golfer", "promoted": False}])

    def test_reserve_list_reshuffle_does_not_alert(self) -> None:
        self.evaluate_and_commit(["Alice Player"], ["Reserve One", "Reserve Two"])

        result = self.evaluate_and_commit(["Alice Player"], ["Reserve Two", "Reserve Three"])

        self.assertFalse(result.slack_needed)
        self.assertEqual(golf.load_state("pgatour")["confirmed_field"], ["Alice Player"])

    def test_short_page_read_is_rejected(self) -> None:
        baseline = [f"Player {chr(65 + index)}" for index in range(12)]
        self.evaluate_and_commit(baseline)

        result = golf.evaluate_reading("pgatour", self.site, self.url, baseline[:4], [])

        self.assertFalse(result.slack_needed)
        self.assertIsNone(result.proposed_state)
        self.assertEqual(golf.load_state("pgatour")["confirmed_field"], baseline)
        self.assertEqual(golf.load_state("pgatour")["reject_streak"], 1)

    def test_new_tournament_url_resets_without_alerting(self) -> None:
        self.evaluate_and_commit(["Alice Player", "Bob Golfer"])
        self.states["pgatour"]["change_history"] = [
            {
                "timestamp": "2026-08-24T10:00:00Z",
                "change": "Addition",
                "player": "Old Tournament Player",
                "note": "",
            }
        ]
        new_url = "https://www.pgatour.com/tournaments/2026/new/R2026002/field"

        result = golf.evaluate_reading(
            "pgatour", self.site, new_url, ["Entirely New", "Different Field"], []
        )

        self.assertFalse(result.slack_needed)
        self.assertIn("new tournament configuration", result.status_lines[0])
        self.assertEqual(result.proposed_state["change_history"], [])

    def test_confirmed_changes_are_appended_to_history(self) -> None:
        self.evaluate_and_commit(["Alice Player", "Bob Golfer"], ["Cara Golfer"])
        self.evaluate_and_commit(["Alice Player", "Cara Golfer"])
        result = golf.evaluate_reading(
            "pgatour", self.site, self.url, ["Alice Player", "Cara Golfer"], []
        )

        with patch.object(golf, "utc_timestamp", return_value="2026-08-24T14:15:00Z"):
            golf.append_change_history(result)

        self.assertEqual(
            result.proposed_state["change_history"],
            [
                {
                    "timestamp": "2026-08-24T14:15:00Z",
                    "change": "Addition",
                    "player": "Cara Golfer",
                    "note": "Promoted from reserve list",
                },
                {
                    "timestamp": "2026-08-24T14:15:00Z",
                    "change": "Withdrawal",
                    "player": "Bob Golfer",
                    "note": "",
                },
            ],
        )

    def test_uncommitted_alert_is_retried_after_slack_failure(self) -> None:
        self.evaluate_and_commit(["Alice Player", "Bob Golfer"])
        self.evaluate_and_commit(["Alice Player"])

        failed_delivery = golf.evaluate_reading(
            "pgatour", self.site, self.url, ["Alice Player"], []
        )
        retried = golf.evaluate_reading("pgatour", self.site, self.url, ["Alice Player"], [])

        self.assertTrue(failed_delivery.slack_needed)
        self.assertTrue(retried.slack_needed)
        self.assertIn("Bob Golfer", golf.load_state("pgatour")["confirmed_field"])

    def test_only_official_https_urls_are_accepted(self) -> None:
        self.assertIsNone(golf.validate_site_url("pgatour", self.url))
        self.assertIsNotNone(golf.validate_site_url("pgatour", "http://www.pgatour.com/test"))
        self.assertIsNotNone(golf.validate_site_url("pgatour", "https://example.com/test"))
        self.assertIsNotNone(
            golf.validate_site_url("pgatour", "https://www.pgatour.com/tournaments/2026/EXAMPLE/field")
        )

    def test_page_reader_splits_field_and_reserve_in_document_order(self) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.skipTest("Playwright is not installed in this test environment")

        site = dict(self.site)
        site["min_field_before_boundary_check"] = 1
        markup = """
        <body>
          <main>
            <a href="/player/1">Alice Player</a>
            <h2>Alternates</h2>
            <a href="/player/2">Reserve Person</a>
          </main>
        </body>
        """
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.set_content(markup)
                reading = golf.read_field(page, site, timeout_s=1)
            finally:
                browser.close()

        self.assertEqual(reading["field"], ["Alice Player"])
        self.assertEqual(reading["alternates"], ["Reserve Person"])

    def test_dp_reader_uses_entry_table_and_ignores_player_attachments(self) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.skipTest("Playwright is not installed in this test environment")

        site = dict(golf.SITE_DEFINITIONS["dpworld"])
        site["min_field_before_boundary_check"] = 1
        markup = """
        <body>
          <a href="/players/unrelated-1/">Unrelated Ranking Player</a>
          <main><table><tbody>
            <tr><td><a href="/players/alice-1/"><strong>PLAYER</strong>, Alice<div>Home Golf Club</div></a></td></tr>
            <tr><td>Current Cut Off Position - subject to change</td></tr>
            <tr><td><a href="/players/reserve-2/"><strong>PERSON</strong>, Reserve<div>Other Club</div></a></td></tr>
          </tbody></table></main>
        </body>
        """
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.set_content(markup)
                reading = golf.read_field(page, site, timeout_s=1)
            finally:
                browser.close()

        self.assertEqual(reading["field"], ["Player, Alice"])
        self.assertEqual(reading["alternates"], ["Person, Reserve"])

    def test_scanner_status_messages_include_enabled_urls(self) -> None:
        sites = [
            (
                "pgatour",
                self.site,
                self.url,
                "2026-08-24T12:00:00Z",
            )
        ]

        active = golf.scanner_status_message(True, sites)
        offline = golf.scanner_status_message(False, sites)

        self.assertIn("scanner active", active)
        self.assertIn(self.url, active)
        self.assertIn("scanner offline", offline)
        self.assertIn(self.url, offline)

    def test_scanner_status_uses_golf_slack_destination(self) -> None:
        sites = [("pgatour", self.site, self.url, "")]
        with patch.object(golf, "send_slack_text", return_value=None) as send:
            sent = golf.announce_scanner_status(True, {"sites": []}, sites)

        self.assertTrue(sent)
        self.assertIn(self.url, send.call_args.args[1])
        self.assertEqual(send.call_args.kwargs["timeout"], 5)


if __name__ == "__main__":
    unittest.main()
