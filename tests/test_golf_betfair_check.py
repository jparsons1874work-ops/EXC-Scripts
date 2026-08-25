from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app import golf_betfair_check as checker


class GolfBetfairCheckTests(unittest.TestCase):
    def test_competition_hint_handles_all_supported_url_shapes(self) -> None:
        self.assertEqual(
            checker.competition_hint(
                "https://www.pgatour.com/tournaments/2026/tour-championship/R2026060/field"
            ),
            "tour championship",
        )
        self.assertEqual(
            checker.competition_hint(
                "https://www.europeantour.com/dpworld-tour/betfred-british-masters-2026/entry-list"
            ),
            "betfred british masters 2026",
        )
        self.assertEqual(
            checker.competition_hint("https://www.lpga.com/tournaments/cpkc-womens-open/entries"),
            "cpkc womens open",
        )

    def test_event_match_handles_sponsor_and_tour_wording(self) -> None:
        score = checker.event_match_score(
            "betfred british masters hosted by sir nick faldo 2026",
            "British Masters",
        )
        self.assertGreaterEqual(score, checker.EVENT_MATCH_THRESHOLD)

        events = [
            checker.BetfairEvent("1", "Tour Championship", "1.1", "Winner", []),
            checker.BetfairEvent("2", "BMW Championship", "1.2", "Winner", []),
        ]
        event, _ = checker.best_event_match("bmw championship", events, set())
        self.assertEqual(event.event_id, "2")

    def test_event_match_allows_different_dp_world_title_sponsors(self) -> None:
        score = checker.event_match_score(
            "husqvarna british masters hosted by sir nick faldo 2026",
            "Betfred British Masters 2026",
        )

        self.assertGreaterEqual(score, checker.EVENT_MATCH_THRESHOLD)

    def test_ambiguous_event_match_is_rejected(self) -> None:
        events = [
            checker.BetfairEvent("1", "Example Open North", "1.1", "Winner", []),
            checker.BetfairEvent("2", "Example Open South", "1.2", "Winner", []),
        ]

        event, score = checker.best_event_match("example open", events, set())

        self.assertIsNone(event)
        self.assertGreater(score, checker.EVENT_MATCH_THRESHOLD)

    def test_player_comparison_reports_both_sides_clearly(self) -> None:
        with patch.object(checker, "load_name_aliases", return_value={}):
            result = checker.compare_player_lists(
                ["Scottie Scheffler", "Rory McIlroy"],
                ["Scheffler, Scottie", "Tommy Fleetwood"],
            )

        self.assertFalse(result["matching"])
        self.assertEqual(result["official_only"], ["Rory McIlroy"])
        self.assertEqual(result["betfair_only"], ["Tommy Fleetwood"])

    def test_player_comparison_matches_non_decomposing_name_accents(self) -> None:
        with patch.object(checker, "load_name_aliases", return_value={}):
            result = checker.compare_player_lists(
                ["Nørgaard, Niklas"],
                ["Niklas Norgaard"],
            )

        self.assertTrue(result["matching"])
        self.assertEqual(result["official_only"], [])
        self.assertEqual(result["betfair_only"], [])

    def test_lpga_lee_suffix_matches_betfair_name(self) -> None:
        with patch.object(checker, "load_name_aliases", return_value={}):
            result = checker.compare_player_lists(
                ["Jeongeun Lee5"],
                ["JeongEun Lee"],
            )

        self.assertTrue(result["matching"])

    def test_betfair_count_keeps_ignored_market_options_visible(self) -> None:
        with patch.object(checker, "load_name_aliases", return_value={}):
            result = checker.compare_player_lists(
                ["Alice Player"],
                ["Alice Player", "Any Other Player", "Field", "The Field"],
            )

        self.assertTrue(result["matching"])
        self.assertEqual(result["betfair_count"], 4)
        self.assertEqual(result["betfair_compared_count"], 1)
        self.assertEqual(len(result["ignored_betfair"]), 3)

    def test_catalogue_selection_prefers_main_winner_market(self) -> None:
        event = SimpleNamespace(id="event-1", name="Test Open")
        top_five = SimpleNamespace(
            event=event,
            market_id="1.top5",
            market_name="Top 5 Finish",
            runners=[SimpleNamespace(selection_id=1, runner_name="Alice Player")],
        )
        winner = SimpleNamespace(
            event=event,
            market_id="1.winner",
            market_name="Winner",
            runners=[
                SimpleNamespace(selection_id=1, runner_name="Alice Player"),
                SimpleNamespace(selection_id=2, runner_name="Bob Golfer"),
            ],
        )

        events = checker.catalogue_events([top_five, winner])

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].market_id, "1.winner")

    def test_all_catalogue_selections_are_compared(self) -> None:
        event = checker.BetfairEvent(
            "event-1",
            "Test Open",
            "1.123",
            "Winner",
            [
                SimpleNamespace(selection_id=1, runner_name="Alice Player"),
                SimpleNamespace(selection_id=2, runner_name="Withdrawn Golfer"),
            ],
        )
        names = checker.betfair_selection_names(event)

        self.assertEqual(names, ["Alice Player", "Withdrawn Golfer"])

    def test_perform_check_sends_fresh_slack_for_mismatch(self) -> None:
        official = [
            {
                "site_id": "pgatour",
                "competition": "PGA Tour",
                "url": "https://www.pgatour.com/tournaments/2026/test-open/R2026001/field",
                "hint": "test open",
                "official_names": ["Alice Player", "Bob Golfer"],
                "ready": True,
            }
        ]
        event = checker.BetfairEvent(
            "event-1",
            "Test Open",
            "1.123",
            "Winner",
            [SimpleNamespace(selection_id=1, runner_name="Alice Player")],
        )
        client = SimpleNamespace(logout=Mock())

        with (
            patch.object(checker, "enabled_official_fields", return_value=official),
            patch.object(checker, "child_environment", return_value={}),
            patch.object(checker, "betfair_login", return_value=client),
            patch.object(checker, "list_betfair_events", return_value=[event]),
            patch.object(checker, "betfair_selection_names", return_value=["Alice Player"]),
            patch.object(checker, "load_name_aliases", return_value={}),
            patch.object(checker, "send_discrepancy_slack", return_value="sent") as slack,
        ):
            result = checker.perform_check()

        self.assertEqual(result["summary"], "mismatch")
        self.assertEqual(result["rows"][0]["official_only"], ["Bob Golfer"])
        self.assertEqual(result["slack_status"], "sent")
        slack.assert_called_once()
        client.logout.assert_called_once()

    def test_service_rejects_duplicate_parallel_start(self) -> None:
        service = checker.GolfBetfairCheckService()
        fake_thread = Mock()
        with (
            patch.object(checker, "_write_result"),
            patch.object(checker.threading, "Thread", return_value=fake_thread),
        ):
            first = service.start()
            second = service.start()

        self.assertTrue(first)
        self.assertFalse(second)
        fake_thread.start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
