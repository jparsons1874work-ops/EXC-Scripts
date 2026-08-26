from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from starlette.requests import Request

import app.main as hub
from app.registry import SCRIPTS_BY_ID
from app.tennis_challenger import (
    BetfairTennisEvent,
    is_game_market,
    match_betfair_event,
    normalize_tournament_url,
    parse_tournament_links,
    participant_match_score,
    perform_game_betting_check,
)
from scripts.Tennis_Challenger_Watcher import (
    ALERT_FLAG_BY_TYPE,
    extract_feed_config,
    fetch_tournament_feed,
    hydrate_initial_alert_flags,
    load_tournament_rows,
    normalize_scraped_match,
    parse_tournament_feed,
    pending_alerts,
    slack_message,
    slack_webhook_url,
)


AUGSBURG_URL = "https://www.flashscore.com/tennis/challenger-men-singles/augsburg/"
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def raw_match(**updates):
    data = {
        "id": "abc123",
        "url": "https://www.flashscore.com/match/tennis/player-a/player-b/?mid=abc123",
        "player1": "Kopp S.",
        "player2": "Krumich M.",
        "raw_status": "27.08. 09:00",
        "row_classes": "event__match event__match--scheduled",
        "set_score": {"home": "-", "away": "-"},
        "set_parts": [],
        "current_points": {"home": "", "away": ""},
        "server_side": "",
        "is_live": False,
        "is_scheduled": True,
    }
    data.update(updates)
    return data


def apply_sent(match, alert_types):
    for alert_type in alert_types:
        match["alerts_sent"][ALERT_FLAG_BY_TYPE[alert_type]] = True


class TennisChallengerTests(unittest.TestCase):
    def test_watcher_can_start_from_its_script_path(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "scripts" / "Tennis_Challenger_Watcher.py"), "--help"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("Monitor configured Flashscore ATP Challenger tournaments", completed.stdout)

    def test_tournament_loader_waits_for_attached_not_visible_rows(self) -> None:
        class FakePage:
            wait_options = None
            url = AUGSBURG_URL

            def goto(self, *_args, **_kwargs):
                return None

            def wait_for_selector(self, _selector, **options):
                self.wait_options = options

            def eval_on_selector_all(self, _selector, _extractor):
                return [raw_match()]

            def locator(self, _selector):
                return SimpleNamespace(count=lambda: 1)

        page = FakePage()
        rows = load_tournament_rows(page, AUGSBURG_URL)
        self.assertEqual(len(rows), 1)
        self.assertEqual(page.wait_options["state"], "attached")

    def test_flashscore_feed_config_is_read_from_tournament_html(self) -> None:
        html = (
            'sport_id":2; country_id = 5729;tournament_id = "SUQ1VjQ1"; '
            '"feed_sign":"SW9D1eZo"'
        )
        config = extract_feed_config(html, AUGSBURG_URL)
        self.assertEqual(config["tournament_id"], "SUQ1VjQ1")
        self.assertEqual(config["country_id"], "5729")
        self.assertEqual(config["feed_sign"], "SW9D1eZo")
        self.assertIn("/2/x/feed/t_2_5729_SUQ1VjQ1_", config["feed_url"])

    def test_flashscore_feed_parser_reads_status_scores_and_server(self) -> None:
        payload = (
            "SA÷2¬~"
            "AA÷live123¬AD÷1787757600¬AB÷2¬AC÷47¬AE÷Basing M.¬AF÷Deckers A."
            "¬AG÷0¬AH÷0¬BA÷6¬BB÷6¬WA÷5¬WB÷3¬AI÷y¬~"
            "AA÷scheduled456¬AD÷1787821200¬AB÷1¬AE÷Monday J.¬AF÷Ribecai M.¬~"
            "AA÷finished789¬AD÷1787739000¬AB÷3¬AE÷Ivanov I.¬AF÷Loffhagen G."
            "¬AG÷0¬AH÷2¬BA÷3¬BB÷6¬BC÷0¬BD÷6¬~"
        )
        rows = parse_tournament_feed(payload, AUGSBURG_URL)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["raw_status"], "Set 1")
        self.assertTrue(rows[0]["is_live"])
        self.assertEqual(rows[0]["server_side"], "home")
        self.assertEqual(rows[0]["set_parts"][0], {"home": "6", "away": "6"})
        self.assertTrue(rows[1]["is_scheduled"])
        self.assertEqual(rows[2]["raw_status"], "Finished")
        self.assertEqual(rows[2]["set_score"], {"home": 0, "away": 2})

    def test_flashscore_feed_request_has_a_unique_cache_buster(self) -> None:
        payload = (
            "AA÷live123¬AD÷1787757600¬AB÷2¬AC÷47¬AE÷Basing M.¬AF÷Deckers A."
            "¬AG÷0¬AH÷0¬WA÷15¬WB÷30¬AI÷y¬~"
        )

        class FakeResponse:
            status_code = 200
            text = payload
            headers = {"Age": "0"}

            @staticmethod
            def raise_for_status() -> None:
                return None

        class FakeSession:
            call = None

            def get(self, url, **kwargs):
                self.call = (url, kwargs)
                return FakeResponse()

        session = FakeSession()
        rows = fetch_tournament_feed(
            session,
            {
                "feed_url": "https://global.flashscore.ninja/2/x/feed/test",
                "feed_sign": "feed-sign",
                "source_url": AUGSBURG_URL,
            },
            "Augsburg (Singles)",
        )

        self.assertEqual(len(rows), 1)
        self.assertIsInstance(session.call[1]["params"]["_"], int)
        self.assertEqual(session.call[1]["headers"]["Cache-Control"], "no-cache")

    def test_registry_contains_manual_challenger_watcher(self) -> None:
        spec = SCRIPTS_BY_ID["tennis-challenger-watcher"]
        self.assertTrue(spec.long_running)
        self.assertFalse(spec.auto_start_on_hub_start)
        self.assertEqual(spec.default_args[:2], ("--poll-seconds", "3"))

    def test_tournament_links_are_normalized_to_summary_page(self) -> None:
        self.assertEqual(
            normalize_tournament_url(AUGSBURG_URL + "fixtures/"),
            AUGSBURG_URL,
        )
        self.assertEqual(
            parse_tournament_links(AUGSBURG_URL + "\n" + AUGSBURG_URL + "draw"),
            [AUGSBURG_URL],
        )
        with self.assertRaises(ValueError):
            normalize_tournament_url("https://www.flashscore.com/tennis/atp-singles/us-open/")

    def test_pre_match_serve_marker_triggers_first_alert(self) -> None:
        match = normalize_scraped_match(
            raw_match(server_side="home"),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        match["alerts_sent"] = hydrate_initial_alert_flags(match)
        alerts = pending_alerts(None, match)
        self.assertEqual(alerts, ["serve_detected"])
        self.assertEqual(match["server"], "Kopp S.")
        self.assertIn("toss decided", slack_message("serve_detected", match))

    def test_slack_alert_includes_the_betfair_event_id(self) -> None:
        match = normalize_scraped_match(raw_match(), AUGSBURG_URL, "Augsburg (Singles)")
        match["betfair_event_id"] = "1.234567890"
        self.assertIn("*Betfair event ID:* `1.234567890`", slack_message("match_started", match))

    def test_dedicated_challenger_webhook_is_used(self) -> None:
        with patch.dict(
            os.environ,
            {
                "Webhook_Challenger": "https://hooks.slack.test/challenger",
                "SLACK_WEBHOOK_URL": "https://hooks.slack.test/shared",
            },
            clear=False,
        ):
            self.assertEqual(slack_webhook_url(), "https://hooks.slack.test/challenger")

    def test_match_transitions_alert_in_operational_order(self) -> None:
        scheduled = normalize_scraped_match(raw_match(), AUGSBURG_URL, "Augsburg (Singles)")
        scheduled["alerts_sent"] = hydrate_initial_alert_flags(scheduled)

        live_set_1 = normalize_scraped_match(
            raw_match(
                raw_status="Set 1",
                row_classes="event__match event__match--live",
                is_live=True,
                is_scheduled=False,
                set_score={"home": "0", "away": "0"},
                set_parts=[{"home": "2", "away": "1"}],
                server_side="away",
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        live_set_1["alerts_sent"] = dict(scheduled["alerts_sent"])
        alerts = pending_alerts(scheduled, live_set_1)
        self.assertEqual(alerts, ["match_started"])
        apply_sent(live_set_1, alerts)

        live_set_2 = normalize_scraped_match(
            raw_match(
                raw_status="Set 2",
                row_classes="event__match event__match--live",
                is_live=True,
                is_scheduled=False,
                set_score={"home": "1", "away": "0"},
                set_parts=[{"home": "6", "away": "4"}, {"home": "0", "away": "0"}],
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        live_set_2["alerts_sent"] = dict(live_set_1["alerts_sent"])
        alerts = pending_alerts(live_set_1, live_set_2)
        self.assertEqual(alerts, ["set_1_complete"])
        apply_sent(live_set_2, alerts)

        live_set_3 = normalize_scraped_match(
            raw_match(
                raw_status="Set 3",
                row_classes="event__match event__match--live",
                is_live=True,
                is_scheduled=False,
                set_score={"home": "1", "away": "1"},
                set_parts=[{"home": "6", "away": "4"}, {"home": "3", "away": "6"}, {"home": "0", "away": "0"}],
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        live_set_3["alerts_sent"] = dict(live_set_2["alerts_sent"])
        alerts = pending_alerts(live_set_2, live_set_3)
        self.assertEqual(alerts, ["set_2_complete"])
        apply_sent(live_set_3, alerts)

        finished = normalize_scraped_match(
            raw_match(
                raw_status="Finished",
                row_classes="event__match",
                is_live=False,
                is_scheduled=False,
                set_score={"home": "2", "away": "1"},
                set_parts=[{"home": "6", "away": "4"}, {"home": "3", "away": "6"}, {"home": "6", "away": "2"}],
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        finished["alerts_sent"] = dict(live_set_3["alerts_sent"])
        self.assertEqual(pending_alerts(live_set_3, finished), ["match_complete"])

    def test_historical_finished_match_does_not_alert_when_first_discovered(self) -> None:
        finished = normalize_scraped_match(
            raw_match(
                raw_status="Finished",
                is_scheduled=False,
                set_score={"home": "2", "away": "0"},
                set_parts=[{"home": "6", "away": "2"}, {"home": "6", "away": "3"}],
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        finished["alerts_sent"] = hydrate_initial_alert_flags(finished)
        self.assertEqual(pending_alerts(None, finished), [])
        self.assertTrue(finished["alerts_sent"]["match_complete"])

    def test_betfair_event_matching_handles_flashscore_initials(self) -> None:
        match = {"player1": "Kopp S.", "player2": "Krumich M."}
        events = [
            BetfairTennisEvent("1", "S Kopp v M Krumich", None),
            BetfairTennisEvent("2", "C Alcaraz v J Sinner", None),
        ]
        event, score, reason = match_betfair_event(match, events)
        self.assertIsNotNone(event)
        self.assertEqual(event.event_id, "1")
        self.assertGreaterEqual(score, 95)
        self.assertEqual(reason, "Matched")
        self.assertGreaterEqual(participant_match_score("Brady C. / Howse M.", "C Brady & M Howse"), 95)

    def test_game_market_detection_uses_market_type_and_name(self) -> None:
        self.assertTrue(is_game_market(SimpleNamespace(description=SimpleNamespace(market_type="GAME_BY_GAME_01_01"), market_name="1st Set Game 1 Winner")))
        self.assertTrue(is_game_market(SimpleNamespace(description=SimpleNamespace(market_type="SPECIAL"), market_name="2nd Set Game 5")))
        self.assertFalse(is_game_market(SimpleNamespace(description=SimpleNamespace(market_type="SET_WINNER"), market_name="Set 1 Winner")))

    def test_game_betting_check_marks_matches_clear_or_needing_action(self) -> None:
        betting = SimpleNamespace(
            list_events=lambda **_kwargs: [
                SimpleNamespace(event=SimpleNamespace(id="1", name="S Kopp v M Krumich", open_date="2026-08-27T09:00:00Z")),
                SimpleNamespace(event=SimpleNamespace(id="2", name="C Brady & M Howse v J Doe & A Roe", open_date="2026-08-27T11:00:00Z")),
            ],
            list_market_catalogue=lambda **_kwargs: [
                SimpleNamespace(
                    event=SimpleNamespace(id="1"),
                    description=SimpleNamespace(market_type="GAME_BY_GAME_01_01"),
                    market_name="1st Set Game 1 Winner",
                ),
                SimpleNamespace(
                    event=SimpleNamespace(id="2"),
                    description=SimpleNamespace(market_type="MATCH_ODDS"),
                    market_name="Match Odds",
                ),
            ],
        )
        client = SimpleNamespace(betting=betting, logout=lambda: None)
        matches = [
            {
                "id": "a",
                "status": "scheduled",
                "player1": "Kopp S.",
                "player2": "Krumich M.",
                "tournament": "Augsburg",
                "betfair_event_id": "1",
            },
            {"id": "b", "status": "scheduled", "player1": "Brady C. / Howse M.", "player2": "Doe J. / Roe A.", "tournament": "Augsburg Doubles"},
        ]

        with patch("app.tennis_challenger.betfair_login", return_value=client):
            result = perform_game_betting_check(matches)

        statuses = {row["match_id"]: row["status"] for row in result["rows"]}
        self.assertEqual(statuses, {"a": "needs_action", "b": "clear"})
        self.assertEqual(result["needs_action_count"], 1)
        self.assertEqual(result["summary"], "needs_action")

    def test_challenger_page_renders_controls_and_match_state(self) -> None:
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/scripts/tennis-challenger-watcher",
                "root_path": "",
                "scheme": "http",
                "server": ("test", 80),
                "headers": [],
                "query_string": b"",
                "app": hub.app,
                "router": hub.app.router,
            }
        )
        spec = SCRIPTS_BY_ID["tennis-challenger-watcher"]
        challenger = {
            "links_text": AUGSBURG_URL,
            "last_saved_at": "26 Aug 2026 10:00:00",
            "configured": True,
            "matches": [
                {
                    "id": "abc123",
                    "tournament": "Augsburg (Singles)",
                    "url": "https://www.flashscore.com/match/tennis/a/b/?mid=abc123",
                    "player1": "Kopp S.",
                    "player2": "Krumich M.",
                    "status": "scheduled",
                    "display_status": "27.08. 09:00",
                    "score_label": "-",
                    "server": "",
                    "game_betting": {"status": "needs_action", "game_market_count": 12, "market_names": []},
                }
            ],
            "game_check": {"status": "complete", "completed_at_label": "26 Aug 2026 10:01:00", "error": ""},
            "last_sweep_label": "26 Aug 2026 10:00:03",
            "watcher": {"last_error": "", "sources": []},
            "counts": {"total": 1, "scheduled": 1, "live": 0, "finished": 0},
            "alerts": [],
        }
        context = hub.template_context(
            request,
            spec=spec,
            state=hub.runner.get_state(spec.id),
            allowed=True,
            window_label="",
            tennis_challenger=challenger,
        )
        html = hub.templates.get_template("tennis_challenger.html").render(context)
        self.assertIn("Start watcher", html)
        self.assertIn("Check game betting", html)
        self.assertIn("Kopp S. v Krumich M.", html)
        self.assertIn("Action needed", html)


if __name__ == "__main__":
    unittest.main()
