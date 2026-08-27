from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from starlette.requests import Request

import app.main as hub
from app.registry import SCRIPTS_BY_ID
from app.tennis_challenger import (
    BetfairTennisEvent,
    group_matches_by_tournament,
    is_future_scheduled_match,
    match_betfair_event,
    normalize_tournament_url,
    parse_tournament_links,
    participant_match_score,
)
from scripts.Tennis_Challenger_Watcher import (
    ALERT_FLAG_BY_TYPE,
    EVENT_ROW_SELECTOR,
    FlashscoreLivePageMonitor,
    extract_feed_config,
    extract_fixtures_feed,
    fetch_tournament_feed,
    hydrate_initial_alert_flags,
    load_tournament_rows,
    normalize_scraped_match,
    overlay_live_rows,
    parse_tournament_feed,
    pending_alerts,
    prune_expired_finished_matches,
    should_refresh_betfair_events,
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
        self.assertEqual(rows[1]["scheduled_at"], "2026-08-27T09:00:00Z")
        self.assertEqual(rows[2]["raw_status"], "Finished")
        self.assertEqual(rows[2]["set_score"], {"home": 0, "away": 2})

    def test_flashscore_fixtures_payload_is_read_from_the_fixtures_page(self) -> None:
        payload = (
            "SA÷2¬~AA÷future123¬AD÷1787824800¬AB÷1¬AE÷Monday J.¬AF÷Ribecai M.¬~"
        )
        html = f"<script>cjs.initialFeeds['fixtures'] = {{ data: `{payload}`, refresh: 0 }};</script>"
        self.assertEqual(extract_fixtures_feed(html), payload)
        rows = parse_tournament_feed(extract_fixtures_feed(html), AUGSBURG_URL)
        self.assertEqual([(row["id"], row["player1"], row["player2"]) for row in rows], [
            ("future123", "Monday J.", "Ribecai M."),
        ])

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
        self.assertEqual(spec.default_args[:2], ("--poll-seconds", "10"))

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

    def test_future_match_split_uses_the_uk_calendar_date(self) -> None:
        reference = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
        self.assertFalse(
            is_future_scheduled_match(
                {"status": "scheduled", "scheduled_at": "2026-08-26T22:30:00Z"},
                reference,
            )
        )
        self.assertTrue(
            is_future_scheduled_match(
                {"status": "scheduled", "scheduled_at": "2026-08-27T08:00:00Z"},
                reference,
            )
        )
        self.assertFalse(
            is_future_scheduled_match(
                {"status": "live", "scheduled_at": "2026-08-27T08:00:00Z"},
                reference,
            )
        )

    def test_match_groups_are_split_by_competition_and_sorted_by_time(self) -> None:
        groups = group_matches_by_tournament(
            [
                {"tournament": "Roehampton (Singles)", "player1": "Later", "scheduled_at": "2026-08-27T12:00:00Z"},
                {"tournament": "Roehampton (Doubles)", "player1": "Doubles", "scheduled_at": "2026-08-27T11:30:00Z"},
                {"tournament": "Roehampton (Singles)", "player1": "Earlier", "scheduled_at": "2026-08-27T10:00:00Z"},
            ]
        )
        self.assertEqual([group["tournament"] for group in groups], ["Roehampton (Doubles)", "Roehampton (Singles)"])
        self.assertEqual([match["player1"] for match in groups[1]["matches"]], ["Earlier", "Later"])

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

    def test_scheduled_server_then_live_row_sends_separate_toss_and_start_alerts(self) -> None:
        scheduled = normalize_scraped_match(raw_match(), AUGSBURG_URL, "Augsburg (Singles)")
        scheduled["alerts_sent"] = hydrate_initial_alert_flags(scheduled)

        toss_decided = normalize_scraped_match(
            raw_match(
                server_side="home",
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        toss_decided["alerts_sent"] = dict(scheduled["alerts_sent"])
        toss_alerts = pending_alerts(scheduled, toss_decided)
        self.assertEqual(toss_alerts, ["serve_detected"])
        apply_sent(toss_decided, toss_alerts)

        live = normalize_scraped_match(
            raw_match(
                raw_status="Set 1",
                row_classes="event__match event__match--live",
                is_live=True,
                is_scheduled=False,
                set_score={"home": "0", "away": "0"},
                set_parts=[{"home": "0", "away": "0"}],
                current_points={"home": "15", "away": "0"},
                server_side="home",
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        live["alerts_sent"] = dict(toss_decided["alerts_sent"])
        self.assertEqual(pending_alerts(toss_decided, live), ["match_started"])

    def test_new_live_row_without_score_sends_match_started(self) -> None:
        live = normalize_scraped_match(
            raw_match(
                raw_status="Set 1",
                row_classes="event__match event__match--live",
                is_live=True,
                is_scheduled=False,
                set_score={"home": "0", "away": "0"},
                set_parts=[{"home": "0", "away": "0"}],
                current_points={"home": "0", "away": "0"},
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        live["alerts_sent"] = hydrate_initial_alert_flags(live)
        self.assertEqual(pending_alerts(None, live), ["match_started"])

    def test_live_page_overlay_preserves_fixture_time_and_replaces_live_fields(self) -> None:
        base = {
            "abc123": {
                **raw_match(),
                "scheduled_at": "2026-08-27T09:00:00Z",
            }
        }
        live = raw_match(
            raw_status="Set 1",
            row_classes="event__match event__match--live",
            is_live=True,
            is_scheduled=False,
            server_side="away",
        )
        merged = overlay_live_rows(base, [live])
        self.assertEqual(EVENT_ROW_SELECTOR, ".event__match[id]")
        self.assertEqual(merged["abc123"]["scheduled_at"], "2026-08-27T09:00:00Z")
        self.assertTrue(merged["abc123"]["is_live"])
        self.assertEqual(merged["abc123"]["server_side"], "away")

    def test_live_page_monitor_cannot_block_feed_scans(self) -> None:
        started = threading.Event()
        release = threading.Event()

        class BlockingProbe:
            def retain(self, _urls) -> None:
                return None

            def rows(self, _url, _label):
                started.set()
                release.wait(2)
                return []

            def discard(self, _url) -> None:
                return None

            def close(self) -> None:
                return None

        monitor = FlashscoreLivePageMonitor(
            probe_factory=BlockingProbe,
            poll_seconds=60,
            max_age_seconds=15,
        )
        try:
            monitor.set_targets({AUGSBURG_URL: "Augsburg (Singles)"})
            self.assertTrue(started.wait(1))
            before = time.monotonic()
            rows, error, age = monitor.snapshot(AUGSBURG_URL)
            self.assertLess(time.monotonic() - before, 0.1)
            self.assertEqual(rows, [])
            self.assertEqual(error, "")
            self.assertIsNone(age)
        finally:
            release.set()
            self.assertTrue(monitor.close(1))

    def test_unmatched_betfair_events_retry_once_per_minute(self) -> None:
        self.assertTrue(should_refresh_betfair_events(0, True, now=100))
        self.assertFalse(should_refresh_betfair_events(100, True, now=159.9))
        self.assertTrue(should_refresh_betfair_events(100, True, now=160))
        self.assertFalse(should_refresh_betfair_events(100, False, now=399.9))
        self.assertTrue(should_refresh_betfair_events(100, False, now=400))

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

    def test_straight_sets_finish_sends_only_match_complete(self) -> None:
        previous = normalize_scraped_match(
            raw_match(
                raw_status="Set 2",
                row_classes="event__match event__match--live",
                is_live=True,
                is_scheduled=False,
                set_score={"home": "1", "away": "0"},
                set_parts=[{"home": "6", "away": "4"}, {"home": "5", "away": "3"}],
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        previous["alerts_sent"] = hydrate_initial_alert_flags(previous)
        previous["alerts_sent"].update({"match_started": True, "set_1_complete": True})
        finished = normalize_scraped_match(
            raw_match(
                raw_status="Finished",
                is_scheduled=False,
                set_score={"home": "2", "away": "0"},
                set_parts=[{"home": "6", "away": "4"}, {"home": "6", "away": "3"}],
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        finished["alerts_sent"] = dict(previous["alerts_sent"])
        self.assertEqual(pending_alerts(previous, finished), ["match_complete"])

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
        self.assertEqual(participant_match_score("Monday J.", "Maxted/Monday"), 0)

        singles, _, _ = match_betfair_event(
            {"player1": "Monday J.", "player2": "Ribecai M."},
            [
                BetfairTennisEvent("singles", "J Monday v M Ribecai", None),
                BetfairTennisEvent("doubles", "Maxted/Monday v Andaloro/Ribecai", None),
            ],
        )
        self.assertIsNotNone(singles)
        self.assertEqual(singles.event_id, "singles")

    def test_betfair_event_matching_handles_surname_only_doubles_pairs(self) -> None:
        match = {
            "player1": "Brady C. / Howse M.",
            "player2": "Jones A. / Smith B.",
        }
        events = [
            BetfairTennisEvent("wrong", "Brady / Other v Jones / Smith", None),
            BetfairTennisEvent("right", "Brady / Howse v Jones / Smith", None),
        ]

        event, score, reason = match_betfair_event(match, events)

        self.assertIsNotNone(event)
        self.assertEqual(event.event_id, "right")
        self.assertEqual(score, 100)
        self.assertEqual(reason, "Matched")
        self.assertEqual(participant_match_score("Howse M. / Brady C.", "Brady / Howse"), 100)

    def test_doubles_matching_uses_each_betfair_surname_once(self) -> None:
        self.assertLess(
            participant_match_score("Smith A. / Smith B.", "Smith / Jones"),
            86,
        )

    def test_finished_matches_are_removed_after_one_hour_and_tombstoned(self) -> None:
        now = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
        matches = {
            "recent": {
                "id": "recent",
                "status": "finished",
                "finished_at": (now - timedelta(minutes=59)).isoformat().replace("+00:00", "Z"),
            },
            "expired": {
                "id": "expired",
                "status": "finished",
                "finished_at": (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
            },
            "live": {"id": "live", "status": "live"},
        }
        tombstones: dict[str, str] = {}

        removed = prune_expired_finished_matches(matches, tombstones, now)

        self.assertEqual(removed, ["expired"])
        self.assertEqual(set(matches), {"recent", "live"})
        self.assertEqual(tombstones, {"expired": "2026-08-27T11:00:00Z"})

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
                }
            ],
            "today_matches": [
                {
                    "id": "abc123",
                    "tournament": "Augsburg (Singles)",
                    "url": "https://www.flashscore.com/match/tennis/a/b/?mid=abc123",
                    "player1": "Kopp S.",
                    "player2": "Krumich M.",
                    "status": "scheduled",
                    "display_status": "26.08. 09:00",
                    "score_label": "-",
                    "server": "",
                }
            ],
            "future_matches": [
                {
                    "id": "future123",
                    "tournament": "Augsburg (Singles)",
                    "url": "https://www.flashscore.com/match/tennis/c/d/?mid=future123",
                    "player1": "Future A.",
                    "player2": "Future B.",
                    "status": "scheduled",
                    "display_status": "27.08. 11:00",
                    "start_time": "27.08. 11:00",
                    "betfair_event_id": "1.222222222",
                }
            ],
            "today_match_groups": [
                {
                    "tournament": "Augsburg (Singles)",
                    "matches": [
                        {
                            "id": "abc123",
                            "tournament": "Augsburg (Singles)",
                            "url": "https://www.flashscore.com/match/tennis/a/b/?mid=abc123",
                            "player1": "Kopp S.",
                            "player2": "Krumich M.",
                            "status": "scheduled",
                            "display_status": "26.08. 09:00",
                            "score_label": "-",
                            "server": "",
                        }
                    ],
                }
            ],
            "future_match_groups": [
                {
                    "tournament": "Augsburg (Singles)",
                    "matches": [
                        {
                            "id": "future123",
                            "tournament": "Augsburg (Singles)",
                            "url": "https://www.flashscore.com/match/tennis/c/d/?mid=future123",
                            "player1": "Future A.",
                            "player2": "Future B.",
                            "status": "scheduled",
                            "display_status": "27.08. 11:00",
                            "start_time": "27.08. 11:00",
                            "betfair_event_id": "1.222222222",
                        }
                    ],
                }
            ],
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
        self.assertNotIn("Check game betting", html)
        self.assertNotIn("Game betting", html)
        self.assertIn("Kopp S. v Krumich M.", html)
        self.assertIn("Tomorrow and future matches", html)
        self.assertIn("Future A. v Future B.", html)
        self.assertIn("Betfair 1.222222222", html)


if __name__ == "__main__":
    unittest.main()
