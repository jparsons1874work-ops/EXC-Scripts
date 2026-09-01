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

from playwright.sync_api import sync_playwright
from starlette.requests import Request

import app.main as hub
from app.registry import SCRIPTS_BY_ID
from app.tennis_challenger import (
    BetfairTennisEvent,
    flashscore_match_id,
    group_matches_by_tournament,
    is_future_scheduled_match,
    is_single_match_url,
    match_betfair_event,
    normalize_tournament_url,
    parse_tournament_links,
    participant_match_score,
)
from scripts.Tennis_Challenger_Watcher import (
    ALERT_FLAG_BY_TYPE,
    DEFAULT_RESET_HOURS,
    EVENT_ROW_SELECTOR,
    FlashscoreLivePageMonitor,
    STOP_EVENT,
    WATCHER_RESTART_EXIT_CODE,
    build_parser,
    carry_alert_flags,
    extract_single_match_row,
    extract_feed_config,
    extract_fixtures_feed,
    fetch_tournament_feed,
    hydrate_initial_alert_flags,
    is_broken_pipe_error,
    load_tournament_rows,
    main as watcher_main,
    match_scan_log_line,
    normalize_scraped_match,
    overlay_live_rows,
    parse_tournament_feed,
    pending_alerts,
    prune_expired_finished_matches,
    set_alerts_enabled,
    should_scan_match_detail,
    should_refresh_betfair_events,
    slack_message,
    slack_webhook_url,
    status_from_raw_match,
)


AUGSBURG_URL = "https://www.flashscore.com/tennis/challenger-men-singles/augsburg/"
ITF_URL = "https://www.flashscore.com/tennis/itf-women-singles/w35-roehampton/"
ATP_URL = "https://www.flashscore.com/tennis/atp-singles/us-open/"
WTA_URL = "https://www.flashscore.com/tennis/wta-singles/us-open/"
MATCH_URL = (
    "https://www.flashscore.com/match/tennis/martin-andres-h2AG5rdm/"
    "mayo-aidan-2mEGNcVp/?mid=r7R6cLgK"
)
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
    def test_watcher_defaults_to_two_hour_process_reset(self) -> None:
        args = build_parser().parse_args([])
        self.assertEqual(args.reset_hours, DEFAULT_RESET_HOURS)
        self.assertEqual(args.reset_hours, 2.0)

    def test_broken_pipe_errors_request_immediate_reset(self) -> None:
        self.assertTrue(is_broken_pipe_error("[Errno 32] Broken pipe"))
        self.assertTrue(is_broken_pipe_error("Browser transport: BROKEN PIPE"))
        self.assertFalse(is_broken_pipe_error("Page.goto timed out"))

    def test_main_replaces_process_after_clean_reset(self) -> None:
        args = SimpleNamespace(poll_seconds=10.0, reload_minutes=15.0, reset_hours=2.0)
        STOP_EVENT.clear()
        with (
            patch("scripts.Tennis_Challenger_Watcher.build_parser") as parser,
            patch(
                "scripts.Tennis_Challenger_Watcher.run_watcher",
                return_value=WATCHER_RESTART_EXIT_CODE,
            ),
            patch("scripts.Tennis_Challenger_Watcher.restart_watcher_process") as restart,
            patch("scripts.Tennis_Challenger_Watcher.signal.signal"),
        ):
            parser.return_value.parse_args.return_value = args
            self.assertEqual(watcher_main(), WATCHER_RESTART_EXIT_CODE)
        restart.assert_called_once_with()

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
        self.assertIn("Monitor configured Flashscore tennis tournaments and matches", completed.stdout)

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
        self.assertIn("www.flashscore.com/x/feed/t_2_5729_SUQ1VjQ1_", config["feed_url"])

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

    def test_fresh_feed_parses_a_live_doubles_match_tiebreak(self) -> None:
        payload = (
            "AA÷doubles123¬AD÷1787857800¬AB÷2"
            "¬AE÷Oliveira B./Rodrigues N.¬AF÷Puttergill C./Rai A."
            "¬AI÷y¬AG÷1¬AH÷1¬BA÷6¬BB÷7¬BC÷6¬BD÷3¬BE÷4¬BF÷9¬WA÷0¬WB÷0¬~"
        )

        raw = parse_tournament_feed(payload, AUGSBURG_URL)[0]
        match = normalize_scraped_match(raw, AUGSBURG_URL, "Kingston 2 (Doubles)")

        self.assertEqual(match["status"], "live")
        self.assertEqual(match["current_set_number"], 3)
        self.assertEqual(match["current_game"], {"home": 4, "away": 9})
        self.assertEqual(match["current_points"], {"home": 0, "away": 0})
        self.assertEqual(match["server_side"], "home")

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
                "feed_url": "https://www.flashscore.com/x/feed/test",
                "feed_sign": "feed-sign",
                "source_url": AUGSBURG_URL,
            },
            "Augsburg (Singles)",
        )

        self.assertEqual(len(rows), 1)
        self.assertIsInstance(session.call[1]["params"]["_"], int)
        self.assertEqual(session.call[1]["headers"]["Cache-Control"], "no-cache")
        self.assertEqual(session.call[1]["headers"]["Origin"], "https://www.flashscore.com")
        self.assertEqual(len(rows[0]["_feed_payload_hash"]), 12)

    def test_match_scan_log_shows_every_score_component_and_source(self) -> None:
        match = normalize_scraped_match(
            raw_match(
                raw_status="Set 2",
                row_classes="event__match event__match--live",
                is_live=True,
                is_scheduled=False,
                set_score={"home": "1", "away": "0"},
                set_parts=[{"home": "6", "away": "4"}, {"home": "3", "away": "2"}],
                current_points={"home": "15", "away": "0"},
                server_side="home",
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        match.update(
            {
                "score_source": "flashscore_live_push_page",
                "score_age_seconds": 0.4,
                "source_payload_hash": "abcdef123456",
            }
        )

        line = match_scan_log_line(match, "changed")

        self.assertIn("Kopp S. v Krumich M. (abc123) -> live", line)
        self.assertIn("sets 1-0 [6-4,3-2]", line)
        self.assertIn("game 3-2", line)
        self.assertIn("points 15-0", line)
        self.assertIn("server home", line)
        self.assertIn("source flashscore_live_push_page age 0.4s", line)
        self.assertIn("payload abcdef123456 | changed", line)

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
        self.assertEqual(normalize_tournament_url(ITF_URL + "results/"), ITF_URL)
        self.assertEqual(normalize_tournament_url(ATP_URL + "fixtures/"), ATP_URL)
        self.assertEqual(normalize_tournament_url(WTA_URL + "draw/"), WTA_URL)

    def test_single_match_links_are_normalized_and_keep_the_match_id(self) -> None:
        source = MATCH_URL + "&utm_source=operations"
        self.assertEqual(normalize_tournament_url(source), MATCH_URL)
        self.assertTrue(is_single_match_url(MATCH_URL))
        self.assertEqual(flashscore_match_id(MATCH_URL), "r7R6cLgK")
        self.assertEqual(parse_tournament_links(f"{MATCH_URL}\n{MATCH_URL}"), [MATCH_URL])
        with self.assertRaises(ValueError):
            normalize_tournament_url(MATCH_URL.split("?", 1)[0])

    def test_single_match_page_is_converted_to_a_watcher_row(self) -> None:
        class FakePage:
            def evaluate(self, expression, argument):
                if "const participant = side" in expression:
                    return {
                        "id": argument["id"],
                        "url": argument["url"],
                        "player1": "Martin A.",
                        "player2": "Mayo A.",
                        "tournament": "ATP - SINGLES: US Open",
                        "raw_status": "Set 1",
                        "row_classes": "event__match event__match--scheduled",
                        "set_score": {"home": "", "away": ""},
                        "set_parts": [],
                        "current_points": {"home": "", "away": ""},
                        "server_side": "",
                        "is_live": False,
                        "is_scheduled": True,
                    }
                return {
                    **argument,
                    "row_classes": "event__match event__match--live",
                    "is_live": True,
                    "is_scheduled": False,
                    "server_side": "home",
                }

        row = extract_single_match_row(FakePage(), MATCH_URL)

        self.assertEqual(row["id"], "r7R6cLgK")
        self.assertEqual(row["player1"], "Martin A.")
        self.assertEqual(row["server_side"], "home")
        self.assertEqual(row["_tournament"], "ATP - SINGLES: US Open")

    def test_single_match_dom_extractor_reads_itf_players_score_and_server(self) -> None:
        html = """
        <div class="tournamentHeader__country">ITF WOMEN - SINGLES: W35 Roehampton</div>
        <div class="detailScore__status">Set 2</div>
        <div class="smh__template tennis">
          <div class="smh__service smh__home"><div title="Serving player"></div></div>
          <div class="smh__participantName smh__home">
            <a class="participant__participantName">Martin A.</a>
          </div>
          <div class="smh__part smh__score smh__live smh__home smh__part--current">1</div>
          <div class="smh__part smh__home smh__part--1">6</div>
          <div class="smh__part smh__home smh__part--2">2</div>
          <div class="smh__part smh__home smh__part--game">30</div>
          <div class="smh__service smh__away"></div>
          <div class="smh__participantName smh__away">
            <a class="participant__participantName">Mayo A.</a>
          </div>
          <div class="smh__part smh__score smh__live smh__away smh__part--current">0</div>
          <div class="smh__part smh__away smh__part--1">4</div>
          <div class="smh__part smh__away smh__part--2">1</div>
          <div class="smh__part smh__away smh__part--game">15</div>
        </div>
        """
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.set_content(html)
                row = extract_single_match_row(page, MATCH_URL)
            finally:
                browser.close()

        self.assertEqual(row["player1"], "Martin A.")
        self.assertEqual(row["player2"], "Mayo A.")
        self.assertEqual(row["raw_status"], "Set 2")
        self.assertEqual(row["set_score"], {"home": "1", "away": "0"})
        self.assertEqual(row["server_side"], "home")
        self.assertTrue(row["is_live"])
        self.assertIn("ITF WOMEN", row["_tournament"])

    def test_non_tennis_flashscore_links_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            normalize_tournament_url("https://www.flashscore.com/football/england/premier-league/")

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

    def test_live_page_overlay_adds_match_missing_from_stale_feed(self) -> None:
        live = raw_match(
            id="new-live-match",
            raw_status="Set 1",
            row_classes="event__match event__match--live",
            is_live=True,
            is_scheduled=False,
        )

        merged = overlay_live_rows({}, [live])

        self.assertIn("new-live-match", merged)
        self.assertTrue(merged["new-live-match"]["is_live"])

    def test_detail_pages_cover_live_and_imminent_matches(self) -> None:
        now = datetime(2026, 8, 27, 18, 0, tzinfo=timezone.utc)
        self.assertTrue(should_scan_match_detail(raw_match(is_live=True, is_scheduled=False), now))
        self.assertTrue(
            should_scan_match_detail(
                raw_match(scheduled_at="2026-08-27T18:30:00Z"),
                now,
            )
        )
        self.assertTrue(
            should_scan_match_detail(
                raw_match(scheduled_at="2026-08-27T21:30:00Z"),
                now,
            )
        )
        self.assertFalse(
            should_scan_match_detail(
                raw_match(scheduled_at="2026-08-28T12:30:00Z"),
                now,
            )
        )

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

    def test_betfair_events_refresh_every_ten_minutes(self) -> None:
        self.assertTrue(should_refresh_betfair_events(0, True, now=100))
        self.assertFalse(should_refresh_betfair_events(100, True, now=699.9))
        self.assertTrue(should_refresh_betfair_events(100, True, now=700))
        self.assertFalse(should_refresh_betfair_events(100, False, now=699.9))
        self.assertTrue(should_refresh_betfair_events(100, False, now=700))

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
        self.assertEqual(alerts, [])
        confirmed_set_2 = {**live_set_2, "alerts_sent": dict(live_set_2["alerts_sent"])}
        alerts = pending_alerts(live_set_2, confirmed_set_2)
        self.assertEqual(alerts, ["set_1_complete"])
        apply_sent(confirmed_set_2, alerts)

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
        live_set_3["alerts_sent"] = dict(confirmed_set_2["alerts_sent"])
        alerts = pending_alerts(confirmed_set_2, live_set_3)
        self.assertEqual(alerts, [])
        confirmed_set_3 = {**live_set_3, "alerts_sent": dict(live_set_3["alerts_sent"])}
        alerts = pending_alerts(live_set_3, confirmed_set_3)
        self.assertEqual(alerts, ["set_2_complete"])
        apply_sent(confirmed_set_3, alerts)

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
        finished["alerts_sent"] = dict(confirmed_set_3["alerts_sent"])
        self.assertEqual(pending_alerts(confirmed_set_3, finished), [])
        confirmed_finished = {**finished, "alerts_sent": dict(finished["alerts_sent"])}
        self.assertEqual(pending_alerts(finished, confirmed_finished), ["match_complete"])

    def test_itf_matches_skip_set_alerts_but_keep_match_complete(self) -> None:
        live_set_2 = normalize_scraped_match(
            raw_match(
                raw_status="Set 2",
                row_classes="event__match event__match--live",
                is_live=True,
                is_scheduled=False,
                set_score={"home": "1", "away": "0"},
                set_parts=[{"home": "6", "away": "4"}, {"home": "0", "away": "0"}],
            ),
            ITF_URL,
            "W35 Roehampton (ITF Women Singles)",
        )
        live_set_2["alerts_sent"] = {"match_started": True}
        repeated_live_set_2 = {**live_set_2, "alerts_sent": dict(live_set_2["alerts_sent"])}

        self.assertFalse(set_alerts_enabled(live_set_2))
        self.assertEqual(pending_alerts(live_set_2, repeated_live_set_2), [])
        self.assertTrue(set_alerts_enabled({"tournament": "ATP US Open"}))
        self.assertTrue(set_alerts_enabled({"tournament": "WTA US Open"}))

        finished = {
            **repeated_live_set_2,
            "status": "finished",
            "completion_confirmed": True,
            "current_set_number": None,
        }
        self.assertEqual(pending_alerts(repeated_live_set_2, finished), [])
        confirmed_finished = {**finished, "alerts_sent": dict(finished["alerts_sent"])}
        self.assertEqual(pending_alerts(finished, confirmed_finished), ["match_complete"])

    def test_all_doubles_matches_skip_set_alerts(self) -> None:
        challenger_doubles = {
            "status": "live",
            "current_set_number": 2,
            "sets": [{"home": 6, "away": 4}, {"home": 0, "away": 0}],
            "alerts_sent": {"match_started": True},
            "tournament": "Kingston 2 (Challenger Doubles)",
            "source_url": "https://www.flashscore.com/tennis/challenger-men-doubles/kingston-2/",
        }
        direct_doubles = {
            **challenger_doubles,
            "tournament": "ATP - DOUBLES: US Open",
            "source_url": MATCH_URL,
        }
        direct_pair_without_competition_label = {
            **challenger_doubles,
            "tournament": "Single match r7R6cLgK",
            "source_url": MATCH_URL,
            "player1": "Player A. / Partner B.",
            "player2": "Player C. / Partner D.",
        }

        self.assertFalse(set_alerts_enabled(challenger_doubles))
        self.assertFalse(set_alerts_enabled(direct_doubles))
        self.assertFalse(set_alerts_enabled(direct_pair_without_competition_label))
        self.assertEqual(
            pending_alerts(challenger_doubles, {**challenger_doubles}),
            [],
        )
        self.assertEqual(
            pending_alerts(direct_doubles, {**direct_doubles}),
            [],
        )
        self.assertTrue(
            set_alerts_enabled(
                {
                    "tournament": "US Open (ATP Singles)",
                    "source_url": ATP_URL,
                }
            )
        )

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
        self.assertEqual(pending_alerts(previous, finished), [])
        confirmed_finished = {**finished, "alerts_sent": dict(finished["alerts_sent"])}
        self.assertEqual(pending_alerts(finished, confirmed_finished), ["match_complete"])

    def test_final_score_overrides_a_stale_live_status(self) -> None:
        finished = normalize_scraped_match(
            raw_match(
                raw_status="Set 2",
                row_classes="event__match event__match--live",
                is_live=True,
                is_scheduled=False,
                set_score={"home": "0", "away": "2"},
                set_parts=[{"home": "2", "away": "6"}, {"home": "4", "away": "6"}],
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )

        self.assertEqual(finished["status"], "finished")

    def test_interrupted_status_overrides_a_score_that_looks_finished(self) -> None:
        interrupted_raw = raw_match(
            raw_status="Interrupted",
            row_classes="event__match event__match--live",
            is_live=True,
            is_scheduled=False,
            set_score={"home": "2", "away": "0"},
            set_parts=[{"home": "6", "away": "4"}, {"home": "6", "away": "3"}],
        )

        self.assertEqual(status_from_raw_match(interrupted_raw), "suspended")
        self.assertEqual(
            status_from_raw_match({**interrupted_raw, "raw_status": "Finished - Interrupted"}),
            "suspended",
        )
        interrupted = normalize_scraped_match(
            interrupted_raw,
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        self.assertEqual(interrupted["status"], "suspended")

    def test_interrupted_match_never_triggers_set_or_match_complete_alerts(self) -> None:
        previous = normalize_scraped_match(
            raw_match(
                raw_status="Set 2",
                row_classes="event__match event__match--live",
                is_live=True,
                is_scheduled=False,
                set_score={"home": "1", "away": "0"},
                set_parts=[{"home": "6", "away": "4"}, {"home": "5", "away": "5"}],
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        previous["alerts_sent"] = {
            "serve_detected": True,
            "match_started": True,
            "set_1_complete": True,
            "set_2_complete": False,
            "match_complete": False,
        }
        provisional_finished = normalize_scraped_match(
            raw_match(
                raw_status="Set 3",
                row_classes="event__match event__match--live",
                is_live=True,
                is_scheduled=False,
                set_score={"home": "2", "away": "0"},
                set_parts=[{"home": "6", "away": "4"}, {"home": "6", "away": "3"}],
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        provisional_finished["alerts_sent"] = carry_alert_flags(previous, provisional_finished)
        self.assertEqual(provisional_finished["status"], "finished")
        self.assertFalse(provisional_finished["completion_confirmed"])
        self.assertEqual(pending_alerts(previous, provisional_finished), [])
        repeated_provisional = {
            **provisional_finished,
            "alerts_sent": dict(provisional_finished["alerts_sent"]),
        }
        self.assertEqual(pending_alerts(provisional_finished, repeated_provisional), [])

        interrupted = normalize_scraped_match(
            raw_match(
                raw_status="Interrupted Set 3",
                row_classes="event__match event__match--live",
                is_live=True,
                is_scheduled=False,
                set_score={"home": "1", "away": "1"},
                set_parts=[{"home": "6", "away": "4"}, {"home": "4", "away": "6"}],
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        interrupted["alerts_sent"] = carry_alert_flags(provisional_finished, interrupted)

        self.assertEqual(interrupted["status"], "suspended")
        self.assertEqual(pending_alerts(provisional_finished, interrupted), [])

    def test_resumed_match_clears_false_completion_and_alerts_on_real_finish(self) -> None:
        interrupted = normalize_scraped_match(
            raw_match(
                raw_status="Interrupted",
                row_classes="event__match event__match--suspended",
                is_live=False,
                is_scheduled=False,
                set_score={"home": "1", "away": "1"},
                set_parts=[{"home": "6", "away": "4"}, {"home": "4", "away": "6"}],
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        interrupted["alerts_sent"] = {
            "serve_detected": True,
            "match_started": True,
            "set_1_complete": True,
            "set_2_complete": True,
            "match_complete": True,
        }
        resumed = normalize_scraped_match(
            raw_match(
                raw_status="Set 3",
                row_classes="event__match event__match--live",
                is_live=True,
                is_scheduled=False,
                set_score={"home": "1", "away": "1"},
                set_parts=[
                    {"home": "6", "away": "4"},
                    {"home": "4", "away": "6"},
                    {"home": "3", "away": "2"},
                ],
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        resumed["alerts_sent"] = carry_alert_flags(interrupted, resumed)
        self.assertFalse(resumed["alerts_sent"]["match_complete"])

        finished = normalize_scraped_match(
            raw_match(
                raw_status="Finished",
                row_classes="event__match event__match--finished",
                is_live=False,
                is_scheduled=False,
                set_score={"home": "2", "away": "1"},
                set_parts=[
                    {"home": "6", "away": "4"},
                    {"home": "4", "away": "6"},
                    {"home": "6", "away": "2"},
                ],
            ),
            AUGSBURG_URL,
            "Augsburg (Singles)",
        )
        finished["alerts_sent"] = carry_alert_flags(resumed, finished)

        self.assertEqual(pending_alerts(resumed, finished), [])
        confirmed_finished = {**finished, "alerts_sent": dict(finished["alerts_sent"])}
        self.assertEqual(pending_alerts(finished, confirmed_finished), ["match_complete"])

    def test_doubles_match_tiebreak_waits_for_ten_points(self) -> None:
        common = {
            "raw_status": "Set 3",
            "row_classes": "event__match event__match--live",
            "is_live": True,
            "is_scheduled": False,
            "set_score": {"home": "1", "away": "1"},
        }
        still_live = normalize_scraped_match(
            raw_match(
                **common,
                set_parts=[
                    {"home": "6", "away": "7"},
                    {"home": "6", "away": "3"},
                    {"home": "4", "away": "9"},
                ],
            ),
            AUGSBURG_URL,
            "Augsburg (Doubles)",
        )
        finished = normalize_scraped_match(
            raw_match(
                **common,
                set_parts=[
                    {"home": "6", "away": "7"},
                    {"home": "6", "away": "3"},
                    {"home": "4", "away": "10"},
                ],
            ),
            AUGSBURG_URL,
            "Augsburg (Doubles)",
        )

        self.assertEqual(still_live["status"], "live")
        self.assertEqual(finished["status"], "finished")

    def test_finished_detail_result_survives_the_stale_feed_overlay(self) -> None:
        monitor = FlashscoreLivePageMonitor.__new__(FlashscoreLivePageMonitor)
        monitor._lock = threading.Lock()
        monitor._rows = {}
        monitor._updated_at = {}
        monitor._errors = {}
        monitor._detail_targets_by_source = {}
        monitor._detail_rows = {}
        monitor._detail_updated_at = {}
        monitor._detail_errors = {}
        monitor._max_age_seconds = 30
        finished_raw = raw_match(
            raw_status="Finished",
            row_classes="event__match event__match--finished",
            is_live=False,
            is_scheduled=False,
            set_score={"home": "0", "away": "2"},
            set_parts=[{"home": "2", "away": "6"}, {"home": "4", "away": "6"}],
        )
        monitor.set_detail_targets(AUGSBURG_URL, [finished_raw])
        monitor._detail_rows["abc123"] = finished_raw
        monitor._detail_updated_at["abc123"] = time.monotonic()

        rows, error, age = monitor.snapshot(AUGSBURG_URL)

        self.assertEqual(error, "")
        self.assertIsNotNone(age)
        self.assertEqual(rows[0]["raw_status"], "Finished")

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
            "player1": "Hands T./Summers M.",
            "player2": "Blaydes B./Frydrych V.",
        }
        events = [
            BetfairTennisEvent("wrong", "Hands/Other - Blaydes/Frydrych", None),
            BetfairTennisEvent("right", "Hands/Summers - Blaydes/Frydrych", None),
        ]

        event, score, reason = match_betfair_event(match, events)

        self.assertIsNotNone(event)
        self.assertEqual(event.event_id, "right")
        self.assertEqual(score, 100)
        self.assertEqual(reason, "Matched")
        self.assertEqual(participant_match_score("Summers M./Hands T.", "Hands/Summers"), 100)
        self.assertEqual(participant_match_score("Hands T./Summers M.", "M Summers / T Hands"), 100)

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
        self.assertIn("Manual Tennis Watcher", html)
        self.assertIn("ITF and all doubles alerts", html)
        self.assertIn("Start watcher", html)
        self.assertNotIn("Check game betting", html)
        self.assertNotIn("Game betting", html)
        self.assertIn("Kopp S. v Krumich M.", html)
        self.assertIn("Tomorrow and future matches", html)
        self.assertIn("Future A. v Future B.", html)
        self.assertIn("Betfair 1.222222222", html)


if __name__ == "__main__":
    unittest.main()
