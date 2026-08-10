from __future__ import annotations

import unittest
from dataclasses import replace
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
    resolve_betfair_certs_dir,
    resolve_config_path,
    resolve_config_source,
    select_reminders,
    select_market_reminders,
    select_reminders_with_reasons,
    scheduled_keys,
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
    market_name: str = "Match Odds",
    market_type_code: str = "MATCH_ODDS",
    country_code: str = "GB",
    wallet: str = "UK wallet",
    regulator: str = "MALTA LOTTERIES AND GAMBLING AUTHORITY",
    market_rules: str = "",
    event_name: str | None = None,
    emoji: str = ":test:",
) -> EventReminder:
    return EventReminder(
        sport=sport,
        emoji=emoji,
        event_type_id="1",
        event_id=event_id,
        event_name=event_name or f"Event {event_id}",
        competition_id=competition_id,
        competition_name=competition_name,
        market_id=market_id or f"market-{event_id}",
        market_name=market_name,
        event_start_utc=start_utc,
        market_type_code=market_type_code,
        country_code=country_code,
        wallet=wallet,
        regulator=regulator,
        market_rules=market_rules,
    )


class BetfairEventReminderTests(unittest.TestCase):
    def test_hourly_uk_scan_window(self) -> None:
        now = datetime(2026, 7, 9, 12, 37, tzinfo=UK_TZ)
        window = build_scan_window(now_uk=now)
        self.assertEqual(window.start_uk.strftime("%Y-%m-%d %H:%M %Z"), "2026-07-09 12:00 BST")
        self.assertEqual(window.end_uk.strftime("%Y-%m-%d %H:%M %Z"), "2026-07-10 12:00 BST")

    def test_uk_scan_window_converts_to_utc(self) -> None:
        now = datetime(2026, 7, 9, 12, 0, tzinfo=UK_TZ)
        window = build_scan_window(now_uk=now)
        self.assertEqual(window.start_utc, datetime(2026, 7, 9, 11, 0, tzinfo=timezone.utc))
        self.assertEqual(window.end_utc, datetime(2026, 7, 10, 11, 0, tzinfo=timezone.utc))

    def test_1500_uk_scan_window(self) -> None:
        now = datetime(2026, 7, 9, 15, 2, tzinfo=UK_TZ)
        window = build_scan_window(now_uk=now)
        self.assertEqual(window.start_uk.strftime("%Y-%m-%d %H:%M %Z"), "2026-07-09 15:00 BST")
        self.assertEqual(window.end_uk.strftime("%Y-%m-%d %H:%M %Z"), "2026-07-10 15:00 BST")

    def test_2300_uk_scan_window(self) -> None:
        now = datetime(2026, 7, 9, 23, 2, tzinfo=UK_TZ)
        window = build_scan_window(now_uk=now)
        self.assertEqual(window.start_uk.strftime("%Y-%m-%d %H:%M %Z"), "2026-07-09 23:00 BST")
        self.assertEqual(window.end_uk.strftime("%Y-%m-%d %H:%M %Z"), "2026-07-10 23:00 BST")

    def test_early_morning_uses_current_hour_scan_window(self) -> None:
        now = datetime(2026, 7, 10, 6, 59, tzinfo=UK_TZ)
        window = build_scan_window(now_uk=now)
        self.assertEqual(window.start_uk.strftime("%Y-%m-%d %H:%M %Z"), "2026-07-10 06:00 BST")
        self.assertEqual(window.end_uk.strftime("%Y-%m-%d %H:%M %Z"), "2026-07-11 06:00 BST")

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
        later = reminder("Snooker", "later", datetime(2026, 7, 9, 20, 0, tzinfo=timezone.utc))
        earlier = reminder("Snooker", "earlier", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc))
        selected = select_reminders([later, earlier], "first")
        self.assertEqual([item.event_id for item in selected], ["earlier"])

    def test_darts_first_match_per_competition_is_selected(self) -> None:
        events = [
            reminder("Darts", "a-early", datetime(2026, 7, 9, 16, 0, tzinfo=timezone.utc), competition_id="a"),
            reminder("Darts", "b-only", datetime(2026, 7, 9, 17, 0, tzinfo=timezone.utc), competition_id="b"),
        ]
        selected = select_reminders(events, "darts_first_competition_gap_and_new_date", datetime(2026, 7, 9, 7, 0, tzinfo=UK_TZ))
        self.assertEqual([item.event_id for item in selected], ["a-early", "b-only"])

    def test_darts_same_competition_within_one_hour_is_not_selected(self) -> None:
        events = [
            reminder("Darts", "first", datetime(2026, 7, 9, 16, 0, tzinfo=timezone.utc), competition_id="a"),
            reminder("Darts", "within", datetime(2026, 7, 9, 16, 59, tzinfo=timezone.utc), competition_id="a"),
        ]
        selected = select_reminders(events, "darts_first_competition_gap_and_new_date", datetime(2026, 7, 9, 7, 0, tzinfo=UK_TZ))
        self.assertEqual([item.event_id for item in selected], ["first"])

    def test_darts_same_competition_gap_greater_than_one_hour_is_selected(self) -> None:
        events = [
            reminder("Darts", "first", datetime(2026, 7, 9, 16, 0, tzinfo=timezone.utc), competition_id="a"),
            reminder("Darts", "gap", datetime(2026, 7, 9, 17, 1, tzinfo=timezone.utc), competition_id="a"),
        ]
        selected = select_reminders_with_reasons(
            events,
            "darts_first_competition_gap_and_new_date",
            datetime(2026, 7, 9, 7, 0, tzinfo=UK_TZ),
        )
        self.assertEqual([item.reminder.event_id for item in selected], ["first", "gap"])
        self.assertEqual(selected[1].reasons, ("new_group_gap_gt_1h",))

    def test_darts_same_competition_exactly_one_hour_later_is_not_selected(self) -> None:
        events = [
            reminder("Darts", "first", datetime(2026, 7, 9, 16, 0, tzinfo=timezone.utc), competition_id="a"),
            reminder("Darts", "exact", datetime(2026, 7, 9, 17, 0, tzinfo=timezone.utc), competition_id="a"),
        ]
        selected = select_reminders(events, "darts_first_competition_gap_and_new_date", datetime(2026, 7, 9, 7, 0, tzinfo=UK_TZ))
        self.assertEqual([item.event_id for item in selected], ["first"])

    def test_darts_first_match_on_new_uk_calendar_date_is_selected(self) -> None:
        events = [
            reminder("Darts", "scan-date", datetime(2026, 7, 9, 22, 30, tzinfo=timezone.utc), competition_id="a"),
            reminder("Darts", "new-date", datetime(2026, 7, 9, 23, 15, tzinfo=timezone.utc), competition_id="a"),
        ]
        selected = select_reminders_with_reasons(
            events,
            "darts_first_competition_gap_and_new_date",
            datetime(2026, 7, 9, 7, 0, tzinfo=UK_TZ),
        )
        self.assertEqual([item.reminder.event_id for item in selected], ["scan-date", "new-date"])
        self.assertEqual(selected[1].reasons, ("first_on_new_scan_date",))

    def test_darts_gap_and_new_date_qualifier_is_selected_once(self) -> None:
        events = [
            reminder("Darts", "scan-date", datetime(2026, 7, 9, 21, 30, tzinfo=timezone.utc), competition_id="a"),
            reminder("Darts", "both", datetime(2026, 7, 9, 23, 30, tzinfo=timezone.utc), competition_id="a"),
        ]
        selected = select_reminders_with_reasons(
            events,
            "darts_first_competition_gap_and_new_date",
            datetime(2026, 7, 9, 7, 0, tzinfo=UK_TZ),
        )
        self.assertEqual([item.reminder.event_id for item in selected], ["scan-date", "both"])
        self.assertEqual(selected[1].reasons, ("new_group_gap_gt_1h", "first_on_new_scan_date"))

    def test_darts_multiple_competitions_are_handled_independently(self) -> None:
        events = [
            reminder("Darts", "a-first", datetime(2026, 7, 9, 16, 0, tzinfo=timezone.utc), competition_id="a"),
            reminder("Darts", "a-skip", datetime(2026, 7, 9, 16, 30, tzinfo=timezone.utc), competition_id="a"),
            reminder("Darts", "b-first", datetime(2026, 7, 9, 16, 45, tzinfo=timezone.utc), competition_id="b"),
        ]
        selected = select_reminders(events, "darts_first_competition_gap_and_new_date", datetime(2026, 7, 9, 7, 0, tzinfo=UK_TZ))
        self.assertEqual([item.event_id for item in selected], ["a-first", "b-first"])

    def test_boxing_first_fight_is_selected(self) -> None:
        fights = [
            reminder("Boxing", "first", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc)),
            reminder("Boxing", "second", datetime(2026, 7, 9, 18, 30, tzinfo=timezone.utc)),
        ]
        selected = select_reminders_with_reasons(fights, "boxing_first_and_gap_batches")
        self.assertEqual([item.reminder.event_id for item in selected], ["first"])
        self.assertEqual(selected[0].reasons, ("first_boxing_fight",))
        self.assertEqual(selected[0].reminder.lead_minutes, 30)

    def test_boxing_selected_fight_expands_to_one_hour_and_thirty_minute_reminders(self) -> None:
        selected = reminder(
            "Boxing",
            "35867681",
            datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc),
            event_name="Fighter A v Fighter B",
            emoji=":boxing:",
        )
        expanded = reminders.expand_boxing_reminder_times([selected])
        self.assertEqual([item.lead_minutes for item in expanded], [60, 30])
        self.assertEqual(
            [reminders.reminder_time(item.event_start_utc, item.lead_minutes).strftime("%H:%M %Z") for item in expanded],
            ["18:00 BST", "18:30 BST"],
        )
        self.assertEqual(
            [reminders.format_slack_text(item) for item in expanded],
            [
                ":boxing: Fighter A v Fighter B (Event ID: 35867681) starting in 1 hour - Prep for manual management",
                ":boxing: Fighter A v Fighter B (Event ID: 35867681) starting in 30 mins - Prep for manual management",
            ],
        )

    def test_boxing_outright_market_is_not_expanded(self) -> None:
        outright = replace(
            reminder("Boxing", "event-1", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc)),
            duplicate_by=reminders.DEDUP_MARKET,
        )
        self.assertEqual(reminders.expand_boxing_reminder_times([outright]), [outright])

    def test_boxing_fight_within_two_hours_is_not_selected(self) -> None:
        fights = [
            reminder("Boxing", "first", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc)),
            reminder("Boxing", "within", datetime(2026, 7, 9, 19, 59, tzinfo=timezone.utc)),
        ]
        selected = select_reminders(fights, "boxing_first_and_gap_batches")
        self.assertEqual([item.event_id for item in selected], ["first"])

    def test_boxing_fight_gap_greater_than_two_hours_is_selected(self) -> None:
        fights = [
            reminder("Boxing", "first", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc)),
            reminder("Boxing", "gap", datetime(2026, 7, 9, 20, 1, tzinfo=timezone.utc)),
        ]
        selected = select_reminders_with_reasons(fights, "boxing_first_and_gap_batches")
        self.assertEqual([item.reminder.event_id for item in selected], ["first", "gap"])
        self.assertEqual(selected[1].reasons, ("new_boxing_batch_gap_gt_2h",))

    def test_boxing_fight_exactly_two_hours_later_is_not_selected(self) -> None:
        fights = [
            reminder("Boxing", "first", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc)),
            reminder("Boxing", "exact", datetime(2026, 7, 9, 20, 0, tzinfo=timezone.utc)),
        ]
        selected = select_reminders(fights, "boxing_first_and_gap_batches")
        self.assertEqual([item.event_id for item in selected], ["first"])

    def test_boxing_multiple_new_batches_are_selected(self) -> None:
        fights = [
            reminder("Boxing", "first", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc)),
            reminder("Boxing", "skip", datetime(2026, 7, 9, 18, 30, tzinfo=timezone.utc)),
            reminder("Boxing", "second-batch", datetime(2026, 7, 9, 21, 0, tzinfo=timezone.utc)),
            reminder("Boxing", "third-batch", datetime(2026, 7, 9, 23, 30, tzinfo=timezone.utc)),
        ]
        selected = select_reminders(fights, "boxing_first_and_gap_batches")
        self.assertEqual([item.event_id for item in selected], ["first", "second-batch", "third-batch"])

    def test_boxing_dedupe_prevents_duplicate_selection(self) -> None:
        fights = [
            reminder("Boxing", "first", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc)),
            reminder("Boxing", "first", datetime(2026, 7, 9, 21, 0, tzinfo=timezone.utc), market_id="second-market"),
        ]
        selected = select_reminders_with_reasons(fights, "boxing_first_and_gap_batches")
        self.assertEqual([item.reminder.event_id for item in selected], ["first"])
        self.assertEqual(len(selected), 1)

    def test_all_event_sports_still_select_all_events(self) -> None:
        for sport in ("American Football", "Gaelic Games", "Rugby League", "Rugby Union"):
            events = [
                reminder(sport, f"{sport}-1", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc)),
                reminder(sport, f"{sport}-2", datetime(2026, 7, 9, 19, 0, tzinfo=timezone.utc)),
            ]
            selected = select_reminders(events, "all")
            self.assertEqual([item.event_id for item in selected], [f"{sport}-1", f"{sport}-2"])

    def test_mma_and_snooker_still_select_first_event_only(self) -> None:
        for sport in ("Mixed Martial Arts", "Snooker"):
            events = [
                reminder(sport, f"{sport}-late", datetime(2026, 7, 9, 20, 0, tzinfo=timezone.utc)),
                reminder(sport, f"{sport}-early", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc)),
            ]
            selected = select_reminders(events, "first")
            self.assertEqual([item.event_id for item in selected], [f"{sport}-early"])
            expected_lead = 30 if sport == "Mixed Martial Arts" else 5
            self.assertEqual(selected[0].lead_minutes, expected_lead)

    def test_requested_sport_message_formats(self) -> None:
        start = datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc)
        cases = (
            (
                reminder("Rugby League", "35844821", start, event_name="Leeds Rhinos v Bradford Bulls", emoji=":rugby_football:"),
                ":rugby_football: Ensure Leeds Rhinos v Bradford Bulls goes in play (Event ID: 35844821)",
            ),
            (
                reminder("Boxing", "35785447", start, event_name="Otto Wallin v Vladyslav Sirenko", emoji=":boxing:"),
                ":boxing: Otto Wallin v Vladyslav Sirenko (Event ID: 35785447) starting in 30 mins - Prep for manual management",
            ),
            (
                reminder("American Football", "35844693", start, event_name="Hamilton Tiger-Cats @ Montreal Alouettes", emoji=":football:"),
                ":football: Ensure Hamilton Tiger-Cats @ Montreal Alouettes goes in play (Event ID: 35844693)",
            ),
            (
                reminder("Snooker", "35880456", start, competition_name="World Championship", emoji=":8ball:"),
                ':8ball: Ensure "World Championship" is grabbed by the bot',
            ),
            (
                reminder("Darts", "35880865", start, competition_name="World Seniors Matchplay", emoji=":dart:"),
                ':dart: Ensure "World Seniors Matchplay" is grabbed by bot',
            ),
            (
                reminder("Mixed Martial Arts", "35879407", start, event_name="Amru Magomedov v Angel Alvarez", emoji=":martial_arts_uniform:"),
                ":martial_arts_uniform: Amru Magomedov v Angel Alvarez (Event ID: 35879407) starting in 30 mins Prep for manual management",
            ),
        )
        for item, expected in cases:
            with self.subTest(sport=item.sport):
                self.assertEqual(reminders.format_slack_text(item), expected)

    def test_duplicate_key_generation(self) -> None:
        item = reminder("Snooker", "123", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc))
        self.assertEqual(duplicate_key(item, 1783605300, "C123"), "Snooker|123|1783605300|C123")

    def test_overlapping_scans_reuse_persisted_duplicate_key(self) -> None:
        item = reminder("Golf", "event-1", datetime(2026, 7, 10, 5, 0, tzinfo=timezone.utc), market_id="1.golf")
        morning_window = build_scan_window(datetime(2026, 7, 9, 7, 0, tzinfo=UK_TZ))
        afternoon_window = build_scan_window(datetime(2026, 7, 9, 15, 0, tzinfo=UK_TZ))
        self.assertTrue(morning_window.start_utc <= item.event_start_utc < morning_window.end_utc)
        self.assertTrue(afternoon_window.start_utc <= item.event_start_utc < afternoon_window.end_utc)

        post_epoch = int(reminder_time(item.event_start_utc).timestamp())
        key = duplicate_key(item, post_epoch, "C123")
        state = {"scheduled": [{"duplicate_key": key}]}
        self.assertIn(duplicate_key(item, post_epoch, "C123"), scheduled_keys(state))

    def test_time_change_finds_only_active_pending_reminder_in_same_channel(self) -> None:
        item = reminder("Boxing", "event-1", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc))
        new_post_epoch = int(datetime(2026, 7, 9, 18, 30, tzinfo=UK_TZ).timestamp())
        old_post_epoch = new_post_epoch - 3600
        state = {
            "scheduled": [
                {
                    "duplicate_key": f"Boxing|event-1|{old_post_epoch}|C07FXG95GQ6",
                    "sport": "Boxing",
                    "event_id": "event-1",
                    "scheduled_slack_post_epoch": old_post_epoch,
                    "slack_scheduled_message_id": "Q123",
                },
                {
                    "duplicate_key": f"Boxing|event-1|{old_post_epoch - 60}|C07FXG95GQ6",
                    "sport": "Boxing",
                    "event_id": "event-1",
                    "scheduled_slack_post_epoch": old_post_epoch - 60,
                    "slack_scheduled_message_id": "Q122",
                    "status": "superseded",
                },
                {
                    "duplicate_key": f"Boxing|event-1|{old_post_epoch}|C_OLD_CHANNEL",
                    "sport": "Boxing",
                    "event_id": "event-1",
                    "scheduled_slack_post_epoch": old_post_epoch,
                    "slack_scheduled_message_id": "Q121",
                },
            ]
        }
        matches = reminders.superseded_scheduled_records(
            state,
            item,
            new_post_epoch,
            "C07FXG95GQ6",
            old_post_epoch - 1,
        )
        self.assertEqual([record["slack_scheduled_message_id"] for record in matches], ["Q123"])

    def test_boxing_dual_reminder_slots_do_not_supersede_each_other(self) -> None:
        one_hour = replace(
            reminder("Boxing", "event-1", datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc), emoji=":boxing:"),
            lead_minutes=60,
        )
        thirty_minute_epoch = int(reminder_time(one_hour.event_start_utc, 30).timestamp())
        state = {
            "scheduled": [
                {
                    "duplicate_key": f"Boxing|event-1|{thirty_minute_epoch}|C07FXG95GQ6",
                    "sport": "Boxing",
                    "event_id": "event-1",
                    "scheduled_slack_post_epoch": thirty_minute_epoch,
                    "reminder_offset_minutes": 30,
                    "slack_text": ":boxing: Fighter A v Fighter B starting in 30 mins",
                    "slack_scheduled_message_id": "Q30",
                }
            ]
        }
        matches = reminders.superseded_scheduled_records(
            state,
            one_hour,
            int(reminder_time(one_hour.event_start_utc, 60).timestamp()),
            "C07FXG95GQ6",
            thirty_minute_epoch - 3600,
        )
        self.assertEqual(matches, [])

    def test_politics_market_inside_window_is_selected(self) -> None:
        window = build_scan_window(datetime(2026, 7, 9, 12, 0, tzinfo=UK_TZ))
        item = reminder(
            "Politics",
            "event-1",
            datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc),
            market_id="1.pol",
            market_name="Next Prime Minister",
            emoji=":classical_building:",
        )
        selected = select_market_reminders([item], "Politics", window)
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0].selection_reason, "politics_market_in_window")

    def test_politics_multiple_markets_under_one_event_are_each_selected(self) -> None:
        start = datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)
        markets = [
            reminder("Politics", "event-1", start, market_id="1.pol-a", market_name="Seat A"),
            reminder("Politics", "event-1", start, market_id="1.pol-b", market_name="Seat B"),
        ]
        selected = select_market_reminders(markets, "Politics")
        self.assertEqual([item.market_id for item in selected], ["1.pol-a", "1.pol-b"])

    def test_politics_market_outside_window_is_excluded(self) -> None:
        window = build_scan_window(datetime(2026, 7, 9, 12, 0, tzinfo=UK_TZ))
        item = reminder("Politics", "event-1", datetime(2026, 7, 10, 12, 1, tzinfo=UK_TZ))
        self.assertEqual(select_market_reminders([item], "Politics", window), [])

    def test_politics_reminder_is_five_minutes_before_market_start_and_dedupes_by_market_id(self) -> None:
        item = select_market_reminders(
            [
                reminder(
                    "Politics",
                    "event-1",
                    datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc),
                    market_id="1.pol",
                    market_name="Next Prime Minister",
                )
            ],
            "Politics",
        )[0]
        post_epoch = int(reminder_time(item.event_start_utc, item.lead_minutes).timestamp())
        self.assertEqual(reminder_time(item.event_start_utc, item.lead_minutes).strftime("%H:%M %Z"), "14:55 BST")
        self.assertEqual(duplicate_key(item, post_epoch, "C123"), "Politics|1.pol|1783605300|C123")

    def test_athletics_is_configured_with_fallback_id_and_emoji(self) -> None:
        athletics = next(sport for sport in reminders.SPORTS if sport["name"] == "Athletics")
        self.assertEqual(athletics["rule"], reminders.SPORT_RULE_WINNER_MARKETS)
        self.assertEqual(athletics["emoji"], ":athletics:")
        self.assertEqual(reminders.FALLBACK_EVENT_TYPE_IDS["Athletics"], "3988")

    def test_athletics_scans_all_market_types(self) -> None:
        self.assertIsNone(
            reminders.api_market_type_filter_for_sport("Athletics", reminders.SPORT_RULE_WINNER_MARKETS)
        )

    def test_athletics_selects_requested_outright_market_types(self) -> None:
        start = datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)
        markets = [
            reminder(
                "Athletics",
                f"event-{market_type}",
                start,
                market_id=f"1.{index}",
                market_name=market_type.replace("_", " ").title(),
                market_type_code=market_type,
                emoji=":athletics:",
            )
            for index, market_type in enumerate(("WINNER", "OUTRIGHT_WINNER", "GOLD_MEDAL_WINNER"), start=1)
        ]
        markets.append(
            reminder(
                "Athletics",
                "event-heat",
                start,
                market_id="1.heat",
                market_name="Heat Winner",
                market_type_code="HEAT_WINNER",
                emoji=":athletics:",
            )
        )

        selected = select_market_reminders(markets, "Athletics")

        self.assertEqual({item.market_id for item in selected}, {"1.1", "1.2", "1.3"})
        self.assertTrue(all(item.duplicate_by == reminders.DEDUP_MARKET for item in selected))

    def test_athletics_outright_uses_five_minute_timing_and_outright_message(self) -> None:
        item = select_market_reminders(
            [
                reminder(
                    "Athletics",
                    "athletics-event",
                    datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc),
                    market_id="1.athletics",
                    market_name="Men's 100m Gold Medal Winner",
                    market_type_code="GOLD_MEDAL_WINNER",
                    event_name="World Athletics Championships",
                    emoji=":athletics:",
                )
            ],
            "Athletics",
        )[0]

        self.assertEqual(reminder_time(item.event_start_utc, item.lead_minutes).strftime("%H:%M %Z"), "14:55 BST")
        self.assertEqual(
            reminders.format_slack_text(item),
            ":athletics: Athletics: World Athletics Championships - Men's 100m Gold Medal Winner "
            "(Market ID: 1.athletics)",
        )

    def test_athletics_uses_authoritative_market_day_and_replaces_early_catalogue_time(self) -> None:
        class FakeResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self):
                return {
                    "eventTypes": [
                        {
                            "eventNodes": [
                                {
                                    "marketNodes": [
                                        {
                                            "marketId": "1.260973277",
                                            "description": {"marketTime": "2026-08-11T20:30:00.000Z"},
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }

        catalogue_market = reminder(
            "Athletics",
            "31664995",
            datetime(2026, 8, 10, 19, 35, tzinfo=timezone.utc),
            market_id="1.260973277",
            market_name="Womens 100m Hurdles Gold Medal Winner",
            market_type_code="WINNER",
            event_name="Women's European Championships 2026",
            emoji=":athletics:",
        )
        window = build_scan_window(datetime(2026, 8, 10, 18, 0, tzinfo=UK_TZ))

        with patch.object(reminders.requests, "get", return_value=FakeResponse()) as request_get:
            enriched, missing = reminders.apply_athletics_authoritative_start_times(
                [catalogue_market],
                "test-app-key",
            )

        self.assertEqual(missing, [])
        self.assertEqual(enriched[0].event_start_uk.strftime("%Y-%m-%d %H:%M %Z"), "2026-08-11 21:30 BST")
        selected = select_market_reminders(enriched, "Athletics", window)
        self.assertEqual([item.market_id for item in selected], ["1.260973277"])
        self.assertEqual(
            reminder_time(selected[0].event_start_utc, selected[0].lead_minutes).strftime("%Y-%m-%d %H:%M %Z"),
            "2026-08-11 21:25 BST",
        )
        request_get.assert_called_once()
        self.assertEqual(request_get.call_args.kwargs["headers"]["X-Application"], "test-app-key")
        self.assertEqual(request_get.call_args.kwargs["params"]["marketIds"], "1.260973277")

    def test_athletics_market_without_authoritative_time_is_not_enriched(self) -> None:
        class FakeResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self):
                return {"eventTypes": []}

        catalogue_market = reminder(
            "Athletics",
            "event-1",
            datetime(2026, 8, 10, 19, 35, tzinfo=timezone.utc),
            market_id="1.missing",
            market_name="Gold Medal Winner",
            market_type_code="GOLD_MEDAL_WINNER",
        )
        with patch.object(reminders.requests, "get", return_value=FakeResponse()):
            enriched, missing = reminders.apply_athletics_authoritative_start_times(
                [catalogue_market],
                "test-app-key",
            )

        self.assertEqual(enriched, [])
        self.assertEqual(missing, [catalogue_market])

    def test_corrected_athletics_time_supersedes_existing_early_slack_reminder(self) -> None:
        corrected = replace(
            reminder(
                "Athletics",
                "31664995",
                datetime(2026, 8, 11, 20, 30, tzinfo=timezone.utc),
                market_id="1.260973277",
                market_name="Womens 100m Hurdles Gold Medal Winner",
                market_type_code="WINNER",
            ),
            duplicate_by=reminders.DEDUP_MARKET,
        )
        old_post_epoch = int(datetime(2026, 8, 10, 20, 30, tzinfo=UK_TZ).timestamp())
        corrected_post_epoch = int(reminder_time(corrected.event_start_utc).timestamp())
        state = {
            "scheduled": [
                {
                    "duplicate_key": f"Athletics|1.260973277|{old_post_epoch}|C123",
                    "sport": "Athletics",
                    "event_id": "31664995",
                    "market_id": "1.260973277",
                    "scheduled_slack_post_epoch": old_post_epoch,
                    "reminder_offset_minutes": 5,
                    "slack_channel_id": "C123",
                    "slack_scheduled_message_id": "Q-old",
                    "status": "scheduled",
                }
            ]
        }

        superseded = reminders.superseded_scheduled_records(
            state,
            corrected,
            corrected_post_epoch,
            "C123",
            int(datetime(2026, 8, 10, 18, 0, tzinfo=UK_TZ).timestamp()),
        )

        self.assertEqual([record["slack_scheduled_message_id"] for record in superseded], ["Q-old"])

    def test_cycling_winner_market_is_selected_and_side_market_excluded(self) -> None:
        start = datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)
        selected = select_market_reminders(
            [
                reminder("Cycling", "event-1", start, market_id="1.cyc-win", market_type_code="WINNER"),
                reminder("Cycling", "event-1", start, market_id="1.cyc-h2h", market_type_code="HEAD_TO_HEAD"),
            ],
            "Cycling",
        )
        self.assertEqual([item.market_id for item in selected], ["1.cyc-win"])
        self.assertEqual(selected[0].selection_reason, "cycling_main_winner:market_type_code=WINNER")

    def test_cycling_observed_outright_winner_market_is_selected(self) -> None:
        item = reminder(
            "Cycling",
            "tour-event",
            datetime(2026, 7, 16, 11, 30, tzinfo=timezone.utc),
            market_id="1.246865525",
            market_name="Tour Winner",
            market_type_code="OUTRIGHT_WINNER",
            event_name="Tour de France",
        )
        self.assertEqual([market.market_id for market in select_market_reminders([item], "Cycling")], ["1.246865525"])

    def test_cycling_stage_winner_side_market_is_excluded_for_tour_event(self) -> None:
        item = reminder(
            "Cycling",
            "tour-event",
            datetime(2026, 7, 16, 11, 30, tzinfo=timezone.utc),
            market_id="1.stage",
            market_name="Stage 10 Winner",
            market_type_code="STAGE_WINNER",
            event_name="Tour de France",
        )
        self.assertEqual(select_market_reminders([item], "Cycling"), [])

    def test_cycling_stage_winner_is_selected_for_specific_stage_event(self) -> None:
        item = reminder(
            "Cycling",
            "stage-event",
            datetime(2026, 7, 16, 11, 30, tzinfo=timezone.utc),
            market_id="1.stage",
            market_name="Tour de France Stage 10 Winner",
            market_type_code="STAGE_WINNER",
            event_name="Tour de France Stage 10",
        )
        self.assertEqual([market.market_id for market in select_market_reminders([item], "Cycling")], ["1.stage"])

    def test_cycling_classification_markets_are_excluded(self) -> None:
        start = datetime(2026, 7, 16, 11, 30, tzinfo=timezone.utc)
        markets = [
            reminder("Cycling", "tour", start, market_id="1.points", market_name="Points Classification", market_type_code="POINTS_WINNER"),
            reminder("Cycling", "tour", start, market_id="1.team", market_name="Team Classification", market_type_code="TEAM_WINNER"),
            reminder("Cycling", "tour", start, market_id="1.young", market_name="Young Rider Classification", market_type_code="YOUNG_RIDER_WINNER"),
        ]
        self.assertEqual(select_market_reminders(markets, "Cycling"), [])

    def test_cycling_match_bets_and_head_to_heads_are_excluded(self) -> None:
        start = datetime(2026, 7, 16, 11, 30, tzinfo=timezone.utc)
        markets = [
            reminder("Cycling", "tour", start, market_id="1.match", market_name="Rider Match Bet", market_type_code="MATCH_BET"),
            reminder("Cycling", "tour", start, market_id="1.h2h", market_name="Rider Head To Head", market_type_code="HEAD_TO_HEAD"),
        ]
        self.assertEqual(select_market_reminders(markets, "Cycling"), [])

    def test_cycling_catalogue_has_no_api_side_market_type_filter(self) -> None:
        self.assertIsNone(reminders.api_market_type_filter_for_sport("Cycling", reminders.SPORT_RULE_WINNER_MARKETS))
        self.assertIsNone(reminders.api_market_type_filter_for_sport("Golf", reminders.SPORT_RULE_WINNER_MARKETS))

    def test_cricket_catalogue_uses_api_side_toss_market_filter(self) -> None:
        self.assertEqual(
            reminders.api_market_type_filter_for_sport("Cricket", reminders.SPORT_RULE_CRICKET_TOSS),
            (reminders.TO_WIN_THE_TOSS,),
        )

    def test_cycling_winner_reminder_is_five_minutes_before_start_and_dedupes_by_market_id(self) -> None:
        item = select_market_reminders(
            [reminder("Cycling", "event-1", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc), market_id="1.cyc", market_type_code="WINNER")],
            "Cycling",
        )[0]
        post_epoch = int(reminder_time(item.event_start_utc, item.lead_minutes).timestamp())
        self.assertEqual(reminder_time(item.event_start_utc, item.lead_minutes).strftime("%H:%M %Z"), "14:55 BST")
        self.assertEqual(duplicate_key(item, post_epoch, "C123"), "Cycling|1.cyc|1783605300|C123")

    def test_golf_winner_market_is_selected_and_top_five_excluded(self) -> None:
        start = datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)
        selected = select_market_reminders(
            [
                reminder("Golf", "event-1", start, market_id="1.golf-win", market_type_code="WINNER"),
                reminder("Golf", "event-1", start, market_id="1.golf-top5", market_type_code="TOP_5", market_name="Top 5 Finish"),
            ],
            "Golf",
        )
        self.assertEqual([item.market_id for item in selected], ["1.golf-win"])
        self.assertEqual(selected[0].selection_reason, "golf_main_outright:market_type_code=WINNER")

    def test_golf_outright_winner_market_is_selected(self) -> None:
        item = reminder(
            "Golf",
            "golf-event",
            datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc),
            market_id="1.golf-outright",
            market_name="Tournament Winner",
            market_type_code="OUTRIGHT_WINNER",
        )
        selected = select_market_reminders([item], "Golf")
        self.assertEqual([market.market_id for market in selected], ["1.golf-outright"])

    def test_winner_regular_and_tournament_winner_are_main_outrights(self) -> None:
        start = datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)
        regular = reminder(
            "Golf",
            "regular",
            start,
            market_name="Winner - Regular",
            market_type_code="WINNER",
        )
        tournament = reminder(
            "Basketball",
            "tournament",
            start,
            market_name="Tournament Winner",
            market_type_code="TOURNAMENT_WINNER",
        )
        self.assertTrue(reminders.outright_market_selection(regular)[0])
        self.assertTrue(reminders.outright_market_selection(tournament)[0])

    def test_all_sports_outright_side_markets_are_excluded(self) -> None:
        start = datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)
        side_markets = [
            reminder("Cycling", "stage", start, market_name="Stage 10 Winner", market_type_code="STAGE_WINNER"),
            reminder("Cycling", "points", start, market_name="Points Winner", market_type_code="POINTS_WINNER"),
            reminder("Esports", "map", start, market_name="Map 1 Winner", market_type_code="MAP_WINNER"),
            reminder("Boxing", "round", start, market_name="Round 3 Winner", market_type_code="ROUND_WINNER"),
            reminder("Cycling", "team", start, market_name="Team Winner", market_type_code="TEAM_WINNER"),
            reminder("Golf", "without", start, market_name="Winner Without Favourite", market_type_code="WINNER"),
        ]
        self.assertTrue(all(not reminders.outright_market_selection(item)[0] for item in side_markets))

    def test_all_listed_rugby_competitions_are_blacklisted(self) -> None:
        start = datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)
        competitions = {
            "Rugby League": (
                "NRL",
                "Womens NRL",
                "State of Origin",
                "Womens State of Origin",
                "All-Stars match",
                "Women's All-Stars",
                "NRL 9s",
                "Womens NRL 9s",
            ),
            "Rugby Union": ("Super Rugby - Pacific", "NZ NPC", "Bledisloe Cup"),
        }
        for sport, names in competitions.items():
            for competition_name in names:
                with self.subTest(sport=sport, competition=competition_name):
                    item = reminder(sport, competition_name, start, competition_name=competition_name)
                    self.assertTrue(reminders.is_disallowed_market(item))
                    self.assertEqual(select_reminders([item], reminders.SPORT_RULE_ALL), [])

    def test_other_rugby_competitions_are_not_blacklisted(self) -> None:
        item = reminder(
            "Rugby Union",
            "six-nations",
            datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc),
            competition_name="Six Nations",
        )
        self.assertFalse(reminders.is_disallowed_market(item))

    def test_football_outrights_are_blacklisted_for_soccer_and_football_names(self) -> None:
        start = datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)
        for sport in ("Soccer", "Football"):
            with self.subTest(sport=sport):
                item = reminder(
                    sport,
                    f"{sport}-outright",
                    start,
                    market_name="Tournament Winner",
                    market_type_code="OUTRIGHT_WINNER",
                )
                self.assertTrue(reminders.is_disallowed_market(item))
                self.assertEqual(
                    reminders.outright_market_selection(item),
                    (False, "disallowed_football_outright"),
                )

    def test_football_match_odds_are_not_blacklisted(self) -> None:
        item = reminder("Soccer", "match", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc))
        self.assertFalse(reminders.is_disallowed_market(item))

    def test_au_market_is_excluded_from_event_and_market_selection(self) -> None:
        item = reminder(
            "Golf",
            "au-event",
            datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc),
            market_name="Tournament Winner",
            market_type_code="OUTRIGHT_WINNER",
            country_code="AU",
        )
        self.assertEqual(select_market_reminders([item], "Golf"), [])
        self.assertEqual(select_reminders([item], reminders.SPORT_RULE_ALL), [])
        self.assertEqual(
            reminders.outright_market_selection(item),
            (False, "disallowed_australian_market:country_code=AU"),
        )

    def test_australian_wallet_market_is_excluded_when_country_is_wrong(self) -> None:
        item = reminder(
            "Golf",
            "wallet-event",
            datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc),
            country_code="GB",
            wallet="Australian wallet",
        )
        self.assertEqual(reminders.australian_market_reason(item), "wallet=Australian wallet")
        self.assertEqual(select_market_reminders([item], "Golf"), [])

    def test_australian_regulator_market_is_excluded_when_country_and_wallet_are_wrong(self) -> None:
        item = reminder(
            "Golf",
            "regulator-event",
            datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc),
            country_code="GB",
            wallet="UK wallet",
            regulator="NORTHERN TERRITORY RACING AND WAGERING COMMISSION",
        )
        self.assertIn("regulator=", reminders.australian_market_reason(item))
        self.assertEqual(select_market_reminders([item], "Golf"), [])

    def test_australian_exchange_market_id_is_excluded_as_final_fallback(self) -> None:
        item = reminder(
            "Golf",
            "exchange-event",
            datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc),
            market_id="2.123456789",
            country_code="GB",
            wallet="",
            regulator="",
        )
        self.assertEqual(
            reminders.australian_market_reason(item),
            "australian_exchange_market_id=2.123456789",
        )
        self.assertEqual(select_market_reminders([item], "Golf"), [])

    def test_australian_market_rules_are_used_when_other_metadata_is_wrong(self) -> None:
        item = reminder(
            "Golf",
            "rules-event",
            datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc),
            country_code="GB",
            wallet="UK wallet",
            regulator="MALTA LOTTERIES AND GAMBLING AUTHORITY",
            market_rules='<img src="ausflag.jpg"> This Australian market is offered by Betfair Pty Ltd.',
        )
        self.assertEqual(reminders.australian_market_reason(item), "market_rules_marker=australian market")
        self.assertEqual(select_market_reminders([item], "Golf"), [])

    def test_catalogue_country_code_is_captured(self) -> None:
        catalogue = {
            "event": {"id": "event-1", "name": "Australian Event", "countryCode": "AU"},
            "event_type": {"id": "3"},
            "competition": {"id": "c1", "name": "Competition"},
            "market_id": "1.au",
            "market_name": "Tournament Winner",
            "market_start_time": "2026-07-09T14:00:00Z",
            "description": {
                "marketType": "OUTRIGHT_WINNER",
                "wallet": "Australian wallet",
                "regulator": "TASMANIAN GAMING COMMISSION",
            },
        }
        item = reminders.catalogue_to_reminder(catalogue, "Golf", ":golf:", "3")
        self.assertIsNotNone(item)
        self.assertEqual(item.country_code, "AU")
        self.assertEqual(item.wallet, "Australian wallet")
        self.assertEqual(item.regulator, "TASMANIAN GAMING COMMISSION")

    def test_market_country_allowlist_excludes_au(self) -> None:
        self.assertEqual(
            reminders.allowed_market_country_codes(["GB", "au", "IE", "AU", ""]),
            ("GB", "IE"),
        )

    def test_market_country_codes_are_discovered_from_betfair_filter_results(self) -> None:
        class FakeBetting:
            def list_countries(self, **kwargs):
                self.filter = kwargs["filter"]
                return [
                    {"countryCode": "AU", "marketCount": 4},
                    {"countryCode": "GB", "marketCount": 12},
                ]

        betting = FakeBetting()
        client = type("FakeClient", (), {"betting": betting})()
        window = build_scan_window(datetime(2026, 7, 9, 12, 0, tzinfo=UK_TZ))
        self.assertEqual(reminders.list_market_country_codes(client, window), ("AU", "GB"))
        self.assertIn("marketStartTime", betting.filter)

    def test_market_catalogue_query_applies_non_au_country_allowlist(self) -> None:
        class FakeBetting:
            def list_market_catalogue(self, **kwargs):
                self.filter = kwargs["filter"]
                return []

        betting = FakeBetting()
        client = type("FakeClient", (), {"betting": betting})()
        window = build_scan_window(datetime(2026, 7, 9, 12, 0, tzinfo=UK_TZ))
        reminders.list_market_catalogues(
            client,
            "3",
            window,
            market_type_codes=("WINNER",),
            market_countries=("GB", "IE"),
        )
        self.assertEqual(betting.filter["marketCountries"], ["GB", "IE"])
        self.assertNotIn("AU", betting.filter["marketCountries"])

    def test_market_catalogue_too_much_data_retries_smaller_windows_and_dedupes(self) -> None:
        class FakeBetting:
            def __init__(self):
                self.filters = []

            def list_market_catalogue(self, **kwargs):
                self.filters.append(kwargs["filter"])
                if len(self.filters) == 1:
                    raise RuntimeError("TOO_MUCH_DATA")
                if len(self.filters) == 2:
                    return [{"market_id": "1.shared"}]
                return [{"market_id": "1.shared"}, {"market_id": "1.right"}]

        betting = FakeBetting()
        client = type("FakeClient", (), {"betting": betting})()
        window = build_scan_window(datetime(2026, 7, 9, 12, 0, tzinfo=UK_TZ))
        catalogues = reminders.list_market_catalogues(
            client,
            "4",
            window,
            market_type_codes=(reminders.TO_WIN_THE_TOSS,),
            market_countries=("GB",),
            sport_name="Cricket",
        )

        self.assertEqual([item["market_id"] for item in catalogues], ["1.shared", "1.right"])
        self.assertEqual(len(betting.filters), 3)
        self.assertNotEqual(
            betting.filters[1]["marketStartTime"]["to"],
            betting.filters[2]["marketStartTime"]["to"],
        )

    def test_market_catalogue_too_much_data_retry_is_bounded(self) -> None:
        class FakeBetting:
            def __init__(self):
                self.calls = 0

            def list_market_catalogue(self, **kwargs):
                self.calls += 1
                raise RuntimeError("TOO_MUCH_DATA")

        betting = FakeBetting()
        client = type("FakeClient", (), {"betting": betting})()
        window = build_scan_window(datetime(2026, 7, 9, 12, 0, tzinfo=UK_TZ))
        with self.assertRaisesRegex(RuntimeError, "TOO_MUCH_DATA"):
            reminders.list_market_catalogues(
                client,
                "4",
                window,
                market_type_codes=None,
                market_countries=("GB",),
                sport_name="Cricket",
                max_split_depth=1,
            )
        self.assertEqual(betting.calls, 2)

    def test_sport_catalogue_failure_is_recorded_and_skipped(self) -> None:
        stats = reminders.RunStats()
        window = build_scan_window(datetime(2026, 7, 9, 12, 0, tzinfo=UK_TZ))
        with patch.object(reminders, "list_market_catalogues", side_effect=RuntimeError("API unavailable")):
            catalogues = reminders.list_sport_market_catalogues(
                object(),
                "Cricket",
                reminders.SPORT_RULE_CRICKET_TOSS,
                "4",
                window,
                ("GB",),
                stats,
            )
        self.assertIsNone(catalogues)
        self.assertEqual(stats.failures, 1)

    def test_all_sports_discovery_selects_unknown_sport_and_excludes_au(self) -> None:
        start = "2026-07-09T14:00:00Z"
        catalogues = [
            {
                "event": {"id": "gb-event", "name": "World Championship", "countryCode": "GB"},
                "event_type": {"id": "99"},
                "competition": {},
                "market_id": "1.gb",
                "market_name": "Tournament Winner",
                "market_start_time": start,
                "description": {"marketType": "TOURNAMENT_WINNER"},
            },
            {
                "event": {"id": "au-event", "name": "Australian Championship", "countryCode": "AU"},
                "event_type": {"id": "99"},
                "competition": {},
                "market_id": "1.au",
                "market_name": "Tournament Winner",
                "market_start_time": start,
                "description": {"marketType": "TOURNAMENT_WINNER"},
            },
        ]
        window = build_scan_window(datetime(2026, 7, 9, 12, 0, tzinfo=UK_TZ))
        with patch.object(reminders, "list_market_type_codes", return_value=("MATCH_ODDS", "TOURNAMENT_WINNER")), patch.object(
            reminders,
            "list_market_catalogues",
            return_value=catalogues,
        ):
            selected, aus_excluded = reminders.discover_all_sports_outright_reminders(
                object(),
                {"New Sport": "99"},
                window,
            )
        self.assertEqual([item.market_id for item in selected], ["1.gb"])
        self.assertEqual(selected[0].sport, "New Sport")
        self.assertEqual(selected[0].emoji, reminders.GENERIC_OUTRIGHT_EMOJI)
        self.assertEqual(aus_excluded, {"1.au"})

    def test_all_sports_discovery_does_not_scan_special_bets(self) -> None:
        window = build_scan_window(datetime(2026, 7, 9, 12, 0, tzinfo=UK_TZ))
        with patch.object(reminders, "list_market_type_codes") as list_market_types, patch.object(
            reminders,
            "list_market_catalogues",
        ) as list_catalogues:
            selected, aus_excluded = reminders.discover_all_sports_outright_reminders(
                object(),
                {"Special Bets": "10"},
                window,
            )
        self.assertEqual(selected, [])
        self.assertEqual(aus_excluded, set())
        list_market_types.assert_not_called()
        list_catalogues.assert_not_called()

    def test_all_sports_discovery_does_not_scan_football_outrights(self) -> None:
        window = build_scan_window(datetime(2026, 7, 9, 12, 0, tzinfo=UK_TZ))
        with patch.object(reminders, "list_market_type_codes") as list_market_types, patch.object(
            reminders,
            "list_market_catalogues",
        ) as list_catalogues:
            selected, blacklist_excluded = reminders.discover_all_sports_outright_reminders(
                object(),
                {"Soccer": "1"},
                window,
            )
        self.assertEqual(selected, [])
        self.assertEqual(blacklist_excluded, set())
        list_market_types.assert_not_called()
        list_catalogues.assert_not_called()

    def test_all_sports_discovery_leaves_athletics_to_authoritative_scan(self) -> None:
        window = build_scan_window(datetime(2026, 8, 10, 18, 0, tzinfo=UK_TZ))
        with patch.object(reminders, "list_market_type_codes") as list_market_types, patch.object(
            reminders,
            "list_market_catalogues",
        ) as list_catalogues:
            selected, blacklist_excluded = reminders.discover_all_sports_outright_reminders(
                object(),
                {"Athletics": "3988"},
                window,
            )

        self.assertEqual(selected, [])
        self.assertEqual(blacklist_excluded, set())
        list_market_types.assert_not_called()
        list_catalogues.assert_not_called()

    def test_golf_winner_reminder_is_five_minutes_before_start_and_dedupes_by_market_id(self) -> None:
        item = select_market_reminders(
            [reminder("Golf", "event-1", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc), market_id="1.golf", market_type_code="WINNER")],
            "Golf",
        )[0]
        post_epoch = int(reminder_time(item.event_start_utc, item.lead_minutes).timestamp())
        self.assertEqual(reminder_time(item.event_start_utc, item.lead_minutes).strftime("%H:%M %Z"), "14:55 BST")
        self.assertEqual(duplicate_key(item, post_epoch, "C123"), "Golf|1.golf|1783605300|C123")

    def test_rugby_league_first_try_scorer_appends_tip_with_stream(self) -> None:
        selected = [reminders.SelectedReminder(reminder("Rugby League", "event-1", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)))]
        first_try = reminders.first_try_scorer_by_event(
            [reminder("Rugby League", "event-1", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc), market_id="1.fts", market_type_code="FIRST_TRY_SCORER")]
        )
        enriched = reminders.apply_rugby_first_try_flags(selected, first_try)[0].reminder
        self.assertTrue(enriched.has_first_try_scorer)
        self.assertTrue(reminders.format_slack_text(enriched).endswith(" TIP with stream"))

    def test_rugby_union_first_try_scorer_name_fallback_appends_tip_with_stream(self) -> None:
        selected = [reminders.SelectedReminder(reminder("Rugby Union", "event-1", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)))]
        first_try = reminders.first_try_scorer_by_event(
            [reminder("Rugby Union", "event-1", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc), market_id="1.fts", market_name="First Try Scorer")]
        )
        enriched = reminders.apply_rugby_first_try_flags(selected, first_try)[0].reminder
        self.assertTrue(enriched.has_first_try_scorer)
        self.assertTrue(reminders.format_slack_text(enriched).endswith(" TIP with stream"))

    def test_rugby_without_first_try_scorer_keeps_original_message(self) -> None:
        selected = [reminders.SelectedReminder(reminder("Rugby Union", "event-1", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)))]
        enriched = reminders.apply_rugby_first_try_flags(selected, {})[0].reminder
        self.assertFalse(enriched.has_first_try_scorer)
        self.assertNotIn("TIP with stream", reminders.format_slack_text(enriched))

    def test_anytime_try_scorer_does_not_trigger_tip_with_stream_or_create_reminder(self) -> None:
        selected = [reminders.SelectedReminder(reminder("Rugby Union", "event-1", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)))]
        first_try = reminders.first_try_scorer_by_event(
            [reminder("Rugby Union", "event-1", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc), market_id="1.any", market_name="Anytime Try Scorer")]
        )
        enriched = reminders.apply_rugby_first_try_flags(selected, first_try)
        self.assertEqual(first_try, {})
        self.assertEqual(len(enriched), 1)
        self.assertNotIn("TIP with stream", reminders.format_slack_text(enriched[0].reminder))

    def test_batched_cached_first_try_lookup_maps_per_event(self) -> None:
        first_try = reminders.first_try_scorer_by_event(
            [
                reminder("Rugby Union", "event-2", datetime(2026, 7, 9, 15, 0, tzinfo=timezone.utc), market_id="1.fts-2", market_name="First Try Scorer"),
                reminder("Rugby Union", "event-1", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc), market_id="1.any", market_name="Anytime Try Scorer"),
            ]
        )
        self.assertEqual(set(first_try), {"event-2"})
        self.assertEqual(first_try["event-2"].market_id, "1.fts-2")

    def test_cricket_exact_to_win_the_toss_market_is_selected_and_others_excluded(self) -> None:
        start = datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)
        selected = select_market_reminders(
            [
                reminder("Cricket", "event-1", start, market_id="1.toss", market_name="To Win the Toss"),
                reminder("Cricket", "event-1", start, market_id="1.match", market_name="Match Odds", market_type_code="MATCH_ODDS"),
            ],
            "Cricket",
        )
        self.assertEqual([item.market_id for item in selected], ["1.toss"])
        self.assertEqual(selected[0].selection_reason, "cricket_to_win_toss")

    def test_cricket_toss_reminder_is_forty_minutes_before_start(self) -> None:
        item = select_market_reminders(
            [
                reminder(
                    "Cricket",
                    "event-1",
                    datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc),
                    competition_id="other-competition",
                    market_id="1.toss",
                    market_name="To Win the Toss",
                )
            ],
            "Cricket",
        )[0]
        self.assertEqual(item.lead_minutes, 40)
        self.assertEqual(reminder_time(item.event_start_utc, item.lead_minutes).strftime("%H:%M %Z"), "14:20 BST")

    def test_hundred_competitions_toss_reminders_are_fifty_five_minutes_before_start(self) -> None:
        start = datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc)
        for competition_id in ("12267948", "12351359"):
            with self.subTest(competition_id=competition_id):
                item = select_market_reminders(
                    [
                        reminder(
                            "Cricket",
                            f"event-{competition_id}",
                            start,
                            competition_id=competition_id,
                            market_id=f"1.toss-{competition_id}",
                            market_name="To Win the Toss",
                        )
                    ],
                    "Cricket",
                )[0]
                self.assertEqual(item.lead_minutes, 55)
                self.assertEqual(
                    reminder_time(item.event_start_utc, item.lead_minutes).strftime("%H:%M %Z"),
                    "14:05 BST",
                )

    def test_cricket_slack_message_uses_vs_and_market_id_not_event_id(self) -> None:
        item = select_market_reminders(
            [
                reminder(
                    "Cricket",
                    "event-123",
                    datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc),
                    market_id="213681787",
                    market_name="To Win the Toss",
                    event_name="Team A v Team B",
                    emoji=":cricket:",
                )
            ],
            "Cricket",
        )[0]
        self.assertEqual(reminders.format_slack_text(item), ":cricket: Suspend toss in Team A vs Team B - Market ID: 213681787")
        self.assertNotIn("event-123", reminders.format_slack_text(item))

    def test_cricket_toss_reminder_in_past_can_be_identified_and_dedupes_by_market_id(self) -> None:
        item = select_market_reminders(
            [reminder("Cricket", "event-1", datetime(2026, 7, 9, 14, 0, tzinfo=timezone.utc), market_id="1.toss", market_name="To Win the Toss")],
            "Cricket",
        )[0]
        post_time = reminder_time(item.event_start_utc, item.lead_minutes)
        now_after_post = datetime(2026, 7, 9, 14, 21, tzinfo=UK_TZ)
        self.assertLessEqual(post_time, now_after_post)
        self.assertEqual(duplicate_key(item, int(post_time.timestamp()), "C123"), "Cricket|1.toss|1783603200|C123")

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
        self.assertEqual(config.slack_channel_id, "C07FXG95GQ6")
        self.assertNotIn("slack_channel_id", message)
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

    def test_linux_windows_cert_path_falls_back_to_repo_certs(self) -> None:
        repo_root = ROOT / "runtime" / "output" / f"repo-{uuid.uuid4()}"
        repo_certs = repo_root / "certs"
        repo_certs.mkdir(parents=True, exist_ok=True)
        (repo_certs / "client-2048.crt").write_text("test cert", encoding="utf-8")
        (repo_certs / "client-2048.key").write_text("test key", encoding="utf-8")
        resolved = resolve_betfair_certs_dir(
            r"C:\BetfairScripts\certs",
            repo_root,
            is_windows_host=False,
            ec2_certs_dir=repo_root / "missing-ec2-certs",
        )
        self.assertEqual(resolved, repo_certs.resolve())

    def test_linux_absolute_cert_path_resolves_directly(self) -> None:
        repo_root = ROOT / "runtime" / "output" / f"repo-{uuid.uuid4()}"
        configured_certs = "/opt/betfair-scripts/certs"
        with patch.object(reminders, "missing_cert_files", return_value=[]):
            resolved = resolve_betfair_certs_dir(
                configured_certs,
                repo_root,
                is_windows_host=False,
                ec2_certs_dir=repo_root / "missing-ec2-certs",
            )
        self.assertEqual(resolved, Path(configured_certs))

    def test_relative_cert_path_resolves_against_repo_root(self) -> None:
        repo_root = ROOT / "runtime" / "output" / f"repo-{uuid.uuid4()}"
        repo_certs = repo_root / "certs"
        repo_certs.mkdir(parents=True, exist_ok=True)
        (repo_certs / "client-2048.crt").write_text("test cert", encoding="utf-8")
        (repo_certs / "client-2048.key").write_text("test key", encoding="utf-8")
        resolved = resolve_betfair_certs_dir(
            "certs",
            repo_root,
            is_windows_host=False,
            ec2_certs_dir=repo_root / "missing-ec2-certs",
        )
        self.assertEqual(resolved, repo_certs.resolve())

    def test_linux_windows_cert_path_uses_valid_cert_alias(self) -> None:
        repo_root = ROOT / "runtime" / "output" / f"repo-{uuid.uuid4()}"
        alias_certs = repo_root / "alias-certs"
        alias_certs.mkdir(parents=True, exist_ok=True)
        (alias_certs / "client-2048.crt").write_text("test cert", encoding="utf-8")
        (alias_certs / "client-2048.key").write_text("test key", encoding="utf-8")
        resolved = resolve_betfair_certs_dir(
            (r"C:\BetfairScripts\certs", "alias-certs"),
            repo_root,
            is_windows_host=False,
            ec2_certs_dir=repo_root / "missing-ec2-certs",
        )
        self.assertEqual(resolved, alias_certs.resolve())

    def test_missing_cert_files_raise_clear_error(self) -> None:
        repo_root = ROOT / "runtime" / "output" / f"repo-{uuid.uuid4()}"
        configured_certs = repo_root / "configured-certs"
        configured_certs.mkdir(parents=True, exist_ok=True)
        with self.assertRaises(FileNotFoundError) as context:
            resolve_betfair_certs_dir(
                str(configured_certs),
                repo_root,
                is_windows_host=False,
                ec2_certs_dir=repo_root / "missing-ec2-certs",
            )
        message = str(context.exception)
        self.assertIn("Betfair certificate files not found", message)
        self.assertIn("client-2048.crt", message)
        self.assertIn("client-2048.key", message)


if __name__ == "__main__":
    unittest.main()
