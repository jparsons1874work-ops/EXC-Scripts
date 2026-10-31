"""Regression coverage for toss markets hidden by discovery filters."""

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import Betfair_Event_Reminders as r


class CricketDiscoveryTests(unittest.TestCase):
    def test_uncategorised_toss_reaches_selection_and_blacklist_still_applies(self):
        window = r.build_scan_window(datetime(2026, 10, 31, 12, tzinfo=timezone.utc))
        base = {
            "market_start_time": "2026-10-31T18:00:00Z",
            "market_name": "To Win the Toss",
            "description": {"marketType": "UNEXPECTED_TOSS_TYPE"},
            "event": {"id": "36259620", "name": "Montreal Warriors v Vancouver Guardians"},
        }
        markets = [
            dict(base, market_id="1.toss"),
            dict(base, market_id="1.au", event=dict(base["event"], countryCode="AU")),
            dict(base, market_id="1.nz", event=dict(base["event"], countryCode="NZ")),
            dict(base, market_id="2.wallet"),
            dict(base, market_id="1.match", market_name="Match Odds",
                 description={"marketType": "MATCH_ODDS"}),
        ]

        class Betting:
            def list_market_catalogue(self, **kwargs):
                # Simulate Betfair's positive-filter behaviour: countryless
                # events and unexpected market types disappear when filtered.
                f = kwargs["filter"]
                return [m for m in markets
                        if ("marketCountries" not in f or
                            m["event"].get("countryCode") in f["marketCountries"])
                        and ("marketTypeCodes" not in f or
                             m["description"]["marketType"] in f["marketTypeCodes"])]

        stats = r.RunStats()
        cats = r.list_sport_market_catalogues(
            SimpleNamespace(betting=Betting()), "Cricket", r.SPORT_RULE_CRICKET_TOSS,
            "4", window, ("GB", "CA"), stats,
        )
        selected = r.select_market_reminders(
            [r.catalogue_to_reminder(c, "Cricket", ":cricket:", "4") for c in cats],
            "Cricket", window,
        )
        self.assertEqual([m.market_id for m in selected], ["1.toss"])
        self.assertEqual(selected[0].lead_minutes, 40)
        self.assertEqual(stats.failures, 0)

    def test_full_catalogue_is_split_to_retrieve_later_markets(self):
        window = r.build_scan_window(datetime(2026, 10, 31, 12, tzinfo=timezone.utc))
        with patch.object(r, "list_market_catalogues_once", side_effect=[
            [{"market_id": str(i)} for i in range(1000)],
            [{"market_id": "1.early"}],
            [{"market_id": "1.late-toss"}],
        ]) as query:
            cats = r.list_market_catalogues(object(), "4", window, market_type_codes=None)
        self.assertEqual([m["market_id"] for m in cats], ["1.early", "1.late-toss"])
        self.assertEqual(query.call_count, 3)

    def test_full_catalogue_at_split_limit_fails_visibly(self):
        window = r.build_scan_window(datetime(2026, 10, 31, 12, tzinfo=timezone.utc))
        with patch.object(r, "list_market_catalogues_once", return_value=[{}] * 1000):
            with self.assertRaisesRegex(RuntimeError, "1000-result limit"):
                r.list_market_catalogues(object(), "4", window, max_split_depth=0)


if __name__ == "__main__":
    unittest.main()
