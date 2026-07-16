from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import unittest
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEST_TEMP_ROOT = ROOT / "runtime" / "output"

from app import cricket_fixture_api as fixture_api  # noqa: E402


def sample_payload(target_date: str = "2026-07-09") -> dict[str, object]:
    return {
        "source": "decimal",
        "target_date": target_date,
        "timezone": "Europe/London",
        "generated_at": "2026-07-09T16:55:55+01:00",
        "fixture_count": 1,
        "fixtures": [
            {
                "match_name": "Zimbabwe v Bangladesh",
                "competition": "Tour Match 2",
                "venue": "Harare Sports Club",
                "start_time": "2026-07-09T08:30:00+01:00",
                "start_time_uk": "2026-07-09 08:30 BST",
                "source": "decimal",
                "event_id": "1000167150LIVE2026",
            }
        ],
    }


def sample_upcoming_payload() -> dict[str, object]:
    payload = sample_payload()
    payload.pop("target_date")
    payload["scope"] = "all_upcoming"
    payload["sections"] = ["this_month", "next_month_and_beyond"]
    return payload


def new_test_output_dir() -> Path:
    output_dir = TEST_TEMP_ROOT / f"fixture-api-{uuid.uuid4()}"
    output_dir.mkdir(parents=True)
    return output_dir


class CricketFixtureApiTests(unittest.TestCase):
    def test_api_key_requires_bearer_scheme_and_exact_key(self) -> None:
        self.assertTrue(fixture_api.api_key_is_valid("Bearer secret-value", "secret-value"))
        self.assertTrue(fixture_api.api_key_is_valid("bearer secret-value", "secret-value"))
        self.assertFalse(fixture_api.api_key_is_valid("Bearer wrong", "secret-value"))
        self.assertFalse(fixture_api.api_key_is_valid("Basic secret-value", "secret-value"))
        self.assertFalse(fixture_api.api_key_is_valid("Bearer secret-value", ""))

    def test_api_auth_dependency_fails_closed(self) -> None:
        credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials="secret-value")
        with patch.object(fixture_api, "cricket_fixture_api_key", return_value="secret-value"):
            self.assertIsNone(fixture_api.require_cricket_fixture_api_key(credentials))
            with self.assertRaises(HTTPException) as invalid_context:
                fixture_api.require_cricket_fixture_api_key(None)
        self.assertEqual(invalid_context.exception.status_code, 401)
        with patch.object(fixture_api, "cricket_fixture_api_key", return_value=""):
            with self.assertRaises(HTTPException) as missing_context:
                fixture_api.require_cricket_fixture_api_key(credentials)
        self.assertEqual(missing_context.exception.status_code, 503)

    def test_resolve_target_date_aliases_and_iso_date(self) -> None:
        today = date(2026, 7, 16)
        self.assertEqual(fixture_api.resolve_target_date("today", today), today)
        self.assertEqual(fixture_api.resolve_target_date("tomorrow", today), date(2026, 7, 17))
        self.assertEqual(fixture_api.resolve_target_date("2026-07-09", today), date(2026, 7, 9))
        with self.assertRaises(fixture_api.FixtureDataError) as context:
            fixture_api.resolve_target_date("09-07-2026", today)
        self.assertEqual(context.exception.code, "invalid_fixture_date")
        with self.assertRaises(fixture_api.FixtureDataError):
            fixture_api.resolve_target_date("20260709", today)

    def test_loads_valid_cache_and_lists_available_dates(self) -> None:
        output_dir = new_test_output_dir()
        payload = sample_payload()
        (output_dir / "decimal_cricket_fixtures_2026-07-09.json").write_text(
            json.dumps(payload), encoding="utf-8"
        )
        (output_dir / "unrelated.json").write_text("{}", encoding="utf-8")
        with patch.object(fixture_api, "OUTPUT_DIR", output_dir):
            self.assertEqual(fixture_api.load_fixture_payload(date(2026, 7, 9)), payload)
            self.assertEqual(fixture_api.available_fixture_dates(), ["2026-07-09"])

    def test_missing_cache_returns_structured_error(self) -> None:
        output_dir = new_test_output_dir()
        with patch.object(fixture_api, "OUTPUT_DIR", output_dir):
            response = asyncio.run(fixture_api.cricket_fixtures("2026-07-09"))
        self.assertEqual(response.status_code, 404)
        body = json.loads(response.body)
        self.assertEqual(body["detail"]["code"], "fixture_data_not_found")
        self.assertEqual(body["detail"]["target_date"], "2026-07-09")

    def test_full_payload_and_individual_fixture_responses(self) -> None:
        output_dir = new_test_output_dir()
        payload = sample_payload()
        (output_dir / "decimal_cricket_fixtures_2026-07-09.json").write_text(
            json.dumps(payload), encoding="utf-8"
        )
        with patch.object(fixture_api, "OUTPUT_DIR", output_dir):
            full_response = asyncio.run(fixture_api.cricket_fixtures("2026-07-09"))
            item_response = asyncio.run(
                fixture_api.cricket_fixture("2026-07-09", "1000167150LIVE2026")
            )
            missing_response = asyncio.run(fixture_api.cricket_fixture("2026-07-09", "missing"))
        self.assertEqual(json.loads(full_response.body), payload)
        self.assertEqual(json.loads(item_response.body), payload["fixtures"][0])
        self.assertEqual(missing_response.status_code, 404)
        self.assertEqual(json.loads(missing_response.body)["detail"]["code"], "fixture_not_found")

    def test_all_upcoming_payload_and_individual_fixture_responses(self) -> None:
        output_dir = new_test_output_dir()
        payload = sample_upcoming_payload()
        (output_dir / fixture_api.ALL_FIXTURE_FILE_NAME).write_text(json.dumps(payload), encoding="utf-8")
        with patch.object(fixture_api, "OUTPUT_DIR", output_dir):
            full_response = asyncio.run(fixture_api.all_cricket_fixtures())
            item_response = asyncio.run(fixture_api.all_cricket_fixture("1000167150LIVE2026"))
            missing_response = asyncio.run(fixture_api.all_cricket_fixture("missing"))
        self.assertEqual(json.loads(full_response.body), payload)
        self.assertEqual(full_response.headers["X-Fixture-Scope"], "all_upcoming")
        self.assertEqual(json.loads(item_response.body), payload["fixtures"][0])
        self.assertEqual(missing_response.status_code, 404)

    def test_missing_all_upcoming_cache_returns_structured_error(self) -> None:
        output_dir = new_test_output_dir()
        with patch.object(fixture_api, "OUTPUT_DIR", output_dir):
            response = asyncio.run(fixture_api.all_cricket_fixtures())
        self.assertEqual(response.status_code, 404)
        self.assertEqual(json.loads(response.body)["detail"]["code"], "upcoming_fixture_data_not_found")

    def test_stale_header_is_only_applied_to_current_or_future_data(self) -> None:
        payload = sample_payload("2026-07-16")
        payload["generated_at"] = "2026-07-16T08:00:00+01:00"
        with patch.object(fixture_api, "get_setting", return_value="60"):
            current_headers = fixture_api.response_headers(payload, date(2026, 7, 16), date(2026, 7, 16))
            historical_headers = fixture_api.response_headers(payload, date(2026, 7, 15), date(2026, 7, 16))
        self.assertEqual(current_headers["X-Fixture-Data-Stale"], "true")
        self.assertIn("Warning", current_headers)
        self.assertEqual(historical_headers["X-Fixture-Data-Stale"], "false")

    def test_payload_age_uses_timezone_aware_generated_time(self) -> None:
        payload = sample_payload()
        age = fixture_api.payload_age_minutes(payload, datetime(2026, 7, 9, 16, 55, 55, tzinfo=timezone.utc))
        self.assertEqual(age, 60.0)

    def test_refresh_service_collects_today_and_tomorrow(self) -> None:
        async def run_refresh() -> fixture_api.FixtureRefreshService:
            service = fixture_api.FixtureRefreshService()
            service._refresh_date = AsyncMock(return_value="ok")
            service._refresh_all = AsyncMock(return_value="ok")
            await service.refresh()
            return service

        service = asyncio.run(run_refresh())
        snapshot = service.snapshot()
        self.assertFalse(snapshot["running"])
        self.assertEqual(list(snapshot["last_results"].values()), ["ok", "ok", "ok"])
        self.assertTrue(snapshot["last_success"])
        self.assertEqual(service._refresh_date.await_count, 2)
        self.assertEqual(service._refresh_all.await_count, 1)


class DecimalFixtureMetadataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        checker_path = ROOT / "scripts" / "exc-cric-time-check" / "betfair_decimal_time_checker.py"
        spec = importlib.util.spec_from_file_location("fixture_api_test_checker", checker_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Could not load {checker_path}")
        cls.checker = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = cls.checker
        spec.loader.exec_module(cls.checker)
        fetcher_path = ROOT / "scripts" / "exc-cric-time-check" / "decimal_fixture_json_fetcher.py"
        fetcher_spec = importlib.util.spec_from_file_location("fixture_api_test_fetcher", fetcher_path)
        if fetcher_spec is None or fetcher_spec.loader is None:
            raise RuntimeError(f"Could not load {fetcher_path}")
        cls.fetcher = importlib.util.module_from_spec(fetcher_spec)
        sys.modules[fetcher_spec.name] = cls.fetcher
        fetcher_spec.loader.exec_module(cls.fetcher)

    def test_decimal_row_is_enriched_for_api_output(self) -> None:
        row = {
            "match_id": "1000167150LIVE2026",
            "display_name": "Zimbabwe v Bangladesh",
            "data_name": "Zimbabwe v Bangladesh, Tour Match 2, from Harare Sports Club",
            "teamA": "Zimbabwe",
            "teamB": "Bangladesh",
            "section": "today",
            "start_time": "2026-07-09T07:30:00.000Z",
            "raw_text": "Zimbabwe Bangladesh 09/07/2026 08:30 A",
            "data_attrs": {"id": "1000167150LIVE2026", "marketcount": "0"},
        }
        fixture, reason = self.checker.parse_decimal_legacy_fixture_payload_with_reason(
            row, date(2026, 7, 9), False
        )
        self.assertEqual(reason, "kept")
        self.assertIsNotNone(fixture)
        self.assertEqual(fixture.competition, "Tour Match 2")
        self.assertEqual(fixture.venue, "Harare Sports Club")
        self.assertEqual(fixture.event_id, "1000167150LIVE2026")
        self.assertEqual(fixture.metadata["data_attrs"]["marketcount"], "0")

    def test_forward_sections_parse_multiple_dates_and_discard_past_rows(self) -> None:
        def row(event_id: str, start_time: str, section: str) -> dict[str, object]:
            return {
                "match_id": event_id,
                "display_name": f"Team {event_id} A v Team {event_id} B",
                "data_name": f"Team {event_id} A v Team {event_id} B, Test Competition from Test Ground",
                "teamA": f"Team {event_id} A",
                "teamB": f"Team {event_id} B",
                "section": section,
                "start_time": start_time,
                "raw_text": f"Team {event_id} A Team {event_id} B",
                "data_attrs": {"id": event_id},
            }

        fixtures = self.checker.build_decimal_legacy_upcoming_fixtures_from_rows(
            [
                row("past", "2026-07-01T12:00:00Z", "this_month"),
                row("this", "2026-07-20T12:00:00Z", "this_month"),
                row("next", "2026-08-15T12:00:00Z", "next_month"),
                row("beyond", "2026-10-10T12:00:00Z", "beyond"),
                row("this", "2026-07-20T12:00:00Z", "this_month"),
            ],
            date(2026, 7, 16),
            False,
        )
        self.assertEqual([fixture.event_id for fixture in fixtures], ["this", "next", "beyond"])
        self.assertEqual(
            [fixture.metadata["section"] for fixture in fixtures],
            ["this_month", "next_month", "beyond"],
        )

    def test_upcoming_payload_reports_detected_decimal_panels(self) -> None:
        fixtures = [
            self.checker.Fixture(
                match_name="A v B",
                competition="Competition",
                start_time=datetime(2026, 7, 20, 12, 0, tzinfo=self.checker.UK_TZ),
                source="decimal",
                event_id="this",
                metadata={"section": "this_month"},
            ),
            self.checker.Fixture(
                match_name="C v D",
                competition="Competition",
                start_time=datetime(2026, 8, 20, 12, 0, tzinfo=self.checker.UK_TZ),
                source="decimal",
                event_id="next",
                metadata={"section": "next_month_and_beyond"},
            ),
        ]
        payload = self.fetcher.build_upcoming_payload(fixtures, self.checker)
        self.assertEqual(payload["scope"], "all_upcoming")
        self.assertEqual(payload["sections"], ["this_month", "next_month_and_beyond"])
        self.assertEqual(payload["fixture_count"], 2)


if __name__ == "__main__":
    unittest.main()
