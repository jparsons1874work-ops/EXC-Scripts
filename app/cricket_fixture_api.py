from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import secrets
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import OUTPUT_DIR, PROJECT_ROOT, child_environment, cricket_fixture_api_key, get_setting


UK_TZ = ZoneInfo("Europe/London")
FIXTURE_FILE_PATTERN = re.compile(r"^decimal_cricket_fixtures_(\d{4}-\d{2}-\d{2})\.json$")
ALL_FIXTURE_FILE_NAME = "decimal_cricket_fixtures_all.json"
FETCHER_PATH = PROJECT_ROOT / "scripts" / "exc-cric-time-check" / "decimal_fixture_json_fetcher.py"
logger = logging.getLogger("uvicorn.error")
router = APIRouter(prefix="/api/v1/cricket-fixtures", tags=["cricket-fixtures"])
bearer_scheme = HTTPBearer(auto_error=False)


class FixtureDataError(RuntimeError):
    def __init__(self, code: str, message: str, status_code: int) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def _positive_int_setting(name: str, default: int, minimum: int = 1) -> int:
    raw_value = get_setting(name, str(default))
    try:
        return max(minimum, int(raw_value))
    except ValueError:
        logger.warning("invalid_integer_setting name=%s value=%r default=%s", name, raw_value, default)
        return default


def _boolean_setting(name: str, default: bool) -> bool:
    raw_value = get_setting(name, "true" if default else "false").lower()
    if raw_value in {"1", "true", "yes", "on"}:
        return True
    if raw_value in {"0", "false", "no", "off"}:
        return False
    logger.warning("invalid_boolean_setting name=%s value=%r default=%s", name, raw_value, default)
    return default


def api_key_is_valid(authorization: str, configured_key: str) -> bool:
    scheme, separator, supplied_key = (authorization or "").partition(" ")
    if not configured_key or not separator or scheme.lower() != "bearer":
        return False
    return secrets.compare_digest(supplied_key.strip(), configured_key)


def require_cricket_fixture_api_key(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> None:
    configured_key = cricket_fixture_api_key()
    if not configured_key:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "fixture_api_not_configured",
                "message": "The cricket fixture API key has not been configured on the server.",
            },
        )
    authorization = f"{credentials.scheme} {credentials.credentials}" if credentials else ""
    if not api_key_is_valid(authorization, configured_key):
        raise HTTPException(
            status_code=401,
            headers={"WWW-Authenticate": "Bearer"},
            detail={
                "code": "invalid_api_key",
                "message": "Supply a valid bearer token in the Authorization header.",
            },
        )


def resolve_target_date(value: str, today: date | None = None) -> date:
    current_day = today or datetime.now(UK_TZ).date()
    normalized = value.strip().lower()
    if normalized == "today":
        return current_day
    if normalized == "tomorrow":
        return current_day + timedelta(days=1)
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalized):
        raise FixtureDataError(
            "invalid_fixture_date",
            "Use today, tomorrow, or a date formatted as YYYY-MM-DD.",
            422,
        )
    try:
        return date.fromisoformat(normalized)
    except ValueError as exc:
        raise FixtureDataError(
            "invalid_fixture_date",
            "Use today, tomorrow, or a date formatted as YYYY-MM-DD.",
            422,
        ) from exc


def fixture_file_path(target_day: date) -> Path:
    return OUTPUT_DIR / f"decimal_cricket_fixtures_{target_day.isoformat()}.json"


def available_fixture_dates() -> list[str]:
    dates: list[str] = []
    if not OUTPUT_DIR.exists():
        return dates
    for path in OUTPUT_DIR.glob("decimal_cricket_fixtures_*.json"):
        match = FIXTURE_FILE_PATTERN.fullmatch(path.name)
        if match:
            dates.append(match.group(1))
    return sorted(set(dates), reverse=True)


def load_fixture_payload(target_day: date) -> dict[str, Any]:
    path = fixture_file_path(target_day)
    if not path.is_file():
        raise FixtureDataError(
            "fixture_data_not_found",
            f"No cached Decimal fixture data is available for {target_day.isoformat()}.",
            404,
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("fixture_cache_read_failed file=%s error=%r", path.name, exc)
        raise FixtureDataError(
            "fixture_data_unavailable",
            f"The cached fixture data for {target_day.isoformat()} could not be read.",
            503,
        ) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("fixtures"), list):
        raise FixtureDataError(
            "fixture_data_invalid",
            f"The cached fixture data for {target_day.isoformat()} has an invalid structure.",
            503,
        )
    if payload.get("target_date") != target_day.isoformat():
        raise FixtureDataError(
            "fixture_data_date_mismatch",
            f"The cached fixture data does not match {target_day.isoformat()}.",
            503,
        )
    return payload


def load_upcoming_fixture_payload() -> dict[str, Any]:
    path = OUTPUT_DIR / ALL_FIXTURE_FILE_NAME
    if not path.is_file():
        raise FixtureDataError(
            "upcoming_fixture_data_not_found",
            "No cached all-upcoming Decimal fixture data is available.",
            404,
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("fixture_cache_read_failed file=%s error=%r", path.name, exc)
        raise FixtureDataError(
            "upcoming_fixture_data_unavailable",
            "The cached all-upcoming fixture data could not be read.",
            503,
        ) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("fixtures"), list):
        raise FixtureDataError(
            "upcoming_fixture_data_invalid",
            "The cached all-upcoming fixture data has an invalid structure.",
            503,
        )
    if payload.get("scope") != "all_upcoming":
        raise FixtureDataError(
            "upcoming_fixture_scope_mismatch",
            "The cached fixture data is not an all-upcoming scan.",
            503,
        )
    return payload


def payload_age_minutes(payload: dict[str, Any], now: datetime | None = None) -> float | None:
    generated_at = payload.get("generated_at")
    if not isinstance(generated_at, str):
        return None
    try:
        generated = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    if generated.tzinfo is None:
        return None
    current_time = now or datetime.now(timezone.utc)
    return max(0.0, (current_time.astimezone(timezone.utc) - generated.astimezone(timezone.utc)).total_seconds() / 60)


def response_headers(payload: dict[str, Any], target_day: date, today: date | None = None) -> dict[str, str]:
    headers = {
        "Cache-Control": "private, max-age=60",
        "X-Fixture-Target-Date": target_day.isoformat(),
    }
    generated_at = payload.get("generated_at")
    if isinstance(generated_at, str):
        headers["X-Fixture-Generated-At"] = generated_at
    age = payload_age_minutes(payload)
    current_day = today or datetime.now(UK_TZ).date()
    max_age = _positive_int_setting("CRICKET_FIXTURE_API_MAX_AGE_MINUTES", 240)
    stale = target_day >= current_day and (age is None or age > max_age)
    headers["X-Fixture-Data-Stale"] = "true" if stale else "false"
    if stale:
        headers["Warning"] = '110 - "Cached cricket fixture data is stale"'
    return headers


def upcoming_response_headers(payload: dict[str, Any]) -> dict[str, str]:
    headers = {
        "Cache-Control": "private, max-age=60",
        "X-Fixture-Scope": "all_upcoming",
    }
    generated_at = payload.get("generated_at")
    if isinstance(generated_at, str):
        headers["X-Fixture-Generated-At"] = generated_at
    age = payload_age_minutes(payload)
    max_age = _positive_int_setting("CRICKET_FIXTURE_API_MAX_AGE_MINUTES", 240)
    stale = age is None or age > max_age
    headers["X-Fixture-Data-Stale"] = "true" if stale else "false"
    if stale:
        headers["Warning"] = '110 - "Cached cricket fixture data is stale"'
    return headers


def fixture_error_response(exc: FixtureDataError, target_day: date | None = None) -> JSONResponse:
    detail: dict[str, Any] = {"code": exc.code, "message": exc.message}
    if target_day is not None:
        detail["target_date"] = target_day.isoformat()
    available_dates = available_fixture_dates()
    if available_dates:
        detail["available_dates"] = available_dates
    return JSONResponse(status_code=exc.status_code, content={"detail": detail})


@router.get("/status", dependencies=[Depends(require_cricket_fixture_api_key)])
async def cricket_fixture_api_status() -> dict[str, Any]:
    return {
        "status": "ok",
        "available_dates": await asyncio.to_thread(available_fixture_dates),
        "all_upcoming_available": (OUTPUT_DIR / ALL_FIXTURE_FILE_NAME).is_file(),
        "refresh": fixture_refresh_service.snapshot(),
    }


@router.get("/all", dependencies=[Depends(require_cricket_fixture_api_key)])
async def all_cricket_fixtures() -> JSONResponse:
    try:
        payload = await asyncio.to_thread(load_upcoming_fixture_payload)
    except FixtureDataError as exc:
        return fixture_error_response(exc)
    return JSONResponse(content=payload, headers=upcoming_response_headers(payload))


@router.get("/all/{event_id}", dependencies=[Depends(require_cricket_fixture_api_key)])
async def all_cricket_fixture(event_id: str) -> JSONResponse:
    try:
        payload = await asyncio.to_thread(load_upcoming_fixture_payload)
    except FixtureDataError as exc:
        return fixture_error_response(exc)
    fixture = next(
        (item for item in payload["fixtures"] if isinstance(item, dict) and str(item.get("event_id", "")) == event_id),
        None,
    )
    if fixture is None:
        return JSONResponse(
            status_code=404,
            content={
                "detail": {
                    "code": "fixture_not_found",
                    "message": f"No all-upcoming fixture with event_id {event_id!r} was found.",
                    "event_id": event_id,
                }
            },
        )
    return JSONResponse(content=fixture, headers=upcoming_response_headers(payload))


@router.get("/{fixture_date}", dependencies=[Depends(require_cricket_fixture_api_key)])
async def cricket_fixtures(fixture_date: str) -> JSONResponse:
    try:
        target_day = resolve_target_date(fixture_date)
        payload = await asyncio.to_thread(load_fixture_payload, target_day)
    except FixtureDataError as exc:
        return fixture_error_response(exc, locals().get("target_day"))
    return JSONResponse(content=payload, headers=response_headers(payload, target_day))


@router.get("/{fixture_date}/{event_id}", dependencies=[Depends(require_cricket_fixture_api_key)])
async def cricket_fixture(fixture_date: str, event_id: str) -> JSONResponse:
    try:
        target_day = resolve_target_date(fixture_date)
        payload = await asyncio.to_thread(load_fixture_payload, target_day)
    except FixtureDataError as exc:
        return fixture_error_response(exc, locals().get("target_day"))
    fixture = next(
        (item for item in payload["fixtures"] if isinstance(item, dict) and str(item.get("event_id", "")) == event_id),
        None,
    )
    if fixture is None:
        return JSONResponse(
            status_code=404,
            content={
                "detail": {
                    "code": "fixture_not_found",
                    "message": f"No fixture with event_id {event_id!r} was found for {target_day.isoformat()}.",
                    "target_date": target_day.isoformat(),
                    "event_id": event_id,
                }
            },
        )
    return JSONResponse(content=fixture, headers=response_headers(payload, target_day))


class FixtureRefreshService:
    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._running = False
        self._last_attempt = ""
        self._last_success = ""
        self._last_error = ""
        self._last_results: dict[str, str] = {}

    def snapshot(self) -> dict[str, Any]:
        return {
            "enabled": _boolean_setting("CRICKET_FIXTURE_REFRESH_ENABLED", True),
            "configured": bool(get_setting("DECIMAL_USERNAME") and get_setting("DECIMAL_PASSWORD")),
            "running": self._running,
            "interval_minutes": _positive_int_setting("CRICKET_FIXTURE_REFRESH_INTERVAL_MINUTES", 180),
            "last_attempt": self._last_attempt,
            "last_success": self._last_success,
            "last_error": self._last_error,
            "last_results": dict(self._last_results),
        }

    def start(self) -> asyncio.Task[None] | None:
        if not _boolean_setting("CRICKET_FIXTURE_REFRESH_ENABLED", True):
            logger.info("cricket_fixture_refresh_disabled")
            return None
        if not get_setting("DECIMAL_USERNAME") or not get_setting("DECIMAL_PASSWORD"):
            logger.warning("cricket_fixture_refresh_not_started reason=missing_decimal_credentials")
            return None
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run_loop(), name="cricket-fixture-refresh")
        return self._task

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _run_loop(self) -> None:
        initial_delay = _positive_int_setting("CRICKET_FIXTURE_REFRESH_INITIAL_DELAY_SECONDS", 30, minimum=0)
        if initial_delay:
            await asyncio.sleep(initial_delay)
        while True:
            try:
                await self.refresh()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("cricket_fixture_refresh_unhandled_error")
            interval_seconds = _positive_int_setting("CRICKET_FIXTURE_REFRESH_INTERVAL_MINUTES", 180) * 60
            await asyncio.sleep(interval_seconds)

    async def refresh(self) -> None:
        if self._lock.locked():
            logger.info("cricket_fixture_refresh_skipped reason=already_running")
            return
        async with self._lock:
            self._running = True
            self._last_attempt = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            self._last_error = ""
            results: dict[str, str] = {}
            try:
                today = datetime.now(UK_TZ).date()
                for offset in (0, 1):
                    target_day = today + timedelta(days=offset)
                    results[target_day.isoformat()] = await self._refresh_date(target_day)
                results["all_upcoming"] = await self._refresh_all()
                self._last_results = results
                failures = [result for result in results.values() if result != "ok"]
                if failures:
                    self._last_error = "; ".join(failures)
                else:
                    self._last_success = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            finally:
                self._running = False

    async def _refresh_date(self, target_day: date) -> str:
        return await self._run_fetcher(
            target_day.isoformat(),
            "--date",
            target_day.isoformat(),
        )

    async def _refresh_all(self) -> str:
        return await self._run_fetcher("all_upcoming", "--all-upcoming")

    async def _run_fetcher(self, label: str, *arguments: str) -> str:
        timeout_seconds = _positive_int_setting("CRICKET_FIXTURE_REFRESH_TIMEOUT_SECONDS", 1200)
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                str(FETCHER_PATH),
                *arguments,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(PROJECT_ROOT),
                env=child_environment(),
            )
        except OSError as exc:
            message = f"{label}: could not start fixture fetcher ({exc})"
            logger.error("cricket_fixture_refresh_failed error=%r", message)
            return message
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
        except asyncio.CancelledError:
            process.kill()
            await process.wait()
            raise
        except TimeoutError:
            process.kill()
            await process.wait()
            message = f"{label}: refresh timed out after {timeout_seconds} seconds"
            logger.error("cricket_fixture_refresh_failed error=%r", message)
            return message
        if process.returncode != 0:
            error_text = stderr.decode(errors="replace").strip()[-1500:]
            message = f"{label}: {error_text or f'fetcher exited {process.returncode}'}"
            logger.error("cricket_fixture_refresh_failed error=%r", message)
            return message
        logger.info(
            "cricket_fixture_refresh_completed target=%s output=%r",
            label,
            stdout.decode(errors="replace").strip()[-500:],
        )
        return "ok"


fixture_refresh_service = FixtureRefreshService()
