from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.auth import clear_login_cookie, is_authenticated, password_configured, require_auth, set_login_cookie, verify_password
from app.config import APP_DIR, CONFIG_DIR, PROJECT_ROOT, app_password, branding_assets, ensure_runtime_dirs
from app.cricket_fixture_api import fixture_refresh_service, router as cricket_fixture_api_router
from app.parsers import parse_cricket_time_check_output, parse_inplay_checker_state
from app.registry import CATEGORIES, SCRIPT_REGISTRY, SCRIPTS_BY_ID
from app.reminders import daily_reminders_context
from app.runner import RUNNING, STOPPING, default_args_for, runner
from app.scheduler import window_status


ensure_runtime_dirs()

app = FastAPI(title="Betfair Scripts Hub")
app.include_router(cricket_fixture_api_router)
app.mount("/static", StaticFiles(directory=APP_DIR / "static"), name="static")
templates = Jinja2Templates(directory=APP_DIR / "templates")
templates.env.cache = None
logger = logging.getLogger("uvicorn.error")
PARSER_TIMEOUT_SECONDS = 2.0
_parser_locks = {script_id: threading.Lock() for script_id in SCRIPTS_BY_ID}
GOLF_SCRIPT_ID = "golf-non-runner-check"
GOLF_CONFIG_PATH = CONFIG_DIR / "golf_field_checker.json"
GOLF_SITES = (
    ("pgatour", "PGA Tour", ("pgatour.com", "www.pgatour.com")),
    ("pgachampions", "PGA Tour Champions", ("pgatour.com", "www.pgatour.com")),
    ("dpworld", "DP World Tour", ("europeantour.com", "www.europeantour.com")),
    ("lpga", "LPGA", ("lpga.com", "www.lpga.com")),
)
UFC_SCRIPT_ID = "ufc-live-start-scanner"
UFC_CONFIG_PATH = CONFIG_DIR / "ufc_live_start_scanner.json"
PFL_SCRIPT_ID = "pfl-live-start-scanner"
PFL_CONFIG_PATH = CONFIG_DIR / "pfl_live_start_scanner.json"
REMINDERS_SCRIPT_ID = "betfair-event-reminders"


class ParserBusyError(RuntimeError):
    pass


def _event(event: str, script_id: str, level: int = logging.INFO, **details: Any) -> None:
    spec = SCRIPTS_BY_ID[script_id]
    detail_text = " ".join(f"{key}={value!r}" for key, value in details.items())
    logger.log(
        level,
        "%s script_id=%s script_name=%r%s",
        event,
        script_id,
        spec.name,
        f" {detail_text}" if detail_text else "",
    )


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_golf_config() -> dict[str, Any]:
    try:
        if not GOLF_CONFIG_PATH.exists():
            return {}
        data = json.loads(GOLF_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("golf_config_read_failed path=%s error=%r", GOLF_CONFIG_PATH, exc)
        return {}
    return data if isinstance(data, dict) else {}


def _write_golf_config(data: dict[str, Any]) -> None:
    ensure_runtime_dirs()
    temporary = GOLF_CONFIG_PATH.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(GOLF_CONFIG_PATH)


def _golf_context() -> dict[str, Any]:
    data = _read_golf_config()
    configured = {
        str(item.get("id", "")): item
        for item in data.get("sites", [])
        if isinstance(item, dict)
    }
    sites = []
    for site_id, label, _ in GOLF_SITES:
        item = configured.get(site_id, {})
        sites.append(
            {
                "id": site_id,
                "label": label,
                "url": str(item.get("url", "") or ""),
                "enabled": bool(item.get("enabled")),
            }
        )
    return {
        "sites": sites,
        "configured": any(item["enabled"] and item["url"] for item in sites),
        "last_saved_at": str(data.get("last_saved_at", "") or ""),
    }


def _valid_golf_url(site_id: str, value: str) -> bool:
    site = next((item for item in GOLF_SITES if item[0] == site_id), None)
    if site is None:
        return False
    parsed = urlparse(value.strip())
    return parsed.scheme == "https" and (parsed.hostname or "").lower() in site[2]


def _read_ufc_config() -> dict[str, Any]:
    try:
        if not UFC_CONFIG_PATH.exists():
            return {}
        data = json.loads(UFC_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("ufc_config_read_failed path=%s error=%r", UFC_CONFIG_PATH, exc)
        return {}
    return data if isinstance(data, dict) else {}


def _write_ufc_config(data: dict[str, Any]) -> None:
    ensure_runtime_dirs()
    UFC_CONFIG_PATH.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def _ufc_context() -> dict[str, Any]:
    data = _read_ufc_config()
    return {
        "ufc_event_url": str(data.get("ufc_event_url", "") or ""),
        "last_saved_at": str(data.get("last_saved_at", "") or ""),
        "last_check_time": str(data.get("last_check_time", "") or ""),
        "last_detected_live_fight": str(data.get("last_detected_live_fight", "") or ""),
        "last_slack_alert_sent": str(data.get("last_slack_alert_sent", "") or ""),
        "alerted_fights": list(data.get("alerted_fights", []) or [])[-25:],
    }


def _read_pfl_config() -> dict[str, Any]:
    try:
        if not PFL_CONFIG_PATH.exists():
            return {}
        data = json.loads(PFL_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("pfl_config_read_failed path=%s error=%r", PFL_CONFIG_PATH, exc)
        return {}
    return data if isinstance(data, dict) else {}


def _write_pfl_config(data: dict[str, Any]) -> None:
    ensure_runtime_dirs()
    PFL_CONFIG_PATH.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def _pfl_context() -> dict[str, Any]:
    data = _read_pfl_config()
    return {
        "pfl_event_url": str(data.get("pfl_event_url", "") or ""),
        "last_saved_at": str(data.get("last_saved_at", "") or ""),
        "last_check_time": str(data.get("last_check_time", "") or ""),
        "last_detected_live_fight": str(data.get("last_detected_live_fight", "") or ""),
        "last_slack_alert_sent": str(data.get("last_slack_alert_sent", "") or ""),
        "betfair_events_cached": int(data.get("betfair_events_cached", 0) or 0),
        "betfair_cache_loaded_at": str(data.get("betfair_cache_loaded_at", "") or ""),
        "alerted_fights": list(data.get("alerted_fights", []) or [])[-25:],
    }


def _default_args_for_start(spec, form: dict[str, str]) -> list[str]:
    if spec.id == UFC_SCRIPT_ID:
        data = _read_ufc_config()
        ufc_url = str(form.get("ufc_event_url", "") or data.get("ufc_event_url", "") or "").strip()
        return ["--ufc-url", ufc_url] if ufc_url else []
    if spec.id == PFL_SCRIPT_ID:
        data = _read_pfl_config()
        pfl_url = str(form.get("pfl_event_url", "") or data.get("pfl_event_url", "") or "").strip()
        return ["--pfl-url", pfl_url] if pfl_url else []
    return default_args_for(spec, form)


def _run_parser(script_id: str, parser: Callable[..., Any], *args: Any) -> Any:
    parser_lock = _parser_locks[script_id]
    if not parser_lock.acquire(blocking=False):
        raise ParserBusyError("A previous parser call is still running.")
    try:
        return parser(*args)
    finally:
        parser_lock.release()


async def _parsed_output(spec, state):
    if not spec.parsed_output:
        return None, None, ""
    if state.status in {RUNNING, STOPPING}:
        _event("script_parser_skipped_running", spec.id, job_id=state.job_id)
        return None, None, "Parsed output unavailable while the script is running."

    try:
        if spec.id == "betfair-in-play-start-checker":
            inplay = await asyncio.wait_for(
                asyncio.to_thread(_run_parser, spec.id, parse_inplay_checker_state),
                timeout=PARSER_TIMEOUT_SECONDS,
            )
            return None, inplay, ""
        cricket = await asyncio.wait_for(
            asyncio.to_thread(_run_parser, spec.id, parse_cricket_time_check_output, state.output_lines),
            timeout=PARSER_TIMEOUT_SECONDS,
        )
        return cricket, None, ""
    except (TimeoutError, ParserBusyError) as exc:
        _event("script_parser_timeout", spec.id, level=logging.WARNING, error=str(exc))
        return None, None, "Parsed output unavailable."
    except Exception as exc:
        _event("script_parser_failed", spec.id, level=logging.WARNING, error=str(exc))
        return None, None, "Parsed output unavailable."


async def _window_monitor() -> None:
    while True:
        try:
            await asyncio.to_thread(runner.run_automations)
        except Exception:
            logger.exception("script_automation_monitor_failed")
        await asyncio.sleep(15)


@app.on_event("startup")
async def startup() -> None:
    ensure_runtime_dirs()
    await asyncio.to_thread(runner.startup_cleanup)
    await asyncio.to_thread(runner.run_automations, catch_up=True)
    app.state.window_monitor_task = asyncio.create_task(_window_monitor())
    app.state.cricket_fixture_refresh_task = fixture_refresh_service.start()


@app.on_event("shutdown")
async def shutdown() -> None:
    await fixture_refresh_service.stop()
    task = getattr(app.state, "window_monitor_task", None)
    if task is None:
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def template_context(request: Request, **extra):
    context = {
        "request": request,
        "scripts": SCRIPT_REGISTRY,
        "categories": CATEGORIES,
        "password_missing": not password_configured(),
        "authenticated": is_authenticated(request),
        "assets": branding_assets(),
    }
    context.update(extra)
    return context


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if is_authenticated(request) and app_password():
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse(request, "login.html", template_context(request, error=""))


@app.post("/login")
async def login(request: Request, password: str = Form("")):
    if not verify_password(password):
        return templates.TemplateResponse(
            request,
            "login.html",
            template_context(request, error="Incorrect password."),
            status_code=401,
        )
    response = RedirectResponse("/", status_code=303)
    set_login_cookie(response)
    return response


@app.post("/logout")
async def logout():
    response = RedirectResponse("/login", status_code=303)
    clear_login_cookie(response)
    return response


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    states = await asyncio.to_thread(runner.get_all_states)
    return templates.TemplateResponse(request, "dashboard.html", template_context(request, states=states))


@app.head("/")
async def dashboard_head(request: Request):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    return Response(status_code=200)


@app.get("/scripts/{script_id}", response_class=HTMLResponse)
async def script_detail(request: Request, script_id: str):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    render_started = time.perf_counter()
    _event("script_page_render_started", script_id)
    spec = SCRIPTS_BY_ID[script_id]
    try:
        state = await asyncio.to_thread(runner.get_state, script_id)
        allowed, window_label = window_status(spec)
        cricket, inplay, parsed_output_message = await _parsed_output(spec, state)
        return templates.TemplateResponse(
            request,
            "script_detail.html",
            template_context(
                request,
                spec=spec,
                state=state,
                allowed=allowed,
                window_label=window_label,
                cricket=cricket,
                inplay=inplay,
                parsed_output_message=parsed_output_message,
                golf=_golf_context() if spec.id == GOLF_SCRIPT_ID else None,
                ufc=_ufc_context() if spec.id == UFC_SCRIPT_ID else None,
                pfl=_pfl_context() if spec.id == PFL_SCRIPT_ID else None,
                reminders=daily_reminders_context() if spec.id == REMINDERS_SCRIPT_ID else None,
            ),
        )
    finally:
        _event(
            "script_page_render_completed",
            script_id,
            elapsed_ms=round((time.perf_counter() - render_started) * 1000, 1),
        )


@app.post("/scripts/{script_id}/start")
async def start_script(request: Request, script_id: str):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    spec = SCRIPTS_BY_ID[script_id]
    form = dict(await request.form())
    if spec.needs_parameters and not str(form.get("identifier", "")).strip():
        return RedirectResponse(f"/scripts/{script_id}?error=missing-identifier", status_code=303)
    if script_id == GOLF_SCRIPT_ID and not _golf_context()["configured"]:
        return RedirectResponse(f"/scripts/{script_id}?error=missing-golf-urls", status_code=303)
    if script_id == UFC_SCRIPT_ID and not str(form.get("ufc_event_url", "") or _read_ufc_config().get("ufc_event_url", "")).strip():
        return RedirectResponse(f"/scripts/{script_id}?error=missing-ufc-url", status_code=303)
    if script_id == PFL_SCRIPT_ID and not str(form.get("pfl_event_url", "") or _read_pfl_config().get("pfl_event_url", "")).strip():
        return RedirectResponse(f"/scripts/{script_id}?error=missing-pfl-url", status_code=303)
    await asyncio.to_thread(runner.start, script_id, _default_args_for_start(spec, form))
    return RedirectResponse(f"/scripts/{script_id}", status_code=303)


@app.post("/scripts/golf-non-runner-check/config")
async def save_golf_config(request: Request):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    form = dict(await request.form())
    sites: list[dict[str, Any]] = []
    for site_id, _, _ in GOLF_SITES:
        url = str(form.get(f"{site_id}_url", "") or "").strip()
        enabled = f"{site_id}_enabled" in form
        if enabled and (not url or not _valid_golf_url(site_id, url)):
            return RedirectResponse(
                f"/scripts/{GOLF_SCRIPT_ID}?error=invalid-golf-url&site={site_id}",
                status_code=303,
            )
        sites.append({"id": site_id, "enabled": enabled, "url": url})
    await asyncio.to_thread(
        _write_golf_config,
        {"sites": sites, "last_saved_at": _utc_timestamp()},
    )
    return RedirectResponse(f"/scripts/{GOLF_SCRIPT_ID}", status_code=303)


@app.post("/scripts/ufc-live-start-scanner/config")
async def save_ufc_config(request: Request, ufc_event_url: str = Form("")):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    data = _read_ufc_config()
    url = ufc_event_url.strip()
    previous_url = str(data.get("ufc_event_url", "") or "")
    data["ufc_event_url"] = url
    data["last_saved_at"] = _utc_timestamp()
    if previous_url and previous_url != url:
        data["alerted_fight_keys"] = []
        data["alerted_fights"] = []
        data["last_detected_live_fight"] = ""
        data["last_slack_alert_sent"] = ""
    await asyncio.to_thread(_write_ufc_config, data)
    return RedirectResponse("/scripts/ufc-live-start-scanner", status_code=303)


@app.post("/scripts/ufc-live-start-scanner/clear-alerted")
async def clear_ufc_alerted(request: Request):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    data = _read_ufc_config()
    data["alerted_fight_keys"] = []
    data["alerted_fights"] = []
    data["last_detected_live_fight"] = ""
    data["last_slack_alert_sent"] = ""
    data["alerted_cleared_at"] = _utc_timestamp()
    await asyncio.to_thread(_write_ufc_config, data)
    return RedirectResponse("/scripts/ufc-live-start-scanner", status_code=303)


@app.post("/scripts/pfl-live-start-scanner/config")
async def save_pfl_config(request: Request, pfl_event_url: str = Form("")):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    data = _read_pfl_config()
    url = pfl_event_url.strip()
    previous_url = str(data.get("pfl_event_url", "") or "")
    data["pfl_event_url"] = url
    data["last_saved_at"] = _utc_timestamp()
    if previous_url and previous_url != url:
        data["alerted_fight_keys"] = []
        data["alerted_fights"] = []
        data["last_detected_live_fight"] = ""
        data["last_slack_alert_sent"] = ""
        data["betfair_events_cached"] = 0
        data["betfair_cache_loaded_at"] = ""
    await asyncio.to_thread(_write_pfl_config, data)
    return RedirectResponse("/scripts/pfl-live-start-scanner", status_code=303)


@app.post("/scripts/pfl-live-start-scanner/clear-alerted")
async def clear_pfl_alerted(request: Request):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    data = _read_pfl_config()
    data["alerted_fight_keys"] = []
    data["alerted_fights"] = []
    data["last_detected_live_fight"] = ""
    data["last_slack_alert_sent"] = ""
    data["alerted_cleared_at"] = _utc_timestamp()
    await asyncio.to_thread(_write_pfl_config, data)
    return RedirectResponse("/scripts/pfl-live-start-scanner", status_code=303)


@app.post("/scripts/{script_id}/stop")
async def stop_script(request: Request, script_id: str):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    await asyncio.to_thread(runner.stop, script_id)
    return RedirectResponse(f"/scripts/{script_id}", status_code=303)


@app.get("/scripts/{script_id}/status", response_class=HTMLResponse)
async def script_status(request: Request, script_id: str):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    state = await asyncio.to_thread(runner.get_state, script_id)
    return templates.TemplateResponse(
        request,
        "partials/status_badge.html",
        template_context(request, state=state),
    )


@app.get("/scripts/{script_id}/output", response_class=HTMLResponse)
async def script_output(request: Request, script_id: str):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    render_started = time.perf_counter()
    _event("script_output_render_started", script_id)
    spec = SCRIPTS_BY_ID[script_id]
    try:
        state = await asyncio.to_thread(runner.get_state, script_id)
        cricket, inplay, parsed_output_message = await _parsed_output(spec, state)
        return templates.TemplateResponse(
            request,
            "partials/output_console.html",
            template_context(
                request,
                spec=spec,
                state=state,
                cricket=cricket,
                inplay=inplay,
                parsed_output_message=parsed_output_message,
                ufc=_ufc_context() if spec.id == UFC_SCRIPT_ID else None,
                pfl=_pfl_context() if spec.id == PFL_SCRIPT_ID else None,
            ),
        )
    finally:
        _event(
            "script_output_render_completed",
            script_id,
            elapsed_ms=round((time.perf_counter() - render_started) * 1000, 1),
        )


@app.get("/health")
async def health():
    snapshot = await asyncio.to_thread(runner.health_snapshot)
    snapshot["status"] = "ok"
    snapshot["project_root"] = str(PROJECT_ROOT)
    return snapshot
