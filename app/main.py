from __future__ import annotations

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.auth import clear_login_cookie, is_authenticated, password_configured, require_auth, set_login_cookie, verify_password
from app.config import APP_DIR, PROJECT_ROOT, app_password, branding_assets, ensure_runtime_dirs
from app.parsers import parse_cricket_time_check_output
from app.registry import CATEGORIES, SCRIPT_REGISTRY, SCRIPTS_BY_ID
from app.runner import RUNNING, default_args_for, runner
from app.scheduler import window_status


ensure_runtime_dirs()

app = FastAPI(title="Betfair Scripts Hub")
app.mount("/static", StaticFiles(directory=APP_DIR / "static"), name="static")
templates = Jinja2Templates(directory=APP_DIR / "templates")
templates.env.cache = None


@app.on_event("startup")
async def startup() -> None:
    ensure_runtime_dirs()


@app.middleware("http")
async def monitor_windows(request: Request, call_next):
    runner.stop_expired_windows()
    return await call_next(request)


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
    states = {spec.id: runner.get_state(spec.id) for spec in SCRIPT_REGISTRY}
    return templates.TemplateResponse(request, "dashboard.html", template_context(request, states=states))


@app.get("/scripts/{script_id}", response_class=HTMLResponse)
async def script_detail(request: Request, script_id: str):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    spec = SCRIPTS_BY_ID[script_id]
    state = runner.get_state(script_id)
    allowed, window_label = window_status(spec)
    cricket = None
    if spec.parsed_output and state.status != RUNNING:
        try:
            cricket = parse_cricket_time_check_output(state.output_lines)
        except Exception:
            cricket = None
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
        ),
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
    runner.start(script_id, default_args_for(spec, form))
    return RedirectResponse(f"/scripts/{script_id}", status_code=303)


@app.post("/scripts/{script_id}/stop")
async def stop_script(request: Request, script_id: str):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    runner.stop(script_id)
    return RedirectResponse(f"/scripts/{script_id}", status_code=303)


@app.get("/scripts/{script_id}/status", response_class=HTMLResponse)
async def script_status(request: Request, script_id: str):
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    state = runner.get_state(script_id)
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
    spec = SCRIPTS_BY_ID[script_id]
    state = runner.get_state(script_id)
    cricket = None
    if spec.parsed_output and state.status != RUNNING:
        try:
            cricket = parse_cricket_time_check_output(state.output_lines)
        except Exception:
            cricket = None
    return templates.TemplateResponse(
        request,
        "partials/output_console.html",
        template_context(request, spec=spec, state=state, cricket=cricket),
    )


@app.get("/health")
async def health():
    return {"status": "ok", "project_root": str(PROJECT_ROOT)}
