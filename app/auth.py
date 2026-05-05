from __future__ import annotations

import secrets

from fastapi import Request
from fastapi.responses import RedirectResponse, Response
from itsdangerous import BadSignature, URLSafeSerializer

from app.config import app_password, session_secret


COOKIE_NAME = "betfair_scripts_session"


def _serializer() -> URLSafeSerializer:
    return URLSafeSerializer(session_secret(), salt="betfair-scripts-hub")


def password_configured() -> bool:
    return bool(app_password())


def verify_password(candidate: str) -> bool:
    configured = app_password()
    if not configured:
        return True
    return secrets.compare_digest(candidate or "", configured)


def is_authenticated(request: Request) -> bool:
    if not password_configured():
        return True
    cookie = request.cookies.get(COOKIE_NAME)
    if not cookie:
        return False
    try:
        payload = _serializer().loads(cookie)
    except BadSignature:
        return False
    return payload == {"authenticated": True}


def require_auth(request: Request) -> RedirectResponse | None:
    if is_authenticated(request):
        return None
    return RedirectResponse("/login", status_code=303)


def set_login_cookie(response: Response) -> None:
    value = _serializer().dumps({"authenticated": True})
    response.set_cookie(
        COOKIE_NAME,
        value,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=60 * 60 * 12,
    )


def clear_login_cookie(response: Response) -> None:
    response.delete_cookie(COOKIE_NAME)
