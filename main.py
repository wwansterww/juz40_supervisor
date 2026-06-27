import os
import logging

import httpx
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from logging_setup import configure_logging
configure_logging()
logger = logging.getLogger("juz40.main")

from config import SECRET_KEY, BASE_URL, SESSION_HTTPS_ONLY, SESSION_MAX_AGE
# 16 subjects (informatics, math, biology, ...) are now built from a single
# config registry instead of 16 copy-pasted routes.py files.
from subjects._factory import make_subject_router
from subjects._registry import SUBJECTS
# The /section-report router for informatics is still a one-off (lives under
# /section-report at the root, not under /informatics/...).
from subjects.informatics.section.routes import router as section_router
# VPS multi-subject combined reports live under /vps/*.
from subjects.vps.routes import router as vps_router
# СМАРТ айлық СТ есебі (monthly САБАҚ ТАПСЫРУ report) lives under /smart-monthly/*.
from subjects.smart_monthly.routes import router as smart_monthly_router
# Curator accounts (role CURATOR) get their own per-group flow under /curator/*.
from subjects.curator.routes import router as curator_router

app = FastAPI()
app.add_middleware(
    SessionMiddleware,
    secret_key=SECRET_KEY,
    https_only=SESSION_HTTPS_ONLY,   # Secure flag — enable in production (HTTPS)
    same_site="lax",
    max_age=SESSION_MAX_AGE,
)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/health")
async def health():
    # Liveness + Redis readiness in one probe, for load balancers / monitoring
    # and so a multi-worker deploy can be health-checked. Never raises: returns
    # 200 when Redis is reachable, 503 (with the reason) when it isn't.
    from redis_client import redis_client
    try:
        await redis_client.ping()
        return {"status": "ok", "redis": "ok"}
    except Exception as exc:
        logger.warning("health check: redis unreachable: %s", exc)
        return JSONResponse(
            {"status": "degraded", "redis": "down", "detail": str(exc)},
            status_code=503,
        )


def pct_class(val):
    if val == "-" or val is None:
        return "pct-none"
    try:
        v = float(val)
        if v >= 80: return "pct-high"
        elif v >= 60: return "pct-mid"
        else: return "pct-low"
    except Exception:
        return "pct-none"


templates.env.globals["pct_class"] = pct_class


def _home_path(request: Request) -> str:
    # Curators have a different home (their own per-group flow); everyone else
    # (supervisor / меңгеруші) keeps the existing subject dashboard.
    roles = request.session.get("roles") or []
    if "CURATOR" in roles:
        return "/curator/dashboard"
    return "/dashboard"


async def _load_profile_into_session(request: Request, token: str) -> None:
    # Fetch the signed-in user's profile so we know their role(s). Best-effort:
    # a failure here just leaves roles empty (→ treated as a supervisor), it
    # must never block login.
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{BASE_URL}/v1/users/profile",
                headers={"Authorization": f"Bearer {token}"},
                timeout=15,
            )
        if resp.status_code < 400:
            profile = resp.json()
            request.session["roles"] = profile.get("roles") or []
            request.session["profile"] = {
                "firstname": profile.get("firstname"),
                "lastname": profile.get("lastname"),
            }
            return
    except Exception as exc:
        logger.warning("profile fetch failed after login: %s", exc)
    request.session["roles"] = []


@app.get("/landing", response_class=HTMLResponse)
async def landing(request: Request):
    # Public marketing page — also served at "/" (see index below).
    return templates.TemplateResponse("landing.html", {"request": request})


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    # Visitors land on the marketing page; signed-in users skip straight to
    # their dashboard. The login form now lives at /login, and the landing's
    # "Кіру" CTAs point there.
    if request.session.get("token"):
        return RedirectResponse(_home_path(request), status_code=302)
    return templates.TemplateResponse("landing.html", {"request": request})


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if request.session.get("token"):
        return RedirectResponse(_home_path(request), status_code=302)
    return templates.TemplateResponse("index.html", {"request": request, "error": None})


@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    # Two distinct failure modes with two distinct messages: a 4xx from the
    # auth endpoint means the credentials are wrong; a network error or a 5xx
    # means the API itself is unavailable — telling the user "wrong password"
    # in that case sends them off resetting a perfectly good password.
    error_creds = "Логин немесе пароль қате. Қайталап көріңіз."
    error_api   = "Сервер уақытша қолжетімсіз. Сәл кейінірек қайталап көріңіз."

    def _fail(message: str):
        return templates.TemplateResponse("index.html", {
            "request": request,
            "error": message,
        })

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{BASE_URL}/v1/auth/signin",
                json={"username": username, "password": password},
                timeout=15,
            )
    except httpx.HTTPError as exc:
        logger.warning("auth signin: upstream unreachable: %s", exc)
        return _fail(error_api)

    if 400 <= resp.status_code < 500:
        return _fail(error_creds)
    if resp.status_code >= 500:
        logger.warning("auth signin: upstream returned %s", resp.status_code)
        return _fail(error_api)

    try:
        token = resp.json().get("token")
    except Exception:
        token = None
    if not token:
        return _fail(error_api)

    request.session["token"] = token
    # Load the profile so we know whether this is a CURATOR (→ /curator/...) or
    # a supervisor (→ /dashboard).
    await _load_profile_into_session(request, token)
    return RedirectResponse(_home_path(request), status_code=302)


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=302)


app.include_router(section_router)
app.include_router(vps_router, prefix="/vps")
app.include_router(smart_monthly_router, prefix="/smart-monthly")
app.include_router(curator_router, prefix="/curator")

# Fail-fast at startup if a subject's report_template doesn't exist on disk —
# better than a 500 the first time a user clicks the report button.
_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
_missing = [c.report_template for c in SUBJECTS
            if not os.path.exists(os.path.join(_TEMPLATE_DIR, c.report_template))]
if _missing:
    raise RuntimeError(
        f"Subject registry references missing templates: {sorted(set(_missing))}. "
        f"Either create them in templates/, or update _registry.py to point at "
        f"an existing template."
    )

for cfg in SUBJECTS:
    app.include_router(make_subject_router(cfg), prefix=cfg.prefix)