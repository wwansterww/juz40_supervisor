import os
import httpx
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from config import SECRET_KEY, BASE_URL
# 16 subjects (informatics, math, biology, ...) are now built from a single
# config registry instead of 16 copy-pasted routes.py files.
from subjects._factory import make_subject_router
from subjects._registry import SUBJECTS
# The /section-report router for informatics is still a one-off (lives under
# /section-report at the root, not under /informatics/...).
from subjects.informatics.section.routes import router as section_router
# VPS multi-subject combined reports live under /vps/*.
from subjects.vps.routes import router as vps_router

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


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


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if request.session.get("token"):
        return RedirectResponse("/dashboard", status_code=302)
    return templates.TemplateResponse("index.html", {"request": request, "error": None})


@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{BASE_URL}/v1/auth/signin",
                json={"username": username, "password": password},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            token = data.get("token")
            if not token:
                raise ValueError("no token")
        request.session["token"] = token
        return RedirectResponse("/dashboard", status_code=302)
    except Exception:
        return templates.TemplateResponse("index.html", {
            "request": request,
            "error": "Логин немесе пароль қате. Қайталап көріңіз.",
        })


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=302)


app.include_router(section_router)
app.include_router(vps_router, prefix="/vps")

# Fail-fast at startup if a subject's report_template doesn't exist on disk —
# better than a 500 the first time a user clicks the report button.
import os as _os
_TEMPLATE_DIR = _os.path.join(_os.path.dirname(__file__), "templates")
_missing = [c.report_template for c in SUBJECTS
            if not _os.path.exists(_os.path.join(_TEMPLATE_DIR, c.report_template))]
if _missing:
    raise RuntimeError(
        f"Subject registry references missing templates: {sorted(set(_missing))}. "
        f"Either create them in templates/, or update _registry.py to point at "
        f"an existing template."
    )

for cfg in SUBJECTS:
    app.include_router(make_subject_router(cfg), prefix=cfg.prefix)