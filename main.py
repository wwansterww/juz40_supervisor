import os
import httpx
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from config import SECRET_KEY, BASE_URL
from subjects.informatics.routes import router as informatics_router
from subjects.informatics.section.routes import router as section_router
from subjects.physics.routes import router as physics_router
from subjects.chemistry.routes import router as chemistry_router
from subjects.ms.routes import router as ms_router
from subjects.geometry.routes import router as geometry_router
from subjects.math.routes import router as math_router
from subjects.geography.routes import router as geography_router  # ← жаңа

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


app.include_router(informatics_router)
app.include_router(section_router)
app.include_router(math_router, prefix="/math")
app.include_router(geometry_router, prefix="/geometry")
app.include_router(ms_router, prefix="/ms")
app.include_router(physics_router, prefix="/physics")
app.include_router(chemistry_router, prefix="/chemistry")
app.include_router(geography_router, prefix="/geography")