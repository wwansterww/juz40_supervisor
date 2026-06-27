"""
Curator routes — mounted under /curator (see main.py).

Mirrors the supervisor flow (dashboard → loading → result) but:
  • the dashboard lists only THIS curator's own streams (/v2/curator/groups);
  • the report is built for that single group via subjects.curator.builder;
  • the result page reuses each subject's columns and adds a "кто не сдал" block.
"""

import csv
import io
import logging
import uuid

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response

from config import BASE_URL, STUDY_MONTHS
from cache import api_get_async, get_shared_client
from store import PROGRESS, REPORT_STORE, JOB_META
from concurrency import get_queue_position, spawn
from subjects.curator.builder import _build_curator_report_job, get_cfg, resolve_metric_fns

logger = logging.getLogger("juz40.curator.routes")

router = APIRouter()

# Same loading-screen phases the supervisor report uses.
_REPORT_STAGES = [
    {"p": 0,  "icon": "📥", "title": "Деректер жүктелуде…"},
    {"p": 18, "icon": "📊", "title": "Сабақтар талдануда…"},
    {"p": 45, "icon": "🧮", "title": "Ортақ балл есептелуде…"},
    {"p": 70, "icon": "📈", "title": "Кесте құрастырылуда…"},
    {"p": 90, "icon": "✨", "title": "Қорытынды дайындалуда…"},
]


def _is_curator(request: Request) -> bool:
    return "CURATOR" in (request.session.get("roles") or [])


def _curator_name(request: Request) -> str:
    p = request.session.get("profile") or {}
    name = f"{(p.get('firstname') or '').strip()} {(p.get('lastname') or '').strip()}".strip()
    return name or "Куратор"


# ── Dashboard ──────────────────────────────────────────────────────────────────

@router.get("/dashboard", response_class=HTMLResponse)
async def curator_dashboard(request: Request):
    from main import templates
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)

    error = None
    groups = []
    try:
        data = await api_get_async(f"{BASE_URL}/v2/curator/groups", token, get_shared_client())
        groups = data if isinstance(data, list) else []
    except Exception:
        error = "Топтарды жүктеу кезінде қате шықты. Қайта кіріп көріңіз."

    # Only keep groups whose subject we actually know how to report on.
    groups = [g for g in groups if get_cfg(g.get("subjectId")) is not None]

    return templates.TemplateResponse("curator_dashboard.html", {
        "request": request,
        "groups": groups,
        "study_months": STUDY_MONTHS,
        "curator_name": _curator_name(request),
        "error": error,
    })


# ── Report (loading screen) ─────────────────────────────────────────────────────

@router.post("/report", response_class=HTMLResponse)
async def curator_report(
    request: Request,
    group_id: str = Form(...),
    course_name: str = Form(...),
    subject_id: str = Form(...),
    study_month: str = Form(...),
    week: str = Form("all"),
):
    from main import templates
    if week and week != "all":
        subtitle = f"<strong>{course_name}</strong> · {study_month} · {week}-апта"
    else:
        subtitle = f"<strong>{course_name}</strong> · {study_month}"
    return templates.TemplateResponse("loading.html", {
        "request": request,
        "title": "Отчет жасалуда…",
        "subtitle_html": subtitle,
        "unit": "Апта",
        "start_url": "/curator/report/start",
        "progress_url_base": "/curator/report/progress",
        "result_url": "/curator/report/result",
        "hidden_fields": {
            "group_id": group_id,
            "course_name": course_name,
            "subject_id": subject_id,
            "study_month": study_month,
            "week": week,
        },
        "stages": _REPORT_STAGES,
    })


@router.post("/report/start")
async def curator_report_start(
    request: Request,
    group_id: str = Form(...),
    course_name: str = Form(...),
    subject_id: str = Form(...),
    study_month: str = Form(...),
    week: str = Form("all"),
):
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)
    if get_cfg(subject_id) is None:
        return JSONResponse({"error": "Бұл пән бойынша отчет қолжетімсіз."}, status_code=400)
    try:
        month_num = int(study_month.replace("-ай", ""))
    except ValueError:
        return JSONResponse({"error": "Жарамсыз оқу айы"}, status_code=400)

    week_filter = None
    if week and week != "all":
        try:
            wf = int(week)
            if wf in (1, 2, 3, 4):
                week_filter = wf
        except ValueError:
            pass

    # Pull the full group object so the builder has courseName etc.; fall back to
    # a minimal stub if the lookup fails (the build only needs the id + name).
    group = {"groupId": group_id, "courseName": course_name, "subjectId": subject_id}
    try:
        data = await api_get_async(f"{BASE_URL}/v2/curator/groups", token, get_shared_client())
        if isinstance(data, list):
            match = next((g for g in data if g.get("groupId") == group_id), None)
            if match:
                group = match
    except Exception:
        pass

    job_id = str(uuid.uuid4())
    JOB_META[job_id] = {
        "course_name": course_name,
        "study_month": study_month,
        "week_filter": week_filter,
        "subject_id": subject_id,
    }
    request.session["last_curator_job_id"] = job_id

    spawn(_build_curator_report_job(
        job_id, group, _curator_name(request), subject_id, token, month_num,
        week_filter=week_filter,
    ))
    return JSONResponse({"job_id": job_id, "total": 1 if week_filter else 4})


@router.get("/report/progress/{job_id}")
async def curator_report_progress(job_id: str):
    p = await PROGRESS.aget(job_id)
    if not p:
        return JSONResponse({"total": 0, "done": 0, "status": "initializing", "queue_position": 0})
    return JSONResponse({
        "total": p.get("total", 0),
        "done": p.get("done", 0),
        "status": p.get("status", "running"),
        "queue_position": get_queue_position(job_id),
        "error": p.get("error"),
    })


@router.get("/report/result", response_class=HTMLResponse)
async def curator_report_result(request: Request, job: str = ""):
    from main import templates
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)

    job_id = job or request.session.get("last_curator_job_id")
    meta = (await JOB_META.aget(job_id)) or {} if job_id else {}
    course_name = meta.get("course_name", "")
    study_month = meta.get("study_month", "")
    subject_id = meta.get("subject_id", "")
    week_filter = meta.get("week_filter")

    p = (await PROGRESS.aget(job_id)) if job_id else None
    if not p or p.get("status") != "done" or not p.get("results"):
        return RedirectResponse("/curator/dashboard", status_code=302)

    fns = resolve_metric_fns(subject_id)
    if fns is None:
        return RedirectResponse("/curator/dashboard", status_code=302)
    _extract, _merge, _empty, metrics_to_row = fns

    gr = p["results"][0]
    weeks_to_show = [week_filter] if week_filter in (1, 2, 3, 4) else [1, 2, 3, 4]

    tables = []
    for w in weeks_to_show:
        row = metrics_to_row(gr["base"], gr["weeks"][str(w)])
        tables.append({
            "title": f"{w}-апта",
            "subtitle": f"{study_month} {w}-апта нәтижелері",
            "week": w,
            "rows": [row],
            "avg_row": None,
            "not_submitted": gr.get("weeks_not_submitted", {}).get(str(w), []),
        })

    if week_filter is None:
        monthly_row = metrics_to_row(gr["base"], gr["monthly"])
        tables.append({
            "title": "Айлық қорытынды",
            "subtitle": f"{study_month} бойынша жалпы қорытынды",
            "week": "monthly",
            "rows": [monthly_row],
            "avg_row": None,
            "not_submitted": [],
        })

    report_key = job_id
    REPORT_STORE[report_key] = {
        "tables": [
            {"title": f"{study_month} {t['title']}", "rows": t["rows"], "avg_row": t["avg_row"]}
            for t in tables
        ],
        "title": f"{course_name} {study_month}",
    }
    request.session["last_curator_report_key"] = report_key

    return templates.TemplateResponse("curator_report.html", {
        "request": request,
        "tables": tables,
        "course_name": course_name,
        "study_month": study_month,
        "curator_name": _curator_name(request),
        "error": None,
        "export_url": f"/curator/export?key={report_key}",
        "dashboard_url": "/curator/dashboard",
    })


# ── CSV export ──────────────────────────────────────────────────────────────────

@router.get("/export")
async def curator_export(request: Request, key: str = ""):
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)
    report_key = key or request.session.get("last_curator_report_key")
    store = (await REPORT_STORE.aget(report_key)) if report_key else None
    if not store or not store.get("tables"):
        return Response(content="Экспортқа деректер жоқ. Алдымен отчет жасаңыз.", status_code=400)

    output = io.StringIO()
    for table in store["tables"]:
        rows = list(table.get("rows", []))
        avg_row = table.get("avg_row")
        if not rows:
            continue
        if avg_row:
            rows.append(avg_row)
        output.write(f"# {table['title']}\n")
        fieldnames: list = []
        for r in rows:
            for k in r.keys():
                if k not in fieldnames:
                    fieldnames.append(k)
        writer = csv.DictWriter(output, fieldnames=fieldnames, restval="")
        writer.writeheader()
        writer.writerows(rows)
        output.write("\n")

    return Response(
        content=output.getvalue().encode("utf-8-sig"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=curator_report.csv"},
    )
