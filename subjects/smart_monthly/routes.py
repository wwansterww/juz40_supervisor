"""СМАРТ айлық СТ есебі — HTTP routes + view assembly."""

import uuid

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

from store import PROGRESS, JOB_META
from concurrency import get_queue_position, spawn
from subjects.smart_monthly.constants import (
    open_streams, subjects_for_category,
    CATEGORY_SUBGROUPS, CATEGORY_LABEL, SUBGROUP_LABEL,
    MONTH_NUM_TO_NAME, stream_position,
)
from subjects.smart_monthly.builder import build_smart_monthly_job

router = APIRouter()

WEEK_HEADERS = ["1-АПТА", "2-АПТА", "3-АПТА", "4-АПТА"]


def _ctx():
    return {"active_subject": "smart-monthly", "subject_name": "СМАРТ айлық",
            "subject_prefix": "/smart-monthly"}


# ── Number helpers ────────────────────────────────────────────────────────────

def _fmt(v) -> str:
    if v is None:
        return "-"
    return f"{v:.2f}"


def _mean(values):
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return round(sum(nums) / len(nums), 2)


# ── Selection parsing ─────────────────────────────────────────────────────────

def _parse_selections(raw: list[str]) -> list[dict]:
    """Form sends checkbox values "streamMonth:studyMonth" (e.g. "11:5").
    Returns deduped, position-sorted [{stream_month, study_month}]."""
    seen = set()
    out = []
    for item in raw:
        try:
            sm_s, st_s = item.split(":")
            sm, st = int(sm_s), int(st_s)
        except (ValueError, AttributeError):
            continue
        key = (sm, st)
        if key in seen:
            continue
        seen.add(key)
        out.append({"stream_month": sm, "study_month": st})
    out.sort(key=lambda s: (stream_position(s["stream_month"]), s["study_month"]))
    return out


def _block_meta(sel: dict) -> dict:
    sm = sel["stream_month"]
    pos = stream_position(sm)
    return {
        "stream_month": sm,
        "study_month":  sel["study_month"],
        "position":     pos,
        "name":         MONTH_NUM_TO_NAME.get(sm, str(sm)),
        "label":        f"{pos}Т",
        "study_label":  f"{sel['study_month']}-ай",
    }


# ── View assembly ─────────────────────────────────────────────────────────────

def _assemble(results: list, category: str, selections: list[dict]) -> dict:
    subjects = subjects_for_category(category)
    subgroups = CATEGORY_SUBGROUPS.get(category, [])
    blocks = [_block_meta(s) for s in selections]

    # lookup[(slug, stream_month, study_month)] -> weeks dict {1..4: score|None}
    lookup = {}
    for r in results:
        lookup[(r["slug"], r["stream_month"], r["study_month"])] = r["weeks"]

    def weeks_for(slug, block):
        return lookup.get((slug, block["stream_month"], block["study_month"]), {})

    subs_by_group = {sg: [s for s in subjects if s.subgroup == sg] for sg in subgroups}

    # ── ОРТАҚ view: weeks averaged across all blocks ──────────────────────────
    ortaq_groups = []
    for sg in subgroups:
        sg_subjects = subs_by_group.get(sg, [])

        # Left: subjects × weeks (averaged across blocks) + ОРТАҚ
        left_rows = []
        col_pools = {w: [] for w in (1, 2, 3, 4)}
        ortaq_pool = []
        for subj in sg_subjects:
            wk = {w: _mean([weeks_for(subj.slug, b).get(w) for b in blocks])
                  for w in (1, 2, 3, 4)}
            row_ortaq = _mean([wk[w] for w in (1, 2, 3, 4)])
            left_rows.append({
                "abbr":  subj.abbr,
                "cells": [_fmt(wk[w]) for w in (1, 2, 3, 4)],
                "ortaq": _fmt(row_ortaq),
            })
            for w in (1, 2, 3, 4):
                col_pools[w].append(wk[w])
            ortaq_pool.append(row_ortaq)
        left_avg = {
            "cells": [_fmt(_mean(col_pools[w])) for w in (1, 2, 3, 4)],
            "ortaq": _fmt(_mean(ortaq_pool)),
        }

        # Right: subjects × streams (each stream's 4th-week СТ, /20) + ОРТАҚ
        side_rows = []
        side_pools = {i: [] for i in range(len(blocks))}
        side_ortaq_pool = []
        for subj in sg_subjects:
            vals = [weeks_for(subj.slug, b).get(4) for b in blocks]
            r_ortaq = _mean(vals)
            side_rows.append({
                "abbr":  subj.abbr,
                "cells": [_fmt(v) for v in vals],
                "ortaq": _fmt(r_ortaq),
            })
            for i, v in enumerate(vals):
                side_pools[i].append(v)
            side_ortaq_pool.append(r_ortaq)
        side_avg = {
            "cells": [_fmt(_mean(side_pools[i])) for i in range(len(blocks))],
            "ortaq": _fmt(_mean(side_ortaq_pool)),
        }

        ortaq_groups.append({
            "label":        SUBGROUP_LABEL.get(sg, sg),
            "week_headers": WEEK_HEADERS,
            "rows":         left_rows,
            "avg_row":      left_avg,
            "side_headers": [b["label"] for b in blocks],
            "side_rows":    side_rows,
            "side_avg":     side_avg,
        })

    # ── БӨЛЕК view: one block per selected stream ─────────────────────────────
    bolek_blocks = []
    for b in blocks:
        bg_tables = []
        for sg in subgroups:
            sg_subjects = subs_by_group.get(sg, [])
            rows = []
            col_pools = {w: [] for w in (1, 2, 3, 4)}
            ortaq_pool = []
            for subj in sg_subjects:
                wk = weeks_for(subj.slug, b)
                vals = {w: wk.get(w) for w in (1, 2, 3, 4)}
                row_ortaq = _mean([vals[w] for w in (1, 2, 3, 4)])
                rows.append({
                    "abbr":    subj.abbr,
                    "cells":   [_fmt(vals[w]) for w in (1, 2, 3, 4)],
                    "ortaq":   _fmt(row_ortaq),
                    "week4_20": _fmt(vals[4]),
                })
                for w in (1, 2, 3, 4):
                    col_pools[w].append(vals[w])
                ortaq_pool.append(row_ortaq)
            avg_row = {
                "cells":   [_fmt(_mean(col_pools[w])) for w in (1, 2, 3, 4)],
                "ortaq":   _fmt(_mean(ortaq_pool)),
                "week4_20": _fmt(_mean(col_pools[4])),
            }
            bg_tables.append({
                "label":        SUBGROUP_LABEL.get(sg, sg),
                "week_headers": WEEK_HEADERS,
                "rows":         rows,
                "avg_row":      avg_row,
            })
        bolek_blocks.append({**b, "subgroups": bg_tables})

    return {
        "category":       category,
        "category_label": CATEGORY_LABEL.get(category, category),
        "blocks":         blocks,
        "ortaq_groups":   ortaq_groups,
        "bolek_blocks":   bolek_blocks,
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    from main import templates
    if not request.session.get("token"):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("smart_monthly_dashboard.html", {
        "request":  request,
        "streams":  open_streams(),
        "error":    None,
        **_ctx(),
    })


@router.post("/report", response_class=HTMLResponse)
async def report(request: Request,
                 category: str = Form(...),
                 selections: list[str] = Form(default=[])):
    from main import templates
    sels = _parse_selections(selections)
    cat_label = CATEGORY_LABEL.get(category, category)
    streams_label = ", ".join(
        f"{stream_position(s['stream_month'])}Т·{s['study_month']}-ай" for s in sels
    ) or "—"
    return templates.TemplateResponse("loading.html", {
        "request":           request,
        "title":             "СМАРТ айлық СТ есебі…",
        "subtitle_html":     f"<strong>{cat_label}</strong> · {streams_label}",
        "unit":              "Блок",
        "start_url":         "/smart-monthly/report/start",
        "progress_url_base": "/smart-monthly/report/progress",
        "result_url":        "/smart-monthly/report/result",
        "hidden_fields":     {
            "category":   category,
            # re-serialize the parsed selections as a comma list the start
            # endpoint can split back apart (Form lists don't survive a single
            # hidden input otherwise)
            "selections": ",".join(f"{s['stream_month']}:{s['study_month']}" for s in sels),
        },
        "stages": [
            {"p": 0,  "icon": "📥", "title": "Курстар жүктелуде…"},
            {"p": 20, "icon": "📊", "title": "Топтар талдануда…"},
            {"p": 50, "icon": "🧮", "title": "СТ балдары есептелуде…"},
            {"p": 80, "icon": "📈", "title": "Кестелер құрастырылуда…"},
            {"p": 92, "icon": "✨", "title": "Қорытынды дайындалуда…"},
        ],
    })


@router.post("/report/start")
async def report_start(request: Request,
                       category: str = Form(...),
                       selections: str = Form("")):
    token = request.session.get("token")
    if not token:
        return RedirectResponse("/", status_code=302)
    if category not in CATEGORY_SUBGROUPS:
        return JSONResponse({"error": "Жарамсыз санат"}, status_code=400)

    sels = _parse_selections([s for s in selections.split(",") if s])
    if not sels:
        return JSONResponse({"error": "Кемінде бір ағын/ай таңдаңыз."}, status_code=400)

    job_id = str(uuid.uuid4())
    JOB_META[job_id] = {"category": category, "selections": sels}
    request.session["last_sm_job_id"] = job_id

    spawn(build_smart_monthly_job(job_id, category, sels, token))
    total = len(subjects_for_category(category)) * len(sels)
    return JSONResponse({"job_id": job_id, "total": total})


@router.get("/report/progress/{job_id}")
async def report_progress(job_id: str):
    p = await PROGRESS.aget(job_id)
    if not p:
        return JSONResponse({"total": 0, "done": 0, "status": "initializing", "queue_position": 0})
    return JSONResponse({
        "total":          p.get("total", 0),
        "done":           p.get("done", 0),
        "status":         p.get("status", "running"),
        "queue_position": get_queue_position(job_id),
        "error":          p.get("error"),
    })


@router.get("/report/result", response_class=HTMLResponse)
async def report_result(request: Request, job: str = ""):
    from main import templates
    if not request.session.get("token"):
        return RedirectResponse("/", status_code=302)

    job_id = job or request.session.get("last_sm_job_id")
    p = (await PROGRESS.aget(job_id)) if job_id else None
    if not p or p.get("status") != "done":
        return RedirectResponse("/smart-monthly/", status_code=302)

    meta = (await JOB_META.aget(job_id)) or {}
    category = meta.get("category") or p.get("category", "esep")
    selections = meta.get("selections") or p.get("selections", [])

    view = _assemble(p.get("results", []), category, selections)

    return templates.TemplateResponse("smart_monthly_report.html", {
        "request": request,
        "view":    view,
        **_ctx(),
    })
