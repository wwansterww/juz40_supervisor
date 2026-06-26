"""
СМАРТ айлық СТ есебі — configuration.

This report aggregates ONE metric — the САБАҚ ТАПСЫРУ (СТ) average score — for
SMART courses, laid out as subjects (rows) × weeks (columns), per selected
stream (поток). It is intentionally separate from the per-subject dashboards
and the раздел report.

Two report categories the user picks between:
  • esep     — ИНФО, МС, ГЕОМ, МАТ, ХИМ, ФИЗ  (one subtable)
  • auyzsha  — ауызша/жазбаша, rendered as two subtables:
                 oral  — ЛИТ, ТАРИХ, ГЕО, ӘДЕБ, ДЖТ, БИО, ҚҰҚЫҚ
                 lang  — РУС, ТІЛ, АНГЛ
    (the user treats ауызша and жазбаша as one selectable category, but the
     sheet visually splits them — hence one category, two subgroups.)

Each report has two views:
  • ОРТАҚ  — weeks averaged across all selected streams (+ a per-stream side
             table whose columns are the streams).
  • БӨЛЕК  — one block per selected stream.
"""

from datetime import date

from config import (
    INFORMATICS_SUBJECT_ID, MS_SUBJECT_ID, GEOMETRY_SUBJECT_ID,
    MATH_SUBJECT_ID, CHEMISTRY_SUBJECT_ID, PHYSICS_SUBJECT_ID,
    RUSSIAN_LITERATURE_SUBJECT_ID, HISTORY_SUBJECT_ID, GEOGRAPHY_SUBJECT_ID,
    KAZAKH_LITERATURE_SUBJECT_ID, WORLD_HISTORY_SUBJECT_ID, BIOLOGY_SUBJECT_ID,
    KUKYK_SUBJECT_ID, RUSSIAN_LANGUAGE_SUBJECT_ID, KAZAKH_LANGUAGE_SUBJECT_ID,
    ENGLISH_SUBJECT_ID,
)
# Reuse the stream-month order / names that the раздел report already defines,
# so the two reports agree on what "ТАМЫЗ ағыны" or "4Т" means.
from subjects.informatics.section.constants import (
    STREAM_MONTH_ORDER, MONTH_NUM_TO_NAME,
)

# SMART umbrella covers these API products (same mapping the раздел report uses).
SMART_PRODUCTS = ["SMART", "EXPRESS", "INTENSIVE"]

# How many study months a SMART course runs (1-ай .. 5-ай).
MAX_STUDY_MONTH = 5


# ── Subject catalogue ─────────────────────────────────────────────────────────
# slug          — module name under subjects/ (also the build_group_all_weeks src)
# abbr          — the ПӘН label shown in the report (matches the Excel)
# subject_id    — platform UUID used to list that subject's courses
# subgroup      — "esep" | "oral" | "lang"  (drives which subtable it lands in)
# Order within each list is the row order in the report.

class _Subj:
    __slots__ = ("slug", "abbr", "subject_id", "subgroup")

    def __init__(self, slug, abbr, subject_id, subgroup):
        self.slug = slug
        self.abbr = abbr
        self.subject_id = subject_id
        self.subgroup = subgroup


_ESEP = [
    _Subj("informatics", "ИНФО", INFORMATICS_SUBJECT_ID, "esep"),
    _Subj("ms",          "МС",   MS_SUBJECT_ID,          "esep"),
    _Subj("geometry",    "ГЕОМ", GEOMETRY_SUBJECT_ID,    "esep"),
    _Subj("math",        "МАТ",  MATH_SUBJECT_ID,        "esep"),
    _Subj("chemistry",   "ХИМ",  CHEMISTRY_SUBJECT_ID,   "esep"),
    _Subj("physics",     "ФИЗ",  PHYSICS_SUBJECT_ID,     "esep"),
]

_ORAL = [
    _Subj("russian_literature", "ЛИТ",   RUSSIAN_LITERATURE_SUBJECT_ID, "oral"),
    _Subj("history",            "ТАРИХ", HISTORY_SUBJECT_ID,            "oral"),
    _Subj("geography",          "ГЕО",   GEOGRAPHY_SUBJECT_ID,          "oral"),
    _Subj("kazakh_literature",  "ӘДЕБ",  KAZAKH_LITERATURE_SUBJECT_ID,  "oral"),
    _Subj("world_history",      "ДЖТ",   WORLD_HISTORY_SUBJECT_ID,      "oral"),
    _Subj("biology",            "БИО",   BIOLOGY_SUBJECT_ID,            "oral"),
    _Subj("kukyk",              "ҚҰҚЫҚ", KUKYK_SUBJECT_ID,              "oral"),
]

_LANG = [
    _Subj("russian_language", "РУС",  RUSSIAN_LANGUAGE_SUBJECT_ID, "lang"),
    _Subj("kazakh_language",  "ТІЛ",  KAZAKH_LANGUAGE_SUBJECT_ID,  "lang"),
    _Subj("english",          "АНГЛ", ENGLISH_SUBJECT_ID,          "lang"),
]

# category key -> list of subjects, in report order
CATEGORY_SUBJECTS = {
    "esep":    _ESEP,
    "auyzsha": _ORAL + _LANG,
}

# category key -> ordered subgroups it renders as (subtables)
CATEGORY_SUBGROUPS = {
    "esep":    ["esep"],
    "auyzsha": ["oral", "lang"],
}

CATEGORY_LABEL = {
    "esep":    "ЕСЕП",
    "auyzsha": "АУЫЗША / ЖАЗБАША",
}

SUBGROUP_LABEL = {
    "esep": "ЕСЕП",
    "oral": "АУЫЗША",
    "lang": "ЖАЗБАША",
}


def subjects_for_category(category: str):
    return CATEGORY_SUBJECTS.get(category, [])


# ── Stream / study-month helpers ──────────────────────────────────────────────

def _report_number_today() -> int:
    """Position (1-based) of the current calendar month in the study year —
    i.e. how many "reports" deep into the year we are. ШІЛДЕ → 1, ТАМЫЗ → 2…"""
    m = date.today().month
    if m in STREAM_MONTH_ORDER:
        return STREAM_MONTH_ORDER.index(m) + 1
    return 1


def stream_position(stream_month: int) -> int:
    """The stream's "NТ" number — its position in the study year (ҚАРАША → 5)."""
    if stream_month in STREAM_MONTH_ORDER:
        return STREAM_MONTH_ORDER.index(stream_month) + 1
    return 0


def open_streams(report_num: int | None = None) -> list[dict]:
    """Streams that have started, each with the study months currently open.

    A stream that enrolled at position P is, at report number R, in study
    month (R - P + 1). Everything from 1-ай up to that (capped at
    MAX_STUDY_MONTH, since a SMART course is 5 months) is "open" and offered
    to the user. Streams that haven't started yet (study month < 1) are
    omitted.

    Returns, newest-stream first:
        [{"stream_month": 11, "position": 4, "name": "ҚАРАША",
          "open_months": [1, 2, 3, 4, 5]}, ...]
    """
    if report_num is None:
        report_num = _report_number_today()

    out = []
    for stream_month in STREAM_MONTH_ORDER:
        position = STREAM_MONTH_ORDER.index(stream_month) + 1
        current_study = report_num - position + 1
        if current_study < 1:
            continue  # stream hasn't started yet
        open_count = min(current_study, MAX_STUDY_MONTH)
        out.append({
            "stream_month": stream_month,
            "position":     position,
            "name":         MONTH_NUM_TO_NAME.get(stream_month, str(stream_month)),
            "open_months":  list(range(1, open_count + 1)),
        })
    # Show the most recent streams first (highest position).
    out.sort(key=lambda s: s["position"], reverse=True)
    return out
