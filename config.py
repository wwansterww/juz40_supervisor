import os
import secrets
import sys

BASE_URL = "https://api.juz40-edu.kz"

INFORMATICS_SUBJECT_ID = "6e172165-57c2-4b01-9fd1-70ccca7b96a7"
MATH_SUBJECT_ID        = "11c81c50-c914-4030-8083-e5d4bfe6e6d0"
GEOMETRY_SUBJECT_ID    = "aefcbf13-8928-40a5-bddb-1b5c7eac2e07"
MS_SUBJECT_ID          = "e6d6f884-5f5a-46c0-9b5a-929051b9a3d8"
PHYSICS_SUBJECT_ID     = "0b14d605-8adb-436d-8953-e2472d4ff048"
CHEMISTRY_SUBJECT_ID   = "24beb7d0-dc5f-4e2e-a66c-c44b51af9e67"
GEOGRAPHY_SUBJECT_ID = "3019bfe4-8e9e-4c9a-9059-626d6dff3d48"
KUKYK_SUBJECT_ID       = "79d6a013-68f6-4829-a75f-f1618fc9c244"
HISTORY_SUBJECT_ID = "2f9a8bf5-4a39-4c5f-aa32-4c7ae09521b2"
WORLD_HISTORY_SUBJECT_ID = "8e0889f0-320c-405a-8d14-44fb9f396ea7"
BIOLOGY_SUBJECT_ID = "3a58ebfe-a668-4761-a5f4-53142a6571c1"
KAZAKH_LANGUAGE_SUBJECT_ID = "dc37f366-6f09-41e8-a5d3-3cd925fb72db"
KAZAKH_LITERATURE_SUBJECT_ID = "e82b4f1e-6a1f-4b75-b622-6703e2495520"
RUSSIAN_LANGUAGE_SUBJECT_ID = "4e0e069a-0ff8-4664-b01d-c491a69788ee"
RUSSIAN_LITERATURE_SUBJECT_ID = "58b3f11e-20cd-453a-a486-afa4cdf261f3"
ENGLISH_SUBJECT_ID = "ee08e1f3-3658-44d5-ab8b-206a5049ffc5"

# Session-cookie signing key. Without a key in the environment we generate a
# random one instead of falling back to a publicly-known placeholder (anyone
# who has seen the source could forge a session cookie with it). The cost of
# the random fallback: all sessions are invalidated on every restart — set
# SECRET_KEY in the environment for production.
SECRET_KEY = os.environ.get("SECRET_KEY")
if not SECRET_KEY:
    SECRET_KEY = secrets.token_hex(32)
    print(
        "WARNING: SECRET_KEY is not set — using a random per-start key. "
        "Sessions will not survive a restart. Set the SECRET_KEY environment "
        "variable in production.",
        file=sys.stderr,
    )

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


# ── Session cookie hardening ─────────────────────────────────────────────────
# SESSION_HTTPS_ONLY marks the session cookie Secure (only sent over HTTPS).
# Default False so local http:// development still works; set it to 1/true in
# production (behind HTTPS) so the cookie can't leak over a plain connection.
# SESSION_MAX_AGE bounds how long a session stays valid (default 14 days).
def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


SESSION_HTTPS_ONLY = _env_bool("SESSION_HTTPS_ONLY", False)
SESSION_MAX_AGE = int(os.environ.get("SESSION_MAX_AGE", 14 * 24 * 3600))


# ── Scaling / concurrency tunables ───────────────────────────────────────────
# Everything below can be overridden via environment variables so the app can
# be tuned for the host's CPU count and the external API's tolerance WITHOUT
# touching code. The *_TOTAL values describe the WHOLE deployment; when running
# with several uvicorn workers each worker self-limits to its fair share
# (TOTAL // WEB_CONCURRENCY), so the combined load on api.juz40-edu.kz never
# exceeds the total no matter how many workers you start.

def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        val = int(raw.strip())
    except (ValueError, AttributeError):
        return default
    return val if val > 0 else default


def _env_int_min0(name: str, default: int) -> int:
    """Like _env_int but allows 0 (used for knobs where 0 means 'disabled')."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        val = int(raw.strip())
    except (ValueError, AttributeError):
        return default
    return val if val >= 0 else default


# Number of uvicorn worker processes. Set by the run script (run_prod.sh) and
# inherited by every worker, so the per-worker division below is consistent
# across all of them. Defaults to 1 (the old single-worker behaviour).
WEB_CONCURRENCY = _env_int("WEB_CONCURRENCY", 1)

# Whole-deployment ceilings (split across workers just below).
#   API_LIMIT_TOTAL   — max parallel HTTP requests to the external API across
#                       the ENTIRE deployment. The API tolerates ~250-300.
#   REPORT_SLOT_TOTAL — max reports building at once across the deployment.
API_LIMIT_TOTAL   = _env_int("API_LIMIT_TOTAL", 250)
REPORT_SLOT_TOTAL = _env_int("REPORT_SLOT_TOTAL", 10)

# Per-worker shares — what the in-process semaphores actually enforce. With
# WEB_CONCURRENCY=1 these equal the totals (unchanged single-worker behaviour).
GLOBAL_API_LIMIT  = max(1, API_LIMIT_TOTAL // WEB_CONCURRENCY)
REPORT_SLOT_LIMIT = max(1, REPORT_SLOT_TOTAL // WEB_CONCURRENCY)

# Max parallel requests a SINGLE report fans out to. Still funnelled through the
# per-worker API semaphore above, so this is a within-report cap, not a global
# one — the real ceiling on API load stays GLOBAL_API_LIMIT. Raised from 50 to
# 100 so a single/low-traffic report builds ~2x faster; when many reports run at
# once API_SEM (GLOBAL_API_LIMIT) is what actually bounds upstream load, so this
# higher per-report cap can't overload the API.
GLOBAL_SEMAPHORE_LIMIT = _env_int("REPORT_FANOUT_LIMIT", 100)

# L1 in-memory response cache size (entries). Bigger = fewer Redis round-trips
# when many concurrent reports request overlapping data, at the cost of RAM.
L1_CACHE_MAX = _env_int("L1_CACHE_MAX", 8192)

# Upper bound on the shared Redis connection pool per worker. Without a bound,
# a burst of concurrent requests can open thousands of sockets to Redis.
REDIS_MAX_CONNECTIONS = _env_int("REDIS_MAX_CONNECTIONS", 100)


CACHE_TTL = 600  # default 10 min

# Differentiated TTL per endpoint type (seconds)
CACHE_TTL_BY_TYPE = {
    "students":  300,    # 5 min  — may change during the month
    "groups":    900,    # 15 min — group list is fairly stable
    "courses":   1800,   # 30 min — course list changes rarely
    "progresses": 1800,  # 30 min — student progress per lesson
    "summary":   3600,   # 60 min — lesson summary is stable within a day
    "themes":    3600,   # 60 min — theme list doesn't change
}

# ── Empty-response handling (the "report came back empty" fix) ────────────────
# The upstream occasionally answers 200 OK with an empty body (no students /
# themes / lessons) when the data momentarily "didn't take". Two guards:
#
#   EMPTY_CACHE_TTL     — empty responses are cached only this long instead of
#                         the full TTL above (themes/summary are 1h!), so a
#                         transient empty self-heals within a minute and a
#                         re-run actually re-fetches instead of serving the
#                         frozen empty again.
#   EMPTY_RETRY_ATTEMPTS — how many extra quick retries to give a *suspicious*
#                         empty 200 (students/progresses/summary/group list,
#                         where empty almost always means a glitch) before
#                         accepting it. Set 0 for max speed, higher for max
#                         reliability. themes/courses are excluded from this
#                         retry because an empty there is often legitimate.
EMPTY_CACHE_TTL      = _env_int("EMPTY_CACHE_TTL", 60)
EMPTY_RETRY_ATTEMPTS = _env_int_min0("EMPTY_RETRY_ATTEMPTS", 1)

# VPS is now its own top-level page (/vps/dashboard) rather than a course
# type pill. The list below is what regular subject dashboards show.
COURSE_TYPES = ["SMART", "TURBO", "EXPRESS", "JUNIOR", "INTENSIVE", "GENIUS", "PAKET"]


# ── VPS (multi-subject combined courses) ─────────────────────────────────────
# A VPS "pack" (e.g. ИНФО-МАТ) is one cohort of students that takes 5 distinct
# subject-courses simultaneously. In juz40-edu.kz each constituent is a normal
# course under its own subject_id, linked by a shared streamId. We aggregate
# them client-side into one combined report.

# Тариф levels — API uses `key` as the `product` query param, `label` is what
# we show as the section header inside each subject table.
VPS_PRODUCTS = [
    {"key": "SMART_VIP",      "label": "VIP",  "icon": "👑"},
    {"key": "SMART_PREMIUM",  "label": "PREM", "icon": "💜"},
    {"key": "SMART_STANDARD", "label": "STAN", "icon": "💎"},
]

# Map the trailing token of a VPS course name to its subject. Example:
#   "SMART STAN ИНФО-МАТ МАТ" → suffix "МАТ" → Math subject.
# `label` is what we show in the report (ГЕОМ for Geometry, etc).
VPS_SUFFIX_TO_SUBJECT = {
    "МАТ":   {"slug": "math",        "subject_id": "11c81c50-c914-4030-8083-e5d4bfe6e6d0", "label": "МАТ"},
    "ИНФО":  {"slug": "informatics", "subject_id": "6e172165-57c2-4b01-9fd1-70ccca7b96a7", "label": "ИНФО"},
    "ГЕО":   {"slug": "geometry",    "subject_id": "aefcbf13-8928-40a5-bddb-1b5c7eac2e07", "label": "ГЕОМ"},
    "МС":    {"slug": "ms",          "subject_id": "e6d6f884-5f5a-46c0-9b5a-929051b9a3d8", "label": "МС"},
    "ТАРИХ": {"slug": "history",     "subject_id": "2f9a8bf5-4a39-4c5f-aa32-4c7ae09521b2", "label": "ТАРИХ"},
}

# Which constituent subjects each pack contains, in display order. Add new
# packs here as you start supporting them (ГЕО-МАТ, ФИЗ-МАТ, etc).
VPS_PACKS = {
    "ИНФО-МАТ": ["ИНФО", "МАТ", "ГЕО", "МС", "ТАРИХ"],
}

# Per-week subject visibility:
#   odd weeks (1, 3) → these subjects appear in the report
#   even weeks (2, 4) → these subjects appear
# Reflects the business rule that VPS курстары rotate subjects weekly.
VPS_WEEK_SUBJECTS = {
    "odd":  ["МАТ", "ТАРИХ"],
    "even": ["ИНФО", "МС", "ГЕО"],
}

# All VPS courses currently live in February — no point asking the user to
# pick a month. If this ever changes, replace with a per-stream lookup.
VPS_DEFAULT_MONTH = 2

COURSE_TYPE_TO_PRODUCTS = {
    "SMART":     ["SMART"],
    "TURBO":     ["TURBO"],
    "VPS":       ["SMART_STANDARD", "SMART_PREMIUM", "SMART_VIP"],
    "EXPRESS":   ["EXPRESS"],
    "JUNIOR":    ["JUNIOR"],
    "INTENSIVE": ["INTENSIVE"],
    "GENIUS":    ["GENIUS"],
    "PAKET":     ["PAKET"],
}

# ── Stream months ─────────────────────────────────────────────────────────────
# Single source of truth for stream-month names, numbers, and study-year order.
# The study year starts in July (ШІЛДЕ = 1Т) and runs through June. The раздел /
# СМАРТ reports import STREAM_MONTH_ORDER / MONTH_NUM_TO_NAME from here; the
# dropdown and the name↔number maps are DERIVED below — so the lists can never
# drift apart again (no month forgotten, order always matches the reports).
MONTH_NUM_TO_NAME = {
    1: "ҚАҢТАР", 2: "АҚПАН", 3: "НАУРЫЗ", 4: "СӘУІР", 5: "МАМЫР",
    6: "МАУСЫМ", 7: "ШІЛДЕ", 8: "ТАМЫЗ", 9: "ҚЫРКҮЙЕК",
    10: "ҚАЗАН", 11: "ҚАРАША", 12: "ЖЕЛТОҚСАН",
}

# Order the streams run in within a study year (July → June).
STREAM_MONTH_ORDER = [7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6]

# name → calendar-month number (reverse of MONTH_NUM_TO_NAME).
MONTH_NAME_TO_NUM = {name: num for num, name in MONTH_NUM_TO_NAME.items()}

# Dropdown options, in study-year order (ШІЛДЕ first) to match the reports.
STREAM_MONTHS = [MONTH_NUM_TO_NAME[m] for m in STREAM_MONTH_ORDER]

STUDY_MONTHS = ["1-ай", "2-ай", "3-ай", "4-ай", "5-ай"]

TYPE_NAME_KEYWORDS = {
    "SMART":     ["SMART"],
    "TURBO":     ["TURBO", " T ", " T-"],
    "VPS":       ["STANDARD", "PREMIUM", "VIP", "STAN", "PREM"],
    "EXPRESS":   ["EXPRESS"],
    "JUNIOR":    ["JUNIOR"],
    "INTENSIVE": ["INTENSIVE"],
    "GENIUS":    ["GENIUS"],
    "PAKET":     ["PAKET"],
}

TYPE_EXCLUDE_KEYWORDS = {
    "SMART": ["STANDARD", "PREMIUM", "VIP", "STAN", "PREM"],
}