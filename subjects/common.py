from typing import Optional
from utils import normalize

# ── Normalized theme-keyword sets ─────────────────────────────────────────────
# Theme names reach extract_metrics() AFTER utils.normalize() has replaced
# Latin look-alike letters (I → І, T → Т, E → Е, S → С, ...) with their
# Cyrillic counterparts. Comparing a raw Latin "QUIZ" against a normalized
# theme name like "QUІZІZ ТЕСТ" silently fails — the substring isn't there
# because the letters are different code points. Pre-normalizing the
# keywords once (here) and reusing them everywhere keeps both sides of the
# comparison in the same alphabet system.
QUIZ_KW = tuple({
    normalize(k) for k in ("QUIZ", "КУИЗ", "КВИЗ", "QUIZIZ", "QUIZIZZ", "TEST")
})

# ҚАЙТАЛАУ (Repeat) handling — curators sometimes type the theme name with a
# typo, dropping the second "А" (ҚАЙТАЛУ instead of ҚАЙТАЛАУ). Both spellings
# show up on the platform, so every check needs to tolerate both. The
# normalized forms below are used by the QUIZ-theme matcher; the raw helpers
# below are used by per-subject metric extractors.
_REPEAT_KW_VARIANTS = tuple({normalize(k) for k in ("ҚАЙТАЛАУ", "ҚАЙТАЛУ")})


def is_quiz_theme(theme_name_upper: str) -> bool:
    """True if the (already normalized) theme name is a QUIZ theme.
    Excludes repeat-tests (ҚАЙТАЛАУ ТЕСТ / ҚАЙТАЛУ ТЕСТ), which belong to
    САБАҚ ТАПСЫРУ rather than QUIZ."""
    if not any(kw in theme_name_upper for kw in QUIZ_KW):
        return False
    return not any(rk in theme_name_upper for rk in _REPEAT_KW_VARIANTS)


def is_kaitalau_test(text_upper: str) -> bool:
    """True if the string contains "ҚАЙТАЛАУ ТЕСТ" with EITHER spelling.

    Use this for theme- or item-name checks that mean "is this a Repeat
    Test". Direct ``"ҚАЙТАЛАУ ТЕСТ" in text`` will silently miss the typo'd
    form curators sometimes type.
    """
    return ("ҚАЙТАЛАУ ТЕСТ" in text_upper) or ("ҚАЙТАЛУ ТЕСТ" in text_upper)


def has_kaitalau(text_upper: str) -> bool:
    """True if the string contains the ҚАЙТАЛАУ root (any spelling).
    Used when filtering out Repeat-Test items inside a QUIZ summary."""
    return ("ҚАЙТАЛАУ" in text_upper) or ("ҚАЙТАЛУ" in text_upper)


def safe_pct(submitted, total):
    if not total:
        return None
    return min(round(submitted / total * 100, 1), 100)

def avg_of(values):
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return round(sum(vals) / len(vals), 1)

def fmt(val):
    return "-" if val is None else val

def empty_metrics(keys: list) -> dict:
    return {k: None for k in keys}

def merge_metrics(all_metrics: list, keys: list) -> dict:
    merged = {}
    for k in keys:
        vals = [mm[k] for mm in all_metrics if mm.get(k) is not None]
        merged[k] = avg_of(vals)
    return merged

def weighted_avg(rows, pct_key, count_key):
    total_students = 0
    total_submitted = 0
    for row in rows:
        pct = row.get(pct_key)
        count = row.get(count_key)
        if pct == "-" or pct is None or not count:
            continue
        try:
            pct = float(pct)
            count = float(count)
            total_submitted += pct / 100 * count
            total_students += count
        except (ValueError, TypeError):
            pass
    if total_students == 0:
        return "-"
    return round(total_submitted / total_students * 100, 1)

def compute_avg_row(rows: list, percent_cols: list, score_cols: list) -> Optional[dict]:
    if not rows:
        return None
    avg_row = {
        "Поток": "—",
        "Куратор": "⌀ Орта көрсеткіш",
        "Оқушы саны": sum(
            r["Оқушы саны"] for r in rows
            if isinstance(r.get("Оқушы саны"), (int, float))
        ),
    }
    for col in percent_cols:
        avg_row[col] = weighted_avg(rows, col, "Оқушы саны")
    for col in score_cols:
        vals = []
        for r in rows:
            v = r.get(col)
            if v != "-" and v is not None:
                try:
                    vals.append(float(v))
                except (ValueError, TypeError):
                    pass
        avg_row[col] = round(sum(vals) / len(vals), 1) if vals else "-"
    return avg_row