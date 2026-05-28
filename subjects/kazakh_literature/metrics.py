from typing import Optional
from subjects.common import safe_pct, avg_of, fmt, empty_metrics, merge_metrics, is_quiz_theme
from subjects.common import compute_avg_row as _compute_avg_row

METRIC_KEYS = [
    "video_pct",
    "quiz_pct", "quiz_score",
    "jd_pct", "jd_score",
    "ps_pct",
    "shygarma_pct", "shygarma_score",
    "sabak_pct", "sabak_score",
]

PERCENT_COLS = [
    "Видео сабақ %",
    "Quiz %",
    "Жұмыс дәптері %",
    "Практикалық сабақ %",
    "Шығарма талдау/куиз %",
    "Сабақ тапсыру %",
]

SCORE_COLS = [
    "Quiz балл",
    "Жұмыс дәптері балл",
    "Шығарма талдау/куиз балл",
    "Сабақ тапсыру балл",
]


def empty_metrics_kazakh_literature():
    return empty_metrics(METRIC_KEYS)


def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = empty_metrics_kazakh_literature()

    # ВИДЕОСАБАҚ
    if "ВИДЕО" in theme_name_upper or "ВИДЕОСАБАҚ" in theme_name_upper:
        vals = []

        for item in summary:
            if item.get("lessonType") == "LECTURE":
                v = item.get("averageVideoViewing")

                if v is not None:
                    vals.append(min(v, 100))
                else:
                    sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
                    sub = item.get("submittedCount") or 0
                    p = safe_pct(sub, sc)
                    if p is not None:
                        vals.append(p)

        m["video_pct"] = avg_of(vals)

    # QUIZ / КУИЗ — use the shared helper so themes whose names have been
    # normalized to Cyrillic "QUІZ" still match. ШЫҒАРМА themes are handled
    # by their own block below.
    if is_quiz_theme(theme_name_upper) and "ШЫҒАРМА" not in theme_name_upper:
        pcts, scores = [], []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)
            if p is not None:
                pcts.append(p)

            score = item.get("averageScore")
            if score is not None:
                scores.append(score)

        m["quiz_pct"] = avg_of(pcts)
        m["quiz_score"] = avg_of(scores)

    # ЖҰМЫС ДӘПТЕРІ
    if "ЖҰМЫС ДӘПТЕРІ" in theme_name_upper:
        pcts, scores = [], []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)
            if p is not None:
                pcts.append(p)

            score = item.get("averageScore")
            if score is not None:
                scores.append(score)

        m["jd_pct"] = avg_of(pcts)
        m["jd_score"] = avg_of(scores)

    # ПРАКТИКАЛЫҚ САБАҚ / ПС
    if "ПРАКТИКАЛЫҚ" in theme_name_upper or "ПС" in theme_name_upper:
        vals = []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            if item.get("general") is False:
                continue

            name = (item.get("name") or "").upper()

            if "ПРАКТИКАЛЫҚ" not in name and "ПС" not in name:
                continue

            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)
            if p is not None:
                vals.append(p)

        m["ps_pct"] = avg_of(vals)

    # ШЫҒАРМА ТАЛДАУ / ШЫҒАРМА КУИЗ
    if "ШЫҒАРМА" in theme_name_upper:
        pcts, scores = [], []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)
            if p is not None:
                pcts.append(p)

            score = item.get("averageScore")
            if score is not None:
                scores.append(score)

        m["shygarma_pct"] = avg_of(pcts)
        m["shygarma_score"] = avg_of(scores)

    # САБАҚ ТАПСЫРУ
    if "САБАҚ ТАПСЫРУ" in theme_name_upper or "ҚАЙТАЛАУ ТЕСТ" in theme_name_upper:
        pcts, scores = [], []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)
            if p is not None:
                pcts.append(p)

            score = item.get("averageScore")
            if score is not None:
                scores.append(score)

        m["sabak_pct"] = avg_of(pcts)
        m["sabak_score"] = avg_of(scores)

    return m


def merge_metrics_kazakh_literature(all_metrics: list) -> dict:
    return merge_metrics(all_metrics, METRIC_KEYS)


def metrics_to_row(base: dict, m: dict) -> dict:
    return {
        **base,
        "Видео сабақ %": fmt(m.get("video_pct")),
        "Quiz %": fmt(m.get("quiz_pct")),
        "Quiz балл": fmt(m.get("quiz_score")),
        "Жұмыс дәптері %": fmt(m.get("jd_pct")),
        "Жұмыс дәптері балл": fmt(m.get("jd_score")),
        "Практикалық сабақ %": fmt(m.get("ps_pct")),
        "Шығарма талдау/куиз %": fmt(m.get("shygarma_pct")),
        "Шығарма талдау/куиз балл": fmt(m.get("shygarma_score")),
        "Сабақ тапсыру %": fmt(m.get("sabak_pct")),
        "Сабақ тапсыру балл": fmt(m.get("sabak_score")),
    }


def compute_avg_row(rows: list) -> Optional[dict]:
    return _compute_avg_row(rows, PERCENT_COLS, SCORE_COLS)