from typing import Optional
from subjects.common import safe_pct, avg_of, fmt, empty_metrics, merge_metrics, is_quiz_theme
from subjects.common import compute_avg_row as _compute_avg_row

METRIC_KEYS = [
    "jumys_dapter_pct", "jumys_dapter_score",
    "quiz_pct", "quiz_score",
    "praktika_pct",
    "sabak_pct", "sabak_score",
]

PERCENT_COLS = [
    "Жұмыс дәптері %",
    "Практикалық сабақ %",
    "Quiz %",
    "Сабақ тапсыру %",
]

SCORE_COLS = [
    "Жұмыс дәптері балл",
    "Quiz балл",
    "Сабақ тапсыру балл",
]


def empty_metrics_biology():
    return empty_metrics(METRIC_KEYS)


def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = empty_metrics_biology()

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

        m["jumys_dapter_pct"] = avg_of(pcts)
        m["jumys_dapter_score"] = avg_of(scores)

    # ПРАКТИКАЛЫҚ САБАҚ / ПС
    if "ПРАКТИКАЛЫҚ" in theme_name_upper or "ПС" in theme_name_upper:
        pr = []

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
                pr.append(p)

        m["praktika_pct"] = avg_of(pr)

    # QUIZ — match the normalized theme name against pre-normalized keywords
    # so themes named "QUIZIZ TEST" / "QUIZZ ТЕСТ" / "КВИЗ" all hit. Skip
    # "ҚАЙТАЛАУ ТЕСТ" themes — those are aggregated under САБАҚ ТАПСЫРУ.
    if is_quiz_theme(theme_name_upper):
        qp, qs = [], []

        for item in summary:
            name = (item.get("name") or "").upper()

            if "ҚАЙТАЛАУ" in name:
                continue

            if item.get("parentId") is not None:
                continue

            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)
            if p is not None:
                qp.append(p)

            score = item.get("averageScore")
            if score is not None:
                qs.append(score)

        m["quiz_pct"] = avg_of(qp)
        m["quiz_score"] = avg_of(qs)

    # САБАҚ ТАПСЫРУ / ҚАЙТАЛАУ ТЕСТ
    if (
        "САБАҚ ТАПСЫРУ" in theme_name_upper
        or "ҚАЙТАЛАУ ТЕСТ" in theme_name_upper
    ):
        sp, ss = [], []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)
            if p is not None:
                sp.append(p)

            score = item.get("averageScore")
            if score is not None:
                ss.append(score)

        m["sabak_pct"] = avg_of(sp)
        m["sabak_score"] = avg_of(ss)

    return m


def merge_metrics_biology(all_metrics: list) -> dict:
    return merge_metrics(all_metrics, METRIC_KEYS)


def metrics_to_row(base: dict, m: dict) -> dict:
    return {
        **base,
        "Жұмыс дәптері %": fmt(m.get("jumys_dapter_pct")),
        "Жұмыс дәптері балл": fmt(m.get("jumys_dapter_score")),
        "Практикалық сабақ %": fmt(m.get("praktika_pct")),
        "Quiz %": fmt(m.get("quiz_pct")),
        "Quiz балл": fmt(m.get("quiz_score")),
        "Сабақ тапсыру %": fmt(m.get("sabak_pct")),
        "Сабақ тапсыру балл": fmt(m.get("sabak_score")),
    }


def compute_avg_row(rows: list) -> Optional[dict]:
    return _compute_avg_row(rows, PERCENT_COLS, SCORE_COLS)