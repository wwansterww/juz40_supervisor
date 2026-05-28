from typing import Optional
from subjects.common import (
    safe_pct,
    avg_of,
    fmt,
    empty_metrics,
    merge_metrics,
    is_quiz_theme,
)
from subjects.common import compute_avg_row as _compute_avg_row


METRIC_KEYS = [
    "video_pct",

    "shygarma_pct",
    "shygarma_score",

    "sht_qzh_pct",
    "sht_qzh_score",

    "quiz_pct",
    "quiz_score",

    "ps_pct",

    "sabak_pct",
    "sabak_score",
]


PERCENT_COLS = [
    "Видео сабақ %",
    "Шығарма талдау %",
    "ШТ ҚЖ %",
    "Quiz %",
    "Практикалық сабақ %",
    "Сабақ тапсыру %",
]

SCORE_COLS = [
    "Шығарма талдау балл",
    "ШТ ҚЖ балл",
    "Quiz балл",
    "Сабақ тапсыру балл",
]


def empty_metrics_russian_literature():
    return empty_metrics(METRIC_KEYS)


def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = empty_metrics_russian_literature()

    # ВИДЕО
    if "ВИДЕО" in theme_name_upper:
        vals = []

        for item in summary:
            if item.get("lessonType") == "LECTURE":
                v = item.get("averageVideoViewing")

                if v is not None:
                    vals.append(min(v, 100))

        m["video_pct"] = avg_of(vals)

    # ШЫҒАРМА ТАЛДАУ + ШТ ҚЖ
    if "ШЫҒАРМА" in theme_name_upper:
        shygarma_pcts, shygarma_scores = [], []
        qzh_pcts, qzh_scores = [], []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            name = (item.get("name") or "").upper()

            # негізгі Шығарма талдау
            if "ШЫҒАРМА" in name and "ҚЖ" not in name:
                sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
                sub = item.get("submittedCount") or 0

                p = safe_pct(sub, sc)
                if p is not None:
                    shygarma_pcts.append(p)

                score = item.get("averageScore")
                if score is not None:
                    shygarma_scores.append(score)

            # children ішіндегі ШТ ҚЖ
            for child in item.get("children", []):
                child_name = (child.get("name") or "").upper()

                if "ҚЖ" not in child_name:
                    continue

                c_sc = child.get("studentsCount") or child.get("totalStudentsCount") or 0
                c_sub = child.get("submittedCount") or 0

                cp = safe_pct(c_sub, c_sc)
                if cp is not None:
                    qzh_pcts.append(cp)

                c_score = child.get("averageScore")
                if c_score is not None:
                    qzh_scores.append(c_score)

        m["shygarma_pct"] = avg_of(shygarma_pcts)
        m["shygarma_score"] = avg_of(shygarma_scores)

        m["sht_qzh_pct"] = avg_of(qzh_pcts)
        m["sht_qzh_score"] = avg_of(qzh_scores)

    # QUIZ
    if is_quiz_theme(theme_name_upper):
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

    # ПС
    if (
        "ПС" in theme_name_upper
        or "ПРАКТИКАЛЫҚ" in theme_name_upper
    ):
        vals = []

        for item in summary:
            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)

            if p is not None:
                vals.append(p)

        m["ps_pct"] = avg_of(vals)

    # САБАҚ ТАПСЫРУ
    if (
        "САБАҚ ТАПСЫРУ" in theme_name_upper
        or "ҚАЙТАЛАУ ТЕСТ" in theme_name_upper
    ):
        pcts, scores = [], []

        for item in summary:
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


def merge_metrics_russian_literature(all_metrics: list) -> dict:
    return merge_metrics(all_metrics, METRIC_KEYS)


def metrics_to_row(base: dict, m: dict) -> dict:
    return {
        **base,

        "Видео сабақ %": fmt(m.get("video_pct")),

        "Шығарма талдау %": fmt(m.get("shygarma_pct")),
        "Шығарма талдау балл": fmt(m.get("shygarma_score")),

        "ШТ ҚЖ %": fmt(m.get("sht_qzh_pct")),
        "ШТ ҚЖ балл": fmt(m.get("sht_qzh_score")),

        "Quiz %": fmt(m.get("quiz_pct")),
        "Quiz балл": fmt(m.get("quiz_score")),

        "Практикалық сабақ %": fmt(m.get("ps_pct")),

        "Сабақ тапсыру %": fmt(m.get("sabak_pct")),
        "Сабақ тапсыру балл": fmt(m.get("sabak_score")),
    }


def compute_avg_row(rows: list) -> Optional[dict]:
    return _compute_avg_row(rows, PERCENT_COLS, SCORE_COLS)