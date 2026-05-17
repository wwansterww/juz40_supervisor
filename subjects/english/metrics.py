from typing import Optional
from subjects.common import safe_pct, avg_of, fmt, empty_metrics, merge_metrics
from subjects.common import compute_avg_row as _compute_avg_row


def restore_latin(s: str) -> str:
    table = str.maketrans({
        "А": "A",
        "В": "B",
        "С": "C",
        "Е": "E",
        "Н": "H",
        "І": "I",
        "К": "K",
        "М": "M",
        "О": "O",
        "Р": "P",
        "Т": "T",
        "Х": "X",
        "У": "Y",
    })
    return s.translate(table)


METRIC_KEYS = [
    "video_pct",
    "conspect_pct",

    "uy_pct", "uy_score",
    "qj_pct", "qj_score",

    "quiz_pct", "quiz_score",

    "reading_pct", "reading_score",
    "reading_qj_pct", "reading_qj_score",

    "praktika_pct",

    "sabak_pct", "sabak_score",
]


PERCENT_COLS = [
    "Видео сабақ %",
    "Конспект %",
    "Үй жұмысы %",
    "ҚЖ %",
    "Quiz %",
    "Reading task %",
    "Reading task ҚЖ %",
    "Практикалық сабақ %",
    "Сабақ тапсыру %",
]


SCORE_COLS = [
    "Үй жұмысы балл",
    "ҚЖ балл",
    "Quiz балл",
    "Reading task балл",
    "Reading task ҚЖ балл",
    "Сабақ тапсыру балл",
]


def empty_metrics_english():
    return empty_metrics(METRIC_KEYS)


def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = empty_metrics_english()

    theme_en = restore_latin(theme_name_upper)

    # ВИДЕОСАБАҚ = VIDEO LESSONS
    if (
            "ВИДЕО" in theme_name_upper
            or "VIDEO" in theme_en
    ):
        vals = []

        for item in summary:
            name = restore_latin((item.get("name") or "").upper())

            if item.get("lessonType") != "LECTURE":
                continue

            if "PRACTICE" in name:
                continue

            v = item.get("averageVideoViewing")

            if v is not None:
                vals.append(min(v, 100))

        m["video_pct"] = avg_of(vals)

    # КОНСПЕКТ = SUMMARY
    if (
            "КОНСПЕКТ" in theme_name_upper
            or "SUMMARY" in theme_en
            or "CONSPECT" in theme_en
            or any(
        "SUMMARY" in restore_latin((item.get("name") or "").upper())
        for item in summary
    )
    ):
        vals = []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            name_en = restore_latin((item.get("name") or "").upper())

            if (
                    "SUMMARY" not in name_en
                    and "CONSPECT" not in name_en
                    and "КОНСПЕКТ" not in (item.get("name") or "").upper()
            ):
                continue

            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)
            if p is not None:
                vals.append(p)

        m["conspect_pct"] = avg_of(vals)

        # ҮЙ ЖҰМЫСЫ + ҚЖ
    if (
            "ҮЙ ЖҰМЫСЫ" in theme_name_upper
            or "ҮЙ ЖУМЫСЫ" in theme_name_upper
            or "HOMEWORK" in theme_en
            or "HOME WORK" in theme_en
            or "HOME TASK" in theme_en
    ):
        uy_pcts, uy_scores = [], []
        qj_pcts, qj_scores = [], []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            name = (item.get("name") or "").upper()

            # негізгі үй жұмысы
            if "ҚЖ" not in name and "ҚАТЕМЕН" not in name:
                sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
                sub = item.get("submittedCount") or 0

                p = safe_pct(sub, sc)
                if p is not None:
                    uy_pcts.append(p)

                score = item.get("averageScore")
                if score is not None:
                    uy_scores.append(score)

            # child ішіндегі ҚЖ / Қатемен жұмыс
            for child in item.get("children", []):
                child_name = (child.get("name") or "").upper()

                if "ҚЖ" not in child_name and "ҚАТЕМЕН" not in child_name:
                    continue

                c_sc = child.get("studentsCount") or child.get("totalStudentsCount") or 0
                c_sub = child.get("submittedCount") or 0

                cp = safe_pct(c_sub, c_sc)
                if cp is not None:
                    qj_pcts.append(cp)

                c_score = child.get("averageScore")
                if c_score is not None:
                    qj_scores.append(c_score)

        m["uy_pct"] = avg_of(uy_pcts)
        m["uy_score"] = avg_of(uy_scores)
        m["qj_pct"] = avg_of(qj_pcts)
        m["qj_score"] = avg_of(qj_scores)

    # ЖЕКЕ ҚЖ ТЕМА БОЛЫП КЕЛСЕ
    if (
            "ҚЖ" in theme_name_upper
            or "ҚАТЕМЕН" in theme_name_upper
    ) and "READING" not in theme_en:
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

        m["qj_pct"] = avg_of(pcts)
        m["qj_score"] = avg_of(scores)

    # QUIZ / QUIZIZZ
    if (
        "QUIZ" in theme_en
        or "КУИЗ" in theme_name_upper
        or "КВИЗ" in theme_name_upper
    ):
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

    # READING TASK + READING TASK ҚЖ
    if "READING" in theme_en:
        reading_pcts, reading_scores = [], []
        qj_pcts, qj_scores = [], []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            name = restore_latin((item.get("name") or "").upper())

            if "READING" in name and "ҚЖ" not in name:
                sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
                sub = item.get("submittedCount") or 0

                p = safe_pct(sub, sc)
                if p is not None:
                    reading_pcts.append(p)

                score = item.get("averageScore")
                if score is not None:
                    reading_scores.append(score)

            for child in item.get("children", []):
                child_name = restore_latin((child.get("name") or "").upper())

                if "READING" not in child_name or "ҚЖ" not in child_name:
                    continue

                c_sc = child.get("studentsCount") or child.get("totalStudentsCount") or 0
                c_sub = child.get("submittedCount") or 0

                cp = safe_pct(c_sub, c_sc)
                if cp is not None:
                    qj_pcts.append(cp)

                c_score = child.get("averageScore")
                if c_score is not None:
                    qj_scores.append(c_score)

        m["reading_pct"] = avg_of(reading_pcts)
        m["reading_score"] = avg_of(reading_scores)
        m["reading_qj_pct"] = avg_of(qj_pcts)
        m["reading_qj_score"] = avg_of(qj_scores)

    # ПРАКТИКАЛЫҚ САБАҚ = PRACTICE LESSON
    if (
        "PRACTICE" in theme_en
        or "ПРАКТИКАЛЫҚ" in theme_name_upper
        or "ПС" in theme_name_upper
    ):
        vals = []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            name = restore_latin((item.get("name") or "").upper())

            if (
                "PRACTICE" not in name
                and "ПРАКТИКАЛЫҚ" not in name
                and "ПС" not in name
            ):
                continue

            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)
            if p is not None:
                vals.append(p)

        m["praktika_pct"] = avg_of(vals)

    # САБАҚ ТАПСЫРУ
    if (
            "САБАҚ ТАПСЫРУ" in theme_name_upper
            or "CАБАҚ ТАПСЫРУ" in theme_name_upper
            or "PRACTICE" in theme_en
            or "CT" in theme_en
    ):
        pcts, scores = [], []

        for item in summary:
            name = (item.get("name") or "").upper()
            name_en = restore_latin(name)

            is_sabak = (
                    item.get("lessonType") == "ORAL"
                    or "САБАҚ ТАПСЫРУ" in name
                    or "CАБАҚ ТАПСЫРУ" in name
                    or "CT" in name_en
            )

            if not is_sabak:
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


def merge_metrics_english(all_metrics: list) -> dict:
    return merge_metrics(all_metrics, METRIC_KEYS)


def metrics_to_row(base: dict, m: dict) -> dict:
    return {
        **base,
        "Видео сабақ %": fmt(m.get("video_pct")),
        "Конспект %": fmt(m.get("conspect_pct")),
        "Үй жұмысы %": fmt(m.get("uy_pct")),
        "Үй жұмысы балл": fmt(m.get("uy_score")),
        "ҚЖ %": fmt(m.get("qj_pct")),
        "ҚЖ балл": fmt(m.get("qj_score")),
        "Quiz %": fmt(m.get("quiz_pct")),
        "Quiz балл": fmt(m.get("quiz_score")),
        "Reading task %": fmt(m.get("reading_pct")),
        "Reading task балл": fmt(m.get("reading_score")),
        "Reading task ҚЖ %": fmt(m.get("reading_qj_pct")),
        "Reading task ҚЖ балл": fmt(m.get("reading_qj_score")),
        "Практикалық сабақ %": fmt(m.get("praktika_pct")),
        "Сабақ тапсыру %": fmt(m.get("sabak_pct")),
        "Сабақ тапсыру балл": fmt(m.get("sabak_score")),
    }


def compute_avg_row(rows: list) -> Optional[dict]:
    return _compute_avg_row(rows, PERCENT_COLS, SCORE_COLS)