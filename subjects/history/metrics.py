from typing import Optional
from subjects.common import safe_pct, avg_of, fmt, empty_metrics, merge_metrics
from subjects.common import compute_avg_row as _compute_avg_row

METRIC_KEYS = [
    "video",
    "jumys_dapter_pct", "jumys_dapter_score",
    "quiz_pct", "quiz_score",
    "praktika_pct",
    "karta_pct", "karta_score",
    "sabak_pct", "sabak_score",
]

PERCENT_COLS = [
    "Видео сабақ %",
    "Жұмыс дәптері %",
    "Quiz %",
    "Практикалық сабақ %",
    "Картамен жұмыс %",
    "Сабақ тапсыру %",
]

SCORE_COLS = [
    "Жұмыс дәптері балл",
    "Quiz балл",
    "Картамен жұмыс балл",
    "Сабақ тапсыру балл",
]


def empty_metrics_history():
    return empty_metrics(METRIC_KEYS)


def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = empty_metrics_history()

    # ВИДЕОСАБАҚ
    if "ВИДЕОСАБАҚ" in theme_name_upper:
        video_vals = []

        for item in summary:
            if item.get("lessonType") == "LECTURE":
                v = item.get("averageVideoViewing")

                if v is not None:
                    video_vals.append(min(v, 100))

        m["video"] = avg_of(video_vals)

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

    # QUIZ
    # Тек QUIZ / КУИЗ кіреді.
    # ҚАЙТАЛАУ ТЕСТ quiz емес, ол Сабақ тапсыруға fallback ретінде кіреді.
    qp, qs = [], []

    for item in summary:
        name = (item.get("name") or "").upper()

        is_quiz = (
            "QUIZ" in name
            or "КУИЗ" in name
            or "КВИЗ" in name
            or "QUIZIZ" in name
        )

        if not is_quiz:
            continue

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

    if qp:
        m["quiz_pct"] = avg_of(qp)

    if qs:
        m["quiz_score"] = avg_of(qs)

    # ПРАКТИКАЛЫҚ САБАҚ
    # TEXT сабақтар ғана. "ПС КН" сияқты individual тапсырмалар кірмейді.
    if "ПРАКТИКАЛЫҚ" in theme_name_upper or "ПС" in theme_name_upper:
        pr = []

        for item in summary:
            if item.get("lessonType") != "TEXT":
                continue

            if item.get("general") is False:
                continue

            if item.get("parentId") is not None:
                continue

            name = (item.get("name") or "").upper()

            if "ПС КН" in name:
                continue

            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)

            if p is not None:
                pr.append(p)

        m["praktika_pct"] = avg_of(pr)


    if "КАРТАМЕН ЖҰМЫС" in theme_name_upper:
        kp, ks = [], []

        for item in summary:
            if item.get("lessonType") != "TASK":
                continue

            if item.get("parentId") is not None:
                continue

            name = (item.get("name") or "").upper()

            if "КАРТАМЕН ЖҰМЫС" not in name:
                continue

            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0

            p = safe_pct(sub, sc)
            if p is not None:
                kp.append(p)

            score = item.get("averageScore")
            if score is not None:
                ks.append(score)

        m["karta_pct"] = avg_of(kp)
        m["karta_score"] = avg_of(ks)

    # САБАҚ ТАПСЫРУ
    # Егер нақты САБАҚ ТАПСЫРУ жоқ болса, ҚАЙТАЛАУ ТЕСТ осы бағанға кіреді.
    if (
        "САБАҚ ТАПСЫРУ" in theme_name_upper
        or "ҚАЙТАЛАУ ТЕСТ" in theme_name_upper
    ):
        sp, ss = [], []

        for item in summary:
            if item.get("parentId") is not None:
                continue

            name = (item.get("name") or "").upper()

            is_sabak = "САБАҚ ТАПСЫРУ" in name
            is_kaitalau = "ҚАЙТАЛАУ ТЕСТ" in name

            if not is_sabak and not is_kaitalau:
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


def merge_metrics_history(all_metrics: list) -> dict:
    return merge_metrics(all_metrics, METRIC_KEYS)


def metrics_to_row(base: dict, m: dict) -> dict:
    return {
        **base,
        "Видео сабақ %": fmt(m.get("video")),
        "Жұмыс дәптері %": fmt(m.get("jumys_dapter_pct")),
        "Жұмыс дәптері балл": fmt(m.get("jumys_dapter_score")),
        "Quiz %": fmt(m.get("quiz_pct")),
        "Quiz балл": fmt(m.get("quiz_score")),
        "Практикалық сабақ %": fmt(m.get("praktika_pct")),
        "Картамен жұмыс %": fmt(m.get("karta_pct")),
        "Картамен жұмыс балл": fmt(m.get("karta_score")),
        "Сабақ тапсыру %": fmt(m.get("sabak_pct")),
        "Сабақ тапсыру балл": fmt(m.get("sabak_score")),
    }


def compute_avg_row(rows: list) -> Optional[dict]:
    return _compute_avg_row(rows, PERCENT_COLS, SCORE_COLS)