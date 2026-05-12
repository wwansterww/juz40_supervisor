# subjects/geometry/metrics.py
from typing import Optional
from subjects.informatics.metrics import (
    METRIC_KEYS, PERCENT_COLS, SCORE_COLS,
    empty_metrics_info, merge_metrics_info, metrics_to_row, compute_avg_row_info,
    extract_metrics as info_extract_metrics,
)
from subjects.common import safe_pct, avg_of


def empty_metrics_geo():
    return empty_metrics_info()


def merge_metrics_geo(all_metrics: list) -> dict:
    return merge_metrics_info(all_metrics)


def compute_avg_row(rows: list) -> Optional[dict]:
    return compute_avg_row_info(rows)


def extract_metrics(summary: list, theme_name_upper: str) -> dict:
    m = info_extract_metrics(summary, theme_name_upper)

    if "КОНСПЕКТ" in theme_name_upper:
        k_pcts, k_scores = [], []
        for item in summary:
            name = (item.get("name") or "").upper()
            if "КОНСПЕКТ" not in name:
                continue
            sc = item.get("studentsCount") or item.get("totalStudentsCount") or 0
            sub = item.get("submittedCount") or 0
            p = safe_pct(sub, sc)
            if p is not None:
                k_pcts.append(p)
            score = item.get("averageScore")
            if score is not None:
                k_scores.append(score)
        m["konspekt_pct"] = avg_of(k_pcts)
        m["konspekt_score"] = avg_of(k_scores)

    return m