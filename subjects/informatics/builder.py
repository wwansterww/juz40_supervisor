"""Informatics report builder.

All report logic lives in subjects.base_builder.make_builder; this module just
wires the subject's metric functions into it. (Previously each subject hand-
rolled its own _process_single_course + _build_section_report_job.)

CLIENT_LIMITS and GLOBAL_SEMAPHORE_LIMIT are re-exported here because
subjects.informatics.section.builder and subjects.vps.builder import them (and
build_group_all_weeks) from this module.
"""
from subjects.base_builder import make_builder, CLIENT_LIMITS, GLOBAL_SEMAPHORE_LIMIT
from subjects.informatics.metrics import (
    empty_metrics_info,
    extract_metrics,
    merge_metrics_info,
    metrics_to_row,
)

(
    _fetch_week_metrics,
    build_group_all_weeks,
    _build_report_job,
    _build_section_report_job,
) = make_builder(
    extract_metrics_fn=extract_metrics,
    merge_metrics_fn=merge_metrics_info,
    empty_metrics_fn=empty_metrics_info,
    metrics_to_row_fn=metrics_to_row,
)
