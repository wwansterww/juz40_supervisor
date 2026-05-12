from subjects.base_builder import make_builder
from subjects.informatics.metrics import empty_metrics_info, extract_metrics, merge_metrics_info

_fetch_week_metrics, build_group_all_weeks, _build_report_job = make_builder(
    extract_metrics_fn=extract_metrics,
    merge_metrics_fn=merge_metrics_info,
    empty_metrics_fn=empty_metrics_info,
)