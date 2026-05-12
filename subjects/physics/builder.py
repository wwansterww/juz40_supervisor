from subjects.base_builder import make_builder
from subjects.physics.metrics import empty_metrics_phys, extract_metrics, merge_metrics_phys

_fetch_week_metrics, build_group_all_weeks, _build_report_job = make_builder(
    extract_metrics_fn=extract_metrics,
    merge_metrics_fn=merge_metrics_phys,
    empty_metrics_fn=empty_metrics_phys,
)