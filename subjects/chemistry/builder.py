from subjects.base_builder import make_builder
from subjects.chemistry.metrics import empty_metrics_chem, extract_metrics, merge_metrics_chem

_fetch_week_metrics, build_group_all_weeks, _build_report_job = make_builder(
    extract_metrics_fn=extract_metrics,
    merge_metrics_fn=merge_metrics_chem,
    empty_metrics_fn=empty_metrics_chem,
)