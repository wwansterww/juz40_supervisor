# In-memory stores shared across the app

# job_id -> {"total": N, "done": M, "status": "running"|"done", "results": [...]}
PROGRESS: dict = {}

# report_key -> {"tables": [...], "title": "..."}
REPORT_STORE: dict = {}
