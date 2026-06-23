"""
Central logging configuration.

Before this, the backend was effectively blind: errors were swallowed by
`except Exception: pass` / `return None` with no trace anywhere, so problems
like "the report came back empty" left nothing to diagnose. Modules now do
`logger = logging.getLogger(__name__)` and emit structured messages; this sets
up a single stream handler so those messages actually go somewhere.

Level is controlled by the LOG_LEVEL env var (default INFO). Call
configure_logging() once at process startup (main.py does this on import).
"""

import logging
import os
import sys

_configured = False


def configure_logging() -> None:
    global _configured
    if _configured:
        return

    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))

    root = logging.getLogger()
    # Don't stack duplicate handlers if uvicorn / a reload already added ours.
    if not any(getattr(h, "_juz40", False) for h in root.handlers):
        handler._juz40 = True  # type: ignore[attr-defined]
        root.addHandler(handler)
    root.setLevel(level)

    _configured = True
