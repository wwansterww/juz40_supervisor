#!/usr/bin/env bash
#
# Production launcher: multiple uvicorn workers so the app uses every CPU core,
# while the per-worker concurrency shares (computed in config.py) keep the
# combined load on api.juz40-edu.kz bounded.
#
# Usage:
#   export SECRET_KEY=$(python -c 'import secrets; print(secrets.token_hex(32))')
#   ./run_prod.sh
#
# Tune via env vars (all optional except SECRET_KEY):
#   WEB_CONCURRENCY     number of worker processes        (default: CPU cores)
#   API_LIMIT_TOTAL     max parallel upstream requests    (default: 250)
#   REPORT_SLOT_TOTAL   max reports building at once       (default: 10)
#   REPORT_FANOUT_LIMIT max requests per single report     (default: 50)
#   PORT                listen port                        (default: 8742)
#   REDIS_URL           redis connection                   (default: localhost)

set -euo pipefail
cd "$(dirname "$0")"

# Workers default to the number of CPU cores.
if [ -z "${WEB_CONCURRENCY:-}" ]; then
  WEB_CONCURRENCY="$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)"
fi
export WEB_CONCURRENCY

export API_LIMIT_TOTAL="${API_LIMIT_TOTAL:-250}"
export REPORT_SLOT_TOTAL="${REPORT_SLOT_TOTAL:-10}"
export REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"

# REQUIRED in multi-worker mode. Without a fixed key each worker signs session
# cookies with its OWN random key, so a user logged in via one worker is logged
# out the moment a request lands on another. Fail fast rather than ship that.
if [ -z "${SECRET_KEY:-}" ]; then
  echo "ERROR: SECRET_KEY is not set." >&2
  echo "Generate one once and keep it stable across restarts:" >&2
  echo "  export SECRET_KEY=\$(python -c 'import secrets; print(secrets.token_hex(32))')" >&2
  exit 1
fi

echo "Starting ${WEB_CONCURRENCY} worker(s) — upstream budget ${API_LIMIT_TOTAL}, report slots ${REPORT_SLOT_TOTAL}"

exec .venv/bin/python -m uvicorn main:app \
  --host 0.0.0.0 \
  --port "${PORT:-8742}" \
  --workers "${WEB_CONCURRENCY}"
