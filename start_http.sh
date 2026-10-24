#!/usr/bin/env bash
set -euo pipefail
APP_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$APP_ROOT"
PORT_OVERRIDE="${PORT:-}"
HOST_OVERRIDE="${HOST:-127.0.0.1}"
if [[ -f .runtime.env ]]; then set -a; source .runtime.env; set +a; fi
export AUDIT_SESSION_COOKIE_SECURE=0
exec .venv/bin/gunicorn --workers 1 --worker-class gthread --threads 8 --timeout 180 --bind "$HOST_OVERRIDE:${PORT_OVERRIDE:-5000}" --access-logfile /dev/null app:app
