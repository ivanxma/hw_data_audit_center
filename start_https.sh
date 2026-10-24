#!/usr/bin/env bash
set -euo pipefail
APP_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$APP_ROOT"
PORT_OVERRIDE="${PORT:-}"
if [[ -f .runtime.env ]]; then set -a; source .runtime.env; set +a; fi
export HOST="${HOST:-0.0.0.0}" PORT="${PORT_OVERRIDE:-${AUDIT_HTTPS_PORT:-443}}"
export AUDIT_SESSION_COOKIE_SECURE=1
exec .venv/bin/gunicorn --workers 1 --worker-class gthread --threads 8 --timeout 180 --bind "$HOST:$PORT" --certfile "${SSL_CERT_FILE:-$APP_ROOT/tls/server.crt}" --keyfile "${SSL_KEY_FILE:-$APP_ROOT/tls/server.key}" --access-logfile /dev/null app:app
