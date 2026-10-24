#!/usr/bin/env bash
set -euo pipefail
APP_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_ROOT"
if [[ -f .runtime.env ]]; then set -a; source .runtime.env; set +a; fi
: "${AUDIT_CONFIG_FILE:?Set AUDIT_CONFIG_FILE in the private .runtime.env.}"
exec "$APP_ROOT/.venv/bin/python" "$APP_ROOT/scripts/image_build_worker.py"
