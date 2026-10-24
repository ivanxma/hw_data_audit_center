#!/usr/bin/env bash
set -euo pipefail
umask 077
APP_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$APP_ROOT"
[[ "$EUID" -ne 0 ]] || { echo 'Run setup as the non-root application owner with sudo access.' >&2; exit 1; }
source "$APP_ROOT/ol9/setup_platform.sh"
platform_verify
if [[ "${SKIP_PRIVILEGED_SETUP:-0}" != 1 ]]; then platform_install_dependencies; fi
PYTHON_BIN="${AUDIT_PYTHON_BIN:-$(command -v python3.12 || true)}"
[[ -n "$PYTHON_BIN" && "$PYTHON_BIN" != *$'\n'* && -x "$PYTHON_BIN" ]] || { echo 'Install Python 3.12+ or set AUDIT_PYTHON_BIN.' >&2; exit 1; }
"$PYTHON_BIN" -c 'import sys; assert sys.version_info >= (3,12), "Python 3.12+ required"'
[[ -x .venv/bin/python ]] || "$PYTHON_BIN" -m venv .venv
.venv/bin/python -c 'import sys; assert sys.version_info >= (3,12), "Existing venv requires Python 3.12+"'
.venv/bin/pip install -r requirements.txt
.venv/bin/python scripts/install_services.py --check-config
mkdir -p .data/run .data/log .data/tmp .embedded/mysql-server downloads etc tls
chmod 700 .data .data/run .data/log .data/tmp tls
if [[ ! -x .embedded/mysql-server/current/bin/mysqld ]]; then
  : "${AUDIT_MYSQL_TARBALL_URL:?Set the official MySQL 9.x Linux generic tarball URL.}"
  : "${AUDIT_MYSQL_TARBALL_SHA256:?Set the verified SHA256 of that tarball.}"
  [[ "$AUDIT_MYSQL_TARBALL_SHA256" =~ ^[0-9a-fA-F]{64}$ ]] || { echo 'Provide a 64-character SHA256.' >&2; exit 1; }
  curl --fail --location --proto '=https' --proto-redir '=https' --tlsv1.2 "$AUDIT_MYSQL_TARBALL_URL" -o downloads/mysql-server.tar.xz
  printf '%s  %s\n' "$AUDIT_MYSQL_TARBALL_SHA256" downloads/mysql-server.tar.xz | sha256sum -c -
  [[ ! -e .embedded/mysql-server/current && ! -L .embedded/mysql-server/current ]] || { echo 'Embedded MySQL installation is incomplete; review it before replacement.' >&2; exit 1; }
  staging="$(mktemp -d "$APP_ROOT/.embedded/mysql-server/install.XXXXXX")"
  trap 'rm -rf -- "$staging"' EXIT
  tar -xJf downloads/mysql-server.tar.xz --strip-components=1 -C "$staging"
  "$staging/bin/mysqld" --version | grep -E 'Ver 9\.' >/dev/null || { echo 'Embedded MySQL 9.x is required.' >&2; exit 1; }
  ln -s "$(basename "$staging")" .embedded/mysql-server/current
  trap - EXIT
fi
.embedded/mysql-server/current/bin/mysqld --version | grep -E 'Ver 9\.' >/dev/null || { echo 'Embedded MySQL 9.x is required.' >&2; exit 1; }
.venv/bin/python scripts/install_services.py
set -a; source .runtime.env; set +a
if [[ ! -d .data/mysql/mysql ]]; then
  .embedded/mysql-server/current/bin/mysqld --defaults-file="$APP_ROOT/etc/my.cnf" --initialize
  chmod 600 .data/log/mysqld.err
fi
sudo systemctl daemon-reload
sudo systemctl enable --now heatwave-audit-local-mysql.service
for attempt in {1..60}; do [[ -S .data/run/mysql.sock ]] && break; sleep 1; done
[[ -S .data/run/mysql.sock ]] || { echo 'Local MySQL did not start. Check its private error log.' >&2; exit 1; }
if [[ -z "${SSL_CERT_FILE:-}" && ! -f tls/server.crt ]]; then
  .venv/bin/python scripts/create_tls.py
fi
if [[ -n "${LOCAL_MYSQL_ADMIN_PASSWORD:-}" ]]; then
  .venv/bin/python scripts/enroll_admin.py --environment
fi
.venv/bin/python scripts/install_streaming_java.py
.venv/bin/python -m compileall -q app.py modules scripts deploy worker.py
.venv/bin/pip check
.venv/bin/python scripts/install_image_builder.py
sudo systemctl enable heatwave-audit-https.service
sudo systemctl restart heatwave-audit-https.service
platform_open_https
printf '%s\n' 'HTTPS service installed. Complete local-admin enrollment if not yet configured.'
