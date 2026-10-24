#!/usr/bin/env bash
set -euo pipefail
APP_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec "$APP_ROOT/.embedded/mysql-server/current/bin/mysqld" --defaults-file="$APP_ROOT/etc/my.cnf"
