#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-root_dev_pass}"
MIN_FREE_GB="${MIN_FREE_GB:-40}"
MAX_BINLOG_MB="${MAX_BINLOG_MB:-100}"

fail() {
  echo "PRE-FLIGHT FAILED: $*" >&2
  exit 1
}

mysql() {
  docker compose exec -T db mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "$@"
}

mysql_scalar() {
  mysql -N -B -e "$1" 2>/dev/null | awk 'NF {print $NF; exit}'
}

container_state="$(docker compose ps --status running --format '{{.Service}}' | tr '\n' ' ')"
[[ "$container_state" == *"db"* ]] || fail "db container is not running"
[[ "$container_state" == *"sm-shop"* ]] || fail "sm-shop container is not running"

available_gb="$(df -BG --output=avail / | tail -1 | tr -dc '0-9')"
[[ -n "$available_gb" ]] || fail "could not read free disk space"
if (( available_gb < MIN_FREE_GB )); then
  fail "only ${available_gb}GB free on /; require at least ${MIN_FREE_GB}GB"
fi

log_bin="$(mysql_scalar "SHOW VARIABLES LIKE 'log_bin';")"
[[ "$log_bin" == "OFF" ]] || fail "MySQL log_bin is ${log_bin:-unknown}; expected OFF"

binlog_bytes="$(docker compose exec -T db bash -lc "find /var/lib/mysql -maxdepth 1 -name 'binlog.*' ! -name 'binlog.index' -printf '%s\n' 2>/dev/null | awk '{s+=\$1} END {print s+0}'")"
binlog_mb=$(( binlog_bytes / 1024 / 1024 ))
if (( binlog_mb > MAX_BINLOG_MB )); then
  fail "binlog files are ${binlog_mb}MB; expected <= ${MAX_BINLOG_MB}MB"
fi

health_body="$(curl -m 20 -fsS http://localhost:8081/actuator/health || true)"
[[ "$health_body" == *'"UP"'* ]] || fail "sm-shop health endpoint is not UP"

outbox_summary="$(mysql -N -B SALESMANAGER -e "
SELECT
  COUNT(*) AS retained_rows,
  COALESCE(SUM(SENT_AT IS NULL),0) AS pending_rows,
  COALESCE(SUM(LAST_ERROR IS NOT NULL AND LAST_ERROR <> ''),0) AS error_rows
FROM OPERATIONAL_EVENT_OUTBOX
WHERE SENT_AT IS NULL OR SENT_AT >= NOW() - INTERVAL 30 MINUTE;
" 2>/dev/null || true)"

echo "PRE-FLIGHT OK"
echo "free_gb=${available_gb}"
echo "mysql_log_bin=${log_bin}"
echo "binlog_mb=${binlog_mb}"
echo "outbox_retained_pending_errors=${outbox_summary:-unavailable}"
