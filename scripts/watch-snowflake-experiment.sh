#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-root_dev_pass}"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-60}"
STOP_FREE_GB="${STOP_FREE_GB:-25}"
WARN_FREE_GB="${WARN_FREE_GB:-40}"
MAX_BINLOG_MB="${MAX_BINLOG_MB:-256}"
STOP_OUTBOX_PENDING="${STOP_OUTBOX_PENDING:-1500000}"
MAX_HEALTH_FAILURES="${MAX_HEALTH_FAILURES:-3}"

health_failures=0

mysql() {
  docker compose exec -T db mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "$@"
}

mysql_scalar() {
  mysql -N -B -e "$1" 2>/dev/null | awk 'NF {print $NF; exit}'
}

stop_load() {
  local reason="$1"
  echo "FATAL: ${reason}"
  echo "Stopping Gatling to protect the database."
  docker rm -f gatling-experiment 2>/dev/null || true
  docker compose --profile loadtest stop gatling 2>/dev/null || true
  exit 2
}

while true; do
  now="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  available_gb="$(df -BG --output=avail / | tail -1 | tr -dc '0-9')"
  log_bin="$(mysql_scalar "SHOW VARIABLES LIKE 'log_bin';" || true)"
  binlog_bytes="$(docker compose exec -T db bash -lc "find /var/lib/mysql -maxdepth 1 -name 'binlog.*' ! -name 'binlog.index' -printf '%s\n' 2>/dev/null | awk '{s+=\$1} END {print s+0}'" 2>/dev/null || echo 0)"
  binlog_mb=$(( binlog_bytes / 1024 / 1024 ))

  outbox="$(mysql -N -B SALESMANAGER -e "
SELECT
  COALESCE(SUM(SENT_AT IS NULL),0) AS pending,
  COALESCE(SUM(SENT_AT IS NOT NULL AND SENT_AT >= NOW() - INTERVAL 60 SECOND),0) AS sent_last_60s,
  COALESCE(SUM(LAST_ERROR IS NOT NULL AND LAST_ERROR <> ''),0) AS errors
FROM OPERATIONAL_EVENT_OUTBOX
WHERE SENT_AT IS NULL OR SENT_AT >= NOW() - INTERVAL 30 MINUTE;
" 2>/dev/null || echo "mysql_unavailable mysql_unavailable mysql_unavailable")"

  pending="$(printf '%s' "$outbox" | awk '{print $1}')"
  sent_last_60s="$(printf '%s' "$outbox" | awk '{print $2}')"
  errors="$(printf '%s' "$outbox" | awk '{print $3}')"

  if curl -m 10 -fsS http://localhost:8081/actuator/health >/dev/null 2>&1; then
    health_failures=0
    app_health="UP"
  else
    health_failures=$((health_failures + 1))
    app_health="FAIL_${health_failures}"
  fi

  echo "${now} free_gb=${available_gb:-unknown} log_bin=${log_bin:-unknown} binlog_mb=${binlog_mb} pending=${pending} sent_last_60s=${sent_last_60s} errors=${errors} app_health=${app_health}"

  [[ "${log_bin:-}" == "OFF" ]] || stop_load "MySQL binary logging is ${log_bin:-unknown}, expected OFF"

  if [[ -n "${available_gb:-}" ]] && (( available_gb < STOP_FREE_GB )); then
    stop_load "only ${available_gb}GB free on /; threshold is ${STOP_FREE_GB}GB"
  fi

  if (( binlog_mb > MAX_BINLOG_MB )); then
    stop_load "binlog files reached ${binlog_mb}MB; threshold is ${MAX_BINLOG_MB}MB"
  fi

  if [[ "$pending" =~ ^[0-9]+$ ]] && (( pending > STOP_OUTBOX_PENDING )); then
    stop_load "outbox pending rows reached ${pending}; threshold is ${STOP_OUTBOX_PENDING}"
  fi

  if (( health_failures >= MAX_HEALTH_FAILURES )); then
    stop_load "sm-shop health failed ${health_failures} consecutive checks"
  fi

  if [[ -n "${available_gb:-}" ]] && (( available_gb < WARN_FREE_GB )); then
    echo "WARN: free disk below ${WARN_FREE_GB}GB"
  fi

  sleep "$CHECK_INTERVAL_SECONDS"
done
