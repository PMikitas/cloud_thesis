#!/bin/sh
set -eu

mkdir -p "${METRICS_OUTPUT_DIR:-/app/results}"

mode="${1:-cron}"
warehouses="${METRICS_WAREHOUSES:-}"
if [ -z "$warehouses" ]; then
  warehouses="${TRACING_MIGRATIONS_CLOUD_SOLUTIONS:-snowflake}"
  case "${TRACING_ENABLED_MIGRATIONS:-true}" in
    false|FALSE|0|no|NO|off|OFF) warehouses="" ;;
  esac
fi
warehouse_args=""
if [ -n "$warehouses" ]; then
  warehouse_args="--warehouses $(echo "$warehouses" | tr ',' ' ')"
fi

case "$mode" in
  cron)
    crontab_file=/app/metrics.cron
    cat > "$crontab_file" <<EOF
${METRICS_OPERATIONAL_CRON:-*/2 * * * *} cd /app && python -m metrics_scheduler.run_metrics --group operational_dashboard ${warehouse_args}
${METRICS_BUSINESS_CRON:-*/30 * * * *} cd /app && python -m metrics_scheduler.run_metrics --group business_dashboard ${warehouse_args}
${METRICS_BEHAVIORAL_CRON:-0 */2 * * *} cd /app && python -m metrics_scheduler.run_metrics --group behavioral_analytics ${warehouse_args}
${METRICS_DAILY_REPORTING_CRON:-0 */6 * * *} cd /app && python -m metrics_scheduler.run_metrics --group daily_reporting ${warehouse_args}
EOF
    echo "Starting supercronic with:"
    cat "$crontab_file"
    exec supercronic "$crontab_file"
    ;;
  run-once)
    shift || true
    exec python -m metrics_scheduler.run_metrics "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
