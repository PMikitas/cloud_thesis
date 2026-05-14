#!/bin/sh
set -eu

mkdir -p "${METRICS_OUTPUT_DIR:-/app/results}"

mode="${1:-cron}"
warehouses="$(echo "${METRICS_WAREHOUSES:-bigquery}" | tr ',' ' ')"

case "$mode" in
  cron)
    crontab_file=/app/metrics.cron
    cat > "$crontab_file" <<EOF
${METRICS_OPERATIONAL_CRON:-*/2 * * * *} cd /app && python -m metrics_scheduler.run_metrics --group operational_dashboard --warehouses ${warehouses}
${METRICS_BUSINESS_CRON:-*/30 * * * *} cd /app && python -m metrics_scheduler.run_metrics --group business_dashboard --warehouses ${warehouses}
${METRICS_BEHAVIORAL_CRON:-0 */2 * * *} cd /app && python -m metrics_scheduler.run_metrics --group behavioral_analytics --warehouses ${warehouses}
${METRICS_DAILY_REPORTING_CRON:-0 */6 * * *} cd /app && python -m metrics_scheduler.run_metrics --group daily_reporting --warehouses ${warehouses}
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
