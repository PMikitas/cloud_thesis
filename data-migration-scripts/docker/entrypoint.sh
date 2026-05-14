#!/bin/sh
# Entrypoint supports two modes:
#   cron       -> render a crontab from MIGRATION_CRON and run supercronic
#   scheduler  -> run the APScheduler-based pure-Python runner
#   <other>    -> exec the args verbatim (useful for ad-hoc one-shot runs)
set -eu

# Ensure dlt has a writable working directory.
mkdir -p "${DLT_DATA_DIR:-/app/.dlt-data}" "${DLT_PIPELINES_DIR:-/app/.dlt-data/pipelines}"

mode="${1:-cron}"

case "$mode" in
  cron)
    cron_expr="${MIGRATION_CRON:-0 2 * * *}"
    destinations="${MIGRATION_DESTINATIONS:-bigquery}"
    crontab_file=/app/migration.cron

    # supercronic inherits env from this process, so credentials in env are
    # available to the job without templating them into the crontab.
    printf '%s cd /app && python -m data_migration_scripts.operational_tables_migration --destinations %s\n' \
        "$cron_expr" "$(echo "$destinations" | tr ',' ' ')" \
        > "$crontab_file"

    echo "Starting supercronic with: $(cat "$crontab_file")"
    exec supercronic "$crontab_file"
    ;;
  scheduler)
    exec python -m data_migration_scripts.scheduler
    ;;
  run-once)
    shift || true
    exec python -m data_migration_scripts.operational_tables_migration "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
