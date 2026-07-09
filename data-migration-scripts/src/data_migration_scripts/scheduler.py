"""APScheduler-based runner: schedule the operational refresh on a cron expression
inside any plain Python environment (no system cron needed).

Env vars:
    MIGRATION_CRON          5-field cron expression (default "0 2 * * *")
    TRACING_MIGRATIONS_CLOUD_SOLUTIONS
                            comma-separated, e.g. "snowflake"
    MIGRATION_TZ            IANA tz name (default "UTC")
    RUN_ONCE_ON_START       "true" to also fire once at startup
"""

from __future__ import annotations

import logging
import os
import signal
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from data_migration_scripts.operational_tables_migration import (
    default_migration_destinations,
    run as run_migration,
)

logger = logging.getLogger("operational_tables_migration.scheduler")


def _job(destinations: list[str], tables: list[str]) -> None:
    try:
        run_migration(destinations, tables)
    except Exception:
        logger.exception("Scheduled migration run failed")


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    cron_expr = os.environ.get("MIGRATION_CRON", "0 2 * * *")
    destinations = default_migration_destinations()
    timezone = os.environ.get("MIGRATION_TZ", "UTC")

    from data_migration_scripts.operational_tables_migration import TABLES_TO_MIGRATE

    tables = [
        table.strip()
        for table in os.environ.get("MIGRATION_TABLES", ",".join(TABLES_TO_MIGRATE)).split(",")
        if table.strip()
    ]

    trigger = CronTrigger.from_crontab(cron_expr, timezone=timezone)
    scheduler = BlockingScheduler(timezone=timezone)
    scheduler.add_job(
        _job,
        trigger=trigger,
        args=[destinations, tables],
        id="operational_tables_migration",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
    )

    logger.info(
        "Scheduled operational refresh: cron=%r tz=%s destinations=%s tables=%s",
        cron_expr,
        timezone,
        destinations,
        tables,
    )

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: scheduler.shutdown(wait=False))

    if os.environ.get("RUN_ONCE_ON_START", "false").lower() == "true":
        logger.info("RUN_ONCE_ON_START set; firing one immediate run")
        _job(destinations, tables)

    scheduler.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
