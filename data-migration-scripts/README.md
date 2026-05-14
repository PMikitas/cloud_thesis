# Data Migration Bridge

This package refreshes selected operational MySQL tables and rewrites them to:

- BigQuery
- Snowflake
- Redshift

It is designed to run either:

- on a cron expression via `supercronic`
- via APScheduler in a plain Python process
- once on demand for testing

## What it refreshes

By default the bridge mirrors these MySQL tables from `SALESMANAGER`:

- `CUSTOMER`
- `PRODUCT`
- `PRODUCT_AVAILABILITY`
- `PRODUCT_DESCRIPTION`
- `PRODUCT_PRICE`
- `SHOPPING_CART`
- `SHOPPING_CART_ITEM`

Override them with `MIGRATION_TABLES` if needed.

## Naming conventions

This repo now targets the warehouse layout you described:

- BigQuery dataset: `performance_tracking`
- Snowflake namespace: `THESIS_EXPERIMENTS_DB.performance_tracking`
- Redshift namespace: `dev.public`

The bridge therefore uses:

- `MIGRATION_NAMESPACE=performance_tracking`
- `SF_SCHEMA=performance_tracking`
- `RS_SCHEMA=public`
- `MIGRATION_WRITE_DISPOSITION=merge` for normal incremental runs
- `MIGRATION_RESET_PIPELINE_STATE=false` so incremental cursor state is preserved
- `MIGRATION_DROP_DESTINATION_TABLES_ON_REPLACE=false` so refreshes do not create table-not-found windows

BigQuery still defaults to `BQ_DATASET` or `MIGRATION_NAMESPACE`. Snowflake uses
the same `performance_tracking` schema as the API tracing table
`API_TRACKING`.

## Environment

Copy `.env.example` to `.env` and fill in the warehouse credentials.

Important variables:

- `MYSQL_DSN`
- `MIGRATION_DESTINATIONS`
- `MIGRATION_NAMESPACE`
- `MIGRATION_WRITE_DISPOSITION`
- `MIGRATION_INCREMENTAL_ENABLED`
- `MIGRATION_RESET_PIPELINE_STATE`
- `MIGRATION_DROP_DESTINATION_TABLES_ON_REPLACE`
- `GOOGLE_APPLICATION_CREDENTIALS`
- `SNOWFLAKE_PRIVATE_KEY_PATH` or `SNOWFLAKE_PRIVATE_KEY`
- `REDSHIFT_*`

## Local runs

Run once:

```bash
python -m data_migration_scripts.operational_tables_migration
```

Run once with explicit destinations:

```bash
python -m data_migration_scripts.operational_tables_migration \
  --destinations bigquery snowflake redshift
```

Start the APScheduler runner:

```bash
python -m data_migration_scripts.scheduler
```

## Docker compose

The compose file in this folder expects the main Shopizer stack to already own
the `shopizer_default` network, or a compatible external network supplied via:

```bash
SHOPIZER_NETWORK_NAME=shopizer_default
```

Cron-style runner:

```bash
docker compose up --build migration-cron
```

Python scheduler runner:

```bash
docker compose --profile python-only up --build migration-scheduler
```

## Notes

- Each destination gets a fresh `dlt` SQL source instance, so one warehouse run
  cannot consume the source state intended for the next one.
- Normal cron runs use `MIGRATION_WRITE_DISPOSITION=merge` with
  `MIGRATION_INCREMENTAL_ENABLED=true`. The bridge applies per-table primary key
  and cursor hints so dlt only extracts new or changed rows after the first
  incremental baseline run.
- Keep `MIGRATION_RESET_PIPELINE_STATE=false` for incremental runs. Resetting
  the local dlt state erases cursor progress and forces the next run to scan the
  full source tables again.
- For a deliberate full rebuild, temporarily set
  `MIGRATION_WRITE_DISPOSITION=replace` and `MIGRATION_RESET_PIPELINE_STATE=true`.
- For BigQuery full refreshes, the default behavior is now to keep the tables
  in place and let `dlt` perform its normal `replace` flow. With the default
  `truncate-and-insert` strategy, tables are truncated and refilled instead of
  being pre-dropped by the bridge, which avoids transient `table not found`
  windows for readers such as the metrics scheduler.
- If you explicitly want the old destructive BigQuery pre-drop behavior, set
  `MIGRATION_DROP_DESTINATION_TABLES_ON_REPLACE=true`.
- Destination failures are isolated and summarized after the run instead of
  silently stopping at the first warehouse.
- The old module name `mysql2bq_tables_migration.py` is kept as a compatibility
  wrapper for existing imports and commands.
