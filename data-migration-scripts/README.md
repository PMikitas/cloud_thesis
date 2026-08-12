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
- Redshift namespace: `dev.performance_tracking`

The bridge therefore uses:

- `MIGRATION_NAMESPACE=performance_tracking`
- `SF_SCHEMA=performance_tracking`
- `RS_SCHEMA=performance_tracking`
- `MIGRATION_WRITE_DISPOSITION=merge` for normal incremental runs
- `MIGRATION_RESET_PIPELINE_STATE=false` so incremental cursor state is preserved
- `MIGRATION_DROP_DESTINATION_TABLES_ON_REPLACE=true` so full replace runs clear stale BigQuery/Snowflake schemas

BigQuery still defaults to `BQ_DATASET` or `MIGRATION_NAMESPACE`. Snowflake uses
the same `performance_tracking` schema as the API tracing table
`API_TRACKING`.

## Environment

Copy `.env.example` to `.env` and fill in the warehouse credentials.

Important variables:

- `MYSQL_DSN`
- `TRACING_ENABLED_MIGRATIONS` and `TRACING_MIGRATIONS_CLOUD_SOLUTIONS` in
  `../tracing.env`
- `MIGRATION_DESTINATIONS`, optional comma-separated migration-only override,
  for example `snowflake`
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

Cron-style runner for only one destination:

```bash
MIGRATION_DESTINATIONS=snowflake docker compose up --build -d --force-recreate migration-cron
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
- For BigQuery and Snowflake full refreshes, the bridge drops the managed
  destination tables first by default. This avoids stale schema problems when
  MySQL `BIT` columns change from inferred integers to explicit booleans. The
  drop is only evaluated for `MIGRATION_WRITE_DISPOSITION=replace`; normal
  incremental `merge` runs do not drop tables.
- If you explicitly want to keep existing destination tables during a replace run,
  set `MIGRATION_DROP_DESTINATION_TABLES_ON_REPLACE=false`.
- Destination failures are isolated and summarized after the run instead of
  silently stopping at the first warehouse.
- The old module name `mysql2bq_tables_migration.py` is kept as a compatibility
  wrapper for existing imports and commands.
