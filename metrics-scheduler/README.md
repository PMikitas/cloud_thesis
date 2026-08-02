# Metrics Scheduler

This folder contains a standalone scheduled runner for the metrics queries in
the repo-root [metrics_scripts.sql](../metrics_scripts.sql).

It reuses the same credential sources already proven in the repo:

- Redshift settings from `sm-shop/src/main/resources/redshift.properties`
- Snowflake settings from `sm-shop/src/main/resources/snowflake.properties`
- BigQuery project/dataset settings from `data-migration-scripts/.env` and
  the API event table from repo-root `.env` / `METRICS_BIGQUERY_EVENT_TABLE`,
  plus ADC

For Snowflake event metrics, the scheduler uses the event table configured in
`snowflake.properties`, currently `API_TRACKING`. If you want to pin a different
table manually, set `METRICS_SNOWFLAKE_EVENT_TABLE` in `.env`.

Snowflake metrics read from the schema in `snowflake.properties`
(`PERFORMANCE_TRACKING`), including the API event table `API_TRACKING` and the
operational tables refreshed by the migration bridge. Metrics use
`ANALYTICS_WH`; API event streaming can continue to use `INGEST_WH`.

The legacy SQL file still refers to `performance_tracking.api_performance`.
At runtime the scheduler rewrites that event table to the active tracing table:

- BigQuery: `performance_tracking.api_tracking`
- Snowflake: `performance_tracking.api_tracking`
- Redshift: `performance_tracking.api_tracking`

## Query groups

- `operational_dashboard`
  - queries `1, 2, 3, 6, 8`
  - default schedule: every 15 minutes
- `business_dashboard`
  - queries `7, 9, 10, 11, 12`
  - default schedule: every 30 minutes
- `behavioral_analytics`
  - queries `14, 15, 16, 17, 18`
  - default schedule: every 2 hours
- `daily_reporting`
  - queries `4, 5, 13`
  - default schedule: every 6 hours

## Expected SQL file format

The runner expects `metrics_scripts.sql` to contain query blocks with metadata
headers. Each block must start with at least:

```sql
-- id: 1
-- slug: average_session_duration
-- warehouse: all
SELECT ...
```

If the SQL syntax differs by warehouse, add one block per warehouse:

```sql
-- id: 10
-- slug: top_products_by_sold_quantity_and_revenue
-- warehouse: redshift
SELECT ...;

-- id: 10
-- slug: top_products_by_sold_quantity_and_revenue
-- warehouse: bigquery
SELECT ...;

-- id: 10
-- slug: top_products_by_sold_quantity_and_revenue
-- warehouse: snowflake
SELECT ...;
```

Supported values for `warehouse`:

- `all`
- `redshift`
- `bigquery`
- `snowflake`

## Outputs

Each execution writes:

- a CSV result set
- a small JSON metadata file with row count, runtime, and freshness fields:
  - `calculated_at_utc`
  - `latest_data_used_at_utc`
  - `data_latency_seconds`
  - `data_latency_minutes`
  - `data_volume_bytes`
  - `data_volume_megabytes`
  - `data_volume_source`
  - `result_size_bytes`
  - `result_size_megabytes`
  - `result_size_source`

under:

```text
metrics-scheduler/results/<warehouse>/<group>/qNN_<slug>/
```

After each group run, the scheduler also logs the average data latency across
all executed metrics in that run.

`data_volume_*` is warehouse-specific:

- BigQuery: actual `total_bytes_processed`
- Snowflake: `bytes_scanned` from query history
- Redshift: best-effort scan-volume estimate from `SYS_QUERY_DETAIL`, primarily
  `blocks_read * 1 MB`, with `input_bytes` as a secondary source

For Redshift, `warehouse_returned_bytes` is stored separately in metadata for
debugging, but it is no longer used as the cross-warehouse `data_volume_*`
value because returned result size is not comparable to bytes scanned.

`result_size_*` is the common cross-warehouse payload-size metric. It is
measured from the saved CSV result file, so it is available and directly
comparable for BigQuery, Snowflake, and Redshift.

## Docker usage

Copy `.env.example` to `.env`, then start the scheduler:

```bash
docker compose up --build -d metrics-cron
```

By default, the scheduler uses `../tracing.env`
`TRACING_MIGRATIONS_CLOUD_SOLUTIONS` for warehouse selection. Set
`METRICS_WAREHOUSES=snowflake` in `.env` to run scheduled metrics for only one
warehouse without changing migration or API-event tracing destinations.

Run one group manually:

```bash
docker compose run --rm metrics-cron run-once --group operational_dashboard
```

Run only one warehouse for a quick test:

```bash
docker compose run --rm metrics-cron run-once --group operational_dashboard --warehouses redshift
```

## Redshift reliability knobs

The scheduler can use its own Redshift connection settings, separate from the
app tracing service. These are read from `.env`:

- `METRICS_REDSHIFT_CONNECT_TIMEOUT_SECONDS`
- `METRICS_REDSHIFT_CONNECT_ATTEMPTS`
- `METRICS_REDSHIFT_RETRY_DELAY_SECONDS`
- `METRICS_REDSHIFT_DISABLE_RESULT_CACHE`

This is useful for Redshift Serverless, where the endpoint may occasionally be
slower to accept new connections than the app's tracing timeout allows.
Disabling the Redshift result cache is also useful when you want data-volume
metrics to reflect a real execution instead of a cached result.

## Note

The runner now supports two SQL file formats:

1. Metadata blocks:

```sql
-- id: 1
-- slug: average_session_duration
-- warehouse: bigquery
SELECT ...
```

2. The current repo's legacy note format using headings like:

```sql
[+] - average session duration
SELECT ...

9. Revenue by endpoint session intensity
SELECT ...
```

The scheduler uses `TRACING_MIGRATIONS_CLOUD_SOLUTIONS` from `../tracing.env` by
default. To benchmark a one-off subset, pass `--warehouses` explicitly. To make
the scheduled container run a subset, set `METRICS_WAREHOUSES` in
`metrics-scheduler/.env`.
