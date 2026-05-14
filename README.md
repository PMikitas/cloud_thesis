# Shopizer Cloud Warehouse Benchmark

This repository contains a Shopizer 3.2.7 based e-commerce backend extended for
cloud warehouse benchmarking. It combines:

- the Shopizer API and MySQL source database
- API event tracing into BigQuery, Snowflake, and Redshift
- operational table migration from MySQL into warehouse tables
- scheduled analytical metrics from `metrics_scripts.sql`
- Gatling load generation for repeatable user activity

The repository is meant to be run locally with Docker Compose and configured
with local-only credential files. See [CREDENTIALS.md](CREDENTIALS.md) before
starting anything that talks to cloud services.

## Repository Layout

- `docker-compose.yml`: Shopizer, MySQL, catalog seeding, and Gatling load tests.
- `sm-shop/`: Shopizer API service plus tracing sinks.
- `data-migration-scripts/`: MySQL operational table refresh into warehouses.
- `metrics-scheduler/`: scheduled runner for analytical SQL metrics.
- `metrics_scripts.sql`: business and behavioral analytics queries.
- `shopizer-gatling/`: Gatling simulations used to generate API traffic.
- `mysql/conf.d/`: MySQL tuning used by the local benchmark database.

## Prerequisites

- Docker Desktop or Docker Engine with Compose v2.
- A local `.env` copied from `.env.example`.
- Local credential files described in [CREDENTIALS.md](CREDENTIALS.md).
- Optional: `gcloud` CLI if you use BigQuery with application-default credentials.

## 1. Prepare Local Config

```bash
cp .env.example .env
cp sm-shop/src/main/resources/snowflake.properties.example sm-shop/src/main/resources/snowflake.properties
cp sm-shop/src/main/resources/redshift.properties.example sm-shop/src/main/resources/redshift.properties
mkdir -p secrets
touch secrets/ecomm_sf_key.p8
```

Edit the copied files with your local credentials. If you do not need a cloud
sink for a run, leave its `*.tracing.enabled=false` setting.

## 2. Start Shopizer And MySQL

```bash
docker compose up --build -d db sm-shop
```

Check that the API is healthy:

```bash
curl http://localhost:8081/actuator/health
```

Useful logs:

```bash
docker compose logs -f sm-shop
```

## 3. Seed The Catalog

The seed step inserts the catalog used by the Gatling simulations.

```bash
docker compose --profile seed up --build setup-catalog
```

## 4. Generate Load With Gatling

Run the default user activity simulation:

```bash
docker compose --profile loadtest up --build gatling
```

Override simulation parameters from `.env`, for example:

```text
GATLING_ACTIVITY_USERS=600
GATLING_SESSION_DURATION_SECONDS=21600
GATLING_RAMP_SECONDS=300
```

Generated Gatling reports are written under `shopizer-gatling/results/` and are
ignored by Git.

## 5. Run Operational Table Migration

The migration bridge copies selected operational MySQL tables into BigQuery,
Snowflake, and optionally Redshift.

```bash
cd data-migration-scripts
cp .env.example .env
```

Edit `data-migration-scripts/.env`, then start the cron runner:

```bash
docker compose up --build -d migration-cron
```

Run one refresh manually:

```bash
docker compose run --rm migration-cron run-once --destinations bigquery snowflake
```

The migration compose file joins the Shopizer Docker network. The root `.env`
sets `COMPOSE_PROJECT_NAME=shopizer`, so the expected network is
`shopizer_default`.

## 6. Run Metrics Scheduler

The metrics scheduler executes the query groups defined in
`metrics-scheduler/src/metrics_scheduler/run_metrics.py` against the SQL in
`metrics_scripts.sql`.

```bash
cd metrics-scheduler
cp .env.example .env
```

Edit `metrics-scheduler/.env`, then start scheduled execution:

```bash
docker compose up --build -d metrics-cron
```

Run a single group manually:

```bash
docker compose run --rm metrics-cron run-once --group operational_dashboard
```

Run a single group for one warehouse:

```bash
docker compose run --rm metrics-cron run-once --group operational_dashboard --warehouses bigquery
```

Metric outputs are written under `metrics-scheduler/results/` and are ignored by
Git.

## 7. Stop Services

From the repository root:

```bash
docker compose down
```

From each subproject if those schedulers are running:

```bash
cd data-migration-scripts && docker compose down
cd ../metrics-scheduler && docker compose down
```

To remove local Docker volumes as well:

```bash
docker compose down -v
```

## Notes For The Thesis Setup

- API tracing creates event-level behavioral data.
- The migration bridge creates operational mirror tables for dashboard queries.
- The metrics scheduler records result files and metadata such as runtime,
  latency, data volume, and result size.
- Gatling generates realistic API traffic so the behavioral queries have
  session and endpoint data to analyze.

Generated result data, local pipeline state, credentials, keys, IDE files, and
virtual environments are intentionally ignored by Git.
