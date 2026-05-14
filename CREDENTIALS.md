# Credentials And Local Configuration

This project should be cloned and deployed with local-only credential files.
The repository includes example files, but the real files are ignored by Git.

Do not commit:

- `.env` files
- cloud service-account JSON files
- Snowflake private keys
- Redshift or Snowflake property files with real values
- local dlt state under `.dlt/`
- generated benchmark outputs

## Files To Create After Clone

From the repository root:

```bash
cp .env.example .env
cp sm-shop/src/main/resources/snowflake.properties.example sm-shop/src/main/resources/snowflake.properties
cp sm-shop/src/main/resources/redshift.properties.example sm-shop/src/main/resources/redshift.properties
cp data-migration-scripts/.env.example data-migration-scripts/.env
cp metrics-scheduler/.env.example metrics-scheduler/.env
mkdir -p secrets
```

If you are not using Snowflake locally, create an empty placeholder key so the
Docker mount path exists:

```bash
touch secrets/ecomm_sf_key.p8
```

If you are using Snowflake, put the real private key at that path or set
`SNOWFLAKE_PRIVATE_KEY_MOUNT_SOURCE` in each relevant `.env` file to the real
host path.

## Root `.env`

Used by the root `docker-compose.yml`.

Important values:

- `COMPOSE_PROJECT_NAME`: keep as `shopizer` so subproject compose files can
  join the `shopizer_default` network.
- `MYSQL_ROOT_PASSWORD`, `MYSQL_USER`, `MYSQL_PASSWORD`: local MySQL container
  credentials.
- `SHOPIZER_ADMIN_EMAIL`, `SHOPIZER_ADMIN_PASS`: local admin credentials used by
  catalog seeding and Gatling setup.
- `GOOGLE_CLOUD_PROJECT`: BigQuery project used when BigQuery tracing is
  enabled.
- `SNOWFLAKE_PRIVATE_KEY_MOUNT_SOURCE`: host path to the Snowflake private key.
- `GATLING_*`: optional load-test parameters.

## Shopizer Warehouse Properties

The application reads these files from `sm-shop/src/main/resources/`:

- `snowflake.properties`
- `redshift.properties`

Create them from the `.example` files. They control API event streaming from
the Shopizer app.

For Snowflake, fill in:

- `snowflake.tracing.enabled`
- `snowflake.url`
- `snowflake.user`
- `snowflake.role`
- `snowflake.warehouse`
- `snowflake.database`
- `snowflake.schema`
- `snowflake.table`
- `snowflake.private.key.path`
- `snowflake.private.key.passphrase` if the key is encrypted

For Redshift, fill in:

- `redshift.tracing.enabled`
- `redshift.jdbc.url`
- `redshift.user`
- `redshift.password`
- `redshift.schema`
- `redshift.table`
- optional batching and timeout settings

Leave `*.tracing.enabled=false` for destinations you are not testing.

## BigQuery Credentials

The Docker files expect Google application-default credentials on the host.
The usual local setup is:

```bash
gcloud auth application-default login
```

That creates:

```text
~/.config/gcloud/application_default_credentials.json
```

The compose files mount this into containers. Do not copy this JSON into the
repository.

Set the project ID in:

- root `.env`: `GOOGLE_CLOUD_PROJECT`
- `data-migration-scripts/.env`: `BQ_PROJECT_ID`
- `metrics-scheduler/.env`: `BQ_PROJECT_ID`

## Data Migration `.env`

Used by `data-migration-scripts/docker-compose.yml`.

Fill in:

- `MYSQL_DSN`: connection to the Shopizer MySQL container.
- `MIGRATION_DESTINATIONS`: comma-separated destinations, for example
  `bigquery,snowflake` or `bigquery,snowflake,redshift`.
- `MIGRATION_NAMESPACE`: target dataset/schema, normally `performance_tracking`.
- BigQuery values: `BQ_PROJECT_ID`, `BQ_DATASET`.
- Snowflake values: `SNOWFLAKE_DATABASE`, `SNOWFLAKE_USERNAME`,
  `SNOWFLAKE_HOST`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_ROLE`,
  `SNOWFLAKE_PRIVATE_KEY_PATH`, `SF_SCHEMA`.
- Redshift values: `REDSHIFT_DATABASE`, `REDSHIFT_USERNAME`,
  `REDSHIFT_PASSWORD`, `REDSHIFT_HOST`, `REDSHIFT_PORT`, `RS_SCHEMA`.

## Metrics Scheduler `.env`

Used by `metrics-scheduler/docker-compose.yml`.

Fill in:

- `METRICS_WAREHOUSES`: comma-separated warehouses to benchmark.
- `BQ_PROJECT_ID`: BigQuery project for analytical queries.
- `METRICS_SNOWFLAKE_EVENT_TABLE`: Snowflake event table if it differs from the
  app property file.
- Redshift retry/cache knobs if Redshift is enabled.

The metrics scheduler also reads:

- `data-migration-scripts/.env`
- `sm-shop/src/main/resources/snowflake.properties`
- `sm-shop/src/main/resources/redshift.properties`

Create those files even if you run only one warehouse, because the scheduler
loads them during startup.

## Rotation And History

If any real credential was committed before this cleanup, deleting it now is
not enough for a public repository. Rotate the credential in the cloud provider
and push the cleaned codebase to a new history-free repository.
