"""Refresh operational MySQL tables into BigQuery, Snowflake, and Redshift.

This module is the destination-agnostic entrypoint for the bridge in
`data-migration-scripts`. It is intentionally separate from the older
`mysql2bq_tables_migration.py` name because the bridge now targets three
warehouses, not just BigQuery.

Design notes:
* each destination gets a fresh sql_database source instance
* destination naming is aligned via shared env defaults
* Snowflake supports either an inline private key or a mounted key file
* failures are isolated per destination and summarized at the end
"""

from __future__ import annotations

import argparse
import base64
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Callable, Iterable

import dlt
from dlt.destinations import redshift as dlt_redshift_destination
from dlt.destinations import snowflake as dlt_snowflake_destination
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_database

logger = logging.getLogger("operational_tables_migration")

TABLES_TO_MIGRATE = [
    "CUSTOMER",
    "PRODUCT",
    "PRODUCT_AVAILABILITY",
    "PRODUCT_DESCRIPTION",
    "PRODUCT_PRICE",
    "SHOPPING_CART",
    "SHOPPING_CART_ITEM",
]

DEFAULT_DESTINATIONS = ("snowflake",)
DEFAULT_MYSQL_DSN = "mysql+pymysql://root:root_dev_pass@localhost:3306/SALESMANAGER"
DEFAULT_NAMESPACE = "performance_tracking"
DEFAULT_WRITE_DISPOSITION = "merge"
DLT_INTERNAL_TABLES = ("_dlt_version", "_dlt_loads", "_dlt_pipeline_state")

# dlt reflects primary keys from MySQL, but making them explicit keeps merge
# behavior stable across destination/schema resets.
TABLE_PRIMARY_KEYS = {
    "customer": "CUSTOMER_ID",
    "product": "PRODUCT_ID",
    "product_availability": "PRODUCT_AVAIL_ID",
    "product_description": "DESCRIPTION_ID",
    "product_price": "PRODUCT_PRICE_ID",
    "shopping_cart": "SHP_CART_ID",
    "shopping_cart_item": "SHP_CART_ITEM_ID",
}

# MySQL BIT columns are not mapped by dlt's SQLAlchemy reflection. When a
# replace load has no values for one of them, dlt cannot infer a type and skips
# materializing the column unless we provide an explicit schema hint.
TABLE_COLUMN_HINTS = {
    "customer": {
        "CUSTOMER_ANONYMOUS": {"data_type": "bool"},
    },
    "product": {
        "AVAILABLE": {"data_type": "bool"},
        "PREORDER": {"data_type": "bool"},
        "PRODUCT_VIRTUAL": {"data_type": "bool"},
        "PRODUCT_SHIP": {"data_type": "bool"},
        "PRODUCT_FREE": {"data_type": "bool"},
    },
    "product_availability": {
        "STATUS": {"data_type": "bool"},
        "FREE_SHIPPING": {"data_type": "bool"},
        "AVAILABLE": {"data_type": "bool"},
    },
    "product_price": {
        "DEFAULT_PRICE": {"data_type": "bool"},
    },
}

# Timestamp cursors catch updates for the hot cart tables. ID cursors are used
# for mostly-static/reference tables that do not reliably maintain audit fields.
TABLE_INCREMENTAL_CURSORS = {
    "customer": "CUSTOMER_ID",
    "product": "DATE_MODIFIED",
    "product_availability": "PRODUCT_AVAIL_ID",
    "product_description": "DESCRIPTION_ID",
    "product_price": "PRODUCT_PRICE_ID",
    "shopping_cart": "DATE_MODIFIED",
    "shopping_cart_item": "DATE_MODIFIED",
}


def _csv_list(value: str | None, default: Iterable[str]) -> list[str]:
    if not value:
        return list(default)
    return [item.strip() for item in value.split(",") if item.strip()]


def _read_key_value_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _tracing_env_file_candidates() -> list[Path]:
    candidates: list[Path] = []
    configured = os.environ.get("TRACING_CONFIG_PATH", "").strip()
    if configured:
        candidates.append(Path(configured).expanduser())

    for base in [Path.cwd(), *Path.cwd().parents, *Path(__file__).resolve().parents]:
        candidate = base / "tracing.env"
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _load_tracing_env_file() -> dict[str, str]:
    for candidate in _tracing_env_file_candidates():
        if candidate.is_file():
            return _read_key_value_file(candidate)
    return {}


TRACING_ENV_VALUES = _load_tracing_env_file()


def _first_non_blank(*values: str | None) -> str | None:
    for value in values:
        if value is not None and value.strip():
            return value.strip()
    return None


def _tracing_config_value(env_name: str) -> str | None:
    return _first_non_blank(
        os.environ.get(env_name),
        TRACING_ENV_VALUES.get(env_name),
    )


def _tracing_config_bool(env_name: str, default: bool) -> bool:
    value = _tracing_config_value(env_name)
    if value is None:
        return default
    return value.lower() not in {"false", "0", "no", "off"}


def default_migration_destinations() -> list[str]:
    explicit_destinations = _first_non_blank(
        os.environ.get("MIGRATION_DESTINATIONS_OVERRIDE"),
        os.environ.get("MIGRATION_DESTINATIONS"),
    )
    if explicit_destinations:
        return _csv_list(explicit_destinations, DEFAULT_DESTINATIONS)

    if not _tracing_config_bool("TRACING_ENABLED_MIGRATIONS", True):
        return []
    return _csv_list(
        _tracing_config_value("TRACING_MIGRATIONS_CLOUD_SOLUTIONS"),
        DEFAULT_DESTINATIONS,
    )


def _require_env(*names: str) -> dict[str, str]:
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
    return {n: os.environ[n] for n in names}


def _base_namespace() -> str:
    return os.environ.get("MIGRATION_NAMESPACE", DEFAULT_NAMESPACE)


def _bq_dataset() -> str:
    return os.environ.get("BQ_DATASET", _base_namespace())


def _snowflake_schema() -> str:
    return os.environ.get("SF_SCHEMA") or os.environ.get("SNOWFLAKE_SCHEMA") or _base_namespace()


def _redshift_schema() -> str:
    return os.environ.get("RS_SCHEMA") or os.environ.get("REDSHIFT_SCHEMA") or "public"


def _write_disposition() -> str:
    return os.environ.get("MIGRATION_WRITE_DISPOSITION", DEFAULT_WRITE_DISPOSITION)


def _incremental_enabled(write_disposition: str) -> bool:
    if write_disposition != "merge":
        return False
    return _bool_env(
        "MIGRATION_INCREMENTAL_ENABLED",
        default=True,
    )


def _merge_write_disposition_config() -> str | dict[str, str]:
    strategy = os.environ.get("MIGRATION_MERGE_STRATEGY", "").strip()
    if not strategy:
        return "merge"
    return {"disposition": "merge", "strategy": strategy}


def _bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _pipeline_name(env_name: str, default_value: str) -> str:
    return os.environ.get(env_name, default_value)


def _normalized_table_names(tables: Iterable[str]) -> list[str]:
    return [table.strip().lower() for table in tables if table.strip()]


def _managed_destination_tables(tables: Iterable[str]) -> list[str]:
    managed = _normalized_table_names(tables)
    for table in DLT_INTERNAL_TABLES:
        if table not in managed:
            managed.append(table)
    return managed


def _pipelines_root() -> Path:
    configured = os.environ.get("DLT_PIPELINES_DIR")
    if configured:
        return Path(configured).expanduser()

    data_dir = os.environ.get("DLT_DATA_DIR")
    if data_dir:
        return Path(data_dir).expanduser() / "pipelines"

    return Path.home() / ".dlt" / "pipelines"


def _reset_pipeline_state_if_requested(
    pipeline_name: str,
    destination_name: str,
    dataset_name: str,
    write_disposition: str,
) -> None:
    should_reset = _bool_env(
        "MIGRATION_RESET_PIPELINE_STATE",
        default=write_disposition == "replace",
    )
    if not should_reset:
        return

    pipeline_dir = _pipelines_root() / pipeline_name
    if not pipeline_dir.exists():
        return

    logger.info(
        "Resetting local dlt pipeline state for %s (pipeline=%s, dataset=%s, path=%s)",
        destination_name,
        pipeline_name,
        dataset_name,
        pipeline_dir,
    )
    shutil.rmtree(pipeline_dir)


def _drop_bigquery_tables_if_requested(
    project_id: str,
    dataset_name: str,
    tables: Iterable[str],
    write_disposition: str,
) -> None:
    if write_disposition != "replace":
        return

    should_drop = _bool_env(
        "MIGRATION_DROP_DESTINATION_TABLES_ON_REPLACE",
        default=True,
    )
    if not should_drop:
        return

    table_names = _managed_destination_tables(tables)
    logger.info(
        "Dropping existing BigQuery tables before replace: dataset=%s tables=%s",
        dataset_name,
        table_names,
    )

    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    for table_name in table_names:
        client.delete_table(f"{project_id}.{dataset_name}.{table_name}", not_found_ok=True)


def _snowflake_identifier(value: str) -> str:
    return '"' + value.upper().replace('"', '""') + '"'


def _snowflake_account(value: str) -> str:
    account = value.strip()
    for prefix in ("https://", "http://"):
        if account.startswith(prefix):
            account = account[len(prefix) :]
    account = account.rstrip("/")
    suffix = ".snowflakecomputing.com"
    if account.endswith(suffix):
        account = account[: -len(suffix)]
    return account


def _snowflake_connector_private_key(private_key: str) -> bytes:
    if "BEGIN" not in private_key:
        return base64.b64decode(private_key)

    from cryptography.hazmat.primitives import serialization

    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").strip()
    loaded_key = serialization.load_pem_private_key(
        private_key.encode("utf-8"),
        password=passphrase.encode("utf-8") if passphrase else None,
    )
    return loaded_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _drop_snowflake_tables_if_requested(
    creds: dict[str, str],
    private_key: str,
    dataset_name: str,
    tables: Iterable[str],
    write_disposition: str,
) -> None:
    if write_disposition != "replace":
        return

    should_drop = _bool_env(
        "MIGRATION_DROP_DESTINATION_TABLES_ON_REPLACE",
        default=True,
    )
    if not should_drop:
        return

    table_names = _managed_destination_tables(tables)
    database = _snowflake_identifier(creds["SNOWFLAKE_DATABASE"])
    schema = _snowflake_identifier(dataset_name)

    logger.info(
        "Dropping existing Snowflake tables before replace: schema=%s.%s tables=%s",
        creds["SNOWFLAKE_DATABASE"],
        dataset_name,
        table_names,
    )

    import snowflake.connector

    connection_config = {
        "user": creds["SNOWFLAKE_USERNAME"],
        "private_key": _snowflake_connector_private_key(private_key),
        "account": _snowflake_account(creds["SNOWFLAKE_HOST"]),
        "warehouse": creds["SNOWFLAKE_WAREHOUSE"],
        "database": creds["SNOWFLAKE_DATABASE"],
        "schema": dataset_name,
        "role": creds["SNOWFLAKE_ROLE"],
    }

    connection = snowflake.connector.connect(**connection_config)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
            for table_name in table_names:
                table = _snowflake_identifier(table_name)
                cursor.execute(f"DROP TABLE IF EXISTS {database}.{schema}.{table}")
        finally:
            cursor.close()
    finally:
        connection.close()


def _load_snowflake_private_key() -> str:
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()
    if key_path:
        path = Path(os.path.expanduser(key_path))
        if not path.is_file():
            raise RuntimeError(f"SNOWFLAKE_PRIVATE_KEY_PATH does not exist: {path}")
        return path.read_text(encoding="utf-8").strip()

    inline_key = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "").strip()
    if inline_key:
        return inline_key

    raise RuntimeError(
        "Missing Snowflake private key. Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PRIVATE_KEY."
    )


def _find_resource(source, table_name: str):
    resource_name = table_name.strip().lower()
    resources = getattr(source, "resources", None)
    if resources is None:
        return None

    for candidate in (resource_name, table_name.strip(), table_name.strip().upper()):
        try:
            return resources[candidate]
        except (KeyError, TypeError):
            pass

    try:
        iterable = resources.values()
    except AttributeError:
        iterable = resources

    for resource in iterable:
        if getattr(resource, "name", "").lower() == resource_name:
            return resource
    return None


def _apply_resource_hints(source, tables: Iterable[str], write_disposition: str) -> None:
    incremental_enabled = _incremental_enabled(write_disposition)
    merge_write_disposition = _merge_write_disposition_config() if incremental_enabled else None
    for table in tables:
        table_name = table.strip().lower()
        column_hints = TABLE_COLUMN_HINTS.get(table_name)
        primary_key = TABLE_PRIMARY_KEYS.get(table_name)
        cursor = TABLE_INCREMENTAL_CURSORS.get(table_name)
        resource = _find_resource(source, table_name)
        if resource is None:
            logger.warning("Could not find dlt resource for table %s; resource hints were not applied", table)
            continue

        hint_kwargs = {}
        if column_hints:
            hint_kwargs["columns"] = column_hints

        if incremental_enabled and primary_key and cursor:
            hint_kwargs.update(
                write_disposition=merge_write_disposition,
                primary_key=primary_key,
                incremental=dlt.sources.incremental(cursor),
            )
        elif incremental_enabled:
            logger.warning("No incremental merge hints configured for table %s", table)

        if hint_kwargs:
            resource.apply_hints(**hint_kwargs)

        if column_hints:
            logger.info("Configured column type hints for %s columns=%s", table_name, list(column_hints))
        if incremental_enabled and primary_key and cursor:
            logger.info(
                "Configured incremental merge for %s with primary_key=%s cursor=%s",
                table_name,
                primary_key,
                cursor,
            )


def build_source(tables: Iterable[str], write_disposition: str):
    dsn = os.environ.get("MYSQL_DSN", DEFAULT_MYSQL_DSN)
    creds = ConnectionStringCredentials(dsn)
    selected_tables = list(tables)
    source = sql_database(creds).with_resources(*selected_tables)
    _apply_resource_hints(source, selected_tables, write_disposition)
    return source


def _run_kwargs(write_disposition: str) -> dict[str, str]:
    # In merge mode, resources carry per-table write_disposition/primary_key/
    # incremental hints. Passing a global write_disposition could override those.
    if _incremental_enabled(write_disposition):
        return {}
    return {"write_disposition": write_disposition}


def run_bigquery(tables: list[str], write_disposition: str) -> None:
    dataset_name = _bq_dataset()
    pipeline_name = _pipeline_name("BQ_PIPELINE_NAME", "shopizer_mysql_to_bigquery")
    project = os.environ.get("BQ_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if project:
        os.environ.setdefault("GOOGLE_CLOUD_PROJECT", project)
        os.environ.setdefault("GCLOUD_PROJECT", project)

    logger.info("Running BigQuery operational-table refresh into dataset=%s", dataset_name)
    _reset_pipeline_state_if_requested(pipeline_name, "bigquery", dataset_name, write_disposition)
    if not project:
        import google.auth

        _, detected_project = google.auth.default()
        project = detected_project
    _drop_bigquery_tables_if_requested(project, dataset_name, tables, write_disposition)
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="bigquery",
        dataset_name=dataset_name,
    )

    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    creds: dict[str, str] = {}
    if project:
        creds["project_id"] = project
    if key_path:
        creds["json_key_filepath"] = key_path

    info = pipeline.run(
        build_source(tables, write_disposition),
        credentials=creds,
        **_run_kwargs(write_disposition),
    )
    logger.info("BigQuery load info: %s", info)


def run_snowflake(tables: list[str], write_disposition: str) -> None:
    creds = _require_env(
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_USERNAME",
        "SNOWFLAKE_HOST",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_ROLE",
    )
    private_key = _load_snowflake_private_key()

    dataset_name = _snowflake_schema()
    pipeline_name = _pipeline_name("SF_PIPELINE_NAME", "shopizer_mysql_to_snowflake")
    logger.info(
        "Running Snowflake operational-table refresh into %s.%s",
        creds["SNOWFLAKE_DATABASE"],
        dataset_name,
    )

    destination_config = {
        "database": creds["SNOWFLAKE_DATABASE"],
        "username": creds["SNOWFLAKE_USERNAME"],
        "private_key": private_key,
        "host": creds["SNOWFLAKE_HOST"],
        "warehouse": creds["SNOWFLAKE_WAREHOUSE"],
        "role": creds["SNOWFLAKE_ROLE"],
    }
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").strip()
    if passphrase:
        destination_config["private_key_passphrase"] = passphrase

    destination = dlt_snowflake_destination(destination_config)
    _reset_pipeline_state_if_requested(pipeline_name, "snowflake", dataset_name, write_disposition)
    _drop_snowflake_tables_if_requested(creds, private_key, dataset_name, tables, write_disposition)
    pipeline = dlt.pipeline(
        destination=destination,
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
    )
    info = pipeline.run(build_source(tables, write_disposition), **_run_kwargs(write_disposition))
    logger.info("Snowflake load info: %s", info)


def run_redshift(tables: list[str], write_disposition: str) -> None:
    creds = _require_env(
        "REDSHIFT_USERNAME",
        "REDSHIFT_PASSWORD",
        "REDSHIFT_HOST",
    )
    dataset_name = _redshift_schema()
    pipeline_name = _pipeline_name("RS_PIPELINE_NAME", "shopizer_mysql_to_redshift")
    logger.info(
        "Running Redshift operational-table refresh into %s.%s",
        os.environ.get("REDSHIFT_DATABASE", "dev"),
        dataset_name,
    )
    destination = dlt_redshift_destination(
        {
            "database": os.environ.get("REDSHIFT_DATABASE", "dev"),
            "username": creds["REDSHIFT_USERNAME"],
            "password": creds["REDSHIFT_PASSWORD"],
            "host": creds["REDSHIFT_HOST"],
            "port": int(os.environ.get("REDSHIFT_PORT", "5439")),
            "connect_timeout": int(os.environ.get("REDSHIFT_CONNECT_TIMEOUT", "30")),
        }
    )
    _reset_pipeline_state_if_requested(pipeline_name, "redshift", dataset_name, write_disposition)
    pipeline = dlt.pipeline(
        destination=destination,
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
    )
    info = pipeline.run(build_source(tables, write_disposition), **_run_kwargs(write_disposition))
    logger.info("Redshift load info: %s", info)


DESTINATIONS: dict[str, Callable[[list[str], str], None]] = {
    "bigquery": run_bigquery,
    "snowflake": run_snowflake,
    "redshift": run_redshift,
}


def run(destinations: list[str], tables: list[str], write_disposition: str | None = None) -> None:
    disposition = write_disposition or _write_disposition()
    failures: list[str] = []

    if not destinations:
        logger.info("Operational refresh skipped because migration tracing is disabled")
        return

    logger.info(
        "Starting operational refresh: destinations=%s tables=%s write_disposition=%s namespace=%s",
        destinations,
        tables,
        disposition,
        _base_namespace(),
    )

    for name in destinations:
        try:
            DESTINATIONS[name](tables, disposition)
        except Exception:
            failures.append(name)
            logger.exception("Destination %s failed", name)

    if failures:
        raise RuntimeError(f"Operational refresh failed for destinations: {', '.join(failures)}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--destinations",
        nargs="+",
        choices=list(DESTINATIONS),
        default=default_migration_destinations(),
        help="Destinations to refresh",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=_csv_list(os.environ.get("MIGRATION_TABLES"), TABLES_TO_MIGRATE),
        help="Operational tables to refresh",
    )
    parser.add_argument(
        "--write-disposition",
        default=_write_disposition(),
        choices=["replace", "append", "merge"],
        help="dlt write disposition to use for the refresh",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    try:
        run(args.destinations, args.tables, args.write_disposition)
    except Exception:
        logger.exception("Operational-table refresh failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
