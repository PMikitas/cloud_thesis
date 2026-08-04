from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter, sleep
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import psycopg2
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from google.auth import default as google_auth_default
from google.auth import load_credentials_from_file
from google.cloud import bigquery

logger = logging.getLogger("metrics_scheduler")

BQ_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PROJECT_RE = re.compile(r"^[A-Za-z0-9-]+$")
WAREHOUSES = ("redshift", "bigquery", "snowflake")
METADATA_PREFIX_RE = re.compile(r"^--\s*(id|slug|warehouse)\s*:\s*(.+?)\s*$", re.IGNORECASE)
LEGACY_BRACKET_HEADING_RE = re.compile(r"^\s*\[\+\]\s*(.+?)\s*$")
LEGACY_NUMBERED_HEADING_RE = re.compile(r"^\s*(\d+)\.\s+(.+?)\s*$")
SQL_BODY_START_RE = re.compile(r"^(WITH|SELECT)\b", re.IGNORECASE)
QUALIFIED_NAME_RE = re.compile(
    r"`?(?:[A-Za-z0-9_-]+\.)?performance_tracking\.([A-Za-z_][A-Za-z0-9_]*)`?",
    re.IGNORECASE,
)

QUERY_CATALOG: dict[int, str] = {
    1: "average_session_duration",
    2: "cart_to_sale_conversion_rate",
    3: "average_session_value_session_ltv",
    4: "average_cart_statistics",
    5: "average_item_sold_price",
    6: "visit_to_purchase_conversion_rate",
    7: "funnel_stage_distribution",
    8: "revenue_by_hour",
    9: "revenue_by_session_intensity_bucket",
    10: "top_products_by_sold_quantity_and_revenue",
    11: "cart_abandonment_by_cart_size",
    12: "successful_carts_per_successful_session",
    13: "estimated_abandoned_cart_value",
    14: "session_duration_by_conversion_status",
    15: "request_count_by_conversion_status",
    16: "endpoint_popularity_by_conversion_status",
    17: "first_endpoint_entry_point_analysis",
    18: "last_endpoint_before_abandoned_session",
}

GROUPS: dict[str, tuple[int, ...]] = {
    "operational_dashboard": (1, 2, 3, 6, 8),
    "business_dashboard": (7, 9, 10, 11, 12),
    "behavioral_analytics": (14, 15, 16, 17, 18),
    "daily_reporting": (4, 5, 13),
}

LEGACY_TITLE_TO_ID: dict[str, int] = {
    "average session duration": 1,
    "percentage of sessions that resulted in a successful sale": 2,
    "average session ltv": 3,
    "average cart stats": 4,
    "avg item sold price": 5,
    "conversion rate from visits to purchase": 6,
    "sessions cart sessions checkout sessions": 7,
    "revenue by hour": 8,
    "revenue by endpoint session intensity": 9,
    "top products by sold quantity and revenue": 10,
    "cart abandonment by cart size": 11,
    "successful carts per successful session": 12,
    "revenue lost from abandoned carts": 13,
    "session duration by conversion status": 14,
    "request count by conversion status": 15,
    "endpoint popularity by converted vs non-converted sessions": 16,
    "first endpoint entry-point analysis": 17,
    "last endpoint before abandoned session": 18,
}


@dataclass(frozen=True)
class QueryDefinition:
    query_id: int
    slug: str
    warehouse: str
    sql: str


@dataclass(frozen=True)
class MetricLatencyInfo:
    calculated_at_utc: datetime
    latest_data_used_at_utc: datetime | None
    latency_seconds: float | None


@dataclass(frozen=True)
class QueryExecutionResult:
    dataframe: pd.DataFrame
    data_volume_bytes: int | None
    data_volume_source: str | None
    query_id: str | None = None
    result_cache_hit: bool | None = None
    returned_bytes: int | None = None
    extra_metadata: dict[str, Any] | None = None


def detect_repo_root() -> Path:
    override = os.environ.get("SHOPIZER_REPO_ROOT")
    if override:
        candidate = Path(override).expanduser()
        if candidate.exists():
            return candidate

    for candidate in [Path.cwd(), *Path.cwd().parents]:
        if (candidate / "sm-shop").exists() and (candidate / "data-migration-scripts").exists():
            return candidate
    raise RuntimeError("Could not detect the Shopizer repo root")


REPO_ROOT = detect_repo_root()
METRICS_ROOT = REPO_ROOT / "metrics-scheduler"
DATA_MIGRATION_ROOT = REPO_ROOT / "data-migration-scripts"
TRACING_ENV = REPO_ROOT / "tracing.env"
ROOT_ENV = REPO_ROOT / ".env"
REDSHIFT_PROPERTIES = REPO_ROOT / "sm-shop" / "src" / "main" / "resources" / "redshift.properties"
SNOWFLAKE_PROPERTIES = REPO_ROOT / "sm-shop" / "src" / "main" / "resources" / "snowflake.properties"
METRICS_ENV = METRICS_ROOT / ".env"
MIGRATION_ENV = DATA_MIGRATION_ROOT / ".env"
DEFAULT_SQL_FILE = REPO_ROOT / "metrics_scripts.sql"
DEFAULT_OUTPUT_DIR = Path(os.environ.get("METRICS_OUTPUT_DIR", "/app/results"))


def load_key_value_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


REDSHIFT_PROPERTY_VALUES = load_key_value_file(REDSHIFT_PROPERTIES)
SNOWFLAKE_PROPERTY_VALUES = load_key_value_file(SNOWFLAKE_PROPERTIES)
ROOT_ENV_VALUES = load_key_value_file(ROOT_ENV) if ROOT_ENV.exists() else {}
METRICS_ENV_VALUES = load_key_value_file(METRICS_ENV) if METRICS_ENV.exists() else {}
MIGRATION_ENV_VALUES = load_key_value_file(MIGRATION_ENV)
TRACING_ENV_VALUES = load_key_value_file(TRACING_ENV) if TRACING_ENV.exists() else {}


def config_value(name: str, *value_maps: dict[str, str]) -> str | None:
    env_value = os.environ.get(name)
    if env_value and env_value.strip():
        return env_value.strip()
    for value_map in value_maps:
        value = value_map.get(name)
        if value and value.strip():
            return value.strip()
    return None


def snowflake_account_from_url(url_value: str) -> str | None:
    if not url_value:
        return None
    parsed = urlparse(url_value)
    host = parsed.netloc or parsed.path
    suffix = ".snowflakecomputing.com"
    return host[:-len(suffix)] if host.endswith(suffix) else host


def split_bigquery_table_id(table_id: str | None) -> tuple[str | None, str | None, str | None]:
    if not table_id:
        return None, None, None
    parts = [part.strip() for part in table_id.split(".") if part.strip()]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, None, None


ROOT_BIGQUERY_PROJECT, ROOT_BIGQUERY_DATASET, ROOT_BIGQUERY_TABLE = split_bigquery_table_id(
    ROOT_ENV_VALUES.get("BIGQUERY_TABLE_ID")
)
BIGQUERY_DATASET = (
    config_value("METRICS_BIGQUERY_DATASET", METRICS_ENV_VALUES)
    or MIGRATION_ENV_VALUES.get("BQ_DATASET")
    or ROOT_BIGQUERY_DATASET
    or "performance_tracking"
)
BIGQUERY_EVENT_TABLE = (
    config_value("METRICS_BIGQUERY_EVENT_TABLE", METRICS_ENV_VALUES)
    or ROOT_BIGQUERY_TABLE
    or "api_tracking"
)
REDSHIFT_DEFAULT_SCHEMA = REDSHIFT_PROPERTY_VALUES.get("redshift.schema", "public")
REDSHIFT_EVENT_TABLE = REDSHIFT_PROPERTY_VALUES.get("redshift.table", "api_performance")
SNOWFLAKE_DEFAULT_SCHEMA = (
    config_value("METRICS_SNOWFLAKE_SCHEMA", METRICS_ENV_VALUES)
    or SNOWFLAKE_PROPERTY_VALUES.get("snowflake.schema")
    or MIGRATION_ENV_VALUES.get("SF_SCHEMA")
    or MIGRATION_ENV_VALUES.get("SNOWFLAKE_SCHEMA")
    or "performance_tracking"
).strip().lower()
SNOWFLAKE_EVENT_TABLE = SNOWFLAKE_PROPERTY_VALUES.get("snowflake.table", "API_TRACKING").lower()
SNOWFLAKE_EVENT_TABLE_OVERRIDE = (
    config_value("METRICS_SNOWFLAKE_EVENT_TABLE", METRICS_ENV_VALUES) or ""
).lower() or None
SNOWFLAKE_EVENT_TABLE_CANDIDATES = tuple(
    dict.fromkeys(
        candidate
        for candidate in (
            SNOWFLAKE_EVENT_TABLE_OVERRIDE,
            SNOWFLAKE_EVENT_TABLE,
        )
        if candidate
    )
)
SNOWFLAKE_EVENT_TABLE_REFERENCE_RE = re.compile(
    rf"\b{re.escape(SNOWFLAKE_DEFAULT_SCHEMA)}\.(api_tracking|api_performance)\b",
    re.IGNORECASE,
)
RESOLVED_SNOWFLAKE_EVENT_TABLE: str | None = None
SNOWFLAKE_EVENT_TABLE_PROBE_RESULTS: dict[str, dict[str, Any]] = {}


def slugify(text: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", text.strip().lower()).strip("_")
    return slug or "query"


def first_non_blank(*values: str | None) -> str | None:
    for value in values:
        if value is not None and value.strip():
            return value.strip()
    return None


def tracing_config_value(env_name: str) -> str | None:
    return first_non_blank(
        os.environ.get(env_name),
        TRACING_ENV_VALUES.get(env_name),
    )


def tracing_config_bool(env_name: str, default: bool) -> bool:
    value = tracing_config_value(env_name)
    if value is None:
        return default
    return value.strip().lower() not in {"false", "0", "no", "off"}


def default_metrics_warehouses() -> list[str]:
    metrics_warehouses = config_value("METRICS_WAREHOUSES", METRICS_ENV_VALUES)
    if metrics_warehouses:
        return [warehouse.strip() for warehouse in metrics_warehouses.split(",") if warehouse.strip()]

    if not tracing_config_bool("TRACING_ENABLED_MIGRATIONS", True):
        return []
    configured = tracing_config_value("TRACING_MIGRATIONS_CLOUD_SOLUTIONS") or "snowflake"
    return [warehouse.strip() for warehouse in configured.split(",") if warehouse.strip()]


def normalize_legacy_title(title: str) -> str:
    normalized = title.strip()
    normalized = re.sub(r"^\d+\.\s*", "", normalized)
    normalized = normalized.replace("→", " ")
    normalized = normalized.replace("/", " ")
    normalized = normalized.replace("-", " ")
    normalized = re.sub(r"[^A-Za-z0-9\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    return normalized


def safe_identifier(value: str, label: str) -> str:
    if not IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def safe_project_id(value: str) -> str:
    if not PROJECT_RE.match(value):
        raise ValueError(f"Invalid BigQuery project id: {value!r}")
    return value


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return int(value)


def env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return float(value)


def parse_redshift_jdbc_url(jdbc_url: str) -> dict[str, Any]:
    if not jdbc_url.startswith("jdbc:redshift://"):
        raise ValueError(f"Unsupported Redshift JDBC URL: {jdbc_url}")
    parsed = urlparse(jdbc_url.removeprefix("jdbc:"))
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5439,
        "database": parsed.path.lstrip("/"),
    }


def resolve_bigquery_credentials_path(env_values: dict[str, str]) -> Path | None:
    candidates = [
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        env_values.get("GOOGLE_APPLICATION_CREDENTIALS"),
        str(Path.home() / ".config" / "gcloud" / "application_default_credentials.json"),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if path.is_file():
            return path
    return None


def build_bigquery_client(env_values: dict[str, str]) -> tuple[bigquery.Client, str]:
    project_id = (
        config_value("METRICS_BIGQUERY_PROJECT_ID", METRICS_ENV_VALUES)
        or env_values.get("BQ_PROJECT_ID")
        or ROOT_BIGQUERY_PROJECT
        or ROOT_ENV_VALUES.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
    )
    credentials_path = resolve_bigquery_credentials_path(env_values)

    if credentials_path:
        credentials, detected_project = load_credentials_from_file(
            str(credentials_path),
            scopes=BQ_SCOPES,
        )
    else:
        credentials, detected_project = google_auth_default(scopes=BQ_SCOPES)

    resolved_project = safe_project_id(project_id or detected_project)
    return bigquery.Client(project=resolved_project, credentials=credentials), resolved_project


def normalize_private_key(raw_key: bytes, passphrase: str | None) -> bytes:
    password = passphrase.encode("utf-8") if passphrase else None
    payload = raw_key.strip()
    if payload.startswith(b"-----BEGIN"):
        key = serialization.load_pem_private_key(payload, password=password)
    else:
        try:
            der_bytes = base64.b64decode(payload, validate=True)
        except Exception:
            der_bytes = payload
        key = serialization.load_der_private_key(der_bytes, password=password)

    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def resolve_snowflake_private_key(
    property_values: dict[str, str],
    env_values: dict[str, str],
) -> bytes:
    passphrase = env_values.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    path_candidates = [
        os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
        env_values.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
        property_values.get("snowflake.private.key.path"),
    ]
    for candidate in path_candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if path.is_file():
            return normalize_private_key(path.read_bytes(), passphrase)

    inline_key = env_values.get("SNOWFLAKE_PRIVATE_KEY") or property_values.get("snowflake.private.key")
    if inline_key:
        return normalize_private_key(inline_key.encode("utf-8"), passphrase)

    raise RuntimeError("Snowflake private key not found in properties, .env, or environment")


def build_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    property_values = load_key_value_file(SNOWFLAKE_PROPERTIES)
    env_values = load_key_value_file(MIGRATION_ENV)
    url_value = property_values.get("snowflake.url", "")
    account = (
        config_value("METRICS_SNOWFLAKE_ACCOUNT", METRICS_ENV_VALUES)
        or config_value("METRICS_SNOWFLAKE_HOST", METRICS_ENV_VALUES)
        or snowflake_account_from_url(url_value)
        or env_values.get("SNOWFLAKE_HOST")
    )

    if not account:
        raise RuntimeError("Snowflake account/host is missing")

    warehouse = (
        config_value("METRICS_SNOWFLAKE_WAREHOUSE", METRICS_ENV_VALUES)
        or property_values["snowflake.warehouse"]
        or env_values.get("SNOWFLAKE_WAREHOUSE")
    )
    if not warehouse:
        raise RuntimeError("Snowflake warehouse is missing")

    connection = snowflake.connector.connect(
        account=account,
        user=(
            config_value("METRICS_SNOWFLAKE_USERNAME", METRICS_ENV_VALUES)
            or config_value("METRICS_SNOWFLAKE_USER", METRICS_ENV_VALUES)
            or property_values["snowflake.user"]
            or env_values.get("SNOWFLAKE_USERNAME")
        ),
        private_key=resolve_snowflake_private_key(property_values, env_values),
        warehouse=warehouse,
        role=(
            config_value("METRICS_SNOWFLAKE_ROLE", METRICS_ENV_VALUES)
            or property_values["snowflake.role"]
            or env_values.get("SNOWFLAKE_ROLE")
        ),
        database=(
            config_value("METRICS_SNOWFLAKE_DATABASE", METRICS_ENV_VALUES)
            or property_values["snowflake.database"]
            or env_values.get("SNOWFLAKE_DATABASE")
        ),
        schema=(
            config_value("METRICS_SNOWFLAKE_SCHEMA", METRICS_ENV_VALUES)
            or property_values["snowflake.schema"]
            or env_values.get("SF_SCHEMA")
            or env_values.get("SNOWFLAKE_SCHEMA")
        ),
    )
    with connection.cursor() as cursor:
        cursor.execute(f"USE WAREHOUSE {safe_identifier(warehouse, 'Snowflake warehouse')}")
    return connection


def build_snowflake_latest_data_sql(table_name: str) -> str:
    return (
        "SELECT MAX(event_timestamp) AS latest_data_used_timestamp "
        f"FROM {SNOWFLAKE_DEFAULT_SCHEMA}.{table_name}"
    )


def build_snowflake_event_table_probe_sql(table_name: str) -> str:
    return (
        "SELECT COUNT(*) AS row_count, "
        "MAX(event_timestamp) AS latest_data_used_timestamp "
        f"FROM {SNOWFLAKE_DEFAULT_SCHEMA}.{table_name}"
    )


def apply_snowflake_event_table(sql: str, table_name: str) -> str:
    return SNOWFLAKE_EVENT_TABLE_REFERENCE_RE.sub(
        f"{SNOWFLAKE_DEFAULT_SCHEMA}.{table_name}",
        sql,
    )


def resolve_snowflake_event_table() -> str:
    global RESOLVED_SNOWFLAKE_EVENT_TABLE

    if RESOLVED_SNOWFLAKE_EVENT_TABLE:
        return RESOLVED_SNOWFLAKE_EVENT_TABLE

    configured_table = SNOWFLAKE_EVENT_TABLE_OVERRIDE or SNOWFLAKE_EVENT_TABLE
    if not IDENTIFIER_RE.fullmatch(configured_table):
        raise RuntimeError(f"Invalid Snowflake event table identifier: {configured_table}")

    connection = build_snowflake_connection()
    try:
        freshest_table: str | None = None
        freshest_timestamp: datetime | None = None
        populated_table: str | None = None
        populated_row_count = 0

        for candidate in SNOWFLAKE_EVENT_TABLE_CANDIDATES:
            qualified_candidate = f"{SNOWFLAKE_DEFAULT_SCHEMA}.{candidate}"
            if not IDENTIFIER_RE.fullmatch(candidate):
                logger.warning("Skipping invalid Snowflake event table candidate: %s", candidate)
                SNOWFLAKE_EVENT_TABLE_PROBE_RESULTS[qualified_candidate] = {
                    "usable": False,
                    "error": "invalid_identifier",
                }
                continue

            try:
                with connection.cursor() as cursor:
                    cursor.execute(build_snowflake_event_table_probe_sql(candidate))
                    row = cursor.fetchone()
            except Exception as exc:
                logger.info(
                    "Snowflake event table candidate %s.%s is not usable for metrics probing: %s",
                    SNOWFLAKE_DEFAULT_SCHEMA,
                    candidate,
                    exc,
                )
                SNOWFLAKE_EVENT_TABLE_PROBE_RESULTS[qualified_candidate] = {
                    "usable": False,
                    "error": str(exc),
                }
                continue

            row_count = as_int_or_none(row[0] if row else None) or 0
            latest_timestamp = normalize_timestamp_value(row[1] if row and len(row) > 1 else None)
            SNOWFLAKE_EVENT_TABLE_PROBE_RESULTS[qualified_candidate] = {
                "usable": True,
                "row_count": row_count,
                "latest_data_used_at_utc": isoformat_utc(latest_timestamp),
            }
            if row_count > populated_row_count:
                populated_row_count = row_count
                populated_table = candidate
            if latest_timestamp is None:
                continue

            if freshest_timestamp is None or latest_timestamp > freshest_timestamp:
                freshest_timestamp = latest_timestamp
                freshest_table = candidate

        if freshest_table is not None:
            RESOLVED_SNOWFLAKE_EVENT_TABLE = freshest_table
            if freshest_table != configured_table:
                logger.warning(
                    "Snowflake metrics switched event table from configured %s.%s to %s.%s "
                    "because the configured table had no fresher data",
                    SNOWFLAKE_DEFAULT_SCHEMA,
                    configured_table,
                    SNOWFLAKE_DEFAULT_SCHEMA,
                    freshest_table,
                )
            else:
                logger.info(
                    "Snowflake metrics using configured event table %s.%s",
                    SNOWFLAKE_DEFAULT_SCHEMA,
                    freshest_table,
                )
            return RESOLVED_SNOWFLAKE_EVENT_TABLE

        if populated_table is not None:
            RESOLVED_SNOWFLAKE_EVENT_TABLE = populated_table
            logger.warning(
                "Snowflake metrics found rows but no event timestamps; using populated table %s.%s "
                "(rows=%d) so result metadata can expose the timestamp problem",
                SNOWFLAKE_DEFAULT_SCHEMA,
                populated_table,
                populated_row_count,
            )
            return RESOLVED_SNOWFLAKE_EVENT_TABLE

        RESOLVED_SNOWFLAKE_EVENT_TABLE = configured_table
        logger.warning(
            "Could not find a Snowflake event table with data among %s; "
            "falling back to configured table %s.%s",
            ", ".join(f"{SNOWFLAKE_DEFAULT_SCHEMA}.{candidate}" for candidate in SNOWFLAKE_EVENT_TABLE_CANDIDATES),
            SNOWFLAKE_DEFAULT_SCHEMA,
            configured_table,
        )
        return RESOLVED_SNOWFLAKE_EVENT_TABLE
    finally:
        connection.close()


def as_int_or_none(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


def fetch_redshift_query_volume(
    cursor: psycopg2.extensions.cursor,
) -> tuple[int | None, str | None, str | None, bool | None, int | None]:
    cursor.execute("SELECT pg_last_query_id()")
    row = cursor.fetchone()
    query_id = row[0] if row else None
    if query_id in (None, -1):
        return None, None, None, None, None

    cursor.execute(
        """
        SELECT
          SUM(blocks_read) AS scan_blocks_read,
          SUM(input_bytes) AS scan_input_bytes
        FROM sys_query_detail
        WHERE query_id = %s
          AND lower(metrics_level) = 'step'
          AND lower(step_name) = 'scan'
        """,
        (query_id,),
    )
    row = cursor.fetchone()
    scan_blocks_read = as_int_or_none(row[0] if row else None)
    scan_input_bytes = as_int_or_none(row[1] if row and len(row) > 1 else None)

    cursor.execute(
        """
        SELECT result_cache_hit, returned_bytes
        FROM sys_query_history
        WHERE query_id = %s
        """,
        (query_id,),
    )
    row = cursor.fetchone()
    result_cache_hit = None if not row else row[0]
    returned_bytes = as_int_or_none(row[1] if row and len(row) > 1 else None)

    if scan_blocks_read is not None and scan_blocks_read > 0:
        return (
            scan_blocks_read * 1024 * 1024,
            "sys_query_detail.scan_blocks_read_x_1mb",
            str(query_id),
            result_cache_hit,
            returned_bytes,
        )

    if scan_input_bytes is not None and scan_input_bytes > 0:
        return (
            scan_input_bytes,
            "sys_query_detail.scan_input_bytes",
            str(query_id),
            result_cache_hit,
            returned_bytes,
        )

    if result_cache_hit is True:
        return 0, "sys_query_history.result_cache_hit_zero_scan", str(query_id), result_cache_hit, returned_bytes

    return None, "redshift.scan_bytes_unavailable", str(query_id), result_cache_hit, returned_bytes


def fetch_redshift_dataframe(sql: str) -> QueryExecutionResult:
    props = load_key_value_file(REDSHIFT_PROPERTIES)
    jdbc_parts = parse_redshift_jdbc_url(props["redshift.jdbc.url"])
    connect_timeout = env_int(
        "METRICS_REDSHIFT_CONNECT_TIMEOUT_SECONDS",
        int(props.get("redshift.driver.connect.timeout.seconds", "5")),
    )
    connect_attempts = env_int("METRICS_REDSHIFT_CONNECT_ATTEMPTS", 3)
    retry_delay_seconds = env_float("METRICS_REDSHIFT_RETRY_DELAY_SECONDS", 5.0)
    disable_result_cache = os.environ.get("METRICS_REDSHIFT_DISABLE_RESULT_CACHE", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    last_error: Exception | None = None

    for attempt in range(1, connect_attempts + 1):
        try:
            with psycopg2.connect(
                host=jdbc_parts["host"],
                port=jdbc_parts["port"],
                dbname=jdbc_parts["database"],
                user=props["redshift.user"],
                password=props["redshift.password"],
                connect_timeout=connect_timeout,
            ) as conn:
                with conn.cursor() as cursor:
                    if disable_result_cache:
                        cursor.execute("SET enable_result_cache_for_session TO off")
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    columns = [description[0] for description in cursor.description]
                    (
                        data_volume_bytes,
                        data_volume_source,
                        query_id,
                        result_cache_hit,
                        returned_bytes,
                    ) = fetch_redshift_query_volume(cursor)
                return QueryExecutionResult(
                    dataframe=pd.DataFrame(rows, columns=columns),
                    data_volume_bytes=data_volume_bytes,
                    data_volume_source=data_volume_source,
                    query_id=query_id,
                    result_cache_hit=result_cache_hit,
                    returned_bytes=returned_bytes,
                )
        except psycopg2.OperationalError as exc:
            last_error = exc
            if attempt == connect_attempts:
                break
            logger.warning(
                "Redshift connection attempt %d/%d failed (connect_timeout=%ss): %s. "
                "Retrying in %.1fs",
                attempt,
                connect_attempts,
                connect_timeout,
                exc,
                retry_delay_seconds,
            )
            sleep(retry_delay_seconds)

    assert last_error is not None
    raise last_error


def fetch_bigquery_dataframe(sql: str) -> QueryExecutionResult:
    env_values = load_key_value_file(MIGRATION_ENV)
    client, _project_id = build_bigquery_client(env_values)
    job = client.query(sql)
    rows = list(job.result())
    return QueryExecutionResult(
        dataframe=pd.DataFrame([dict(row.items()) for row in rows]),
        data_volume_bytes=as_int_or_none(job.total_bytes_processed),
        data_volume_source="bigquery.total_bytes_processed",
        query_id=job.job_id,
    )


def fetch_snowflake_query_volume(
    connection: snowflake.connector.SnowflakeConnection,
    query_id: str,
) -> tuple[int | None, str | None]:
    database_name = connection.database or (
        MIGRATION_ENV_VALUES.get("SNOWFLAKE_DATABASE") or SNOWFLAKE_PROPERTY_VALUES["snowflake.database"]
    )
    safe_query_id = query_id.replace("'", "''")
    history_sql = f"""
        SELECT bytes_scanned
        FROM TABLE({database_name}.INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 100))
        WHERE query_id = '{safe_query_id}'
        ORDER BY end_time DESC
        LIMIT 1
    """
    with connection.cursor() as cursor:
        cursor.execute(history_sql)
        row = cursor.fetchone()
    bytes_scanned = as_int_or_none(row[0] if row else None)
    if bytes_scanned is not None:
        return bytes_scanned, "snowflake.query_history.bytes_scanned"
    return None, None


def fetch_snowflake_dataframe(sql: str) -> QueryExecutionResult:
    connection = build_snowflake_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            query_id = cursor.sfqid
        data_volume_bytes: int | None = None
        data_volume_source: str | None = None
        if query_id:
            try:
                data_volume_bytes, data_volume_source = fetch_snowflake_query_volume(connection, query_id)
            except Exception as exc:
                logger.warning(
                    "Could not resolve Snowflake data volume for query_id=%s: %s",
                    query_id,
                    exc,
                )
    finally:
        connection.close()
    return QueryExecutionResult(
        dataframe=pd.DataFrame(rows, columns=columns),
        data_volume_bytes=data_volume_bytes,
        data_volume_source=data_volume_source,
        query_id=query_id,
    )


def split_top_level_args(text: str) -> list[str]:
    args: list[str] = []
    current: list[str] = []
    depth = 0
    quote: str | None = None
    i = 0

    while i < len(text):
        ch = text[i]
        if quote:
            current.append(ch)
            if ch == quote:
                if i + 1 < len(text) and text[i + 1] == quote:
                    current.append(text[i + 1])
                    i += 1
                else:
                    quote = None
            i += 1
            continue

        if ch in {"'", '"'}:
            quote = ch
            current.append(ch)
            i += 1
            continue

        if ch == "(":
            depth += 1
            current.append(ch)
            i += 1
            continue

        if ch == ")":
            depth -= 1
            current.append(ch)
            i += 1
            continue

        if ch == "," and depth == 0:
            args.append("".join(current).strip())
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    if current:
        args.append("".join(current).strip())
    return args


def _find_matching_paren(text: str, open_index: int) -> int:
    depth = 1
    quote: str | None = None
    i = open_index + 1
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == quote:
                if i + 1 < len(text) and text[i + 1] == quote:
                    i += 2
                    continue
                quote = None
            i += 1
            continue
        if ch in {"'", '"'}:
            quote = ch
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise RuntimeError("Unbalanced parentheses while translating SQL")


def replace_function_calls(sql: str, function_name: str, repl: callable[[str], str]) -> str:
    lower_sql = sql.lower()
    needle = function_name.lower()
    result: list[str] = []
    i = 0

    while i < len(sql):
        start = lower_sql.find(needle, i)
        if start == -1:
            result.append(sql[i:])
            break

        result.append(sql[i:start])
        prev_char = sql[start - 1] if start > 0 else ""
        if prev_char and (prev_char.isalnum() or prev_char == "_"):
            result.append(sql[start:start + len(function_name)])
            i = start + len(function_name)
            continue

        j = start + len(function_name)
        while j < len(sql) and sql[j].isspace():
            j += 1
        if j >= len(sql) or sql[j] != "(":
            result.append(sql[start:j])
            i = j
            continue

        end = _find_matching_paren(sql, j)
        args_text = sql[j + 1:end]
        result.append(repl(args_text))
        i = end + 1

    return "".join(result)


def replace_approx_quantiles(sql: str) -> str:
    pattern = re.compile(r"APPROX_QUANTILES\s*\(", re.IGNORECASE)
    result: list[str] = []
    i = 0

    while True:
        match = pattern.search(sql, i)
        if not match:
            result.append(sql[i:])
            break

        result.append(sql[i:match.start()])
        open_index = sql.find("(", match.start())
        close_index = _find_matching_paren(sql, open_index)
        args = split_top_level_args(sql[open_index + 1:close_index])
        if len(args) != 2:
            raise RuntimeError("APPROX_QUANTILES expected two arguments")

        offset_match = re.match(r"\s*\[\s*OFFSET\s*\(\s*(\d+)\s*\)\s*\]", sql[close_index + 1:], re.IGNORECASE)
        if not offset_match:
            raise RuntimeError("APPROX_QUANTILES translation expected [OFFSET(n)]")

        percentile = int(offset_match.group(1)) / 100
        replacement = f"PERCENTILE_CONT({percentile}) WITHIN GROUP (ORDER BY {args[0]})"
        result.append(replacement)
        i = close_index + 1 + offset_match.end()

    return "".join(result)


def rewrite_relation_names(sql: str, warehouse: str) -> str:
    def repl(match: re.Match[str]) -> str:
        table_name = match.group(1)
        if table_name.lower() in {"api_performance", "api_tracking"}:
            if warehouse == "redshift":
                return f"{REDSHIFT_DEFAULT_SCHEMA}.{REDSHIFT_EVENT_TABLE}"
            if warehouse == "snowflake":
                return f"{SNOWFLAKE_DEFAULT_SCHEMA}.{SNOWFLAKE_EVENT_TABLE}"
            if warehouse == "bigquery":
                return f"{BIGQUERY_DATASET}.{BIGQUERY_EVENT_TABLE}"
        if warehouse == "redshift":
            return f"{REDSHIFT_DEFAULT_SCHEMA}.{table_name.lower()}"
        if warehouse == "snowflake":
            return f"{SNOWFLAKE_DEFAULT_SCHEMA}.{table_name.lower()}"
        return match.group(0)

    return QUALIFIED_NAME_RE.sub(repl, sql)


def translate_bigquery_sql(sql: str, warehouse: str) -> str:
    if warehouse == "bigquery":
        return rewrite_relation_names(sql, warehouse)

    translated = sql
    translated = re.sub(r"\br'", "'", translated)
    translated = re.sub(r'`([^`]+)`', r"\1", translated)
    translated = rewrite_relation_names(translated, warehouse)

    def countif_repl(args_text: str) -> str:
        condition = args_text.strip()
        if warehouse == "snowflake":
            return f"COUNT_IF({condition})"
        return f"SUM(CASE WHEN {condition} THEN 1 ELSE 0 END)"

    def safe_divide_repl(args_text: str) -> str:
        left, right = split_top_level_args(args_text)
        return (
            f"(CASE WHEN ({right}) = 0 THEN NULL "
            f"ELSE CAST(({left}) AS DOUBLE PRECISION) / NULLIF(({right}), 0) END)"
        )

    def timestamp_diff_repl(args_text: str) -> str:
        end_expr, start_expr, unit = split_top_level_args(args_text)
        unit_name = unit.strip().strip("'").strip('"').lower()
        if warehouse == "snowflake":
            return f"DATEDIFF('{unit_name}', {start_expr}, {end_expr})"
        return f"DATEDIFF({unit_name}, {start_expr}, {end_expr})"

    def timestamp_trunc_repl(args_text: str) -> str:
        value_expr, unit = split_top_level_args(args_text)
        unit_name = unit.strip().strip("'").strip('"').lower()
        return f"DATE_TRUNC('{unit_name}', {value_expr})"

    def regexp_contains_repl(args_text: str) -> str:
        value_expr, pattern = split_top_level_args(args_text)
        if warehouse == "snowflake":
            return f"REGEXP_LIKE({value_expr}, {pattern})"
        return f"({value_expr} ~ {pattern})"

    def regexp_extract_repl(args_text: str) -> str:
        value_expr, pattern = split_top_level_args(args_text)
        pattern_text = pattern.lower()
        if "/api/v1/auth/cart/" in pattern_text and "/checkout" in pattern_text:
            return f"SPLIT_PART(SPLIT_PART({value_expr}, '/api/v1/auth/cart/', 2), '/checkout', 1)"
        raise RuntimeError(f"Unsupported REGEXP_EXTRACT pattern for {warehouse}: {pattern}")

    translated = replace_function_calls(translated, "COUNTIF", countif_repl)
    translated = replace_function_calls(translated, "SAFE_DIVIDE", safe_divide_repl)
    translated = replace_function_calls(translated, "TIMESTAMP_DIFF", timestamp_diff_repl)
    translated = replace_function_calls(translated, "TIMESTAMP_TRUNC", timestamp_trunc_repl)
    translated = replace_function_calls(translated, "REGEXP_CONTAINS", regexp_contains_repl)
    translated = replace_function_calls(translated, "REGEXP_EXTRACT", regexp_extract_repl)
    translated = replace_approx_quantiles(translated)
    return translated


def expand_translated_definitions(
    definitions: dict[tuple[int, str], QueryDefinition],
    warehouses: tuple[str, ...],
) -> dict[tuple[int, str], QueryDefinition]:
    expanded = dict(definitions)
    for query_id, slug in QUERY_CATALOG.items():
        source = expanded.get((query_id, "all")) or expanded.get((query_id, "bigquery"))
        if source is None:
            continue

        for warehouse in warehouses:
            key = (query_id, warehouse)
            if key in expanded:
                continue
            if source.warehouse == warehouse:
                expanded[key] = source
                continue
            if source.warehouse in {"all", "bigquery"} and warehouse in {"redshift", "snowflake"}:
                translated_sql = translate_bigquery_sql(source.sql, warehouse)
                expanded[key] = QueryDefinition(
                    query_id=query_id,
                    slug=slug,
                    warehouse=warehouse,
                    sql=translated_sql,
                )
                logger.info(
                    "Generated %s SQL variant for q%02d (%s) from the BigQuery definition",
                    warehouse,
                    query_id,
                    slug,
                )
    return expanded


def extract_sql_body(lines: list[str]) -> str:
    sql_start_index: int | None = None
    for index, line in enumerate(lines):
        if SQL_BODY_START_RE.match(line.strip()):
            sql_start_index = index
            break

    if sql_start_index is None:
        return ""

    body = "".join(lines[sql_start_index:]).strip()
    return body


def infer_warehouse_from_sql(sql: str) -> str:
    bigquery_markers = (
        "`performance_tracking.",
        "COUNTIF(",
        "SAFE_DIVIDE(",
        "REGEXP_CONTAINS(",
        "REGEXP_EXTRACT(",
        "APPROX_QUANTILES(",
        "TIMESTAMP_DIFF(",
        "TIMESTAMP_TRUNC(",
    )
    if any(marker in sql for marker in bigquery_markers):
        return "bigquery"
    return "all"


def parse_legacy_query_definitions(sql_file: Path) -> dict[tuple[int, str], QueryDefinition]:
    definitions: dict[tuple[int, str], QueryDefinition] = {}
    current_heading: str | None = None
    current_query_id: int | None = None
    current_lines: list[str] = []

    def finalize_current() -> None:
        nonlocal current_heading, current_query_id, current_lines
        if current_heading is None:
            current_lines = []
            return

        resolved_query_id = current_query_id
        if resolved_query_id is None:
            resolved_query_id = LEGACY_TITLE_TO_ID.get(normalize_legacy_title(current_heading))

        sql = extract_sql_body(current_lines)
        if resolved_query_id is None:
            logger.info("Skipping unmapped legacy metrics section: %s", current_heading)
        elif not sql:
            logger.info("Skipping empty legacy metrics section: q%02d %s", resolved_query_id, current_heading)
        else:
            warehouse = infer_warehouse_from_sql(sql)
            definitions[(resolved_query_id, warehouse)] = QueryDefinition(
                query_id=resolved_query_id,
                slug=QUERY_CATALOG.get(resolved_query_id, slugify(current_heading)),
                warehouse=warehouse,
                sql=sql,
            )

        current_heading = None
        current_query_id = None
        current_lines = []

    for raw_line in sql_file.read_text(encoding="utf-8").splitlines(keepends=True):
        bracket_match = LEGACY_BRACKET_HEADING_RE.match(raw_line)
        numbered_match = LEGACY_NUMBERED_HEADING_RE.match(raw_line)

        if bracket_match:
            finalize_current()
            heading = bracket_match.group(1).strip()
            explicit_number = LEGACY_NUMBERED_HEADING_RE.match(heading)
            if explicit_number:
                current_query_id = int(explicit_number.group(1))
                current_heading = explicit_number.group(2).strip()
            else:
                current_query_id = None
                current_heading = heading
            continue

        if numbered_match:
            finalize_current()
            current_query_id = int(numbered_match.group(1))
            current_heading = numbered_match.group(2).strip()
            continue

        current_lines.append(raw_line)

    finalize_current()
    return definitions


def load_query_definitions(sql_file: Path) -> dict[tuple[int, str], QueryDefinition]:
    if not sql_file.is_file():
        raise RuntimeError(f"SQL file not found: {sql_file}")

    text = sql_file.read_text(encoding="utf-8")
    if "-- id:" not in text.lower():
        definitions = parse_legacy_query_definitions(sql_file)
        if not definitions:
            raise RuntimeError(
                f"No legacy query definitions could be parsed from {sql_file}. "
                "Expected headings like '[+] Average session duration' or '9. Revenue by ...'."
            )
        return definitions

    definitions: dict[tuple[int, str], QueryDefinition] = {}
    current_meta: dict[str, str] = {}
    current_sql: list[str] = []

    def finalize_current() -> None:
        nonlocal current_meta, current_sql
        if not current_meta and not current_sql:
            return
        query_id_raw = current_meta.get("id")
        if not query_id_raw:
            raise RuntimeError(
                "Each query block must start with a metadata header like '-- id: 1'"
            )
        query_id = int(query_id_raw)
        slug = current_meta.get("slug") or QUERY_CATALOG.get(query_id)
        if not slug:
            raise RuntimeError(f"Missing slug for query id {query_id}")
        warehouse = (current_meta.get("warehouse") or "all").strip().lower()
        if warehouse != "all" and warehouse not in WAREHOUSES:
            raise RuntimeError(f"Unsupported warehouse '{warehouse}' in query {query_id}")
        sql = "".join(current_sql).strip()
        if not sql:
            raise RuntimeError(f"Query {query_id} ({slug}) has no SQL body")
        key = (query_id, warehouse)
        definitions[key] = QueryDefinition(
            query_id=query_id,
            slug=slug,
            warehouse=warehouse,
            sql=sql,
        )
        current_meta = {}
        current_sql = []

    for raw_line in text.splitlines(keepends=True):
        match = METADATA_PREFIX_RE.match(raw_line.strip())
        if match and (not current_sql or not "".join(current_sql).strip()):
            key = match.group(1).lower()
            value = match.group(2).strip()
            if key == "id" and "id" in current_meta:
                finalize_current()
                current_meta["id"] = value
            else:
                current_meta[key] = value
            continue

        current_sql.append(raw_line)

    finalize_current()

    if not definitions:
        raise RuntimeError(
            f"No query definitions were found in {sql_file}. "
            "Add blocks with headers like '-- id: 1', '-- slug: average_session_duration', "
            "and optional '-- warehouse: redshift|bigquery|snowflake|all'."
        )
    return definitions


def resolve_definition(
    definitions: dict[tuple[int, str], QueryDefinition],
    query_id: int,
    warehouse: str,
) -> QueryDefinition:
    return definitions.get((query_id, warehouse)) or definitions.get((query_id, "all"))


def validate_group_coverage(
    definitions: dict[tuple[int, str], QueryDefinition],
    group_name: str,
    warehouses: tuple[str, ...],
) -> None:
    missing: list[str] = []
    for query_id in GROUPS[group_name]:
        for warehouse in warehouses:
            if resolve_definition(definitions, query_id, warehouse) is None:
                missing.append(f"q{query_id}:{warehouse}")
    if missing:
        raise RuntimeError(
            f"Missing query definitions for group '{group_name}': {', '.join(missing)}"
        )


def execute_query_for_warehouse(warehouse: str, sql: str) -> QueryExecutionResult:
    if warehouse == "redshift":
        return fetch_redshift_dataframe(rewrite_relation_names(sql, warehouse))
    if warehouse == "bigquery":
        return fetch_bigquery_dataframe(rewrite_relation_names(sql, warehouse))
    if warehouse == "snowflake":
        resolved_table = resolve_snowflake_event_table()
        normalized_sql = rewrite_relation_names(sql, warehouse)
        result = fetch_snowflake_dataframe(apply_snowflake_event_table(normalized_sql, resolved_table))
        return QueryExecutionResult(
            dataframe=result.dataframe,
            data_volume_bytes=result.data_volume_bytes,
            data_volume_source=result.data_volume_source,
            query_id=result.query_id,
            result_cache_hit=result.result_cache_hit,
            returned_bytes=result.returned_bytes,
            extra_metadata={
                **(result.extra_metadata or {}),
                "snowflake_event_table_used": f"{SNOWFLAKE_DEFAULT_SCHEMA}.{resolved_table}",
                "snowflake_schema_used": SNOWFLAKE_DEFAULT_SCHEMA,
                "snowflake_event_table_candidates": [
                    f"{SNOWFLAKE_DEFAULT_SCHEMA}.{candidate}"
                    for candidate in SNOWFLAKE_EVENT_TABLE_CANDIDATES
                ],
                "snowflake_event_table_probe_results": dict(SNOWFLAKE_EVENT_TABLE_PROBE_RESULTS),
            },
        )
    raise RuntimeError(f"Unsupported warehouse: {warehouse}")


def dataframe_for_warehouse(warehouse: str, sql: str) -> pd.DataFrame:
    return execute_query_for_warehouse(warehouse, sql).dataframe


def timestamp_slug() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def isoformat_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def normalize_timestamp_value(value: Any) -> datetime | None:
    if value is None or pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        resolved = value.to_pydatetime()
    elif isinstance(value, datetime):
        resolved = value
    else:
        parsed = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(parsed):
            return None
        if isinstance(parsed, pd.Timestamp):
            resolved = parsed.to_pydatetime()
        else:
            resolved = parsed

    if resolved.tzinfo is None:
        return resolved.replace(tzinfo=UTC)
    return resolved.astimezone(UTC)


def build_latest_data_timestamp_sql(warehouse: str) -> str:
    if warehouse == "bigquery":
        return (
            "SELECT MAX(event_timestamp) AS latest_data_used_timestamp "
            f"FROM `{BIGQUERY_DATASET}.{BIGQUERY_EVENT_TABLE}`"
        )
    if warehouse == "redshift":
        return (
            "SELECT MAX(event_timestamp) AS latest_data_used_timestamp "
            f"FROM {REDSHIFT_DEFAULT_SCHEMA}.{REDSHIFT_EVENT_TABLE}"
        )
    if warehouse == "snowflake":
        return build_snowflake_latest_data_sql(resolve_snowflake_event_table())
    raise RuntimeError(f"Unsupported warehouse: {warehouse}")


def fetch_latest_data_used_timestamp(warehouse: str) -> datetime | None:
    freshness_df = dataframe_for_warehouse(warehouse, build_latest_data_timestamp_sql(warehouse))
    if freshness_df.empty or freshness_df.shape[1] == 0:
        return None
    return normalize_timestamp_value(freshness_df.iloc[0, 0])


def build_metric_latency_info(
    calculated_at_utc: datetime,
    latest_data_used_at_utc: datetime | None,
) -> MetricLatencyInfo:
    latency_seconds: float | None = None
    if latest_data_used_at_utc is not None:
        latency_seconds = max(
            0.0,
            (calculated_at_utc - latest_data_used_at_utc).total_seconds(),
        )
    return MetricLatencyInfo(
        calculated_at_utc=calculated_at_utc,
        latest_data_used_at_utc=latest_data_used_at_utc,
        latency_seconds=latency_seconds,
    )


def write_result_files(
    output_dir: Path,
    group_name: str,
    warehouse: str,
    definition: QueryDefinition,
    execution_result: QueryExecutionResult,
    duration_seconds: float,
    latency_info: MetricLatencyInfo,
) -> None:
    run_key = latency_info.calculated_at_utc.strftime("%Y%m%dT%H%M%SZ")
    query_folder = output_dir / warehouse / group_name / f"q{definition.query_id:02d}_{definition.slug}"
    query_folder.mkdir(parents=True, exist_ok=True)

    csv_path = query_folder / f"{run_key}.csv"
    metadata_path = query_folder / f"{run_key}.json"
    dataframe = execution_result.dataframe

    dataframe.to_csv(csv_path, index=False)
    result_size_bytes = csv_path.stat().st_size
    metadata = {
        "executed_at_utc": run_key,
        "calculated_at_utc": isoformat_utc(latency_info.calculated_at_utc),
        "latest_data_used_at_utc": isoformat_utc(latency_info.latest_data_used_at_utc),
        "data_latency_seconds": (
            round(latency_info.latency_seconds, 3)
            if latency_info.latency_seconds is not None
            else None
        ),
        "data_latency_minutes": (
            round(latency_info.latency_seconds / 60, 3)
            if latency_info.latency_seconds is not None
            else None
        ),
        "group": group_name,
        "warehouse": warehouse,
        "query_id": definition.query_id,
        "slug": definition.slug,
        "row_count": len(dataframe),
        "duration_seconds": round(duration_seconds, 3),
        "data_volume_bytes": execution_result.data_volume_bytes,
        "data_volume_megabytes": (
            round(execution_result.data_volume_bytes / (1024 * 1024), 3)
            if execution_result.data_volume_bytes is not None
            else None
        ),
        "data_volume_source": execution_result.data_volume_source,
        "result_size_bytes": result_size_bytes,
        "result_size_megabytes": round(result_size_bytes / (1024 * 1024), 6),
        "result_size_source": "csv_output_file_size",
        "warehouse_query_id": execution_result.query_id,
        "warehouse_result_cache_hit": execution_result.result_cache_hit,
        "warehouse_returned_bytes": execution_result.returned_bytes,
        "csv_path": str(csv_path),
    }
    if execution_result.extra_metadata:
        metadata.update(execution_result.extra_metadata)
    metadata_path.write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    logger.info(
        "Stored metrics result: warehouse=%s group=%s query=q%02d slug=%s rows=%d "
        "latest_data_used_at=%s latency_seconds=%s data_volume_mb=%s result_size_kb=%s path=%s",
        warehouse,
        group_name,
        definition.query_id,
        definition.slug,
        len(dataframe),
        isoformat_utc(latency_info.latest_data_used_at_utc) or "n/a",
        (
            f"{latency_info.latency_seconds:.3f}"
            if latency_info.latency_seconds is not None
            else "n/a"
        ),
        (
            f"{execution_result.data_volume_bytes / (1024 * 1024):.3f}"
            if execution_result.data_volume_bytes is not None
            else "n/a"
        ),
        f"{result_size_bytes / 1024:.3f}",
        csv_path,
    )


def run_group(
    group_name: str,
    sql_file: Path,
    output_dir: Path,
    warehouses: tuple[str, ...] = WAREHOUSES,
) -> None:
    if group_name not in GROUPS:
        raise RuntimeError(f"Unknown query group: {group_name}")

    definitions = expand_translated_definitions(load_query_definitions(sql_file), warehouses)
    validate_group_coverage(definitions, group_name, warehouses)
    output_dir.mkdir(parents=True, exist_ok=True)
    collected_latencies: list[float] = []

    logger.info(
        "Running metrics group=%s warehouses=%s sql_file=%s output_dir=%s",
        group_name,
        warehouses,
        sql_file,
        output_dir,
    )

    for query_id in GROUPS[group_name]:
        for warehouse in warehouses:
            definition = resolve_definition(definitions, query_id, warehouse)
            started = perf_counter()
            execution_result = execute_query_for_warehouse(warehouse, definition.sql)
            duration_seconds = perf_counter() - started
            calculated_at_utc = datetime.now(UTC)
            latest_data_used_at_utc = fetch_latest_data_used_timestamp(warehouse)
            latency_info = build_metric_latency_info(calculated_at_utc, latest_data_used_at_utc)
            if latency_info.latency_seconds is not None:
                collected_latencies.append(latency_info.latency_seconds)
            write_result_files(
                output_dir,
                group_name,
                warehouse,
                definition,
                execution_result,
                duration_seconds,
                latency_info,
            )

    if collected_latencies:
        average_latency_seconds = sum(collected_latencies) / len(collected_latencies)
        logger.info(
            "Completed metrics group=%s warehouses=%s average_data_latency_seconds=%.3f "
            "average_data_latency_minutes=%.3f metrics=%d",
            group_name,
            warehouses,
            average_latency_seconds,
            average_latency_seconds / 60,
            len(collected_latencies),
        )
    else:
        logger.info(
            "Completed metrics group=%s warehouses=%s average_data_latency_seconds=n/a metrics=0",
            group_name,
            warehouses,
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    default_warehouses = default_metrics_warehouses()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--group",
        required=True,
        choices=sorted(GROUPS),
        help="Named query group to execute",
    )
    parser.add_argument(
        "--sql-file",
        default=str(Path(os.environ.get("METRICS_SQL_FILE", str(DEFAULT_SQL_FILE))).expanduser()),
        help="Path to metrics_scripts.sql",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where CSV and metadata outputs should be written",
    )
    parser.add_argument(
        "--warehouses",
        nargs="+",
        choices=list(WAREHOUSES),
        default=default_warehouses,
        help="Subset of warehouses to execute",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = parse_args(argv)
    run_group(
        group_name=args.group,
        sql_file=Path(args.sql_file).expanduser(),
        output_dir=Path(args.output_dir).expanduser(),
        warehouses=tuple(args.warehouses),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
