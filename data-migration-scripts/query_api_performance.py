from __future__ import annotations

import argparse
import base64
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import psycopg2
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from google.auth import default as google_auth_default
from google.auth import load_credentials_from_file
from google.cloud import bigquery

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_MIGRATION_ROOT = Path(__file__).resolve().parent
METRICS_ROOT = REPO_ROOT / "metrics-scheduler"
REDSHIFT_PROPERTIES = REPO_ROOT / "sm-shop" / "src" / "main" / "resources" / "redshift.properties"
SNOWFLAKE_PROPERTIES = REPO_ROOT / "sm-shop" / "src" / "main" / "resources" / "snowflake.properties"
MIGRATION_ENV = DATA_MIGRATION_ROOT / ".env"
METRICS_ENV = METRICS_ROOT / ".env"
DEFAULT_CUTOFF = "2026-04-29"
DEFAULT_EVENT_TABLE = "api_performance"
DEFAULT_SNOWFLAKE_ANALYTICS_WAREHOUSE = "ANALYTICS_WH"
BQ_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PROJECT_RE = re.compile(r"^[A-Za-z0-9-]+$")


def load_key_value_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def first_non_blank(*values: str | None) -> str | None:
    for value in values:
        if value and value.strip():
            return value.strip()
    return None


def safe_identifier(value: str, label: str) -> str:
    if not IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def safe_project_id(value: str) -> str:
    if not PROJECT_RE.match(value):
        raise ValueError(f"Invalid BigQuery project id: {value!r}")
    return value


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


def build_bigquery_client(env_values: dict[str, str]) -> tuple[bigquery.Client, str, str]:
    project_id = env_values.get("BQ_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    dataset = env_values.get("BQ_DATASET", "performance_tracking")
    credentials_path = resolve_bigquery_credentials_path(env_values)

    if credentials_path:
        credentials, detected_project = load_credentials_from_file(
            str(credentials_path),
            scopes=BQ_SCOPES,
        )
    else:
        credentials, detected_project = google_auth_default(scopes=BQ_SCOPES)

    resolved_project = safe_project_id(project_id or detected_project)
    safe_identifier(dataset, "BigQuery dataset")
    return bigquery.Client(project=resolved_project, credentials=credentials), resolved_project, dataset


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


def build_snowflake_connection(
    property_values: dict[str, str],
    env_values: dict[str, str],
) -> tuple[snowflake.connector.SnowflakeConnection, str, str, str]:
    metrics_values = load_key_value_file(METRICS_ENV)
    url_value = property_values.get("snowflake.url", "")
    account = env_values.get("SNOWFLAKE_HOST")
    if not account and url_value:
        parsed = urlparse(url_value)
        host = parsed.netloc or parsed.path
        suffix = ".snowflakecomputing.com"
        account = host[:-len(suffix)] if host.endswith(suffix) else host

    if not account:
        raise RuntimeError("Snowflake account/host is missing")

    database = env_values.get("SNOWFLAKE_DATABASE") or property_values["snowflake.database"]
    schema = env_values.get("SF_SCHEMA") or property_values["snowflake.schema"]
    table = property_values.get("snowflake.table", DEFAULT_EVENT_TABLE)
    warehouse = first_non_blank(
        os.environ.get("METRICS_SNOWFLAKE_WAREHOUSE"),
        metrics_values.get("METRICS_SNOWFLAKE_WAREHOUSE"),
        env_values.get("METRICS_SNOWFLAKE_WAREHOUSE"),
        DEFAULT_SNOWFLAKE_ANALYTICS_WAREHOUSE,
    )

    safe_identifier(schema, "Snowflake schema")
    safe_identifier(table, "Snowflake table")
    safe_identifier(warehouse, "Snowflake warehouse")

    connection = snowflake.connector.connect(
        account=account,
        user=env_values.get("SNOWFLAKE_USERNAME") or property_values["snowflake.user"],
        private_key=resolve_snowflake_private_key(property_values, env_values),
        warehouse=warehouse,
        role=env_values.get("SNOWFLAKE_ROLE") or property_values["snowflake.role"],
        database=database,
        schema=schema,
    )
    with connection.cursor() as cursor:
        cursor.execute(f"USE WAREHOUSE {warehouse}")
    return connection, database, schema, table


def fetch_redshift_dataframe(cutoff: str, table: str = DEFAULT_EVENT_TABLE) -> pd.DataFrame:
    props = load_key_value_file(REDSHIFT_PROPERTIES)
    jdbc_parts = parse_redshift_jdbc_url(props["redshift.jdbc.url"])
    schema = safe_identifier(props.get("redshift.schema", "public"), "Redshift schema")
    safe_identifier(table, "Redshift table")

    with psycopg2.connect(
        host=jdbc_parts["host"],
        port=jdbc_parts["port"],
        dbname=jdbc_parts["database"],
        user=props["redshift.user"],
        password=props["redshift.password"],
        connect_timeout=int(props.get("redshift.driver.connect.timeout.seconds", "5")),
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT *
                FROM {schema}.{table}
                WHERE event_timestamp > %s
                ORDER BY event_timestamp
                """,
                (cutoff,),
            )
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]

    return pd.DataFrame(rows, columns=columns)


def fetch_bigquery_dataframe(cutoff: str, table: str = DEFAULT_EVENT_TABLE) -> pd.DataFrame:
    env_values = load_key_value_file(MIGRATION_ENV)
    client, project_id, dataset = build_bigquery_client(env_values)
    safe_identifier(table, "BigQuery table")

    query = f"""
        SELECT *
        FROM `{project_id}.{dataset}.{table}`
        WHERE event_timestamp > @cutoff
        ORDER BY event_timestamp
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cutoff", "TIMESTAMP", f"{cutoff} 00:00:00+00:00")
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    return pd.DataFrame([dict(row.items()) for row in rows])


def fetch_snowflake_dataframe(cutoff: str, table: str | None = None) -> pd.DataFrame:
    props = load_key_value_file(SNOWFLAKE_PROPERTIES)
    env_values = load_key_value_file(MIGRATION_ENV)
    connection, database, schema, default_table = build_snowflake_connection(props, env_values)
    chosen_table = safe_identifier(table or default_table, "Snowflake table")

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT *
                FROM {database}.{schema}.{chosen_table}
                WHERE event_timestamp > %s
                ORDER BY event_timestamp
                """,
                (cutoff,),
            )
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
    finally:
        connection.close()

    return pd.DataFrame(rows, columns=columns)


def fetch_api_performance_dataframes(
    cutoff: str = DEFAULT_CUTOFF,
    redshift_table: str = DEFAULT_EVENT_TABLE,
    bigquery_table: str = DEFAULT_EVENT_TABLE,
    snowflake_table: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    redshift_df = fetch_redshift_dataframe(cutoff=cutoff, table=redshift_table)
    bigquery_df = fetch_bigquery_dataframe(cutoff=cutoff, table=bigquery_table)
    snowflake_df = fetch_snowflake_dataframe(cutoff=cutoff, table=snowflake_table)
    return redshift_df, bigquery_df, snowflake_df


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Query api_performance-style event data from Redshift, BigQuery, and Snowflake."
    )
    parser.add_argument("--cutoff", default=DEFAULT_CUTOFF, help="Inclusive date filter baseline")
    parser.add_argument("--redshift-table", default=DEFAULT_EVENT_TABLE)
    parser.add_argument("--bigquery-table", default=DEFAULT_EVENT_TABLE)
    parser.add_argument(
        "--snowflake-table",
        default=None,
        help="Defaults to the table configured in snowflake.properties",
    )
    args = parser.parse_args()

    redshift_df, bigquery_df, snowflake_df = fetch_api_performance_dataframes(
        cutoff=args.cutoff,
        redshift_table=args.redshift_table,
        bigquery_table=args.bigquery_table,
        snowflake_table=args.snowflake_table,
    )

    print(f"redshift_df rows={len(redshift_df)}")
    print(f"bigquery_df rows={len(bigquery_df)}")
    print(f"snowflake_df rows={len(snowflake_df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
