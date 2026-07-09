"""Backward-compatible wrapper for the older module name.

The bridge now targets BigQuery, Snowflake, and Redshift, but some scripts still
import `mysql2bq_tables_migration`. Re-export the new operational refresh entry
point so existing commands keep working.
"""

from __future__ import annotations

import sys

from data_migration_scripts.operational_tables_migration import *  # noqa: F401,F403
from data_migration_scripts.operational_tables_migration import main


if __name__ == "__main__":
    sys.exit(main())
