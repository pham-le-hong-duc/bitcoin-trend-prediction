"""
Shared TimescaleDB utility client.

Provides helper methods for:
- connecting to TimescaleDB/PostgreSQL
- inspecting tables
- creating tables and hypertables
- upserting Polars DataFrames
"""

from __future__ import annotations

import os
import time
from typing import List

import polars as pl
import psycopg2
from psycopg2.extras import Json, execute_values


class TimescaleDBClient:
    """Utility client for TimescaleDB operations."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        self.host = host or os.getenv("TIMESCALE_HOST", "localhost")
        self.port = port or int(os.getenv("TIMESCALE_PORT", "5432"))
        self.database = database or os.getenv("TIMESCALE_DB", "base")
        self.user = user or os.getenv("TIMESCALE_USER", "admin")
        self.password = password or os.getenv("TIMESCALE_PASSWORD", "password")

        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )
        self.conn.autocommit = True

        print(f"Connected to TimescaleDB: {self.host}:{self.port}/{self.database}")

    def execute(self, query: str, params: tuple | None = None):
        """Execute a SQL query and return rows when available."""
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            try:
                return cur.fetchall()
            except psycopg2.ProgrammingError:
                return None

    def query_to_df(self, query: str) -> pl.DataFrame:
        """Run a query and return the result as a Polars DataFrame."""
        with self.conn.cursor() as cur:
            cur.execute(query)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return pl.DataFrame(data, schema=columns)

    def list_schemas(self) -> List[str]:
        """List all non-system schemas in the current database."""
        result = self.execute(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
            AND schema_name NOT LIKE 'pg_toast%%'
            AND schema_name NOT LIKE 'pg_temp%%'
            ORDER BY schema_name
            """
        )
        return [row[0] for row in result] if result else []

    def schema_exists(self, schema_name: str) -> bool:
        """Check whether a schema exists."""
        result = self.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.schemata
                WHERE schema_name = %s
            )
            """,
            (schema_name,),
        )
        return result[0][0] if result else False

    def create_schema(self, schema_name: str) -> bool:
        """Create a schema if it does not already exist."""
        try:
            self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            print(f"Ensured schema '{schema_name}' exists")
            return True
        except Exception as exc:
            print(f"Error creating schema '{schema_name}': {exc}")
            return False

    def ensure_table(
        self,
        schema_name: str,
        table_name: str,
        columns_sql: str,
        hypertable_time_column: str | None = None,
    ) -> None:
        """Create a table if needed and optionally register it as a hypertable."""
        qualified_table = f"{schema_name}.{table_name}"
        self.create_schema(schema_name)
        self.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {qualified_table} (
                {columns_sql}
            )
            """
        )

        if hypertable_time_column:
            self.execute(
                f"""
                SELECT create_hypertable(
                    '{qualified_table}',
                    '{hypertable_time_column}',
                    if_not_exists => TRUE
                )
                """
            )

    def list_tables(self, schema_name: str = "public") -> List[str]:
        """List tables in the given schema."""
        result = self.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = %s
            ORDER BY tablename
            """,
            (schema_name,),
        )
        return [row[0] for row in result] if result else []

    def table_exists(self, table_name: str, schema_name: str = "public") -> bool:
        """Check whether a table exists in the given schema."""
        result = self.execute(
            """
            SELECT EXISTS (
                SELECT FROM pg_tables
                WHERE schemaname = %s
                AND tablename = %s
            )
            """,
            (schema_name, table_name),
        )
        return result[0][0] if result else False

    def get_table_info(self, table_name: str, schema_name: str = "public") -> None:
        """Print table row count and column schema."""
        qualified_table = f"{schema_name}.{table_name}"
        if not self.table_exists(table_name, schema_name=schema_name):
            print(f"Table '{qualified_table}' does not exist")
            return

        count = self.execute(f"SELECT COUNT(*) FROM {qualified_table}")[0][0]
        schema = self.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s
            AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_name, table_name),
        )

        print(f"\nTable: {qualified_table}")
        print(f"Rows: {count:,}")
        print("\nSchema:")
        for col_name, data_type in schema or []:
            print(f"  {col_name}: {data_type}")

    def drop_table(self, table_name: str, schema_name: str = "public") -> bool:
        """Drop a table if it exists."""
        qualified_table = f"{schema_name}.{table_name}"
        try:
            self.execute(f"DROP TABLE IF EXISTS {qualified_table} CASCADE")
            print(f"Dropped table '{qualified_table}'")
            return True
        except Exception as exc:
            print(f"Error dropping table '{qualified_table}': {exc}")
            return False

    def get_existing_timestamps(
        self,
        table_name: str,
        target_date=None,
        schema_name: str = "public",
    ) -> set:
        """Get existing ts_ms values from a table."""
        qualified_table = f"{schema_name}.{table_name}"
        try:
            if target_date:
                date_str = target_date.strftime("%Y-%m-%d")
                result = self.execute(
                    f"SELECT ts_ms FROM {qualified_table} WHERE DATE(timestamp_dt) = %s",
                    (date_str,),
                )
            else:
                result = self.execute(f"SELECT ts_ms FROM {qualified_table}")

            return {row[0] for row in result or []}
        except Exception as exc:
            print(f"Warning: Could not fetch timestamps from '{qualified_table}': {exc}")
            return set()

    def get_complete_timestamps(
        self,
        table_name: str,
        target_date=None,
        schema_name: str = "public",
    ) -> set:
        """Return timestamps for rows that exist in the table."""
        return self.get_existing_timestamps(
            table_name,
            target_date=target_date,
            schema_name=schema_name,
        )

    def upsert_dataframe(
        self,
        df: pl.DataFrame,
        table_name: str,
        key_column: str = "ts_ms",
        schema_name: str = "public",
    ) -> int:
        """Upsert a Polars DataFrame into the target table."""
        qualified_table = f"{schema_name}.{table_name}"
        if df.is_empty():
            print(f"No data to upsert into '{qualified_table}'")
            return 0

        try:
            if not self.table_exists(table_name, schema_name=schema_name):
                raise ValueError(
                    f"Table '{qualified_table}' does not exist. "
                    "Create the table explicitly before upserting data."
                )

            data = [
                tuple(self._adapt_value(value) for value in row)
                for row in df.iter_rows()
            ]
            columns = df.columns

            columns_sql = ", ".join([f'"{col}"' for col in columns])
            placeholders = ", ".join(["%s"] * len(columns))
            update_columns = ", ".join(
                [f'"{col}" = EXCLUDED."{col}"' for col in columns if col != key_column]
            )

            upsert_sql = f"""
                INSERT INTO {qualified_table} ({columns_sql})
                VALUES %s
                ON CONFLICT ("{key_column}")
                DO UPDATE SET {update_columns}
            """

            with self.conn.cursor() as cur:
                execute_values(cur, upsert_sql, data, template=f"({placeholders})")

            return len(df)
        except Exception as exc:
            print(f"Error upserting into '{qualified_table}': {exc}")
            raise

    def _adapt_value(self, value):
        """Adapt Python values for psycopg2, including JSON-like payloads."""
        if isinstance(value, (dict, list)):
            return Json(self._clean_json_value(value))
        return value

    def _clean_json_value(self, value):
        """Recursively drop null-like expansion artifacts before JSON upsert."""
        if isinstance(value, dict):
            cleaned = {}
            for key, item in value.items():
                if item is None:
                    continue
                cleaned[key] = self._clean_json_value(item)
            return cleaned

        if isinstance(value, list):
            cleaned = []
            for item in value:
                if item is None:
                    continue
                cleaned.append(self._clean_json_value(item))
            return cleaned

        return value

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()
        print("TimescaleDB connection closed")

    def __enter__(self) -> "TimescaleDBClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

if __name__ == "__main__":
    with TimescaleDBClient() as ts:
        tables = ts.list_tables()
        print(f"\nTables: {tables}")


def wait_for_timescaledb(max_retries: int = 30, retry_delay_seconds: int = 2) -> TimescaleDBClient:
    """Wait until TimescaleDB is reachable and return a ready client."""
    last_error = None
    for _ in range(max_retries):
        try:
            client = TimescaleDBClient()
            client.execute("SELECT 1")
            return client
        except Exception as exc:
            last_error = exc
            time.sleep(retry_delay_seconds)
    raise RuntimeError(f"TimescaleDB is not ready: {last_error}")
