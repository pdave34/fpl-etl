"""ETL loaders for reading and inserting data into various data sources.

This module provides classes for loading data from files into different
backends, including Oracle Database. It supports CSV, Excel, and JSON formats.
"""

import json
import logging
import time
import os
from pathlib import Path
from typing import Optional

import oracledb as odb
import pandas as pd
import pyarrow as pa

from etl import utils


class BaseLoader:
    """Base class for data loaders with common functionality.

    Provides methods for reading data from various file formats (CSV, Excel, JSON)
    and logging capabilities.
    """

    def __init__(self):
        """Initialize the BaseLoader with logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger(__name__)

    def read_data(self, source: Path) -> pd.DataFrame:
        """Read data from a file into a pandas DataFrame.

        Args:
            source: Path to the file. Supports CSV, Excel (.xlsx), and JSON formats.

        Returns:
            A pandas DataFrame containing the file data.

        Raises:
            ValueError: If the file format is not supported.
        """
        self.logger.info(f"Reading data from {source}")
        if source.suffix.lower() == ".csv":
            df = pd.read_csv(source)
        elif source.suffix.lower() == ".xlsx":
            df = pd.read_excel(source)
        elif source.suffix.lower() == ".json":
            data = json.load(open(source, "r"))
            df = pd.DataFrame(data)
        else:
            self.logger.error(f"Unsupported file format: {source.suffix}")
            raise ValueError(f"Unsupported file format: {source.suffix}")
        self.logger.info(f"Data read successfully with shape {df.shape}")
        return pa.table(df)


class OracleDBLoader(BaseLoader):
    """Loader for reading data from files and inserting into Oracle Database.

    Manages connections to Oracle Database using connection pooling and provides
    methods for creating tables, inserting data, and querying metadata.
    """

    def __init__(
        self,
        dsn: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        min_pool: int = 1,
        max_pool: int = 1,
        increment_pool: int = 0,
        timeout: int = 60,
    ):
        """Initialize OracleDBLoader with connection pool.

        Args:
            dsn: Oracle database service name. Defaults to env variable "ORACLE_DSN".
            user: Database username. Defaults to env variable "ORACLE_USR".
            password: Database password. Defaults to env variable "ORACLE_PWD".
            min_pool: Minimum pool size. Defaults to 1.
            max_pool: Maximum pool size. Defaults to 1.
            increment_pool: Pool increment size. Defaults to 0.
            timeout: Connection timeout in seconds. Defaults to 60.

        Raises:
            DatabaseError: If connection pool creation fails.
        """
        super().__init__()
        self.dsn = dsn or os.getenv("ORACLE_DSN")
        self.user = user or os.getenv("ORACLE_USR")
        self.password = password or os.getenv("ORACLE_PWD")
        self.pool_alias = utils.id_generator()
        odb.defaults.config_dir = (
            "/Users/astropd/Projects/oracle/oracle-19c/oradata/dbconfig/D7WOB1D1"
        )
        odb.create_pool(
            user=self.user,
            password=self.password,
            dsn=self.dsn,
            min=min_pool,
            max=max_pool,
            increment=increment_pool,
            pool_alias=self.pool_alias,
            timeout=timeout,
        )
        self.test_connection()

    def test_connection(self) -> bool:
        """Test the connection to the Oracle database.

        Returns:
            True if connection is healthy, False otherwise.
        """
        self.logger.info(f"Testing connection to Oracle DB at {self.dsn}")
        healthy = False
        try:
            with odb.connect(pool_alias=self.pool_alias) as conn:
                healthy = conn.is_healthy()
                self.logger.info(f"Connection healthy: {healthy}")
        except odb.DatabaseError as e:
            self.logger.error(f"Connection failed: {e}")
            healthy = False
        return healthy

    def insert_data(self, data: pa.Table, table_name: str) -> None:
        """Insert pyarrow Table data into an Oracle table using parameterized queries.

        Args:
            data: pyarrow Table containing the data to insert.
            table_name: Name of the target table.
        """
        self.logger.info(f"Inserting data into table {table_name}")
        timing = []
        with odb.connect(pool_alias=self.pool_alias) as conn:
            cols = ", ".join([f'"{c.upper()}"' for c in data.column_names])
            placeholders = ",".join([f":{i + 1}" for i in range(len(data.column_names))])
            sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
            cursor = conn.cursor()
            timing.append(time.perf_counter())
            cursor.executemany(sql, data)
            conn.commit()
            timing.append(time.perf_counter())
            self.logger.info(
                f"Inserted {cursor.rowcount} rows into {table_name} in \
                    {timing[-1] - timing[0]:.2f} seconds"
            )
            self.logger.info(f"Executed SQL: {sql}")

    def insert_full_data(self, table: pa.Table, table_name: str) -> None:
        """Insert DataFrame data using Oracle direct path load (high performance).

        Args:
            table: pyarrow Table containing the data to insert.
            table_name: Name of the target table.
        """
        self.logger.info(f"Inserting full data into table {table_name}")
        timing = []
        with odb.connect(pool_alias=self.pool_alias) as conn:
            timing.append(time.perf_counter())
            conn.direct_path_load(
                schema_name=self.user,
                table_name=table_name,
                column_names=table.column_names,
                data=table,
            )
            conn.commit()
            timing.append(time.perf_counter())
            self.logger.info(
                f"Full data inserted into {table_name} in {timing[-1] - timing[0]:.2f} seconds"
            )

    def truncate_table(self, table_name: str) -> None:
        """Truncate (remove all data from) an Oracle table.

        Args:
            table_name: Name of the table to truncate.

        Raises:
            DatabaseError: If truncation fails.
        """
        self.logger.info(f"Truncating table {table_name}")
        try:
            with odb.connect(pool_alias=self.pool_alias) as conn:
                cursor = conn.cursor()
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                conn.commit()
                self.logger.info(f"Table {table_name} truncated successfully")
        except odb.DatabaseError as e:
            self.logger.error(f"Truncation failed: {e}")
            raise

    def check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database.

        Args:
            table_name: Name of the table to check.

        Returns:
            True if the table exists, False otherwise.

        Raises:
            DatabaseError: If the check fails.
        """
        self.logger.info(f"Checking if table {table_name} exists")
        exists = False
        with odb.connect(pool_alias=self.pool_alias) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM all_tables WHERE table_name \
                    = :tbl_name AND owner = :owner",
                tbl_name=table_name.upper(),
                owner=self.user.upper(),
            )
            count = cursor.fetchone()[0]
            exists = count > 0
            self.logger.info(f"Table {table_name} exists: {exists}")
        return exists

    def execute_query(self, query: str) -> odb.DataFrame:
        """Execute a SQL query and return the results as a DataFrame.

        Args:
            query: The SQL query to execute.

        Returns:
            An Oracle DataFrame containing the query results.

        Raises:
            DatabaseError: If the query fails.
        """
        self.logger.info(f"Executing query: {query}")
        with odb.connect(pool_alias=self.pool_alias) as conn:
            df: odb.DataFrame = conn.fetch_df_all(query)
            # self.logger.info(f"Query executed successfully with shape {df.shape}")
            self.logger.info(f"Query returned {df.num_rows()} rows")
            return df

    def count_rows(self, table_name: str) -> int:
        """Get the number of rows in a table.

        Args:
            table_name: Name of the table.

        Returns:
            The number of rows in the table.

        Raises:
            DatabaseError: If the query fails.
        """
        self.logger.info(f"Getting row count for table {table_name}")
        row_count = 0
        try:
            with odb.connect(pool_alias=self.pool_alias) as conn:
                cursor: odb.Cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count: int = cursor.fetchone()[0]
                self.logger.info(f"Table {table_name} has {row_count} rows")
        except odb.DatabaseError as e:
            self.logger.error(f"Row count retrieval failed: {e}")
            raise
        return row_count

    def drop_table(self, table_name: str) -> None:
        """Drop (delete) a table from the database.

        Args:
            table_name: Name of the table to drop.

        Raises:
            DatabaseError: If the drop operation fails.
        """
        self.logger.info(f"Dropping table {table_name}")
        try:
            with odb.connect(pool_alias=self.pool_alias) as conn:
                cursor: odb.Cursor = conn.cursor()
                cursor.execute(f"DROP TABLE {table_name} PURGE")
                conn.commit()
                self.logger.info(f"Table {table_name} dropped successfully")
        except odb.DatabaseError as e:
            self.logger.error(f"Dropping table failed: {e}")
            raise

    def create_table(self, table_name: str, table: pa.Table) -> None:
        """Create a new table in the database based on DataFrame structure.

        Args:
            table_name: Name of the table to create.
            df: DataFrame whose structure defines the table columns and types.

        Raises:
            DatabaseError: If table creation fails.
        """
        ddl: str = self.generate_oracle_ddl(table, table_name)
        self.logger.info(f"Generated DDL: {ddl}")
        try:
            with odb.connect(pool_alias=self.pool_alias) as conn:
                cursor: odb.Cursor = conn.cursor()
                cursor.execute(ddl)
                conn.commit()
                self.logger.info(f"Table {table_name} created successfully")
        except odb.DatabaseError as e:
            self.logger.error(f"Creating table failed: {e}")
            raise

    def generate_oracle_ddl(self, table: pa.Table, table_name: str) -> str:
        """Generate Oracle DDL CREATE TABLE statement from a pyarrow Table.

        Args:
            table: pyarrow Table whose schema defines the table columns.
            table_name: Name of the table to create.

        Returns:
            A SQL CREATE TABLE statement string.
        """
        column_defs = []

        def _field_to_oracle_type(field: pa.Field, column=None):
            t: pa.DataType = field.type
            # Decimal -> NUMBER(precision,scale)
            if pa.types.is_decimal(t):
                try:
                    return f"NUMBER({t.precision},{t.scale})"
                except Exception:
                    return "NUMBER"

            # Integers -> NUMBER
            if pa.types.is_integer(t):
                return "NUMBER"

            # Floats -> BINARY_DOUBLE / BINARY_FLOAT
            if pa.types.is_float64(t):
                return "BINARY_DOUBLE"
            if pa.types.is_float32(t):
                return "BINARY_FLOAT"

            # Booleans -> NUMBER(1)
            if pa.types.is_boolean(t):
                return "NUMBER(1)"

            # Binary types -> BLOB
            if pa.types.is_large_binary(t) or pa.types.is_binary(t):
                return "BLOB"

            # Timestamp -> TIMESTAMP (with TZ if present)
            if pa.types.is_timestamp(t):
                tz = getattr(t, "tz", None)
                if tz:
                    return "TIMESTAMP WITH TIME ZONE"
                return "TIMESTAMP"

            # Strings -> VARCHAR2 with a safe length or CLOB for large values
            if pa.types.is_large_string(t):
                return "CLOB"
            if pa.types.is_string(t):
                # try to infer max length from the column values if provided
                max_len = 0
                if column is not None:
                    try:
                        max_len: int = max(
                            (len(x) for x in column.to_pylist() if x is not None), default=0
                        )
                    except Exception:
                        max_len = 0
                # conservative byte multiplier for multibyte characters
                length = max(255, int(max_len * 3))
                if length <= 4000:
                    return f"VARCHAR2({min(length, 4000)})"
                return "CLOB"

            # Lists or structs representing vectors -> BLOB (DB_TYPE_VECTOR)
            if pa.types.is_list(t) or pa.types.is_struct(t):
                return "BLOB"

            # Fallback to CLOB for other types
            return "CLOB"

        # Map each field in the schema to an Oracle type
        for i, field in enumerate(table.schema):
            col_name = field.name
            # provide the column chunk for possible length inference
            column = None
            try:
                column = table.column(i)
            except Exception:
                column = None

            oracle_type = _field_to_oracle_type(field, column)
            column_defs.append(f'    "{col_name.upper()}" {oracle_type}')

        ddl = f'CREATE TABLE "{table_name.upper()}" (\n' + ",\n".join(column_defs) + "\n)"
        return ddl

    def rebuild_table(self, table_name: str, data: pa.Table):
        if self.check_table_exists(table_name):
            self.drop_table(table_name)
        self.create_table(table_name, data)
        self.insert_data(data, table_name=table_name)

    def reload_table(self, table_name: str, data: pa.Table):
        if self.check_table_exists(table_name):
            self.truncate_table(table_name)
        self.insert_data(data, table_name=table_name)

    def upsert_data(self, data: pa.Table, table_name: str, key_columns: list[str]) -> None:
        """Upsert pyarrow Table data into an Oracle table using MERGE statement.

        Args:
            data: pyarrow Table containing the data to upsert.
            table_name: Name of the target table.
            key_columns: List of column names that form the primary key for upsert.
        """
        self.logger.info(f"Upserting data into table {table_name}")
        timing = []
        with odb.connect(pool_alias=self.pool_alias) as conn:
            cols = [f'"{c.upper()}"' for c in data.column_names]
            insert_cols = ", ".join(cols)
            insert_values = ",".join([f":{i + 1}" for i in range(len(data.column_names))])
            update_set = ", ".join(
                [
                    f"{col} = :{i + 1}"
                    for i, col in enumerate(cols)
                    if col.strip('"') not in key_columns
                ]
            )
            on_conditions = " AND ".join(
                [f'target."{col.upper()}" = source."{col.upper()}"' for col in key_columns]
            )
            sql = f"""
                MERGE INTO {table_name} target
                USING (SELECT {insert_values} FROM dual) source ({insert_cols})
                ON ({on_conditions})
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols}) VALUES ({insert_values})
            """
            cursor = conn.cursor()
            timing.append(time.perf_counter())
            cursor.executemany(sql, data)
            conn.commit()
            timing.append(time.perf_counter())
            self.logger.info(
                f"Upserted {cursor.rowcount} rows into {table_name} in \
                    {timing[-1] - timing[0]:.2f} seconds"
            )
            self.logger.info(f"Executed SQL: {sql}")

    def close(self):
        """Close the connection pool and clean up resources.

        Raises:
            DatabaseError: If closing the pool fails.
        """
        self.logger.info("Closing OracleDBLoader and its connection pool")
        try:
            pool = odb.get_pool(self.pool_alias)
            if isinstance(pool, odb.ConnectionPool):
                pool.close()
            self.logger.info("Connection pool closed successfully")
        except odb.DatabaseError as e:
            self.logger.error(f"Failed to close connection pool: {e}")
