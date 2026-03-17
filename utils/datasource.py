"""
Live datasource connectors — Snowflake, SQL Server / Azure SQL, Databricks.

Each connector provides:
  test_*_connection(...)  → raises HTTPException on failure
  read_*_table(...)       → returns pd.DataFrame
"""

import pandas as pd
from fastapi import HTTPException


# ─────────────────────────────────────────────────────────────
# Snowflake
# ─────────────────────────────────────────────────────────────

def test_snowflake_connection(
    account: str, user: str, password: str,
    warehouse: str, database: str,
) -> None:
    """Test Snowflake connectivity. Raises HTTPException on failure."""
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            account=account, user=user, password=password,
            warehouse=warehouse, database=database,
        )
        conn.close()
        print("✅ Snowflake connection OK")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Snowflake connection failed: {e}")


def read_snowflake_table(
    account: str, user: str, password: str,
    warehouse: str, database: str, schema: str, table: str,
) -> pd.DataFrame:
    """Read a full table from Snowflake."""
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            account=account, user=user, password=password,
            warehouse=warehouse, database=database, schema=schema,
        )
        df = pd.read_sql(f'SELECT * FROM "{schema}"."{table}"', conn)
        conn.close()
        print(f"   ✅ Snowflake '{table}': {len(df):,} rows")
        return df
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read Snowflake table '{table}': {e}",
        )


# ─────────────────────────────────────────────────────────────
# SQL Server / Azure SQL
# ─────────────────────────────────────────────────────────────

def _sqlserver_conn_string(server: str, database: str, username: str, password: str) -> str:
    if ".database.windows.net" in server:
        return (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server=tcp:{server},1433;Database={database};"
            f"UID={username};PWD={password};"
            f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
    return (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={server};Database={database};"
        f"UID={username};PWD={password};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )


def test_sqlserver_connection(
    server: str, database: str, username: str, password: str,
) -> None:
    """Test SQL Server / Azure SQL connectivity."""
    try:
        import pyodbc
        conn = pyodbc.connect(
            _sqlserver_conn_string(server, database, username, password),
            timeout=10,
        )
        conn.close()
        label = "Azure SQL" if ".database.windows.net" in server else "SQL Server"
        print(f"✅ {label} connection OK")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQL Server connection failed: {e}")


def read_sqlserver_table(
    server: str, database: str, username: str, password: str, table_name: str,
) -> pd.DataFrame:
    """Read a full table from SQL Server or Azure SQL."""
    try:
        import pyodbc
        conn = pyodbc.connect(
            _sqlserver_conn_string(server, database, username, password)
        )
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        # Sanitise nulls
        df = df.where(pd.notnull(df), None)
        conn.close()
        print(f"   ✅ SQL Server '{table_name}': {len(df):,} rows")
        return df
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read SQL Server table '{table_name}': {e}",
        )


# ─────────────────────────────────────────────────────────────
# Databricks
# ─────────────────────────────────────────────────────────────

def test_databricks_connection(
    server_hostname: str, http_path: str,
    access_token: str, catalog: str = "hive_metastore",
) -> None:
    """Test Databricks SQL connectivity."""
    try:
        from databricks import sql as dbsql
        conn   = dbsql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        print("✅ Databricks connection OK")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Databricks connection failed: {e}")


def read_databricks_table(
    server_hostname: str, http_path: str, access_token: str,
    catalog: str, schema: str, table: str,
) -> pd.DataFrame:
    """Read a full table from Databricks SQL."""
    try:
        from databricks import sql as dbsql

        conn = dbsql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        )

        # Build fully-qualified reference
        if catalog and schema:
            ref = f"`{catalog}`.`{schema}`.`{table}`"
        elif schema:
            ref = f"`{schema}`.`{table}`"
        else:
            ref = f"`{table}`"

        df = pd.read_sql(f"SELECT * FROM {ref}", conn)
        conn.close()
        print(f"   ✅ Databricks '{table}': {len(df):,} rows")
        return df
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read Databricks table '{table}': {e}",
        )
