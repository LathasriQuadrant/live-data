"""
utils/refresh_engine.py
───────────────────────
Migration-agnostic refresh engine.

Given any registry record, this module knows how to:
  1. Re-read data from the original source (extracted .hyper OR live DB)
  2. Overwrite the Fabric Lakehouse Delta tables

This is intentionally decoupled from routes so the scheduler, a manual
API endpoint, and future migration types all share the same code path.
"""

import time
from datetime import datetime, timezone

import utils.registry as registry
from utils.auth          import get_fabric_token
from utils.fabric        import write_table_to_lakehouse
from utils.type_mapper   import coerce_dataframe_to_tableau_types, build_delta_schema



# ─────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────

def refresh_migration(workbook_name: str) -> dict:
    """
    Full refresh cycle for one registered migration:
      1. Load record from registry & decrypt credentials
      2. Re-read all tables from the original source
      3. Overwrite Delta tables in Fabric Lakehouse
      4. Update registry with outcome

    Returns a result dict:
    {
        "workbook_name": str,
        "status":        "success" | "partial" | "failed",
        "tables_refreshed": [...],
        "tables_failed":    [...],
        "duration_seconds": float,
        "refreshed_at":     ISO str,
    }
    """
    started_at = time.time()
    print(f"\n{'='*60}")
    print(f"🔄  Refresh: {workbook_name}  [{datetime.now(timezone.utc).isoformat()}]")
    print(f"{'='*60}\n")

    record = registry.get(workbook_name)
    if not record:
        raise ValueError(f"No registry record found for '{workbook_name}'")

    migration_type = record.get("migration_type", "tableau")
    data_mode      = record["data_mode"]
    source_type    = record["source_type"]

    print(f"📋 Migration type : {migration_type}")
    print(f"📋 Data mode      : {data_mode}")
    print(f"📋 Source         : {source_type}")
    print(f"📋 Tables         : {record['tables']}\n")

    # Extracted workbooks have no live source — skip refresh entirely
    if data_mode == "extracted":
        print(f"⏭️  '{workbook_name}' is an extracted workbook — skipping refresh (no live source).")
        return {
            "status":                  "skipped",
            "workbook_name":           workbook_name,
            "file_name":               record.get("file_name"),
            "data_mode":               data_mode,
            "lakehouse_name":          record.get("lakehouse_name"),
            "lakehouse_id":            record.get("lakehouse_id"),
            "workspace_id":            record.get("workspace_id"),
            "sql_endpoint_connection": None,
            "tables_migrated":         [],
            "tables_failed":           [],
            "summary": {
                "total_migrated": 0,
                "total_failed":   0,
                "total_rows":     0,
            },
            "duration_seconds":        round(time.time() - started_at, 2),
            "refreshed_at":            datetime.now(timezone.utc).isoformat(),
            "message":                 f"⏭️  '{workbook_name}' skipped — extracted workbooks have no live source to refresh.",
        }

    # Decrypt password (empty string for extracted workbooks)
    password = registry.get_password(workbook_name)

    refreshed = []
    failed    = []

    try:
        if migration_type == "tableau":
            refreshed, failed = _refresh_tableau(record, password)
        else:
            # ── Future migration types plug in here ──────────────────
            # elif migration_type == "powerbi":
            #     refreshed, failed = _refresh_powerbi(record, password)
            raise ValueError(f"Unknown migration_type '{migration_type}'")
    except Exception as e:
        err_msg = str(e)
        print(f"❌ Refresh aborted: {err_msg}")
        registry.update_refresh_status(workbook_name, "failed", err_msg)
        return {
            "status":                  "failed",
            "workbook_name":           workbook_name,
            "file_name":               record.get("file_name"),
            "data_mode":               record.get("data_mode"),
            "lakehouse_name":          record.get("lakehouse_name"),
            "lakehouse_id":            record.get("lakehouse_id"),
            "workspace_id":            record.get("workspace_id"),
            "sql_endpoint_connection": None,
            "tables_migrated":         [],
            "tables_failed":           [{"table": "ALL", "error": err_msg}],
            "summary": {
                "total_migrated": 0,
                "total_failed":   1,
                "total_rows":     0,
            },
            "duration_seconds":        round(time.time() - started_at, 2),
            "refreshed_at":            datetime.now(timezone.utc).isoformat(),
            "message": f"❌ Refresh failed for '{workbook_name}': {err_msg}",
        }

    # ── Update registry ───────────────────────────────────────
    overall = "success" if not failed else ("partial" if refreshed else "failed")
    error_summary = "; ".join(f"{f['table']}: {f['error']}" for f in failed) if failed else None
    registry.update_refresh_status(workbook_name, overall, error_summary)

    duration = round(time.time() - started_at, 2)
    print(f"\n{'='*60}")
    print(f"✅ Refresh done — {len(refreshed)} ok, {len(failed)} failed, {duration}s")
    print(f"{'='*60}\n")

    # Fetch SQL endpoint so the response mirrors /migrate exactly
    sql_endpoint = None
    try:
        from utils.fabric import get_lakehouse_sql_endpoint
        token        = get_fabric_token()
        sql_endpoint = get_lakehouse_sql_endpoint(
            token, record["workspace_id"], record["lakehouse_id"]
        )
    except Exception as e:
        print(f"⚠️  Could not fetch SQL endpoint: {e}")

    total_rows = sum(t.get("rows", 0) for t in refreshed)

    return {
        "status":                  overall,
        "workbook_name":           workbook_name,
        "file_name":               record.get("file_name"),
        "data_mode":               record.get("data_mode"),
        "lakehouse_name":          record.get("lakehouse_name"),
        "lakehouse_id":            record.get("lakehouse_id"),
        "workspace_id":            record.get("workspace_id"),
        "sql_endpoint_connection": sql_endpoint,
        "tables_migrated":         refreshed,
        "tables_failed":           failed,
        "summary": {
            "total_migrated": len(refreshed),
            "total_failed":   len(failed),
            "total_rows":     total_rows,
        },
        "duration_seconds":        duration,
        "refreshed_at":            datetime.now(timezone.utc).isoformat(),
        "message": (
            f"✅ '{workbook_name}' refreshed — {len(refreshed)} table(s)."
            + (f"  ⚠️ {len(failed)} failed." if failed else "")
        ),
    }


# ─────────────────────────────────────────────────────────────
# Tableau-specific refresh
# ─────────────────────────────────────────────────────────────

def _refresh_tableau(record: dict, password: str) -> tuple[list, list]:
    """Re-read all tables for a Tableau migration and overwrite in Lakehouse."""
    data_mode   = record["data_mode"]
    source_type = record["source_type"]
    tables      = record["tables"]

    if data_mode == "extracted":
        return _refresh_tableau_extracted(record)
    else:
        return _refresh_tableau_live(record, password, source_type, tables)


def _refresh_tableau_extracted(record: dict) -> tuple[list, list]:
    """
    Re-download the .twbx from blob and re-read .hyper tables.
    This handles the case where someone uploads a new .twbx with updated data.
    """
    from utils.blob_storage import download_twbx_from_blob
    from utils.hyper_reader import read_hyper_tables
    from utils.file_handler import extract_twb_from_twbx

    file_name    = record["file_name"]
    container    = record["container_name"]
    workspace_id = record["workspace_id"]
    lakehouse_id = record["lakehouse_id"]
    lakehouse_nm = record["lakehouse_name"]
    tables       = record["tables"]

    print(f"📦 Downloading '{file_name}' from blob container '{container}'...")
    local_path   = download_twbx_from_blob(file_name, container)
    hyper_result = read_hyper_tables(local_path)
    hyper_tables = [t for t in hyper_result["tables"] if t["name"] in tables]

    # Extract column types from TWB XML
    col_types = _extract_col_types_from_twbx(local_path)

    fabric_token = get_fabric_token()
    return _write_tables_to_lakehouse(
        fabric_token, workspace_id, lakehouse_id, lakehouse_nm,
        [(t["name"], t["df"]) for t in hyper_tables],
        col_types,
    )


def _refresh_tableau_live(
    record: dict, password: str, source_type: str, tables: list[str]
) -> tuple[list, list]:
    """
    Re-read tables from live source (Snowflake / SQL / Databricks).

    Connection details are read directly from the registry record —
    the .twbx is never downloaded or parsed during refresh.

    Also performs schema sync:
      - New tables found at source are added to the Lakehouse and registry.
      - Tables that no longer exist at source are removed from the Lakehouse
        and dropped from the registry.
      - Column additions / removals are handled automatically because
        write_deltalake uses schema_mode="overwrite".
    """
    workspace_id = record["workspace_id"]
    lakehouse_id = record["lakehouse_id"]
    lakehouse_nm = record["lakehouse_name"]
    datasource   = record.get("connection_details", {})

    if not datasource:
        raise ValueError(
            f"No connection_details stored for '{record['workbook_name']}'. "
            "Re-run /migrate to populate the registry with connection info."
        )

    # ── Schema sync: discover current tables at source ────────
    print(f"🔍 Discovering current tables at {source_type.upper()}...")
    try:
        source_tables = _list_source_tables(datasource, source_type, password)
        print(f"   Source tables : {source_tables}")
        print(f"   Registered    : {tables}")
    except Exception as e:
        # If discovery fails, fall back to the registered list so existing
        # data is still refreshed — just log the warning.
        print(f"   ⚠️  Could not discover source tables ({e}) — using registered list")
        source_tables = None

    if source_tables is not None:
        registered_set = set(tables)
        source_set     = set(source_tables)

        new_tables     = source_set - registered_set
        removed_tables = registered_set - source_set

        if new_tables:
            print(f"   ➕ New tables detected: {sorted(new_tables)}")
        if removed_tables:
            print(f"   ➖ Removed tables detected: {sorted(removed_tables)}")

        # Update tables list to reflect current source state
        tables = [t for t in source_tables]

        # Drop stale tables from Lakehouse
        if removed_tables:
            _drop_lakehouse_tables(workspace_id, lakehouse_id, removed_tables)

        # Update registry with new table list
        record["tables"] = tables
        import utils.registry as _reg
        _reg._cache[record["workbook_name"]] = record
        _reg._save()
    else:
        # Keep registered table list unchanged
        pass

    # ── Read all current tables from source ───────────────────
    print(f"🔗 Reading {len(tables)} table(s) from {source_type.upper()}...")
    table_dfs   = []
    read_errors = []
    for table_name in tables:
        try:
            df = _read_live_table(datasource, source_type, password, table_name)
            table_dfs.append((table_name, df))
            print(f"   ✅ Read '{table_name}': {len(df):,} rows")
        except Exception as e:
            print(f"   ❌ Read '{table_name}': {e}")
            read_errors.append({"table": table_name, "error": str(e)})

    # col_types not needed from TWB — schema_mode=overwrite handles drift
    fabric_token = get_fabric_token()
    written, write_errors = _write_tables_to_lakehouse(
        fabric_token, workspace_id, lakehouse_id, lakehouse_nm,
        table_dfs, col_types={},
    )

    return written, read_errors + write_errors


# ─────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────

def _write_tables_to_lakehouse(
    token, workspace_id, lakehouse_id, lakehouse_name,
    table_dfs: list[tuple[str, object]],   # [(table_name, df), ...]
    col_types: dict,
) -> tuple[list, list]:
    """Write a list of (table_name, df) pairs to Lakehouse. Returns (ok, errors)."""
    ok     = []
    errors = []

    for i, (table_name, df) in enumerate(table_dfs):
        try:
            df_coerced   = coerce_dataframe_to_tableau_types(df, col_types)
            spark_schema = build_delta_schema(df_coerced, col_types)
            result       = write_table_to_lakehouse(
                token=token, workspace_id=workspace_id,
                lakehouse_id=lakehouse_id, lakehouse_name=lakehouse_name,
                table_name=table_name,
                df=df_coerced, spark_schema=spark_schema, mode="overwrite",
            )
            ok.append(result)
            print(f"   ✅ '{table_name}': {result['rows']:,} rows written")
            if i < len(table_dfs) - 1:
                time.sleep(5)   # avoid back-to-back Spark capacity spikes
        except Exception as e:
            print(f"   ❌ '{table_name}': {e}")
            errors.append({"table": table_name, "error": str(e)})

    return ok, errors


def _extract_col_types_from_twbx(local_path: str) -> dict:
    """Parse column datatype metadata from a .twbx's embedded .twb XML."""
    import xml.etree.ElementTree as ET
    from utils.file_handler import extract_twb_from_twbx

    col_types = {}
    try:
        twb_path = extract_twb_from_twbx(local_path)
        tree     = ET.parse(twb_path)
        root     = tree.getroot()
        for col in root.findall(".//column"):
            name     = col.get("name")
            datatype = col.get("datatype")
            if not name or not datatype:
                continue
            if name.startswith("[__tableau") or datatype == "table":
                continue
            col_types[name.strip("[]")] = datatype.lower()
        print(f"   📋 Extracted {len(col_types)} column type(s) from workbook XML")
    except Exception as e:
        print(f"   ⚠️  Could not extract column types from TWB: {e}")
    return col_types


def _read_live_table(datasource: dict, source_type: str, password: str, table: str):
    """Dispatch table read to the correct source connector."""
    from utils.datasource import (
        read_snowflake_table, read_sqlserver_table, read_databricks_table,
    )
    from fastapi import HTTPException

    if source_type == "snowflake":
        return read_snowflake_table(
            account=datasource["account"], user=datasource["username"],
            password=password, warehouse=datasource["warehouse"],
            database=datasource["database"], schema=datasource.get("schema", ""),
            table=table,
        )
    elif source_type in ("sqlserver", "azuresql"):
        return read_sqlserver_table(
            server=datasource["server"], database=datasource["database"],
            username=datasource["username"], password=password,
            table_name=table,
        )
    elif source_type == "databricks":
        return read_databricks_table(
            server_hostname=datasource["server_hostname"],
            http_path=datasource["http_path"],
            access_token=password,
            catalog=datasource.get("catalog", "hive_metastore"),
            schema=datasource.get("schema", "default"),
            table=table,
        )
    else:
        raise HTTPException(400, f"Unsupported source type: '{source_type}'")

# ─────────────────────────────────────────────────────────────
# Schema sync helpers
# ─────────────────────────────────────────────────────────────

def _list_source_tables(datasource: dict, source_type: str, password: str) -> list[str]:
    """
    Return the current list of table names from the live source.
    Used by schema sync to detect additions and deletions since migration.
    """
    if source_type == "snowflake":
        import snowflake.connector
        conn = snowflake.connector.connect(
            account=datasource["account"],
            user=datasource["username"],
            password=password,
            warehouse=datasource["warehouse"],
            database=datasource["database"],
            schema=datasource.get("schema", ""),
        )
        cursor = conn.cursor()
        schema = datasource.get("schema", "PUBLIC")
        cursor.execute(f"SHOW TABLES IN SCHEMA \"{datasource['database']}\".\"{schema}\"")
        tables = [row[1] for row in cursor.fetchall()]   # column 1 is table name
        cursor.close()
        conn.close()
        return tables

    elif source_type in ("sqlserver", "azuresql"):
        import pyodbc
        from utils.datasource import _sqlserver_conn_string
        conn = pyodbc.connect(
            _sqlserver_conn_string(
                datasource["server"], datasource["database"],
                datasource["username"], password,
            )
        )
        cursor = conn.cursor()
        cursor.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE'"
        )
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return tables

    elif source_type == "databricks":
        from databricks import sql as dbsql
        conn = dbsql.connect(
            server_hostname=datasource["server_hostname"],
            http_path=datasource["http_path"],
            access_token=password,
        )
        cursor = conn.cursor()
        catalog = datasource.get("catalog", "hive_metastore")
        schema  = datasource.get("schema", "default")
        cursor.execute(f"SHOW TABLES IN `{catalog}`.`{schema}`")
        tables = [row[1] for row in cursor.fetchall()]   # column 1 is tableName
        cursor.close()
        conn.close()
        return tables

    else:
        raise ValueError(f"Cannot list tables for unsupported source type: '{source_type}'")


def _drop_lakehouse_tables(workspace_id: str, lakehouse_id: str, table_names: set) -> None:
    """
    Delete Delta tables from OneLake that no longer exist at the source.
    Uses the OneLake DFS REST API to delete the Tables/<table_name> directory.
    """
    from utils.auth import get_fabric_token
    from config import TENANT_ID, CLIENT_ID, CLIENT_SECRET
    import requests

    try:
        # Need Azure Storage scope to delete from OneLake DFS
        resp = requests.post(
            f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
            data={
                "grant_type":    "client_credentials",
                "client_id":     CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "scope":         "https://storage.azure.com/.default",
            },
            timeout=30,
        )
        resp.raise_for_status()
        onelake_token = resp.json()["access_token"]
    except Exception as e:
        print(f"   ⚠️  Could not get OneLake token to drop tables: {e}")
        return

    for table_name in sorted(table_names):
        url = (
            f"https://onelake.dfs.fabric.microsoft.com/"
            f"{workspace_id}/{lakehouse_id}/Tables/{table_name}"
            f"?resource=directory&recursive=true"
        )
        try:
            r = requests.delete(
                url,
                headers={"Authorization": f"Bearer {onelake_token}"},
                timeout=30,
            )
            if r.status_code in (200, 202, 404):
                print(f"   🗑️  Dropped stale table '{table_name}' from Lakehouse")
            else:
                print(f"   ⚠️  Could not drop '{table_name}': HTTP {r.status_code} — {r.text[:200]}")
        except Exception as e:
            print(f"   ⚠️  Error dropping '{table_name}': {e}")