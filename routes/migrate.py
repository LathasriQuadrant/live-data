"""
POST /api/v1/lakehouse/migrate

COMPLETE FIX for extracted workbook handling.

Downloads a Tableau .twbx from Azure Blob Storage, determines whether
it uses extracted (.hyper) or live data, creates a named Fabric Lakehouse,
and writes every table as a Delta table preserving Tableau metadata exactly.

Payload:
{
    "file_name":      "Sales_Dashboard.twbx",
    "workspace_id":   "fabric-workspace-guid",
    "container_name": "tabluea-raw",
    "password":       "source-db-password",
    "tables":         ["Orders", "Customers"]
}
"""

import time
from fastapi import APIRouter, HTTPException, Body
from datetime import datetime, timezone

from utils.blob_storage import download_twbx_from_blob
from utils.file_handler import (
    get_workbook_name,
    extract_twb_from_twbx,
)
from utils.hyper_reader import twbx_has_extract, read_hyper_tables
from utils.type_mapper  import coerce_dataframe_to_tableau_types, build_delta_schema
from utils.auth         import get_fabric_token
from utils.fabric       import create_lakehouse, write_table_to_lakehouse
from utils.datasource   import (
    test_snowflake_connection,
    test_sqlserver_connection,
    test_databricks_connection,
    read_snowflake_table,
    read_sqlserver_table,
    read_databricks_table,
)

router = APIRouter(prefix="/api/v1/lakehouse", tags=["Lakehouse"])


@router.post("/migrate")
def migrate(payload: dict = Body(...)):
    """
    Migrate a Tableau workbook from Azure Blob Storage to a Fabric Lakehouse.
    Auto-detects Extracted (.hyper) vs Live (Snowflake / Azure SQL / Databricks).
    
    COMPLETE FIX: Properly handles extracted workbooks without requiring live connections.
    """
    file_name               = payload.get("file_name")
    workspace_id            = payload.get("workspace_id")
    password                = payload.get("password", "")
    container               = payload.get("container_name", "tabluea-raw")
    tables_filter           = payload.get("tables", [])
    enable_refresh          = payload.get("enable_scheduled_refresh", False)
    refresh_interval        = payload.get("refresh_interval_minutes")

    # Validate interval only if scheduled refresh is enabled
    if enable_refresh:
        if refresh_interval is None:
            raise HTTPException(400, "'refresh_interval_minutes' is required when 'enable_scheduled_refresh' is true.")
        if not isinstance(refresh_interval, int) or refresh_interval < 5:
            raise HTTPException(400, "'refresh_interval_minutes' must be an integer ≥ 5.")

    if not file_name or not workspace_id:
        raise HTTPException(400, "'file_name' and 'workspace_id' are required.")

    print(f"\n{'='*60}")
    print(f"🚀  Lakehouse Migration: {file_name}")
    print(f"{'='*60}\n")

    # ── 1. Download workbook from blob ────────────────────────
    local_path    = download_twbx_from_blob(file_name, container)
    workbook_name = get_workbook_name(local_path)
    print(f"📋 Workbook name: {workbook_name}\n")

    # ── 2. Detect data mode ───────────────────────────────────
    is_extracted = twbx_has_extract(local_path)
    data_mode    = "extracted" if is_extracted else "live"
    print(f"🔍 Data mode: {data_mode.upper()}\n")

    # ── 3. Create Fabric Lakehouse ────────────────────────────
    fabric_token = get_fabric_token()
    lh_info      = create_lakehouse(fabric_token, workspace_id, workbook_name)
    lakehouse_id   = lh_info["id"]
    lakehouse_name = lh_info["displayName"]
    print(f"🏗️  Lakehouse: {lakehouse_name}  (id={lakehouse_id})\n")

    migrated   = []
    failed     = []
    datasource = {}   # populated in live branch; harmless for extracted

    # ─────────────────────────────────────────────────────────
    # EXTRACTED — read .hyper, write each table as Delta
    # ─────────────────────────────────────────────────────────
    if is_extracted:
        print("📦 Reading embedded .hyper extract...\n")
        hyper_result = read_hyper_tables(local_path)
        hyper_tables = hyper_result["tables"]

        # Extract column types from TWB XML (safe - doesn't check for live connections)
        col_types = {}
        try:
            twb_path = extract_twb_from_twbx(local_path)
            import xml.etree.ElementTree as ET
            tree = ET.parse(twb_path)
            root = tree.getroot()
            
            # Parse column types directly from XML
            for col in root.findall(".//column"):
                name     = col.get("name")
                datatype = col.get("datatype")
                if not name or not datatype:
                    continue
                if name.startswith("[__tableau") or datatype == "table":
                    continue
                clean = name.strip("[]")
                col_types[clean] = datatype.lower()
            
            print(f"   📋 Extracted {len(col_types)} column type(s) from workbook XML\n")
        except Exception as e:
            print(f"   ⚠️  Could not extract column types from TWB: {e}\n")
            col_types = {}

        if tables_filter:
            hyper_tables = [t for t in hyper_tables if t["name"] in tables_filter]
            print(f"   🎯 Filtered to {len(hyper_tables)} table(s) based on request\n")

        if not hyper_tables:
            raise HTTPException(
                400,
                f"No tables found in extract. Available tables: {[t['name'] for t in hyper_result['tables']]}, Requested: {tables_filter}",
            )

        for tbl in hyper_tables:
            table_name = tbl["name"]
            try:
                print(f"📊 Processing '{table_name}' from .hyper extract...")
                df           = coerce_dataframe_to_tableau_types(tbl["df"], col_types)
                spark_schema = build_delta_schema(df, col_types)
                result       = write_table_to_lakehouse(
                    token=fabric_token, workspace_id=workspace_id,
                    lakehouse_id=lakehouse_id, lakehouse_name=lakehouse_name,
                    table_name=table_name,
                    df=df, spark_schema=spark_schema, mode="overwrite",
                )
                migrated.append(result)
                print(f"   ✅ '{table_name}': {result['rows']:,} rows written\n")
                
                # Brief pause so capacity isn't hit by back-to-back Spark loads
                if hyper_tables.index(tbl) < len(hyper_tables) - 1:
                    time.sleep(5)
            except Exception as e:
                print(f"   ❌ '{table_name}': {e}\n")
                failed.append({"table": table_name, "error": str(e)})

    # ─────────────────────────────────────────────────────────
    # LIVE — connect to source, read tables, write as Delta
    # ─────────────────────────────────────────────────────────
    else:
        # Import the live connection parser only when needed
        from utils.file_handler import extract_datasource_from_twb
        
        twb_path   = extract_twb_from_twbx(local_path)
        
        try:
            datasource = extract_datasource_from_twb(twb_path)
        except HTTPException as e:
            raise HTTPException(
                status_code=e.status_code,
                detail=f"Cannot migrate workbook: {e.detail}"
            )
        
        ds_type    = datasource.get("type")
        col_types  = datasource.get("column_types", {})
        wb_tables  = datasource.get("tables", [])

        print(f"🔗 Live source: {ds_type.upper()}")
        print(f"   Tables in workbook: {wb_tables}\n")

        to_migrate = (
            [t for t in wb_tables if t in tables_filter]
            if tables_filter else wb_tables
        )
        if not to_migrate:
            raise HTTPException(
                400,
                f"No tables to migrate. Workbook tables: {wb_tables}, Requested: {tables_filter}",
            )

        _test_connection(datasource, ds_type, password)

        for table_name in to_migrate:
            try:
                print(f"📊 Reading '{table_name}' from {ds_type.upper()}...")
                df           = _read_table(datasource, ds_type, password, table_name)
                df           = coerce_dataframe_to_tableau_types(df, col_types)
                spark_schema = build_delta_schema(df, col_types)
                result       = write_table_to_lakehouse(
                    token=fabric_token, workspace_id=workspace_id,
                    lakehouse_id=lakehouse_id, lakehouse_name=lakehouse_name,
                    table_name=table_name,
                    df=df, spark_schema=spark_schema, mode="overwrite",
                )
                migrated.append(result)
                print(f"   ✅ '{table_name}': {result['rows']:,} rows written\n")
                
                # Brief pause so capacity isn't hit by back-to-back Spark loads
                if to_migrate.index(table_name) < len(to_migrate) - 1:
                    time.sleep(5)
            except HTTPException:
                raise
            except Exception as e:
                print(f"   ❌ '{table_name}': {e}\n")
                failed.append({"table": table_name, "error": str(e)})

    # ── 4. Persist migration record & register scheduler job ──
    import utils.registry as registry
    import utils.scheduler as scheduler

    source_type = "hyper" if is_extracted else datasource.get("type", "unknown")

    # Strip keys that are not connection params before persisting
    _EXCLUDE = {"type", "tables", "column_types", "relationships"}
    connection_details = (
        {}
        if is_extracted
        else {k: v for k, v in datasource.items() if k not in _EXCLUDE}
    )

    record = registry.register(
        workbook_name              = workbook_name,
        file_name                  = file_name,
        container_name             = container,
        workspace_id               = workspace_id,
        lakehouse_id               = lakehouse_id,
        lakehouse_name             = lakehouse_name,
        data_mode                  = data_mode,
        source_type                = source_type,
        tables                     = [t["table_name"] for t in migrated],
        password                   = password,
        powerbi_dataset_id         = payload.get("powerbi_dataset_id"),
        connection_details         = connection_details,
        enable_scheduled_refresh   = enable_refresh,
        refresh_interval_minutes   = refresh_interval,
        migration_type             = "tableau",
    )

    if enable_refresh:
        scheduler.add_or_replace_job(record)
        print(f"   ⏰ Scheduled refresh every {refresh_interval} min")
    else:
        print(f"   ⏭️  Scheduled refresh disabled for '{workbook_name}'")

    # ── Fetch SQL endpoint now that tables are written ───────
    # The endpoint is not available immediately after Lakehouse creation
    # but becomes ready once data has been written.
    from utils.fabric import get_lakehouse_sql_endpoint
    sql_endpoint_connection = None
    try:
        fabric_token            = get_fabric_token()
        sql_endpoint_connection = get_lakehouse_sql_endpoint(fabric_token, workspace_id, lakehouse_id)
    except Exception as e:
        print(f"⚠️  Could not fetch SQL endpoint after migration: {e}")

    print(f"\n{'='*60}")
    print(f"✅ Migration complete — {len(migrated)} migrated, {len(failed)} failed")
    print(f"{'='*60}\n")

    return {
        "status":                    "success" if not failed else "partial",
        "workbook_name":             workbook_name,
        "file_name":                 file_name,
        "data_mode":                 data_mode,
        "lakehouse_name":            lakehouse_name,
        "lakehouse_id":              lakehouse_id,
        "workspace_id":              workspace_id,
        "sql_endpoint_connection":   sql_endpoint_connection,
        "enable_scheduled_refresh":  enable_refresh,
        "refresh_interval_minutes":  refresh_interval,
        "tables_migrated":           migrated,
        "tables_failed":             failed,
        "summary": {
            "total_migrated": len(migrated),
            "total_failed":   len(failed),
            "total_rows":     sum(t["rows"] for t in migrated),
        },
        "message": (
            f"✅ '{workbook_name}' migrated to Fabric Lakehouse with "
            f"{len(migrated)} table(s)."
            + (f"  ⚠️ {len(failed)} failed." if failed else "")
            + (f"  ⏰ Scheduled every {refresh_interval} min." if enable_refresh else "  ⏭️  No scheduled refresh.")
        ),
    }


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _test_connection(datasource, ds_type, password):
    if ds_type == "snowflake":
        test_snowflake_connection(
            account=datasource["account"], user=datasource["username"],
            password=password, warehouse=datasource["warehouse"],
            database=datasource["database"],
        )
    elif ds_type in ("sqlserver", "azuresql"):
        test_sqlserver_connection(
            server=datasource["server"], database=datasource["database"],
            username=datasource["username"], password=password,
        )
    elif ds_type == "databricks":
        test_databricks_connection(
            server_hostname=datasource["server_hostname"],
            http_path=datasource["http_path"],
            access_token=password,
        )
    else:
        raise HTTPException(400, f"Unsupported datasource: '{ds_type}'")


def _read_table(datasource, ds_type, password, table):
    if ds_type == "snowflake":
        return read_snowflake_table(
            account=datasource["account"], user=datasource["username"],
            password=password, warehouse=datasource["warehouse"],
            database=datasource["database"], schema=datasource.get("schema", ""),
            table=table,
        )
    elif ds_type in ("sqlserver", "azuresql"):
        return read_sqlserver_table(
            server=datasource["server"], database=datasource["database"],
            username=datasource["username"], password=password,
            table_name=table,
        )
    elif ds_type == "databricks":
        return read_databricks_table(
            server_hostname=datasource["server_hostname"],
            http_path=datasource["http_path"],
            access_token=password,
            catalog=datasource.get("catalog", "hive_metastore"),
            schema=datasource.get("schema", "default"),
            table=table,
        )
    else:
        raise HTTPException(400, f"Unsupported datasource: '{ds_type}'")