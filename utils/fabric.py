"""
Microsoft Fabric REST API utilities.
"""

import time
import requests
import pyarrow as pa
from fastapi import HTTPException
from config import FABRIC_API, ONELAKE_API, TENANT_ID, CLIENT_ID, CLIENT_SECRET

_PROVISION_TIMEOUT  = 180
_PROVISION_POLL_SEC = 5


def _hdr(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _get_onelake_token() -> str:
    """OneLake DFS requires Azure Storage scope, not PowerBI scope."""
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
    token = resp.json().get("access_token")
    if not token:
        raise HTTPException(500, "No access_token in OneLake token response")
    print("✅ OneLake storage token obtained")
    return token


# ─────────────────────────────────────────────────────────────
# Lakehouse creation
# ─────────────────────────────────────────────────────────────

def _attach_sql_endpoint(token, workspace_id, item: dict) -> dict:
    """Enrich a Lakehouse item dict with its SQL analytics endpoint connection string."""
    lakehouse_id = item.get("id")
    if lakehouse_id:
        item["sqlEndpointConnectionString"] = get_lakehouse_sql_endpoint(
            token, workspace_id, lakehouse_id
        )
    return item


def create_lakehouse(token, workspace_id, lakehouse_name):
    existing = get_lakehouse_by_name(token, workspace_id, lakehouse_name)
    if existing:
        print(f"ℹ️  Lakehouse '{lakehouse_name}' already exists — reusing  (id={existing['id']})")
        existing["created"] = False
        return _attach_sql_endpoint(token, workspace_id, existing)

    print(f"🏗️  Creating Lakehouse '{lakehouse_name}'...")
    url  = f"{FABRIC_API}/workspaces/{workspace_id}/items"
    body = {"displayName": lakehouse_name, "type": "Lakehouse"}
    resp = requests.post(url, json=body, headers=_hdr(token), timeout=60)

    if resp.status_code == 201:
        item = resp.json()
        item["created"] = True
        print(f"✅ Lakehouse created: {item['id']}")
        return _attach_sql_endpoint(token, workspace_id, item)

    if resp.status_code == 202:
        op_url = resp.headers.get("Location") or resp.headers.get("Operation-Location")
        item   = _poll_provision(token, op_url, workspace_id, lakehouse_name)
        item["created"] = True
        return _attach_sql_endpoint(token, workspace_id, item)

    raise HTTPException(status_code=resp.status_code, detail=f"Failed to create Lakehouse: {resp.text}")


def _poll_provision(token, op_url, workspace_id, lakehouse_name):
    deadline = time.time() + _PROVISION_TIMEOUT
    if op_url:
        while time.time() < deadline:
            resp = requests.get(op_url, headers=_hdr(token), timeout=30)
            if resp.status_code == 200:
                status = resp.json().get("status", "").lower()
                if status in ("succeeded", "completed"):
                    item = get_lakehouse_by_name(token, workspace_id, lakehouse_name)
                    if item:
                        return item
                if status == "failed":
                    raise HTTPException(500, f"Lakehouse provisioning failed: {resp.json()}")
            time.sleep(_PROVISION_POLL_SEC)
    else:
        while time.time() < deadline:
            item = get_lakehouse_by_name(token, workspace_id, lakehouse_name)
            if item:
                print(f"✅ Lakehouse ready: {item['id']}")
                return item
            print(f"   ⏳ Waiting for '{lakehouse_name}'...")
            time.sleep(_PROVISION_POLL_SEC)
    raise HTTPException(504, f"Lakehouse '{lakehouse_name}' provision timed out after {_PROVISION_TIMEOUT}s")


def get_lakehouse_sql_endpoint(token, workspace_id, lakehouse_id) -> str | None:
    """
    Fetch the SQL analytics endpoint connection string for a Lakehouse.
    Returns a connection string of the form:
        <server>.datawarehouse.fabric.microsoft.com
    or None if it cannot be determined.
    """
    url  = f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
    resp = requests.get(url, headers=_hdr(token), timeout=30)
    if resp.status_code != 200:
        print(f"⚠️  Could not fetch Lakehouse details for SQL endpoint: {resp.text}")
        return None
    data = resp.json()
    # The SQL endpoint lives under properties.sqlEndpointProperties
    sql_props = (
        data.get("properties", {}).get("sqlEndpointProperties", {})
    )
    connection_string = sql_props.get("connectionString")
    if connection_string:
        print(f"✅ SQL endpoint: {connection_string}")
    else:
        print("⚠️  SQL endpoint not yet available in Lakehouse properties.")
    return connection_string


def get_lakehouse_by_name(token, workspace_id, lakehouse_name):
    url  = f"{FABRIC_API}/workspaces/{workspace_id}/items"
    resp = requests.get(url, params={"type": "Lakehouse"}, headers=_hdr(token), timeout=30)
    if resp.status_code != 200:
        return None
    for item in resp.json().get("value", []):
        if item.get("displayName", "").lower() == lakehouse_name.lower():
            return item
    return None


def get_lakehouse_tables(token, workspace_id, lakehouse_id):
    url  = f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
    resp = requests.get(url, headers=_hdr(token), timeout=30)
    if resp.status_code != 200:
        print(f"⚠️  Could not fetch Lakehouse tables: {resp.text}")
        return []
    return [t.get("name", "") for t in resp.json().get("data", [])]


# ─────────────────────────────────────────────────────────────
# DataFrame preparation
# ─────────────────────────────────────────────────────────────

def _prepare_for_parquet(df):
    """
    Convert pandas extension types to numpy/datetime types that PyArrow can
    serialise cleanly. Timezone-naive datetimes are localised to UTC so that
    PyArrow produces timestamp[us, UTC] (TIMESTAMP_LTZ) rather than
    timestamp[us] (TIMESTAMP_NTZ), which Fabric Lakehouse Delta does not support.
    """
    import pandas as pd
    df = df.copy()
    for col in df.columns:
        dtype = str(df[col].dtype)
        # Pandas nullable integers
        if dtype in ("Int8", "Int16", "Int32", "Int64",
                     "UInt8", "UInt16", "UInt32", "UInt64"):
            df[col] = df[col].astype("float64") if df[col].isna().any() else df[col].astype("int64")
        # Pandas nullable boolean
        elif dtype == "boolean":
            df[col] = df[col].astype("object") if df[col].isna().any() else df[col].astype("bool")
        # Object columns
        elif dtype == "object":
            df[col] = df[col].where(df[col].isna(), df[col].astype(str))
        # Timezone-naive datetime64 columns -> UTC-aware
        # Fabric Delta does not support TIMESTAMP_NTZ; requires TIMESTAMP_LTZ (UTC)
        elif "datetime64" in dtype and "UTC" not in dtype and "tz" not in dtype.lower():
            df[col] = pd.to_datetime(df[col]).dt.tz_localize("UTC")
        elif hasattr(df[col].dtype, "tz") and df[col].dtype.tz is None:
            df[col] = df[col].dt.tz_localize("UTC")
    return df


def _is_date_only_series(series) -> bool:
    """
    Return True if every non-null value in a string/object series looks like
    a plain date (YYYY-MM-DD) with no time component, i.e. the time part is
    midnight or absent entirely.
    """
    import pandas as pd
    non_null = series.dropna()
    if len(non_null) == 0:
        return False
    try:
        parsed = pd.to_datetime(non_null, errors="raise")
        # All times are exactly midnight → pure date column
        return (parsed.dt.hour == 0).all() and (parsed.dt.minute == 0).all() and (parsed.dt.second == 0).all()
    except Exception:
        return False


def _df_to_arrow(df):
    """
    Convert a pandas DataFrame to a clean PyArrow Table, resolving types that
    Fabric Lakehouse Delta does not support:

    - object columns that contain date-only values  → pa.date32()
    - TIMESTAMP_NTZ (no timezone) with a time part  → timestamp[us, UTC]
    - datetime64 columns (pandas)                   → cast via same rules
    """
    import pandas as pd
    import datetime

    df = df.copy()

    # Pre-pass on object columns: detect date-only strings and convert in pandas
    # so PyArrow sees them as date32, not large_string.
    for col in df.columns:
        if df[col].dtype == object:
            non_null = df[col].dropna()
            if len(non_null) == 0:
                continue
            
            # ✅ FIXED: Check if already date objects - don't re-parse!
            first_val = non_null.iloc[0]
            if isinstance(first_val, datetime.date) and not isinstance(first_val, datetime.datetime):
                print(f"      '{col}': Already date objects -> DATE")
                continue  # Skip to next column!
            
            # Quick check: does it look like a date/datetime string?
            sample = str(non_null.iloc[0])
            if len(sample) < 8 or not sample[:4].isdigit():
                continue
            try:
                parsed = pd.to_datetime(non_null, errors="raise")
                if (parsed.dt.hour == 0).all() and (parsed.dt.minute == 0).all() and (parsed.dt.second == 0).all():
                    # Pure date — store as date only (no timestamp type at all)
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
                    print(f"      Cast '{col}': date-only strings -> DATE")
                else:
                    # Has a real time component — localise to UTC
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_localize("UTC")
                    print(f"      Cast '{col}': datetime strings -> TIMESTAMP_LTZ (UTC)")
            except Exception:
                pass  # Not a datetime string — leave as string

    table = pa.Table.from_pandas(df, preserve_index=False)

    # Post-pass at Arrow level: catch any remaining TIMESTAMP_NTZ columns
    # (e.g. from pandas datetime64[ns] without tz that slipped through)
    new_fields  = []
    new_columns = []
    cast_needed = False
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_timestamp(field.type) and field.type.tz is None:
            # Check if all times are midnight — treat as DATE
            try:
                as_pandas = col.to_pydict() if hasattr(col, "to_pydict") else None
                ts_series = col.cast(pa.timestamp("us")).to_pandas()
                all_midnight = (ts_series.dt.hour == 0).all() and (ts_series.dt.minute == 0).all()
            except Exception:
                all_midnight = False

            if all_midnight:
                new_type = pa.date32()
                col      = col.cast(new_type)
                field    = field.with_type(new_type)
                print(f"      Cast '{field.name}' TIMESTAMP_NTZ (midnight) -> DATE")
            else:
                new_type = pa.timestamp(field.type.unit, tz="UTC")
                col      = col.cast(new_type)
                field    = field.with_type(new_type)
                print(f"      Cast '{field.name}' TIMESTAMP_NTZ -> TIMESTAMP_LTZ (UTC)")
            cast_needed = True

        new_fields.append(field)
        new_columns.append(col)

    if cast_needed:
        table = pa.table(new_columns, schema=pa.schema(new_fields))

    # Strip pandas metadata
    table = table.replace_schema_metadata({})
    return table


# ─────────────────────────────────────────────────────────────
# Write DataFrame as Delta table
# ─────────────────────────────────────────────────────────────

def write_table_to_lakehouse(token, workspace_id, lakehouse_id, lakehouse_name, table_name, df, spark_schema, mode="overwrite"):
    """
    Write a pandas DataFrame as a Delta table directly into OneLake using
    delta-rs (the `deltalake` package). No Spark, no staging file, no Load
    Table API — just a pure Python write over the ADLS Gen2 / OneLake endpoint.

    The Service Principal bearer token obtained for the Storage scope is passed
    as `bearer_token` in storage_options, which delta-rs forwards to the
    OneLake DFS endpoint.  `use_fabric_endpoint=true` tells delta-rs to use
    the OneLake-specific URL form.
    """
    from deltalake import write_deltalake

    if df.empty:
        print(f"   ⚠️  '{table_name}' is empty — skipping")
        return {"table_name": table_name, "rows": 0, "columns": len(df.columns), "status": "skipped_empty"}

    print(f"   📤 Writing '{table_name}': {len(df):,} rows × {len(df.columns)} cols")

    # Convert to clean Arrow table (strip pandas metadata, fix nullable types)
    pa_table = _df_to_arrow(_prepare_for_parquet(df))
    print(f"      📋 Arrow schema: {pa_table.schema}")

    # abfss path pointing straight at the Tables/ directory in OneLake
    dest_path = (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com"
        f"/{lakehouse_id}/Tables/{table_name}"
    )

    # Re-use the OneLake storage token (Storage scope) that delta-rs needs
    onelake_token = _get_onelake_token()
    storage_options = {
        "bearer_token":        onelake_token,
        "use_fabric_endpoint": "true",
    }

    delta_mode = "overwrite" if mode.lower() == "overwrite" else "append"
    write_deltalake(
        dest_path,
        pa_table,
        mode=delta_mode,
        schema_mode="overwrite",   # always update schema on overwrite
        storage_options=storage_options,
    )

    print(f"   ✅ '{table_name}' written as Delta table via delta-rs")
    return {"table_name": table_name, "rows": len(df), "columns": len(df.columns), "status": "success"}