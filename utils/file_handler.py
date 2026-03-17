"""
File handling — TWB / TWBX parsing.

Extracts from Tableau workbook XML:
  - Live source connection details (Snowflake / Azure SQL / Databricks)
  - Table names referenced in the workbook
  - Column names + Tableau data types (for metadata preservation)
  - Table relationships (for future semantic model building)
"""

import os
import zipfile
import tempfile
import xml.etree.ElementTree as ET
from fastapi import HTTPException


# ─────────────────────────────────────────────────────────────
# Workbook name
# ─────────────────────────────────────────────────────────────

def get_workbook_name(file_path: str) -> str:
    """Return the workbook name (filename stem without extension)."""
    try:
        return os.path.splitext(os.path.basename(file_path))[0]
    except Exception:
        return "Tableau_Workbook"


# ─────────────────────────────────────────────────────────────
# TWBX → TWB extraction
# ─────────────────────────────────────────────────────────────

def extract_twb_from_twbx(twbx_path: str) -> str:
    """
    If the input is a .twbx (ZIP), extract the embedded .twb XML file
    to a temp directory and return its path.
    If already a .twb, return it unchanged.
    """
    if not twbx_path.lower().endswith(".twbx"):
        return twbx_path

    print(f"📦 Extracting .twb from .twbx: {os.path.basename(twbx_path)}")
    temp_dir = tempfile.mkdtemp()

    with zipfile.ZipFile(twbx_path, "r") as zf:
        for name in zf.namelist():
            if name.lower().endswith(".twb"):
                zf.extract(name, temp_dir)
                twb_path = os.path.join(temp_dir, name)
                print(f"   ✅ Extracted: {twb_path}")
                return twb_path

    raise HTTPException(
        status_code=400,
        detail=f"No .twb file found inside '{os.path.basename(twbx_path)}'.",
    )


# ─────────────────────────────────────────────────────────────
# Column type extraction
# ─────────────────────────────────────────────────────────────

def _extract_column_types(root) -> dict[str, str]:
    """
    Parse <column> elements from workbook XML.

    Returns:
        {clean_column_name: tableau_datatype}
        e.g. {"ORDER_ID": "integer", "REVENUE": "real", "NAME": "string"}
    """
    col_types = {}
    for col in root.findall(".//column"):
        name     = col.get("name")
        datatype = col.get("datatype")
        if not name or not datatype:
            continue
        if name.startswith("[__tableau") or datatype == "table":
            continue
        clean = name.strip("[]")
        col_types[clean] = datatype.lower()

    print(f"   📋 Extracted {len(col_types)} column type(s) from workbook XML")
    return col_types


# ─────────────────────────────────────────────────────────────
# Relationship extraction
# ─────────────────────────────────────────────────────────────

def _extract_relationships(root) -> list[dict]:
    """
    Extract table join relationships from the workbook's object-graph.

    Returns list of:
        {"fromTable": str, "fromColumn": str, "toTable": str, "toColumn": str}
    """
    relationships = []
    rel_container = root.find(".//datasource/object-graph/relationships")
    if rel_container is None:
        return []

    seen = set()
    for rel_elem in rel_container:
        if "relationship" not in rel_elem.tag:
            continue

        expressions = rel_elem.findall(".//expression")
        if len(expressions) < 2:
            continue

        columns = []
        for expr in expressions:
            nested = expr.find("expression")
            op = nested.get("op") if nested is not None else expr.get("op")
            if op and op != "=":
                clean = op.strip("[]").split("(")[0].strip()
                if clean:
                    columns.append(clean)

        if len(columns) < 2:
            continue

        from_col = columns[0]
        to_col   = columns[1]

        ep1 = rel_elem.find("first-end-point")
        ep2 = rel_elem.find("second-end-point")
        if ep1 is None or ep2 is None:
            continue

        from_table = ep1.get("object-id", "").split("_")[0].split("(")[0].strip()
        to_table   = ep2.get("object-id", "").split("_")[0].split("(")[0].strip()

        if not (from_table and to_table):
            continue

        key = f"{from_table}.{from_col}->{to_table}.{to_col}"
        if key in seen:
            continue
        seen.add(key)

        relationships.append({
            "fromTable":  from_table,
            "fromColumn": from_col,
            "toTable":    to_table,
            "toColumn":   to_col,
        })

    print(f"   🔗 Found {len(relationships)} relationship(s)")
    return relationships


# ─────────────────────────────────────────────────────────────
# Table name extraction
# ─────────────────────────────────────────────────────────────

def _extract_tables(root) -> list[str]:
    """
    Extract REAL table names from the workbook datasource.
    Only includes tables from <relation type="table"> elements.
    Skips Tableau aliases (names with spaces), fully-qualified names
    with dot notation (schema.table), and duplicates.
    """
    tables       = []
    tables_lower = set()

    for rel in root.findall(".//relation[@type='table']"):
        raw = rel.get("table", "")
        if not raw:
            continue

        # Parse [schema].[table] or [table] format
        parts = raw.strip("[]").split("].[")
        if len(parts) >= 2:
            clean = parts[-1]   # take just the table name, drop schema
        else:
            clean = raw.strip("[]")

        # Skip if contains a dot (fully qualified name like LATHASRIDB.CUSTOMERS)
        if "." in clean:
            print(f"   ⚠️  Skipping qualified name: {clean}")
            continue

        # Skip Tableau display aliases (contain spaces) — e.g. "Fact Sales"
        # Real DB table names use underscores, not spaces
        if " " in clean:
            print(f"   ⚠️  Skipping display alias: {clean}")
            continue

        if clean.lower() not in tables_lower:
            tables.append(clean)
            tables_lower.add(clean.lower())

    print(f"   📊 Found {len(tables)} real table(s): {tables}")
    return tables


# ─────────────────────────────────────────────────────────────
# Main public function
# ─────────────────────────────────────────────────────────────

def extract_datasource_from_twb(twb_path: str) -> dict:
    """
    Parse a .twb XML file and return all datasource information needed
    to connect to the live source and reproduce Tableau's metadata.

    Returns dict with keys varying by source type, plus always:
        type          : "snowflake" | "azuresql" | "sqlserver" | "databricks"
        tables        : [str]
        column_types  : {col_name: tableau_type}
        relationships : [{fromTable, fromColumn, toTable, toColumn}]

    Raises:
        HTTPException 400 if no supported connection found.
        HTTPException 400 if the file is not valid XML.
    """
    if not os.path.exists(twb_path):
        raise HTTPException(status_code=404, detail=f"TWB file not found: {twb_path}")

    print(f"\n🔍 Parsing TWB: {os.path.basename(twb_path)}")

    try:
        tree = ET.parse(twb_path)
        root = tree.getroot()
    except ET.ParseError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid TWB XML: {str(e)}",
        )

    connections = {}

    # ── Snowflake ─────────────────────────────────────────────
    for nc in root.findall(".//named-connection"):
        sf = nc.find("connection[@class='snowflake']")
        if sf is not None:
            server = sf.get("server", "")
            connections["snowflake"] = {
                "type":      "snowflake",
                "server":    server,
                "account":   server.split(".")[0] if server else None,
                "database":  sf.get("dbname"),
                "schema":    sf.get("schema"),
                "warehouse": sf.get("warehouse"),
                "username":  sf.get("username"),
            }
            print(f"   ✅ Snowflake: {server}")
            break

    # ── SQL Server / Azure SQL ────────────────────────────────
    if "sqlserver" not in connections:
        for class_name in ("sqlserver", "azure_sqldb"):
            for nc in root.findall(".//named-connection"):
                conn_elem = nc.find(f"connection[@class='{class_name}']")
                if conn_elem is not None:
                    server   = conn_elem.get("server", "")
                    is_azure = ".database.windows.net" in server
                    connections["sqlserver"] = {
                        "type":     "azuresql" if is_azure else "sqlserver",
                        "server":   server,
                        "database": conn_elem.get("dbname"),
                        "username": conn_elem.get("username"),
                        "is_azure": is_azure,
                    }
                    print(f"   ✅ {'Azure SQL' if is_azure else 'SQL Server'}: {server}")
                    break
            if "sqlserver" in connections:
                break

    # ── Databricks / Spark ────────────────────────────────────
    if "databricks" not in connections:
        for class_name in ("databricks", "spark"):
            for nc in root.findall(".//named-connection"):
                conn_elem = nc.find(f"connection[@class='{class_name}']")
                if conn_elem is not None:
                    server    = conn_elem.get("server", "")
                    http_path = conn_elem.get("v-http-path")
                    catalog   = conn_elem.get("catalog") or conn_elem.get("database")
                    schema    = conn_elem.get("schema") or conn_elem.get("dbname")
                    connections["databricks"] = {
                        "type":            "databricks",
                        "server_hostname": server,
                        "http_path":       http_path,
                        "catalog":         catalog,
                        "schema":          schema,
                    }
                    print(f"   ✅ Databricks: {server}  http_path={http_path}")
                    break
            if "databricks" in connections:
                break

    if not connections:
        raise HTTPException(
            status_code=400,
            detail=(
                "No supported live connection found in this workbook. "
                "Supported: Snowflake, SQL Server, Azure SQL, Databricks."
            ),
        )

    # Take the first (and usually only) connection
    conn_key = next(iter(connections))
    result   = connections[conn_key].copy()

    result["tables"]        = _extract_tables(root)
    result["column_types"]  = _extract_column_types(root)
    result["relationships"] = _extract_relationships(root)

    return result