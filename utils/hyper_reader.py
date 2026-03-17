"""
Correct Hyper file reader with proper table naming.

Extracts table names from the actual table names inside .hyper files,
not from datasource captions.

Example: 'customers.csv_8DD21EEE...' → 'customers'
"""

import os
import re
import sys
import zipfile
import tempfile
import pandas as pd
from fastapi import HTTPException


def twbx_has_extract(twbx_path: str) -> bool:
    if not twbx_path.lower().endswith(".twbx"):
        return False
    try:
        with zipfile.ZipFile(twbx_path, "r") as zf:
            return any(n.lower().endswith(".hyper") for n in zf.namelist())
    except zipfile.BadZipFile:
        return False


def _normalize(name: str) -> str:
    return name.strip('"')


def _extract_table_name(raw_name: str) -> str:
    """
    Extract clean table name from Tableau's internal naming.
    
    Examples:
        'customers.csv_8DD21EEE7ABC4E3C930A78618FCDF78B' → 'customers'
        'products.csv_65B082EB52EE4058988DD08746187706' → 'products'
        'sales.csv_57BDF416384B493EB6270775A72222E2' → 'sales'
        'Extract' → 'Extract'
    """
    # Remove .csv extension if present
    if '.csv_' in raw_name:
        # Split on .csv_ and take the first part
        clean_name = raw_name.split('.csv_')[0]
        return clean_name
    
    # Remove any GUID-like suffixes (32 hex chars)
    clean_name = re.sub(r'_[0-9A-F]{32}$', '', raw_name, flags=re.IGNORECASE)
    
    # If it ends with .csv, remove it
    if clean_name.lower().endswith('.csv'):
        clean_name = clean_name[:-4]
    
    return clean_name or "table"


def _is_default_schema(schema_name: str) -> bool:
    return schema_name.lower() in {
        "public", "information_schema", "pg_catalog", "sys", "extract"
    }


def _extract_all_hyper_files(twbx_path: str, temp_dir: str) -> list:
    """Extract ALL .hyper files from the workbook."""
    hyper_files = []
    
    with zipfile.ZipFile(twbx_path, "r") as zf:
        for name in zf.namelist():
            if name.lower().endswith(".hyper"):
                print(f"   Extracting: {name}")
                sys.stdout.flush()
                zf.extract(name, temp_dir)
                local_path = os.path.join(temp_dir, name)
                hyper_files.append(local_path)
    
    if not hyper_files:
        raise HTTPException(400, "No .hyper files found in workbook")
    
    print(f"   Found {len(hyper_files)} .hyper file(s)\n")
    sys.stdout.flush()
    
    return hyper_files


def read_hyper_tables(twbx_path: str) -> dict:
    """
    Read tables from ALL .hyper files and extract clean names.
    
    Strategy:
    1. Extract all .hyper files
    2. Read tables from each file
    3. Extract clean table name from the actual table name in the .hyper file
       (e.g., 'customers.csv_8DD21EEE...' → 'customers')
    """
    from tableauhyperapi import HyperProcess, Connection, Telemetry

    temp_dir = tempfile.mkdtemp()
    hyper_files = _extract_all_hyper_files(twbx_path, temp_dir)
    
    all_results = []
    table_names_seen = {}  # Track names to avoid duplicates

    try:
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            print("   HyperProcess started\n")
            sys.stdout.flush()

            for hyper_idx, hyper_path in enumerate(hyper_files, 1):
                print(f"   [{hyper_idx}/{len(hyper_files)}] Processing: {os.path.basename(hyper_path)}")
                sys.stdout.flush()
                
                with Connection(endpoint=hyper.endpoint, database=hyper_path) as conn:
                    schema_list = conn.catalog.get_schema_names()
                    print(f"   [{hyper_idx}/{len(hyper_files)}] Found {len(schema_list)} schema(s)")
                    sys.stdout.flush()

                    for schema in schema_list:
                        schema_name = _normalize(str(schema))
                        table_list  = conn.catalog.get_table_names(schema)
                        
                        if not table_list:
                            continue
                        
                        print(f"   [{hyper_idx}/{len(hyper_files)}] [Schema '{schema_name}'] {len(table_list)} table(s)")
                        sys.stdout.flush()

                        for table_idx, table in enumerate(table_list):
                            raw_name = _normalize(str(table.name))
                            
                            # Extract clean table name from the raw name
                            clean_name = _extract_table_name(raw_name)
                            
                            # Handle duplicates
                            if clean_name in table_names_seen:
                                # Check if it's actually the same table (same columns and row count)
                                # If so, skip it to avoid duplicates
                                print(f"   [{hyper_idx}/{len(hyper_files)}] [{table_idx + 1}/{len(table_list)}] Skipping duplicate '{clean_name}' (already processed)")
                                sys.stdout.flush()
                                continue
                            
                            table_names_seen[clean_name] = True
                            
                            print(f"   [{hyper_idx}/{len(hyper_files)}] [{table_idx + 1}/{len(table_list)}] Reading '{clean_name}' (from '{raw_name}')...")
                            sys.stdout.flush()

                            try:
                                table_def = conn.catalog.get_table_definition(table)
                                columns   = [_normalize(str(c.name)) for c in table_def.columns]
                                print(f"   [{hyper_idx}/{len(hyper_files)}] [{table_idx + 1}/{len(table_list)}] {len(columns)} columns: {columns}")
                                sys.stdout.flush()

                                query = f'SELECT * FROM "{schema_name}"."{raw_name}"'
                                rows = conn.execute_list_query(query)
                                print(f"   [{hyper_idx}/{len(hyper_files)}] [{table_idx + 1}/{len(table_list)}] {len(rows):,} rows fetched")
                                sys.stdout.flush()

                                df = pd.DataFrame(rows, columns=columns)
                                print(f"   [{hyper_idx}/{len(hyper_files)}] [{table_idx + 1}/{len(table_list)}] '{clean_name}': {df.shape}  ✅\n")
                                sys.stdout.flush()

                                all_results.append({
                                    "name":    clean_name,
                                    "schema":  schema_name,
                                    "df":      df,
                                    "columns": {c: str(t) for c, t in df.dtypes.items()},
                                })

                            except Exception as table_err:
                                print(f"   [{hyper_idx}/{len(hyper_files)}] [{table_idx + 1}/{len(table_list)}] ERROR '{clean_name}': {table_err}\n")
                                sys.stdout.flush()

        print(f"   ✅ Read {len(all_results)} unique table(s) from {len(hyper_files)} .hyper file(s)\n")
        sys.stdout.flush()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Failed to read .hyper files: {str(e)}")

    if not all_results:
        raise HTTPException(400, "No tables found in .hyper files")

    return {"tables": all_results, "hyper_files": hyper_files}