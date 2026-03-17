"""
FIXED type_mapper.py - Proper handling of Tableau date vs datetime types.

KEY FIX: 
- Tableau 'date' type → pandas date (no time component)
- Tableau 'datetime' type → pandas datetime with time
"""

import pandas as pd


# ─────────────────────────────────────────────────────────────
# Type tables
# ─────────────────────────────────────────────────────────────

TABLEAU_TO_SPARK: dict[str, str] = {
    "integer":  "BIGINT",
    "real":     "DOUBLE",
    "boolean":  "BOOLEAN",
    "string":   "STRING",
    "date":     "DATE",        # Date only, no time
    "datetime": "TIMESTAMP",   # Date + time
    "spatial":  "STRING",
    "unknown":  "STRING",
}

TABLEAU_TO_PANDAS: dict[str, str] = {
    "integer":  "Int64",
    "real":     "float64",
    "boolean":  "boolean",
    "string":   "object",
    "date":     "object",         # Will be converted to date objects
    "datetime": "datetime64[ns]", # Will be datetime with time
    "spatial":  "object",
    "unknown":  "object",
}


# ─────────────────────────────────────────────────────────────
# Converters
# ─────────────────────────────────────────────────────────────

def tableau_type_to_spark(tableau_type: str) -> str:
    return TABLEAU_TO_SPARK.get(tableau_type.lower(), "STRING")


def _infer_spark_from_pandas(dtype_str: str) -> str:
    """Fallback: infer Spark type from a pandas dtype string."""
    d = dtype_str.lower()
    if "int" in d:
        return "BIGINT"
    if "float" in d or "double" in d:
        return "DOUBLE"
    if "bool" in d:
        return "BOOLEAN"
    if "datetime" in d or "timestamp" in d:
        return "TIMESTAMP"
    if "date" in d:
        return "DATE"
    return "STRING"


def build_delta_schema(
    df: pd.DataFrame,
    tableau_column_types: dict[str, str],
) -> dict[str, str]:
    """
    Build {column_name: spark_type} for every column in df.

    Priority:
      1. Tableau metadata from workbook XML  (exact match)
      2. Inferred from pandas dtype          (fallback)
    """
    schema: dict[str, str] = {}
    for col in df.columns:
        t_type = tableau_column_types.get(col, "").lower()
        schema[col] = (
            tableau_type_to_spark(t_type)
            if t_type
            else _infer_spark_from_pandas(str(df[col].dtype))
        )
    return schema


# ─────────────────────────────────────────────────────────────
# DataFrame coercion
# ─────────────────────────────────────────────────────────────
def coerce_dataframe_to_tableau_types(
    df: pd.DataFrame,
    column_types: dict[str, str],
) -> pd.DataFrame:
    """..."""
    df = df.copy()
    
    # DEBUG: Print what column types we got from TWB
    print(f"   🔍 DEBUG column_types dict: {column_types}")
    print(f"   🔍 DEBUG df.columns: {df.columns.tolist()}")

    for col in df.columns:
        t_type = column_types.get(col, "").lower()
        
        # DEBUG: Print for each column
        print(f"   🔍 DEBUG Processing column '{col}': type='{t_type}'")
        
        if not t_type:
            print(f"   🔍 DEBUG '{col}' has no type in column_types, skipping")
            continue
        
        try:
            if t_type == "integer":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

            elif t_type == "real":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

            elif t_type == "boolean":
                df[col] = df[col].map(
                    lambda v: (
                        True  if str(v).lower() in ("true",  "1", "yes") else
                        False if str(v).lower() in ("false", "0", "no")  else
                        None
                    )
                ).astype("boolean")

            # elif t_type == "date":
            #     # FIXED: Date only - no time component
            #     # Convert to datetime first, then extract just the date
            #     parsed = pd.to_datetime(df[col], errors="coerce")
                
            #     # Convert to date objects (removes time component)
            #     df[col] = parsed.dt.date
                
            #     null_count = df[col].isna().sum()
            #     if null_count > 0:
            #         print(f"   ⚠️  '{col}': {null_count} NULL value(s) after date conversion")

            elif t_type == "date":
                # FIXED: Handle Tableau Date objects from .hyper files
                # The hyper file returns tableauhyperapi.Date objects, not strings
                # We need to convert them to Python date objects first
                
                print(f"   🔍 DEBUG '{col}' BEFORE conversion:")
                print(f"      dtype: {df[col].dtype}")
                print(f"      First 3 values: {df[col].head(3).tolist()}")
                
                # Check if values are already Date-like objects (from hyper)
                if df[col].dtype == object:
                    non_null = df[col].dropna()
                    if len(non_null) > 0:
                        first_val = non_null.iloc[0]
                        # Check if it's a Tableau Date object or similar
                        if hasattr(first_val, 'year') and hasattr(first_val, 'month') and hasattr(first_val, 'day'):
                            # Convert Tableau Date objects to Python date objects
                            import datetime
                            df[col] = df[col].apply(
                                lambda x: datetime.date(x.year, x.month, x.day) if pd.notna(x) and hasattr(x, 'year') else None
                            )
                            print(f"      ✅ Converted Tableau Date objects to Python date objects")
                            continue
                
                # Otherwise, try to parse as strings
                parsed = pd.to_datetime(df[col], errors="coerce")
                df[col] = parsed.dt.date
                
                null_count = df[col].isna().sum()
                if null_count > 0:
                    print(f"   ⚠️  '{col}': {null_count} NULL value(s) after date conversion")

            elif t_type == "string":
                df[col] = (
                    df[col]
                    .astype(str)
                    .replace({"nan": None, "<NA>": None, "None": None})
                )

        except Exception as e:
            print(f"   ⚠️  Could not coerce '{col}' to {t_type}: {e}")

    return df