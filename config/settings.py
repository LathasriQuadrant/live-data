"""
Configuration settings — Tableau to Fabric Lakehouse API
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Azure AD / Service Principal ──────────────────────────────
TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# ── Azure Blob Storage ────────────────────────────────────────
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_BLOB_CONTAINER            = os.getenv("AZURE_BLOB_CONTAINER", "tabluea-raw")

# ── Microsoft Fabric ──────────────────────────────────────────
FABRIC_API       = "https://api.fabric.microsoft.com/v1"
ONELAKE_API      = "https://onelake.dfs.fabric.microsoft.com"
FABRIC_SCOPE     = "https://analysis.windows.net/powerbi/api/.default"
FABRIC_WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID")

# ── Supported live source types ───────────────────────────────
SUPPORTED_DATABASES = {
    "snowflake":  "Snowflake",
    "sqlserver":  "SQL Server",
    "azuresql":   "Azure SQL Database",
    "databricks": "Databricks",
}
