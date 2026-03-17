"""
utils/registry.py
─────────────────
Persistent migration registry backed by a local JSON file.

Every successful migration writes a record here so:
  • The scheduler can re-run any migration after restart.
  • Passwords are stored AES-256 encrypted (Fernet) — never plain-text.
  • Power BI dataset IDs are stored alongside so the scheduler can
    trigger a PBI refresh after each Lakehouse update.

Record schema (per workbook_name key):
{
    "workbook_name":    str,
    "file_name":        str,          # blob filename (.twbx)
    "container_name":   str,
    "workspace_id":     str,
    "lakehouse_id":     str,
    "lakehouse_name":   str,
    "data_mode":        "extracted" | "live",
    "source_type":      "hyper" | "snowflake" | "sqlserver" | "azuresql" | "databricks",
    "tables":           [str, ...],
    "encrypted_password": str | None, # Fernet-encrypted, None for extracted
    "powerbi_dataset_id": str | None, # optional, triggers PBI refresh if set
    "connection_details": {           # persisted at migration — refresh never re-parses the .twbx
        # Snowflake:  account, server, database, schema, warehouse, username
        # SQL Server: server, database, username, is_azure
        # Databricks: server_hostname, http_path, catalog, schema
        # Extracted:  {} (empty — no live connection)
    },
    "refresh_interval_minutes": int,  # default 5
    "last_migrated_at": ISO-8601 str,
    "last_refreshed_at": ISO-8601 str | None,
    "last_refresh_status": "success" | "failed" | None,
    "last_refresh_error":  str | None,
    "refresh_count":    int,
    "migration_type":   str,          # "tableau" | "powerbi" | ... (future-proof)
}
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from cryptography.fernet import Fernet
from fastapi import HTTPException

# ── File location ─────────────────────────────────────────────
_REGISTRY_PATH = Path(os.getenv("REGISTRY_PATH", "migration_registry.json"))

# ── Encryption key ─────────────────────────────────────────────
# Generate once and store in env: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# If not set, a temporary key is used (passwords lost on restart — warn loudly).
_RAW_KEY = os.getenv("REGISTRY_ENCRYPTION_KEY", "").strip()

if _RAW_KEY:
    _FERNET = Fernet(_RAW_KEY.encode())
else:
    _tmp_key = Fernet.generate_key()
    _FERNET  = Fernet(_tmp_key)
    print(
        "\n⚠️  WARNING: REGISTRY_ENCRYPTION_KEY not set in environment.\n"
        "   A temporary encryption key was generated — encrypted passwords\n"
        "   WILL BE LOST on server restart. Set this env var for production:\n"
        f"   REGISTRY_ENCRYPTION_KEY={_tmp_key.decode()}\n"
    )

# ── In-memory cache (avoids re-reading file on every request) ──
_cache: dict = {}


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────

def _load() -> dict:
    """Load registry from disk into cache."""
    global _cache
    if _REGISTRY_PATH.exists():
        try:
            with open(_REGISTRY_PATH, "r", encoding="utf-8") as f:
                _cache = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"⚠️  Registry file corrupted or unreadable: {e} — starting fresh.")
            _cache = {}
    else:
        _cache = {}
    return _cache


def _save() -> None:
    """Persist in-memory cache to disk."""
    try:
        with open(_REGISTRY_PATH, "w", encoding="utf-8") as f:
            json.dump(_cache, f, indent=2, ensure_ascii=False)
    except OSError as e:
        print(f"❌ Could not save registry to {_REGISTRY_PATH}: {e}")


def _encrypt(password: str) -> str:
    """Encrypt a plaintext password → base64 Fernet token (str)."""
    return _FERNET.encrypt(password.encode()).decode()


def _decrypt(token: str) -> str:
    """Decrypt a Fernet token → plaintext password."""
    return _FERNET.decrypt(token.encode()).decode()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# Load on module import
_load()


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────

def register(
    workbook_name: str,
    file_name: str,
    container_name: str,
    workspace_id: str,
    lakehouse_id: str,
    lakehouse_name: str,
    data_mode: str,
    source_type: str,
    tables: list[str],
    password: str = "",
    powerbi_dataset_id: str | None = None,
    connection_details: dict | None = None,
    enable_scheduled_refresh: bool = False,
    refresh_interval_minutes: int | None = None,
    migration_type: str = "tableau",
) -> dict:
    """
    Register (or update) a migration record.
    Called by /migrate after a successful migration.

    connection_details holds everything needed to reconnect to the live
    source without ever touching the .twbx again:
      Snowflake:  account, server, database, schema, warehouse, username
      SQL Server: server, database, username, is_azure
      Databricks: server_hostname, http_path, catalog, schema
      Extracted:  {} (no live connection)
    """
    encrypted_pw = _encrypt(password) if password else None

    record = {
        "workbook_name":           workbook_name,
        "file_name":               file_name,
        "container_name":          container_name,
        "workspace_id":            workspace_id,
        "lakehouse_id":            lakehouse_id,
        "lakehouse_name":          lakehouse_name,
        "data_mode":               data_mode,
        "source_type":             source_type,
        "tables":                  tables,
        "encrypted_password":      encrypted_pw,
        "powerbi_dataset_id":      powerbi_dataset_id,
        "connection_details":          connection_details or {},
        "enable_scheduled_refresh":    enable_scheduled_refresh,
        "refresh_interval_minutes":    refresh_interval_minutes,
        "last_migrated_at":        _now_iso(),
        "last_refreshed_at":       None,
        "last_refresh_status":     None,
        "last_refresh_error":      None,
        "refresh_count":           0,
        "migration_type":          migration_type,
    }

    _cache[workbook_name] = record
    _save()
    print(f"📝 Registry: registered '{workbook_name}' (type={migration_type}, source={source_type})")
    return record


def get(workbook_name: str) -> dict | None:
    """Return a registry record (password still encrypted)."""
    return _cache.get(workbook_name)


def get_all() -> list[dict]:
    """Return all records (passwords still encrypted)."""
    return list(_cache.values())


def get_password(workbook_name: str) -> str:
    """
    Return decrypted password for a workbook.
    Raises HTTPException 404 if not found, 500 if decryption fails.
    """
    record = _cache.get(workbook_name)
    if not record:
        raise HTTPException(404, f"No registry record for '{workbook_name}'")
    encrypted = record.get("encrypted_password")
    if not encrypted:
        return ""  # Extracted workbooks have no password
    try:
        return _decrypt(encrypted)
    except Exception as e:
        raise HTTPException(500, f"Could not decrypt password for '{workbook_name}': {e}")


def update_refresh_status(
    workbook_name: str,
    status: str,           # "success" | "failed"
    error: str | None = None,
) -> None:
    """Update refresh tracking fields after a scheduled or manual refresh."""
    record = _cache.get(workbook_name)
    if not record:
        return
    record["last_refreshed_at"]    = _now_iso()
    record["last_refresh_status"]  = status
    record["last_refresh_error"]   = error
    record["refresh_count"]        = record.get("refresh_count", 0) + 1
    _save()


def update_powerbi_dataset_id(workbook_name: str, dataset_id: str) -> None:
    """Attach or update the Power BI dataset ID for a workbook."""
    record = _cache.get(workbook_name)
    if not record:
        raise HTTPException(404, f"No registry record for '{workbook_name}'")
    record["powerbi_dataset_id"] = dataset_id
    _save()


def delete(workbook_name: str) -> bool:
    """Remove a record from the registry. Returns True if it existed."""
    existed = workbook_name in _cache
    _cache.pop(workbook_name, None)
    if existed:
        _save()
    return existed


def reload() -> None:
    """Force reload from disk (useful after external edits)."""
    _load()