"""
GET    /api/v1/lakehouse/status                   — all migrated workbooks
GET    /api/v1/lakehouse/status/{workbook_name}   — detail + live table list
DELETE /api/v1/lakehouse/state/{workbook_name}    — remove state record
"""

from fastapi import APIRouter, HTTPException
from utils.auth     import get_fabric_token
from utils.fabric   import get_lakehouse_tables
import utils.registry as registry
import utils.scheduler as scheduler

router = APIRouter(prefix="/api/v1/lakehouse", tags=["Lakehouse"])


# Backward-compat shim — registry.register() is now called directly in migrate.py
def register_migration(workbook_name: str, record: dict) -> None:
    pass


@router.get("/status")
def status_all():
    """List all migrations (current session + persisted from previous runs)."""
    records = registry.get_all()
    safe    = [_safe_record(r) for r in records]
    return {
        "status":    "success",
        "total":     len(safe),
        "workbooks": safe,
    }


@router.get("/status/{workbook_name}")
def status_one(workbook_name: str):
    """Return status for one workbook including a live table list."""
    record = registry.get(workbook_name)
    if not record:
        raise HTTPException(
            404,
            f"No migration record found for '{workbook_name}'. Run /migrate first.",
        )

    live_tables: list[str] = []
    try:
        token       = get_fabric_token()
        live_tables = get_lakehouse_tables(
            token        = token,
            workspace_id = record["workspace_id"],
            lakehouse_id = record["lakehouse_id"],
        )
    except Exception as e:
        print(f"⚠️  Could not fetch live table list from Fabric: {e}")

    return {
        "status":        "success",
        "workbook_name": workbook_name,
        "state":         _safe_record(record),
        "live_tables":   live_tables,
        "table_count":   len(live_tables) if live_tables else len(record.get("tables", [])),
    }


@router.delete("/state/{workbook_name}")
def delete_state(workbook_name: str):
    """Remove a migration record and cancel its scheduled refresh job."""
    existed = registry.delete(workbook_name)
    if not existed:
        raise HTTPException(404, f"No record for '{workbook_name}'")

    scheduler.remove_job(workbook_name)

    return {
        "status":        "deleted",
        "workbook_name": workbook_name,
        "message":       f"Registry record and scheduled job removed for '{workbook_name}'.",
    }


def _safe_record(record: dict) -> dict:
    """Return record with encrypted password stripped."""
    safe = {k: v for k, v in record.items() if k != "encrypted_password"}
    safe["has_stored_credentials"] = bool(record.get("encrypted_password"))
    return safe
