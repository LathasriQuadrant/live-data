"""
routes/refresh.py
─────────────────
Refresh management endpoints.

  POST   /api/v1/lakehouse/refresh/{workbook_name}
      Manually trigger an immediate refresh for one workbook.

  POST   /api/v1/lakehouse/refresh/all
      Manually trigger refresh for every registered migration.

  PATCH  /api/v1/lakehouse/refresh/{workbook_name}/schedule
      Update the scheduled refresh interval (minutes) for a workbook.

  GET    /api/v1/lakehouse/refresh/jobs
      List all scheduled refresh jobs and their next-run times.
"""

from fastapi import APIRouter, HTTPException, Body
from concurrent.futures import ThreadPoolExecutor, as_completed

import utils.registry       as registry
import utils.scheduler      as scheduler
from utils.refresh_engine   import refresh_migration

router = APIRouter(prefix="/api/v1/lakehouse", tags=["Refresh"])


# ─────────────────────────────────────────────────────────────
# Manual refresh — single workbook
# ─────────────────────────────────────────────────────────────

@router.post("/refresh/{workbook_name}")
def manual_refresh(workbook_name: str):
    """
    Immediately re-read source data and overwrite Lakehouse tables,
    then trigger a Power BI dataset refresh (if dataset_id is registered).

    Works for ALL migration types (tableau, powerbi, ...).
    """
    record = registry.get(workbook_name)
    if not record:
        raise HTTPException(
            404,
            f"No migration record for '{workbook_name}'. "
            "Run /migrate first or check the exact workbook name."
        )

    print(f"\n🖐️  Manual refresh requested: '{workbook_name}'")
    result = refresh_migration(workbook_name)

    if result["status"] == "failed":
        raise HTTPException(500, detail=result)

    return result


# ─────────────────────────────────────────────────────────────
# Manual refresh — all workbooks
# ─────────────────────────────────────────────────────────────

@router.post("/refresh/all")
def manual_refresh_all(parallel: bool = False):
    """
    Trigger an immediate refresh for every registered migration.

    Query param:
      ?parallel=true  — run all refreshes concurrently (faster, higher load)
      ?parallel=false — run sequentially (default, safer)
    """
    records = registry.get_all()
    if not records:
        return {"status": "nothing_to_refresh", "total": 0, "results": []}

    print(f"\n🖐️  Manual refresh ALL ({len(records)} workbooks, parallel={parallel})")

    results = []
    if parallel:
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {
                pool.submit(refresh_migration, r["workbook_name"]): r["workbook_name"]
                for r in records
            }
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    name = futures[future]
                    results.append({
                        "workbook_name": name,
                        "status":        "failed",
                        "error":         str(e),
                    })
    else:
        for record in records:
            try:
                results.append(refresh_migration(record["workbook_name"]))
            except Exception as e:
                results.append({
                    "workbook_name": record["workbook_name"],
                    "status":        "failed",
                    "error":         str(e),
                })

    total_ok     = sum(1 for r in results if r.get("status") == "success")
    total_failed = sum(1 for r in results if r.get("status") == "failed")

    return {
        "status":        "done",
        "total":          len(results),
        "total_success":  total_ok,
        "total_failed":   total_failed,
        "results":        results,
    }


# ─────────────────────────────────────────────────────────────
# Update scheduled refresh interval
# ─────────────────────────────────────────────────────────────

@router.patch("/refresh/{workbook_name}/schedule")
def update_schedule(workbook_name: str, payload: dict = Body(...)):
    """
    Change how often a workbook is automatically refreshed.

    Body: { "interval_minutes": 30 }

    Minimum: 5 minutes. The change takes effect immediately — the scheduler
    replaces the existing job with the new interval.
    """
    interval = payload.get("interval_minutes")
    enable   = payload.get("enable_scheduled_refresh")

    if interval is not None and (not isinstance(interval, int) or interval < 5):
        raise HTTPException(400, "'interval_minutes' must be an integer ≥ 5")

    record = registry.get(workbook_name)
    if not record:
        raise HTTPException(404, f"No migration record for '{workbook_name}'")

    # Apply only the fields that were provided
    if interval is not None:
        record["refresh_interval_minutes"] = interval
    if enable is not None:
        record["enable_scheduled_refresh"] = enable

    # Validate: enabling refresh requires an interval to be set
    if record.get("enable_scheduled_refresh") and not record.get("refresh_interval_minutes"):
        raise HTTPException(400, "Cannot enable scheduled refresh — 'interval_minutes' is not set.")

    registry._cache[workbook_name] = record
    registry._save()

    # Reschedule or remove job depending on enabled state
    if record.get("enable_scheduled_refresh"):
        scheduler.add_or_replace_job(record)
    else:
        scheduler.remove_job(workbook_name)

    return {
        "status":                    "updated",
        "workbook_name":             workbook_name,
        "enable_scheduled_refresh":  record.get("enable_scheduled_refresh"),
        "interval_minutes":          record.get("refresh_interval_minutes"),
        "message": (
            f"Scheduled refresh enabled every {record['refresh_interval_minutes']} minute(s)."
            if record.get("enable_scheduled_refresh")
            else "Scheduled refresh disabled."
        ),
    }


# ─────────────────────────────────────────────────────────────
# List scheduled jobs
# ─────────────────────────────────────────────────────────────

@router.get("/refresh/jobs")
def list_refresh_jobs():
    """
    List all active scheduled refresh jobs and when they next run.
    """
    jobs = scheduler.list_jobs()
    return {
        "status":     "success",
        "total_jobs": len(jobs),
        "jobs":       jobs,
    }