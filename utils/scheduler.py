"""
utils/scheduler.py
──────────────────
APScheduler-backed refresh scheduler.

On startup, one interval job is registered per migrated workbook using
that workbook's configured refresh_interval_minutes (default 60).

When /migrate is called:
  • A new job is added (or replaced) for the new workbook automatically.

When a workbook's interval is updated via PATCH /api/v1/lakehouse/refresh/{name}:
  • The existing job is rescheduled at the new interval.

Architecture note:
  Each job calls refresh_engine.refresh_migration(workbook_name) which
  handles ALL migration types — so future Power BI or other migrations are
  automatically scheduled the same way just by being registered.
"""

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval     import IntervalTrigger
from apscheduler.events                import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

log = logging.getLogger("scheduler")

_scheduler: BackgroundScheduler | None = None


# ─────────────────────────────────────────────────────────────
# Lifecycle
# ─────────────────────────────────────────────────────────────

def start(records: list[dict]) -> None:
    """
    Start the background scheduler and register one job per registry record.
    Called once from main.py lifespan startup.

    Only jobs with enable_scheduled_refresh=True AND refresh_interval_minutes set are added.

    Args:
        records: list of registry records (from registry.get_all())
    """
    global _scheduler

    if _scheduler and _scheduler.running:
        log.warning("Scheduler already running — skipping start()")
        return

    _scheduler = BackgroundScheduler(timezone="UTC")
    _scheduler.add_listener(_on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    job_count = 0
    for record in records:
        if _add_job(record):
            job_count += 1

    _scheduler.start()
    log.info(f"⏰ Scheduler started — {job_count} job(s) registered")
    print(f"\n⏰ Scheduler started — {job_count} refresh job(s) active\n")


def shutdown() -> None:
    """Gracefully stop the scheduler. Called from main.py lifespan shutdown."""
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        print("⏰ Scheduler stopped")


def add_or_replace_job(record: dict) -> None:
    """
    Register or update the refresh job for a single workbook.
    Safe to call after /migrate (new workbook) or interval update.
    
    If enable_scheduled_refresh is False or interval is None, removes any existing job.
    """
    if _scheduler is None or not _scheduler.running:
        log.warning("Scheduler not running — job not added for '%s'", record["workbook_name"])
        return
    
    workbook_name = record["workbook_name"]
    
    # If refresh is disabled or no interval set, remove the job
    if not record.get("enable_scheduled_refresh", False) or record.get("refresh_interval_minutes") is None:
        job_id = _job_id(workbook_name)
        if _scheduler.get_job(job_id):
            _scheduler.remove_job(job_id)
            print(f"⏰ Scheduler: removed job for '{workbook_name}' (refresh disabled)")
        return
    
    _add_job(record)


def remove_job(workbook_name: str) -> None:
    """Remove the refresh job for a workbook (called on DELETE /state)."""
    if _scheduler is None:
        return
    job_id = _job_id(workbook_name)
    if _scheduler.get_job(job_id):
        _scheduler.remove_job(job_id)
        print(f"⏰ Scheduler: removed job for '{workbook_name}'")


def list_jobs() -> list[dict]:
    """Return summary of all scheduled jobs (for /status responses)."""
    if _scheduler is None:
        return []
    jobs = []
    for job in _scheduler.get_jobs():
        jobs.append({
            "job_id":   job.id,
            "name":     job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger":  str(job.trigger),
        })
    return jobs


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────

def _job_id(workbook_name: str) -> str:
    return f"refresh__{workbook_name}"


def _add_job(record: dict) -> bool:
    """
    Add or replace a job in the scheduler for this record.
    
    Returns:
        True if job was added, False if skipped (refresh disabled or no interval).
    """
    workbook_name = record["workbook_name"]
    
    # Skip if refresh is disabled or no interval configured
    if not record.get("enable_scheduled_refresh", False):
        log.debug(f"Skipping job for '{workbook_name}' — refresh disabled")
        return False
    
    interval_min = record.get("refresh_interval_minutes")
    if interval_min is None:
        log.debug(f"Skipping job for '{workbook_name}' — no interval configured")
        return False
    
    job_id = _job_id(workbook_name)

    if _scheduler.get_job(job_id):
        _scheduler.remove_job(job_id)

    _scheduler.add_job(
        func     = _run_refresh,
        trigger  = IntervalTrigger(minutes=interval_min),
        id       = job_id,
        name     = f"Refresh: {workbook_name}",
        args     = [workbook_name],
        replace_existing = True,
        misfire_grace_time = 300,   # allow up to 5min late start
    )
    print(f"   ⏰ Scheduled '{workbook_name}' every {interval_min} min")
    return True


def _run_refresh(workbook_name: str) -> None:
    """Job function — wraps refresh_engine to catch and log all errors."""
    try:
        from utils.refresh_engine import refresh_migration
        result = refresh_migration(workbook_name)
        status = result.get("status", "unknown")
        tables = len(result.get("tables_refreshed", []))
        print(f"⏰ Scheduled refresh '{workbook_name}': {status} ({tables} tables)")
    except Exception as e:
        log.error("Scheduled refresh failed for '%s': %s", workbook_name, e, exc_info=True)
        print(f"⏰ Scheduled refresh FAILED for '{workbook_name}': {e}")


def _on_job_event(event) -> None:
    """APScheduler event listener — log errors from any job."""
    if event.exception:
        log.error("Job '%s' raised an exception: %s", event.job_id, event.exception)