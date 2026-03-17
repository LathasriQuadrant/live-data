"""
Tableau → Microsoft Fabric Lakehouse API

Endpoints:
  POST   /api/v1/lakehouse/migrate                         Migrate workbook from blob to Lakehouse
  GET    /api/v1/lakehouse/status                          List all migrated workbooks
  GET    /api/v1/lakehouse/status/{workbook_name}          Detail for one workbook
  DELETE /api/v1/lakehouse/state/{workbook_name}           Remove state record + cancel scheduler job
  POST   /api/v1/lakehouse/refresh/{workbook_name}         Manually trigger refresh for one workbook
  POST   /api/v1/lakehouse/refresh/all                     Manually trigger refresh for all workbooks
  PATCH  /api/v1/lakehouse/refresh/{workbook_name}/schedule  Update refresh interval (minutes)
  PATCH  /api/v1/lakehouse/refresh/{workbook_name}/powerbi   Attach Power BI dataset ID
  GET    /api/v1/lakehouse/refresh/jobs                    List all scheduled refresh jobs
  GET    /health                                           Health check
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes import migrate_router, status_router, health_router, refresh_router
import utils.registry  as registry
import utils.scheduler as scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the refresh scheduler on boot; shut it down cleanly on exit."""
    print("\n🚀 Starting Tableau → Lakehouse API...")

    # Load persisted registry and register a refresh job for every record
    all_records = registry.get_all()
    print(f"   📂 Loaded {len(all_records)} migration record(s) from registry")
    scheduler.start(all_records)

    yield  # ── app is running ──

    scheduler.shutdown()
    print("👋 API shut down cleanly")


app = FastAPI(
    title       = "Tableau → Fabric Lakehouse API",
    description = (
        "Migrates Tableau workbooks (.twbx) from Azure Blob Storage into "
        "Microsoft Fabric Lakehouses as Delta tables.\n\n"
        "Supports **Extracted** (.hyper) and **Live** sources "
        "(Snowflake, Azure SQL, Databricks).\n\n"
        "**Automatic refresh**: every migration is re-synced on a configurable "
        "interval (default 60 min). After each Lakehouse refresh, a linked "
        "Power BI dataset refresh is triggered automatically if a dataset ID "
        "has been registered."
    ),
    version  = "2.0.0",
    docs_url = "/docs",
    redoc_url= "/redoc",
    lifespan = lifespan,
)

# Add CORS FIRST, before routes
app.add_middleware(
    CORSMiddleware,
    allow_origins = [
    "https://id-preview--1115fb10-6ea8-4052-8d1b-31238016c02e.lovable.app",
    "https://reportmigration-frontend-g9ceape5ddgxa5gq.eastus-01.azurewebsites.net"
    ],
    allow_credentials = True,
    allow_methods     = ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allow_headers     = ["*"],
)

# Then include routers
app.include_router(migrate_router)
app.include_router(status_router)
app.include_router(health_router)
app.include_router(refresh_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000)
