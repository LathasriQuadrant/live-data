"""Health check endpoint."""

from fastapi import APIRouter
from config import FABRIC_WORKSPACE_ID, AZURE_BLOB_CONTAINER

router = APIRouter(tags=["Health"])

@router.get("/health")
def health():
    return {
        "status":  "ok",
        "service": "Tableau → Fabric Lakehouse API",
        "version": "1.0.0",
        "config": {
            "fabric_workspace_id": FABRIC_WORKSPACE_ID or "⚠️ not set",
            "blob_container":      AZURE_BLOB_CONTAINER,
        },
    }
