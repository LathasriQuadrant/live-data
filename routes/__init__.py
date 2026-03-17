"""Routes package — Tableau to Fabric Lakehouse"""

from .migrate import router as migrate_router
from .status  import router as status_router
from .health  import router as health_router
from .refresh import router as refresh_router

__all__ = [
    "migrate_router",
    "status_router",
    "health_router",
    "refresh_router",
]
