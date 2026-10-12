"""FastAPI dependencies."""

from volmigrate.core.database import get_db

# Re-export for route handlers
__all__ = ["get_db"]
