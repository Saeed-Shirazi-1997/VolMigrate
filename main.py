"""Volmigrate main runner."""

import uvicorn
from volmigrate.api.main import app
from volmigrate.core.config import get_settings

if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "volmigrate.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
    )
