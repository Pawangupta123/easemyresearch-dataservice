"""App entry point: settings, logging, error handlers, routers (Node's main.js + app.routes.ts).

Run: `uv run granian --interface asgi dataservice.main:app --port 8100`
"""

from fastapi import Depends, FastAPI

from dataservice.common.auth import require_service_token
from dataservice.common.errors import register_error_handlers
from dataservice.config import get_settings
from dataservice.config.logging import configure_logging
from dataservice.modules.health.router import router as health_router
from dataservice.storage.reader import ReaderLimits, configure


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings)
    configure(ReaderLimits(settings.duckdb_threads, settings.duckdb_memory_limit, settings.duckdb_temp_directory))
    # Every route declares a Pydantic return type: FastAPI then serialises straight to JSON
    # bytes in Pydantic's Rust core — faster than a custom (orjson) response class.
    app = FastAPI(title="EaseMyResearch Data Service", dependencies=[Depends(require_service_token)])
    register_error_handlers(app)
    app.include_router(health_router)
    return app


app = create_app()
