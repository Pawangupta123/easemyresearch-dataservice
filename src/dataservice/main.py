"""App entry point: settings, logging, error handlers, routers (Node's main.js + app.routes.ts).

Run: `uv run granian --interface asgi dataservice.main:app --port 8100`
"""

from fastapi import Depends, FastAPI

from dataservice.common.auth import require_service_token
from dataservice.common.errors import register_error_handlers
from dataservice.config import get_settings
from dataservice.config.logging import configure_logging
from dataservice.modules.datasets.router import router as datasets_router
from dataservice.modules.health.router import router as health_router
from dataservice.modules.ingest.router import router as ingest_router
from dataservice.modules.writer.router import router as writer_router
from dataservice.storage.reader import ReaderLimits, configure


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings)
    configure(ReaderLimits(settings.duckdb_threads, settings.duckdb_memory_limit, settings.duckdb_temp_directory))
    # Every route declares a Pydantic return type: FastAPI then serialises straight to JSON
    # bytes in Pydantic's Rust core — faster than a custom (orjson) response class.
    # Each router carries its own auth — /health the scaffold token, /v1/datasets/... the scoped
    # dataset token (common/auth.py); tests/unit/test_auth_routes.py fails on a route with none.
    # No public docs: the contract is exported by scripts/export_contract.py, not served.
    app = FastAPI(title="EaseMyResearch Data Service", docs_url=None, redoc_url=None, openapi_url=None)
    register_error_handlers(app)
    app.include_router(health_router, dependencies=[Depends(require_service_token)])
    app.include_router(datasets_router)  # each route takes the scoped dataset token itself
    app.include_router(writer_router)  # Node's 'job' token; the writer then signs its calls to Node
    app.include_router(ingest_router)  # Node's 'job' token for the dataset reserved for an upload
    return app


app = create_app()
