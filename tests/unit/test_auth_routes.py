"""Every route is guarded, and a dataset route accepts only Node's token for that dataset and
an allowed scope (api.md §2)."""

import time
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from dataservice.common.auth import require_dataset_token
from dataservice.common.errors import register_error_handlers
from dataservice.common.utils import dataset_token
from dataservice.common.utils.dataset_token import DatasetClaims
from dataservice.main import app
from tests.conftest import SECRET


def _routes() -> list[tuple[str, object]]:
    """(path, dependant) of every route — FastAPI 0.16x wraps an included router, so look inside."""
    found: list[tuple[str, object]] = []
    for route in app.routes:
        if isinstance(route, APIRoute):
            found.append((route.path, route.dependant))
        contexts = getattr(route, "effective_route_contexts", None)
        for context in contexts() if callable(contexts) else []:
            found.append((str(getattr(context, "path", "")), getattr(context, "dependant", None)))
    return found


def _guards(dependant: object) -> list[str]:
    names: list[str] = []
    for child in getattr(dependant, "dependencies", []):
        names.append(getattr(getattr(child, "call", None), "__qualname__", ""))
        names += _guards(child)
    return [name for name in names if "require_" in name]


def test_no_route_of_the_service_is_reachable_without_a_token() -> None:
    routes = _routes()
    assert "/health" in [path for path, _ in routes]
    assert [path for path, dependant in routes if not _guards(dependant)] == []


def test_the_service_serves_no_public_docs() -> None:
    client = TestClient(app)
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert client.get(path).status_code == 404


def _probe_app() -> TestClient:
    router = APIRouter()

    @router.get("/v1/datasets/{dataset_id}/probe")
    def probe(claims: Annotated[DatasetClaims, Depends(require_dataset_token("read"))]) -> dict[str, str]:
        return {"user": claims.user_id, "rows": claims.row_scope}

    probe_app = FastAPI()
    register_error_handlers(probe_app)
    probe_app.include_router(router)
    return TestClient(probe_app)


def _bearer(dataset_id: str = "d1", scope: str = "read") -> dict[str, str]:
    claims = DatasetClaims(
        user_id="u1", dataset_id=dataset_id, scope="read" if scope == "read" else "write", row_scope="u1"
    )
    return {"Authorization": f"Bearer {dataset_token.sign(SECRET, claims, int(time.time()))}"}


def test_a_dataset_route_takes_the_token_for_that_dataset() -> None:
    response = _probe_app().get("/v1/datasets/d1/probe", headers=_bearer())
    assert response.status_code == 200 and response.json() == {"user": "u1", "rows": "u1"}


def test_another_datasets_token_or_scope_is_forbidden() -> None:
    client = _probe_app()
    assert client.get("/v1/datasets/d2/probe", headers=_bearer("d1")).status_code == 403
    assert client.get("/v1/datasets/d1/probe", headers=_bearer(scope="write")).status_code == 403


def test_no_token_or_a_bad_one_is_unauthorized() -> None:
    client = _probe_app()
    assert client.get("/v1/datasets/d1/probe").status_code == 401
    assert client.get("/v1/datasets/d1/probe", headers={"Authorization": "Basic abc"}).status_code == 401
    assert client.get("/v1/datasets/d1/probe", headers={"Authorization": "Bearer nope"}).status_code == 401
