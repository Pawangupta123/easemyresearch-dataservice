"""Every error leaves in the backend's envelope: { success: false, message, errors?, data: { code } }."""

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from dataservice.common.error_codes import ErrorCode
from dataservice.common.errors import (
    GENERIC_ERROR_MESSAGE,
    AppError,
    ConflictError,
    NotFoundError,
    register_error_handlers,
)
from dataservice.common.responses import ApiResponse, ok


class RowsIn(BaseModel):
    rows: list[int]


def _app() -> FastAPI:
    app = FastAPI()
    register_error_handlers(app)

    @app.get("/missing")
    def missing() -> None:
        raise NotFoundError("Dataset not found")

    @app.get("/duplicate")
    def duplicate() -> None:
        raise ConflictError("Patient ID P-12 already exists", code=ErrorCode.DUPLICATE_UNIQUE_ID)

    @app.get("/technical")
    def technical() -> None:
        raise ConflictError("delta log at s3://bucket/x version 7 is corrupt", user_safe=False)

    @app.get("/raw-app-error")
    def raw_app_error() -> None:
        raise AppError("pyarrow buffer overflow in column f_12")

    @app.get("/crash")
    def crash() -> None:
        raise RuntimeError("secret internal detail")

    @app.post("/rows")
    def rows(body: RowsIn) -> ApiResponse[int]:
        return ok(len(body.rows))

    return app


client = TestClient(_app(), raise_server_exceptions=False)


def test_a_user_safe_error_shows_its_message_and_code() -> None:
    response = client.get("/missing")
    assert response.status_code == 404
    assert response.json() == {"success": False, "message": "Dataset not found", "data": {"code": "NOT_FOUND"}}


def test_a_domain_code_travels_with_the_error() -> None:
    response = client.get("/duplicate")
    assert response.status_code == 409
    assert response.json()["data"] == {"code": "DUPLICATE_UNIQUE_ID"}
    assert response.json()["message"] == "Patient ID P-12 already exists"


def test_a_technical_message_is_never_shown() -> None:
    for path in ("/technical", "/raw-app-error"):
        body = client.get(path).json()
        assert body["message"] == GENERIC_ERROR_MESSAGE
        assert "s3://" not in str(body) and "pyarrow" not in str(body)


def test_an_unexpected_crash_is_a_generic_500() -> None:
    response = client.get("/crash")
    assert response.status_code == 500
    assert response.json() == {"success": False, "message": GENERIC_ERROR_MESSAGE, "data": {"code": "INTERNAL_ERROR"}}


def test_an_invalid_request_lists_field_errors_like_the_backend() -> None:
    response = client.post("/rows", json={"rows": [1, "x"]})
    assert response.status_code == 400
    body = response.json()
    assert body["success"] is False and body["message"] == "Validation failed"
    assert body["data"] == {"code": "VALIDATION_ERROR"}
    assert [error["field"] for error in body["errors"]] == ["rows.1"]


def test_a_valid_request_gets_the_success_envelope() -> None:
    assert client.post("/rows", json={"rows": [1, 2]}).json() == {"success": True, "message": "Success", "data": 2}


def test_unknown_routes_and_methods() -> None:
    not_found = client.get("/nope")
    assert not_found.status_code == 404
    assert not_found.json() == {
        "success": False,
        "message": "Route not found: GET /nope",
        "data": {"code": "ROUTE_NOT_FOUND"},
    }
    wrong_method = client.delete("/missing")
    assert wrong_method.status_code == 405
    assert wrong_method.json()["data"] == {"code": "METHOD_NOT_ALLOWED"}
