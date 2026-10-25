"""Domain errors and the ONE place any error becomes an HTTP response (the backend's
common/errors + middleware/error-handler.ts).

Deny-by-default, as in the backend: only a user-safe AppError shows its own message; anything
else answers GENERIC_ERROR_MESSAGE and the real cause is always logged in full.
"""

import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from dataservice.common.error_codes import ErrorCode
from dataservice.common.responses import ApiErrorResponse, ErrorData, FieldError

GENERIC_ERROR_MESSAGE = "Something went wrong. Please try again."

log = structlog.get_logger()


class AppError(Exception):
    status_code = 500
    code = ErrorCode.INTERNAL_ERROR
    default_message = GENERIC_ERROR_MESSAGE

    def __init__(
        self,
        message: str | None = None,
        *,
        code: ErrorCode | None = None,
        errors: list[FieldError] | None = None,
        user_safe: bool = True,
    ) -> None:
        self.message = message or self.default_message
        super().__init__(self.message)
        if code is not None:
            self.code = code
        self.errors = errors
        # Whether `message` is written for the reader and safe to show. The base class is
        # unsafe (a raw failure); the subclasses carry hand-written domain text.
        self.user_safe = user_safe and type(self) is not AppError


class ValidationError(AppError):
    status_code = 400
    code = ErrorCode.VALIDATION_ERROR
    default_message = "Validation failed"


class UnauthorizedError(AppError):
    status_code = 401
    code = ErrorCode.UNAUTHORIZED
    default_message = "Unauthorized"


class ForbiddenError(AppError):
    status_code = 403
    code = ErrorCode.FORBIDDEN
    default_message = "Forbidden"


class NotFoundError(AppError):
    status_code = 404
    code = ErrorCode.NOT_FOUND
    default_message = "Resource not found"


class ConflictError(AppError):
    status_code = 409
    code = ErrorCode.CONFLICT
    default_message = "Conflict"


class OperationFailedError(ConflictError):
    """Node finished the operation as failed (its attempts ran out — a poison operation)."""

    code = ErrorCode.OPERATION_FAILED
    default_message = "The operation failed"


class FenceError(ConflictError):
    """A newer writer has fenced this table; the current writer must stop (consistency §4.2)."""

    code = ErrorCode.FENCED
    default_message = "A newer writer holds this dataset"


def _respond(status_code: int, message: str, code: ErrorCode, errors: list[FieldError] | None = None) -> JSONResponse:
    body = ApiErrorResponse(message=message, errors=errors, data=ErrorData(code=code)).body()
    return JSONResponse(status_code=status_code, content=body)


def _field_errors(error: RequestValidationError) -> list[FieldError]:
    """Pydantic's error list → the backend's `{ field, message }`, field as a dotted path
    without the request part (`body.rows.0.value` → `rows.0.value`)."""
    found = []
    for item in error.errors():
        location = [str(part) for part in item.get("loc", ())]
        if location and location[0] in {"body", "query", "path", "header"}:
            location = location[1:]
        found.append(FieldError(field=".".join(location) or "request", message=str(item.get("msg", "Invalid value"))))
    return found


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def _app_error(_: Request, error: AppError) -> JSONResponse:
        if error.user_safe:
            log.warning("request failed", status=error.status_code, code=error.code, reason=error.message)
            return _respond(error.status_code, error.message, error.code, error.errors)
        log.warning("request failed (client saw a generic message)", status=error.status_code, reason=error.message)
        return _respond(error.status_code, GENERIC_ERROR_MESSAGE, error.code)

    @app.exception_handler(RequestValidationError)
    async def _invalid_request(_: Request, error: RequestValidationError) -> JSONResponse:
        return _respond(400, "Validation failed", ErrorCode.VALIDATION_ERROR, _field_errors(error))

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(request: Request, error: StarletteHTTPException) -> JSONResponse:
        if error.status_code == 404:
            message = f"Route not found: {request.method} {request.url.path}"
            return _respond(404, message, ErrorCode.ROUTE_NOT_FOUND)
        if error.status_code == 405:
            message = f"Method not allowed: {request.method} {request.url.path}"
            return _respond(405, message, ErrorCode.METHOD_NOT_ALLOWED)
        log.warning("http error", status=error.status_code, reason=error.detail)
        return _respond(error.status_code, GENERIC_ERROR_MESSAGE, ErrorCode.INTERNAL_ERROR)

    @app.exception_handler(Exception)
    async def _unexpected(_: Request, error: Exception) -> JSONResponse:
        log.exception("unhandled error", error_type=type(error).__name__)
        return _respond(500, GENERIC_ERROR_MESSAGE, ErrorCode.INTERNAL_ERROR)
