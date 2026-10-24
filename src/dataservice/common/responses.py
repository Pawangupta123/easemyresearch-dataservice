"""The response envelope — identical to the backend's sendSuccess / sendError.

    success: { "success": true,  "message": "Success", "data": … }
    error:   { "success": false, "message": "…", "errors"?: [{ field, message }], "data"?: { "code": … } }

Routes return `ApiResponse[T]` (built with `ok`); errors are produced only by the handlers in
common/errors.py, so no route ever shapes an error itself.
"""

from pydantic import BaseModel

from dataservice.common.error_codes import ErrorCode


class FieldError(BaseModel):
    field: str
    message: str


class ApiResponse[T](BaseModel):
    success: bool = True
    message: str = "Success"
    data: T


class ErrorData(BaseModel):
    code: ErrorCode


class ApiErrorResponse(BaseModel):
    success: bool = False
    message: str
    errors: list[FieldError] | None = None
    data: ErrorData

    def body(self) -> dict[str, object]:
        """JSON body without the absent optional keys, like the backend's spread."""
        return self.model_dump(mode="json", exclude_none=True)


def ok[T](data: T, message: str = "Success") -> ApiResponse[T]:
    return ApiResponse[T](message=message, data=data)
