"""Every machine-readable error code the service returns (api.md §4) — the one catalog.

Node maps these to user-facing text through its friendly-messaging layer, so a code is a
contract: add new ones here, never rename one.
"""

from enum import StrEnum


class ErrorCode(StrEnum):
    # Generic (the backend's AppError family)
    INTERNAL_ERROR = "INTERNAL_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    NOT_FOUND = "NOT_FOUND"
    ROUTE_NOT_FOUND = "ROUTE_NOT_FOUND"
    METHOD_NOT_ALLOWED = "METHOD_NOT_ALLOWED"
    CONFLICT = "CONFLICT"

    # Writer protocol (consistency.md)
    FENCED = "FENCED"

    # Data Service domain (api.md §4)
    UNSUPPORTED_FORMAT = "UNSUPPORTED_FORMAT"
    FILE_TOO_LARGE = "FILE_TOO_LARGE"
    R_FILE_TOO_LARGE_FOR_MEMORY = "R_FILE_TOO_LARGE_FOR_MEMORY"
    ENCODING_UNDETECTED = "ENCODING_UNDETECTED"
    RDATA_MULTIPLE_OBJECTS = "RDATA_MULTIPLE_OBJECTS"
    SCHEMA_CONFLICT = "SCHEMA_CONFLICT"
    DATASET_BUSY = "DATASET_BUSY"
    DUPLICATE_UNIQUE_ID = "DUPLICATE_UNIQUE_ID"
    ROW_GONE = "ROW_GONE"
    REVISION_EXPIRED = "REVISION_EXPIRED"
    UNDO_CONFLICT = "UNDO_CONFLICT"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    OPERATION_FAILED = "OPERATION_FAILED"
    IDEMPOTENCY_MISMATCH = "IDEMPOTENCY_MISMATCH"
    TOO_LATE = "TOO_LATE"
    FORM_DELETED = "FORM_DELETED"
