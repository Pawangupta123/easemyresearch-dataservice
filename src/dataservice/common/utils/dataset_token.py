"""The scoped token Node signs for every Data Service call on a dataset (api.md §2).

A short-lived HS256 JWT: `{ sub: userId, datasetId, scope: read|write|job, rowScope: all|<userId>,
iat, exp }`, valid at most `MAX_LIFETIME_SECONDS`. Pure functions — no I/O.
"""

from dataclasses import dataclass
from typing import Literal

import jwt

Scope = Literal["read", "write", "job"]
ALGORITHM = "HS256"
MAX_LIFETIME_SECONDS = 60
CLOCK_SKEW_SECONDS = 30
ALL_ROWS = "all"
_SCOPES: dict[str, Scope] = {"read": "read", "write": "write", "job": "job"}


class InvalidDatasetToken(Exception):
    """The token is missing, forged, expired or malformed — never accept the call."""


@dataclass(frozen=True)
class DatasetClaims:
    user_id: str
    dataset_id: str
    scope: Scope
    row_scope: str  # ALL_ROWS, or the only user whose rows the call may see


def sign(secret: str, claims: DatasetClaims, issued_at: int, lifetime_seconds: int = MAX_LIFETIME_SECONDS) -> str:
    """How Node signs (its service-token signer mirrors this) — used by tests and probes."""
    payload = {
        "sub": claims.user_id,
        "datasetId": claims.dataset_id,
        "scope": claims.scope,
        "rowScope": claims.row_scope,
        "iat": issued_at,
        "exp": issued_at + lifetime_seconds,
    }
    return jwt.encode(payload, secret, algorithm=ALGORITHM)


def verify(secret: str, token: str, now: int) -> DatasetClaims:
    """The token's claims, or InvalidDatasetToken. Times are checked against `now` here (with
    CLOCK_SKEW_SECONDS of slack) rather than by the library's own clock, so tests control time."""
    try:
        payload = jwt.decode(
            token,
            secret,
            algorithms=[ALGORITHM],  # never "none", never an algorithm the token names itself
            options={
                "require": ["sub", "datasetId", "scope", "rowScope", "iat", "exp"],
                "verify_exp": False,
                "verify_iat": False,
                "verify_nbf": False,
            },
        )
    except jwt.PyJWTError as error:
        raise InvalidDatasetToken(str(error)) from None
    issued_at, expires_at = payload["iat"], payload["exp"]
    if isinstance(issued_at, bool) or isinstance(expires_at, bool):
        raise InvalidDatasetToken("iat and exp must be whole seconds")
    if not isinstance(issued_at, int) or not isinstance(expires_at, int):
        raise InvalidDatasetToken("iat and exp must be whole seconds")
    if expires_at - issued_at > MAX_LIFETIME_SECONDS:
        raise InvalidDatasetToken("token lives longer than allowed")
    if issued_at > now + CLOCK_SKEW_SECONDS:
        raise InvalidDatasetToken("token issued in the future")
    if expires_at + CLOCK_SKEW_SECONDS < now:
        raise InvalidDatasetToken("token expired")
    scope = _SCOPES.get(payload["scope"]) if isinstance(payload["scope"], str) else None
    user_id, dataset_id, row_scope = payload["sub"], payload["datasetId"], payload["rowScope"]
    if scope is None or not all(isinstance(value, str) and value for value in (user_id, dataset_id, row_scope)):
        raise InvalidDatasetToken("malformed claims")
    if row_scope not in (ALL_ROWS, user_id):
        raise InvalidDatasetToken("a row scope is all rows or the caller's own")
    return DatasetClaims(user_id=str(user_id), dataset_id=str(dataset_id), scope=scope, row_scope=str(row_scope))
