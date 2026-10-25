import time
from collections.abc import Callable
from typing import Annotated

from fastapi import Depends, Header, Path

from dataservice.common.errors import ForbiddenError, UnauthorizedError
from dataservice.common.utils import dataset_token, service_token
from dataservice.common.utils.dataset_token import DatasetClaims, Scope
from dataservice.config import Settings, get_settings


def require_service_token(
    settings: Annotated[Settings, Depends(get_settings)],
    x_service_token: Annotated[str | None, Header()] = None,
) -> None:
    """The scaffold token (`/health`): issued-at + HMAC, signed by Node."""
    if not x_service_token or not service_token.verify(
        settings.service_token_secret, x_service_token, int(time.time()), settings.service_token_max_age_seconds
    ):
        raise UnauthorizedError("Invalid service token")


def require_dataset_token(*scopes: Scope) -> Callable[..., DatasetClaims]:
    """A route on `/v1/datasets/{dataset_id}/…`: Node's scoped token (api.md §2), for this very
    dataset and one of `scopes`. Returns the claims — the row scope travels with them."""

    def dependency(
        settings: Annotated[Settings, Depends(get_settings)],
        dataset_id: Annotated[str, Path()],
        authorization: Annotated[str | None, Header()] = None,
    ) -> DatasetClaims:
        kind, _, token = (authorization or "").partition(" ")
        if kind != "Bearer" or not token:
            raise UnauthorizedError("Invalid service token")
        try:
            claims = dataset_token.verify(settings.service_token_secret, token, int(time.time()))
        except dataset_token.InvalidDatasetToken:
            raise UnauthorizedError("Invalid service token") from None
        if claims.dataset_id != dataset_id or claims.scope not in scopes:
            raise ForbiddenError("This token is not for this dataset or action")
        return claims

    return dependency
