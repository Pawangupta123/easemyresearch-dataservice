import time
from typing import Annotated

from fastapi import Depends, Header

from dataservice.common.errors import UnauthorizedError
from dataservice.common.utils import service_token
from dataservice.config import Settings, get_settings


def require_service_token(
    settings: Annotated[Settings, Depends(get_settings)],
    x_service_token: Annotated[str | None, Header()] = None,
) -> None:
    """Every route requires a short-lived token signed by Node."""
    if not x_service_token or not service_token.verify(
        settings.service_token_secret, x_service_token, int(time.time()), settings.service_token_max_age_seconds
    ):
        raise UnauthorizedError("Invalid service token")
