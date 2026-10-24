import logging

import structlog

from dataservice.config.settings import Settings


def configure_logging(settings: Settings) -> None:
    """JSON logs in deployed environments (Loki), readable console logs locally."""
    renderer = structlog.processors.JSONRenderer() if settings.log_json else structlog.dev.ConsoleRenderer()
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelNamesMapping()[settings.log_level.upper()]),
    )
