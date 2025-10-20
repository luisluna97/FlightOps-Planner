"""
Central logging configuration helpers.

We keep logging opinionated but lightweight: logs are structured enough for
observability while still being readable when run locally.
"""

from __future__ import annotations

import logging
import sys
from typing import Optional


def configure_logging(level: int = logging.INFO, *, module: Optional[str] = None) -> None:
    """
    Configure a root logger with a consistent formatter.

    Parameters
    ----------
    level:
        The minimum logging level to emit.
    module:
        Optional name of the module (used when individual entrypoints want a
        custom logger namespace).
    """

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)

    logger = logging.getLogger(module)
    logger.setLevel(level)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False


__all__ = ["configure_logging"]
