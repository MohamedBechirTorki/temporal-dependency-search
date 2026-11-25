"""Structured logging setup."""
from __future__ import annotations

import logging
import sys

try:
    from pythonjsonlogger.json import JsonFormatter  # python-json-logger >= 3
except ImportError:  # pragma: no cover
    from pythonjsonlogger.jsonlogger import JsonFormatter  # type: ignore


def configure_logging(level: str = "INFO", json_output: bool = True) -> None:
    handler = logging.StreamHandler(sys.stdout)
    if json_output:
        handler.setFormatter(
            JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s", rename_fields={"asctime": "ts"})
        )
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    # Quiet noisy drivers
    logging.getLogger("neo4j").setLevel(logging.WARNING)
    logging.getLogger("pymongo").setLevel(logging.WARNING)
