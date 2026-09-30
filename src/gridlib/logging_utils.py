"""Structured (JSON) logging shared by every stage of the pipeline."""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str, stage: str):
        super().__init__()
        self.service = service
        self.stage = stage

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "service": self.service,
            "stage": self.stage,
            "event": record.getMessage(),
        }
        payload.update(getattr(record, "ctx", {}))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class StructuredLogger:
    """Thin wrapper so call sites read `log.info("event_name", key=value)`."""

    def __init__(self, logger: logging.Logger):
        self._logger = logger

    def _log(self, level: int, event: str, exc_info: bool = False, **ctx: Any) -> None:
        self._logger.log(level, event, extra={"ctx": ctx}, exc_info=exc_info)

    def debug(self, event: str, **ctx: Any) -> None:
        self._log(logging.DEBUG, event, **ctx)

    def info(self, event: str, **ctx: Any) -> None:
        self._log(logging.INFO, event, **ctx)

    def warning(self, event: str, **ctx: Any) -> None:
        self._log(logging.WARNING, event, **ctx)

    def error(self, event: str, **ctx: Any) -> None:
        self._log(logging.ERROR, event, **ctx)

    def exception(self, event: str, **ctx: Any) -> None:
        self._log(logging.ERROR, event, exc_info=True, **ctx)


def get_logger(service: str, stage: str, level: str = "INFO") -> StructuredLogger:
    """Return a JSON logger. `stage` is one of ingestion/processing/storage/serving."""
    logger = logging.getLogger(f"grid.{service}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter(service, stage))
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return StructuredLogger(logger)
