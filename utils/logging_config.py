"""Structured JSON logging with request_id tracking.

Uses only Python's built-in ``logging`` module — no external dependencies.
"""

import json
import logging
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone

# Context variable that handlers/middleware can set per-request.
request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)


def generate_request_id() -> str:
    """Return a short unique identifier suitable for log correlation."""
    return uuid.uuid4().hex[:12]


class JSONFormatter(logging.Formatter):
    """Formats every log record as a single-line JSON object.

    Output fields:
        timestamp  – ISO-8601 in UTC
        level      – e.g. INFO, WARNING, ERROR
        logger     – logger name
        message    – the formatted log message
        request_id – value from the context variable (omitted when ``None``)

    Any *extra* keys attached to the record (via ``logger.info("…", extra={…})``)
    are merged into the top level of the JSON object.
    """

    # Keys that belong to the standard LogRecord and should NOT be forwarded
    # as user-supplied extras.
    _BUILTIN_ATTRS = frozenset(
        {
            "args",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "message",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "taskName",
            "thread",
            "threadName",
        }
    )

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Attach request_id when available.
        rid = request_id_var.get()
        if rid is not None:
            log_entry["request_id"] = rid

        # Merge any user-supplied extras.
        for key, value in record.__dict__.items():
            if key not in self._BUILTIN_ATTRS and key not in log_entry:
                log_entry[key] = value

        # Include exception info when present.
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str, ensure_ascii=False)


def setup_logging(level: int = logging.INFO) -> None:
    """Configure the root logger with the JSON formatter.

    Replaces any existing handlers on the root logger so that all loggers
    (including third-party libraries) emit structured JSON to stderr.
    """
    root = logging.getLogger()
    root.setLevel(level)

    # Remove pre-existing handlers to avoid duplicate output.
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Return a logger that inherits the JSON formatter from the root.

    Usage::

        from utils.logging_config import get_logger
        logger = get_logger(__name__)
        logger.info("something happened", extra={"user_id": 42})
    """
    return logging.getLogger(name)
