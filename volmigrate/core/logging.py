"""Structured logging with contextual tags and secret redaction."""

import logging
import sys
import json
from datetime import datetime, timezone
from typing import Any, Dict


class SafeJsonFormatter(logging.Formatter):
    """JSON log formatter that redacts common sensitive keys."""

    SENSITIVE_KEYS = {"password", "token", "secret", "kubeconfig", "key", "authorization"}

    def format(self, record: logging.LogRecord) -> str:
        log_obj: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add contextual attributes if attached to record
        for attr in ("migration_id", "worker_id", "step"):
            if hasattr(record, attr):
                log_obj[attr] = getattr(record, attr)

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj)


def setup_logging(log_level: str = "INFO", json_format: bool = False) -> None:
    """Configure root logger."""
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level.upper())

    handler = logging.StreamHandler(sys.stdout)
    if json_format:
        handler.setFormatter(SafeJsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s")
        )

    root_logger.handlers = [handler]
