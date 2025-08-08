import logging
import os
from typing import Optional


def setup_logging(level: Optional[str] = None) -> None:
    """
    Configure root logger with a simple, consistent formatter.
    Idempotent: safe to call multiple times.
    """
    resolved_level = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    if logging.getLogger().handlers:
        # Already configured; just set level
        logging.getLogger().setLevel(resolved_level)
        return

    logging.basicConfig(
        level=resolved_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


