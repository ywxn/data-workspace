"""
Centralized logging configuration for the AI Data Workspace application.

Provides a unified logging interface across all modules with consistent
formatting and level handling.
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from constants import LOG_DIR_NAME, LOG_FILE_NAME, LOG_LEVEL_DEFAULT

# Create logs directory if it doesn't exist
LOG_DIR = Path(LOG_DIR_NAME)
LOG_DIR.mkdir(exist_ok=True)

# Logging level may be provided as a string in constants; resolve to numeric level
LOG_LEVEL = getattr(logging, LOG_LEVEL_DEFAULT, logging.INFO)
LOG_FILE = LOG_DIR / LOG_FILE_NAME
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger with the specified name.

    Args:
        name: Module name (typically __name__)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(LOG_LEVEL)

        # File handler
        file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
        file_handler.setLevel(LOG_LEVEL)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
        logger.addHandler(file_handler)

        # Console handler (shows INFO level and above for visibility)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
        logger.addHandler(console_handler)

    return logger


def set_log_level(level: int) -> None:
    """
    Set the global logging level.

    Args:
        level: Logging level (e.g., logging.DEBUG, logging.INFO)
    """
    logging.getLogger().setLevel(level)
    for handler in logging.getLogger().handlers:
        handler.setLevel(level)
