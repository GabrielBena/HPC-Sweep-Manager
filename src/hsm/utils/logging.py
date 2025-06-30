"""Logging utilities for HPC Sweep Manager."""

import logging
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler


def setup_logging(
    level: str = "INFO", log_file: Optional[Path] = None, console: Optional[Console] = None
) -> logging.Logger:
    """Set up logging configuration for HSM.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional file to write logs to
        console: Optional Rich console for output

    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger("hsm")
    logger.setLevel(getattr(logging, level.upper()))

    # Clear existing handlers
    logger.handlers.clear()

    # Create Rich handler for console output
    if console is None:
        console = Console(stderr=True)

    rich_handler = RichHandler(
        console=console, show_time=True, show_path=False, markup=True, rich_tracebacks=True
    )
    rich_handler.setLevel(getattr(logging, level.upper()))

    # Format for console output
    console_formatter = logging.Formatter(fmt="%(message)s", datefmt="[%X]")
    rich_handler.setFormatter(console_formatter)
    logger.addHandler(rich_handler)

    # Add file handler if specified
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # Always log everything to file

        # More detailed format for file output
        file_formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str = "hsm") -> logging.Logger:
    """Get a logger instance.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return logging.getLogger(name)
