"""Utility functions for HPC Sweep Manager."""

import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path


def setup_logging(level: str = "INFO", log_file: Path | None = None) -> logging.Logger:
    """Set up logging configuration."""

    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")

    # Create logger
    logger = logging.getLogger("hpc_sweep_manager")
    logger.setLevel(numeric_level)

    # Clear any existing handlers
    logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler if specified
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def create_sweep_id(prefix: str = "sweep") -> str:
    """Create a unique sweep ID with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}"


def task_index(name: str) -> int | None:
    """The task number in a ``tasks/`` dir name, or None for any other name. Each source names
    its dirs its own way: ``task_7`` (array), ``task_007`` (local, ssh), ``<sweep_id>_task_007``
    (individual Slurm jobs); consumers' scripts read these names, so readers parse all three."""
    m = re.fullmatch(r"(?:.+_)?task_(\d+)", name)
    return int(m[1]) if m else None


def parse_walltime(walltime: str) -> int:
    """Seconds in a Slurm time: ``M``, ``M:S``, ``H:M:S``, ``D-H``, ``D-H:M`` or ``D-H:M:S``."""
    days, _, rest = walltime.rpartition("-")
    try:
        parts = [int(p) for p in rest.split(":")]
        days_s = int(days or 0) * 86400
    except ValueError:
        raise ValueError(f"Invalid walltime format: {walltime}") from None
    if len(parts) > 3:
        raise ValueError(f"Invalid walltime format: {walltime}")
    if days:  # D-H, D-H:M, D-H:M:S
        h, m, s = (parts + [0, 0])[:3]
    elif len(parts) == 1:  # M
        h, m, s = 0, parts[0], 0
    else:  # M:S, H:M:S
        h, m, s = ([0] + parts)[-3:]
    return days_s + h * 3600 + m * 60 + s


def format_walltime(seconds: int) -> str:
    """Format seconds to walltime string (HH:MM:SS)."""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def write_atomic(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` through a temp file in its dir and a rename: a reader never
    sees half a file, and a write failing midway leaves the old one."""
    tmp = path.with_name(f"{path.name}.tmp")
    try:
        tmp.write_text(text)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
