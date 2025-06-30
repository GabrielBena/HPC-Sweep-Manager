"""Common utility functions for HSM v2."""

from datetime import datetime
from pathlib import Path
from typing import Optional
import logging


def create_sweep_id(prefix: str = "sweep") -> str:
    """Create a unique sweep ID with timestamp.

    Args:
        prefix: Prefix for the sweep ID

    Returns:
        Date-based sweep ID in format: prefix_YYYYMMDD_HHMMSS
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}"


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Human readable duration string
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def safe_filename(name: str) -> str:
    """Create a safe filename from a string.

    Args:
        name: String to convert to safe filename

    Returns:
        Safe filename string
    """
    # Replace unsafe characters
    unsafe_chars = ["/", "\\", ":", "*", "?", '"', "<", ">", "|", " "]
    safe_name = name
    for char in unsafe_chars:
        safe_name = safe_name.replace(char, "_")

    # Remove multiple consecutive underscores
    while "__" in safe_name:
        safe_name = safe_name.replace("__", "_")

    # Remove leading/trailing underscores
    safe_name = safe_name.strip("_")

    # Limit length
    if len(safe_name) > 100:
        safe_name = safe_name[:100]

    return safe_name
