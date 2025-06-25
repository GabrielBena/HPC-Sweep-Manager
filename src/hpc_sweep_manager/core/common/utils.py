"""Utility functions for HPC Sweep Manager."""

from datetime import datetime
import logging
from pathlib import Path
import sys
from typing import Optional


def setup_logging(level: str = "INFO", log_file: Optional[Path] = None) -> logging.Logger:
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


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def format_memory(bytes_value: int) -> str:
    """Format memory in bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f}{unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f}PB"


def validate_resources_string(resources: str) -> bool:
    """Validate PBS/Slurm resources string format."""
    # Basic validation for PBS format: select=1:ncpus=4:mem=16gb
    if "select=" in resources and "ncpus=" in resources:
        return True
    # Basic validation for Slurm format: --nodes=1 --ntasks=4 --mem=16G
    if "--nodes=" in resources or "--ntasks=" in resources:
        return True
    return False


def parse_walltime(walltime: str) -> int:
    """Parse walltime string to seconds."""
    # Handle formats like "04:00:00", "2:30:00", "30:00"
    parts = walltime.split(":")

    if len(parts) == 3:
        hours, minutes, seconds = map(int, parts)
        return hours * 3600 + minutes * 60 + seconds
    elif len(parts) == 2:
        minutes, seconds = map(int, parts)
        return minutes * 60 + seconds
    else:
        raise ValueError(f"Invalid walltime format: {walltime}")


def format_walltime(seconds: int) -> str:
    """Format seconds to walltime string (HH:MM:SS)."""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def safe_filename(name: str) -> str:
    """Create a safe filename from a string."""
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


def count_jobs_in_queue() -> int:
    """Count the current user's jobs in the queue."""
    import os
    import subprocess

    try:
        # Try PBS first
        result = subprocess.run(
            ["qstat", "-u", os.getenv("USER")],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            # Count lines (excluding header)
            lines = result.stdout.strip().split("\n")
            return max(0, len(lines) - 2)  # Subtract header lines
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    try:
        # Try Slurm
        result = subprocess.run(
            ["squeue", "-u", os.getenv("USER")],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            # Count lines (excluding header)
            lines = result.stdout.strip().split("\n")
            return max(0, len(lines) - 1)  # Subtract header line
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    return 0


class ProgressTracker:
    """Track progress of sweep execution."""

    def __init__(self, total_jobs: int):
        self.total_jobs = total_jobs
        self.completed_jobs = 0
        self.failed_jobs = 0
        self.start_time = datetime.now()

    def update(self, completed: int, failed: int = 0):
        """Update progress counters."""
        self.completed_jobs = completed
        self.failed_jobs = failed

    def get_progress(self) -> dict:
        """Get current progress information."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        running_jobs = self.total_jobs - self.completed_jobs - self.failed_jobs

        progress_pct = (self.completed_jobs / self.total_jobs) * 100 if self.total_jobs > 0 else 0

        # Estimate time remaining
        if self.completed_jobs > 0:
            avg_time_per_job = elapsed / self.completed_jobs
            estimated_remaining = avg_time_per_job * running_jobs
        else:
            estimated_remaining = 0

        return {
            "total_jobs": self.total_jobs,
            "completed_jobs": self.completed_jobs,
            "failed_jobs": self.failed_jobs,
            "running_jobs": running_jobs,
            "progress_percent": progress_pct,
            "elapsed_time": elapsed,
            "estimated_remaining": estimated_remaining,
            "start_time": self.start_time.isoformat(),
        }

    def get_status_summary(self) -> str:
        """Get a human-readable status summary."""
        progress = self.get_progress()

        return (
            f"Progress: {progress['completed_jobs']}/{progress['total_jobs']} "
            f"({progress['progress_percent']:.1f}%) completed, "
            f"{progress['failed_jobs']} failed, "
            f"{progress['running_jobs']} running"
        )
