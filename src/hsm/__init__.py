"""HPC Sweep Manager v2 - Unified sweep execution across multiple compute sources.

This package provides a unified interface for running parameter sweeps across:
- Local execution
- SSH remote machines
- Distributed execution (multiple sources)
- HPC clusters (PBS/Slurm)

Key Features:
- Compute source agnostic sweep engine
- Unified output structure across all modes
- Cross-mode completion support
- Centralized result collection and error tracking
- Real-time progress monitoring
"""

__version__ = "2.0.0"
__author__ = "HPC Sweep Manager Team"

# Core components
from .core.engine import SweepEngine  # noqa: I001
from .core.tracker import SweepTracker

# Compute sources
from .compute.base import ComputeSource, SweepContext, Task, TaskResult
from .compute.local import LocalComputeSource
from .compute.ssh import SSHComputeSource
from .compute.hpc import HPCComputeSource

# Configuration
from .config.sweep import SweepConfig
from .config.hsm import HSMConfig

# Utilities
from .utils.params import ParameterGenerator
from .utils.logging import setup_logging

__all__ = [
    # Core
    "SweepEngine",
    "SweepTracker",
    # Compute sources
    "ComputeSource",
    "Task",
    "TaskResult",
    "SweepContext",
    "LocalComputeSource",
    "SSHComputeSource",
    "HPCComputeSource",
    # Configuration
    "SweepConfig",
    "HSMConfig",
    # Utilities
    "ParameterGenerator",
    "setup_logging",
]
