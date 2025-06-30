"""Compute source implementations for HSM v2."""

from .base import (
    CollectionResult,
    ComputeSource,
    ComputeSourceStats,
    HealthReport,
    HealthStatus,
    SweepContext,
    Task,
    TaskResult,
    TaskStatus,
)
from .local import LocalComputeSource
from .ssh import SSHComputeSource, SSHConfig
from .hpc import HPCComputeSource

__all__ = [
    # Base classes and enums
    "ComputeSource",
    "Task",
    "TaskResult",
    "TaskStatus",
    "HealthStatus",
    "HealthReport",
    "SweepContext",
    "CollectionResult",
    "ComputeSourceStats",
    # Concrete implementations
    "LocalComputeSource",
    "SSHComputeSource",
    "SSHConfig",
    "HPCComputeSource",
]
