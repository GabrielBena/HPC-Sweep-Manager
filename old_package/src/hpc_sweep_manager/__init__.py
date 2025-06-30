"""HPC Sweep Manager - Automated hyperparameter sweeps on HPC systems."""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from .core.common.config import SweepConfig
from .core.common.param_generator import ParameterGenerator
from .core.common.path_detector import PathDetector
from .core.hpc.hpc_base import HPCJobManager
from .core.local.manager import LocalJobManager

__all__ = [
    "SweepConfig",
    "ParameterGenerator",
    "HPCJobManager",
    "LocalJobManager",
    "PathDetector",
]
