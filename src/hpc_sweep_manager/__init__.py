"""HPC Sweep Manager - Automated hyperparameter sweeps on HPC systems."""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from .core.config_parser import SweepConfig
from .core.param_generator import ParameterGenerator
from .core.job_manager import JobManager
from .core.path_detector import PathDetector

__all__ = [
    "SweepConfig",
    "ParameterGenerator",
    "JobManager",
    "PathDetector",
]
