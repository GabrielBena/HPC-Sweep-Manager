"""Base class for job managers."""

from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseJobManager(ABC):
    """Abstract base class for job managers."""

    def __init__(self, max_parallel_jobs: int, show_progress: bool):
        self.max_parallel_jobs = max_parallel_jobs
        self.show_progress = show_progress

        self.running_jobs: Dict[str, Any] = {}
        self.job_counter = 0
        self.total_jobs_planned = 0
        self.jobs_completed = 0
        self.system_type = "base"

    @abstractmethod
    def submit_single_job(
        self, params: Dict[str, Any], job_name: str, sweep_id: str, **kwargs
    ) -> str:
        """Submit a single job."""
        pass

    @abstractmethod
    def get_job_status(self, job_id: str) -> str:
        """Get status of a job."""
        pass

    @abstractmethod
    def cancel_job(self, job_id: str, timeout: int = 10) -> bool:
        """Cancel a specific job."""
        pass

    @abstractmethod
    def cancel_all_jobs(self, timeout: int = 10) -> dict:
        """Cancel all running jobs."""
        pass

    @abstractmethod
    def wait_for_all_jobs(self, use_progress_bar: bool = False):
        """Wait for all jobs to complete."""
        pass

    def _params_to_string(self, params: Dict[str, Any]) -> str:
        """Convert parameters dictionary to command line arguments for Hydra."""
        param_strs = []
        for key, value in params.items():
            if isinstance(value, (list, tuple)):
                value_str = str(list(value))
                param_strs.append(f'"{key}={value_str}"')
            elif value is None:
                param_strs.append(f'"{key}=null"')
            elif isinstance(value, bool):
                param_strs.append(f'"{key}={str(value).lower()}"')
            elif isinstance(value, str) and (" " in value or "," in value):
                param_strs.append(f'"{key}={value}"')
            else:
                param_strs.append(f'"{key}={value}"')
        return " ".join(param_strs)
