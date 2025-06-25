"""Compute source abstraction for distributed job execution."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class JobInfo:
    """Information about a submitted job."""

    job_id: str
    job_name: str
    params: Dict[str, Any]
    source_name: str
    status: str = "PENDING"
    submit_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    complete_time: Optional[datetime] = None
    task_dir: Optional[str] = None


@dataclass
class ComputeSourceStats:
    """Statistics for a compute source."""

    name: str
    source_type: str
    max_parallel_jobs: int
    active_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    total_submitted: int = 0
    health_status: str = "unknown"
    last_health_check: Optional[datetime] = None
    average_job_duration: Optional[float] = None  # seconds


class ComputeSource(ABC):
    """Abstract base class for compute sources."""

    def __init__(self, name: str, source_type: str, max_parallel_jobs: int):
        self.name = name
        self.source_type = source_type  # "local", "ssh_remote"
        self.max_parallel_jobs = max_parallel_jobs
        self.active_jobs: Dict[str, JobInfo] = {}
        self.completed_jobs: Dict[str, JobInfo] = {}
        self.stats = ComputeSourceStats(name, source_type, max_parallel_jobs)

    @property
    def current_job_count(self) -> int:
        """Get current number of active jobs."""
        return len(self.active_jobs)

    @property
    def available_slots(self) -> int:
        """Get number of available job slots."""
        return max(0, self.max_parallel_jobs - self.current_job_count)

    @property
    def is_available(self) -> bool:
        """Check if source has available slots and is healthy."""
        return self.available_slots > 0 and self.stats.health_status in [
            "healthy",
            "unknown",
        ]

    @property
    def utilization(self) -> float:
        """Get utilization percentage (0.0 to 1.0)."""
        if self.max_parallel_jobs == 0:
            return 1.0
        return self.current_job_count / self.max_parallel_jobs

    @abstractmethod
    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        """Setup the compute source for job execution."""
        pass

    @abstractmethod
    async def submit_job(
        self,
        params: Dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: Optional[str] = None,
    ) -> str:
        """Submit a single job and return job ID."""
        pass

    @abstractmethod
    async def get_job_status(self, job_id: str) -> str:
        """Get status of a job."""
        pass

    @abstractmethod
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job."""
        pass

    @abstractmethod
    async def collect_results(self, job_ids: Optional[List[str]] = None) -> bool:
        """Collect results from completed jobs."""
        pass

    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check and return status."""
        pass

    @abstractmethod
    async def cleanup(self):
        """Cleanup resources."""
        pass

    def update_job_status(self, job_id: str, new_status: str):
        """Update job status and move between active/completed as needed."""
        if job_id in self.active_jobs:
            job_info = self.active_jobs[job_id]
            job_info.status = new_status

            # Move to completed if job is done
            if new_status in ["COMPLETED", "FAILED", "CANCELLED"]:
                job_info.complete_time = datetime.now()
                self.completed_jobs[job_id] = job_info
                del self.active_jobs[job_id]

                # Update stats
                if new_status == "COMPLETED":
                    self.stats.completed_jobs += 1
                elif new_status == "FAILED":
                    self.stats.failed_jobs += 1

                # Update average job duration
                if job_info.start_time and job_info.complete_time:
                    duration = (job_info.complete_time - job_info.start_time).total_seconds()
                    if self.stats.average_job_duration is None:
                        self.stats.average_job_duration = duration
                    else:
                        # Simple moving average
                        total_completed = self.stats.completed_jobs + self.stats.failed_jobs
                        self.stats.average_job_duration = (
                            self.stats.average_job_duration * (total_completed - 1) + duration
                        ) / total_completed

    def get_stats(self) -> ComputeSourceStats:
        """Get current statistics."""
        self.stats.active_jobs = len(self.active_jobs)
        self.stats.total_submitted = len(self.active_jobs) + len(self.completed_jobs)
        return self.stats

    def __str__(self) -> str:
        return f"{self.name} ({self.source_type}): {self.current_job_count}/{self.max_parallel_jobs} jobs"
