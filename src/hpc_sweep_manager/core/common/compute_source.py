"""Compute source abstraction for distributed job execution."""

from __future__ import annotations

from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional

from .resource_spec import ResourceSpec

logger = logging.getLogger(__name__)


SubmissionMode = Literal["individual", "array"]
ProgressCallback = Callable[[int, int], None]

# Job states that mark a job as no longer active. Backends should normalize
# their native state strings into these canonical values via update_job_status.
TERMINAL_STATES: frozenset[str] = frozenset({"COMPLETED", "FAILED", "CANCELLED"})


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
        spec: Optional[ResourceSpec] = None,
    ) -> str:
        """Submit a single job and return job ID.

        ``spec`` carries scheduler-relevant resource requirements; backends
        that don't need them (e.g. local) may ignore it. Existing
        implementations that don't yet accept ``spec`` will continue to work
        because Python ignores extra keyword arguments only if explicitly
        ``**kwargs``-captured — subclasses should add the parameter to their
        signature as they're migrated.
        """
        pass

    async def submit_batch(
        self,
        params_list: List[Dict[str, Any]],
        sweep_id: str,
        mode: SubmissionMode = "individual",
        spec: Optional[ResourceSpec] = None,
        wandb_group: Optional[str] = None,
        job_name_prefix: Optional[str] = None,
    ) -> List[str]:
        """Submit a batch of jobs and return their IDs.

        Default implementation submits jobs one-by-one via
        :meth:`submit_job` for ``mode="individual"``. Backends that support
        true scheduler-side array submission should override this method for
        ``mode="array"``; the default raises :class:`NotImplementedError` in
        that case.
        """
        if mode == "individual":
            prefix = job_name_prefix or sweep_id
            job_ids: List[str] = []
            for i, params in enumerate(params_list):
                job_name = f"{prefix}_task_{i + 1:03d}"
                job_id = await self.submit_job(
                    params=params,
                    job_name=job_name,
                    sweep_id=sweep_id,
                    wandb_group=wandb_group,
                    spec=spec,
                )
                job_ids.append(job_id)
            return job_ids
        if mode == "array":
            raise NotImplementedError(
                f"{type(self).__name__} ({self.source_type}) does not support array submission"
            )
        raise ValueError(f"Unknown submission mode: {mode!r}")

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

    async def wait_for_all(
        self,
        poll_interval: float = 5.0,
        on_progress: Optional[ProgressCallback] = None,
    ) -> Dict[str, str]:
        """Block until every active job reaches a terminal state.

        Returns a mapping of ``job_id -> final_status``. Polls each active job
        via :meth:`get_job_status` (which is expected to call
        :meth:`update_job_status` internally so finished jobs leave
        ``active_jobs``). Override for backends that can poll all jobs at once
        more efficiently (e.g. a single Slurm ``squeue`` call).
        """
        final_statuses: Dict[str, str] = {}
        # Seed with anything already moved to completed before we started waiting.
        for job_id, info in list(self.completed_jobs.items()):
            final_statuses[job_id] = info.status

        total = len(self.active_jobs) + len(final_statuses)
        if on_progress is not None:
            on_progress(len(final_statuses), max(total, 1))

        while self.active_jobs:
            for job_id in list(self.active_jobs.keys()):
                status = await self.get_job_status(job_id)
                if status in TERMINAL_STATES and job_id not in final_statuses:
                    final_statuses[job_id] = status
            if on_progress is not None:
                total = len(self.active_jobs) + len(final_statuses)
                on_progress(len(final_statuses), max(total, 1))
            if self.active_jobs:
                await asyncio.sleep(poll_interval)
        return final_statuses

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
