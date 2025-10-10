"""Local compute source implementation."""

import asyncio
from datetime import datetime
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..common.compute_source import ComputeSource, JobInfo
from .local_manager import LocalJobManager

logger = logging.getLogger(__name__)


class LocalComputeSource(ComputeSource):
    """Local compute source wrapping LocalJobManager."""

    def __init__(
        self,
        name: str = "local",
        max_parallel_jobs: int = 4,
        python_path: str = "python",
        script_path: str = "",
        project_dir: str = ".",
    ):
        super().__init__(name, "local", max_parallel_jobs)
        self.python_path = python_path
        self.script_path = script_path
        self.project_dir = project_dir
        self.local_manager: Optional[LocalJobManager] = None
        self.local_sweep_dir: Optional[Path] = None

    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        """Setup the local compute source for job execution."""
        try:
            logger.info(f"Setting up local compute source: {self.name}")

            # Store local sweep directory
            self.local_sweep_dir = sweep_dir

            # Create LocalJobManager
            self.local_manager = LocalJobManager(
                walltime="04:00:00",  # Default walltime (not used for local)
                resources="local",
                python_path=self.python_path,
                script_path=self.script_path,
                project_dir=self.project_dir,
                max_parallel_jobs=self.max_parallel_jobs,
                show_progress=False,  # We'll handle progress at distributed level
                show_output=False,  # Don't show individual job output
            )

            self.stats.health_status = "healthy"
            self.stats.last_health_check = datetime.now()
            logger.info(f"✓ Local compute source {self.name} setup successful")
            return True

        except Exception as e:
            logger.error(f"Error setting up local compute source {self.name}: {e}")
            self.stats.health_status = "unhealthy"
            return False

    async def submit_job(
        self,
        params: Dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: Optional[str] = None,
    ) -> str:
        """Submit a single job to the local compute source."""
        if not self.local_manager or not self.local_sweep_dir:
            raise RuntimeError(f"Local compute source {self.name} not properly setup")

        try:
            # Create directories for local job execution
            logs_dir = self.local_sweep_dir / "logs"
            scripts_dir = self.local_sweep_dir / "local_scripts"

            # Submit job to local manager (note: this method is not async)
            job_id = await asyncio.to_thread(
                self.local_manager.submit_single_job,
                params=params,
                job_name=job_name,
                sweep_dir=self.local_sweep_dir,
                sweep_id=sweep_id,
                wandb_group=wandb_group,
                pbs_dir=scripts_dir,
                logs_dir=logs_dir,
            )

            # Create job info for tracking
            job_info = JobInfo(
                job_id=job_id,
                job_name=job_name,
                params=params,
                source_name=self.name,
                status="RUNNING",
                submit_time=datetime.now(),
                start_time=datetime.now(),  # Local jobs start immediately
            )

            # Add to active jobs
            self.active_jobs[job_id] = job_info
            self.stats.total_submitted += 1

            logger.debug(f"Job {job_name} submitted to local source {self.name} with ID {job_id}")
            return job_id

        except Exception as e:
            logger.error(f"Failed to submit job {job_name} to local source {self.name}: {e}")
            raise

    async def get_job_status(self, job_id: str) -> str:
        """Get status of a job."""
        if not self.local_manager:
            return "UNKNOWN"

        try:
            # Get status from local manager (not async)
            status = await asyncio.to_thread(self.local_manager.get_job_status, job_id)

            # Update our tracking
            if job_id in self.active_jobs:
                self.update_job_status(job_id, status)

            return status

        except Exception as e:
            logger.error(f"Error getting job status for {job_id} on {self.name}: {e}")
            return "UNKNOWN"

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job."""
        if not self.local_manager:
            return False

        try:
            # Cancel job through local manager (not async)
            success = await asyncio.to_thread(self.local_manager.cancel_job, job_id)

            if success and job_id in self.active_jobs:
                self.update_job_status(job_id, "CANCELLED")

            return success

        except Exception as e:
            logger.error(f"Error cancelling job {job_id} on {self.name}: {e}")
            return False

    async def collect_results(self, job_ids: Optional[List[str]] = None) -> bool:
        """Collect results from completed jobs."""
        # For local jobs, results are already in the local sweep directory
        # No need to collect from remote
        try:
            logger.debug("Local results already available in sweep directory")
            return True

        except Exception as e:
            logger.error(f"Error with local result collection: {e}")
            return False

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check and return status."""
        try:
            # Simple health check for local system
            import psutil

            # Get system information
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage("/")

            health_info = {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "system": {
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "memory_available_gb": memory.available / (1024**3),
                    "disk_free_gb": disk.free / (1024**3),
                },
                "active_jobs": len(self.active_jobs),
                "max_jobs": self.max_parallel_jobs,
                "utilization": f"{self.utilization:.1%}",
                "python_path": self.python_path,
                "project_dir": self.project_dir,
            }

            # Check if we have too many processes or system is overloaded
            if cpu_percent > 90 or memory.percent > 95:
                health_info["status"] = "overloaded"
                health_info["warning"] = "System resources highly utilized"

            self.stats.health_status = health_info["status"]
            self.stats.last_health_check = datetime.now()

            return health_info

        except Exception as e:
            logger.error(f"Health check failed for {self.name}: {e}")
            health_info = {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }

            self.stats.health_status = "unhealthy"
            self.stats.last_health_check = datetime.now()

            return health_info

    async def cleanup(self):
        """Cleanup resources."""
        try:
            if self.local_manager:
                # Cancel any remaining jobs
                if self.active_jobs:
                    logger.info(f"Cancelling {len(self.active_jobs)} remaining local jobs")
                    for job_id in list(self.active_jobs.keys()):
                        await self.cancel_job(job_id)

                # Local manager cleanup (if it has any)
                # LocalJobManager handles cleanup via signal handlers

            logger.info(f"Local compute source {self.name} cleanup completed")

        except Exception as e:
            logger.warning(f"Error during cleanup of local source {self.name}: {e}")

    async def update_all_job_statuses(self):
        """Update statuses of all active jobs."""
        if not self.active_jobs:
            return

        # Update status for all active jobs
        for job_id in list(self.active_jobs.keys()):
            try:
                await self.get_job_status(job_id)
            except Exception as e:
                logger.warning(f"Failed to update status for job {job_id}: {e}")

    def __str__(self) -> str:
        return f"Local:{self.name}: {self.current_job_count}/{self.max_parallel_jobs} jobs"
