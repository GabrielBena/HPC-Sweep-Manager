"""SSH compute source implementation."""

import asyncio
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

from .compute_source import ComputeSource, JobInfo
from .remote_job_manager import RemoteJobManager
from .remote_discovery import RemoteConfig

logger = logging.getLogger(__name__)


class SSHComputeSource(ComputeSource):
    """SSH-based compute source wrapping RemoteJobManager."""

    def __init__(
        self,
        name: str,
        remote_config: RemoteConfig,
        max_parallel_jobs: Optional[int] = None,
    ):
        # Use max_parallel_jobs from config if not specified
        if max_parallel_jobs is None:
            max_parallel_jobs = remote_config.max_parallel_jobs or 4

        super().__init__(name, "ssh_remote", max_parallel_jobs)
        self.remote_config = remote_config
        self.remote_manager: Optional[RemoteJobManager] = None
        self.local_sweep_dir: Optional[Path] = None

    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        """Setup the SSH compute source for job execution."""
        try:
            logger.info(f"Setting up SSH compute source: {self.name}")

            # Store local sweep directory for result collection
            self.local_sweep_dir = sweep_dir

            # Create RemoteJobManager
            self.remote_manager = RemoteJobManager(
                remote_config=self.remote_config,
                local_sweep_dir=sweep_dir,
                max_parallel_jobs=self.max_parallel_jobs,
                show_progress=False,  # We'll handle progress at distributed level
            )

            # Setup remote environment
            setup_success = await self.remote_manager.setup_remote_environment(
                verify_sync=True,  # Enable sync verification
                auto_sync=False,  # Don't auto-sync, may prompt user
                interactive=True,  # Allow user interaction for sync decisions
            )

            if setup_success:
                self.stats.health_status = "healthy"
                self.stats.last_health_check = datetime.now()
                logger.info(f"✓ SSH compute source {self.name} setup successful")
                return True
            else:
                self.stats.health_status = "unhealthy"
                logger.error(f"✗ SSH compute source {self.name} setup failed")
                return False

        except Exception as e:
            logger.error(f"Error setting up SSH compute source {self.name}: {e}")
            self.stats.health_status = "unhealthy"
            return False

    async def submit_job(
        self,
        params: Dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: Optional[str] = None,
    ) -> str:
        """Submit a single job to the SSH compute source."""
        if not self.remote_manager:
            raise RuntimeError(f"SSH compute source {self.name} not properly setup")

        try:
            # Submit job to remote manager
            job_id = await self.remote_manager.submit_single_job(
                params=params,
                job_name=job_name,
                sweep_id=sweep_id,
                wandb_group=wandb_group,
            )

            # Create job info for tracking
            job_info = JobInfo(
                job_id=job_id,
                job_name=job_name,
                params=params,
                source_name=self.name,
                status="RUNNING",
                submit_time=datetime.now(),
                start_time=datetime.now(),  # SSH jobs start immediately
            )

            # Add to active jobs
            self.active_jobs[job_id] = job_info
            self.stats.total_submitted += 1

            logger.info(
                f"Job {job_name} submitted to SSH source {self.name} with ID {job_id}"
            )
            return job_id

        except Exception as e:
            logger.error(
                f"Failed to submit job {job_name} to SSH source {self.name}: {e}"
            )
            raise

    async def get_job_status(self, job_id: str) -> str:
        """Get status of a job."""
        if not self.remote_manager:
            return "UNKNOWN"

        try:
            # Get status from remote manager
            status = await self.remote_manager.get_job_status(job_id)

            # Update our tracking
            if job_id in self.active_jobs:
                self.update_job_status(job_id, status)

            return status

        except Exception as e:
            logger.error(f"Error getting job status for {job_id} on {self.name}: {e}")
            return "UNKNOWN"

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job."""
        if not self.remote_manager:
            return False

        try:
            success = await self.remote_manager.cancel_job(job_id)

            if success and job_id in self.active_jobs:
                self.update_job_status(job_id, "CANCELLED")

            return success

        except Exception as e:
            logger.error(f"Error cancelling job {job_id} on {self.name}: {e}")
            return False

    async def collect_results(self, job_ids: Optional[List[str]] = None) -> bool:
        """Collect results from completed jobs."""
        if not self.remote_manager:
            return False

        try:
            # Collect all results (remote manager handles which jobs to collect)
            success = await self.remote_manager.collect_results(
                job_ids=job_ids,
                cleanup_after_sync=False,  # Don't cleanup yet, may have more jobs
            )

            logger.info(
                f"Result collection from {self.name}: {'success' if success else 'failed'}"
            )
            return success

        except Exception as e:
            logger.error(f"Error collecting results from {self.name}: {e}")
            return False

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check and return status."""
        try:
            if not self.remote_manager:
                return {
                    "status": "unhealthy",
                    "error": "Remote manager not initialized",
                    "timestamp": datetime.now().isoformat(),
                }

            # Use remote manager's health check capabilities
            # For now, we'll do a simple connectivity test
            from .remote_discovery import create_ssh_connection

            async with await create_ssh_connection(
                self.remote_config.host,
                self.remote_config.ssh_key,
                self.remote_config.ssh_port,
            ) as conn:
                # Simple health check - run date command
                result = await conn.run("date")

                health_info = {
                    "status": "healthy",
                    "timestamp": datetime.now().isoformat(),
                    "remote_time": result.stdout.strip(),
                    "connection": "ok",
                    "active_jobs": len(self.active_jobs),
                    "max_jobs": self.max_parallel_jobs,
                    "utilization": f"{self.utilization:.1%}",
                }

                self.stats.health_status = "healthy"
                self.stats.last_health_check = datetime.now()

                return health_info

        except Exception as e:
            logger.error(f"Health check failed for {self.name}: {e}")
            health_info = {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "connection": "failed",
            }

            self.stats.health_status = "unhealthy"
            self.stats.last_health_check = datetime.now()

            return health_info

    async def cleanup(self):
        """Cleanup resources."""
        try:
            if self.remote_manager:
                # Cancel any remaining jobs
                if self.active_jobs:
                    logger.info(
                        f"Cancelling {len(self.active_jobs)} remaining jobs on {self.name}"
                    )
                    await self.remote_manager.cancel_all_jobs(timeout=30)

                # Cleanup remote environment
                await self.remote_manager.cleanup_remote_environment()

            logger.info(f"SSH compute source {self.name} cleanup completed")

        except Exception as e:
            logger.warning(f"Error during cleanup of SSH source {self.name}: {e}")

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
        return f"SSH:{self.name} ({self.remote_config.host}): {self.current_job_count}/{self.max_parallel_jobs} jobs"
