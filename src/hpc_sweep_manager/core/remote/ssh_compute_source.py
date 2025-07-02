"""SSH compute source implementation."""

from datetime import datetime
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..common.compute_source import ComputeSource, JobInfo
from .discovery import RemoteConfig
from .remote_manager import RemoteJobManager

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
            logger.debug(f"Setting up SSH compute source: {self.name}")

            # Store local sweep directory for result collection
            self.local_sweep_dir = sweep_dir

            # Create RemoteJobManager
            self.remote_manager = RemoteJobManager(
                remote_config=self.remote_config,
                local_sweep_dir=sweep_dir,
                max_parallel_jobs=self.max_parallel_jobs,
                show_progress=False,  # We'll handle progress at distributed level
            )

            # Mark this remote manager as being used in distributed mode
            # This enforces strict sync verification for distributed execution
            self.remote_manager.is_distributed_mode = True

            # For distributed sweeps, we want strict sync verification
            # to ensure all compute sources have the same config state
            logger.info(
                f"🔒 Verifying strict project sync for {self.name} (ensuring identical configs across all sources)"
            )

            # Setup remote environment with strict sync verification
            setup_success = await self.remote_manager.setup_remote_environment(
                verify_sync=True,  # Always verify sync for distributed execution
                auto_sync=False,  # Don't auto-sync, require user confirmation for safety
                interactive=True,  # Allow user to review and confirm sync changes
            )

            if setup_success:
                self.stats.health_status = "healthy"
                self.stats.last_health_check = datetime.now()
                logger.info(
                    f"✅ SSH compute source {self.name} setup successful - strict project sync enforced"
                )
                return True
            else:
                self.stats.health_status = "unhealthy"
                logger.error(
                    f"❌ SSH compute source {self.name} setup failed - project sync enforcement failed"
                )
                logger.error(
                    "   Distributed execution requires identical configs across all compute sources"
                )
                logger.error("   Ensure local changes are synced before running distributed sweep")
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

            logger.debug(f"Job {job_name} submitted to SSH source {self.name} with ID {job_id}")
            return job_id

        except Exception as e:
            logger.error(f"Failed to submit job {job_name} to SSH source {self.name}: {e}")
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

            logger.debug(
                f"Result collection from {self.name}: {'success' if success else 'failed'}"
            )
            return success

        except Exception as e:
            logger.error(f"Error collecting results from {self.name}: {e}")
            return False

    async def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check including disk space monitoring."""
        try:
            if not self.remote_manager:
                return {
                    "status": "unhealthy",
                    "error": "Remote manager not initialized",
                    "timestamp": datetime.now().isoformat(),
                }

            # Use remote manager's health check capabilities
            from .discovery import create_ssh_connection

            async with await create_ssh_connection(
                self.remote_config.host,
                self.remote_config.ssh_key,
                self.remote_config.ssh_port,
            ) as conn:
                health_info = {
                    "status": "healthy",
                    "timestamp": datetime.now().isoformat(),
                    "connection": "ok",
                    "active_jobs": len(self.active_jobs),
                    "max_jobs": self.max_parallel_jobs,
                    "utilization": f"{self.utilization:.1%}",
                }

                # Basic connectivity test
                result = await conn.run("date")
                health_info["remote_time"] = result.stdout.strip()

                # Enhanced disk space checking
                disk_health = await self._check_disk_space_health(conn)
                health_info.update(disk_health)

                # System load and memory check
                system_health = await self._check_system_health(conn)
                health_info.update(system_health)

                # Check for recent job failures due to disk space
                failure_rate = await self._check_disk_failure_rate(conn)
                health_info["disk_failure_rate"] = failure_rate

                # Determine overall health status
                if disk_health.get("disk_status") == "critical":
                    health_info["status"] = "unhealthy"
                    health_info["error"] = "Critical disk space shortage"
                elif disk_health.get("disk_status") == "warning" and failure_rate > 0.3:
                    health_info["status"] = "degraded"
                    health_info["warning"] = "High disk failure rate with low space"
                elif failure_rate > 0.5:
                    health_info["status"] = "degraded"
                    health_info["warning"] = f"High failure rate: {failure_rate:.1%}"

                self.stats.health_status = health_info["status"]
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

    async def _check_disk_space_health(self, conn) -> Dict[str, Any]:
        """Check disk space health on remote machine."""
        try:
            # Check project root disk space
            project_result = await conn.run(
                f"df {self.remote_config.project_root} | tail -1 | awk '{{print $4/1024/1024, 100-($3/$2*100)}}'"
            )
            project_space_gb, project_space_percent = map(
                float, project_result.stdout.strip().split()
            )

            # Check tmp/home space
            home_result = await conn.run(
                "df ~ | tail -1 | awk '{print $4/1024/1024, 100-($3/$2*100)}'"
            )
            home_space_gb, home_space_percent = map(float, home_result.stdout.strip().split())

            disk_info = {
                "project_space_gb": project_space_gb,
                "project_space_percent": project_space_percent,
                "home_space_gb": home_space_gb,
                "home_space_percent": home_space_percent,
            }

            # Determine disk status
            min_space_gb = min(project_space_gb, home_space_gb)
            min_space_percent = min(project_space_percent, home_space_percent)

            if min_space_gb < 1.0 or min_space_percent < 5:
                disk_info["disk_status"] = "critical"
                disk_info["disk_message"] = (
                    f"Critical: {min_space_gb:.1f}GB ({min_space_percent:.1f}%) free"
                )
            elif min_space_gb < 3.0 or min_space_percent < 10:
                disk_info["disk_status"] = "warning"
                disk_info["disk_message"] = (
                    f"Warning: {min_space_gb:.1f}GB ({min_space_percent:.1f}%) free"
                )
            else:
                disk_info["disk_status"] = "healthy"
                disk_info["disk_message"] = (
                    f"Healthy: {min_space_gb:.1f}GB ({min_space_percent:.1f}%) free"
                )

            return disk_info

        except Exception as e:
            logger.warning(f"Disk space check failed for {self.name}: {e}")
            return {
                "disk_status": "unknown",
                "disk_message": f"Check failed: {e}",
            }

    async def _check_system_health(self, conn) -> Dict[str, Any]:
        """Check system load and memory health."""
        try:
            # Get load average
            load_result = await conn.run(
                "uptime | awk -F'load average:' '{print $2}' | awk '{print $1}' | tr -d ','"
            )
            load_avg = float(load_result.stdout.strip())

            # Get memory usage
            mem_result = await conn.run("free | grep Mem | awk '{print $3/$2 * 100.0}'")
            memory_percent = float(mem_result.stdout.strip())

            return {
                "load_average": load_avg,
                "memory_percent": memory_percent,
                "system_health": "healthy"
                if load_avg < 5.0 and memory_percent < 85
                else "overloaded",
            }

        except Exception as e:
            logger.warning(f"System health check failed for {self.name}: {e}")
            return {
                "system_health": "unknown",
                "system_message": f"Check failed: {e}",
            }

    async def _check_disk_failure_rate(self, conn) -> float:
        """Check recent job failure rate due to disk space issues."""
        try:
            if not self.remote_manager or not self.remote_manager.remote_sweep_dir:
                return 0.0

            # Look for recent disk space failures in task directories
            check_cmd = f"""
            find {self.remote_manager.remote_sweep_dir}/tasks -name "task_info.txt" -type f 2>/dev/null | \
            xargs grep -l "FAILED_DISK_SPACE\\|no space left on device" 2>/dev/null | wc -l
            """

            failed_result = await conn.run(check_cmd)
            failed_count = int(failed_result.stdout.strip() or "0")

            # Count total tasks
            total_cmd = f"""
            find {self.remote_manager.remote_sweep_dir}/tasks -name "task_info.txt" -type f 2>/dev/null | wc -l
            """

            total_result = await conn.run(total_cmd)
            total_count = int(total_result.stdout.strip() or "0")

            if total_count == 0:
                return 0.0

            failure_rate = failed_count / total_count
            logger.debug(
                f"Disk failure rate for {self.name}: {failed_count}/{total_count} = {failure_rate:.2%}"
            )

            return failure_rate

        except Exception as e:
            logger.warning(f"Disk failure rate check failed for {self.name}: {e}")
            return 0.0

    async def cleanup(self):
        """Cleanup resources."""
        try:
            if self.remote_manager:
                # Cancel any remaining jobs
                if self.active_jobs:
                    logger.info(f"Cancelling {len(self.active_jobs)} remaining jobs on {self.name}")
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
