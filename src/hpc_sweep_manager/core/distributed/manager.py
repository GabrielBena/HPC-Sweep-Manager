"""Distributed job manager for multi-source job execution."""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..common.compute_source import ComputeSource, JobInfo
from ..remote.ssh_compute_source import SSHComputeSource

logger = logging.getLogger(__name__)


class DistributionStrategy(Enum):
    """Job distribution strategies."""

    ROUND_ROBIN = "round_robin"
    LEAST_LOADED = "least_loaded"
    CAPABILITY_BASED = "capability_based"


@dataclass
class DistributedSweepConfig:
    """Configuration for distributed sweep execution."""

    strategy: DistributionStrategy = DistributionStrategy.ROUND_ROBIN
    collect_interval: int = 300  # seconds
    health_check_interval: int = 60  # seconds
    max_retries: int = 3
    enable_auto_sync: bool = False
    enable_interactive_sync: bool = True


class DistributedJobManager:
    """Manages job distribution across multiple compute sources."""

    def __init__(
        self,
        sweep_dir: Path,
        config: DistributedSweepConfig = None,
        show_progress: bool = True,
    ):
        self.sweep_dir = sweep_dir
        self.config = config or DistributedSweepConfig()
        self.show_progress = show_progress

        # Compute sources
        self.sources: List[ComputeSource] = []
        self.source_by_name: Dict[str, ComputeSource] = {}

        # Job tracking
        self.job_queue = asyncio.Queue()
        self.all_jobs: Dict[str, JobInfo] = {}  # job_id -> job_info
        self.job_to_source: Dict[str, str] = {}  # job_id -> source_name
        self.failed_jobs: Dict[str, int] = {}  # job_id -> retry_count

        # Execution state
        self.total_jobs_planned = 0
        self.jobs_submitted = 0
        self.jobs_completed = 0
        self.jobs_failed = 0
        self.jobs_cancelled = 0

        # Control flags
        self._running = False
        self._cancelled = False
        self._distribution_task = None
        self._monitoring_task = None
        self._collection_task = None

        # Round-robin state
        self._round_robin_index = 0

    def add_compute_source(self, source: ComputeSource):
        """Add a compute source to the distributed manager."""
        self.sources.append(source)
        self.source_by_name[source.name] = source
        logger.info(f"Added compute source: {source}")

    async def setup_all_sources(self, sweep_id: str) -> bool:
        """Setup all compute sources for job execution."""
        logger.info(f"Setting up {len(self.sources)} compute sources for distributed execution")

        setup_tasks = []
        for source in self.sources:
            setup_tasks.append(source.setup(self.sweep_dir, sweep_id))

        # Setup all sources concurrently
        results = await asyncio.gather(*setup_tasks, return_exceptions=True)

        successful_sources = []
        failed_sources = []

        for i, result in enumerate(results):
            source = self.sources[i]
            if isinstance(result, Exception):
                logger.error(f"Setup failed for {source.name}: {result}")
                failed_sources.append(source.name)
            elif result is True:
                logger.info(f"✓ Setup successful for {source.name}")
                successful_sources.append(source.name)
            else:
                logger.error(f"Setup failed for {source.name}")
                failed_sources.append(source.name)

        if not successful_sources:
            logger.error("No compute sources successfully set up")
            # Clear all sources since none succeeded
            self.sources = []
            self.source_by_name = {}
            return False

        if failed_sources:
            logger.warning(f"Some sources failed setup: {failed_sources}")
            # Remove failed sources
            self.sources = [s for s in self.sources if s.name in successful_sources]
            self.source_by_name = {
                name: s for name, s in self.source_by_name.items() if name in successful_sources
            }

        logger.info(f"✓ {len(successful_sources)} compute sources ready for distributed execution")
        return True

    async def submit_distributed_sweep(
        self,
        param_combinations: List[Dict[str, Any]],
        sweep_id: str,
        wandb_group: Optional[str] = None,
    ) -> List[str]:
        """Submit a complete sweep for distributed execution."""
        self.total_jobs_planned = len(param_combinations)
        self._running = True

        logger.info(
            f"Starting distributed sweep: {len(param_combinations)} jobs across {len(self.sources)} sources"
        )

        # Queue all parameter combinations
        for i, params in enumerate(param_combinations):
            job_name = f"{sweep_id}_task_{i + 1:03d}"
            job_info = JobInfo(
                job_id="",  # Will be set when submitted
                job_name=job_name,
                params=params,
                source_name="",  # Will be set when assigned
                status="QUEUED",
                submit_time=datetime.now(),
            )
            await self.job_queue.put((job_info, sweep_id, wandb_group))

        # Start background tasks
        self._distribution_task = asyncio.create_task(self._distribute_jobs())
        self._monitoring_task = asyncio.create_task(self._monitor_jobs())
        self._collection_task = asyncio.create_task(self._collect_results_continuously())

        # Wait for all jobs to complete or be cancelled
        try:
            await self._wait_for_completion()
        except asyncio.CancelledError:
            logger.info("Distributed sweep cancelled")
            self._cancelled = True
        finally:
            await self._cleanup_tasks()

        # Return all job IDs
        return list(self.all_jobs.keys())

    async def _distribute_jobs(self):
        """Background task to distribute jobs to available sources."""
        logger.info("Started job distribution task")

        while self._running and not self._cancelled:
            try:
                # Get next job from queue (with timeout to check for cancellation)
                try:
                    job_info, sweep_id, wandb_group = await asyncio.wait_for(
                        self.job_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                # Find available compute source
                source = await self._select_compute_source()
                if not source:
                    # No sources available, put job back and wait
                    await self.job_queue.put((job_info, sweep_id, wandb_group))
                    await asyncio.sleep(2)
                    continue

                # Submit job to selected source
                try:
                    job_id = await source.submit_job(
                        params=job_info.params,
                        job_name=job_info.job_name,
                        sweep_id=sweep_id,
                        wandb_group=wandb_group,
                    )

                    # Update job info with actual job ID and source
                    job_info.job_id = job_id
                    job_info.source_name = source.name
                    job_info.status = "RUNNING"
                    job_info.start_time = datetime.now()

                    # Track the job
                    self.all_jobs[job_id] = job_info
                    self.job_to_source[job_id] = source.name
                    self.jobs_submitted += 1

                    if self.show_progress:
                        logger.info(
                            f"[{self.jobs_submitted}/{self.total_jobs_planned}] "
                            f"Job {job_info.job_name} submitted to {source.name}"
                        )

                except Exception as e:
                    logger.error(f"Failed to submit job {job_info.job_name} to {source.name}: {e}")
                    # Put job back in queue for retry
                    retry_count = self.failed_jobs.get(job_info.job_name, 0)
                    if retry_count < self.config.max_retries:
                        self.failed_jobs[job_info.job_name] = retry_count + 1
                        await self.job_queue.put((job_info, sweep_id, wandb_group))
                        logger.info(
                            f"Job {job_info.job_name} queued for retry ({retry_count + 1}/{self.config.max_retries})"
                        )
                    else:
                        logger.error(
                            f"Job {job_info.job_name} failed after {self.config.max_retries} retries"
                        )
                        self.jobs_failed += 1

            except Exception as e:
                logger.error(f"Error in job distribution: {e}")
                await asyncio.sleep(1)

        logger.info("Job distribution task completed")

    async def _select_compute_source(self) -> Optional[ComputeSource]:
        """Select an available compute source based on the configured strategy."""
        available_sources = [s for s in self.sources if s.is_available]

        if not available_sources:
            return None

        if self.config.strategy == DistributionStrategy.ROUND_ROBIN:
            # Round-robin selection
            source = available_sources[self._round_robin_index % len(available_sources)]
            self._round_robin_index += 1
            return source

        elif self.config.strategy == DistributionStrategy.LEAST_LOADED:
            # Select source with lowest utilization
            return min(available_sources, key=lambda s: s.utilization)

        elif self.config.strategy == DistributionStrategy.CAPABILITY_BASED:
            # For now, prefer sources with more available slots
            return max(available_sources, key=lambda s: s.available_slots)

        # Default to round-robin
        return available_sources[0]

    async def _monitor_jobs(self):
        """Background task to monitor job statuses and update progress."""
        logger.info("Started job monitoring task")

        while self._running and not self._cancelled:
            try:
                # Update job statuses for all sources
                update_tasks = []
                for source in self.sources:
                    if hasattr(source, "update_all_job_statuses"):
                        update_tasks.append(source.update_all_job_statuses())

                if update_tasks:
                    await asyncio.gather(*update_tasks, return_exceptions=True)

                # Update our statistics (this now syncs statuses from sources)
                old_completed = self.jobs_completed
                old_failed = self.jobs_failed
                self._update_progress_stats()

                # Log status changes for debugging
                if self.jobs_completed != old_completed or self.jobs_failed != old_failed:
                    logger.debug(
                        f"Status update: completed {old_completed}->{self.jobs_completed}, "
                        f"failed {old_failed}->{self.jobs_failed}"
                    )

                # Show progress if enabled
                if self.show_progress:
                    self._show_progress_summary()

                await asyncio.sleep(5)  # Check every 5 seconds for faster detection

            except Exception as e:
                logger.error(f"Error in job monitoring: {e}")
                await asyncio.sleep(5)

        logger.info("Job monitoring task completed")

    async def _collect_results_continuously(self):
        """Background task to continuously collect results from remote sources."""
        logger.info("Started continuous result collection task")

        while self._running and not self._cancelled:
            try:
                # Collect results from all SSH sources
                ssh_sources = [s for s in self.sources if isinstance(s, SSHComputeSource)]

                if ssh_sources:
                    collection_tasks = []
                    for source in ssh_sources:
                        # Collect results for completed jobs
                        completed_job_ids = [
                            job_id
                            for job_id, job_info in self.all_jobs.items()
                            if (
                                job_info.source_name == source.name
                                and job_info.status in ["COMPLETED", "FAILED"]
                            )
                        ]

                        if completed_job_ids:
                            collection_tasks.append(source.collect_results(completed_job_ids))

                    if collection_tasks:
                        await asyncio.gather(*collection_tasks, return_exceptions=True)

                await asyncio.sleep(self.config.collect_interval)

            except Exception as e:
                logger.error(f"Error in result collection: {e}")
                await asyncio.sleep(10)  # Shorter sleep on error

        logger.info("Result collection task completed")

    def _update_progress_stats(self):
        """Update progress statistics based on current job states."""
        # First, sync job statuses from compute sources
        self._sync_job_statuses_from_sources()

        completed = sum(1 for job in self.all_jobs.values() if job.status == "COMPLETED")
        failed = sum(1 for job in self.all_jobs.values() if job.status == "FAILED")
        cancelled = sum(1 for job in self.all_jobs.values() if job.status == "CANCELLED")

        self.jobs_completed = completed
        self.jobs_failed = failed
        self.jobs_cancelled = cancelled

    def _sync_job_statuses_from_sources(self):
        """Synchronize job statuses from compute sources to distributed manager tracking."""
        for source in self.sources:
            # Update statuses from active jobs
            for job_id, job_info in source.active_jobs.items():
                if job_id in self.all_jobs:
                    self.all_jobs[job_id].status = job_info.status
                    # Also sync completion time if available
                    if job_info.complete_time:
                        self.all_jobs[job_id].complete_time = job_info.complete_time

            # Update statuses from completed jobs
            for job_id, job_info in source.completed_jobs.items():
                if job_id in self.all_jobs:
                    self.all_jobs[job_id].status = job_info.status
                    # Also sync completion time
                    if job_info.complete_time:
                        self.all_jobs[job_id].complete_time = job_info.complete_time

    def _show_progress_summary(self):
        """Show progress summary across all sources."""
        total_finished = self.jobs_completed + self.jobs_failed + self.jobs_cancelled
        total_running = sum(1 for job in self.all_jobs.values() if job.status == "RUNNING")

        # Show progress more frequently for better user experience
        if total_finished > 0 and (
            total_finished == 1 or total_finished % 5 == 0
        ):  # Show first completion then every 5
            progress_pct = (total_finished / self.total_jobs_planned) * 100
            logger.info(
                f"Progress: {total_finished}/{self.total_jobs_planned} ({progress_pct:.1f}%) "
                f"- ✓ {self.jobs_completed} completed, ✗ {self.jobs_failed} failed, "
                f"🔄 {total_running} running"
            )

            # Also show per-source breakdown for debugging
            for source in self.sources:
                source_completed = len(source.completed_jobs)
                source_active = len(source.active_jobs)
                if source_completed > 0 or source_active > 0:
                    logger.debug(
                        f"  {source.name}: {source_completed} completed, {source_active} active"
                    )

    async def _wait_for_completion(self):
        """Wait for all jobs to complete."""
        while self._running and not self._cancelled:
            # Update progress statistics first
            self._update_progress_stats()

            # Check if all jobs are done
            total_finished = self.jobs_completed + self.jobs_failed + self.jobs_cancelled

            # More detailed logging for debugging
            queue_empty = self.job_queue.empty()
            logger.debug(
                f"Completion check: {total_finished}/{self.total_jobs_planned} finished "
                f"(✓{self.jobs_completed} ✗{self.jobs_failed} ⚫{self.jobs_cancelled}), "
                f"queue empty: {queue_empty}, submitted: {self.jobs_submitted}"
            )

            # Check completion with better logic
            if queue_empty and total_finished >= self.total_jobs_planned:
                logger.info(
                    f"All jobs completed! Final status: ✓ {self.jobs_completed} completed, "
                    f"✗ {self.jobs_failed} failed, ⚫ {self.jobs_cancelled} cancelled"
                )
                self._running = False
                break
            elif queue_empty and self.jobs_submitted >= self.total_jobs_planned:
                # Also check if all submitted jobs are accounted for
                total_accounted = total_finished + sum(
                    1 for job in self.all_jobs.values() if job.status == "RUNNING"
                )
                logger.debug(f"All jobs submitted, {total_accounted} accounted for")

                if total_accounted >= self.total_jobs_planned:
                    # Wait a bit more for final status updates
                    await asyncio.sleep(2)
                    self._update_progress_stats()
                    total_finished_final = (
                        self.jobs_completed + self.jobs_failed + self.jobs_cancelled
                    )

                    if total_finished_final >= self.total_jobs_planned:
                        logger.info(
                            f"All jobs completed! Final status: ✓ {self.jobs_completed} completed, "
                            f"✗ {self.jobs_failed} failed, ⚫ {self.jobs_cancelled} cancelled"
                        )
                        self._running = False
                        break

            await asyncio.sleep(3)  # Check more frequently

    async def _cleanup_tasks(self):
        """Cleanup background tasks."""
        logger.info("Cleaning up distributed job manager tasks")

        # Cancel background tasks
        for task in [
            self._distribution_task,
            self._monitoring_task,
            self._collection_task,
        ]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def cancel_all_jobs(self) -> Dict[str, int]:
        """Cancel all running jobs across all sources."""
        self._cancelled = True
        self._running = False

        logger.info(f"Cancelling all jobs across {len(self.sources)} sources")

        results = {"cancelled": 0, "failed": 0}

        # Cancel jobs on each source
        cancel_tasks = []
        for source in self.sources:
            # Get job IDs for this source
            source_job_ids = [
                job_id
                for job_id, source_name in self.job_to_source.items()
                if source_name == source.name
            ]

            if source_job_ids:
                for job_id in source_job_ids:
                    cancel_tasks.append(self._cancel_job_on_source(source, job_id))

        if cancel_tasks:
            cancel_results = await asyncio.gather(*cancel_tasks, return_exceptions=True)
            for result in cancel_results:
                if isinstance(result, Exception):
                    results["failed"] += 1
                elif result:
                    results["cancelled"] += 1
                else:
                    results["failed"] += 1

        # Cleanup tasks
        await self._cleanup_tasks()

        logger.info(
            f"Job cancellation complete: {results['cancelled']} cancelled, {results['failed']} failed"
        )
        return results

    async def _cancel_job_on_source(self, source: ComputeSource, job_id: str) -> bool:
        """Cancel a specific job on a specific source."""
        try:
            return await source.cancel_job(job_id)
        except Exception as e:
            logger.error(f"Failed to cancel job {job_id} on {source.name}: {e}")
            return False

    async def cleanup(self):
        """Cleanup all resources."""
        logger.info("Cleaning up distributed job manager")

        # Cleanup all sources
        cleanup_tasks = []
        for source in self.sources:
            cleanup_tasks.append(source.cleanup())

        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        logger.info("Distributed job manager cleanup completed")

    def get_distributed_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics for the distributed sweep."""
        source_stats = {}
        for source in self.sources:
            stats = source.get_stats()
            source_stats[source.name] = {
                "type": stats.source_type,
                "active_jobs": stats.active_jobs,
                "completed_jobs": stats.completed_jobs,
                "failed_jobs": stats.failed_jobs,
                "total_submitted": stats.total_submitted,
                "max_parallel_jobs": stats.max_parallel_jobs,
                "health_status": stats.health_status,
                "utilization": f"{source.utilization:.1%}",
                "avg_duration": stats.average_job_duration,
            }

        return {
            "overall": {
                "total_planned": self.total_jobs_planned,
                "submitted": self.jobs_submitted,
                "completed": self.jobs_completed,
                "failed": self.jobs_failed,
                "cancelled": self.jobs_cancelled,
                "running": self.jobs_submitted
                - self.jobs_completed
                - self.jobs_failed
                - self.jobs_cancelled,
                "progress_pct": (self.jobs_completed + self.jobs_failed + self.jobs_cancelled)
                / self.total_jobs_planned
                * 100
                if self.total_jobs_planned > 0
                else 0,
            },
            "sources": source_stats,
            "strategy": self.config.strategy.value,
            "timestamp": datetime.now().isoformat(),
        }
