"""Job distribution component for distributed sweeps."""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..common.compute_source import ComputeSource, JobInfo
from .base_distributed_manager import DistributionStrategy

logger = logging.getLogger(__name__)


class JobDistributor:
    """Handles job distribution across compute sources."""

    def __init__(
        self,
        sources: List[ComputeSource],
        strategy: DistributionStrategy = DistributionStrategy.ROUND_ROBIN,
        max_retries: int = 3,
    ):
        self.sources = sources
        self.strategy = strategy
        self.max_retries = max_retries

        # Distribution state
        self._round_robin_index = 0
        self.disabled_sources: set = set()

        # Job queue and tracking
        self.job_queue = asyncio.PriorityQueue()
        self.failed_jobs: Dict[str, int] = {}  # job_name -> retry_count

        # Control flags
        self._running = False
        self._distribution_task = None

        # Callbacks
        self.job_submitted_callbacks = []
        self.job_failed_callbacks = []

    def register_job_submitted_callback(self, callback):
        """Register a callback for when jobs are submitted."""
        self.job_submitted_callbacks.append(callback)

    def register_job_failed_callback(self, callback):
        """Register a callback for when jobs fail to submit."""
        self.job_failed_callbacks.append(callback)

    async def queue_job(
        self,
        priority: int,
        job_info: JobInfo,
        sweep_id: str,
        wandb_group: Optional[str],
        task_name: str,
    ):
        """Queue a job for distribution."""
        await self.job_queue.put((priority, (job_info, sweep_id, wandb_group, task_name)))
        logger.debug(f"Queued job {job_info.job_name} with priority {priority}")

    async def start_distribution(self):
        """Start the job distribution task."""
        if self._running:
            logger.warning("Job distribution already running")
            return

        self._running = True
        self._distribution_task = asyncio.create_task(self._distribute_jobs())
        logger.info("Started job distribution")

    async def stop_distribution(self):
        """Stop the job distribution task."""
        self._running = False

        if self._distribution_task and not self._distribution_task.done():
            self._distribution_task.cancel()
            try:
                await self._distribution_task
            except asyncio.CancelledError:
                pass

        logger.info("Stopped job distribution")

    async def _distribute_jobs(self):
        """Background task to distribute jobs to available sources."""
        logger.debug("Started job distribution task")

        try:
            while self._running:
                try:
                    # Get next job from queue (with timeout to check for cancellation)
                    try:
                        (
                            priority,
                            (job_info, sweep_id, wandb_group, task_name),
                        ) = await asyncio.wait_for(self.job_queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue

                    # Find available compute source
                    source = await self._select_compute_source()
                    if not source:
                        # No sources available, put job back and wait
                        await self.job_queue.put(
                            (priority, (job_info, sweep_id, wandb_group, task_name))
                        )
                        await asyncio.sleep(2)
                        continue

                    # Submit job to selected source
                    success = await self._submit_job_to_source(
                        source, job_info, sweep_id, wandb_group, task_name
                    )

                    if not success:
                        # Job submission failed, handle retry
                        await self._handle_job_submission_failure(
                            job_info, sweep_id, wandb_group, task_name, priority
                        )

                except Exception as e:
                    logger.error(f"Error in job distribution loop: {e}")
                    await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Critical error in job distribution: {e}")
        finally:
            logger.debug("Job distribution task completed")

    async def _submit_job_to_source(
        self,
        source: ComputeSource,
        job_info: JobInfo,
        sweep_id: str,
        wandb_group: Optional[str],
        task_name: str,
    ) -> bool:
        """Submit a job to a specific source."""
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

            # Notify callbacks
            for callback in self.job_submitted_callbacks:
                try:
                    await callback(job_info, source.name, task_name)
                except Exception as e:
                    logger.error(f"Error in job submitted callback: {e}")

            logger.debug(f"Successfully submitted job {job_info.job_name} to {source.name}")
            return True

        except Exception as e:
            logger.error(f"Failed to submit job {job_info.job_name} to {source.name}: {e}")
            return False

    async def _handle_job_submission_failure(
        self,
        job_info: JobInfo,
        sweep_id: str,
        wandb_group: Optional[str],
        task_name: str,
        priority: int,
    ):
        """Handle job submission failure with retry logic."""
        retry_count = self.failed_jobs.get(job_info.job_name, 0)

        if retry_count < self.max_retries:
            self.failed_jobs[job_info.job_name] = retry_count + 1
            await self.job_queue.put((priority, (job_info, sweep_id, wandb_group, task_name)))
            logger.debug(
                f"Job {job_info.job_name} queued for retry ({retry_count + 1}/{self.max_retries})"
            )
        else:
            logger.error(f"Job {job_info.job_name} failed after {self.max_retries} retries")

            # Notify failure callbacks
            for callback in self.job_failed_callbacks:
                try:
                    await callback(job_info, task_name, retry_count)
                except Exception as e:
                    logger.error(f"Error in job failed callback: {e}")

    async def _select_compute_source(self) -> Optional[ComputeSource]:
        """Select an available compute source based on the configured strategy."""
        available_sources = [
            s for s in self.sources if s.is_available and s.name not in self.disabled_sources
        ]

        if not available_sources:
            return None

        if self.strategy == DistributionStrategy.ROUND_ROBIN:
            # Round-robin selection
            source = available_sources[self._round_robin_index % len(available_sources)]
            self._round_robin_index += 1
            return source

        elif self.strategy == DistributionStrategy.LEAST_LOADED:
            # Select source with lowest utilization
            return min(available_sources, key=lambda s: s.utilization)

        elif self.strategy == DistributionStrategy.CAPABILITY_BASED:
            # For now, prefer sources with more available slots
            return max(available_sources, key=lambda s: s.available_slots)

        # Default to round-robin
        return available_sources[0]

    def disable_source(self, source_name: str, reason: str = ""):
        """Disable a compute source from receiving new jobs."""
        self.disabled_sources.add(source_name)
        logger.warning(f"Disabled compute source '{source_name}': {reason}")

    def enable_source(self, source_name: str):
        """Re-enable a compute source."""
        self.disabled_sources.discard(source_name)
        logger.info(f"Re-enabled compute source '{source_name}'")

    def get_queue_size(self) -> int:
        """Get the current size of the job queue."""
        return self.job_queue.qsize()

    def is_queue_empty(self) -> bool:
        """Check if the job queue is empty."""
        return self.job_queue.empty()

    def get_distribution_stats(self) -> Dict[str, Any]:
        """Get distribution statistics."""
        available_sources = [
            s.name for s in self.sources if s.is_available and s.name not in self.disabled_sources
        ]

        return {
            "total_sources": len(self.sources),
            "available_sources": len(available_sources),
            "disabled_sources": list(self.disabled_sources),
            "queue_size": self.get_queue_size(),
            "failed_jobs": len(self.failed_jobs),
            "strategy": self.strategy.value,
            "source_details": {
                source.name: {
                    "available": source.is_available,
                    "utilization": f"{source.utilization:.1%}",
                    "available_slots": source.available_slots,
                }
                for source in self.sources
            },
        }
