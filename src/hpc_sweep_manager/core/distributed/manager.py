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

try:
    from rich.console import Console
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


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
        self.task_to_source: Dict[str, str] = {}  # task_name -> source_name
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

        # Rich progress tracking
        self._progress = None
        self._task_id = None
        self._console = Console() if RICH_AVAILABLE else None

    def add_compute_source(self, source: ComputeSource):
        """Add a compute source to the distributed manager."""
        self.sources.append(source)
        self.source_by_name[source.name] = source
        logger.debug(f"Added compute source: {source}")

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
                logger.debug(f"✓ Setup successful for {source.name}")
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

        # Setup Rich progress bar if available
        if RICH_AVAILABLE and self.show_progress and self._console:
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("•"),
                TimeElapsedColumn(),
                console=self._console,
            )
            self._task_id = self._progress.add_task(
                f"Running distributed sweep ({len(self.sources)} sources)",
                total=self.total_jobs_planned,
            )
            self._progress.start()

        # Queue all parameter combinations
        for i, params in enumerate(param_combinations):
            job_name = f"{sweep_id}_task_{i + 1:03d}"
            task_name = f"task_{i + 1:03d}"
            job_info = JobInfo(
                job_id="",  # Will be set when submitted
                job_name=job_name,
                params=params,
                source_name="",  # Will be set when assigned
                status="QUEUED",
                submit_time=datetime.now(),
            )
            await self.job_queue.put((job_info, sweep_id, wandb_group, task_name))

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
            # Stop progress bar
            if self._progress:
                self._progress.stop()

        # Normalize results structure
        await self._normalize_results_structure()

        # Collect scripts and logs from all sources
        await self._collect_scripts_and_logs()

        # Save source mapping
        await self._save_source_mapping()

        # Return all job IDs
        return list(self.all_jobs.keys())

    async def _distribute_jobs(self):
        """Background task to distribute jobs to available sources."""
        logger.debug("Started job distribution task")

        try:
            while self._running and not self._cancelled:
                try:
                    # Get next job from queue (with timeout to check for cancellation)
                    try:
                        job_info, sweep_id, wandb_group, task_name = await asyncio.wait_for(
                            self.job_queue.get(), timeout=1.0
                        )
                    except asyncio.TimeoutError:
                        continue

                    # Find available compute source
                    source = await self._select_compute_source()
                    if not source:
                        # No sources available, put job back and wait
                        await self.job_queue.put((job_info, sweep_id, wandb_group, task_name))
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

                        # Track the job and task-to-source mapping
                        self.all_jobs[job_id] = job_info
                        self.job_to_source[job_id] = source.name
                        self.task_to_source[task_name] = source.name
                        self.jobs_submitted += 1

                        # Update progress bar instead of logging
                        if self._progress and self._task_id is not None:
                            self._progress.update(
                                self._task_id,
                                description=f"Submitting jobs ({self.jobs_submitted}/{self.total_jobs_planned})",
                            )

                    except Exception as e:
                        logger.error(
                            f"Failed to submit job {job_info.job_name} to {source.name}: {e}"
                        )
                        # Put job back in queue for retry
                        retry_count = self.failed_jobs.get(job_info.job_name, 0)
                        if retry_count < self.config.max_retries:
                            self.failed_jobs[job_info.job_name] = retry_count + 1
                            await self.job_queue.put((job_info, sweep_id, wandb_group, task_name))
                            logger.debug(
                                f"Job {job_info.job_name} queued for retry ({retry_count + 1}/{self.config.max_retries})"
                            )
                        else:
                            logger.error(
                                f"Job {job_info.job_name} failed after {self.config.max_retries} retries"
                            )
                            self.jobs_failed += 1

                except Exception as e:
                    logger.error(f"Error in job distribution loop: {e}")
                    await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Critical error in job distribution: {e}")
            self._cancelled = True
        finally:
            logger.debug("Job distribution task completed")

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
        logger.debug("Started job monitoring task")

        try:
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
                    self._update_progress_stats()

                    # Update Rich progress bar
                    if self._progress and self._task_id is not None:
                        completed = self.jobs_completed + self.jobs_failed + self.jobs_cancelled
                        self._progress.update(
                            self._task_id,
                            completed=completed,
                            description=f"Running distributed sweep • ✓ {self.jobs_completed} • ✗ {self.jobs_failed}",
                        )

                    await asyncio.sleep(5)  # Check every 5 seconds

                except Exception as e:
                    logger.error(f"Error in job monitoring loop: {e}")
                    await asyncio.sleep(10)

        except Exception as e:
            logger.error(f"Critical error in job monitoring: {e}")
        finally:
            logger.debug("Job monitoring task completed")

    async def _collect_results_continuously(self):
        """Background task to continuously collect results from remote sources."""
        logger.debug("Started continuous result collection task")

        try:
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
                    logger.error(f"Error in result collection loop: {e}")
                    await asyncio.sleep(10)  # Shorter sleep on error

        except Exception as e:
            logger.error(f"Critical error in result collection: {e}")
        finally:
            logger.debug("Result collection task completed")

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

    def _show_completion_summary(self):
        """Show final completion summary."""
        if self._progress:
            # Progress bar shows the details, just log the completion
            logger.info(
                f"Distributed sweep completed: ✓ {self.jobs_completed} completed, ✗ {self.jobs_failed} failed"
            )
        else:
            # Fallback for when Rich is not available
            total_finished = self.jobs_completed + self.jobs_failed + self.jobs_cancelled
            progress_pct = (total_finished / self.total_jobs_planned) * 100
            logger.info(
                f"Progress: {total_finished}/{self.total_jobs_planned} ({progress_pct:.1f}%) "
                f"- ✓ {self.jobs_completed} completed, ✗ {self.jobs_failed} failed"
            )

    async def _wait_for_completion(self):
        """Wait for all jobs to complete."""
        while self._running and not self._cancelled:
            # Update progress statistics first
            self._update_progress_stats()

            # Check if all jobs are done
            total_finished = self.jobs_completed + self.jobs_failed + self.jobs_cancelled

            # Less verbose completion checking
            queue_empty = self.job_queue.empty()

            # Check completion with better logic
            if queue_empty and total_finished >= self.total_jobs_planned:
                self._show_completion_summary()
                self._running = False
                break
            elif queue_empty and self.jobs_submitted >= self.total_jobs_planned:
                # Also check if all submitted jobs are accounted for
                total_accounted = total_finished + sum(
                    1 for job in self.all_jobs.values() if job.status == "RUNNING"
                )

                if total_accounted >= self.total_jobs_planned:
                    # Wait a bit more for final status updates
                    await asyncio.sleep(2)
                    self._update_progress_stats()
                    total_finished_final = (
                        self.jobs_completed + self.jobs_failed + self.jobs_cancelled
                    )

                    if total_finished_final >= self.total_jobs_planned:
                        self._show_completion_summary()
                        self._running = False
                        break

            await asyncio.sleep(5)  # Check every 5 seconds

    async def _cleanup_tasks(self):
        """Cleanup background tasks."""
        logger.debug("Cleaning up distributed job manager tasks")

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
        logger.debug("Cleaning up distributed job manager")

        # Cleanup all sources
        cleanup_tasks = []
        for source in self.sources:
            cleanup_tasks.append(source.cleanup())

        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        logger.debug("Distributed job manager cleanup completed")

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

    async def _normalize_results_structure(self):
        """
        Normalize results to a unified structure: task_001/, task_002/, etc.
        regardless of which compute source ran them.
        """
        logger.info("Normalizing results to unified structure...")

        try:
            unified_tasks_dir = self.sweep_dir / "tasks"
            unified_tasks_dir.mkdir(exist_ok=True)

            # First, log what we currently have
            logger.debug(f"Current tasks directory contents: {list(unified_tasks_dir.iterdir())}")
            logger.debug(f"Task-to-source mapping: {self.task_to_source}")

            # Process each task and move results to the correct location
            moved_count = 0
            for task_name, source_name in self.task_to_source.items():
                source_results_dir = None

                if source_name == "local":
                    # Local results are typically in tasks/ already with correct naming
                    potential_dirs = [
                        unified_tasks_dir / task_name,
                        unified_tasks_dir / f"local_{task_name}",
                    ]
                    logger.debug(
                        f"Checking local potential dirs for {task_name}: {[str(d) for d in potential_dirs]}"
                    )
                else:
                    # Remote results are in tasks/remote_{source_name}/
                    remote_base_dir = unified_tasks_dir / f"remote_{source_name}"
                    potential_dirs = [
                        remote_base_dir / task_name,
                        remote_base_dir / f"{task_name}",
                        unified_tasks_dir / f"remote_{source_name}_{task_name}",
                    ]
                    logger.debug(
                        f"Checking remote potential dirs for {task_name}: {[str(d) for d in potential_dirs]}"
                    )

                # Find the source directory
                for potential_dir in potential_dirs:
                    if potential_dir.exists():
                        source_results_dir = potential_dir
                        logger.debug(
                            f"Found source directory for {task_name}: {source_results_dir}"
                        )
                        break

                if source_results_dir:
                    target_dir = unified_tasks_dir / task_name

                    if source_results_dir != target_dir:
                        if not target_dir.exists():
                            # Move the directory to the correct location
                            import shutil

                            logger.info(f"Moving {source_results_dir} -> {target_dir}")
                            shutil.move(str(source_results_dir), str(target_dir))
                            logger.info(f"✓ Unified {source_name}:{task_name} -> {task_name}")
                            moved_count += 1
                        else:
                            logger.warning(
                                f"Target directory {task_name} already exists, cannot move from {source_results_dir}"
                            )
                    else:
                        logger.debug(f"Task {task_name} already in correct location: {target_dir}")
                else:
                    logger.warning(
                        f"No source directory found for task {task_name} from source {source_name}"
                    )

            # Clean up any empty remote directories
            cleaned_dirs = 0
            for source in self.sources:
                if source.name != "local":
                    remote_dir = unified_tasks_dir / f"remote_{source.name}"
                    if remote_dir.exists():
                        try:
                            if not any(remote_dir.iterdir()):
                                remote_dir.rmdir()
                                logger.debug(f"Removed empty directory: {remote_dir.name}")
                                cleaned_dirs += 1
                        except OSError as e:
                            logger.debug(f"Could not remove directory {remote_dir.name}: {e}")

            # Final verification
            final_contents = [d for d in unified_tasks_dir.iterdir() if d.is_dir()]
            task_dirs = [d for d in final_contents if d.name.startswith("task_")]

            logger.info(
                f"✓ Results normalization completed: {moved_count} directories moved, {cleaned_dirs} empty directories removed"
            )
            logger.info(
                f"Final structure: {len(task_dirs)} task directories: {[d.name for d in task_dirs]}"
            )

        except Exception as e:
            logger.error(f"Error normalizing results structure: {e}")
            # Don't raise - this shouldn't fail the entire sweep

    async def _save_source_mapping(self):
        """Save a mapping of which tasks ran on which compute sources."""
        try:
            mapping_file = self.sweep_dir / "source_mapping.yaml"

            # Create mapping data
            mapping_data = {
                "sweep_metadata": {
                    "total_tasks": len(self.task_to_source),
                    "compute_sources": [source.name for source in self.sources],
                    "strategy": self.config.strategy.value,
                    "timestamp": datetime.now().isoformat(),
                },
                "task_assignments": {},
            }

            # Add task assignments with additional info
            for task_name, source_name in self.task_to_source.items():
                # Find the job info for this task
                job_info = None
                for job_id, info in self.all_jobs.items():
                    if info.source_name == source_name and task_name in info.job_name:
                        job_info = info
                        break

                mapping_data["task_assignments"][task_name] = {
                    "compute_source": source_name,
                    "status": job_info.status if job_info else "unknown",
                    "start_time": job_info.start_time.isoformat()
                    if job_info and job_info.start_time
                    else None,
                    "complete_time": job_info.complete_time.isoformat()
                    if job_info and job_info.complete_time
                    else None,
                }

            # Save to YAML file
            import yaml

            with open(mapping_file, "w") as f:
                yaml.dump(mapping_data, f, default_flow_style=False, indent=2)

            logger.info(f"✓ Source mapping saved to {mapping_file}")

        except Exception as e:
            logger.error(f"Error saving source mapping: {e}")
            # Don't raise - this is just metadata

    async def _collect_scripts_and_logs(self):
        """Collect scripts and logs from all compute sources."""
        logger.info("Collecting scripts and logs from all compute sources...")

        try:
            scripts_dir = self.sweep_dir / "distributed_scripts"
            logs_dir = self.sweep_dir / "logs"

            scripts_dir.mkdir(exist_ok=True)
            logs_dir.mkdir(exist_ok=True)

            # Collect from each source
            for source in self.sources:
                await self._collect_source_scripts_and_logs(source, scripts_dir, logs_dir)

            logger.info("✓ Scripts and logs collection completed")

        except Exception as e:
            logger.error(f"Error collecting scripts and logs: {e}")
            # Don't raise - this shouldn't fail the entire sweep

    async def _collect_source_scripts_and_logs(self, source, scripts_dir: Path, logs_dir: Path):
        """Collect scripts and logs from a specific compute source."""
        try:
            if source.name == "local":
                await self._collect_local_scripts_and_logs(source, scripts_dir, logs_dir)
            else:
                await self._collect_remote_scripts_and_logs(source, scripts_dir, logs_dir)

        except Exception as e:
            logger.error(f"Error collecting scripts and logs from {source.name}: {e}")

    async def _collect_local_scripts_and_logs(self, source, scripts_dir: Path, logs_dir: Path):
        """Collect scripts and logs from local compute source."""
        try:
            # Local scripts are already in the sweep directory structure
            local_scripts_dir = self.sweep_dir / "local_scripts"
            local_logs_dir = self.sweep_dir / "logs"

            if local_scripts_dir.exists():
                # Copy scripts to distributed_scripts with source prefix
                for script_file in local_scripts_dir.glob("*.sh"):
                    target_name = f"local_{script_file.name}"
                    target_path = scripts_dir / target_name

                    if not target_path.exists():
                        import shutil

                        shutil.copy2(script_file, target_path)
                        logger.debug(f"Copied local script: {script_file.name} -> {target_name}")

            # For logs, local logs might already be in the main logs directory
            # We'll organize them by prefixing with source name if they're not already there

        except Exception as e:
            logger.error(f"Error collecting local scripts and logs: {e}")

    async def _collect_remote_scripts_and_logs(self, source, scripts_dir: Path, logs_dir: Path):
        """Collect scripts and logs from remote compute source."""
        try:
            from ..remote.ssh_compute_source import SSHComputeSource

            if not isinstance(source, SSHComputeSource) or not source.remote_manager:
                logger.debug(f"Source {source.name} is not an SSH source or has no remote manager")
                return

            # Get remote manager to access connection details
            remote_manager = source.remote_manager
            remote_config = source.remote_config

            # Only collect if remote environment is set up
            if not remote_manager.remote_sweep_dir:
                logger.debug(
                    f"No remote sweep directory for {source.name}, skipping script/log collection"
                )
                return

            # Use rsync to collect scripts and logs from remote
            import subprocess

            # Collect scripts
            remote_scripts_dir = f"{remote_manager.remote_sweep_dir}/scripts"
            try:
                rsync_scripts_cmd = (
                    f"rsync -avz --compress-level=6 --ignore-missing-args "
                    f"{remote_config.host}:{remote_scripts_dir}/ "
                    f"{scripts_dir}/"
                )

                result = subprocess.run(
                    rsync_scripts_cmd, shell=True, capture_output=True, text=True
                )

                if result.returncode == 0:
                    logger.debug(f"Successfully collected scripts from {source.name}")

                    # Rename collected scripts to include source name
                    for script_file in scripts_dir.glob("*.sh"):
                        if not script_file.name.startswith(f"{source.name}_"):
                            new_name = f"{source.name}_{script_file.name}"
                            new_path = scripts_dir / new_name
                            if not new_path.exists():
                                script_file.rename(new_path)
                                logger.debug(f"Renamed script: {script_file.name} -> {new_name}")
                else:
                    logger.debug(
                        f"No scripts to collect from {source.name} (rsync exit {result.returncode})"
                    )

            except Exception as e:
                logger.debug(f"Error collecting scripts from {source.name}: {e}")

            # Collect logs
            remote_logs_dir = f"{remote_manager.remote_sweep_dir}/logs"
            try:
                rsync_logs_cmd = (
                    f"rsync -avz --compress-level=6 --ignore-missing-args "
                    f"{remote_config.host}:{remote_logs_dir}/ "
                    f"{logs_dir}/"
                )

                result = subprocess.run(rsync_logs_cmd, shell=True, capture_output=True, text=True)

                if result.returncode == 0:
                    logger.debug(f"Successfully collected logs from {source.name}")

                    # Rename collected logs to include source name
                    for log_file in logs_dir.glob("*"):
                        if log_file.is_file() and not log_file.name.startswith(f"{source.name}_"):
                            # Check file extensions we want to rename
                            if log_file.suffix in [".log", ".err", ".out"]:
                                new_name = f"{source.name}_{log_file.name}"
                                new_path = logs_dir / new_name
                                if not new_path.exists():
                                    log_file.rename(new_path)
                                    logger.debug(f"Renamed log: {log_file.name} -> {new_name}")
                else:
                    logger.debug(
                        f"No logs to collect from {source.name} (rsync exit {result.returncode})"
                    )

            except Exception as e:
                logger.debug(f"Error collecting logs from {source.name}: {e}")

        except Exception as e:
            logger.error(f"Error collecting remote scripts and logs from {source.name}: {e}")
