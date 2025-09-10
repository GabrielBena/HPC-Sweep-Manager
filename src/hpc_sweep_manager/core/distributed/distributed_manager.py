"""Distributed job manager for multi-source job execution."""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging
from pathlib import Path
import signal
import sys
from typing import Any, Dict, List, Optional, Set

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
    # Failsafe configuration
    enable_source_failsafe: bool = True
    source_failure_threshold: float = 0.4  # Disable source if 40% of jobs fail
    min_jobs_for_failsafe: int = 5  # Need at least 5 jobs before considering failsafe
    auto_disable_unhealthy_sources: bool = True
    health_check_failure_threshold: int = 3  # Disable after 3 consecutive health check failures


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
        self.job_queue = asyncio.PriorityQueue()
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
        self._health_check_task = None

        # Signal handling state
        self._signal_received = False
        self._cleanup_in_progress = False
        self._source_mapping_backup = None

        # Source failure tracking
        self.source_failure_counts = {}  # source_name -> failure_count
        self.source_job_counts = {}  # source_name -> total_job_count
        self.source_health_failures = {}  # source_name -> consecutive_health_failures
        self.disabled_sources = set()  # Set of disabled source names

        # Round-robin state
        self._round_robin_index = 0

        # Rich progress tracking
        self._progress = None
        self._task_id = None
        self._console = Console() if RICH_AVAILABLE else None
        self._original_log_level = None

        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup signal handlers to catch interrupts and preserve source mapping."""
        # Register signal handlers for Ctrl+C and termination
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Also register cleanup on normal program exit
        import atexit

        atexit.register(self._cleanup_on_exit)

        logger.debug("Signal handlers registered for distributed job manager")

    def _signal_handler(self, signum, frame):
        """Handle signals for graceful shutdown and source mapping preservation."""
        if self._signal_received:
            # If we already received a signal and cleanup is in progress,
            # force immediate exit on second signal
            logger.warning(f"Received signal {signum} again, forcing immediate exit...")
            sys.exit(1)

        self._signal_received = True
        logger.info(f"🛑 Signal {signum} received - preserving source mapping and cleaning up...")
        logger.info(f"📊 Cancelling distributed jobs and saving progress...")

        try:
            # First preserve the source mapping before any cleanup
            self._preserve_source_mapping_sync()

            # Then start async cleanup
            import threading

            cleanup_thread = threading.Thread(target=self._threaded_cleanup)
            cleanup_thread.daemon = True
            cleanup_thread.start()

            # Give cleanup thread time to work
            cleanup_thread.join(timeout=30)

            if cleanup_thread.is_alive():
                logger.warning("Cleanup thread still running after timeout, forcing exit...")
                sys.exit(1)
            else:
                logger.info("✅ Distributed job cleanup completed successfully")
                sys.exit(0)

        except Exception as e:
            logger.error(f"Error during signal cleanup: {e}")
            sys.exit(1)

    def _threaded_cleanup(self):
        """Run async cleanup in a thread (called from signal handler)."""
        try:
            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Run the async cleanup with overall timeout
            try:
                loop.run_until_complete(
                    asyncio.wait_for(self._async_cleanup_on_signal(), timeout=25.0)
                )
                logger.info("Signal cleanup completed successfully")
            except asyncio.TimeoutError:
                logger.warning("Signal cleanup timed out after 25 seconds")
            except Exception as e:
                logger.error(f"Error during async signal cleanup: {e}")

        except Exception as e:
            logger.error(f"Error in threaded cleanup: {e}")
        finally:
            try:
                # Close the loop properly
                if loop and not loop.is_closed():
                    loop.close()
            except Exception:
                pass  # Ignore errors when closing loop

    async def _async_cleanup_on_signal(self):
        """Async cleanup method called when signal is received."""
        if self._cleanup_in_progress:
            logger.info("Cleanup already in progress, skipping duplicate cleanup")
            return

        self._cleanup_in_progress = True
        self._cancelled = True

        try:
            logger.info("Starting cancellation of distributed jobs...")

            # Cancel all running jobs
            try:
                results = await self.cancel_all_jobs()
                logger.info(f"Job cancellation results: {results}")
            except Exception as e:
                logger.error(f"Error during job cancellation: {e}")

            # Cleanup compute sources
            logger.info("Cleaning up compute sources...")
            try:
                await asyncio.wait_for(self.cleanup(), timeout=10.0)
                logger.info("Compute source cleanup completed")
            except asyncio.TimeoutError:
                logger.warning("Compute source cleanup timed out")
            except Exception as e:
                logger.warning(f"Compute source cleanup failed: {e}")

        except Exception as e:
            logger.error(f"Error during async cleanup: {e}")
        finally:
            self._cleanup_in_progress = False

    def _cleanup_on_exit(self):
        """Clean up resources on program exit (called by atexit)."""
        if not self._signal_received and self._running:
            logger.info("Program exiting, preserving source mapping...")
            # For atexit, we can only do synchronous cleanup
            self._preserve_source_mapping_sync()

    def _preserve_source_mapping_sync(self):
        """Synchronously preserve source mapping before cleanup."""
        try:
            mapping_file = self.sweep_dir / "source_mapping.yaml"
            if mapping_file.exists():
                # Create a backup
                backup_file = mapping_file.with_suffix(".yaml.signal_backup")
                import shutil

                shutil.copy2(mapping_file, backup_file)
                logger.info(f"Source mapping backed up to: {backup_file}")

                # Also immediately save current state to preserve what we have
                asyncio.run(self._save_source_mapping())
                logger.info("Current source mapping state preserved")
            else:
                logger.warning("No source mapping file found to preserve")

        except Exception as e:
            logger.error(f"Error preserving source mapping: {e}")

    def add_compute_source(self, source: ComputeSource):
        """Add a compute source to the distributed manager."""
        self.sources.append(source)
        self.source_by_name[source.name] = source
        logger.debug(f"Added compute source: {source}")

    async def setup_all_sources(self, sweep_id: str) -> bool:
        """Setup all compute sources for job execution with strict sync requirements."""
        logger.info(f"Setting up {len(self.sources)} compute sources for distributed execution")
        logger.info(
            "🔒 Distributed mode requires strict project synchronization across all sources"
        )

        setup_tasks = []
        for source in self.sources:
            setup_tasks.append(source.setup(self.sweep_dir, sweep_id))

        # Setup all sources concurrently
        results = await asyncio.gather(*setup_tasks, return_exceptions=True)

        successful_sources = []
        failed_sources = []
        sync_failed_sources = []

        for i, result in enumerate(results):
            source = self.sources[i]
            if isinstance(result, Exception):
                logger.error(f"Setup failed for {source.name}: {result}")
                failed_sources.append(source.name)
            elif result is True:
                logger.debug(f"✓ Setup successful for {source.name}")
                successful_sources.append(source.name)
            else:
                logger.error(f"Setup failed for {source.name} (likely sync enforcement failure)")
                # Check if this was a sync failure by examining the source type
                if hasattr(source, "remote_manager"):
                    sync_failed_sources.append(source.name)
                else:
                    failed_sources.append(source.name)

        # For distributed execution, we need ALL sources to be properly synced
        # We cannot proceed if any remote source failed sync verification
        if sync_failed_sources:
            logger.error("🚨 CRITICAL: Distributed execution cannot proceed!")
            logger.error(f"   Project sync enforcement failed for: {sync_failed_sources}")
            logger.error(
                "   Distributed sweeps require identical configs across ALL compute sources"
            )
            logger.error("   This prevents inconsistent results and ensures reproducibility")
            logger.error("")
            logger.error("📋 TO FIX THIS:")
            logger.error("   1. Review the sync differences shown above")
            logger.error("   2. Confirm that local changes should take priority")
            logger.error("   3. Re-run the command and accept the sync operation when prompted")
            logger.error("   4. Only hsm_config.yaml is allowed to differ between sources")

            # Clear all sources since we cannot proceed
            self.sources = []
            self.source_by_name = {}
            return False

        if not successful_sources:
            logger.error("No compute sources successfully set up")
            # Clear all sources since none succeeded
            self.sources = []
            self.source_by_name = {}
            return False

        if failed_sources:
            logger.warning(f"Some sources failed setup (non-sync issues): {failed_sources}")
            # For non-sync failures, we can continue with remaining sources
            # Remove failed sources
            self.sources = [s for s in self.sources if s.name in successful_sources]
            self.source_by_name = {
                name: s for name, s in self.source_by_name.items() if name in successful_sources
            }

        logger.info(f"✅ {len(successful_sources)} compute sources ready for distributed execution")
        logger.info("🔒 All sources have verified identical project configurations")
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

        # Check if this is a completion run with specific task numbers
        is_completion_run = False
        if param_combinations and isinstance(param_combinations[0], dict):
            # Check if parameter combinations include task numbering info (completion run)
            if all(
                isinstance(combo, dict) and "task_number" in combo and "params" in combo
                for combo in param_combinations
            ):
                is_completion_run = True
                logger.info(
                    f"Detected completion run with {len(param_combinations)} specific tasks"
                )

        # Setup Rich progress bar if available
        if RICH_AVAILABLE and self.show_progress and self._console:
            # Configure the console to handle logging properly
            import logging

            # Temporarily reduce log level to WARNING during progress display
            # to prevent interference with progress bar
            self._original_log_level = logging.getLogger().level
            logging.getLogger().setLevel(logging.WARNING)

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
        for i, combo in enumerate(param_combinations):
            if is_completion_run:
                # For completion runs, use the provided task number and params
                task_number = combo["task_number"]
                params = combo["params"]
                job_name = f"{sweep_id}_task_{task_number:03d}"
                task_name = f"task_{task_number:03d}"
                task_priority = task_number  # Use task number as priority for completion runs
                logger.debug(f"Completion task: {task_name} with priority {task_priority}")
            else:
                # For regular sweeps, use sequential numbering
                params = combo
                task_number = i + 1
                job_name = f"{sweep_id}_task_{task_number:03d}"
                task_name = f"task_{task_number:03d}"
                task_priority = task_number

            job_info = JobInfo(
                job_id="",  # Will be set when submitted
                job_name=job_name,
                params=params,
                source_name="",  # Will be set when assigned
                status="QUEUED",
                submit_time=datetime.now(),
            )
            await self.job_queue.put((task_priority, (job_info, sweep_id, wandb_group, task_name)))

        # Start background tasks
        self._distribution_task = asyncio.create_task(self._distribute_jobs())
        self._monitoring_task = asyncio.create_task(self._monitor_jobs())
        self._collection_task = asyncio.create_task(self._collect_results_continuously())
        if self.config.enable_source_failsafe or self.config.auto_disable_unhealthy_sources:
            self._health_check_task = asyncio.create_task(self._monitor_source_health())

        # Wait for all jobs to complete or be cancelled
        try:
            await self._wait_for_completion()
        except asyncio.CancelledError:
            logger.info("Distributed sweep cancelled")
            self._cancelled = True
        finally:
            await self._cleanup_tasks()
            # Stop progress bar and restore logging level
            if self._progress:
                self._progress.stop()
                # Restore original logging level
                if hasattr(self, "_original_log_level"):
                    import logging

                    logging.getLogger().setLevel(self._original_log_level)

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

                        # Initialize source tracking if needed
                        if source.name not in self.source_job_counts:
                            self.source_job_counts[source.name] = 0
                            self.source_failure_counts[source.name] = 0
                        self.source_job_counts[source.name] += 1

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
                            await self.job_queue.put(
                                (priority, (job_info, sweep_id, wandb_group, task_name))
                            )
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
        available_sources = [
            s for s in self.sources if s.is_available and s.name not in self.disabled_sources
        ]

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

        # Update source failure tracking
        self._update_source_failure_tracking()

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
            self._health_check_task,
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

        # Restore logging level if it was modified
        if hasattr(self, "_original_log_level") and self._original_log_level is not None:
            import logging

            logging.getLogger().setLevel(self._original_log_level)

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

    def _update_source_failure_tracking(self):
        """Update source failure counts based on current job statuses."""
        # Reset failure counts for each update
        for source_name in self.source_job_counts:
            self.source_failure_counts[source_name] = 0

        # Count failures per source
        for job in self.all_jobs.values():
            if job.source_name and job.status in ["FAILED", "CANCELLED"]:
                if job.source_name not in self.source_failure_counts:
                    self.source_failure_counts[job.source_name] = 0
                self.source_failure_counts[job.source_name] += 1

        # Check for sources that should be disabled due to high failure rate
        if self.config.enable_source_failsafe:
            self._check_source_failure_rates()

    def _check_source_failure_rates(self):
        """Check source failure rates and disable sources that exceed threshold."""
        for source_name in list(self.source_job_counts.keys()):
            if source_name in self.disabled_sources:
                continue

            job_count = self.source_job_counts[source_name]
            failure_count = self.source_failure_counts.get(source_name, 0)

            # Only check sources with minimum number of jobs
            if job_count >= self.config.min_jobs_for_failsafe:
                failure_rate = failure_count / job_count

                if failure_rate >= self.config.source_failure_threshold:
                    logger.warning(
                        f"Disabling source '{source_name}' due to high failure rate: "
                        f"{failure_count}/{job_count} ({failure_rate:.1%}) >= {self.config.source_failure_threshold:.1%}"
                    )
                    self.disabled_sources.add(source_name)

                    # Log details about the failures
                    failed_jobs = [
                        job
                        for job in self.all_jobs.values()
                        if job.source_name == source_name and job.status in ["FAILED", "CANCELLED"]
                    ]

                    if failed_jobs:
                        logger.info(
                            f"Recent failures on {source_name}: {[job.job_name for job in failed_jobs[-3:]]}"
                        )

    async def _monitor_source_health(self):
        """Background task to monitor source health and disable unhealthy sources."""
        logger.debug("Started source health monitoring task")

        try:
            while self._running and not self._cancelled:
                try:
                    # Check health of all sources
                    for source in self.sources:
                        if source.name in self.disabled_sources:
                            continue

                        try:
                            health_info = await source.health_check()

                            # Initialize health failure tracking if needed
                            if source.name not in self.source_health_failures:
                                self.source_health_failures[source.name] = 0

                            # Check health status
                            if health_info.get("status") == "unhealthy":
                                self.source_health_failures[source.name] += 1
                                logger.warning(
                                    f"Health check failed for {source.name}: {health_info.get('error', 'Unknown error')} "
                                    f"(consecutive failures: {self.source_health_failures[source.name]})"
                                )

                                # Disable source if too many consecutive failures
                                if (
                                    self.config.auto_disable_unhealthy_sources
                                    and self.source_health_failures[source.name]
                                    >= self.config.health_check_failure_threshold
                                ):
                                    logger.error(
                                        f"Disabling source '{source.name}' due to {self.source_health_failures[source.name]} "
                                        f"consecutive health check failures"
                                    )
                                    self.disabled_sources.add(source.name)

                            elif health_info.get("status") == "degraded":
                                # Reset consecutive failures for degraded but functional sources
                                self.source_health_failures[source.name] = max(
                                    0, self.source_health_failures[source.name] - 1
                                )

                                # Log degraded status with details
                                warning_msg = health_info.get("warning", "Degraded performance")
                                logger.warning(f"Source {source.name} is degraded: {warning_msg}")

                            else:  # healthy
                                # Reset consecutive failures for healthy sources
                                self.source_health_failures[source.name] = 0

                            # Log critical disk space issues
                            if health_info.get("disk_status") == "critical":
                                logger.error(
                                    f"CRITICAL: {source.name} has critical disk space shortage: "
                                    f"{health_info.get('disk_message', 'Unknown disk issue')}"
                                )

                        except Exception as e:
                            logger.error(f"Health check error for {source.name}: {e}")
                            # Treat health check errors as health failures
                            if source.name not in self.source_health_failures:
                                self.source_health_failures[source.name] = 0
                            self.source_health_failures[source.name] += 1

                    # Log current status
                    if self.disabled_sources:
                        logger.debug(
                            f"Currently disabled sources: {', '.join(self.disabled_sources)}"
                        )

                    await asyncio.sleep(self.config.health_check_interval)

                except Exception as e:
                    logger.error(f"Error in source health monitoring loop: {e}")
                    await asyncio.sleep(10)

        except Exception as e:
            logger.error(f"Critical error in source health monitoring: {e}")
        finally:
            logger.debug("Source health monitoring task completed")

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

            # Check if this is a completion run
            is_completion_run = (
                hasattr(self, "preserve_existing_mapping") and self.preserve_existing_mapping
            )

            # Process each task and move results to the correct location
            moved_count = 0
            backup_count = 0
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
                            # Target directory already exists - handle based on completion run status
                            if is_completion_run:
                                # For completion runs, check if the new results should overwrite
                                should_overwrite = await self._should_overwrite_existing_task(
                                    source_results_dir, target_dir, task_name, source_name
                                )

                                if should_overwrite:
                                    from datetime import datetime
                                    import shutil

                                    # Create backup of existing directory
                                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                    backup_dir = target_dir.with_name(
                                        f"{task_name}_backup_{timestamp}"
                                    )

                                    logger.info(f"Backing up existing {target_dir} -> {backup_dir}")
                                    shutil.move(str(target_dir), str(backup_dir))
                                    backup_count += 1

                                    # Now move the new results
                                    logger.info(
                                        f"Overwriting {task_name} with new results from {source_name}"
                                    )
                                    shutil.move(str(source_results_dir), str(target_dir))
                                    logger.info(
                                        f"✓ Updated {source_name}:{task_name} -> {task_name} (backup created)"
                                    )
                                    moved_count += 1
                                else:
                                    logger.info(
                                        f"Keeping existing {task_name} results (new results from {source_name} not better)"
                                    )
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

            # After normalization, verify task statuses from actual directories
            await self._verify_task_statuses_from_directories()

            logger.info(
                f"✓ Results normalization completed: {moved_count} directories moved, {cleaned_dirs} empty directories removed"
            )
            if backup_count > 0:
                logger.info(f"✓ Created {backup_count} backups for overwritten tasks")

        except Exception as e:
            logger.error(f"Error normalizing results structure: {e}")
            # Don't raise - this shouldn't fail the entire sweep

    async def _should_overwrite_existing_task(
        self, source_dir: Path, target_dir: Path, task_name: str, source_name: str
    ) -> bool:
        """
        Determine if we should overwrite an existing task directory with new results.
        For completion runs, we want to overwrite if the new task succeeded.
        """
        try:
            # Check the status of the new results
            new_task_info = source_dir / "task_info.txt"
            if not new_task_info.exists():
                logger.debug(f"No task_info.txt in new results for {task_name}, skipping overwrite")
                return False

            with open(new_task_info) as f:
                new_content = f.read()

            # Check if new task completed successfully
            new_status_lines = [
                line for line in new_content.split("\n") if line.startswith("Status: ")
            ]
            if not new_status_lines:
                logger.debug(f"No status found in new results for {task_name}, skipping overwrite")
                return False

            new_status = new_status_lines[-1]  # Get last status
            new_completed = "COMPLETED" in new_status

            if not new_completed:
                logger.debug(f"New results for {task_name} not completed, skipping overwrite")
                return False

            # Check the status of existing results
            existing_task_info = target_dir / "task_info.txt"
            if not existing_task_info.exists():
                logger.info(
                    f"Existing {task_name} has no task_info.txt, overwriting with completed results"
                )
                return True

            with open(existing_task_info) as f:
                existing_content = f.read()

            existing_status_lines = [
                line for line in existing_content.split("\n") if line.startswith("Status: ")
            ]
            if not existing_status_lines:
                logger.info(
                    f"Existing {task_name} has no status, overwriting with completed results"
                )
                return True

            existing_status = existing_status_lines[-1]  # Get last status
            existing_completed = "COMPLETED" in existing_status
            existing_failed = "FAILED" in existing_status
            existing_cancelled = "CANCELLED" in existing_status

            # Overwrite if existing task failed/cancelled and new task completed
            if (existing_failed or existing_cancelled) and new_completed:
                logger.info(
                    f"Overwriting {existing_status.strip()} task {task_name} with completed results from {source_name}"
                )
                return True

            # Don't overwrite if existing task already completed (unless we have good reason)
            if existing_completed:
                logger.debug(f"Existing {task_name} already completed, keeping existing results")
                return False

            # Default: overwrite if new task completed
            logger.info(f"Overwriting {task_name} with completed results from {source_name}")
            return True

        except Exception as e:
            logger.warning(f"Error checking task status for overwrite decision on {task_name}: {e}")
            return False

    async def _verify_task_statuses_from_directories(self):
        """
        Verify and correct task statuses by reading actual task_info.txt files.
        This ensures we catch any status discrepancies between job tracking and actual results.
        """
        logger.debug("Verifying task statuses from actual task directories...")

        try:
            tasks_dir = self.sweep_dir / "tasks"
            if not tasks_dir.exists():
                return

            status_corrections = 0
            collection_retries = 0

            for task_dir in tasks_dir.iterdir():
                if not task_dir.is_dir() or not task_dir.name.startswith("task_"):
                    continue

                task_name = task_dir.name
                task_info_file = task_dir / "task_info.txt"

                if not task_info_file.exists():
                    continue

                try:
                    with open(task_info_file) as f:
                        content = f.read()

                    # Get the last status line (most recent)
                    status_lines = [
                        line for line in content.split("\n") if line.startswith("Status: ")
                    ]
                    if not status_lines:
                        continue

                    actual_status_line = status_lines[-1]
                    if "COMPLETED" in actual_status_line:
                        actual_status = "COMPLETED"
                    elif "FAILED" in actual_status_line:
                        actual_status = "FAILED"
                    elif "CANCELLED" in actual_status_line:
                        actual_status = "CANCELLED"
                    elif "RUNNING" in actual_status_line:
                        actual_status = "RUNNING"
                    else:
                        continue

                    # Project-agnostic collection verification:
                    # If this is a remote task showing FAILED but might be a collection issue
                    if actual_status == "FAILED" and "Remote Machine:" in content:
                        collection_needed = await self._check_if_collection_needed(
                            task_name, task_dir, content
                        )
                        if collection_needed:
                            collection_retries += 1
                            logger.info(
                                f"🔄 Attempting result collection for {task_name} (appears incomplete)"
                            )

                            # Try to collect results and re-check status
                            await self._attempt_task_result_collection(task_name, task_dir)

                            # Re-read the task_info.txt after collection attempt
                            if task_info_file.exists():
                                with open(task_info_file) as f:
                                    updated_content = f.read()
                                    updated_status_lines = [
                                        line
                                        for line in updated_content.split("\n")
                                        if line.startswith("Status: ")
                                    ]
                                    if updated_status_lines:
                                        updated_status_line = updated_status_lines[-1]
                                        if "COMPLETED" in updated_status_line:
                                            actual_status = "COMPLETED"
                                            logger.info(
                                                f"✓ {task_name} status updated to COMPLETED after collection"
                                            )

                    # Find the corresponding job in our tracking
                    job_to_update = None
                    for job_id, job_info in self.all_jobs.items():
                        if (
                            task_name in job_info.job_name
                            or f"task_{job_info.job_name.split('_')[-1]}" == task_name
                        ):
                            job_to_update = job_info
                            break

                    if job_to_update and job_to_update.status != actual_status:
                        old_status = job_to_update.status
                        job_to_update.status = actual_status

                        # Also update completion time if task completed or failed
                        if actual_status in ["COMPLETED", "FAILED", "CANCELLED"]:
                            end_time_lines = [
                                line
                                for line in content.split("\n")
                                if line.startswith("End Time: ")
                            ]
                            if end_time_lines and not job_to_update.complete_time:
                                try:
                                    from datetime import datetime

                                    end_time_str = (
                                        end_time_lines[-1].split("End Time:", 1)[1].strip()
                                    )
                                    # Try to parse the end time (format might vary)
                                    job_to_update.complete_time = datetime.now()  # Fallback to now
                                except:
                                    job_to_update.complete_time = datetime.now()

                        logger.info(
                            f"Corrected status for {task_name}: {old_status} -> {actual_status}"
                        )
                        status_corrections += 1

                except Exception as e:
                    logger.debug(f"Error reading task info for {task_name}: {e}")
                    continue

            if status_corrections > 0:
                logger.info(
                    f"✓ Corrected {status_corrections} task statuses from directory verification"
                )
                # Update our progress stats with corrected information
                self._update_progress_stats()
            else:
                logger.debug("All task statuses verified - no corrections needed")

            if collection_retries > 0:
                logger.info(f"🔄 Attempted result collection for {collection_retries} tasks")

        except Exception as e:
            logger.warning(f"Error in task status verification: {e}")

    async def _check_if_collection_needed(
        self, task_name: str, task_dir: Path, task_info_content: str
    ) -> bool:
        """
        Project-agnostic check to determine if a task needs result collection.

        This checks if:
        1. The task shows FAILED in task_info.txt
        2. The task is from a remote machine
        3. The task directory only has metadata files (suggesting incomplete collection)
        """
        try:
            # Check if this is a remote task
            if "Remote Machine:" not in task_info_content:
                return False

            # Check if the task directory appears to have incomplete collection
            # by looking at what files exist
            task_files = list(task_dir.glob("*"))

            # Standard metadata files created during job submission
            metadata_files = {
                "task_info.txt",
                "command.txt",
                "job.pid",
                "python.pid",
                "script.pid",
                "process_group.pid",
            }

            # Check if we only have metadata files (no actual results)
            actual_files = {f.name for f in task_files if f.is_file()}
            non_metadata_files = actual_files - metadata_files

            if len(non_metadata_files) == 0:
                logger.debug(f"Task {task_name} has only metadata files - collection likely needed")
                return True

            # Also check if the remote task directory path is mentioned but local files are minimal
            if "Task Directory:" in task_info_content:
                # Extract remote task directory
                for line in task_info_content.split("\n"):
                    if line.startswith("Task Directory:"):
                        remote_task_dir = line.split(":", 1)[1].strip()
                        logger.debug(f"Task {task_name} remote directory: {remote_task_dir}")

                        # If we have very few files locally but the job ran on remote,
                        # it suggests collection might be incomplete
                        if len(actual_files) <= 6:  # Just metadata files basically
                            logger.debug(
                                f"Task {task_name} has minimal local files ({len(actual_files)}) - collection likely needed"
                            )
                            return True

            return False

        except Exception as e:
            logger.debug(f"Error checking collection need for {task_name}: {e}")
            return False

    async def _attempt_task_result_collection(self, task_name: str, task_dir: Path):
        """
        Attempt to collect results for a specific task that might have collection issues.
        """
        try:
            # Find which source this task was assigned to
            source_name = self.task_to_source.get(task_name)
            if not source_name:
                logger.debug(f"No source found for task {task_name}")
                return

            source = self.source_by_name.get(source_name)
            if not source or source_name == "local":
                logger.debug(f"Source {source_name} not available for collection retry")
                return

            logger.info(f"🔄 Attempting result collection for {task_name} from {source_name}")

            # Try to collect results for this specific task
            from ..remote.ssh_compute_source import SSHComputeSource

            if isinstance(source, SSHComputeSource):
                # Find the job ID for this task
                job_ids_for_task = [
                    job_id
                    for job_id, job_info in self.all_jobs.items()
                    if task_name in job_info.job_name and job_info.source_name == source_name
                ]

                if job_ids_for_task:
                    # Before collection, check file count
                    before_files = len(list(task_dir.glob("*")))

                    success = await source.collect_results(job_ids_for_task)

                    # After collection, check if we got more files
                    after_files = len(list(task_dir.glob("*")))

                    if success and after_files > before_files:
                        logger.info(
                            f"✓ Successfully collected results for {task_name} ({after_files - before_files} new files)"
                        )
                    elif success:
                        logger.warning(
                            f"⚠️ Collection reported success for {task_name} but no new files found"
                        )
                    else:
                        logger.warning(f"✗ Failed to collect results for {task_name}")
                else:
                    logger.debug(f"No job IDs found for task {task_name}")

        except Exception as e:
            logger.warning(f"Error attempting result collection for {task_name}: {e}")

    async def _save_source_mapping(self):
        """Save a mapping of which tasks ran on which compute sources."""
        try:
            mapping_file = self.sweep_dir / "source_mapping.yaml"

            # For completion runs, preserve existing task assignments and metadata
            existing_assignments = {}
            existing_metadata = {}

            if hasattr(self, "preserve_existing_mapping") and self.preserve_existing_mapping:
                existing_assignments = getattr(self, "existing_task_assignments", {})
                existing_metadata = getattr(self, "existing_metadata", {})
                logger.info(
                    f"Preserving {len(existing_assignments)} existing task assignments in distributed source mapping"
                )

            # Create mapping data, starting with existing metadata for completion runs
            mapping_data = {
                "sweep_metadata": existing_metadata.copy() if existing_metadata else {},
                "task_assignments": existing_assignments.copy() if existing_assignments else {},
            }

            # Update metadata with current sweep info
            mapping_data["sweep_metadata"].update(
                {
                    "total_tasks": max(
                        len(self.task_to_source) + len(existing_assignments),
                        mapping_data["sweep_metadata"].get("total_tasks", 0),
                    ),
                    "compute_sources": sorted(
                        list(
                            set(
                                [source.name for source in self.sources]
                                + mapping_data["sweep_metadata"].get("compute_sources", [])
                            )
                        )
                    ),
                    "strategy": self.config.strategy.value,
                    "timestamp": datetime.now().isoformat(),
                }
            )

            # Add new task assignments (from current distributed run)
            for task_name, source_name in self.task_to_source.items():
                # Find the job info for this task
                job_info = None
                for job_id, info in self.all_jobs.items():
                    if info.source_name == source_name and task_name in info.job_name:
                        job_info = info
                        break

                # Only add/update if this is a new task or if we're updating an existing one
                new_task_data = {
                    "compute_source": source_name,
                    "status": job_info.status if job_info else "unknown",
                    "start_time": job_info.start_time.isoformat()
                    if job_info and job_info.start_time
                    else None,
                    "complete_time": job_info.complete_time.isoformat()
                    if job_info and job_info.complete_time
                    else None,
                }

                # For completion runs, be careful about overwriting existing data
                if task_name in existing_assignments:
                    # Keep original data but allow status updates
                    existing_task = existing_assignments[task_name]
                    merged_task = existing_task.copy()

                    # Update status if changed
                    if new_task_data["status"] != "unknown" and new_task_data[
                        "status"
                    ] != existing_task.get("status"):
                        merged_task["status"] = new_task_data["status"]

                    # Update timing if not already set
                    if new_task_data["complete_time"] and not existing_task.get("complete_time"):
                        merged_task["complete_time"] = new_task_data["complete_time"]
                    if new_task_data["start_time"] and not existing_task.get("start_time"):
                        merged_task["start_time"] = new_task_data["start_time"]

                    # Update compute source if this is a retry and it changed
                    if new_task_data["compute_source"] != existing_task.get("compute_source"):
                        merged_task["compute_source"] = new_task_data["compute_source"]
                        logger.debug(
                            f"Updated compute source for {task_name}: {existing_task.get('compute_source')} -> {new_task_data['compute_source']}"
                        )

                    mapping_data["task_assignments"][task_name] = merged_task
                else:
                    # New task from this completion run
                    mapping_data["task_assignments"][task_name] = new_task_data

            # Save to YAML file
            import yaml

            with open(mapping_file, "w") as f:
                yaml.dump(mapping_data, f, default_flow_style=False, indent=2)

            if hasattr(self, "preserve_existing_mapping") and self.preserve_existing_mapping:
                logger.info(
                    f"✓ Source mapping saved with {len(existing_assignments)} preserved tasks and {len(self.task_to_source)} completion tasks to {mapping_file}"
                )
            else:
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
