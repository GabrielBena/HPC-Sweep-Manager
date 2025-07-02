"""Refactored distributed job manager using component-based architecture."""

import asyncio
from datetime import datetime
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from dateutil.parser import parse as parse_date

    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False
    parse_date = None

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

from ..common.compute_source import ComputeSource, JobInfo
from .base_distributed_manager import BaseDistributedManager, DistributedSweepConfig
from .job_distributor import JobDistributor
from .job_monitor import JobMonitor
from .result_collector import ResultCollector

logger = logging.getLogger(__name__)


class DistributedJobManager(BaseDistributedManager):
    """
    Refactored distributed job manager using component-based architecture.

    This manager coordinates between:
    - JobDistributor: Handles job distribution across sources
    - JobMonitor: Real-time job status monitoring with validation
    - ResultCollector: Result collection and normalization
    """

    def __init__(
        self,
        sweep_dir: Path,
        config: DistributedSweepConfig = None,
        show_progress: bool = True,
        max_parallel_jobs: int = None,
    ):
        super().__init__(sweep_dir, config, show_progress, max_parallel_jobs)

        # Components - initialized after sources are added
        self.job_distributor: Optional[JobDistributor] = None
        self.job_monitor: Optional[JobMonitor] = None
        self.result_collector: Optional[ResultCollector] = None

        # Progress tracking
        self._progress = None
        self._task_id = None
        self._console = Console() if RICH_AVAILABLE else None
        self._original_log_level = None

        # Completion state
        self.preserve_existing_mapping = False
        self.existing_task_assignments = {}
        self.existing_metadata = {}

    def _initialize_components(self):
        """Initialize components after sources are added."""
        if not self.sources:
            raise ValueError("Cannot initialize components without compute sources")

        # Initialize job distributor
        self.job_distributor = JobDistributor(
            sources=self.sources, strategy=self.config.strategy, max_retries=self.config.max_retries
        )

        # Initialize job monitor
        self.job_monitor = JobMonitor(sweep_dir=self.sweep_dir, sources=self.sources)

        # Initialize result collector
        self.result_collector = ResultCollector(
            sweep_dir=self.sweep_dir,
            sources=self.sources,
            collect_interval=self.config.collect_interval,
        )

        # Wire up callbacks
        self._setup_component_callbacks()

        logger.debug("Initialized distributed job manager components")

    def _setup_component_callbacks(self):
        """Setup callbacks between components."""
        if not all([self.job_distributor, self.job_monitor]):
            return

        # Job distributor callbacks
        self.job_distributor.register_job_submitted_callback(self._on_job_submitted)
        self.job_distributor.register_job_failed_callback(self._on_job_failed)

        # Job monitor callbacks
        self.job_monitor.register_status_change_callback(self._on_status_change)

    async def _on_job_submitted(self, job_info: JobInfo, source_name: str, task_name: str):
        """Callback when a job is submitted."""
        # Add job to monitor
        self.job_monitor.add_job(job_info, source_name, task_name)

        # Update tracking
        self.all_jobs[job_info.job_id] = job_info
        self.job_to_source[job_info.job_id] = source_name
        self.task_to_source[task_name] = source_name
        self.jobs_submitted += 1

        # Initialize source tracking if needed
        if source_name not in self.source_job_counts:
            self.source_job_counts[source_name] = 0
            self.source_failure_counts[source_name] = 0
        self.source_job_counts[source_name] += 1

        # Update progress
        if self._progress and self._task_id is not None:
            self._progress.update(
                self._task_id,
                description=f"Submitting jobs ({self.jobs_submitted}/{self.total_jobs_planned})",
            )

        logger.debug(f"Job {job_info.job_name} submitted to {source_name}")

    async def _on_job_failed(self, job_info: JobInfo, task_name: str, retry_count: int):
        """Callback when a job fails to submit after retries."""
        self.jobs_failed += 1
        logger.error(f"Job {job_info.job_name} failed after {retry_count} retries")

    async def _on_status_change(self, status_changes: List[Dict[str, str]]):
        """Callback when job statuses change."""
        for change in status_changes:
            job_id = change["job_id"]
            old_status = change["old_status"]
            new_status = change["new_status"]
            source_name = change["source"]

            logger.info(f"Job status change: {job_id} ({source_name}) {old_status} -> {new_status}")

            # Update our failure tracking if job failed
            if new_status in ["FAILED", "CANCELLED"]:
                if source_name not in self.source_failure_counts:
                    self.source_failure_counts[source_name] = 0
                self.source_failure_counts[source_name] += 1

                # Check if we should disable the source
                await self._check_source_failure_rate(source_name)

    async def _check_source_failure_rate(self, source_name: str):
        """Check if a source should be disabled due to high failure rate."""
        if not self.config.enable_source_failsafe:
            return

        job_count = self.source_job_counts.get(source_name, 0)
        failure_count = self.source_failure_counts.get(source_name, 0)

        if job_count >= self.config.min_jobs_for_failsafe:
            failure_rate = failure_count / job_count

            if failure_rate >= self.config.source_failure_threshold:
                reason = f"high failure rate: {failure_count}/{job_count} ({failure_rate:.1%})"
                self.job_distributor.disable_source(source_name, reason)
                self.disabled_sources.add(source_name)

    async def submit_distributed_sweep(
        self,
        param_combinations: List[Dict[str, Any]],
        sweep_id: str,
        wandb_group: Optional[str] = None,
    ) -> List[str]:
        """Submit a complete sweep for distributed execution."""
        # Ensure components are initialized
        if not self.job_distributor:
            self._initialize_components()

        self.total_jobs_planned = len(param_combinations)

        logger.info(
            f"Starting distributed sweep: {len(param_combinations)} jobs across {len(self.sources)} sources"
        )

        # Check if this is a completion run
        is_completion_run = self._detect_completion_run(param_combinations)

        # Setup progress tracking
        await self._setup_progress_tracking(is_completion_run)

        try:
            # Start components
            await self._start_components()

            # Queue all jobs
            await self._queue_parameter_combinations(param_combinations, sweep_id, wandb_group)

            # Wait for completion
            await self._wait_for_completion()

            # Post-processing
            await self._post_process_results(is_completion_run)

        finally:
            # Stop components and cleanup
            await self._stop_components()
            await self._cleanup_progress_tracking()

        return list(self.all_jobs.keys())

    def _detect_completion_run(self, param_combinations: List[Dict[str, Any]]) -> bool:
        """Detect if this is a completion run with specific task numbers."""
        if not param_combinations:
            return False

        return all(
            isinstance(combo, dict) and "task_number" in combo and "params" in combo
            for combo in param_combinations
        )

    async def _setup_progress_tracking(self, is_completion_run: bool):
        """Setup Rich progress tracking if available."""
        if RICH_AVAILABLE and self.show_progress and self._console:
            # Temporarily reduce log level during progress display
            import logging

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

            desc = f"Running distributed {'completion' if is_completion_run else 'sweep'} ({len(self.sources)} sources)"
            self._task_id = self._progress.add_task(desc, total=self.total_jobs_planned)
            self._progress.start()

    async def _start_components(self):
        """Start all components."""
        await self.job_monitor.start_monitoring()
        await self.result_collector.start_continuous_collection()
        await self.job_distributor.start_distribution()

    async def _queue_parameter_combinations(
        self, param_combinations: List[Dict[str, Any]], sweep_id: str, wandb_group: Optional[str]
    ):
        """Queue all parameter combinations for distribution."""
        for i, combo in enumerate(param_combinations):
            if isinstance(combo, dict) and "task_number" in combo and "params" in combo:
                # Completion run format
                task_number = combo["task_number"]
                params = combo["params"]
                task_priority = task_number
            else:
                # Regular sweep format
                params = combo
                task_number = i + 1
                task_priority = task_number

            job_name = f"{sweep_id}_task_{task_number:03d}"
            task_name = f"task_{task_number:03d}"

            job_info = JobInfo(
                job_id="",  # Will be set when submitted
                job_name=job_name,
                params=params,
                source_name="",  # Will be set when assigned
                status="QUEUED",
                submit_time=datetime.now(),
            )

            await self.job_distributor.queue_job(
                task_priority, job_info, sweep_id, wandb_group, task_name
            )

    async def _wait_for_completion(self):
        """Wait for all jobs to complete with proper progress tracking."""
        while True:
            # Update progress statistics
            job_counts = self.job_monitor.get_job_counts()

            self.jobs_completed = job_counts["COMPLETED"]
            self.jobs_failed = job_counts["FAILED"]
            self.jobs_cancelled = job_counts["CANCELLED"]

            # Update progress bar
            if self._progress and self._task_id is not None:
                completed = self.jobs_completed + self.jobs_failed + self.jobs_cancelled
                self._progress.update(
                    self._task_id,
                    completed=completed,
                    description=f"Running distributed sweep • ✓ {self.jobs_completed} • ✗ {self.jobs_failed}",
                )

            # Check completion
            total_finished = self.jobs_completed + self.jobs_failed + self.jobs_cancelled
            queue_empty = self.job_distributor.is_queue_empty()

            if queue_empty and total_finished >= self.total_jobs_planned:
                logger.info(
                    f"Distributed sweep completed: ✓ {self.jobs_completed} completed, ✗ {self.jobs_failed} failed"
                )
                break
            elif queue_empty and self.jobs_submitted >= self.total_jobs_planned:
                # All jobs submitted, wait for stragglers
                running_jobs = job_counts["RUNNING"]
                if running_jobs == 0 and total_finished >= self.jobs_submitted:
                    logger.info(
                        f"Distributed sweep completed: ✓ {self.jobs_completed} completed, ✗ {self.jobs_failed} failed"
                    )
                    break

            await asyncio.sleep(5)  # Check every 5 seconds

    async def _post_process_results(self, is_completion_run: bool):
        """Post-process results including normalization and collection."""
        # Normalize results structure
        await self.result_collector.normalize_results_structure(
            self.task_to_source,
            is_completion_run=is_completion_run,
            preserve_existing_mapping=self.preserve_existing_mapping,
        )

        # Collect scripts and logs
        await self.result_collector.collect_scripts_and_logs()

        # Save source mapping
        await self._save_source_mapping()

    async def _stop_components(self):
        """Stop all components."""
        if self.job_distributor:
            await self.job_distributor.stop_distribution()
        if self.job_monitor:
            await self.job_monitor.stop_monitoring()
        if self.result_collector:
            await self.result_collector.stop_collection()

    async def _cleanup_progress_tracking(self):
        """Cleanup progress tracking."""
        if self._progress:
            self._progress.stop()
            if hasattr(self, "_original_log_level"):
                import logging

                logging.getLogger().setLevel(self._original_log_level)

    # Implementation of abstract methods from BaseJobManager
    def submit_single_job(
        self, params: Dict[str, Any], job_name: str, sweep_id: str, **kwargs
    ) -> str:
        """Submit a single job (synchronous interface)."""
        # This is a simplified interface - for distributed jobs, use submit_distributed_sweep
        raise NotImplementedError("Use submit_distributed_sweep for distributed job submission")

    def cancel_job(self, job_id: str, timeout: int = 10) -> bool:
        """Cancel a specific job."""
        # Implementation would need to delegate to the appropriate compute source
        source_name = self.job_to_source.get(job_id)
        if not source_name:
            return False

        source = self.source_by_name.get(source_name)
        if not source:
            return False

        # This would need to be made async in a real implementation
        # For now, this is a placeholder
        return False

    async def cancel_all_jobs(self, timeout: int = 10) -> dict:
        """Cancel all running jobs across all sources."""
        results = {"cancelled": 0, "failed": 0}

        logger.info(f"Cancelling all jobs across {len(self.sources)} sources")

        # Cancel jobs on each source
        cancel_tasks = []
        for source in self.sources:
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

    def wait_for_all_jobs(self, use_progress_bar: bool = False):
        """Wait for all jobs to complete (synchronous interface)."""
        # This would need to be implemented to work with the async architecture
        raise NotImplementedError("Use submit_distributed_sweep which handles waiting internally")

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

                # Extract actual status and timing from task directory
                (
                    actual_status,
                    actual_start_time,
                    actual_complete_time,
                ) = await self._extract_task_status_and_timing(task_name)

                # Prepare new task data with actual directory info
                new_task_data = {
                    "compute_source": source_name,
                    "status": actual_status
                    if actual_status != "UNKNOWN"
                    else (job_info.status if job_info else "unknown"),
                    "start_time": actual_start_time
                    or (
                        job_info.start_time.isoformat()
                        if job_info and job_info.start_time
                        else None
                    ),
                    "complete_time": actual_complete_time
                    or (
                        job_info.complete_time.isoformat()
                        if job_info and job_info.complete_time
                        else None
                    ),
                }

                # For completion runs, be careful about overwriting existing data
                if task_name in existing_assignments:
                    # Keep original data but allow status and timing updates
                    existing_task = existing_assignments[task_name]
                    merged_task = existing_task.copy()

                    # Update status if changed or if we have better info from directory
                    if new_task_data["status"] != "unknown" and new_task_data[
                        "status"
                    ] != existing_task.get("status"):
                        merged_task["status"] = new_task_data["status"]

                    # Update timing if we have actual directory info or if not already set
                    if new_task_data["complete_time"] and (
                        not existing_task.get("complete_time")
                        or new_task_data["status"] in ["COMPLETED", "FAILED", "CANCELLED"]
                    ):
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

    async def _extract_task_status_and_timing(self, task_name: str) -> tuple:
        """Extract actual status and timing information from task directory."""
        try:
            task_dir = self.sweep_dir / "tasks" / task_name
            task_info_file = task_dir / "task_info.txt"

            if not task_info_file.exists():
                # Check if this might be a collection issue - look for the task in job tracking
                task_source = self.task_to_source.get(task_name)
                if task_source and task_source != "local":
                    # This is a remote task - check if we have job info for it
                    for job_id, job_info in self.all_jobs.items():
                        if job_info.source_name == task_source and task_name in job_info.job_name:
                            logger.warning(
                                f"Task {task_name} directory missing but job {job_id} exists with status {job_info.status}"
                            )
                            logger.warning(
                                f"This suggests a result collection issue - using job status"
                            )

                            # Return job-based information
                            return (
                                job_info.status,
                                job_info.start_time.isoformat() if job_info.start_time else None,
                                job_info.complete_time.isoformat()
                                if job_info.complete_time
                                else None,
                            )

                logger.debug(f"No task_info.txt found for {task_name}")
                return "UNKNOWN", None, None

            with open(task_info_file, "r") as f:
                content = f.read()

            # Extract the most recent status
            status_lines = [line for line in content.split("\n") if line.startswith("Status: ")]
            actual_status = "UNKNOWN"
            if status_lines:
                last_status_line = status_lines[-1]
                if "COMPLETED" in last_status_line:
                    actual_status = "COMPLETED"
                elif "FAILED" in last_status_line:
                    actual_status = "FAILED"
                elif "CANCELLED" in last_status_line:
                    actual_status = "CANCELLED"
                elif "RUNNING" in last_status_line:
                    actual_status = "RUNNING"

            # Check if this might be a stale file by comparing with job info
            actual_start_time, actual_complete_time = self._extract_timing_from_content(content)

            # If we have job info for this task, validate the timing
            task_source = self.task_to_source.get(task_name)
            if task_source:
                for job_id, job_info in self.all_jobs.items():
                    if job_info.source_name == task_source and task_name in job_info.job_name:
                        # Check if the file timing is much older than the job timing
                        if job_info.start_time and actual_start_time:
                            try:
                                if DATEUTIL_AVAILABLE:
                                    file_time = parse_date(actual_start_time)
                                else:
                                    # Fallback parsing if dateutil not available
                                    continue
                                job_time = job_info.start_time

                                time_diff = job_time - file_time
                                if time_diff.total_seconds() > 86400:  # More than 24 hours
                                    logger.warning(
                                        f"Task {task_name}: file appears stale (file: {file_time}, job: {job_time})"
                                    )
                                    logger.warning(
                                        f"Using job status {job_info.status} instead of file status {actual_status}"
                                    )
                                    return (
                                        job_info.status,
                                        job_info.start_time.isoformat()
                                        if job_info.start_time
                                        else None,
                                        job_info.complete_time.isoformat()
                                        if job_info.complete_time
                                        else None,
                                    )
                            except Exception as e:
                                logger.debug(f"Error comparing timestamps for {task_name}: {e}")
                        break

            return actual_status, actual_start_time, actual_complete_time

        except Exception as e:
            logger.debug(f"Error extracting task status and timing for {task_name}: {e}")
            return "UNKNOWN", None, None

    def _extract_timing_from_content(self, content: str) -> tuple:
        """Extract timing information from task_info.txt content."""
        actual_start_time = None
        actual_complete_time = None

        for line in content.split("\n"):
            if line.startswith("Start Time:"):
                try:
                    start_time_str = line.split("Start Time:", 1)[1].strip()
                    # Try to parse and convert to ISO format
                    try:
                        if DATEUTIL_AVAILABLE:
                            parsed_time = parse_date(start_time_str)
                            actual_start_time = parsed_time.isoformat()
                        else:
                            # Fallback if dateutil is not available
                            actual_start_time = start_time_str
                    except Exception:
                        actual_start_time = start_time_str
                except:
                    actual_start_time = start_time_str
            elif line.startswith("End Time:"):
                try:
                    end_time_str = line.split("End Time:", 1)[1].strip()
                    # Try to parse and convert to ISO format
                    try:
                        if DATEUTIL_AVAILABLE:
                            parsed_time = parse_date(end_time_str)
                            actual_complete_time = parsed_time.isoformat()
                        else:
                            # Fallback if dateutil is not available
                            actual_complete_time = end_time_str
                    except Exception:
                        actual_complete_time = end_time_str
                except:
                    actual_complete_time = end_time_str

        return actual_start_time, actual_complete_time

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
