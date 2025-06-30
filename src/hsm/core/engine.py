"""Central SweepEngine for unified sweep execution.

The SweepEngine is the core orchestrator that manages sweep execution across
multiple compute sources, providing a unified interface regardless of where
tasks are actually executed.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import logging
from typing import Any, Dict, List, Optional

from ..compute.base import (
    CollectionResult,
    ComputeSource,
    SweepContext,
    Task,
    TaskStatus,
)

logger = logging.getLogger(__name__)


class SweepStatus(Enum):
    """Overall sweep execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class DistributionStrategy(Enum):
    """Strategies for distributing tasks across compute sources."""

    ROUND_ROBIN = "round_robin"
    LEAST_LOADED = "least_loaded"
    RANDOM = "random"
    PRIORITY_BASED = "priority_based"


@dataclass
class SweepResult:
    """Result of sweep execution."""

    sweep_id: str
    status: SweepStatus
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    cancelled_tasks: int

    start_time: datetime
    end_time: Optional[datetime] = None

    # Source utilization
    source_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Result collection info
    collection_results: List[CollectionResult] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_tasks == 0:
            return 0.0
        return (self.completed_tasks / self.total_tasks) * 100.0

    @property
    def duration(self) -> Optional[float]:
        """Calculate sweep duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


class SweepEngine:
    """Central orchestrator for sweep execution across multiple compute sources.

    The SweepEngine provides a unified interface for running parameter sweeps
    regardless of the underlying compute infrastructure. It handles:
    - Task distribution across multiple compute sources
    - Progress monitoring and health checking
    - Result collection and centralization
    - Error handling and recovery
    - Cross-mode completion support
    """

    def __init__(
        self,
        sweep_context: SweepContext,
        sources: List[ComputeSource],
        distribution_strategy: DistributionStrategy = DistributionStrategy.ROUND_ROBIN,
        max_concurrent_tasks: Optional[int] = None,
        health_check_interval: int = 300,  # 5 minutes
        result_collection_interval: int = 60,  # 1 minute
    ):
        """Initialize the SweepEngine.

        Args:
            sweep_context: Context information for the sweep
            sources: List of compute sources to use
            distribution_strategy: Strategy for distributing tasks
            max_concurrent_tasks: Global limit on concurrent tasks (optional)
            health_check_interval: Interval between health checks in seconds
            result_collection_interval: Interval between result collection in seconds
        """
        self.sweep_context = sweep_context
        self.sources = {source.name: source for source in sources}
        self.distribution_strategy = distribution_strategy
        self.max_concurrent_tasks = max_concurrent_tasks or self._calculate_max_concurrent_tasks()
        self.health_check_interval = health_check_interval
        self.result_collection_interval = result_collection_interval

        # Execution state
        self.status = SweepStatus.PENDING
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

        # Task management
        self.pending_tasks: Dict[str, Task] = {}
        self.active_tasks: Dict[str, Task] = {}
        self.completed_tasks: Dict[str, Task] = {}
        self.failed_tasks: Dict[str, Task] = {}
        self.task_to_source: Dict[str, str] = {}  # task_id -> source_name

        # Background tasks
        self._health_check_task: Optional[asyncio.Task] = None
        self._result_collection_task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None

        # Control
        self._shutdown_event = asyncio.Event()
        self._cleanup_complete = asyncio.Event()

        # Round-robin state
        self._round_robin_index = 0

        # Enhanced health monitoring and failure tracking
        self.source_failure_counts: Dict[str, int] = {}  # source_name -> failure_count
        self.source_job_counts: Dict[str, int] = {}  # source_name -> total_job_count
        self.source_health_failures: Dict[
            str, int
        ] = {}  # source_name -> consecutive_health_failures
        self.disabled_sources: set = set()  # Set of disabled source names

        # Failure tracking configuration
        self.source_failure_threshold: float = 0.4  # Disable source if 40% of jobs fail
        self.min_jobs_for_failsafe: int = 5  # Need at least 5 jobs before considering failsafe
        self.health_check_failure_threshold: int = (
            3  # Disable after 3 consecutive health check failures
        )
        self.enable_source_failsafe: bool = True  # Enable automatic source disabling
        self.auto_disable_unhealthy_sources: bool = True

        logger.info(
            f"SweepEngine initialized with {len(self.sources)} sources, max_concurrent_tasks={self.max_concurrent_tasks}"
        )
        logger.info(
            f"Health monitoring: failure_threshold={self.source_failure_threshold}, min_jobs={self.min_jobs_for_failsafe}"
        )
        logger.info(f"Available sources: {list(self.sources.keys())}")
        logger.info(
            f"Health check interval: {self.health_check_interval}s, Result collection interval: {self.result_collection_interval}s"
        )

    async def setup_sources(self) -> bool:
        """Setup all compute sources for sweep execution.

        Returns:
            True if at least one source was successfully setup
        """
        logger.info(f"Setting up {len(self.sources)} compute sources")

        setup_tasks = []
        for source in self.sources.values():
            setup_tasks.append(self._setup_source(source))

        # Setup sources concurrently
        results = await asyncio.gather(*setup_tasks, return_exceptions=True)

        successful_sources = []
        failed_sources = []

        for source, result in zip(self.sources.values(), results):
            if isinstance(result, Exception):
                logger.error(f"Setup failed for {source.name}: {result}")
                failed_sources.append(source.name)
            elif result:
                logger.info(f"✓ Setup successful for {source.name}")
                successful_sources.append(source.name)
            else:
                logger.error(f"Setup failed for {source.name}")
                failed_sources.append(source.name)

        # Remove failed sources
        for source_name in failed_sources:
            del self.sources[source_name]

        if not successful_sources:
            logger.error("No compute sources successfully set up")
            return False

        if failed_sources:
            logger.warning(f"Some sources failed setup and were removed: {failed_sources}")

        logger.info(f"Successfully set up {len(successful_sources)} compute sources")
        return True

    async def _setup_source(self, source: ComputeSource) -> bool:
        """Setup a single compute source."""
        try:
            return await source.setup(self.sweep_context)
        except Exception as e:
            logger.error(f"Exception during setup of {source.name}: {e}")
            return False

    async def run_sweep(self, tasks: List[Task]) -> SweepResult:
        """Run a complete parameter sweep.

        Args:
            tasks: List of tasks to execute

        Returns:
            SweepResult with execution summary
        """
        if not self.sources:
            raise RuntimeError("No compute sources available")

        if not tasks:
            raise ValueError("No tasks provided")

        logger.info(f"Starting sweep {self.sweep_context.sweep_id} with {len(tasks)} tasks")

        # Initialize sweep state
        self.status = SweepStatus.RUNNING
        self.start_time = datetime.now()
        self.pending_tasks = tasks.copy()

        # Create sweep directory structure
        await self._setup_sweep_directories()

        try:
            # Start background tasks
            await self._start_background_tasks()

            # Distribute and execute tasks
            await self._execute_tasks()

            # Wait for all tasks to complete
            await self._wait_for_completion()

            # Collect final results
            await self._collect_all_results()

            # Determine final status
            if self.failed_tasks and not self.completed_tasks:
                self.status = SweepStatus.FAILED
            elif self.failed_tasks:
                self.status = SweepStatus.COMPLETED  # Partial success
            else:
                self.status = SweepStatus.COMPLETED

        except asyncio.CancelledError:
            self.status = SweepStatus.CANCELLED
            logger.info("Sweep execution cancelled")
        except Exception as e:
            self.status = SweepStatus.FAILED
            logger.error(f"Sweep execution failed: {e}")
            raise
        finally:
            self.end_time = datetime.now()
            await self._cleanup()

        return self._create_result()

    async def _setup_sweep_directories(self):
        """Setup the sweep directory structure."""
        base_dir = self.sweep_context.sweep_dir

        # Create main directories
        directories = [
            base_dir / "tasks",
            base_dir / "logs",
            base_dir / "logs" / "sources",
            base_dir / "logs" / "errors",
            base_dir / "scripts",
            base_dir / "scripts" / "job_scripts",
            base_dir / "scripts" / "setup_scripts",
            base_dir / "reports",
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

        logger.debug(f"Created sweep directory structure at {base_dir}")

    async def _execute_tasks(self):
        """Execute all pending tasks using available compute sources."""
        # Convert pending tasks list to dict if needed
        if isinstance(self.pending_tasks, list):
            pending_dict = {task.task_id: task for task in self.pending_tasks}
            self.pending_tasks = pending_dict

        logger.info(f"Starting task execution: {len(self.pending_tasks)} tasks pending")

        while self.pending_tasks and not self._shutdown_event.is_set():
            # Check if we have capacity for more tasks
            if len(self.active_tasks) >= self.max_concurrent_tasks:
                logger.debug(
                    f"At max capacity ({len(self.active_tasks)}/{self.max_concurrent_tasks}), waiting..."
                )
                await asyncio.sleep(2.0)
                continue

            # Get next task
            task_id, task = next(iter(self.pending_tasks.items()))
            task = self.pending_tasks.pop(task_id)

            # Select compute source
            source = await self._select_compute_source()
            if source is None:
                # No sources available, put task back and wait
                self.pending_tasks[task_id] = task
                logger.warning("No compute sources available, waiting...")
                await asyncio.sleep(5.0)
                continue

            # Submit task to source
            await self._submit_task_to_source(task, source)

            # Brief pause to avoid overwhelming sources
            await asyncio.sleep(0.1)

        logger.info("Task submission completed")

    async def _submit_task_to_source(self, task: Task, source: ComputeSource):
        """Submit a single task to a specific compute source."""
        try:
            # Update task output directory
            task.output_dir = self.sweep_context.sweep_dir / "tasks" / task.task_id
            task.output_dir.mkdir(parents=True, exist_ok=True)

            # Submit task
            result = await source.submit_task(task)

            if result.status in [TaskStatus.QUEUED, TaskStatus.RUNNING]:
                # Task submitted successfully
                self.active_tasks[task.task_id] = task
                self.task_to_source[task.task_id] = source.name
                logger.info(f"Task {task.task_id} submitted to {source.name}")
            else:
                # Submission failed
                self.failed_tasks[task.task_id] = task
                logger.error(
                    f"Failed to submit task {task.task_id} to {source.name}: {result.message}"
                )

        except Exception as e:
            logger.error(f"Exception submitting task {task.task_id} to {source.name}: {e}")
            self.failed_tasks[task.task_id] = task

    async def _select_compute_source(self) -> Optional[ComputeSource]:
        """Select an available compute source for task execution.

        Returns:
            ComputeSource if one is available, None otherwise
        """
        # Get available sources (not disabled, not at capacity)
        available_sources = []
        for source_name, source in self.sources.items():
            if source_name in self.disabled_sources:
                logger.debug(f"Skipping disabled source: {source_name}")
                continue

            # Check if source has capacity
            if hasattr(source, "stats") and hasattr(source.stats, "active_tasks"):
                active_tasks = source.stats.active_tasks
                max_tasks = getattr(source.stats, "max_parallel_tasks", source.max_concurrent_tasks)
                if active_tasks >= max_tasks:
                    logger.debug(f"Source {source_name} at capacity: {active_tasks}/{max_tasks}")
                    continue

            available_sources.append((source_name, source))

        if not available_sources:
            logger.warning("No available compute sources for task assignment")
            if self.disabled_sources:
                logger.warning(f"Disabled sources: {list(self.disabled_sources)}")
            return None

        # Select source based on distribution strategy
        if self.distribution_strategy == DistributionStrategy.ROUND_ROBIN:
            # Round-robin selection
            selected_name, selected_source = available_sources[
                self._round_robin_index % len(available_sources)
            ]
            self._round_robin_index += 1
            logger.debug(f"Selected source via round-robin: {selected_name}")
            return selected_source

        elif self.distribution_strategy == DistributionStrategy.LEAST_LOADED:
            # Select source with lowest utilization
            def get_utilization(source_info):
                source_name, source = source_info
                if hasattr(source, "stats") and hasattr(source.stats, "active_tasks"):
                    active = source.stats.active_tasks
                    max_tasks = getattr(
                        source.stats, "max_parallel_tasks", source.max_concurrent_tasks
                    )
                    return active / max_tasks if max_tasks > 0 else 0
                return 0

            selected_name, selected_source = min(available_sources, key=get_utilization)
            logger.debug(f"Selected source via least-loaded: {selected_name}")
            return selected_source

        elif self.distribution_strategy == DistributionStrategy.RANDOM:
            # Random selection
            import random

            selected_name, selected_source = random.choice(available_sources)
            logger.debug(f"Selected source via random: {selected_name}")
            return selected_source

        else:  # Default to round-robin
            selected_name, selected_source = available_sources[0]
            logger.debug(f"Selected source via default: {selected_name}")
            return selected_source

    def _calculate_max_concurrent_tasks(self) -> int:
        """Calculate the maximum number of concurrent tasks."""
        if self.max_concurrent_tasks:
            return self.max_concurrent_tasks

        # Sum available slots across all sources
        total_slots = sum(source.stats.available_slots for source in self.sources.values())
        return max(1, total_slots)

    async def _wait_for_completion(self):
        """Wait for all active tasks to complete."""
        while self.active_tasks and not self._shutdown_event.is_set():
            # Update task statuses
            await self._update_task_statuses()

            # Wait a bit before checking again
            await asyncio.sleep(10.0)

    async def _update_task_statuses(self):
        """Update the status of all active tasks."""
        status_tasks = []

        for task_id in list(self.active_tasks.keys()):
            source_name = self.task_to_source.get(task_id)
            if not source_name or source_name not in self.sources:
                continue

            source = self.sources[source_name]
            status_task = asyncio.create_task(self._update_single_task_status(task_id, source))
            status_tasks.append(status_task)

        if status_tasks:
            await asyncio.gather(*status_tasks, return_exceptions=True)

    async def _update_single_task_status(self, task_id: str, source: ComputeSource):
        """Update the status of a single task."""
        try:
            result = await source.get_task_status(task_id)

            # Handle both TaskResult and TaskStatus returns for backward compatibility
            if hasattr(result, "status"):
                # It's a TaskResult
                status = result.status
                error_message = getattr(result, "error_message", None)
                exit_code = getattr(result, "exit_code", None)
            else:
                # It's a TaskStatus (for backward compatibility)
                status = result
                error_message = None
                exit_code = None

            # Log status changes
            if task_id in self.active_tasks:
                current_task = self.active_tasks[task_id]
                if hasattr(current_task, "status") and current_task.status != status:
                    logger.debug(
                        f"Task {task_id} status changed: {getattr(current_task, 'status', 'UNKNOWN')} -> {status}"
                    )

            if status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                # Task is done, move it to the appropriate collection
                task = self.active_tasks.pop(task_id, None)
                if task:
                    # Update task with final status information
                    task.status = status
                    if error_message:
                        task.error_message = error_message
                    if exit_code is not None:
                        task.exit_code = exit_code

                    if status == TaskStatus.COMPLETED:
                        self.completed_tasks[task_id] = task
                        logger.info(f"Task {task_id} completed successfully")
                    elif status == TaskStatus.FAILED:
                        self.failed_tasks[task_id] = task
                        if error_message:
                            logger.warning(f"Task {task_id} failed: {error_message}")
                        elif exit_code is not None:
                            logger.warning(f"Task {task_id} failed with exit code {exit_code}")
                        else:
                            logger.warning(f"Task {task_id} failed")
                    else:  # CANCELLED
                        self.failed_tasks[task_id] = task  # Treat as failed
                        logger.info(f"Task {task_id} was cancelled")

                    # Update source statistics
                    if hasattr(source, "stats"):
                        if status == TaskStatus.COMPLETED:
                            source.stats.tasks_completed += 1
                        else:
                            source.stats.tasks_failed += 1

            elif status == TaskStatus.RUNNING:
                # Update task status if it's now running
                if task_id in self.active_tasks:
                    self.active_tasks[task_id].status = status

        except Exception as e:
            logger.error(f"Error updating status for task {task_id}: {e}")
            # If we can't get status, assume the task failed
            task = self.active_tasks.pop(task_id, None)
            if task:
                task.status = TaskStatus.FAILED
                task.error_message = f"Status check failed: {str(e)}"
                self.failed_tasks[task_id] = task
                logger.error(f"Task {task_id} marked as failed due to status check error")

    async def _start_background_tasks(self):
        """Start background monitoring and collection tasks."""
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        self._result_collection_task = asyncio.create_task(self._result_collection_loop())
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def _health_check_loop(self):
        """Periodic health checking of compute sources with failure tracking."""
        while not self._shutdown_event.is_set():
            try:
                # Update source failure tracking
                self._update_source_failure_tracking()

                # Check health of all sources
                for source_name, source in self.sources.items():
                    if source_name in self.disabled_sources:
                        continue

                    try:
                        # Perform health check
                        health_report = await source.health_check()

                        # Initialize health failure tracking if needed
                        if source_name not in self.source_health_failures:
                            self.source_health_failures[source_name] = 0

                        # Check health status
                        if hasattr(health_report, "status"):
                            health_status = health_report.status
                        else:
                            # For backward compatibility, assume healthy if no status
                            health_status = "healthy"

                        if health_status == "unhealthy":
                            self.source_health_failures[source_name] += 1
                            error_msg = getattr(health_report, "error", "Unknown error")
                            logger.warning(
                                f"Health check failed for {source_name}: {error_msg} "
                                f"(consecutive failures: {self.source_health_failures[source_name]})"
                            )

                            # Disable source if too many consecutive failures
                            if (
                                self.auto_disable_unhealthy_sources
                                and self.source_health_failures[source_name]
                                >= self.health_check_failure_threshold
                            ):
                                logger.error(
                                    f"Disabling source '{source_name}' due to {self.source_health_failures[source_name]} "
                                    f"consecutive health check failures"
                                )
                                self.disabled_sources.add(source_name)

                        elif health_status == "degraded":
                            # Reset consecutive failures for degraded but functional sources
                            self.source_health_failures[source_name] = max(
                                0, self.source_health_failures[source_name] - 1
                            )

                            # Log degraded status with details
                            warning_msg = getattr(health_report, "warning", "Degraded performance")
                            logger.warning(f"Source {source_name} is degraded: {warning_msg}")

                        else:  # healthy
                            # Reset consecutive failures for healthy sources
                            self.source_health_failures[source_name] = 0

                        # Log critical issues
                        if (
                            hasattr(health_report, "disk_status")
                            and health_report.disk_status == "critical"
                        ):
                            disk_msg = getattr(health_report, "disk_message", "Unknown disk issue")
                            logger.error(
                                f"CRITICAL: {source_name} has critical disk space shortage: {disk_msg}"
                            )

                    except Exception as e:
                        logger.error(f"Health check error for {source_name}: {e}")
                        # Treat health check errors as health failures
                        if source_name not in self.source_health_failures:
                            self.source_health_failures[source_name] = 0
                        self.source_health_failures[source_name] += 1

                # Log current disabled sources
                if self.disabled_sources:
                    logger.debug(f"Currently disabled sources: {', '.join(self.disabled_sources)}")

                await asyncio.sleep(self.health_check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(60)  # Back off on error

    def _update_source_failure_tracking(self):
        """Update source failure counts based on current task statuses."""
        # Reset failure counts for each update
        for source_name in self.sources.keys():
            self.source_failure_counts[source_name] = 0
            if source_name not in self.source_job_counts:
                self.source_job_counts[source_name] = 0

        # Count failures and total jobs per source
        all_tasks = {**self.completed_tasks, **self.failed_tasks, **self.active_tasks}
        for task_id, task in all_tasks.items():
            source_name = self.task_to_source.get(task_id)
            if source_name and source_name in self.sources:
                # Count total jobs
                self.source_job_counts[source_name] += 1

                # Count failures
                if hasattr(task, "status") and task.status in [
                    TaskStatus.FAILED,
                    TaskStatus.CANCELLED,
                ]:
                    self.source_failure_counts[source_name] += 1

        # Check for sources that should be disabled due to high failure rate
        if self.enable_source_failsafe:
            self._check_source_failure_rates()

    def _check_source_failure_rates(self):
        """Check source failure rates and disable sources that exceed threshold."""
        for source_name in list(self.source_job_counts.keys()):
            if source_name in self.disabled_sources:
                continue

            job_count = self.source_job_counts[source_name]
            failure_count = self.source_failure_counts.get(source_name, 0)

            # Only check sources with minimum number of jobs
            if job_count >= self.min_jobs_for_failsafe:
                failure_rate = failure_count / job_count

                if failure_rate >= self.source_failure_threshold:
                    logger.warning(
                        f"Disabling source '{source_name}' due to high failure rate: "
                        f"{failure_count}/{job_count} ({failure_rate:.1%}) >= {self.source_failure_threshold:.1%}"
                    )
                    self.disabled_sources.add(source_name)

                    # Log details about the failures
                    failed_tasks = [
                        task
                        for task_id, task in {**self.failed_tasks}.items()
                        if self.task_to_source.get(task_id) == source_name
                    ]

                    if failed_tasks:
                        failed_names = [
                            getattr(task, "task_id", "unknown") for task in failed_tasks[-3:]
                        ]
                        logger.info(f"Recent failures on {source_name}: {failed_names}")

    def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status of all sources."""
        status = {
            "sources": {},
            "disabled_sources": list(self.disabled_sources),
            "failure_tracking": {
                "threshold": self.source_failure_threshold,
                "min_jobs": self.min_jobs_for_failsafe,
                "health_failure_threshold": self.health_check_failure_threshold,
            },
        }

        for source_name, source in self.sources.items():
            job_count = self.source_job_counts.get(source_name, 0)
            failure_count = self.source_failure_counts.get(source_name, 0)
            health_failures = self.source_health_failures.get(source_name, 0)

            failure_rate = (failure_count / job_count) if job_count > 0 else 0.0

            status["sources"][source_name] = {
                "enabled": source_name not in self.disabled_sources,
                "total_jobs": job_count,
                "failed_jobs": failure_count,
                "failure_rate": failure_rate,
                "consecutive_health_failures": health_failures,
                "health_status": getattr(source.stats, "health_status", "unknown").value
                if hasattr(source, "stats")
                else "unknown",
            }

        return status

    async def _result_collection_loop(self):
        """Periodic result collection from compute sources."""
        while not self._shutdown_event.is_set():
            try:
                await self._collect_results_from_sources()
                await asyncio.sleep(self.result_collection_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in result collection loop: {e}")
                await asyncio.sleep(60)  # Back off on error

    async def _monitor_loop(self):
        """Periodic monitoring and logging of sweep progress."""
        while not self._shutdown_event.is_set():
            try:
                self._log_progress()
                await asyncio.sleep(30)  # Log progress every 30 seconds

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
                await asyncio.sleep(60)

    def _log_progress(self):
        """Log current sweep progress with enhanced information."""
        total_tasks = (
            len(self.pending_tasks)
            + len(self.active_tasks)
            + len(self.completed_tasks)
            + len(self.failed_tasks)
        )
        completed = len(self.completed_tasks)
        failed = len(self.failed_tasks)
        active = len(self.active_tasks)
        pending = len(self.pending_tasks)

        # Calculate progress percentage
        finished = completed + failed
        progress_pct = (finished / total_tasks * 100) if total_tasks > 0 else 0

        # Log main progress
        logger.info(
            f"Sweep progress: {finished}/{total_tasks} tasks finished "
            f"({completed} completed, {failed} failed), {active} active, {pending} pending"
            f" [{progress_pct:.1f}%]"
        )

        # Log source utilization
        if self.sources:
            source_info = []
            for source_name, source in self.sources.items():
                if source_name in self.disabled_sources:
                    source_info.append(f"{source_name}:DISABLED")
                elif hasattr(source, "stats") and hasattr(source.stats, "active_tasks"):
                    active_tasks = source.stats.active_tasks
                    max_tasks = getattr(
                        source.stats, "max_parallel_tasks", source.max_concurrent_tasks
                    )
                    source_info.append(f"{source_name}:{active_tasks}/{max_tasks}")
                else:
                    source_info.append(f"{source_name}:UNKNOWN")

            logger.info(f"Source utilization: {', '.join(source_info)}")

        # Log failure rates if we have enough data
        if self.enable_source_failsafe:
            high_failure_sources = []
            for source_name in self.sources.keys():
                job_count = self.source_job_counts.get(source_name, 0)
                failure_count = self.source_failure_counts.get(source_name, 0)
                if job_count >= 3:  # Only show if we have some data
                    failure_rate = failure_count / job_count
                    if failure_rate > 0.2:  # Show if > 20% failure rate
                        high_failure_sources.append(f"{source_name}:{failure_rate:.1%}")

            if high_failure_sources:
                logger.warning(f"High failure rates: {', '.join(high_failure_sources)}")

    async def _collect_results_from_sources(self):
        """Collect results from all compute sources."""
        collection_tasks = []

        for source in self.sources.values():
            if hasattr(source, "collect_results"):
                task = asyncio.create_task(self._collect_from_source(source))
                collection_tasks.append(task)

        if collection_tasks:
            await asyncio.gather(*collection_tasks, return_exceptions=True)

    async def _collect_from_source(self, source: ComputeSource):
        """Collect results from a specific compute source."""
        try:
            # Get completed task IDs for this source
            completed_task_ids = [
                task_id
                for task_id, source_name in self.task_to_source.items()
                if source_name == source.name and task_id in self.completed_tasks
            ]

            if completed_task_ids:
                result = await source.collect_results(completed_task_ids)
                logger.debug(f"Collected {len(result.collected_tasks)} tasks from {source.name}")

        except Exception as e:
            logger.error(f"Error collecting results from {source.name}: {e}")

    async def _collect_all_results(self):
        """Final collection of all results."""
        logger.info("Performing final result collection...")
        await self._collect_results_from_sources()

    async def _cleanup(self):
        """Cleanup resources and background tasks."""
        logger.info("Cleaning up sweep resources...")

        # Signal shutdown
        self._shutdown_event.set()

        # Cancel background tasks
        tasks_to_cancel = [
            self._health_check_task,
            self._result_collection_task,
            self._monitor_task,
        ]

        for task in tasks_to_cancel:
            if task and not task.done():
                task.cancel()

        # Wait for background tasks to finish
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        # Cleanup compute sources
        cleanup_tasks = []
        for source in self.sources.values():
            cleanup_tasks.append(source.cleanup())

        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        self._cleanup_complete.set()
        logger.info("Sweep cleanup completed")

    def _create_result(self) -> SweepResult:
        """Create the final sweep result."""
        total_tasks = (
            len(self.completed_tasks)
            + len(self.failed_tasks)
            + len(self.active_tasks)
            + len(self.pending_tasks)
        )

        # Gather source statistics
        source_stats = {}
        for source_name, source in self.sources.items():
            stats = source.stats
            source_stats[source_name] = {
                "tasks_submitted": stats.tasks_submitted,
                "tasks_completed": stats.tasks_completed,
                "tasks_failed": stats.tasks_failed,
                "success_rate": stats.success_rate,
                "average_duration": stats.average_task_duration,
                "health_status": stats.health_status.value,
            }

        return SweepResult(
            sweep_id=self.sweep_context.sweep_id,
            status=self.status,
            total_tasks=total_tasks,
            completed_tasks=len(self.completed_tasks),
            failed_tasks=len(self.failed_tasks),
            cancelled_tasks=len(
                [
                    t
                    for t in self.active_tasks.values()
                    if hasattr(t, "status") and t.status == TaskStatus.CANCELLED
                ]
            ),
            start_time=self.start_time,
            end_time=self.end_time,
            source_stats=source_stats,
        )

    async def cancel_sweep(self) -> bool:
        """Cancel the running sweep.

        Returns:
            True if cancellation was successful
        """
        logger.info(f"Cancelling sweep {self.sweep_context.sweep_id}")

        self.status = SweepStatus.CANCELLED

        # Cancel all active tasks
        cancel_tasks = []
        for task_id in list(self.active_tasks.keys()):
            source_name = self.task_to_source.get(task_id)
            if source_name and source_name in self.sources:
                source = self.sources[source_name]
                cancel_tasks.append(source.cancel_task(task_id))

        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)

        # Trigger cleanup
        await self._cleanup()

        return True

    async def get_sweep_status(self) -> Dict[str, Any]:
        """Get comprehensive sweep status including health information."""
        # Calculate basic progress
        total_tasks = (
            len(self.pending_tasks)
            + len(self.active_tasks)
            + len(self.completed_tasks)
            + len(self.failed_tasks)
        )
        completed = len(self.completed_tasks)
        failed = len(self.failed_tasks)
        progress_pct = ((completed + failed) / total_tasks * 100) if total_tasks > 0 else 0

        # Get health status
        health_status = self.get_health_status()

        return {
            "sweep_id": self.sweep_context.sweep_id,
            "status": self.status.value,
            "progress": {
                "total_tasks": total_tasks,
                "completed": completed,
                "failed": failed,
                "active": len(self.active_tasks),
                "pending": len(self.pending_tasks),
                "progress_percentage": progress_pct,
            },
            "sources": {
                source_name: {
                    "enabled": source_name not in self.disabled_sources,
                    "active_tasks": getattr(source.stats, "active_tasks", 0)
                    if hasattr(source, "stats")
                    else 0,
                    "max_tasks": getattr(
                        source.stats, "max_parallel_tasks", source.max_concurrent_tasks
                    )
                    if hasattr(source, "stats")
                    else source.max_concurrent_tasks,
                    "health_status": getattr(source.stats, "health_status", "unknown").value
                    if hasattr(source, "stats")
                    else "unknown",
                }
                for source_name, source in self.sources.items()
            },
            "health": health_status,
            "distribution_strategy": self.distribution_strategy.value,
        }
