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
        self.context = sweep_context
        self.sources = {source.name: source for source in sources}
        self.distribution_strategy = distribution_strategy
        self.max_concurrent_tasks = max_concurrent_tasks
        self.health_check_interval = health_check_interval
        self.result_collection_interval = result_collection_interval

        # Execution state
        self.status = SweepStatus.PENDING
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

        # Task management
        self.pending_tasks: List[Task] = []
        self.active_tasks: Dict[str, Task] = {}  # task_id -> Task
        self.completed_tasks: Dict[str, Task] = {}  # task_id -> Task
        self.failed_tasks: Dict[str, Task] = {}  # task_id -> Task
        self.task_to_source: Dict[str, str] = {}  # task_id -> source_name

        # Distribution state
        self._round_robin_index = 0

        # Background tasks
        self._health_check_task: Optional[asyncio.Task] = None
        self._result_collection_task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None

        # Shutdown control
        self._shutdown_event = asyncio.Event()
        self._cleanup_complete = asyncio.Event()

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
            return await source.setup(self.context)
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

        logger.info(f"Starting sweep {self.context.sweep_id} with {len(tasks)} tasks")

        # Initialize sweep state
        self.status = SweepStatus.RUNNING
        self.start_time = datetime.now()
        self.pending_tasks = tasks.copy()

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

    async def _execute_tasks(self):
        """Distribute and execute all pending tasks."""
        submission_tasks = []

        while self.pending_tasks:
            # Check if we should submit more tasks
            current_active = len(self.active_tasks)
            max_concurrent = self._calculate_max_concurrent_tasks()

            if current_active >= max_concurrent:
                # Wait a bit before checking again
                await asyncio.sleep(1.0)
                continue

            # Select a compute source for the next task
            source = await self._select_compute_source()
            if not source:
                # No available sources, wait and retry
                await asyncio.sleep(5.0)
                continue

            # Submit the next task
            task = self.pending_tasks.pop(0)
            submission_task = asyncio.create_task(self._submit_task_to_source(task, source))
            submission_tasks.append(submission_task)

            # Avoid overwhelming the submission process
            if len(submission_tasks) >= 10:  # Submit in batches
                await asyncio.gather(*submission_tasks, return_exceptions=True)
                submission_tasks = []

        # Wait for any remaining submissions
        if submission_tasks:
            await asyncio.gather(*submission_tasks, return_exceptions=True)

    async def _submit_task_to_source(self, task: Task, source: ComputeSource):
        """Submit a single task to a specific compute source."""
        try:
            # Update task output directory
            task.output_dir = self.context.sweep_dir / "tasks" / task.task_id
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
        """Select an available compute source based on the distribution strategy."""
        available_sources = [source for source in self.sources.values() if source.is_available]

        if not available_sources:
            return None

        if self.distribution_strategy == DistributionStrategy.ROUND_ROBIN:
            source = available_sources[self._round_robin_index % len(available_sources)]
            self._round_robin_index += 1
            return source

        elif self.distribution_strategy == DistributionStrategy.LEAST_LOADED:
            return min(available_sources, key=lambda s: s.current_load)

        elif self.distribution_strategy == DistributionStrategy.RANDOM:
            import random

            return random.choice(available_sources)

        else:  # Default to round-robin
            source = available_sources[self._round_robin_index % len(available_sources)]
            self._round_robin_index += 1
            return source

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

            if result.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                # Task is done, move it to the appropriate collection
                task = self.active_tasks.pop(task_id, None)
                if task:
                    if result.status == TaskStatus.COMPLETED:
                        self.completed_tasks[task_id] = task
                        logger.info(f"Task {task_id} completed successfully")
                    elif result.status == TaskStatus.FAILED:
                        self.failed_tasks[task_id] = task
                        logger.warning(f"Task {task_id} failed")
                    else:  # CANCELLED
                        self.failed_tasks[task_id] = task  # Treat as failed
                        logger.info(f"Task {task_id} was cancelled")

        except Exception as e:
            logger.error(f"Error updating status for task {task_id}: {e}")

    async def _start_background_tasks(self):
        """Start background monitoring and collection tasks."""
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        self._result_collection_task = asyncio.create_task(self._result_collection_loop())
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def _health_check_loop(self):
        """Periodic health checking of compute sources."""
        while not self._shutdown_event.is_set():
            try:
                for source in self.sources.values():
                    await source.update_health_status()

                await asyncio.sleep(self.health_check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(60)  # Back off on error

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
        """Log current sweep progress."""
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

        logger.info(
            f"Sweep progress: {completed + failed}/{total_tasks} tasks finished "
            f"({completed} completed, {failed} failed), {active} active, {pending} pending"
        )

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
            sweep_id=self.context.sweep_id,
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
        logger.info(f"Cancelling sweep {self.context.sweep_id}")

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
        """Get current sweep status information.

        Returns:
            Dictionary with current status information
        """
        total_tasks = (
            len(self.completed_tasks)
            + len(self.failed_tasks)
            + len(self.active_tasks)
            + len(self.pending_tasks)
        )

        # Get source health
        source_health = {}
        for source_name, source in self.sources.items():
            source_health[source_name] = {
                "health": source.stats.health_status.value,
                "active_tasks": len(source.active_tasks),
                "available_slots": source.stats.available_slots,
                "utilization": f"{source.current_load:.1f}%",
            }

        return {
            "sweep_id": self.context.sweep_id,
            "status": self.status.value,
            "progress": {
                "total_tasks": total_tasks,
                "completed": len(self.completed_tasks),
                "failed": len(self.failed_tasks),
                "active": len(self.active_tasks),
                "pending": len(self.pending_tasks),
                "completion_rate": (len(self.completed_tasks) / total_tasks * 100)
                if total_tasks > 0
                else 0,
            },
            "sources": source_health,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "duration": (datetime.now() - self.start_time).total_seconds()
            if self.start_time
            else None,
        }
