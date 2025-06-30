"""Enhanced ComputeSource base class for unified sweep execution.

This module defines the standardized interface that all compute sources must implement,
along with supporting data structures for tasks, results, and execution context.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Enumeration of possible task statuses."""

    PENDING = "PENDING"  # Task created but not yet submitted
    QUEUED = "QUEUED"  # Task submitted to compute source queue
    RUNNING = "RUNNING"  # Task is currently executing
    COMPLETED = "COMPLETED"  # Task completed successfully
    FAILED = "FAILED"  # Task failed with error
    CANCELLED = "CANCELLED"  # Task was cancelled
    UNKNOWN = "UNKNOWN"  # Status cannot be determined


class HealthStatus(Enum):
    """Enumeration of compute source health statuses."""

    HEALTHY = "healthy"  # Source is functioning normally
    DEGRADED = "degraded"  # Source has issues but is still usable
    UNHEALTHY = "unhealthy"  # Source has serious problems
    UNKNOWN = "unknown"  # Health status cannot be determined


@dataclass
class Task:
    """Represents a single task in a sweep."""

    task_id: str  # Unique identifier (e.g., "task_001")
    params: Dict[str, Any]  # Parameter dictionary for this task
    sweep_id: str  # ID of the parent sweep
    wandb_group: Optional[str] = None  # W&B group name
    priority: int = 0  # Task priority (higher = more important)
    retry_count: int = 0  # Number of times this task has been retried
    max_retries: int = 3  # Maximum number of retries allowed

    # Execution metadata
    submit_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    complete_time: Optional[datetime] = None

    # Result information
    exit_code: Optional[int] = None
    error_message: Optional[str] = None
    output_dir: Optional[Path] = None

    def __post_init__(self):
        """Convert output_dir to Path if it's a string."""
        if self.output_dir and not isinstance(self.output_dir, Path):
            self.output_dir = Path(self.output_dir)


@dataclass
class TaskResult:
    """Result of task submission or status query."""

    task_id: str
    status: TaskStatus
    job_id: Optional[str] = None  # Compute source specific job ID
    message: Optional[str] = None  # Status message or error description
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        """Set timestamp if not provided."""
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class CollectionResult:
    """Result of collecting task outputs from a compute source."""

    collected_tasks: List[str] = field(default_factory=list)  # Successfully collected task IDs
    failed_tasks: List[str] = field(default_factory=list)  # Failed to collect task IDs
    errors: List[str] = field(default_factory=list)  # Collection error messages
    total_size_mb: float = 0.0  # Total size of collected data in MB

    @property
    def success(self) -> bool:
        """True if collection was successful (no failed tasks)."""
        return len(self.failed_tasks) == 0

    @property
    def partial_success(self) -> bool:
        """True if some tasks were collected successfully."""
        return len(self.collected_tasks) > 0


@dataclass
class HealthReport:
    """Health status report from a compute source."""

    source_name: str
    status: HealthStatus
    timestamp: datetime

    # Resource information
    available_slots: int = 0
    max_slots: int = 0
    active_tasks: int = 0

    # System metrics (optional)
    cpu_usage: Optional[float] = None  # CPU usage percentage
    memory_usage: Optional[float] = None  # Memory usage percentage
    disk_free_gb: Optional[float] = None  # Free disk space in GB
    load_average: Optional[float] = None  # System load average

    # Health details
    message: Optional[str] = None  # Human readable status message
    issues: List[str] = field(default_factory=list)  # List of identified issues
    warnings: List[str] = field(default_factory=list)  # List of warnings

    @property
    def utilization(self) -> float:
        """Compute utilization as a fraction (0.0 to 1.0)."""
        if self.max_slots == 0:
            return 0.0
        return min(1.0, self.active_tasks / self.max_slots)


@dataclass
class SweepContext:
    """Context information for sweep execution."""

    sweep_id: str
    sweep_dir: Path
    config: Dict[str, Any]  # Sweep configuration

    # Execution settings
    python_path: str = "python"
    script_path: Optional[str] = None
    project_dir: Optional[str] = None

    # Runtime settings
    max_parallel_tasks: Optional[int] = None
    walltime: Optional[str] = None
    resources: Optional[str] = None

    def __post_init__(self):
        """Convert paths to Path objects."""
        if not isinstance(self.sweep_dir, Path):
            self.sweep_dir = Path(self.sweep_dir)


@dataclass
class ComputeSourceStats:
    """Statistics for a compute source."""

    source_name: str
    source_type: str
    max_parallel_tasks: int

    # Counters
    tasks_submitted: int = 0
    tasks_completed: int = 0
    tasks_failed: int = 0
    tasks_cancelled: int = 0

    # Performance metrics
    average_task_duration: Optional[float] = None  # Average task duration in seconds
    success_rate: float = 0.0  # Success rate as percentage

    # Health tracking
    health_status: HealthStatus = HealthStatus.UNKNOWN
    last_health_check: Optional[datetime] = None
    consecutive_failures: int = 0

    @property
    def active_tasks(self) -> int:
        """Number of currently active tasks."""
        return (
            self.tasks_submitted - self.tasks_completed - self.tasks_failed - self.tasks_cancelled
        )

    @property
    def utilization(self) -> float:
        """Current utilization as a fraction."""
        if self.max_parallel_tasks == 0:
            return 0.0
        return min(1.0, self.active_tasks / self.max_parallel_tasks)

    @property
    def available_slots(self) -> int:
        """Number of available task slots."""
        return max(0, self.max_parallel_tasks - self.active_tasks)


class ComputeSource(ABC):
    """Abstract base class for all compute sources.

    This class defines the standard interface that all compute sources must implement
    to work with the unified SweepEngine. Compute sources handle the actual execution
    of tasks on their respective platforms (local, SSH remote, HPC cluster, etc.).
    """

    def __init__(
        self,
        name: str,
        source_type: str,
        max_parallel_tasks: int = 1,
        health_check_interval: int = 300,  # 5 minutes
    ):
        """Initialize compute source.

        Args:
            name: Unique name for this compute source
            source_type: Type identifier (e.g., "local", "ssh", "slurm")
            max_parallel_tasks: Maximum number of tasks that can run simultaneously
            health_check_interval: Interval between health checks in seconds
        """
        self.name = name
        self.source_type = source_type
        self.max_parallel_tasks = max_parallel_tasks
        self.health_check_interval = health_check_interval

        # Initialize statistics
        self.stats = ComputeSourceStats(name, source_type, max_parallel_tasks)

        # Task tracking
        self.active_tasks: Dict[str, Task] = {}  # task_id -> Task
        self.task_to_job_id: Dict[str, str] = {}  # task_id -> job_id

        # Internal state
        self._setup_complete = False
        self._last_health_check = None

    @property
    def is_available(self) -> bool:
        """Check if this source is available for new tasks."""
        return (
            self._setup_complete
            and self.stats.available_slots > 0
            and self.stats.health_status in [HealthStatus.HEALTHY, HealthStatus.UNKNOWN]
        )

    @property
    def current_load(self) -> float:
        """Get current load as a percentage."""
        return self.stats.utilization * 100.0

    # Abstract methods that must be implemented by subclasses

    @abstractmethod
    async def setup(self, context: SweepContext) -> bool:
        """Setup the compute source for sweep execution.

        This method should prepare the compute source for task execution,
        including any necessary authentication, file synchronization, or
        environment setup.

        Args:
            context: Sweep execution context

        Returns:
            True if setup was successful, False otherwise
        """
        pass

    @abstractmethod
    async def submit_task(self, task: Task) -> TaskResult:
        """Submit a single task for execution.

        Args:
            task: Task to submit

        Returns:
            TaskResult with submission status and job ID
        """
        pass

    @abstractmethod
    async def get_task_status(self, task_id: str) -> TaskResult:
        """Get the current status of a submitted task.

        Args:
            task_id: ID of the task to check

        Returns:
            TaskResult with current status
        """
        pass

    @abstractmethod
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a running or queued task.

        Args:
            task_id: ID of the task to cancel

        Returns:
            True if cancellation was successful
        """
        pass

    @abstractmethod
    async def collect_results(self, task_ids: Optional[List[str]] = None) -> CollectionResult:
        """Collect results from completed tasks.

        This method should gather task outputs, logs, and any generated files
        and ensure they are available in the local sweep directory.

        Args:
            task_ids: Specific tasks to collect, or None for all completed tasks

        Returns:
            CollectionResult with collection status
        """
        pass

    @abstractmethod
    async def health_check(self) -> HealthReport:
        """Perform a health check of the compute source.

        Returns:
            HealthReport with current health status and metrics
        """
        pass

    @abstractmethod
    async def cleanup(self) -> bool:
        """Clean up resources and shut down the compute source.

        This method should cancel any remaining tasks, clean up temporary files,
        and release any held resources.

        Returns:
            True if cleanup was successful
        """
        pass

    # Common methods with default implementations

    def register_task(self, task: Task, job_id: Optional[str] = None) -> None:
        """Register a task as active."""
        self.active_tasks[task.task_id] = task
        if job_id:
            self.task_to_job_id[task.task_id] = job_id
        self.stats.tasks_submitted += 1
        logger.debug(f"Registered task {task.task_id} on {self.name}")

    def update_task_status(self, task_id: str, status: TaskStatus, **kwargs) -> None:
        """Update the status of an active task."""
        if task_id not in self.active_tasks:
            logger.warning(f"Attempted to update unknown task: {task_id}")
            return

        task = self.active_tasks[task_id]

        # Update task timing
        if status == TaskStatus.RUNNING and task.start_time is None:
            task.start_time = datetime.now()
        elif status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
            if task.complete_time is None:
                task.complete_time = datetime.now()

            # Update statistics
            if status == TaskStatus.COMPLETED:
                self.stats.tasks_completed += 1
                self.stats.consecutive_failures = 0
            elif status == TaskStatus.FAILED:
                self.stats.tasks_failed += 1
                self.stats.consecutive_failures += 1
            elif status == TaskStatus.CANCELLED:
                self.stats.tasks_cancelled += 1

            # Calculate average duration
            if task.start_time and task.complete_time:
                duration = (task.complete_time - task.start_time).total_seconds()
                if self.stats.average_task_duration is None:
                    self.stats.average_task_duration = duration
                else:
                    # Exponential moving average
                    alpha = 0.1
                    self.stats.average_task_duration = (
                        alpha * duration + (1 - alpha) * self.stats.average_task_duration
                    )

            # Remove from active tasks
            del self.active_tasks[task_id]
            if task_id in self.task_to_job_id:
                del self.task_to_job_id[task_id]

        # Update any additional task attributes
        for key, value in kwargs.items():
            if hasattr(task, key):
                setattr(task, key, value)

        logger.debug(f"Updated task {task_id} status to {status.value}")

    def get_job_id(self, task_id: str) -> Optional[str]:
        """Get the job ID for a task."""
        return self.task_to_job_id.get(task_id)

    def get_active_task_ids(self) -> List[str]:
        """Get list of active task IDs."""
        return list(self.active_tasks.keys())

    async def update_health_status(self) -> HealthReport:
        """Update and return current health status."""
        report = await self.health_check()
        self.stats.health_status = report.status
        self.stats.last_health_check = report.timestamp
        self._last_health_check = report.timestamp
        return report

    def __str__(self) -> str:
        """String representation of the compute source."""
        return (
            f"{self.source_type.title()}ComputeSource({self.name}): "
            f"{self.stats.active_tasks}/{self.max_parallel_tasks} tasks, "
            f"health={self.stats.health_status.value}"
        )

    def __repr__(self) -> str:
        """Detailed string representation."""
        return (
            f"ComputeSource(name='{self.name}', type='{self.source_type}', "
            f"max_tasks={self.max_parallel_tasks}, active={self.stats.active_tasks})"
        )
