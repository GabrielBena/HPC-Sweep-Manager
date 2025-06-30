"""Enhanced sweep task tracking with cross-mode support.

The SweepTracker provides unified task tracking across all execution modes,
supporting seamless transitions between different compute sources and
comprehensive state management.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from ..compute.base import Task, TaskStatus

logger = logging.getLogger(__name__)


class CompletionStatus(Enum):
    """Status of sweep completion."""

    INCOMPLETE = "incomplete"
    COMPLETE = "complete"
    FAILED = "failed"
    PARTIAL = "partial"


@dataclass
class TaskTracking:
    """Enhanced task tracking information."""

    task_id: str
    compute_source: str
    status: TaskStatus
    start_time: Optional[datetime] = None
    complete_time: Optional[datetime] = None
    job_id: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    retry_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "task_id": self.task_id,
            "compute_source": self.compute_source,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "complete_time": self.complete_time.isoformat() if self.complete_time else None,
            "job_id": self.job_id,
            "params": self.params,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskTracking":
        """Create from dictionary."""
        return cls(
            task_id=data["task_id"],
            compute_source=data["compute_source"],
            status=TaskStatus(data["status"]),
            start_time=datetime.fromisoformat(data["start_time"])
            if data.get("start_time")
            else None,
            complete_time=datetime.fromisoformat(data["complete_time"])
            if data.get("complete_time")
            else None,
            job_id=data.get("job_id"),
            params=data.get("params"),
            error_message=data.get("error_message"),
            retry_count=data.get("retry_count", 0),
        )


@dataclass
class SweepMetadata:
    """Metadata about the sweep."""

    sweep_id: str
    total_tasks: int
    compute_sources: List[str]
    strategy: str
    created_time: datetime
    updated_time: datetime
    completed_time: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "sweep_id": self.sweep_id,
            "total_tasks": self.total_tasks,
            "compute_sources": self.compute_sources,
            "strategy": self.strategy,
            "created_time": self.created_time.isoformat(),
            "updated_time": self.updated_time.isoformat(),
            "completed_time": self.completed_time.isoformat() if self.completed_time else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SweepMetadata":
        """Create from dictionary."""
        return cls(
            sweep_id=data["sweep_id"],
            total_tasks=data["total_tasks"],
            compute_sources=data["compute_sources"],
            strategy=data["strategy"],
            created_time=datetime.fromisoformat(data["created_time"]),
            updated_time=datetime.fromisoformat(data["updated_time"]),
            completed_time=datetime.fromisoformat(data["completed_time"])
            if data.get("completed_time")
            else None,
        )


class SweepTracker:
    """Enhanced sweep task tracking with cross-mode support.

    The SweepTracker provides unified task tracking across all execution modes,
    supporting seamless transitions between different compute sources and
    comprehensive state management.
    """

    def __init__(self, sweep_dir: Path, sweep_id: str):
        """Initialize the SweepTracker.

        Args:
            sweep_dir: Directory containing sweep data
            sweep_id: Unique identifier for the sweep
        """
        self.sweep_dir = Path(sweep_dir)
        self.sweep_id = sweep_id

        # Files for persistent state
        self.mapping_file = self.sweep_dir / "task_mapping.yaml"
        self.metadata_file = self.sweep_dir / "sweep_metadata.yaml"

        # State
        self.metadata: Optional[SweepMetadata] = None
        self.task_tracking: Dict[str, TaskTracking] = {}

        # Thread safety
        self._lock = asyncio.Lock()

        # Load existing state
        self._load_state()

    def _load_state(self) -> None:
        """Load existing tracking state from disk."""
        # Load metadata
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file) as f:
                    metadata_data = yaml.safe_load(f)
                    if metadata_data:
                        self.metadata = SweepMetadata.from_dict(metadata_data)
                        logger.debug(f"Loaded sweep metadata for {self.sweep_id}")
            except Exception as e:
                logger.warning(f"Could not load sweep metadata: {e}")

        # Load task mapping
        if self.mapping_file.exists():
            try:
                with open(self.mapping_file) as f:
                    mapping_data = yaml.safe_load(f)
                    if mapping_data and "task_assignments" in mapping_data:
                        for task_id, task_data in mapping_data["task_assignments"].items():
                            # Convert legacy format to new format
                            if isinstance(task_data.get("status"), str):
                                try:
                                    status = TaskStatus(task_data["status"].upper())
                                except ValueError:
                                    # Handle legacy status values
                                    legacy_status = task_data["status"].upper()
                                    if legacy_status == "COMPLETED":
                                        status = TaskStatus.COMPLETED
                                    elif legacy_status == "FAILED":
                                        status = TaskStatus.FAILED
                                    elif legacy_status in ["RUNNING", "PENDING", "QUEUED"]:
                                        status = TaskStatus.RUNNING
                                    elif legacy_status == "CANCELLED":
                                        status = TaskStatus.CANCELLED
                                    else:
                                        status = TaskStatus.PENDING
                            else:
                                status = TaskStatus.PENDING

                            tracking = TaskTracking(
                                task_id=task_id,
                                compute_source=task_data.get("compute_source", "unknown"),
                                status=status,
                                job_id=task_data.get("job_id"),
                                params=task_data.get("params"),
                                retry_count=task_data.get("retry_count", 0),
                            )

                            # Parse timestamps
                            if task_data.get("start_time"):
                                try:
                                    tracking.start_time = datetime.fromisoformat(
                                        task_data["start_time"]
                                    )
                                except:
                                    pass
                            if task_data.get("complete_time"):
                                try:
                                    tracking.complete_time = datetime.fromisoformat(
                                        task_data["complete_time"]
                                    )
                                except:
                                    pass

                            self.task_tracking[task_id] = tracking

                        logger.debug(f"Loaded {len(self.task_tracking)} task assignments")
            except Exception as e:
                logger.warning(f"Could not load task mapping: {e}")

    async def initialize_sweep(
        self, total_tasks: int, compute_sources: List[str], strategy: str = "single_source"
    ) -> None:
        """Initialize or update sweep metadata.

        Args:
            total_tasks: Total number of tasks in the sweep
            compute_sources: List of compute source names
            strategy: Distribution strategy used
        """
        async with self._lock:
            now = datetime.now()

            if self.metadata is None:
                self.metadata = SweepMetadata(
                    sweep_id=self.sweep_id,
                    total_tasks=total_tasks,
                    compute_sources=compute_sources,
                    strategy=strategy,
                    created_time=now,
                    updated_time=now,
                )
                logger.info(
                    f"Initialized sweep tracking: {total_tasks} tasks, sources: {compute_sources}"
                )
            else:
                # Update existing metadata
                self.metadata.total_tasks = max(self.metadata.total_tasks, total_tasks)
                self.metadata.compute_sources = list(
                    set(self.metadata.compute_sources + compute_sources)
                )
                self.metadata.updated_time = now
                logger.info(
                    f"Updated sweep tracking: {total_tasks} tasks, sources: {compute_sources}"
                )

            await self._save_metadata()

    async def register_task(self, task: Task, compute_source: str) -> None:
        """Register a task for tracking.

        Args:
            task: Task to register
            compute_source: Name of the compute source handling the task
        """
        async with self._lock:
            tracking = TaskTracking(
                task_id=task.task_id,
                compute_source=compute_source,
                status=TaskStatus.PENDING,
                start_time=datetime.now(),
                params=task.params,
            )

            self.task_tracking[task.task_id] = tracking
            logger.debug(f"Registered task: {task.task_id} on {compute_source}")

            await self._save_mapping()

    async def update_task_status(
        self,
        task_id: str,
        status: TaskStatus,
        job_id: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Update task status.

        Args:
            task_id: ID of the task to update
            status: New status
            job_id: Optional job ID (for HPC systems)
            error_message: Optional error message for failed tasks
        """
        async with self._lock:
            if task_id not in self.task_tracking:
                logger.warning(f"Attempted to update unknown task: {task_id}")
                return

            tracking = self.task_tracking[task_id]
            old_status = tracking.status
            tracking.status = status

            if job_id:
                tracking.job_id = job_id

            if error_message:
                tracking.error_message = error_message

            # Update timing
            if status == TaskStatus.RUNNING and old_status == TaskStatus.PENDING:
                tracking.start_time = datetime.now()
            elif status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                tracking.complete_time = datetime.now()

                # Mark sweep as completed if all tasks are done
                if self._check_completion():
                    if self.metadata:
                        self.metadata.completed_time = datetime.now()
                        await self._save_metadata()

            logger.debug(f"Updated task status: {task_id} {old_status.value} -> {status.value}")
            await self._save_mapping()

    async def register_task_batch(
        self, tasks: List[Task], compute_source: str, job_ids: Optional[List[str]] = None
    ) -> None:
        """Register a batch of tasks efficiently.

        Args:
            tasks: List of tasks to register
            compute_source: Name of the compute source handling the tasks
            job_ids: Optional list of job IDs
        """
        async with self._lock:
            job_id_map = {}
            if job_ids:
                job_id_map = dict(zip([task.task_id for task in tasks], job_ids))

            for task in tasks:
                tracking = TaskTracking(
                    task_id=task.task_id,
                    compute_source=compute_source,
                    status=TaskStatus.PENDING,
                    start_time=datetime.now(),
                    params=task.params,
                    job_id=job_id_map.get(task.task_id),
                )
                self.task_tracking[task.task_id] = tracking

            logger.info(f"Registered {len(tasks)} tasks on {compute_source}")
            await self._save_mapping()

    def get_task_status(self, task_id: str) -> TaskStatus:
        """Get current status of a task.

        Args:
            task_id: ID of the task

        Returns:
            Current task status
        """
        tracking = self.task_tracking.get(task_id)
        return tracking.status if tracking else TaskStatus.PENDING

    def get_tasks_by_status(self, status: TaskStatus) -> List[str]:
        """Get list of task IDs with the specified status.

        Args:
            status: Status to filter by

        Returns:
            List of task IDs
        """
        return [
            task_id for task_id, tracking in self.task_tracking.items() if tracking.status == status
        ]

    def get_tasks_by_source(self, source_name: str) -> List[str]:
        """Get list of task IDs assigned to a specific source.

        Args:
            source_name: Name of the compute source

        Returns:
            List of task IDs
        """
        return [
            task_id
            for task_id, tracking in self.task_tracking.items()
            if tracking.compute_source == source_name
        ]

    def get_task_summary(self) -> Dict[str, int]:
        """Get summary of task statuses.

        Returns:
            Dictionary with counts for each status
        """
        summary = {
            "total": len(self.task_tracking),
            "pending": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0,
        }

        for tracking in self.task_tracking.values():
            if tracking.status == TaskStatus.PENDING:
                summary["pending"] += 1
            elif tracking.status == TaskStatus.RUNNING:
                summary["running"] += 1
            elif tracking.status == TaskStatus.COMPLETED:
                summary["completed"] += 1
            elif tracking.status == TaskStatus.FAILED:
                summary["failed"] += 1
            elif tracking.status == TaskStatus.CANCELLED:
                summary["cancelled"] += 1

        return summary

    def get_completion_status(self) -> CompletionStatus:
        """Get overall completion status of the sweep.

        Returns:
            Completion status
        """
        if not self.task_tracking:
            return CompletionStatus.INCOMPLETE

        summary = self.get_task_summary()

        if summary["completed"] == summary["total"]:
            return CompletionStatus.COMPLETE
        elif summary["failed"] == summary["total"]:
            return CompletionStatus.FAILED
        elif summary["completed"] + summary["failed"] == summary["total"]:
            return CompletionStatus.PARTIAL
        else:
            return CompletionStatus.INCOMPLETE

    def get_missing_tasks(self, expected_tasks: List[str]) -> List[str]:
        """Get list of expected tasks that aren't registered.

        Args:
            expected_tasks: List of expected task IDs

        Returns:
            List of missing task IDs
        """
        registered_tasks = set(self.task_tracking.keys())
        return [task_id for task_id in expected_tasks if task_id not in registered_tasks]

    async def sync_from_disk(self) -> int:
        """Sync task status from disk-based task directories.

        Returns:
            Number of tasks updated
        """
        tasks_dir = self.sweep_dir / "tasks"
        if not tasks_dir.exists():
            return 0

        updated_count = 0

        async with self._lock:
            for task_dir in tasks_dir.iterdir():
                if not task_dir.is_dir():
                    continue

                task_id = task_dir.name
                status_file = task_dir / "status.yaml"

                if status_file.exists():
                    try:
                        with open(status_file) as f:
                            status_data = yaml.safe_load(f)

                        if status_data and "status" in status_data:
                            try:
                                disk_status = TaskStatus(status_data["status"].upper())

                                # Update if task exists and status changed
                                if task_id in self.task_tracking:
                                    tracking = self.task_tracking[task_id]
                                    if tracking.status != disk_status:
                                        tracking.status = disk_status
                                        if "complete_time" in status_data:
                                            tracking.complete_time = datetime.fromisoformat(
                                                status_data["complete_time"]
                                            )
                                        updated_count += 1

                            except ValueError:
                                logger.debug(
                                    f"Unknown status in {status_file}: {status_data['status']}"
                                )

                    except Exception as e:
                        logger.debug(f"Could not read status from {status_file}: {e}")

        if updated_count > 0:
            logger.info(f"Synced {updated_count} task statuses from disk")
            await self._save_mapping()

        return updated_count

    def _check_completion(self) -> bool:
        """Check if all tasks are in terminal states."""
        if not self.task_tracking:
            return False

        terminal_states = {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}
        return all(tracking.status in terminal_states for tracking in self.task_tracking.values())

    async def _save_metadata(self) -> None:
        """Save sweep metadata to disk."""
        if self.metadata is None:
            return

        try:
            self.metadata_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.metadata_file, "w") as f:
                yaml.dump(self.metadata.to_dict(), f, default_flow_style=False, indent=2)
            logger.debug(f"Saved sweep metadata to {self.metadata_file}")
        except Exception as e:
            logger.error(f"Error saving sweep metadata: {e}")

    async def _save_mapping(self) -> None:
        """Save task mapping to disk."""
        try:
            self.mapping_file.parent.mkdir(parents=True, exist_ok=True)

            # Create mapping data in the expected format
            mapping_data = {
                "sweep_metadata": self.metadata.to_dict() if self.metadata else {},
                "task_assignments": {
                    task_id: tracking.to_dict() for task_id, tracking in self.task_tracking.items()
                },
            }

            with open(self.mapping_file, "w") as f:
                yaml.dump(mapping_data, f, default_flow_style=False, indent=2)

            logger.debug(f"Saved task mapping to {self.mapping_file}")
        except Exception as e:
            logger.error(f"Error saving task mapping: {e}")

    def __str__(self) -> str:
        """String representation of current state."""
        summary = self.get_task_summary()
        return (
            f"SweepTracker({self.sweep_id}): "
            f"{summary['completed']} completed, "
            f"{summary['failed']} failed, "
            f"{summary['running']} running, "
            f"{summary['total']} total"
        )
