"""Unified sweep task tracking across all execution modes."""

from datetime import datetime
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


class SweepTaskTracker:
    """Unified task tracking for sweeps across all execution modes."""

    def __init__(self, sweep_dir: Path, sweep_id: str):
        self.sweep_dir = Path(sweep_dir)
        self.sweep_id = sweep_id
        self.mapping_file = self.sweep_dir / "source_mapping.yaml"

        # Task tracking data
        self.mapping_data = {
            "sweep_metadata": {
                "sweep_id": sweep_id,
                "total_tasks": 0,
                "compute_sources": [],
                "strategy": "single_source",
                "timestamp": datetime.now().isoformat(),
            },
            "task_assignments": {},
        }

        # Load existing mapping if it exists
        self._load_existing_mapping()

    def _load_existing_mapping(self):
        """Load existing source mapping if it exists."""
        if self.mapping_file.exists():
            try:
                with open(self.mapping_file, "r") as f:
                    existing_data = yaml.safe_load(f)
                    if existing_data:
                        self.mapping_data = existing_data
                        logger.debug(
                            f"Loaded existing source mapping with {len(self.mapping_data.get('task_assignments', {}))} tasks"
                        )
            except Exception as e:
                logger.warning(f"Could not load existing source mapping: {e}")

    def initialize_sweep(self, total_tasks: int, compute_source: str, mode: str = "single_source"):
        """Initialize or update sweep metadata."""
        metadata = self.mapping_data["sweep_metadata"]
        metadata["total_tasks"] = max(metadata.get("total_tasks", 0), total_tasks)
        metadata["timestamp"] = datetime.now().isoformat()

        # Update compute sources list
        sources = set(metadata.get("compute_sources", []))
        sources.add(compute_source)
        metadata["compute_sources"] = sorted(list(sources))

        # Update strategy if this is the first initialization
        if len(self.mapping_data["task_assignments"]) == 0:
            metadata["strategy"] = mode

        logger.debug(f"Initialized sweep tracking: {total_tasks} tasks, source: {compute_source}")

    def register_task_submission(
        self, task_name: str, compute_source: str, job_id: str = None, params: Dict[str, Any] = None
    ):
        """Register that a task has been submitted."""
        task_data = {
            "compute_source": compute_source,
            "status": "RUNNING",
            "start_time": datetime.now().isoformat(),
            "complete_time": None,
        }

        if job_id:
            task_data["job_id"] = job_id
        if params:
            task_data["params"] = params

        self.mapping_data["task_assignments"][task_name] = task_data
        logger.debug(f"Registered task submission: {task_name} on {compute_source}")

    def update_task_status(self, task_name: str, status: str, complete_time: datetime = None):
        """Update the status of a task."""
        if task_name in self.mapping_data["task_assignments"]:
            task_data = self.mapping_data["task_assignments"][task_name]
            task_data["status"] = status

            if complete_time:
                task_data["complete_time"] = complete_time.isoformat()
            elif status in ["COMPLETED", "FAILED", "CANCELLED"]:
                task_data["complete_time"] = datetime.now().isoformat()

            logger.debug(f"Updated task status: {task_name} -> {status}")
        else:
            logger.warning(f"Attempted to update unknown task: {task_name}")

    def register_task_batch(
        self, task_names: List[str], compute_source: str, job_ids: List[str] = None
    ):
        """Register a batch of tasks (for efficiency)."""
        job_id_map = {}
        if job_ids:
            job_id_map = dict(zip(task_names, job_ids))

        for task_name in task_names:
            self.register_task_submission(
                task_name, compute_source, job_id=job_id_map.get(task_name)
            )

    def get_task_status(self, task_name: str) -> str:
        """Get the current status of a task."""
        task_data = self.mapping_data["task_assignments"].get(task_name)
        return task_data.get("status", "UNKNOWN") if task_data else "UNKNOWN"

    def get_completed_tasks(self) -> List[str]:
        """Get list of completed task names."""
        return [
            task_name
            for task_name, task_data in self.mapping_data["task_assignments"].items()
            if task_data.get("status") == "COMPLETED"
        ]

    def get_failed_tasks(self) -> List[str]:
        """Get list of failed task names."""
        return [
            task_name
            for task_name, task_data in self.mapping_data["task_assignments"].items()
            if task_data.get("status") == "FAILED"
        ]

    def get_running_tasks(self) -> List[str]:
        """Get list of currently running task names."""
        return [
            task_name
            for task_name, task_data in self.mapping_data["task_assignments"].items()
            if task_data.get("status") in ["RUNNING", "PENDING", "QUEUED"]
        ]

    def get_task_summary(self) -> Dict[str, int]:
        """Get summary of task statuses."""
        summary = {
            "total": len(self.mapping_data["task_assignments"]),
            "completed": 0,
            "failed": 0,
            "running": 0,
            "unknown": 0,
        }

        for task_data in self.mapping_data["task_assignments"].values():
            status = task_data.get("status", "UNKNOWN")
            if status == "COMPLETED":
                summary["completed"] += 1
            elif status == "FAILED":
                summary["failed"] += 1
            elif status in ["RUNNING", "PENDING", "QUEUED"]:
                summary["running"] += 1
            else:
                summary["unknown"] += 1

        return summary

    def save_mapping(self):
        """Save the current mapping to file."""
        try:
            # Ensure directory exists
            self.mapping_file.parent.mkdir(parents=True, exist_ok=True)

            # Update timestamp
            self.mapping_data["sweep_metadata"]["timestamp"] = datetime.now().isoformat()

            # Save to YAML file
            with open(self.mapping_file, "w") as f:
                yaml.dump(self.mapping_data, f, default_flow_style=False, indent=2)

            logger.debug(f"Saved source mapping to {self.mapping_file}")

        except Exception as e:
            logger.error(f"Error saving source mapping: {e}")

    def sync_with_task_directories(self, force_update: bool = False):
        """Sync mapping with actual task directories on disk."""
        tasks_dir = self.sweep_dir / "tasks"
        if not tasks_dir.exists():
            return

        updated_count = 0
        for task_dir in tasks_dir.iterdir():
            if not task_dir.is_dir() or not task_dir.name.startswith("task_"):
                continue

            task_name = task_dir.name
            task_info_file = task_dir / "task_info.txt"

            # If we don't have this task in our mapping, try to infer its status
            if task_name not in self.mapping_data["task_assignments"] or force_update:
                status = "UNKNOWN"
                start_time = None
                complete_time = None

                if task_info_file.exists():
                    try:
                        with open(task_info_file, "r") as f:
                            content = f.read()

                        # Extract status
                        if "Status: COMPLETED" in content:
                            status = "COMPLETED"
                        elif "Status: FAILED" in content:
                            status = "FAILED"
                        elif "Status: RUNNING" in content:
                            status = "RUNNING"
                        elif "Status: CANCELLED" in content:
                            status = "CANCELLED"

                        # Try to extract timing info
                        for line in content.split("\n"):
                            if line.startswith("Start Time:"):
                                try:
                                    start_time = line.split("Start Time:", 1)[1].strip()
                                except:
                                    pass
                            elif line.startswith("End Time:"):
                                try:
                                    complete_time = line.split("End Time:", 1)[1].strip()
                                except:
                                    pass

                    except Exception as e:
                        logger.debug(f"Could not read task info for {task_name}: {e}")

                # Update or create the task entry
                if task_name not in self.mapping_data["task_assignments"]:
                    self.mapping_data["task_assignments"][task_name] = {
                        "compute_source": "unknown",
                        "status": status,
                        "start_time": start_time,
                        "complete_time": complete_time,
                    }
                    updated_count += 1
                elif force_update:
                    task_data = self.mapping_data["task_assignments"][task_name]
                    if task_data.get("status") != status:
                        task_data["status"] = status
                        if complete_time:
                            task_data["complete_time"] = complete_time
                        updated_count += 1

        if updated_count > 0:
            logger.info(f"Synced {updated_count} task statuses from disk")
            self.save_mapping()

    def is_task_registered(self, task_name: str) -> bool:
        """Check if a task is already registered."""
        return task_name in self.mapping_data["task_assignments"]

    def get_missing_tasks(self, expected_task_count: int) -> List[str]:
        """Get list of task names that should exist but aren't registered."""
        expected_tasks = [f"task_{i + 1:03d}" for i in range(expected_task_count)]
        registered_tasks = set(self.mapping_data["task_assignments"].keys())
        return [task for task in expected_tasks if task not in registered_tasks]

    def __str__(self) -> str:
        """String representation showing current state."""
        summary = self.get_task_summary()
        return (
            f"SweepTaskTracker({self.sweep_id}): "
            f"{summary['completed']} completed, "
            f"{summary['failed']} failed, "
            f"{summary['running']} running, "
            f"{summary['total']} total"
        )
