"""Sweep completion analysis (read-only).

Inspects the on-disk state of a sweep directory and answers questions like
"which task numbers completed?", "what's the completion rate?", "which
combinations from the original sweep config are still missing?".

This is the analysis half of what used to be ``completion.py``. The
execution half (``SweepCompletor`` — the bloated job-manager wrangler) was
deleted in Pass B-heavy. Re-running incomplete sweeps will come back as a
cleanly redesigned ``hsm sweep complete`` built directly on top of
:class:`ComputeSource` + a future ``task_number_offset`` knob.

For now, ``hsm sweep status`` and ``hsm sweep report`` use this module to
show users what they have; users manually re-submit if they need to
retry tasks.
"""

import asyncio
from datetime import datetime
import logging
from pathlib import Path
import re
from typing import Any, Dict, List, Tuple

import yaml

from .config import SweepConfig
from .param_generator import ParameterGenerator
from .sweep_tracker import SweepTaskTracker

logger = logging.getLogger(__name__)


class SweepCompletionAnalyzer:
    """Analyzes sweep completion status and identifies missing combinations."""

    def __init__(self, sweep_dir: Path):
        self.sweep_dir = Path(sweep_dir)
        self.sweep_config_path = self.sweep_dir / "sweep_config.yaml"
        self.source_mapping_path = self.sweep_dir / "source_mapping.yaml"

        # Load data
        self.sweep_config = None
        self.source_mapping = None
        self.original_combinations = []
        self.completed_combinations = []
        self.failed_combinations = []
        self.cancelled_combinations = []
        self.missing_combinations = []

    def load_sweep_data(self) -> bool:
        """Load sweep configuration and execution data."""
        try:
            # Load sweep config
            if not self.sweep_config_path.exists():
                logger.error(f"Sweep config not found: {self.sweep_config_path}")
                return False

            self.sweep_config = SweepConfig.from_yaml(self.sweep_config_path)

            # Load source mapping if it exists
            if self.source_mapping_path.exists():
                with open(self.source_mapping_path) as f:
                    self.source_mapping = yaml.safe_load(f)
            else:
                logger.warning(f"Source mapping not found: {self.source_mapping_path}")
                self.source_mapping = {"task_assignments": {}}

            return True

        except Exception as e:
            logger.error(f"Error loading sweep data: {e}")
            return False

    def analyze_from_task_directories(
        self, overwrite_source_mapping: bool = False, verify_running: bool = True
    ) -> Dict[str, Any]:
        """Analyze completion status by directly scanning task directories.

        This method works independently of source_mapping.yaml and is ideal for
        PBS array jobs where the mapping file might not be properly maintained.
        """
        if not self.load_sweep_data():
            return {"error": "Failed to load sweep data"}

        # Generate original combinations
        generator = ParameterGenerator(self.sweep_config)
        self.original_combinations = list(generator.generate_combinations())
        total_expected = len(self.original_combinations)

        # Scan task directories
        tasks_dir = self.sweep_dir / "tasks"
        if not tasks_dir.exists():
            return {
                "error": "Tasks directory not found",
                "tasks_dir": str(tasks_dir),
                "total_expected": total_expected,
                "total_completed": 0,
                "total_failed": 0,
                "total_missing": total_expected,
                "completion_rate": 0.0,
            }

        # Find all task directories
        task_dirs = sorted(
            [d for d in tasks_dir.iterdir() if d.is_dir() and d.name.startswith("task_")]
        )

        completed_tasks = []
        failed_tasks = []
        running_tasks = []
        task_statuses = {}

        for task_dir in task_dirs:
            task_id = task_dir.name

            task_statuses[task_id] = {}

            task_info_file = task_dir / "task_info.txt"
            main_results_file = task_dir / "results.csv"
            baseline_results_file = task_dir / "transformation_baseline_results.csv"

            task_statuses[task_id]["main_results_present"] = main_results_file.exists()
            task_statuses[task_id]["baseline_results_present"] = baseline_results_file.exists()

            if not task_info_file.exists():
                # Task directory exists but no info file - treat as running/incomplete
                running_tasks.append(task_id)
                task_statuses[task_id]["status"] = "RUNNING"
                continue

            try:
                with open(task_info_file) as f:
                    content = f.read()

                # Check for status lines (last occurrence wins)
                status_lines = [line for line in content.split("\n") if line.startswith("Status: ")]

                if status_lines:
                    last_status_line = status_lines[-1]
                    if "SUCCESS" in last_status_line or "COMPLETED" in last_status_line:
                        completed_tasks.append(task_id)
                        task_statuses[task_id]["status"] = "COMPLETED"
                    elif "FAILED" in last_status_line:
                        failed_tasks.append(task_id)
                        task_statuses[task_id]["status"] = "FAILED"
                    elif "RUNNING" in last_status_line:
                        running_tasks.append(task_id)
                        task_statuses[task_id]["status"] = "RUNNING"
                    else:
                        running_tasks.append(task_id)
                        task_statuses[task_id]["status"] = "UNKNOWN"
                else:
                    # No status line found - task might be running
                    running_tasks.append(task_id)
                    task_statuses[task_id] = "RUNNING"

            except Exception as e:
                logger.warning(f"Error reading task info for {task_id}: {e}")
                running_tasks.append(task_id)
                task_statuses[task_id] = "ERROR"

        # Determine missing tasks by checking which task numbers don't exist AT ALL
        # (no directory exists for them)
        existing_task_numbers = set()
        for task_dir in task_dirs:
            task_match = re.search(r"task_(\d+)", task_dir.name)
            if task_match:
                existing_task_numbers.add(int(task_match.group(1)))

        # Missing = tasks with no directory at all
        # Running/Failed/Completed = tasks with directories (already counted above)
        missing_task_numbers = []
        for i in range(1, total_expected + 1):
            if i not in existing_task_numbers:
                missing_task_numbers.append(i)

        # Get actual combinations for completed/failed/missing tasks
        self.completed_combinations = self._get_combinations_for_tasks(completed_tasks)
        self.failed_combinations = self._get_combinations_for_tasks(failed_tasks)
        self.missing_combinations = [
            self.original_combinations[i - 1] for i in missing_task_numbers
        ]

        total_completed = len(completed_tasks)
        total_failed = len(failed_tasks)
        total_missing = len(missing_task_numbers)
        total_running = len(running_tasks)

        # Create summary report
        summary = {
            "sweep_dir": str(self.sweep_dir),
            "total_expected": total_expected,
            "total_completed": total_completed,
            "total_failed": total_failed,
            "total_cancelled": 0,  # PBS array jobs don't have cancelled status, only SUCCESS/FAILED
            "total_missing": total_missing,
            "total_running": total_running,
            "completion_rate": (total_completed / total_expected * 100)
            if total_expected > 0
            else 0,
            "completed_tasks": completed_tasks,
            "failed_tasks": failed_tasks,
            "cancelled_tasks": [],  # PBS array jobs don't have cancelled status
            "running_tasks": running_tasks,
            "missing_task_numbers": missing_task_numbers,
            "missing_combinations": self.missing_combinations,
            "failed_combinations": self.failed_combinations,
            "cancelled_combinations": [],  # PBS array jobs don't have cancelled status
            "needs_completion": total_missing > 0 or total_failed > 0,
            "task_statuses": task_statuses,
            "scan_method": "task_directories",
            "timestamp": datetime.now().isoformat(),
        }

        # Update source_mapping.yaml with actual task statuses from directories
        # This makes source_mapping.yaml the single source of truth
        self._update_source_mapping_from_directories(task_statuses, overwrite_source_mapping)

        return summary

    def analyze_completion_status(
        self, overwrite_source_mapping: bool = False, verify_running: bool = True
    ) -> Dict[str, Any]:
        """Analyze the completion status of the sweep.

        Uses source_mapping.yaml if available, otherwise falls back to
        direct task directory scanning (useful for PBS array jobs).

        Args:
            overwrite_source_mapping: If True, ignore existing source_mapping and rescan
            verify_running: If True, verify RUNNING tasks against actual system state
        """
        if not self.load_sweep_data():
            return {"error": "Failed to load sweep data"}

        # If source_mapping.yaml doesn't exist or has no task assignments,
        # fall back to task directory scanning (useful for PBS array jobs)
        task_assignments = self.source_mapping.get("task_assignments", {})
        if not task_assignments or overwrite_source_mapping:
            logger.info("No task assignments in source_mapping.yaml, using task directory scanning")
            return self.analyze_from_task_directories(overwrite_source_mapping, verify_running)

        # Generate original combinations
        generator = ParameterGenerator(self.sweep_config)
        self.original_combinations = list(generator.generate_combinations())

        # Analyze task assignments from source_mapping.yaml
        # (Keep the rest of the existing logic)

        # Get completed, failed, cancelled, and running tasks
        completed_tasks = []
        failed_tasks = []
        cancelled_tasks = []
        running_tasks = []
        status_fixes_count = 0

        for task_id, task_info in task_assignments.items():
            status = task_info.get("status", "UNKNOWN")
            complete_time = task_info.get("complete_time")

            # Fix status inconsistencies: if a task has complete_time but shows as RUNNING,
            # we need to check the actual task directory to determine the real status
            if status == "RUNNING" and complete_time:
                logger.warning(
                    f"Task {task_id} shows RUNNING but has complete_time, checking actual status"
                )
                # Check the actual task directory to determine real status
                actual_status = self._get_actual_task_status(task_id, verify_running)
                if actual_status and actual_status != "RUNNING":
                    logger.info(f"Updated {task_id} status from RUNNING to {actual_status}")
                    status = actual_status
                    task_info["status"] = actual_status
                    status_fixes_count += 1
                else:
                    logger.warning(
                        f"Could not determine actual status for {task_id}, keeping as RUNNING"
                    )

            # CRITICAL: For completion analysis, always verify status against actual directories
            # This catches cases where job tracking was wrong (like silent local failures)
            actual_status = self._get_actual_task_status(task_id, verify_running)
            if actual_status and actual_status != status:
                logger.warning(
                    f"Status mismatch for {task_id}: mapping shows {status}, directory shows {actual_status}"
                )
                logger.info(f"Correcting {task_id} status: {status} -> {actual_status}")
                status = actual_status
                task_info["status"] = actual_status
                status_fixes_count += 1

            if status == "COMPLETED":
                completed_tasks.append(task_id)
            elif status == "FAILED":
                failed_tasks.append(task_id)
            elif status == "CANCELLED":
                cancelled_tasks.append(task_id)
            elif status in ["RUNNING", "PENDING", "QUEUED"]:
                running_tasks.append(task_id)

        # Save the corrected mapping if we made any status fixes
        # This keeps source_mapping.yaml as the single source of truth
        if status_fixes_count > 0:
            try:
                with open(self.source_mapping_path, "w") as f:
                    yaml.dump(self.source_mapping, f, default_flow_style=False, indent=2)
                logger.info(
                    f"Updated source_mapping.yaml with {status_fixes_count} status corrections"
                )
            except Exception as e:
                logger.error(f"Error saving corrected source mapping: {e}")

        # Map task IDs to parameter combinations
        self.completed_combinations = self._get_combinations_for_tasks(completed_tasks)
        self.failed_combinations = self._get_combinations_for_tasks(failed_tasks)
        self.cancelled_combinations = self._get_combinations_for_tasks(cancelled_tasks)
        running_combinations = self._get_combinations_for_tasks(running_tasks)

        # Find missing combinations (exclude completed, failed, cancelled, AND running)
        # Missing = tasks that have NO directory/status at all
        accounted_for_params = set()
        for params in (
            self.completed_combinations
            + self.failed_combinations
            + self.cancelled_combinations
            + running_combinations
        ):
            accounted_for_params.add(self._params_to_key(params))

        self.missing_combinations = [
            params
            for params in self.original_combinations
            if self._params_to_key(params) not in accounted_for_params
        ]

        # Verify actual completion by checking task directories
        verified_completed = self._verify_task_completion(completed_tasks)
        actually_failed = len(completed_tasks) - len(verified_completed)

        # Update missing combinations based on verification
        if actually_failed > 0:
            verified_completed_set = {self._params_to_key(params) for params in verified_completed}
            self.missing_combinations = [
                params
                for params in self.original_combinations
                if self._params_to_key(params) not in verified_completed_set
            ]
            self.completed_combinations = verified_completed

        total_expected = len(self.original_combinations)
        total_completed = len(self.completed_combinations)
        total_failed = len(self.failed_combinations)
        total_cancelled = len(self.cancelled_combinations)
        total_missing = len(self.missing_combinations)
        total_running = len(running_tasks)

        return {
            "sweep_dir": str(self.sweep_dir),
            "total_expected": total_expected,
            "total_completed": total_completed,
            "total_failed": total_failed,
            "total_cancelled": total_cancelled,
            "total_missing": total_missing,
            "total_running": total_running,
            "completion_rate": (total_completed / total_expected * 100)
            if total_expected > 0
            else 0,
            "completed_tasks": completed_tasks,
            "failed_tasks": failed_tasks,
            "cancelled_tasks": cancelled_tasks,
            "running_tasks": running_tasks,
            "missing_combinations": self.missing_combinations,
            "failed_combinations": self.failed_combinations,
            "cancelled_combinations": self.cancelled_combinations,
            "needs_completion": total_missing > 0 or total_failed > 0 or total_cancelled > 0,
            "source_mapping_exists": self.source_mapping_path.exists(),
            "status_fixes_made": status_fixes_count > 0,
        }

    def _get_combinations_for_tasks(self, task_ids: List[str]) -> List[Dict[str, Any]]:
        """Get parameter combinations for given task IDs."""
        combinations = []

        for task_id in task_ids:
            # Extract task number from task_id (e.g., "task_001" -> 1)
            task_match = re.search(r"task_(\d+)", task_id)
            if task_match:
                task_num = int(task_match.group(1))
                # Task numbers are 1-indexed, but combinations list is 0-indexed
                if 1 <= task_num <= len(self.original_combinations):
                    combinations.append(self.original_combinations[task_num - 1])
                else:
                    logger.warning(f"Task number {task_num} out of range for combinations")

        return combinations

    def _verify_task_completion(self, completed_task_ids: List[str]) -> List[Dict[str, Any]]:
        """Verify that completed tasks actually have valid results."""
        verified_combinations = []
        tasks_dir = self.sweep_dir / "tasks"

        if not tasks_dir.exists():
            logger.warning("Tasks directory not found, cannot verify completion")
            return self.completed_combinations

        for task_id in completed_task_ids:
            task_dir = tasks_dir / task_id
            if task_dir.exists():
                # Check for completion indicators
                task_info_file = task_dir / "task_info.txt"
                if task_info_file.exists():
                    try:
                        with open(task_info_file) as f:
                            content = f.read()
                            # Support both PBS array format (SUCCESS) and local format (COMPLETED)
                            if "Status: COMPLETED" in content or "Status: SUCCESS" in content:
                                # Get the combination for this task
                                task_match = re.search(r"task_(\d+)", task_id)
                                if task_match:
                                    task_num = int(task_match.group(1))
                                    if 1 <= task_num <= len(self.original_combinations):
                                        verified_combinations.append(
                                            self.original_combinations[task_num - 1]
                                        )
                    except Exception as e:
                        logger.warning(f"Error reading task info for {task_id}: {e}")

        return verified_combinations

    def _update_source_mapping_from_directories(
        self, task_statuses: Dict[str, str], overwrite: bool = False
    ):
        """Update source_mapping.yaml with actual task statuses from directories.

        This makes source_mapping.yaml the single source of truth by syncing it
        with actual task directory statuses.
        """
        try:
            import yaml

            if not self.source_mapping_path.exists() or overwrite:
                # Create initial source mapping if it doesn't exist
                mapping_data = {
                    "sweep_metadata": {
                        "total_tasks": len(self.original_combinations),
                        "compute_sources": ["HPC"],
                        "strategy": "task_directory_scan",
                        "timestamp": datetime.now().isoformat(),
                    },
                    "task_assignments": {},
                }
            else:
                with open(self.source_mapping_path) as f:
                    mapping_data = yaml.safe_load(f) or {}

            # Update task assignments with actual statuses
            for task_id, task_info in task_statuses.items():
                if task_id not in mapping_data["task_assignments"]:
                    mapping_data["task_assignments"][task_id] = {}

                # Update status from actual directory
                mapping_data["task_assignments"][task_id]["status"] = task_info["status"]
                mapping_data["task_assignments"][task_id]["main_results_present"] = task_info[
                    "main_results_present"
                ]
                mapping_data["task_assignments"][task_id]["baseline_results_present"] = task_info[
                    "baseline_results_present"
                ]

                # Set compute source if not already set
                if "compute_source" not in mapping_data["task_assignments"][task_id]:
                    mapping_data["task_assignments"][task_id]["compute_source"] = "HPC"

            # Update timestamp
            if "sweep_metadata" not in mapping_data:
                mapping_data["sweep_metadata"] = {}
            mapping_data["sweep_metadata"]["timestamp"] = datetime.now().isoformat()

            # Save updated mapping
            with open(self.source_mapping_path, "w") as f:
                yaml.dump(mapping_data, f, default_flow_style=False, indent=2)

            logger.info(f"Updated source_mapping.yaml with {len(task_statuses)} task statuses")

        except Exception as e:
            logger.warning(f"Could not update source mapping from directories: {e}")

    def _params_to_key(self, params: Dict[str, Any]) -> str:
        """Convert parameters to a hashable key for comparison."""
        # Sort keys for consistent comparison
        items = []
        for key in sorted(params.keys()):
            value = params[key]
            if isinstance(value, (list, tuple)):
                value = (
                    tuple(sorted(value))
                    if all(isinstance(x, (str, int, float)) for x in value)
                    else tuple(value)
                )
            items.append((key, value))
        return str(tuple(items))

    def _get_actual_task_status(self, task_id: str, verify_running: bool = True) -> str:
        """Get the actual status of a task from its directory.

        Args:
            task_id: Task identifier (e.g., "task_001")
            verify_running: If True, verify RUNNING status against actual processes/jobs
        """
        try:
            tasks_dir = self.sweep_dir / "tasks"
            task_dir = tasks_dir / task_id
            if not task_dir.exists():
                return None

            # Check for completion indicators in task_info.txt
            task_info_file = task_dir / "task_info.txt"
            if task_info_file.exists():
                with open(task_info_file) as f:
                    content = f.read()

                    # Check for status lines (last occurrence wins)
                    status_lines = [
                        line for line in content.split("\n") if line.startswith("Status: ")
                    ]
                    if status_lines:
                        last_status_line = status_lines[-1]
                        # Support both PBS array format (SUCCESS/FAILED) and local format (COMPLETED/FAILED)
                        if (
                            "Status: COMPLETED" in last_status_line
                            or "Status: SUCCESS" in last_status_line
                        ):
                            return "COMPLETED"
                        elif "Status: FAILED" in last_status_line:
                            return "FAILED"
                        elif "Status: CANCELLED" in last_status_line:
                            return "CANCELLED"
                        elif "Status: RUNNING" in last_status_line:
                            # If verify_running is disabled, trust the status file
                            if not verify_running:
                                return "RUNNING"

                            # Verify RUNNING status against actual system state
                            is_actually_running = self._verify_task_is_running(
                                task_id, task_dir, content
                            )
                            if is_actually_running:
                                return "RUNNING"
                            else:
                                # Task shows RUNNING but isn't actually running
                                logger.debug(
                                    f"Task {task_id} shows RUNNING but verification failed, "
                                    "marking as FAILED"
                                )
                                return "FAILED"

            return None
        except Exception as e:
            logger.warning(f"Error reading task status for {task_id}: {e}")
            return None

    def _verify_task_is_running(self, task_id: str, task_dir: Path, task_info_content: str) -> bool:
        """Verify if a task marked as RUNNING is actually running.

        Checks:
        1. PBS/Slurm job status (if job ID found in task_info)
        2. Local process PIDs (if PID files exist)

        Returns:
            True if task is verified to be running, False otherwise
        """
        import subprocess

        # Method 1: Check PBS/Slurm job status
        job_id_match = re.search(r"Job ID: (\S+)", task_info_content)
        if job_id_match:
            job_id = job_id_match.group(1)

            # Skip local job IDs (they use PID checking instead)
            if not job_id.startswith("local_"):
                # Try PBS qstat
                try:
                    result = subprocess.run(
                        ["qstat", "-f", job_id],
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    if result.returncode == 0:
                        # Job found in PBS queue - check its state
                        output = result.stdout
                        # Look for job_state line in qstat -f output
                        if "job_state = R" in output or "job_state = Q" in output:
                            logger.debug(f"Task {task_id} verified running via PBS (job {job_id})")
                            return True
                        elif "job_state = C" in output:
                            logger.debug(f"Task {task_id} PBS job {job_id} shows completed")
                            return False
                    else:
                        # Job not in queue - likely finished
                        logger.debug(f"Task {task_id} PBS job {job_id} not in queue")
                except (subprocess.TimeoutExpired, FileNotFoundError):
                    pass

                # Try Slurm squeue
                try:
                    result = subprocess.run(
                        ["squeue", "-j", job_id, "-h"],
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        # Job found in Slurm queue
                        logger.debug(f"Task {task_id} verified running via Slurm (job {job_id})")
                        return True
                    else:
                        logger.debug(f"Task {task_id} Slurm job {job_id} not in queue")
                except (subprocess.TimeoutExpired, FileNotFoundError):
                    pass

        # Method 2: Check local PIDs
        pid_files = list(task_dir.glob("*.pid"))
        if pid_files:
            for pid_file in pid_files:
                try:
                    with open(pid_file) as f:
                        pid = f.read().strip()
                    if pid and pid.isdigit():
                        # Check if process is still running
                        result = subprocess.run(
                            ["ps", "-p", pid],
                            capture_output=True,
                            text=True,
                            timeout=2,
                        )
                        if result.returncode == 0:
                            logger.debug(f"Task {task_id} verified running via PID {pid}")
                            return True
                except Exception as e:
                    logger.debug(f"Error checking PID for {task_id}: {e}")
                    continue

        # If we got here, we couldn't verify the task is running
        return False

def find_incomplete_sweeps(sweeps_root: Path = None) -> List[Dict[str, Any]]:
    """Find all incomplete sweeps in the outputs directory."""
    if sweeps_root is None:
        sweeps_root = Path("sweeps/outputs")

    if not sweeps_root.exists():
        return []

    incomplete_sweeps = []

    for sweep_dir in sweeps_root.iterdir():
        if sweep_dir.is_dir() and sweep_dir.name.startswith("sweep_"):
            analyzer = SweepCompletionAnalyzer(sweep_dir)
            analysis = analyzer.analyze_completion_status()

            if "error" not in analysis and analysis["needs_completion"]:
                incomplete_sweeps.append(
                    {
                        "sweep_id": sweep_dir.name,
                        "sweep_dir": str(sweep_dir),
                        "completion_rate": analysis["completion_rate"],
                        "total_expected": analysis["total_expected"],
                        "total_completed": analysis["total_completed"],
                        "total_missing": analysis["total_missing"],
                        "total_failed": analysis["total_failed"],
                        "total_cancelled": analysis.get("total_cancelled", 0),
                    }
                )

    return incomplete_sweeps


def get_sweep_completion_summary(sweep_dir: Path) -> str:
    """Get a human-readable summary of sweep completion status."""
    analyzer = SweepCompletionAnalyzer(sweep_dir)
    analysis = analyzer.analyze_completion_status()

    if "error" in analysis:
        return f"Error analyzing sweep: {analysis['error']}"

    total_expected = analysis["total_expected"]
    total_completed = analysis["total_completed"]
    total_failed = analysis["total_failed"]
    total_cancelled = analysis.get("total_cancelled", 0)
    total_missing = analysis["total_missing"]
    total_running = analysis["total_running"]
    completion_rate = analysis["completion_rate"]

    summary = []
    summary.append(f"Sweep: {sweep_dir.name}")
    summary.append(f"Progress: {total_completed}/{total_expected} ({completion_rate:.1f}%)")

    if total_missing > 0:
        summary.append(f"Missing: {total_missing} combinations")
    if total_failed > 0:
        summary.append(f"Failed: {total_failed} combinations")
    if total_cancelled > 0:
        summary.append(f"Cancelled: {total_cancelled} combinations")
    if total_running > 0:
        summary.append(f"Running: {total_running} combinations")

    if analysis["needs_completion"]:
        summary.append("Status: NEEDS COMPLETION")
    else:
        summary.append("Status: COMPLETE")

    return "\n".join(summary)
