"""Sweep completion analysis and execution."""

import asyncio
from datetime import datetime
import logging
from pathlib import Path
import re
from typing import Any, Dict, List

import yaml

from .config_parser import SweepConfig
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

    def analyze_from_task_directories(self) -> Dict[str, Any]:
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
            task_info_file = task_dir / "task_info.txt"

            if not task_info_file.exists():
                # Task directory exists but no info file - treat as running/incomplete
                running_tasks.append(task_id)
                task_statuses[task_id] = "RUNNING"
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
                        task_statuses[task_id] = "COMPLETED"
                    elif "FAILED" in last_status_line:
                        failed_tasks.append(task_id)
                        task_statuses[task_id] = "FAILED"
                    elif "RUNNING" in last_status_line:
                        running_tasks.append(task_id)
                        task_statuses[task_id] = "RUNNING"
                    else:
                        running_tasks.append(task_id)
                        task_statuses[task_id] = "UNKNOWN"
                else:
                    # No status line found - task might be running
                    running_tasks.append(task_id)
                    task_statuses[task_id] = "RUNNING"

            except Exception as e:
                logger.warning(f"Error reading task info for {task_id}: {e}")
                running_tasks.append(task_id)
                task_statuses[task_id] = "ERROR"

        # Determine missing tasks by checking which task numbers don't exist
        existing_task_numbers = set()
        for task_dir in task_dirs:
            task_match = re.search(r"task_(\d+)", task_dir.name)
            if task_match:
                existing_task_numbers.add(int(task_match.group(1)))

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
        self._update_source_mapping_from_directories(task_statuses)

        return summary

    def analyze_completion_status(self) -> Dict[str, Any]:
        """Analyze the completion status of the sweep.

        Uses source_mapping.yaml if available, otherwise falls back to
        direct task directory scanning (useful for PBS array jobs).
        """
        if not self.load_sweep_data():
            return {"error": "Failed to load sweep data"}

        # If source_mapping.yaml doesn't exist or has no task assignments,
        # fall back to task directory scanning (useful for PBS array jobs)
        task_assignments = self.source_mapping.get("task_assignments", {})
        if not task_assignments:
            logger.info("No task assignments in source_mapping.yaml, using task directory scanning")
            return self.analyze_from_task_directories()

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
                actual_status = self._get_actual_task_status(task_id)
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
            actual_status = self._get_actual_task_status(task_id)
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

        # Find missing combinations (exclude only completed ones)
        completed_params_set = {
            self._params_to_key(params) for params in self.completed_combinations
        }
        self.missing_combinations = [
            params
            for params in self.original_combinations
            if self._params_to_key(params) not in completed_params_set
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

    def _update_source_mapping_from_directories(self, task_statuses: Dict[str, str]):
        """Update source_mapping.yaml with actual task statuses from directories.

        This makes source_mapping.yaml the single source of truth by syncing it
        with actual task directory statuses.
        """
        try:
            import yaml

            if not self.source_mapping_path.exists():
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
            for task_id, status in task_statuses.items():
                if task_id not in mapping_data["task_assignments"]:
                    mapping_data["task_assignments"][task_id] = {}

                # Update status from actual directory
                mapping_data["task_assignments"][task_id]["status"] = status

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

    def _get_actual_task_status(self, task_id: str) -> str:
        """Get the actual status of a task from its directory."""
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
                            # If we find RUNNING but the process finished, it might be stale
                            # Check if there are any process-related files that indicate completion
                            pid_files = list(task_dir.glob("*.pid"))
                            if pid_files:
                                # If we have PID files, check if processes are still running
                                import subprocess

                                for pid_file in pid_files:
                                    try:
                                        with open(pid_file) as f:
                                            pid = f.read().strip()
                                        if pid and pid.isdigit():
                                            # Check if process is still running
                                            result = subprocess.run(
                                                ["ps", "-p", pid], capture_output=True, text=True
                                            )
                                            if result.returncode != 0:
                                                # Process not running but status shows RUNNING
                                                # This is likely a failed task that didn't update status
                                                logger.debug(
                                                    f"Process {pid} not running for {task_id}, likely failed"
                                                )
                                                return "FAILED"
                                    except Exception:
                                        pass
                            return "RUNNING"

            return None
        except Exception as e:
            logger.warning(f"Error reading task status for {task_id}: {e}")
            return None


class SweepCompletor:
    """Executes completion of missing sweep combinations."""

    def __init__(self, sweep_dir: Path):
        self.sweep_dir = Path(sweep_dir)
        self.analyzer = SweepCompletionAnalyzer(sweep_dir)

    def _detect_original_sweep_mode(self) -> str:
        """Detect the original sweep's execution mode from source_mapping.yaml."""
        try:
            if not self.analyzer.source_mapping_path.exists():
                logger.info("No source_mapping.yaml found, defaulting to array mode detection")
                return "array"  # Will auto-detect HPC system

            import yaml

            with open(self.analyzer.source_mapping_path) as f:
                mapping_data = yaml.safe_load(f) or {}

            strategy = mapping_data.get("sweep_metadata", {}).get("strategy", "")
            compute_sources = mapping_data.get("sweep_metadata", {}).get("compute_sources", [])

            # Detect mode from strategy
            if strategy == "pbs_array":
                logger.info("Detected PBS array sweep, using array mode for completion")
                return "array"
            elif strategy in ["round_robin", "least_loaded", "capability_based"]:
                logger.info(f"Detected distributed sweep ({strategy}), using distributed mode")
                return "distributed"
            elif len(compute_sources) > 1:
                logger.info("Detected multiple compute sources, using distributed mode")
                return "distributed"
            elif "HPC" in compute_sources:
                logger.info("Detected HPC execution, using array mode for completion")
                return "array"
            else:
                logger.info("Could not determine mode, defaulting to array")
                return "array"

        except Exception as e:
            logger.warning(f"Error detecting original sweep mode: {e}, defaulting to array")
            return "array"

    def generate_completion_plan(self) -> Dict[str, Any]:
        """Generate a plan for completing the sweep."""
        analysis = self.analyzer.analyze_completion_status()

        if "error" in analysis:
            return analysis

        plan = {
            "analysis": analysis,
            "actions": [],
            "estimated_runtime": None,
        }

        missing_count = analysis["total_missing"]
        failed_count = analysis["total_failed"]
        cancelled_count = analysis["total_cancelled"]

        if missing_count > 0:
            plan["actions"].append(
                {
                    "type": "run_missing",
                    "count": missing_count,
                    "description": f"Run {missing_count} missing parameter combinations",
                }
            )

        if failed_count > 0:
            plan["actions"].append(
                {
                    "type": "retry_failed",
                    "count": failed_count,
                    "description": f"Retry {failed_count} failed parameter combinations",
                }
            )

        if cancelled_count > 0:
            plan["actions"].append(
                {
                    "type": "retry_cancelled",
                    "count": cancelled_count,
                    "description": f"Retry {cancelled_count} cancelled parameter combinations",
                }
            )

        if not plan["actions"]:
            plan["actions"].append(
                {"type": "no_action", "count": 0, "description": "Sweep is already complete"}
            )

        return plan

    def execute_completion(
        self,
        mode: str = "auto",
        dry_run: bool = False,
        retry_failed: bool = True,
        max_runs: int = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Execute the completion plan."""
        plan = self.generate_completion_plan()

        if "error" in plan:
            return plan

        analysis = plan["analysis"]

        # Auto-detect mode from original sweep if mode is "auto"
        if mode == "auto":
            mode = self._detect_original_sweep_mode()
            if kwargs.get("console"):
                kwargs["console"].print(f"[cyan]Auto-detected execution mode: {mode}[/cyan]")

        # Handle both source_mapping and task_directory analysis formats
        missing_combinations = analysis["missing_combinations"]
        failed_combinations = analysis.get("failed_combinations", []) if retry_failed else []
        cancelled_combinations = analysis.get("cancelled_combinations", []) if retry_failed else []

        # Get task number information for proper re-use
        missing_task_numbers = analysis.get("missing_task_numbers", [])

        # Combine missing, failed, and cancelled combinations to re-run
        combinations_to_run = missing_combinations + failed_combinations + cancelled_combinations

        total_available_to_run = len(combinations_to_run)

        # Find the highest existing task number to continue from there
        # This works with both source_mapping and task_directory analysis
        max_existing_task_num = 0
        failed_task_map = {}  # Maps task number to combination key

        # Method 1: Use source_mapping if available
        existing_task_assignments = self.analyzer.source_mapping.get("task_assignments", {})
        if existing_task_assignments:
            for task_name in existing_task_assignments.keys():
                task_match = re.search(r"task_(\d+)", task_name)
                if task_match:
                    task_num = int(task_match.group(1))
                    max_existing_task_num = max(max_existing_task_num, task_num)

            # For failed and cancelled tasks, we need to re-use their existing task numbers
            if retry_failed and (failed_combinations or cancelled_combinations):
                for task_name, task_info in existing_task_assignments.items():
                    task_status = task_info.get("status")
                    if task_status in ["FAILED", "CANCELLED"]:
                        task_match = re.search(r"task_(\d+)", task_name)
                        if task_match:
                            task_num = int(task_match.group(1))
                            # Find the combination for this failed/cancelled task
                            retry_combinations = failed_combinations + cancelled_combinations
                            for combo in retry_combinations:
                                # Check if this combination hasn't been assigned yet
                                combo_key = self.analyzer._params_to_key(combo)
                                if combo_key not in failed_task_map.values():
                                    failed_task_map[task_num] = combo_key
                                    break

        # Method 2: Use task directory scanning results if source_mapping is not available
        else:
            # Get task numbers from failed tasks
            failed_tasks = analysis.get("failed_tasks", [])
            for task_id in failed_tasks:
                task_match = re.search(r"task_(\d+)", task_id)
                if task_match:
                    task_num = int(task_match.group(1))
                    max_existing_task_num = max(max_existing_task_num, task_num)

                    if retry_failed and failed_combinations:
                        # Map this task number to its combination
                        # Task numbers are 1-indexed, combinations are 0-indexed
                        if 1 <= task_num <= len(self.analyzer.original_combinations):
                            combo = self.analyzer.original_combinations[task_num - 1]
                            combo_key = self.analyzer._params_to_key(combo)
                            failed_task_map[task_num] = combo_key

            # Also check completed tasks to find max task number
            completed_tasks = analysis.get("completed_tasks", [])
            for task_id in completed_tasks:
                task_match = re.search(r"task_(\d+)", task_id)
                if task_match:
                    task_num = int(task_match.group(1))
                    max_existing_task_num = max(max_existing_task_num, task_num)

        # Create tasks to run with proper task numbering
        tasks_to_run = []
        next_task_num = max_existing_task_num + 1

        # First, assign failed and cancelled combinations to their existing task numbers
        if retry_failed and (failed_combinations or cancelled_combinations):
            retry_combinations = failed_combinations + cancelled_combinations
            for combo in retry_combinations:
                combo_key = self.analyzer._params_to_key(combo)
                # Find if this combo was mapped to a failed/cancelled task
                found_task_num = None
                for task_num, mapped_combo_key in failed_task_map.items():
                    if mapped_combo_key == combo_key:
                        found_task_num = task_num
                        break

                if found_task_num:
                    # Re-use existing task number for failed/cancelled combination
                    tasks_to_run.append(
                        {
                            "task_index": found_task_num - 1,
                            "task_number": found_task_num,
                            "params": combo,
                        }
                    )
                else:
                    # Assign new task number for failed/cancelled combination not found in mapping
                    tasks_to_run.append(
                        {
                            "task_index": next_task_num - 1,
                            "task_number": next_task_num,
                            "params": combo,
                        }
                    )
                    next_task_num += 1

        # Then, assign missing combinations to their proper task numbers
        # If we have missing_task_numbers from task directory scanning, use those
        # This ensures we fill the gaps rather than creating new task numbers
        if missing_task_numbers:
            for i, combo in enumerate(missing_combinations):
                if i < len(missing_task_numbers):
                    task_num = missing_task_numbers[i]
                    # SAFETY CHECK: Only prevent overwriting COMPLETED tasks
                    # Failed/running tasks can be safely retried
                    task_dir = self.analyzer.sweep_dir / "tasks" / f"task_{task_num:03d}"
                    if task_dir.exists():
                        # Check if this is a completed task
                        task_info_file = task_dir / "task_info.txt"
                        is_completed = False
                        if task_info_file.exists():
                            try:
                                with open(task_info_file) as f:
                                    content = f.read()
                                    # Check for SUCCESS or COMPLETED status
                                    if (
                                        "Status: SUCCESS" in content
                                        or "Status: COMPLETED" in content
                                    ):
                                        is_completed = True
                            except Exception:
                                pass

                        if is_completed:
                            # Don't overwrite completed tasks
                            if logger := kwargs.get("logger"):
                                logger.warning(
                                    f"Task {task_num:03d} is already COMPLETED, "
                                    f"using new task number to avoid overwriting"
                                )
                            tasks_to_run.append(
                                {
                                    "task_index": next_task_num - 1,
                                    "task_number": next_task_num,
                                    "params": combo,
                                }
                            )
                            next_task_num += 1
                        else:
                            # Allow overwriting failed/incomplete tasks
                            tasks_to_run.append(
                                {
                                    "task_index": task_num - 1,
                                    "task_number": task_num,
                                    "params": combo,
                                }
                            )
                    else:
                        tasks_to_run.append(
                            {"task_index": task_num - 1, "task_number": task_num, "params": combo}
                        )
                else:
                    # Fallback to sequential numbering if we run out of missing numbers
                    tasks_to_run.append(
                        {
                            "task_index": next_task_num - 1,
                            "task_number": next_task_num,
                            "params": combo,
                        }
                    )
                    next_task_num += 1
        else:
            # No missing_task_numbers available, use sequential numbering
            for combo in missing_combinations:
                tasks_to_run.append(
                    {"task_index": next_task_num - 1, "task_number": next_task_num, "params": combo}
                )
                next_task_num += 1

        if max_runs is not None and max_runs > 0:
            if console := kwargs.get("console"):
                console.print(
                    f"[yellow]Limiting completion to {max_runs} runs out of {total_available_to_run} needed.[/yellow]"
                )
            tasks_to_run = tasks_to_run[:max_runs]

        if not tasks_to_run:
            return {
                "status": "complete",
                "message": "Sweep is already complete or no runnable tasks found",
                "jobs_submitted": 0,
            }

        if dry_run:
            message = f"Would run {len(tasks_to_run)} combinations"
            if max_runs is not None and max_runs > 0:
                message += f" (limited by --max-runs from {total_available_to_run} needed)"

            # Show which task numbers would be used
            task_numbers = [t["task_number"] for t in tasks_to_run]
            if console := kwargs.get("console"):
                console.print(f"\n[bold yellow]Execution Mode: {mode.upper()}[/bold yellow]")
                if mode == "array":
                    console.print("[cyan]• Will submit as PBS/Slurm array job[/cyan]")
                elif mode == "distributed":
                    console.print("[cyan]• Will distribute across multiple compute sources[/cyan]")
                elif mode == "remote":
                    console.print("[cyan]• Will execute on remote machine[/cyan]")
                elif mode == "local":
                    console.print("[cyan]• Will execute locally[/cyan]")

                console.print(f"\n[bold]Task numbers to run:[/bold]")
                if len(task_numbers) <= 20:
                    console.print(f"  {task_numbers}")
                else:
                    console.print(f"  {task_numbers[:20]}... and {len(task_numbers) - 20} more")

            return {
                "status": "dry_run",
                "message": message,
                "execution_mode": mode,
                "combinations_to_run": [t["params"] for t in tasks_to_run],
                "task_numbers": task_numbers,
                "missing_count": len(missing_combinations),
                "failed_count": len(failed_combinations),
                "cancelled_count": len(cancelled_combinations),
                "total_to_run": len(tasks_to_run),
            }

        # Execute the actual completion
        console = kwargs.pop("console", None)
        logger = kwargs.pop("logger", None)
        return self._execute_combinations(tasks_to_run, mode, console, logger, **kwargs)

    def _execute_combinations(
        self, tasks_to_run: List[Dict[str, Any]], mode: str, console=None, logger=None, **kwargs
    ) -> Dict[str, Any]:
        """Execute the missing combinations."""
        # Create backup of existing source mapping before any changes
        source_mapping_backup = None
        original_mapping_content = None

        if self.analyzer.source_mapping_path.exists():
            try:
                import shutil

                # Create backup with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = self.analyzer.source_mapping_path.with_suffix(
                    f".yaml.backup_{timestamp}"
                )
                shutil.copy2(self.analyzer.source_mapping_path, backup_path)
                source_mapping_backup = backup_path

                # Also keep the original content in memory for immediate restore
                with open(self.analyzer.source_mapping_path) as f:
                    original_mapping_content = f.read()

                logger.info(f"Created source mapping backup: {backup_path}")
            except Exception as e:
                logger.warning(f"Could not create source mapping backup: {e}")

        # Setup signal handling for completion process
        original_sigint_handler = None
        original_sigterm_handler = None

        def completion_signal_handler(signum, frame):
            """Signal handler for completion process that restores mapping before exit."""
            logger.warning(
                f"🛑 Completion interrupted by signal {signum}, restoring source mapping..."
            )

            # Restore the original mapping immediately
            if original_mapping_content and self.analyzer.source_mapping_path.exists():
                try:
                    with open(self.analyzer.source_mapping_path, "w") as f:
                        f.write(original_mapping_content)
                    logger.info("✅ Source mapping restored successfully")
                except Exception as e:
                    logger.error(f"Failed to restore source mapping: {e}")
                    # Try backup restore as fallback
                    if source_mapping_backup and source_mapping_backup.exists():
                        try:
                            import shutil

                            shutil.copy2(source_mapping_backup, self.analyzer.source_mapping_path)
                            logger.info("✅ Source mapping restored from backup")
                        except Exception as e2:
                            logger.error(f"Failed to restore from backup: {e2}")

            # Re-raise the signal to continue normal handling
            import signal

            if signum == signal.SIGINT:
                signal.signal(signal.SIGINT, signal.SIG_DFL)
                signal.raise_signal(signal.SIGINT)
            elif signum == signal.SIGTERM:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                signal.raise_signal(signal.SIGTERM)

        try:
            # Setup completion-specific signal handlers
            import signal

            original_sigint_handler = signal.signal(signal.SIGINT, completion_signal_handler)
            original_sigterm_handler = signal.signal(signal.SIGTERM, completion_signal_handler)

            # Import here to avoid circular imports
            from ...cli.sweep import create_remote_job_manager_wrapper
            from ..common.config import HSMConfig
            from ..common.path_detector import PathDetector
            from ..distributed.wrapper import create_distributed_sweep_wrapper
            from ..hpc.hpc_base import HPCJobManager
            from ..local.local_manager import LocalJobManager

            # Get sweep ID from directory name
            sweep_id = self.sweep_dir.name

            # Initialize task tracker to maintain sweep-wide tracking
            completion_tracker = SweepTaskTracker(self.sweep_dir, sweep_id)
            # Load existing mapping to preserve already completed work
            completion_tracker._load_existing_mapping()

            # CRITICAL: Enable preservation mode for completion runs
            completion_tracker.enable_preservation_mode()

            # Store the original task assignments to preserve them
            original_task_assignments = completion_tracker.mapping_data.get(
                "task_assignments", {}
            ).copy()
            original_metadata = completion_tracker.mapping_data.get("sweep_metadata", {}).copy()

            logger.info(
                f"Preserving {len(original_task_assignments)} existing task assignments in completion"
            )

            # Load HSM config
            hsm_config = HSMConfig.load()

            # Auto-detect paths
            detector = PathDetector()
            if hsm_config:
                python_path = hsm_config.get_default_python_path() or detector.detect_python_path()
                script_path = hsm_config.get_default_script_path() or detector.detect_train_script()
                project_dir = hsm_config.get_project_root() or str(Path.cwd())
            else:
                python_path = detector.detect_python_path()
                script_path = detector.detect_train_script()
                project_dir = str(Path.cwd())

            # Extract options
            walltime = kwargs.get("walltime", "23:59:59")
            resources = kwargs.get("resources", "select=1:ncpus=4:mem=64gb")
            parallel_jobs = kwargs.get("parallel_jobs")
            no_progress = kwargs.get("no_progress", False)
            show_output = kwargs.get("show_output", False)
            remote_name = kwargs.get("remote")
            no_verify_sync = kwargs.get("no_verify_sync", False)
            auto_sync = kwargs.get("auto_sync", False)

            # For completion runs, we MUST verify sync to ensure consistency
            if mode == "distributed":
                logger.info("🔒 Completion run in distributed mode: enforcing strict project sync")
                logger.info("   This ensures consistent results across all compute sources")
                # Override sync settings for completion safety
                no_verify_sync = False  # Always verify sync for distributed completion
                # Keep auto_sync as provided by user, but warn if disabled
                if not auto_sync:
                    logger.warning(
                        "⚠️  Auto-sync disabled - you will be prompted to confirm sync operations"
                    )
                    logger.warning("   Consider using --auto-sync for unattended completion runs")

            # Create job manager based on mode (similar to main sweep logic)
            job_manager = None

            if mode == "local":
                max_parallel_jobs = 4
                if parallel_jobs is not None:
                    max_parallel_jobs = parallel_jobs
                elif hsm_config:
                    max_parallel_jobs = hsm_config.get_max_array_size() or 4
                    max_parallel_jobs = min(max_parallel_jobs, 8)

                job_manager = LocalJobManager(
                    walltime=walltime,
                    resources=resources,
                    python_path=python_path,
                    script_path=script_path,
                    project_dir=project_dir,
                    max_parallel_jobs=max_parallel_jobs,
                    show_progress=not no_progress,
                    show_output=show_output,
                    setup_signal_handlers=False,
                )

            elif mode == "remote":
                if not hsm_config:
                    return {"error": "hsm_config.yaml required for remote mode"}

                if not remote_name:
                    return {"error": "Remote machine name required for remote mode"}

                if not console or not logger:
                    return {"error": "Console and logger required for remote mode"}

                # Create remote job manager wrapper
                job_manager = create_remote_job_manager_wrapper(
                    remote_name=remote_name,
                    hsm_config=hsm_config,
                    console=console,
                    logger=logger,
                    sweep_dir=self.sweep_dir,
                    parallel_jobs=parallel_jobs,
                    verify_sync=not no_verify_sync,
                    auto_sync=auto_sync,
                    setup_signal_handlers=False,
                )

                if not job_manager:
                    return {"error": f"Failed to create remote job manager for {remote_name}"}

            elif mode == "distributed":
                if not hsm_config:
                    return {"error": "hsm_config.yaml required for distributed mode"}

                if not console or not logger:
                    return {"error": "Console and logger required for distributed mode"}

                # Create distributed sweep wrapper
                try:
                    job_manager = create_distributed_sweep_wrapper(
                        hsm_config=hsm_config,
                        console=console,
                        logger=logger,
                        sweep_dir=self.sweep_dir,
                        show_progress=not no_progress,
                    )
                except Exception as e:
                    return {"error": f"Failed to create distributed job manager: {e}"}

            elif mode == "auto" or mode in ["individual", "array"]:
                # Auto-detect HPC system, fall back to local if none found
                try:
                    job_manager = HPCJobManager.auto_detect(
                        walltime=walltime,
                        resources=resources,
                        python_path=python_path,
                        script_path=script_path,
                        project_dir=project_dir,
                    )
                    # Update mode to reflect what was detected
                    if mode == "auto":
                        mode = "array"  # Default to array mode for HPC systems
                        if console:
                            console.print(
                                f"[green]Auto-detected HPC system, using {mode} mode[/green]"
                            )
                except RuntimeError as e:
                    # No HPC system found, fall back to local mode
                    if mode == "auto":
                        if console:
                            console.print(
                                "[yellow]No HPC system detected, falling back to local mode[/yellow]"
                            )
                        mode = "local"

                        max_parallel_jobs = 4
                        if parallel_jobs is not None:
                            max_parallel_jobs = parallel_jobs
                        elif hsm_config:
                            max_parallel_jobs = hsm_config.get_max_array_size() or 4
                            max_parallel_jobs = min(max_parallel_jobs, 8)

                        job_manager = LocalJobManager(
                            walltime=walltime,
                            resources=resources,
                            python_path=python_path,
                            script_path=script_path,
                            project_dir=project_dir,
                            max_parallel_jobs=max_parallel_jobs,
                            show_progress=not no_progress,
                            show_output=show_output,
                            setup_signal_handlers=False,
                        )
                    else:
                        # User explicitly requested array/individual mode but no HPC found
                        return {
                            "error": f"HPC system not found: {e}. Consider using --mode local or --mode auto"
                        }

            else:
                return {"error": f"Unknown mode '{mode}'"}

            if not job_manager:
                return {"error": f"Failed to create job manager for mode '{mode}'"}

            # CRITICAL: Inject the existing tracker into the job manager to prevent overwriting
            # For completion runs, we must preserve the existing task assignments
            if hasattr(job_manager, "task_tracker"):
                # Replace the job manager's tracker with our completion tracker that has the existing data
                job_manager.task_tracker = completion_tracker
                logger.info("Injected existing task tracker into job manager for completion")

                # Also enable preservation mode on the job manager's tracker
                if hasattr(job_manager.task_tracker, "enable_preservation_mode"):
                    job_manager.task_tracker.enable_preservation_mode()
                    logger.debug("Enabled preservation mode on job manager's task tracker")

            # For distributed mode, we need to set a flag to preserve existing mapping
            if hasattr(job_manager, "preserve_existing_mapping"):
                job_manager.preserve_existing_mapping = True
                job_manager.existing_task_assignments = original_task_assignments
                job_manager.existing_metadata = original_metadata
                logger.info("Set distributed manager to preserve existing task assignments")

            # Set completion run flag for proper task naming
            if hasattr(job_manager, "is_completion_run"):
                job_manager.is_completion_run = True

            # Create subdirectories for organization
            if mode == "local":
                scripts_dir = self.sweep_dir / "local_scripts"
            elif mode == "remote":
                scripts_dir = self.sweep_dir / "remote_scripts"
            elif mode == "distributed":
                scripts_dir = self.sweep_dir / "distributed_scripts"
            elif hasattr(job_manager, "system_type") and job_manager.system_type == "slurm":
                scripts_dir = self.sweep_dir / "slurm_files"
            else:
                scripts_dir = self.sweep_dir / "pbs_files"

            scripts_dir.mkdir(exist_ok=True)
            logs_dir = self.sweep_dir / "logs"
            logs_dir.mkdir(exist_ok=True)

            if console:
                console.print(f"[cyan]Using {mode} mode for completion...[/cyan]")
                if hasattr(job_manager, "system_type"):
                    console.print(f"Execution system: {job_manager.system_type}")

            # Submit the combinations
            submit_kwargs = {
                "param_combinations": tasks_to_run,  # Pass tasks with indices
                "mode": mode,
                "sweep_dir": self.sweep_dir,
                "sweep_id": sweep_id,
                "wandb_group": kwargs.get("group", sweep_id),
                "pbs_dir": scripts_dir,
                "logs_dir": logs_dir,
            }

            job_submission_successful = False
            job_ids = []

            try:
                if isinstance(job_manager, LocalJobManager):
                    job_ids = asyncio.run(job_manager.submit_sweep(**submit_kwargs))
                else:
                    job_ids = job_manager.submit_sweep(**submit_kwargs)

                job_submission_successful = bool(job_ids)
                logger.debug(
                    f"Job submission successful: {job_submission_successful}, job_ids: {job_ids}"
                )

            except Exception as submit_error:
                logger.error(f"Job submission failed: {submit_error}")
                job_submission_successful = False
                # If job submission failed, restore original mapping
                if original_mapping_content:
                    try:
                        with open(self.analyzer.source_mapping_path, "w") as f:
                            f.write(original_mapping_content)
                        logger.info("Restored original source mapping after job submission failure")
                    except Exception as restore_error:
                        logger.error(f"Could not restore original source mapping: {restore_error}")
                        # Try backup restore as fallback
                        if source_mapping_backup and source_mapping_backup.exists():
                            try:
                                import shutil

                                shutil.copy2(
                                    source_mapping_backup, self.analyzer.source_mapping_path
                                )
                                logger.info(
                                    "Restored source mapping from backup after job submission failure"
                                )
                            except Exception as backup_restore_error:
                                logger.error(
                                    f"Could not restore source mapping backup: {backup_restore_error}"
                                )
                raise submit_error

            # Only update task tracker if job submission was successful
            if job_submission_successful and job_ids:
                # For completion runs, we need to ensure the original task assignments are preserved
                # and merged with any new ones from the job submission
                try:
                    # Load what the job manager might have written
                    completion_tracker._load_existing_mapping()
                    current_assignments = completion_tracker.mapping_data.get(
                        "task_assignments", {}
                    )

                    # Merge original assignments back in (original assignments take precedence for existing tasks)
                    merged_assignments = {**current_assignments, **original_task_assignments}
                    completion_tracker.mapping_data["task_assignments"] = merged_assignments

                    # Preserve original metadata with any updates
                    current_metadata = completion_tracker.mapping_data.get("sweep_metadata", {})
                    merged_metadata = {**current_metadata, **original_metadata}
                    # Update timestamp and strategy but keep other original fields
                    merged_metadata["timestamp"] = current_metadata.get(
                        "timestamp", datetime.now().isoformat()
                    )
                    completion_tracker.mapping_data["sweep_metadata"] = merged_metadata

                    # Save the merged mapping
                    completion_tracker.save_mapping()
                    logger.info(
                        f"Preserved {len(original_task_assignments)} original tasks and merged with completion run"
                    )

                except Exception as merge_error:
                    logger.warning(
                        f"Could not merge task assignments after job submission: {merge_error}"
                    )
                    # Try to restore original mapping if merge failed
                    try:
                        completion_tracker.mapping_data["task_assignments"] = (
                            original_task_assignments
                        )
                        completion_tracker.mapping_data["sweep_metadata"] = original_metadata
                        completion_tracker.save_mapping()
                        logger.info("Restored original task assignments after merge failure")
                    except Exception as restore_error:
                        logger.error(
                            f"Failed to restore original task assignments: {restore_error}"
                        )

                # For remote mode, provide additional error reporting
                if mode == "remote":
                    console.print(
                        "\n[cyan]Remote job execution completed. Checking for error summaries...[/cyan]"
                    )
                    self._show_error_summary_if_available(console)

                # Final sync to ensure all task statuses are captured - only on success
                try:
                    # Re-load to get latest from disk, then merge with original
                    completion_tracker._load_existing_mapping()
                    current_assignments = completion_tracker.mapping_data.get(
                        "task_assignments", {}
                    )

                    # For final sync, preserve any status updates but keep original task assignments
                    for task_name, original_task in original_task_assignments.items():
                        if task_name in current_assignments:
                            # Keep original data but allow status updates
                            current_task = current_assignments[task_name]
                            merged_task = {**original_task}
                            # Allow status and timing updates from current
                            if current_task.get("status") != original_task.get("status"):
                                merged_task["status"] = current_task.get("status")
                            if current_task.get("complete_time") and not original_task.get(
                                "complete_time"
                            ):
                                merged_task["complete_time"] = current_task.get("complete_time")
                            if current_task.get("start_time") and not original_task.get(
                                "start_time"
                            ):
                                merged_task["start_time"] = current_task.get("start_time")
                            current_assignments[task_name] = merged_task
                        else:
                            # Restore missing original task
                            current_assignments[task_name] = original_task

                    completion_tracker.mapping_data["task_assignments"] = current_assignments
                    completion_tracker.save_mapping()
                    logger.debug("Final task tracker sync completed with preserved assignments")

                    # Clean up backup on successful completion
                    if source_mapping_backup and source_mapping_backup.exists():
                        try:
                            source_mapping_backup.unlink()
                            logger.debug(
                                "Removed source mapping backup after successful completion"
                            )
                        except Exception:
                            pass  # Don't fail on backup cleanup

                except Exception as final_sync_error:
                    logger.warning(f"Could not perform final task tracker sync: {final_sync_error}")
                    # If final sync fails, restore original mapping
                    if original_mapping_content:
                        try:
                            with open(self.analyzer.source_mapping_path, "w") as f:
                                f.write(original_mapping_content)
                            logger.info("Restored original source mapping after final sync failure")
                        except Exception as restore_error:
                            logger.error(
                                f"Could not restore original source mapping: {restore_error}"
                            )
                            # Try backup restore as final fallback
                            if source_mapping_backup and source_mapping_backup.exists():
                                try:
                                    import shutil

                                    shutil.copy2(
                                        source_mapping_backup, self.analyzer.source_mapping_path
                                    )
                                    logger.info(
                                        "Restored source mapping from backup after final sync failure"
                                    )
                                except Exception as backup_restore_error:
                                    logger.error(
                                        f"Could not restore source mapping backup: {backup_restore_error}"
                                    )

            return {
                "status": "submitted" if job_submission_successful else "failed",
                "message": f"Successfully submitted {len(job_ids)} completion jobs"
                if job_submission_successful
                else "Job submission failed",
                "jobs_submitted": len(job_ids),
                "job_ids": job_ids,
                "combinations_count": len(tasks_to_run),
            }

        except Exception as e:
            if logger:
                logger.error(f"Error executing completion: {e}")

            # Restore original mapping if something went wrong
            if original_mapping_content:
                try:
                    with open(self.analyzer.source_mapping_path, "w") as f:
                        f.write(original_mapping_content)
                    logger.info("Restored original source mapping after execution error")
                except Exception as restore_error:
                    logger.error(f"Could not restore original source mapping: {restore_error}")
                    # Try backup restore as fallback
                    if source_mapping_backup and source_mapping_backup.exists():
                        try:
                            import shutil

                            shutil.copy2(source_mapping_backup, self.analyzer.source_mapping_path)
                            logger.info("Restored source mapping from backup after execution error")
                        except Exception as backup_restore_error:
                            logger.error(
                                f"Could not restore source mapping backup: {backup_restore_error}"
                            )

            return {"error": f"Execution failed: {e}"}

        finally:
            # Restore original signal handlers
            if original_sigint_handler is not None:
                import signal

                signal.signal(signal.SIGINT, original_sigint_handler)
            if original_sigterm_handler is not None:
                import signal

                signal.signal(signal.SIGTERM, original_sigterm_handler)

            # Clean up backup file if it still exists (defensive cleanup)
            if source_mapping_backup and source_mapping_backup.exists():
                try:
                    source_mapping_backup.unlink()
                    logger.debug("Cleaned up source mapping backup in finally block")
                except Exception:
                    pass  # Don't fail on cleanup

    def _show_error_summary_if_available(self, console):
        """Show error summary if error files are available."""
        try:
            error_dir = self.sweep_dir / "errors"
            if not error_dir.exists():
                return

            error_files = list(error_dir.glob("*_error.txt"))
            if not error_files:
                return

            console.print(
                f"\n[red]⚠ Found {len(error_files)} error summaries from failed jobs:[/red]"
            )

            # Show first few error summaries
            for i, error_file in enumerate(error_files[:3]):
                console.print(f"\n[bold red]Error #{i + 1} - {error_file.stem}:[/bold red]")
                try:
                    with open(error_file) as f:
                        content = f.read()
                        # Show first 300 chars of error content
                        if content:
                            preview = content[:300]
                            if len(content) > 300:
                                preview += "..."
                            console.print(f"[dim]{preview}[/dim]")
                except Exception as e:
                    console.print(f"[dim]Could not read error file: {e}[/dim]")

            if len(error_files) > 3:
                console.print(f"\n[yellow]... and {len(error_files) - 3} more error files[/yellow]")

            console.print(f"\n[cyan]📁 All error summaries available in: {error_dir}/[/cyan]")
            console.print("[cyan]💡 Review these files to understand why jobs failed[/cyan]")

            # Try to identify common error patterns
            common_errors = self._analyze_common_error_patterns(error_files)
            if common_errors:
                console.print("\n[yellow]🔍 Common error patterns detected:[/yellow]")
                for pattern, count in common_errors.items():
                    console.print(f"  • {pattern}: {count} occurrences")

        except Exception as e:
            console.print(f"[dim]Error checking error summaries: {e}[/dim]")

    def _analyze_common_error_patterns(self, error_files) -> dict:
        """Analyze error files to find common patterns."""
        patterns = {}

        common_error_indicators = [
            ("Import errors", ["ImportError", "ModuleNotFoundError", "No module named"]),
            ("CUDA/GPU errors", ["CUDA", "GPU", "device"]),
            ("Memory errors", ["OutOfMemoryError", "out of memory", "OOM"]),
            ("Disk space errors", ["No space left", "disk full", "Permission denied"]),
            ("Network errors", ["ConnectionError", "timeout", "network"]),
            ("Configuration errors", ["Config", "configuration", "yaml", "hydra"]),
            ("Python environment errors", ["python", "conda", "pip", "environment"]),
        ]

        try:
            for error_file in error_files:
                with open(error_file) as f:
                    content = f.read().lower()

                    for pattern_name, keywords in common_error_indicators:
                        if any(keyword.lower() in content for keyword in keywords):
                            patterns[pattern_name] = patterns.get(pattern_name, 0) + 1
        except Exception:
            pass  # Ignore errors in pattern analysis

        return patterns


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
