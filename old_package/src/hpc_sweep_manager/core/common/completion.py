"""Sweep completion analysis and execution."""

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
                with open(self.source_mapping_path, "r") as f:
                    self.source_mapping = yaml.safe_load(f)
            else:
                logger.warning(f"Source mapping not found: {self.source_mapping_path}")
                self.source_mapping = {"task_assignments": {}}

            return True

        except Exception as e:
            logger.error(f"Error loading sweep data: {e}")
            return False

    def analyze_completion_status(self) -> Dict[str, Any]:
        """Analyze the completion status of the sweep."""
        if not self.load_sweep_data():
            return {"error": "Failed to load sweep data"}

        # Generate original combinations
        generator = ParameterGenerator(self.sweep_config)
        self.original_combinations = list(generator.generate_combinations())

        # Analyze task assignments
        task_assignments = self.source_mapping.get("task_assignments", {})

        # Get completed and failed tasks
        completed_tasks = []
        failed_tasks = []
        running_tasks = []

        for task_id, task_info in task_assignments.items():
            status = task_info.get("status", "UNKNOWN")
            if status == "COMPLETED":
                completed_tasks.append(task_id)
            elif status == "FAILED":
                failed_tasks.append(task_id)
            elif status in ["RUNNING", "PENDING", "QUEUED"]:
                running_tasks.append(task_id)

        # Map task IDs to parameter combinations
        self.completed_combinations = self._get_combinations_for_tasks(completed_tasks)
        self.failed_combinations = self._get_combinations_for_tasks(failed_tasks)

        # Find missing combinations
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
        total_missing = len(self.missing_combinations)
        total_running = len(running_tasks)

        return {
            "sweep_dir": str(self.sweep_dir),
            "total_expected": total_expected,
            "total_completed": total_completed,
            "total_failed": total_failed,
            "total_missing": total_missing,
            "total_running": total_running,
            "completion_rate": (total_completed / total_expected * 100)
            if total_expected > 0
            else 0,
            "completed_tasks": completed_tasks,
            "failed_tasks": failed_tasks,
            "running_tasks": running_tasks,
            "missing_combinations": self.missing_combinations,
            "failed_combinations": self.failed_combinations,
            "needs_completion": total_missing > 0 or total_failed > 0,
            "source_mapping_exists": self.source_mapping_path.exists(),
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
                        with open(task_info_file, "r") as f:
                            content = f.read()
                            if "Status: COMPLETED" in content:
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


class SweepCompletor:
    """Executes completion of missing sweep combinations."""

    def __init__(self, sweep_dir: Path):
        self.sweep_dir = Path(sweep_dir)
        self.analyzer = SweepCompletionAnalyzer(sweep_dir)

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

        if not plan["actions"]:
            plan["actions"].append(
                {"type": "no_action", "count": 0, "description": "Sweep is already complete"}
            )

        return plan

    def execute_completion(
        self, mode: str = "auto", dry_run: bool = False, retry_failed: bool = True, **kwargs
    ) -> Dict[str, Any]:
        """Execute the completion plan."""
        plan = self.generate_completion_plan()

        if "error" in plan:
            return plan

        analysis = plan["analysis"]
        missing_combinations = analysis["missing_combinations"]
        failed_combinations = analysis["failed_combinations"] if retry_failed else []

        # Combine missing and failed combinations to re-run
        combinations_to_run = missing_combinations + failed_combinations

        if not combinations_to_run:
            return {
                "status": "complete",
                "message": "Sweep is already complete",
                "jobs_submitted": 0,
            }

        if dry_run:
            return {
                "status": "dry_run",
                "message": f"Would run {len(combinations_to_run)} combinations",
                "combinations_to_run": combinations_to_run,
                "missing_count": len(missing_combinations),
                "failed_count": len(failed_combinations),
                "total_to_run": len(combinations_to_run),
            }

        # Execute the actual completion
        console = kwargs.pop("console", None)
        logger = kwargs.pop("logger", None)
        return self._execute_combinations(combinations_to_run, mode, console, logger, **kwargs)

    def _execute_combinations(
        self, combinations: List[Dict[str, Any]], mode: str, console=None, logger=None, **kwargs
    ) -> Dict[str, Any]:
        """Execute the missing combinations."""
        try:
            # Import here to avoid circular imports
            from ...cli.sweep import create_remote_job_manager_wrapper
            from ..common.config import HSMConfig
            from ..common.path_detector import PathDetector
            from ..distributed.wrapper import create_distributed_sweep_wrapper
            from ..hpc.hpc_base import HPCJobManager
            from ..local.manager import LocalJobManager

            # Get sweep ID from directory name
            sweep_id = self.sweep_dir.name

            # Initialize task tracker to maintain sweep-wide tracking
            completion_tracker = SweepTaskTracker(self.sweep_dir, sweep_id)
            # Load existing mapping to preserve already completed work
            completion_tracker._load_existing_mapping()

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
            job_ids = job_manager.submit_sweep(
                param_combinations=combinations,
                mode=mode,
                sweep_dir=self.sweep_dir,
                sweep_id=sweep_id,
                wandb_group=kwargs.get("group", sweep_id),
                pbs_dir=scripts_dir,
                logs_dir=logs_dir,
            )

            # Update task tracker to record completion job submissions
            # This ensures the sweep-wide mapping stays updated even during completion
            completion_source = f"{mode}_completion"
            if not hasattr(job_manager, "task_tracker") or job_manager.task_tracker is None:
                # If the job manager doesn't have its own tracker, update ours manually
                task_names = []
                for i, params in enumerate(combinations):
                    # Try to map back to original task names if possible
                    # For completion, we need to figure out which original tasks these correspond to
                    # This is a bit complex, so for now we'll rely on sync_with_task_directories
                    pass

                # Sync with actual task directories to capture what was actually run
                completion_tracker.sync_with_task_directories(force_update=True)
                completion_tracker.save_mapping()

            # For remote mode, provide additional error reporting
            if mode == "remote":
                console.print(
                    "\n[cyan]Remote job execution completed. Checking for error summaries...[/cyan]"
                )
                self._show_error_summary_if_available(console)

            # Final sync to ensure all task statuses are captured
            completion_tracker.sync_with_task_directories(force_update=True)
            completion_tracker.save_mapping()

            return {
                "status": "submitted",
                "message": f"Successfully submitted {len(job_ids)} completion jobs",
                "jobs_submitted": len(job_ids),
                "job_ids": job_ids,
                "combinations_count": len(combinations),
            }

        except Exception as e:
            if logger:
                logger.error(f"Error executing completion: {e}")
            return {"error": f"Execution failed: {e}"}

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
                    with open(error_file, "r") as f:
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
                console.print(f"\n[yellow]🔍 Common error patterns detected:[/yellow]")
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
                with open(error_file, "r") as f:
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
    if total_running > 0:
        summary.append(f"Running: {total_running} combinations")

    if analysis["needs_completion"]:
        summary.append("Status: NEEDS COMPLETION")
    else:
        summary.append("Status: COMPLETE")

    return "\n".join(summary)
