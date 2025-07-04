"""Local job manager for running sweeps on a single machine."""

from datetime import datetime
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

from ..common.path_detector import PathDetector

logger = logging.getLogger(__name__)


class LocalJobManager:
    """Local job manager for running sweeps on a single machine."""

    def __init__(
        self,
        walltime: str = "04:00:00",
        resources: str = "local",
        python_path: str = "python",
        script_path: str = "",
        project_dir: str = ".",
        max_parallel_jobs: int = 1,
        show_progress: bool = True,
        show_output: bool = False,
    ):
        self.walltime = walltime
        self.resources = resources
        self.python_path = python_path
        self.script_path = script_path
        self.project_dir = project_dir
        self.system_type = "local"
        self.max_parallel_jobs = max_parallel_jobs
        self.running_processes = {}  # job_id -> subprocess.Popen
        self.job_counter = 0
        self.show_progress = show_progress
        self.show_output = show_output
        self.total_jobs_planned = 0
        self.jobs_completed = 0

        # Validate and fix paths for cross-machine compatibility
        self._validate_and_fix_paths()

        # Register cleanup handler for graceful shutdown
        import atexit

        atexit.register(self._cleanup_on_exit)

        # Register signal handlers for Ctrl+C and termination
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _validate_and_fix_paths(self):
        """Validate and fix paths for cross-machine compatibility."""
        # Fix project directory
        if not Path(self.project_dir).exists():
            print(
                f"Warning: Project directory {self.project_dir} not found, using current directory"
            )
            self.project_dir = str(Path.cwd())
        else:
            self.project_dir = str(Path(self.project_dir).resolve())

        # Fix script path - try to resolve relative to project_dir if it's just a filename
        if self.script_path:
            script_path = Path(self.script_path)
            if not script_path.exists():
                if not script_path.is_absolute():
                    # Try relative to project directory
                    potential_script = Path(self.project_dir) / self.script_path
                    if potential_script.exists():
                        self.script_path = str(potential_script)
                    else:
                        # Try to find script in common locations
                        detector = PathDetector(Path(self.project_dir))
                        detected_script = detector.detect_train_script()
                        if detected_script:
                            print(
                                f"Warning: Script {self.script_path} not found, using detected script: {detected_script}"
                            )
                            self.script_path = str(detected_script)
                        else:
                            print(
                                f"Warning: Script {self.script_path} not found and no alternative detected"
                            )
                else:
                    print(f"Warning: Script path {self.script_path} not found")

        # Fix python path - ensure it exists
        if self.python_path and self.python_path != "python":
            if not Path(self.python_path).exists():
                print(
                    f"Warning: Python path {self.python_path} not found, falling back to 'python'"
                )
                self.python_path = "python"

    def submit_single_job(
        self,
        params: Dict[str, Any],
        job_name: str,
        sweep_dir: Path,
        sweep_id: str,
        wandb_group: Optional[str] = None,
        pbs_dir: Optional[Path] = None,
        logs_dir: Optional[Path] = None,
    ) -> str:
        """Submit a single local job."""
        # Use provided directories or fallback to sweep_dir
        if logs_dir is None:
            logs_dir = sweep_dir / "logs"
        if pbs_dir is None:
            pbs_dir = sweep_dir / "scripts"

        # Ensure directories exist
        logs_dir.mkdir(parents=True, exist_ok=True)
        pbs_dir.mkdir(parents=True, exist_ok=True)

        # Create task directory for organized outputs
        task_dir = sweep_dir / "tasks" / f"task_{self.job_counter + 1:03d}"
        task_dir.mkdir(parents=True, exist_ok=True)

        # Increment job counter
        self.job_counter += 1
        job_id = f"local_{sweep_id}_{self.job_counter}"

        # Determine the effective wandb group
        effective_wandb_group = wandb_group or sweep_id

        # Create a shell script for the job (for consistency with HPC)
        script_content = f"""#!/bin/bash
# Local job script for {job_name}
# Generated at {datetime.now()}

# Change to project directory
cd {self.project_dir}

# Create task info file
echo "Job ID: {job_id}" > {task_dir}/task_info.txt
echo "Task Directory: {task_dir}" >> {task_dir}/task_info.txt
echo "Start Time: $(date)" >> {task_dir}/task_info.txt
echo "Parameters: {self._params_to_string(params)}" >> {task_dir}/task_info.txt
echo "Status: RUNNING" >> {task_dir}/task_info.txt

# Store the command for reference
echo "{self.python_path} {self.script_path} {self._params_to_string(params)} output.dir={task_dir} wandb.group={effective_wandb_group}" > {task_dir}/command.txt

# Run the training script and capture output (with output directory set to task directory)
{self.python_path} {self.script_path} {self._params_to_string(params)} output.dir={task_dir} wandb.group={effective_wandb_group} 2>&1

# Update status on completion
if [ $? -eq 0 ]; then
    echo "Status: COMPLETED" >> {task_dir}/task_info.txt
    echo "End Time: $(date)" >> {task_dir}/task_info.txt
else
    echo "Status: FAILED" >> {task_dir}/task_info.txt
    echo "End Time: $(date)" >> {task_dir}/task_info.txt
fi
"""

        # Write job script
        script_path = pbs_dir / f"{job_name}.sh"
        with open(script_path, "w") as f:
            f.write(script_content)
        script_path.chmod(0o755)  # Make executable

        # Start the job as a subprocess
        log_file = logs_dir / f"{job_name}.log"
        error_file = logs_dir / f"{job_name}.err"

        if self.show_output:
            # Show output in real-time while also logging to files
            from threading import Thread

            def stream_output(pipe, file_handle, prefix=""):
                """Stream output from subprocess to both console and file."""
                for line in iter(pipe.readline, b""):
                    line_str = line.decode("utf-8", errors="replace")
                    file_handle.write(line_str)
                    file_handle.flush()
                    if prefix:
                        print(f"[{prefix}] {line_str.rstrip()}")
                    else:
                        print(line_str.rstrip())
                pipe.close()

            with open(log_file, "w") as log_f, open(error_file, "w") as err_f:
                process = subprocess.Popen(
                    ["/bin/bash", str(script_path)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=self.project_dir,
                    preexec_fn=os.setsid,
                    bufsize=1,
                    universal_newlines=False,
                )

                # Start threads to handle output streaming
                stdout_thread = Thread(target=stream_output, args=(process.stdout, log_f, job_name))
                stderr_thread = Thread(
                    target=stream_output,
                    args=(process.stderr, err_f, f"{job_name}-ERR"),
                )
                stdout_thread.daemon = True
                stderr_thread.daemon = True
                stdout_thread.start()
                stderr_thread.start()
        else:
            # Original behavior: redirect to log files
            with open(log_file, "w") as log_f, open(error_file, "w") as err_f:
                process = subprocess.Popen(
                    ["/bin/bash", str(script_path)],
                    stdout=log_f,
                    stderr=err_f,
                    cwd=self.project_dir,
                    preexec_fn=os.setsid,
                )

        self.running_processes[job_id] = {
            "process": process,
            "job_name": job_name,
            "script_path": str(script_path),
            "log_file": str(log_file),
            "error_file": str(error_file),
            "task_dir": str(task_dir),
            "start_time": time.time(),
            "params": params,
        }

        if self.show_progress:
            print(f"Started local job: {job_name} (ID: {job_id})")

        return job_id

    def submit_array_job(
        self,
        param_combinations: List[Dict[str, Any]],
        sweep_id: str,
        sweep_dir: Path,
        wandb_group: Optional[str] = None,
        pbs_dir: Optional[Path] = None,
        logs_dir: Optional[Path] = None,
    ) -> str:
        """Submit an array job as multiple local jobs."""
        self.total_jobs_planned = len(param_combinations)

        if self.show_progress:
            print(f"Starting local array job with {len(param_combinations)} tasks")
            print(f"Max parallel jobs: {self.max_parallel_jobs}")

        job_ids = []
        for i, params in enumerate(param_combinations):
            job_name = f"{sweep_id}_task_{i + 1:03d}"

            # Wait if we've reached max parallel jobs
            while len(self.running_processes) >= self.max_parallel_jobs:
                time.sleep(1)
                self._wait_for_job_completion()

            job_id = self.submit_single_job(
                params, job_name, sweep_dir, sweep_id, wandb_group, pbs_dir, logs_dir
            )
            job_ids.append(job_id)

        # Return a synthetic array job ID
        array_job_id = f"local_array_{sweep_id}"

        # Wait for all jobs to complete
        self.wait_for_all_jobs(use_progress_bar=self.show_progress)

        return array_job_id

    def get_job_status(self, job_id: str) -> str:
        """Get job status."""
        if job_id.startswith("local_array_"):
            # Array job status - check if any jobs are still running
            if self.running_processes:
                return "RUNNING"
            else:
                return "COMPLETED"

        if job_id not in self.running_processes:
            return "UNKNOWN"

        job_info = self.running_processes[job_id]
        process = job_info["process"]

        if process.poll() is None:
            return "RUNNING"
        else:
            # Process completed, determine status from exit code
            exit_code = process.returncode
            if exit_code == 0:
                return "COMPLETED"
            else:
                return "FAILED"

    def _wait_for_job_completion(self):
        """Wait for at least one job to complete."""
        completed_jobs = []
        for job_id, job_info in self.running_processes.items():
            process = job_info["process"]
            if process.poll() is not None:
                completed_jobs.append(job_id)

        # Remove completed jobs
        for job_id in completed_jobs:
            del self.running_processes[job_id]
            self.jobs_completed += 1
            if self.show_progress:
                print(f"Job completed: {job_id} ({self.jobs_completed}/{self.total_jobs_planned})")

    def _params_to_string(self, params: Dict[str, Any]) -> str:
        """Convert parameters dictionary to command line arguments for Hydra."""
        param_strs = []
        for key, value in params.items():
            if isinstance(value, (list, tuple)):
                # Convert list/tuple to Hydra format: [item1,item2,...]
                value_str = str(list(value))  # Ensure it's in list format
                param_strs.append(f'"{key}={value_str}"')
            elif value is None:
                param_strs.append(f'"{key}=null"')
            elif isinstance(value, bool):
                param_strs.append(f'"{key}={str(value).lower()}"')
            elif isinstance(value, str) and (" " in value or "," in value):
                # Quote strings that contain spaces or commas
                param_strs.append(f'"{key}={value}"')
            else:
                param_strs.append(f'"{key}={value}"')
        return " ".join(param_strs)

    def wait_for_all_jobs(self, use_progress_bar: bool = False):
        """Wait for all jobs to complete."""
        if use_progress_bar and self.total_jobs_planned > 0:
            self.monitor_with_progress_bar()
        else:
            while self.running_processes:
                self._wait_for_job_completion()
                time.sleep(1)

    def cancel_job(self, job_id: str, timeout: int = 10) -> bool:
        """Cancel a specific job."""
        if job_id not in self.running_processes:
            return False

        job_info = self.running_processes[job_id]
        return self._cancel_single_job(job_id, job_info, timeout)

    def _cancel_single_job(self, job_id: str, job_info: dict, timeout: int) -> bool:
        """Cancel a single job with graceful shutdown."""
        process = job_info["process"]
        job_name = job_info["job_name"]

        if process.poll() is not None:
            # Process already completed
            return True

        try:
            # Try graceful termination first
            if hasattr(process, "pid"):
                try:
                    # Send SIGTERM to process group
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)

                    # Wait for graceful shutdown
                    start_time = time.time()
                    while time.time() - start_time < timeout and process.poll() is None:
                        time.sleep(0.1)

                    if process.poll() is not None:
                        # Process terminated gracefully
                        self._cleanup_cancelled_job(job_id, job_info, "CANCELLED")
                        return True
                    else:
                        # Force kill if still running
                        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                        process.wait(timeout=5)
                        self._cleanup_cancelled_job(job_id, job_info, "KILLED")
                        return True

                except (ProcessLookupError, OSError):
                    # Process already gone
                    self._cleanup_cancelled_job(job_id, job_info, "CANCELLED")
                    return True
            else:
                # Fallback to process.terminate()
                process.terminate()
                try:
                    process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                self._cleanup_cancelled_job(job_id, job_info, "CANCELLED")
                return True

        except Exception as e:
            logger.warning(f"Error cancelling job {job_id}: {e}")
            return False

    def _cleanup_cancelled_job(self, job_id: str, job_info: dict, status: str):
        """Clean up a cancelled job."""
        task_dir = Path(job_info["task_dir"])
        if task_dir.exists():
            # Update task status
            task_info_file = task_dir / "task_info.txt"
            if task_info_file.exists():
                with open(task_info_file, "a") as f:
                    f.write(f"Status: {status}\n")
                    f.write(f"End Time: {datetime.now()}\n")

        # Remove from running processes
        if job_id in self.running_processes:
            del self.running_processes[job_id]

    def cancel_all_jobs(self, timeout: int = 10) -> dict:
        """Cancel all running jobs."""
        results = {"cancelled": 0, "failed": 0, "already_done": 0}

        jobs_to_cancel = list(self.running_processes.items())

        for job_id, job_info in jobs_to_cancel:
            if self._cancel_single_job(job_id, job_info, timeout):
                results["cancelled"] += 1
            else:
                results["failed"] += 1

        return results

    def get_running_process_info(self) -> dict:
        """Get information about currently running processes."""
        info = {
            "total_running": len(self.running_processes),
            "max_parallel": self.max_parallel_jobs,
            "jobs_completed": self.jobs_completed,
            "jobs_planned": self.total_jobs_planned,
            "running_jobs": {},
        }

        for job_id, job_info in self.running_processes.items():
            process = job_info["process"]
            runtime = time.time() - job_info["start_time"]
            info["running_jobs"][job_id] = {
                "job_name": job_info["job_name"],
                "pid": process.pid if hasattr(process, "pid") else None,
                "runtime_seconds": runtime,
                "status": "RUNNING" if process.poll() is None else "COMPLETED",
            }

        return info

    def show_progress_summary(self):
        """Show a summary of current progress."""
        info = self.get_running_process_info()
        print("\n=== Local Job Manager Status ===")
        print(f"Jobs completed: {info['jobs_completed']}/{info['jobs_planned']}")
        print(f"Currently running: {info['total_running']}/{info['max_parallel']}")

        if info["running_jobs"]:
            print("\nRunning jobs:")
            for job_id, job_info in info["running_jobs"].items():
                runtime_mins = job_info["runtime_seconds"] / 60
                print(f"  {job_info['job_name']} (PID: {job_info['pid']}) - {runtime_mins:.1f}m")
        print()

    def monitor_with_progress_bar(self, update_interval: int = 1):
        """Monitor jobs with a progress bar."""
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

            console = Console()

            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("•"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task(
                    f"Running local jobs (max {self.max_parallel_jobs} parallel)",
                    total=self.total_jobs_planned,
                    completed=self.jobs_completed,
                )

                while self.running_processes:
                    self._wait_for_job_completion()
                    self._update_completed_count()

                    # Count failed jobs for display
                    failed_count = 0
                    for job_info in self.running_processes.values():
                        process = job_info["process"]
                        if process.poll() is not None and process.returncode != 0:
                            failed_count += 1

                    # Add completed failed jobs to the count
                    total_failed = (
                        self.total_jobs_planned
                        - self.jobs_completed
                        - len(self.running_processes)
                        + failed_count
                    )

                    progress.update(
                        task,
                        completed=self.jobs_completed,
                        description=f"Local sweep • ✓ {self.jobs_completed} • ✗ {total_failed} • {len(self.running_processes)} running",
                    )
                    time.sleep(update_interval)

                # Final update
                final_failed = self.total_jobs_planned - self.jobs_completed
                progress.update(
                    task,
                    completed=self.total_jobs_planned,
                    description=f"Local sweep completed • ✓ {self.jobs_completed} • ✗ {final_failed}",
                )
                console.print(f"[green]All {self.total_jobs_planned} local jobs completed![/green]")

        except ImportError:
            # Fallback to simple text progress
            print("Rich not available, using simple progress display")
            while self.running_processes:
                self._wait_for_job_completion()
                print(f"Progress: {self.jobs_completed}/{self.total_jobs_planned} jobs completed")
                time.sleep(update_interval)

    def _update_completed_count(self):
        """Update the completed job count by checking process status."""
        completed_jobs = []
        for job_id, job_info in self.running_processes.items():
            process = job_info["process"]
            if process.poll() is not None:
                completed_jobs.append(job_id)

        # Remove completed jobs and update count
        for job_id in completed_jobs:
            if job_id in self.running_processes:
                del self.running_processes[job_id]
                self.jobs_completed += 1

    def force_cleanup_all(self) -> dict:
        """Force cleanup of all processes (emergency cleanup)."""
        results = {"killed": 0, "failed": 0, "already_done": 0}

        for job_id, job_info in list(self.running_processes.items()):
            try:
                process = job_info["process"]
                if process.poll() is None:
                    # Force kill
                    if hasattr(process, "pid"):
                        try:
                            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                            process.wait(timeout=3)
                            results["killed"] += 1
                        except (ProcessLookupError, OSError):
                            results["already_done"] += 1
                    else:
                        process.kill()
                        process.wait(timeout=3)
                        results["killed"] += 1
                else:
                    results["already_done"] += 1

                # Clean up
                self._cleanup_cancelled_job(job_id, job_info, "FORCE_KILLED")

            except Exception as e:
                logger.error(f"Error force-killing job {job_id}: {e}")
                results["failed"] += 1

        return results

    def _force_kill_process(self, job_id: str, job_info: dict) -> str:
        """Force kill a process and return status."""
        try:
            process = job_info["process"]
            if process.poll() is None:
                if hasattr(process, "pid"):
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                else:
                    process.kill()
                process.wait(timeout=3)
                return "KILLED"
            else:
                return "ALREADY_DEAD"
        except Exception as e:
            logger.error(f"Error force-killing process for job {job_id}: {e}")
            return "ERROR"

    def _cleanup_on_exit(self):
        """Cleanup handler called on exit."""
        if self.running_processes:
            print("\nCleaning up running processes...")
            results = self.cancel_all_jobs(timeout=5)
            if results["cancelled"] > 0:
                print(f"Cancelled {results['cancelled']} running jobs")

    def _signal_handler(self, signum, frame):
        """Signal handler for graceful shutdown."""
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        results = self.cancel_all_jobs(timeout=10)
        print(f"Cancelled {results['cancelled']} jobs")
        sys.exit(0)

    def submit_sweep(
        self,
        param_combinations: List[Dict[str, Any]],
        mode: str,
        sweep_dir: Path,
        sweep_id: str,
        wandb_group: Optional[str] = None,
        pbs_dir: Optional[Path] = None,
        logs_dir: Optional[Path] = None,
    ) -> List[str]:
        """Submit a complete sweep - either individual jobs or array job."""
        # Use provided directories or default to sweep_dir subdirectories
        if pbs_dir is None:
            pbs_dir = sweep_dir / "scripts"
            pbs_dir.mkdir(exist_ok=True)

        if logs_dir is None:
            logs_dir = sweep_dir / "logs"
            logs_dir.mkdir(exist_ok=True)

        if mode == "array":
            job_id = self.submit_array_job(
                param_combinations, sweep_id, sweep_dir, wandb_group, pbs_dir, logs_dir
            )
            return [job_id]
        else:  # individual mode
            job_ids = []
            for i, params in enumerate(param_combinations):
                job_name = f"{sweep_id}_job_{i + 1:03d}"
                job_id = self.submit_single_job(
                    params,
                    job_name,
                    sweep_dir,
                    sweep_id,
                    wandb_group,
                    pbs_dir,
                    logs_dir,
                )
                job_ids.append(job_id)
            return job_ids
