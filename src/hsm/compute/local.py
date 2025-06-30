"""Local compute source implementation for HSM v2.

This module provides local execution capabilities using the enhanced
ComputeSource interface, supporting async task management and comprehensive
health monitoring.
"""

import asyncio
from datetime import datetime
import logging
from pathlib import Path
import subprocess
import sys
from typing import Any, Dict, List, Optional

from .base import (
    CollectionResult,
    ComputeSource,
    ComputeSourceStats,
    HealthReport,
    HealthStatus,
    SweepContext,
    Task,
    TaskResult,
    TaskStatus,
)

logger = logging.getLogger(__name__)


class LocalComputeSource(ComputeSource):
    """Local compute source for running tasks on the local machine.

    This compute source manages local subprocess execution with support for
    concurrent task execution, resource monitoring, and comprehensive error handling.
    """

    def __init__(
        self,
        name: str = "local",
        max_concurrent_tasks: int = 4,
        python_path: Optional[str] = None,
        script_path: Optional[str] = None,
        project_dir: Optional[str] = None,
        conda_env: Optional[str] = None,
        timeout: int = 3600,  # 1 hour default timeout
    ):
        """Initialize the LocalComputeSource.

        Args:
            name: Unique name for this compute source
            max_concurrent_tasks: Maximum number of concurrent tasks
            python_path: Path to Python interpreter (None for auto-detect)
            script_path: Path to main training script (None for auto-detect)
            project_dir: Project directory (None for current directory)
            conda_env: Conda environment name (None for current environment)
            timeout: Default timeout for tasks in seconds
        """
        super().__init__(
            name=name,
            source_type="local",
            max_parallel_tasks=max_concurrent_tasks,
            health_check_interval=300,
        )
        self.max_concurrent_tasks = max_concurrent_tasks
        self.python_path = python_path or sys.executable
        self.script_path = script_path
        self.project_dir = Path(project_dir) if project_dir else Path.cwd()
        self.conda_env = conda_env
        self.timeout = timeout

        # Task management
        self.active_processes: Dict[str, subprocess.Popen] = {}
        self.task_futures: Dict[str, asyncio.Task] = {}
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)

        # Setup complete flag
        self._setup_complete = False

    async def setup(self, context: SweepContext) -> bool:
        """Setup the local compute source.

        Args:
            context: Sweep context containing configuration and paths

        Returns:
            True if setup successful, False otherwise
        """
        try:
            logger.info(f"Setting up local compute source: {self.name}")

            # Auto-detect script path if not provided
            if not self.script_path:
                self.script_path = self._detect_script_path(context)

            # Verify Python interpreter
            if not await self._verify_python_interpreter():
                return False

            # Verify script exists
            if self.script_path and not Path(self.script_path).exists():
                logger.error(f"Training script not found: {self.script_path}")
                return False

            # Verify project directory
            if not self.project_dir.exists():
                logger.error(f"Project directory not found: {self.project_dir}")
                return False

            # Test conda environment if specified
            if self.conda_env and not await self._verify_conda_env():
                logger.warning(f"Conda environment '{self.conda_env}' not found, using default")
                self.conda_env = None

            # Update statistics
            self.stats = ComputeSourceStats(
                source_name=self.name,
                source_type=self.source_type,
                max_parallel_tasks=self.max_concurrent_tasks,
                health_status=HealthStatus.HEALTHY,
                last_health_check=datetime.now(),
            )

            self._setup_complete = True
            logger.info(f"✓ Local compute source {self.name} setup successful")
            return True

        except Exception as e:
            logger.error(f"Error setting up local compute source {self.name}: {e}")
            self.stats.status = HealthStatus.UNHEALTHY
            return False

    async def submit_task(self, task: Task) -> TaskResult:
        """Submit a task for execution.

        Args:
            task: Task to execute

        Returns:
            TaskResult with submission status
        """
        if not self._setup_complete:
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                error_message="Compute source not properly set up",
            )

        try:
            # Register task first
            self.register_task(task)

            # Ensure task directory exists (use the output_dir set by SweepEngine)
            if task.output_dir:
                task.output_dir.mkdir(parents=True, exist_ok=True)
                task_dir = task.output_dir
            else:
                # Fallback: create a basic task directory
                task_dir = self.project_dir / "tasks" / task.task_id
                task_dir.mkdir(parents=True, exist_ok=True)

            # Write task parameters
            params_file = task_dir / "params.yaml"
            with open(params_file, "w") as f:
                import yaml

                yaml.dump(task.params, f, default_flow_style=False, indent=2)

            # Start task execution
            future = asyncio.create_task(self._execute_task(task))
            self.task_futures[task.task_id] = future

            self.stats.tasks_submitted += 1

            logger.debug(f"Task {task.task_id} submitted to local source {self.name}")

            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.QUEUED,
                timestamp=datetime.now(),
            )

        except Exception as e:
            logger.error(f"Failed to submit task {task.task_id}: {e}")
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                error_message=str(e),
            )

    async def get_task_status(self, task_id: str) -> TaskResult:
        """Get the current status of a task.

        Args:
            task_id: ID of the task to check

        Returns:
            TaskResult with current status and metadata
        """
        if task_id not in self.task_registry:
            return TaskResult(
                task_id=task_id,
                status=TaskStatus.UNKNOWN,
                error_message="Task not found in registry",
            )

        current_status = self.task_registry[task_id]

        # Check if task completed and we can read additional details
        task_dir = self.context.sweep_dir / "tasks" / task_id if hasattr(self, "context") else None

        error_message = None
        exit_code = None
        complete_time = None

        if task_dir and task_dir.exists():
            try:
                # Try to read status file for more details
                status_file = task_dir / "status.yaml"
                if status_file.exists():
                    import yaml

                    with open(status_file, "r") as f:
                        status_data = yaml.safe_load(f)

                    if status_data:
                        error_message = status_data.get("error_message")
                        exit_code = status_data.get("exit_code")
                        if status_data.get("complete_time"):
                            complete_time = datetime.fromisoformat(status_data["complete_time"])

                # Also check for error files
                if current_status == TaskStatus.FAILED and not error_message:
                    error_file = task_dir / "stderr.log"
                    if error_file.exists():
                        try:
                            with open(error_file, "r") as f:
                                stderr_content = f.read().strip()
                                if stderr_content:
                                    # Take last few lines as error message
                                    lines = stderr_content.split("\n")
                                    error_message = (
                                        "\n".join(lines[-3:]) if len(lines) > 3 else stderr_content
                                    )
                        except Exception:
                            pass

            except Exception as e:
                logger.debug(f"Error reading task details for {task_id}: {e}")

        return TaskResult(
            task_id=task_id,
            status=current_status,
            submit_time=datetime.now(),  # We should track this better
            complete_time=complete_time,
            exit_code=exit_code,
            error_message=error_message,
        )

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a running task.

        Args:
            task_id: ID of the task to cancel

        Returns:
            True if cancellation successful, False otherwise
        """
        try:
            # Cancel future if it exists
            if task_id in self.task_futures:
                future = self.task_futures[task_id]
                if not future.done():
                    future.cancel()
                    logger.debug(f"Cancelled future for task {task_id}")

            # Terminate process if it exists
            if task_id in self.active_processes:
                process = self.active_processes[task_id]
                if process.returncode is None:  # Process is still running (returncode is None)
                    try:
                        # Graceful termination first
                        process.terminate()
                        await asyncio.sleep(2)

                        # Force kill if still running
                        if process.returncode is None:  # Still running
                            process.kill()

                        logger.debug(f"Terminated process for task {task_id}")
                    except ProcessLookupError:
                        # Process already terminated
                        pass

                del self.active_processes[task_id]

            # Update task status
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                self.update_task_status(task_id, TaskStatus.CANCELLED)

                # Write status to file if we have a task directory
                if task.output_dir:
                    await self._write_task_status_to_dir(
                        task.output_dir, task_id, TaskStatus.CANCELLED
                    )

                # Move to completed tasks as cancelled
                del self.active_tasks[task_id]

            self.stats.tasks_cancelled += 1

            return True

        except Exception as e:
            logger.error(f"Error cancelling task {task_id}: {e}")
            return False

    async def collect_results(self, task_ids: List[str]) -> CollectionResult:
        """Collect results from completed tasks.

        Args:
            task_ids: List of task IDs to collect results for

        Returns:
            CollectionResult with collection status
        """
        collected_tasks = []
        failed_tasks = []

        for task_id in task_ids:
            try:
                # Get task output directory
                task = self.active_tasks.get(task_id)
                if not task or not task.output_dir:
                    failed_tasks.append((task_id, "Task not found or no output directory"))
                    continue

                task_dir = task.output_dir

                if not task_dir.exists():
                    failed_tasks.append((task_id, "Task directory not found"))
                    continue

                # Check if task is completed
                status = await self.get_task_status(task_id)
                if status.status not in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                    continue  # Skip tasks that are not done

                # Results are already local, just verify they exist
                results_dir = task_dir / "results"
                if results_dir.exists():
                    collected_tasks.append(task_id)
                else:
                    # If no results directory, consider the task outputs as results
                    collected_tasks.append(task_id)

            except Exception as e:
                failed_tasks.append((task_id, str(e)))

        return CollectionResult(
            collected_tasks=collected_tasks,
            failed_tasks=[task_id for task_id, _ in failed_tasks],
            errors=[error for _, error in failed_tasks],
        )

    async def health_check(self) -> HealthReport:
        """Perform health check and return comprehensive status.

        Returns:
            HealthReport with detailed health information
        """
        try:
            # Get system metrics
            metrics = await self._get_system_metrics()

            # Update health status based on current state
            if self._setup_complete:
                self.stats.health_status = HealthStatus.HEALTHY
                self.stats.consecutive_failures = 0
            else:
                self.stats.health_status = HealthStatus.UNHEALTHY
                self.stats.consecutive_failures += 1

            # Calculate current active tasks (don't set it, just use for reporting)
            current_active_tasks = len(self.active_processes)

            self.stats.last_health_check = datetime.now()

            return HealthReport(
                source_name=self.name,
                status=self.stats.health_status,
                timestamp=datetime.now(),
                available_slots=self.max_concurrent_tasks - current_active_tasks,
                max_slots=self.max_concurrent_tasks,
                active_tasks=current_active_tasks,
                cpu_usage=metrics.get("cpu_usage"),
                memory_usage=metrics.get("memory_usage"),
                disk_free_gb=metrics.get("disk_free_gb"),
                load_average=metrics.get("load_average"),
                message=f"Local compute source running {current_active_tasks}/{self.max_concurrent_tasks} tasks",
            )

        except Exception as e:
            logger.error(f"Health check failed for {self.name}: {e}")
            self.stats.health_status = HealthStatus.UNHEALTHY
            self.stats.consecutive_failures += 1
            self.stats.last_health_check = datetime.now()

            return HealthReport(
                source_name=self.name,
                status=HealthStatus.UNHEALTHY,
                timestamp=datetime.now(),
                available_slots=0,
                max_slots=self.max_concurrent_tasks,
                active_tasks=0,
                message=f"Health check failed: {str(e)}",
            )

    async def cleanup(self) -> bool:
        """Cleanup compute source resources.

        Returns:
            True if cleanup successful, False otherwise
        """
        try:
            logger.info(f"Cleaning up local compute source: {self.name}")

            # Cancel all running tasks
            cancel_tasks = []
            for task_id in list(self.active_processes.keys()):
                cancel_tasks.append(self.cancel_task(task_id))

            if cancel_tasks:
                await asyncio.gather(*cancel_tasks, return_exceptions=True)

            # Wait for futures to complete
            if self.task_futures:
                await asyncio.gather(*self.task_futures.values(), return_exceptions=True)
                self.task_futures.clear()

            logger.info(f"✓ Local compute source {self.name} cleanup complete")
            return True

        except Exception as e:
            logger.error(f"Error during cleanup of {self.name}: {e}")
            return False

    async def _execute_task(self, task: Task) -> None:
        """Execute a single task.

        Args:
            task: Task to execute
        """
        async with self.semaphore:
            # Use the task's output_dir directly (set by SweepEngine)
            task_dir = task.output_dir
            if not task_dir:
                logger.error(f"Task {task.task_id} has no output directory set")
                return

            start_time = datetime.now()

            try:
                # Update status to running
                self.update_task_status(task.task_id, TaskStatus.RUNNING)

                # Write initial status
                await self._write_task_status_to_dir(
                    task_dir, task.task_id, TaskStatus.RUNNING, start_time=start_time
                )

                # Build command
                command = await self._build_command(task)

                # Run the process
                process = await asyncio.create_subprocess_exec(
                    *command,
                    cwd=self.project_dir,
                    env=self._build_environment(),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                self.active_processes[task.task_id] = process

                # Wait for completion with timeout
                try:
                    stdout_bytes, stderr_bytes = await asyncio.wait_for(
                        process.communicate(), timeout=self.timeout
                    )
                    exit_code = process.returncode

                except asyncio.TimeoutError:
                    logger.warning(f"Task {task.task_id} timed out after {self.timeout} seconds")
                    process.kill()
                    await process.wait()
                    exit_code = -1
                    stdout_bytes = stderr_bytes = b"Task timed out"

                # Clean up
                del self.active_processes[task.task_id]

                # Determine final status
                if exit_code == 0:
                    final_status = TaskStatus.COMPLETED
                    self.stats.tasks_completed += 1
                    logger.info(f"Task {task.task_id} completed successfully")
                else:
                    final_status = TaskStatus.FAILED
                    self.stats.tasks_failed += 1
                    logger.warning(f"Task {task.task_id} failed with exit code {exit_code}")

                # Update status and move task appropriately
                self.update_task_status(task.task_id, final_status)
                if task.task_id in self.active_tasks:
                    del self.active_tasks[task.task_id]

                # Write outputs and status to the task directory
                await self._write_output_files_to_dir(task_dir, stdout_bytes, stderr_bytes)
                await self._write_task_status_to_dir(
                    task_dir,
                    task.task_id,
                    final_status,
                    start_time=start_time,
                    complete_time=datetime.now(),
                    exit_code=exit_code,
                )

            except Exception as e:
                logger.error(f"Error executing task {task.task_id}: {e}")

                # Update status and move task appropriately
                self.update_task_status(task.task_id, TaskStatus.FAILED)
                if task.task_id in self.active_tasks:
                    del self.active_tasks[task.task_id]

                self.stats.tasks_failed += 1

                await self._write_task_status_to_dir(
                    task_dir,
                    task.task_id,
                    TaskStatus.FAILED,
                    start_time=start_time,
                    complete_time=datetime.now(),
                    error_message=str(e),
                )

    def _detect_script_path(self, context: SweepContext) -> Optional[str]:
        """Auto-detect the training script path.

        Args:
            context: Sweep context

        Returns:
            Detected script path or None
        """
        # Common script names to search for
        common_names = ["train.py", "main.py", "run.py", "training.py"]

        for name in common_names:
            script_path = self.project_dir / name
            if script_path.exists():
                logger.debug(f"Auto-detected script: {script_path}")
                return str(script_path)

        logger.warning("Could not auto-detect training script")
        return None

    async def _verify_python_interpreter(self) -> bool:
        """Verify that the Python interpreter is valid.

        Returns:
            True if valid, False otherwise
        """
        try:
            process = await asyncio.create_subprocess_exec(
                self.python_path,
                "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                version = stdout.decode().strip() or stderr.decode().strip()
                logger.debug(f"Python interpreter verified: {version}")
                return True
            else:
                logger.error(f"Python interpreter verification failed: {self.python_path}")
                return False

        except Exception as e:
            logger.error(f"Error verifying Python interpreter: {e}")
            return False

    async def _verify_conda_env(self) -> bool:
        """Verify that the conda environment exists.

        Returns:
            True if environment exists, False otherwise
        """
        try:
            process = await asyncio.create_subprocess_exec(
                "conda",
                "env",
                "list",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                env_list = stdout.decode()
                return self.conda_env in env_list
            else:
                return False

        except Exception:
            return False

    async def _build_command(self, task: Task) -> List[str]:
        """Build the command to execute the task.

        Args:
            task: Task to build command for

        Returns:
            List of command arguments
        """
        cmd = []

        # Add conda activation if needed
        if self.conda_env:
            cmd.extend(["conda", "run", "-n", self.conda_env])

        # Add Python interpreter
        cmd.append(self.python_path)

        # Add script
        if self.script_path:
            cmd.append(self.script_path)

        # Add task parameters as command line arguments
        for key, value in task.params.items():
            if isinstance(value, bool):
                if value:
                    cmd.append(f"--{key}")
            else:
                cmd.extend([f"--{key}", str(value)])

        return cmd

    def _build_environment(self) -> Dict[str, str]:
        """Build environment variables for task execution.

        Returns:
            Dictionary of environment variables
        """
        import os

        env = os.environ.copy()

        # Add any custom environment variables here
        env["HSM_TASK_MODE"] = "local"

        return env

    async def _write_output_files_to_dir(
        self, task_dir: Path, stdout: bytes, stderr: bytes
    ) -> None:
        """Write task output files.

        Args:
            task_dir: Task directory
            stdout: Standard output
            stderr: Standard error
        """
        # Write stdout
        stdout_file = task_dir / "stdout.log"
        with open(stdout_file, "wb") as f:
            f.write(stdout)

        # Write stderr
        stderr_file = task_dir / "stderr.log"
        with open(stderr_file, "wb") as f:
            f.write(stderr)

    async def _write_task_status_to_dir(
        self,
        task_dir: Path,
        task_id: str,
        status: TaskStatus,
        start_time: Optional[datetime] = None,
        complete_time: Optional[datetime] = None,
        exit_code: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Write task status to file.

        Args:
            task_dir: Task directory
            task_id: Task ID
            status: Task status
            start_time: Task start time
            complete_time: Task completion time
            exit_code: Process exit code
            error_message: Error message if failed
        """
        status_file = task_dir / "status.yaml"

        status_data = {
            "status": status.value,
            "timestamp": datetime.now().isoformat(),
        }

        if start_time:
            status_data["start_time"] = start_time.isoformat()
        if complete_time:
            status_data["complete_time"] = complete_time.isoformat()
        if exit_code is not None:
            status_data["exit_code"] = exit_code
        if error_message:
            status_data["error_message"] = error_message

        with open(status_file, "w") as f:
            import yaml

            yaml.dump(status_data, f, default_flow_style=False, indent=2)

    async def _get_system_metrics(self) -> Dict[str, Any]:
        """Get system performance metrics.

        Returns:
            Dictionary of system metrics
        """
        metrics = {}

        try:
            import psutil

            # CPU metrics
            metrics["cpu_percent"] = psutil.cpu_percent(interval=1)
            metrics["cpu_count"] = psutil.cpu_count()

            # Memory metrics
            memory = psutil.virtual_memory()
            metrics["memory_percent"] = memory.percent
            metrics["memory_available_gb"] = memory.available / (1024**3)
            metrics["memory_total_gb"] = memory.total / (1024**3)

            # Disk metrics
            disk = psutil.disk_usage(str(self.project_dir))
            metrics["disk_percent"] = (disk.used / disk.total) * 100
            metrics["disk_free_gb"] = disk.free / (1024**3)
            metrics["disk_total_gb"] = disk.total / (1024**3)

            # Process metrics
            metrics["active_tasks"] = len(self.active_processes)
            metrics["max_tasks"] = self.max_concurrent_tasks

        except ImportError:
            logger.warning("psutil not available, system metrics limited")
            metrics["cpu_percent"] = 0
            metrics["memory_percent"] = 0
            metrics["disk_percent"] = 0

        return metrics
