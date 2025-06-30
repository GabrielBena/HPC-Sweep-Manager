"""SSH compute source implementation for HSM v2.

This module provides SSH-based remote execution capabilities using the enhanced
ComputeSource interface, supporting async remote task management, result syncing,
and comprehensive health monitoring.
"""

import asyncio
from datetime import datetime
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import asyncssh

    ASYNCSSH_AVAILABLE = True
except ImportError:
    ASYNCSSH_AVAILABLE = False
    asyncssh = None

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


class SSHConfig:
    """Configuration for SSH connections."""

    def __init__(
        self,
        host: str,
        username: Optional[str] = None,
        port: int = 22,
        key_file: Optional[str] = None,
        password: Optional[str] = None,
        known_hosts: Optional[str] = None,
        project_dir: Optional[str] = None,
        python_path: Optional[str] = None,
        conda_env: Optional[str] = None,
    ):
        self.host = host
        self.username = username
        self.port = port
        self.key_file = key_file
        self.password = password
        self.known_hosts = known_hosts
        self.project_dir = project_dir
        self.python_path = python_path
        self.conda_env = conda_env

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "host": self.host,
            "username": self.username,
            "port": self.port,
            "key_file": self.key_file,
            "project_dir": self.project_dir,
            "python_path": self.python_path,
            "conda_env": self.conda_env,
        }


class SSHComputeSource(ComputeSource):
    """SSH compute source for running tasks on remote machines.

    This compute source manages SSH connections and remote task execution with
    support for result synchronization, health monitoring, and error recovery.
    """

    def __init__(
        self,
        name: str,
        ssh_config: SSHConfig,
        max_concurrent_tasks: int = 4,
        script_path: Optional[str] = None,
        timeout: int = 3600,  # 1 hour default timeout
        sync_interval: int = 30,  # 30 seconds between syncs
    ):
        """Initialize the SSHComputeSource.

        Args:
            name: Unique name for this compute source
            ssh_config: SSH connection configuration
            max_concurrent_tasks: Maximum number of concurrent tasks
            script_path: Path to main training script (None for auto-detect)
            timeout: Default timeout for tasks in seconds
            sync_interval: Interval between result syncs in seconds
        """
        if not ASYNCSSH_AVAILABLE:
            raise ImportError("asyncssh is required for SSH compute source")

        super().__init__(
            name=name,
            source_type="ssh",
            max_parallel_tasks=max_concurrent_tasks,
            health_check_interval=300,
        )
        self.ssh_config = ssh_config
        self.max_concurrent_tasks = max_concurrent_tasks
        self.script_path = script_path
        self.timeout = timeout
        self.sync_interval = sync_interval

        # Connection management
        self.connection: Optional[asyncssh.SSHClientConnection] = None
        self.connection_lock = asyncio.Lock()

        # Task management
        self.active_processes: Dict[str, asyncssh.SSHClientProcess] = {}
        self.task_futures: Dict[str, asyncio.Task] = {}
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)

        # Remote directories
        self.remote_sweep_dir: Optional[str] = None
        self.local_sweep_dir: Optional[Path] = None

        # Background tasks
        self._sync_task: Optional[asyncio.Task] = None
        self._setup_complete = False

    async def setup(self, context: SweepContext) -> bool:
        """Setup the SSH compute source.

        Args:
            context: Sweep context containing configuration and paths

        Returns:
            True if setup successful, False otherwise
        """
        try:
            logger.info(f"Setting up SSH compute source: {self.name} ({self.ssh_config.host})")

            # Establish SSH connection
            if not await self._connect():
                return False

            # Setup remote environment
            if not await self._setup_remote_environment(context):
                return False

            # Auto-detect script path if not provided
            if not self.script_path:
                self.script_path = await self._detect_script_path(context)

            # Verify remote setup
            if not await self._verify_remote_setup():
                return False

            # Setup result syncing
            self.local_sweep_dir = context.sweep_dir
            await self._setup_result_sync()

            # Update statistics
            self.stats = ComputeSourceStats(
                source_name=self.name,
                source_type=self.source_type,
                max_parallel_tasks=self.max_concurrent_tasks,
                health_status=HealthStatus.HEALTHY,
                last_health_check=datetime.now(),
            )

            self._setup_complete = True
            logger.info(f"✓ SSH compute source {self.name} setup successful")
            return True

        except Exception as e:
            logger.error(f"Error setting up SSH compute source {self.name}: {e}")
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
            # Create remote task directory
            remote_task_dir = f"{self.remote_sweep_dir}/tasks/{task.task_id}"
            await self._execute_command(f"mkdir -p {remote_task_dir}")

            # Write task parameters
            params_content = self._create_params_file(task.params)
            await self._write_remote_file(f"{remote_task_dir}/params.yaml", params_content)

            # Start task execution
            future = asyncio.create_task(self._execute_task(task))
            self.task_futures[task.task_id] = future

            # Register task
            self.register_task(task.task_id, TaskStatus.PENDING)
            self.stats.total_submitted += 1

            logger.debug(f"Task {task.task_id} submitted to SSH source {self.name}")

            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.PENDING,
                submit_time=datetime.now(),
            )

        except Exception as e:
            logger.error(f"Failed to submit task {task.task_id}: {e}")
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                error_message=str(e),
            )

    async def get_task_status(self, task_id: str) -> TaskResult:
        """Get current status of a task.

        Args:
            task_id: ID of the task

        Returns:
            TaskResult with current status and metadata
        """
        try:
            # Check if task is in our registry first
            if task_id in self.task_registry:
                status = self.task_registry[task_id]
                return TaskResult(
                    task_id=task_id,
                    status=status,
                    submit_time=datetime.now(),  # We should track this better
                )

            # If not available, ensure connection and check remote status
            if not await self._check_connection():
                return TaskResult(
                    task_id=task_id,
                    status=TaskStatus.UNKNOWN,
                    error_message="SSH connection not available",
                )

            # Check remote status file
            remote_status_file = f"{self.remote_sweep_dir}/tasks/{task_id}/status.yaml"
            status_content = await self._read_remote_file(remote_status_file)

            if status_content:
                import yaml

                status_data = yaml.safe_load(status_content)
                task_status = TaskStatus(status_data.get("status", "PENDING"))

                # Update our local registry
                self.register_task(task_id, task_status)

                # Check for failure details
                error_message = status_data.get("error_message")
                exit_code = status_data.get("exit_code")

                return TaskResult(
                    task_id=task_id,
                    status=task_status,
                    submit_time=datetime.now(),  # Should be tracked better
                    complete_time=datetime.now()
                    if task_status
                    in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]
                    else None,
                    exit_code=exit_code,
                    error_message=error_message,
                )
            else:
                # No status file found, task might be starting or failed to start
                logger.debug(f"No status file found for task {task_id} on remote")
                return TaskResult(
                    task_id=task_id,
                    status=TaskStatus.PENDING,
                    submit_time=datetime.now(),
                )

        except Exception as e:
            logger.error(f"Error reading remote status for {task_id}: {e}")
            return TaskResult(
                task_id=task_id,
                status=TaskStatus.UNKNOWN,
                error_message=f"Error checking status: {str(e)}",
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

            # Terminate remote process if it exists
            if task_id in self.active_processes:
                process = self.active_processes[task_id]
                try:
                    process.terminate()
                    await asyncio.sleep(2)

                    # Force kill if still running
                    if not process.is_closing():
                        process.kill()

                    logger.debug(f"Terminated remote process for task {task_id}")
                except Exception as e:
                    logger.debug(f"Error terminating remote process: {e}")

                del self.active_processes[task_id]

            # Update task status
            self.register_task(task_id, TaskStatus.CANCELLED)
            self.stats.total_cancelled += 1

            # Write remote status
            await self._write_remote_task_status(task_id, TaskStatus.CANCELLED)

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
                # Check if task is completed
                status = await self.get_task_status(task_id)
                if status not in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                    continue  # Skip tasks that are not done

                # Sync task results
                local_task_dir = self.local_sweep_dir / "tasks" / task_id
                remote_task_dir = f"{self.remote_sweep_dir}/tasks/{task_id}"

                if await self._sync_directory(remote_task_dir, local_task_dir):
                    collected_tasks.append(task_id)
                else:
                    failed_tasks.append((task_id, "Failed to sync results"))

            except Exception as e:
                failed_tasks.append((task_id, str(e)))

        return CollectionResult(
            source_name=self.name,
            collected_tasks=collected_tasks,
            failed_tasks=failed_tasks,
            collection_time=datetime.now(),
        )

    async def health_check(self) -> HealthReport:
        """Perform health check and return comprehensive status.

        Returns:
            HealthReport with detailed health information
        """
        try:
            # Check SSH connection
            if not await self._check_connection():
                return HealthReport(
                    source_name=self.name,
                    status=HealthStatus.UNHEALTHY,
                    timestamp=datetime.now(),
                    error_message="SSH connection failed",
                )

            # Get remote system metrics
            metrics = await self._get_remote_metrics()

            # Determine health status
            status = HealthStatus.HEALTHY
            warnings = []

            # Check resource usage
            if metrics.get("cpu_percent", 0) > 90:
                status = HealthStatus.DEGRADED
                warnings.append("High CPU usage on remote host")

            if metrics.get("memory_percent", 0) > 95:
                status = HealthStatus.UNHEALTHY
                warnings.append("Critical memory usage on remote host")
            elif metrics.get("memory_percent", 0) > 80:
                if status == HealthStatus.HEALTHY:
                    status = HealthStatus.DEGRADED
                warnings.append("High memory usage on remote host")

            if metrics.get("disk_percent", 0) > 95:
                status = HealthStatus.UNHEALTHY
                warnings.append("Critical disk usage on remote host")

            # Update stats
            self.stats.status = status
            self.stats.last_health_check = datetime.now()
            self.stats.active_tasks = len(self.active_processes)

            return HealthReport(
                source_name=self.name,
                status=status,
                timestamp=datetime.now(),
                metrics=metrics,
                warnings=warnings,
                active_tasks=len(self.active_processes),
                max_tasks=self.max_concurrent_tasks,
            )

        except Exception as e:
            logger.error(f"Health check failed for {self.name}: {e}")
            return HealthReport(
                source_name=self.name,
                status=HealthStatus.UNHEALTHY,
                timestamp=datetime.now(),
                error_message=str(e),
            )

    async def cleanup(self) -> bool:
        """Cleanup compute source resources.

        Returns:
            True if cleanup successful, False otherwise
        """
        try:
            logger.info(f"Cleaning up SSH compute source: {self.name}")

            # Stop sync task
            if self._sync_task and not self._sync_task.done():
                self._sync_task.cancel()
                try:
                    await self._sync_task
                except asyncio.CancelledError:
                    pass

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

            # Close SSH connection
            if self.connection:
                self.connection.close()
                await self.connection.wait_closed()
                self.connection = None

            logger.info(f"✓ SSH compute source {self.name} cleanup complete")
            return True

        except Exception as e:
            logger.error(f"Error during cleanup of {self.name}: {e}")
            return False

    # Helper methods for SSH operations

    async def _connect(self) -> bool:
        """Establish SSH connection."""
        try:
            async with self.connection_lock:
                if self.connection:
                    try:
                        # Test if connection is still alive with a simple command
                        await self.connection.run("echo test", check=True, timeout=10)
                        return True
                    except Exception:
                        # Connection is broken, will create a new one
                        self.connection = None

                # Prepare connection options
                options = {
                    "host": self.ssh_config.host,
                    "port": self.ssh_config.port,
                }

                # Only add username if it's provided
                if self.ssh_config.username:
                    options["username"] = self.ssh_config.username

                # Handle SSH key authentication
                if self.ssh_config.key_file:
                    options["client_keys"] = [self.ssh_config.key_file]

                # Handle password authentication
                if self.ssh_config.password:
                    options["password"] = self.ssh_config.password

                # Handle known hosts
                if self.ssh_config.known_hosts:
                    options["known_hosts"] = self.ssh_config.known_hosts
                else:
                    options["known_hosts"] = None  # Skip host key verification

                logger.debug(f"Connecting to SSH with options: {list(options.keys())}")
                self.connection = await asyncssh.connect(**options)
                logger.debug(f"SSH connection established to {self.ssh_config.host}")
                return True

        except Exception as e:
            logger.error(f"Failed to establish SSH connection to {self.ssh_config.host}: {e}")
            self.connection = None
            return False

    async def _check_connection(self) -> bool:
        """Check if SSH connection is alive."""
        try:
            if not self.connection:
                return await self._connect()

            # Test connection with a simple command
            result = await self.connection.run("echo test", check=True, timeout=10)
            return result.stdout.strip() == "test"

        except Exception:
            return await self._connect()

    async def _execute_command(self, command: str, check: bool = True) -> str:
        """Execute a command on the remote host."""
        if not await self._check_connection():
            raise ConnectionError("SSH connection not available")

        result = await self.connection.run(command, check=check)
        return result.stdout

    async def _read_remote_file(self, remote_path: str) -> Optional[str]:
        """Read a file from the remote host."""
        try:
            async with self.connection.start_sftp_client() as sftp:
                async with sftp.open(remote_path, "r") as f:
                    return await f.read()
        except Exception:
            return None

    async def _write_remote_file(self, remote_path: str, content: str) -> None:
        """Write a file to the remote host."""
        async with self.connection.start_sftp_client() as sftp:
            # Ensure directory exists
            remote_dir = "/".join(remote_path.split("/")[:-1])
            await self._execute_command(f"mkdir -p {remote_dir}", check=False)

            async with sftp.open(remote_path, "w") as f:
                await f.write(content)

    async def _setup_remote_environment(self, context: SweepContext) -> bool:
        """Setup the remote environment for task execution."""
        try:
            # Setup remote project directory
            project_dir = self.ssh_config.project_dir or f"~/hsm_projects/{context.sweep_id}"
            self.remote_sweep_dir = f"{project_dir}/sweeps/outputs/{context.sweep_id}"

            # Create directories
            await self._execute_command(f"mkdir -p {self.remote_sweep_dir}/tasks")
            await self._execute_command(f"mkdir -p {self.remote_sweep_dir}/logs")

            # Copy project files if needed (simplified - in real implementation,
            # this would sync the entire project)
            logger.debug(f"Remote environment setup at {project_dir}")
            return True

        except Exception as e:
            logger.error(f"Failed to setup remote environment: {e}")
            return False

    async def _detect_script_path(self, context: SweepContext) -> Optional[str]:
        """Auto-detect the training script path on remote host."""
        common_names = ["train.py", "main.py", "run.py", "training.py"]
        project_dir = self.ssh_config.project_dir or "~"

        for name in common_names:
            script_path = f"{project_dir}/{name}"
            try:
                await self._execute_command(f"test -f {script_path}")
                logger.debug(f"Auto-detected remote script: {script_path}")
                return script_path
            except:
                continue

        logger.warning("Could not auto-detect remote training script")
        return None

    async def _verify_remote_setup(self) -> bool:
        """Verify that the remote setup is correct."""
        try:
            # Check Python interpreter
            python_path = self.ssh_config.python_path or "python"
            await self._execute_command(f"{python_path} --version")

            # Check script if specified
            if self.script_path:
                await self._execute_command(f"test -f {self.script_path}")

            # Check conda environment if specified
            if self.ssh_config.conda_env:
                await self._execute_command(f"conda env list | grep {self.ssh_config.conda_env}")

            return True

        except Exception as e:
            logger.error(f"Remote setup verification failed: {e}")
            return False

    async def _setup_result_sync(self) -> None:
        """Setup background result synchronization."""
        self._sync_task = asyncio.create_task(self._sync_loop())

    async def _sync_loop(self) -> None:
        """Background loop for syncing results."""
        while True:
            try:
                await asyncio.sleep(self.sync_interval)

                # Sync completed task results
                completed_tasks = [
                    task_id
                    for task_id, status in self.task_registry.items()
                    if status in [TaskStatus.COMPLETED, TaskStatus.FAILED]
                ]

                if completed_tasks:
                    await self.collect_results(completed_tasks)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")

    async def _sync_directory(self, remote_dir: str, local_dir: Path) -> bool:
        """Sync a directory from remote to local."""
        try:
            local_dir.mkdir(parents=True, exist_ok=True)

            async with self.connection.start_sftp_client() as sftp:
                # Get list of remote files
                try:
                    remote_files = await sftp.listdir(remote_dir)
                except:
                    return False

                # Download each file
                for filename in remote_files:
                    remote_file = f"{remote_dir}/{filename}"
                    local_file = local_dir / filename

                    try:
                        await sftp.get(remote_file, local_file)
                    except Exception as e:
                        logger.debug(f"Failed to sync {remote_file}: {e}")

                return True

        except Exception as e:
            logger.error(f"Failed to sync directory {remote_dir}: {e}")
            return False

    async def _execute_task(self, task: Task) -> None:
        """Execute a task on the remote host."""
        async with self.semaphore:
            remote_task_dir = f"{self.remote_sweep_dir}/tasks/{task.task_id}"

            try:
                # Update status to running
                self.register_task(task.task_id, TaskStatus.RUNNING)
                await self._write_remote_task_status(
                    task.task_id, TaskStatus.RUNNING, start_time=datetime.now()
                )

                # Build command
                cmd = await self._build_remote_command(task, remote_task_dir)

                # Start remote process
                logger.debug(f"Starting remote task {task.task_id}: {cmd}")

                process = await self.connection.create_process(
                    cmd,
                    cwd=self.ssh_config.project_dir or "~",
                    stdout=asyncssh.PIPE,
                    stderr=asyncssh.PIPE,
                )

                # Store process for cancellation
                self.active_processes[task.task_id] = process

                # Wait for completion with timeout
                try:
                    stdout, stderr = await asyncio.wait_for(
                        process.communicate(), timeout=self.timeout
                    )

                    # Write output files
                    await self._write_remote_output_files(task.task_id, stdout, stderr)

                    # Determine final status
                    if process.returncode == 0:
                        final_status = TaskStatus.COMPLETED
                        self.stats.total_completed += 1
                    else:
                        final_status = TaskStatus.FAILED
                        self.stats.total_failed += 1

                    # Update status
                    self.register_task(task.task_id, final_status)
                    await self._write_remote_task_status(
                        task.task_id,
                        final_status,
                        complete_time=datetime.now(),
                        exit_code=process.returncode,
                    )

                except asyncio.TimeoutError:
                    # Task timed out
                    process.kill()
                    await process.wait()

                    self.register_task(task.task_id, TaskStatus.FAILED)
                    await self._write_remote_task_status(
                        task.task_id,
                        TaskStatus.FAILED,
                        complete_time=datetime.now(),
                        error_message="Task timed out",
                    )
                    self.stats.total_failed += 1

            except Exception as e:
                # Task execution failed
                logger.error(f"Remote task {task.task_id} execution failed: {e}")
                self.register_task(task.task_id, TaskStatus.FAILED)
                await self._write_remote_task_status(
                    task.task_id,
                    TaskStatus.FAILED,
                    complete_time=datetime.now(),
                    error_message=str(e),
                )
                self.stats.total_failed += 1

            finally:
                # Cleanup
                if task.task_id in self.active_processes:
                    del self.active_processes[task.task_id]
                if task.task_id in self.task_futures:
                    del self.task_futures[task.task_id]

    async def _build_remote_command(self, task: Task, remote_task_dir: str) -> str:
        """Build the command to execute the task remotely."""
        cmd_parts = []

        # Change to task directory
        cmd_parts.append(f"cd {remote_task_dir}")

        # Add conda activation if needed
        if self.ssh_config.conda_env:
            cmd_parts.append(f"conda activate {self.ssh_config.conda_env}")

        # Build Python command
        python_path = self.ssh_config.python_path or "python"
        python_cmd = [python_path]

        if self.script_path:
            python_cmd.append(self.script_path)

        # Add task parameters as command line arguments
        for key, value in task.params.items():
            if isinstance(value, bool):
                if value:
                    python_cmd.append(f"--{key}")
            else:
                python_cmd.extend([f"--{key}", str(value)])

        # Redirect output
        python_cmd.extend(
            [">", f"{remote_task_dir}/stdout.log", "2>", f"{remote_task_dir}/stderr.log"]
        )

        cmd_parts.append(" ".join(python_cmd))

        return " && ".join(cmd_parts)

    def _create_params_file(self, params: Dict[str, Any]) -> str:
        """Create YAML content for parameters file."""
        import yaml

        return yaml.dump(params, default_flow_style=False, indent=2)

    async def _write_remote_task_status(
        self,
        task_id: str,
        status: TaskStatus,
        start_time: Optional[datetime] = None,
        complete_time: Optional[datetime] = None,
        exit_code: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Write task status to remote file."""
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

        import yaml

        status_content = yaml.dump(status_data, default_flow_style=False, indent=2)

        remote_status_file = f"{self.remote_sweep_dir}/tasks/{task_id}/status.yaml"
        await self._write_remote_file(remote_status_file, status_content)

    async def _write_remote_output_files(self, task_id: str, stdout: str, stderr: str) -> None:
        """Write task output files to remote host."""
        remote_task_dir = f"{self.remote_sweep_dir}/tasks/{task_id}"

        # Note: In a real implementation, output is already redirected
        # This is here for completeness in case direct capture is used
        if stdout:
            await self._write_remote_file(f"{remote_task_dir}/stdout.log", stdout)
        if stderr:
            await self._write_remote_file(f"{remote_task_dir}/stderr.log", stderr)

    async def _get_remote_metrics(self) -> Dict[str, Any]:
        """Get system performance metrics from remote host."""
        metrics = {}

        try:
            # CPU usage
            cpu_cmd = 'python3 -c "import psutil; print(psutil.cpu_percent(interval=1))"'
            try:
                cpu_output = await self._execute_command(cpu_cmd, check=False)
                metrics["cpu_percent"] = float(cpu_output.strip())
            except:
                metrics["cpu_percent"] = 0

            # Memory usage
            mem_cmd = "python3 -c \"import psutil; m=psutil.virtual_memory(); print(f'{m.percent},{m.available/1024**3},{m.total/1024**3}')\""
            try:
                mem_output = await self._execute_command(mem_cmd, check=False)
                mem_percent, mem_avail, mem_total = map(float, mem_output.strip().split(","))
                metrics["memory_percent"] = mem_percent
                metrics["memory_available_gb"] = mem_avail
                metrics["memory_total_gb"] = mem_total
            except:
                metrics["memory_percent"] = 0
                metrics["memory_available_gb"] = 0
                metrics["memory_total_gb"] = 0

            # Disk usage
            disk_path = self.ssh_config.project_dir or "~"
            disk_cmd = f"python3 -c \"import psutil; d=psutil.disk_usage('{disk_path}'); print(f'{{(d.used/d.total)*100:.2f}},{{d.free/1024**3:.2f}},{{d.total/1024**3:.2f}}')\""
            try:
                disk_output = await self._execute_command(disk_cmd, check=False)
                disk_percent, disk_free, disk_total = map(float, disk_output.strip().split(","))
                metrics["disk_percent"] = disk_percent
                metrics["disk_free_gb"] = disk_free
                metrics["disk_total_gb"] = disk_total
            except:
                metrics["disk_percent"] = 0
                metrics["disk_free_gb"] = 0
                metrics["disk_total_gb"] = 0

            # Process metrics
            metrics["active_tasks"] = len(self.active_processes)
            metrics["max_tasks"] = self.max_concurrent_tasks

        except Exception as e:
            logger.debug(f"Failed to get remote metrics: {e}")

        return metrics
