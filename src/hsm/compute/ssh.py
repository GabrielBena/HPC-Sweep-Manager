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

from ..core.result_collector import RemoteResultCollector
from ..core.sync_manager import ProjectStateChecker
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

        # Advanced sync and result collection
        self.project_state_checker: Optional[ProjectStateChecker] = None
        self.remote_result_collector: Optional[RemoteResultCollector] = None

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

            # Discover remote project configuration
            if not await self._discover_remote_project():
                return False

            # Initialize project state checker for sync verification
            from pathlib import Path

            local_project_dir = Path.cwd()
            self.project_state_checker = ProjectStateChecker(
                local_project_dir=str(local_project_dir),
                ssh_config=self.ssh_config,
            )

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

            # Initialize remote result collector for comprehensive collection
            self.remote_result_collector = RemoteResultCollector(
                local_sweep_dir=self.local_sweep_dir,
                remote_host=self.ssh_config.host,
                ssh_key=self.ssh_config.key_file,
                ssh_port=self.ssh_config.port,
            )

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
                message="Compute source not properly set up",
            )

        try:
            logger.debug(f"Starting task submission {task.task_id} to SSH source {self.name}")

            # Create remote task directory
            remote_task_dir = f"{self.remote_sweep_dir}/tasks/{task.task_id}"
            logger.debug(f"Creating remote task directory: {remote_task_dir}")
            await self._execute_command(f"mkdir -p {remote_task_dir}")

            # Write task parameters
            logger.debug(f"Writing task parameters for {task.task_id}")
            params_content = self._create_params_file(task.params)
            await self._write_remote_file(f"{remote_task_dir}/params.yaml", params_content)

            # Start task execution
            logger.debug(f"Starting task execution future for {task.task_id}")
            future = asyncio.create_task(self._execute_task(task))
            self.task_futures[task.task_id] = future

            # Register task
            logger.debug(f"Registering task {task.task_id}")
            self.register_task(task)

            logger.info(f"Task {task.task_id} successfully submitted to SSH source {self.name}")

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
                message=str(e),
            )

    async def get_task_status(self, task_id: str) -> TaskResult:
        """Get current status of a task.

        Args:
            task_id: ID of the task

        Returns:
            TaskResult with current status and metadata
        """
        try:
            # Check if task is in our active tasks first
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                # Get current status from task or default to RUNNING if active
                status = getattr(task, "status", TaskStatus.RUNNING)
                return TaskResult(
                    task_id=task_id,
                    status=status,
                    timestamp=datetime.now(),
                )

            # If not available, ensure connection and check remote status
            if not await self._check_connection():
                return TaskResult(
                    task_id=task_id,
                    status=TaskStatus.UNKNOWN,
                    message="SSH connection not available",
                )

            # Check remote status file
            remote_status_file = f"{self.remote_sweep_dir}/tasks/{task_id}/status.yaml"
            status_content = await self._read_remote_file(remote_status_file)

            if status_content:
                import yaml

                status_data = yaml.safe_load(status_content)
                task_status = TaskStatus(status_data.get("status", "PENDING"))

                # Update our local registry
                self.update_task_status(task_id, task_status)

                # Check for failure details
                error_message = status_data.get("error_message")
                exit_code = status_data.get("exit_code")

                return TaskResult(
                    task_id=task_id,
                    status=task_status,
                    message=error_message,
                    timestamp=datetime.now()
                    if task_status
                    in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]
                    else datetime.now(),
                )
            else:
                # No status file found, task might be starting or failed to start
                logger.debug(f"No status file found for task {task_id} on remote")
                return TaskResult(
                    task_id=task_id,
                    status=TaskStatus.PENDING,
                    timestamp=datetime.now(),
                )

        except Exception as e:
            logger.error(f"Error reading remote status for {task_id}: {e}")
            return TaskResult(
                task_id=task_id,
                status=TaskStatus.UNKNOWN,
                message=f"Error checking status: {str(e)}",
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

            # Update task status (this will automatically update stats)
            self.update_task_status(task_id, TaskStatus.CANCELLED)

            # Write remote status
            await self._write_remote_task_status(task_id, TaskStatus.CANCELLED)

            return True

        except Exception as e:
            logger.error(f"Error cancelling task {task_id}: {e}")
            return False

    async def collect_results(self, task_ids: List[str]) -> CollectionResult:
        """Collect results from completed tasks with enhanced error collection.

        Args:
            task_ids: List of task IDs to collect results for

        Returns:
            CollectionResult with collection status
        """
        # Try comprehensive result collection first if available
        if self.remote_result_collector and self.remote_sweep_dir:
            try:
                logger.debug(f"Using comprehensive result collection for {len(task_ids)} tasks")

                # Use the comprehensive result collector
                success = await self.remote_result_collector.collect_results(
                    remote_sweep_dir=self.remote_sweep_dir,
                    job_ids=task_ids,
                    cleanup_after_sync=False,  # Don't cleanup during sweep execution
                )

                if success:
                    return CollectionResult(
                        collected_tasks=task_ids,
                        failed_tasks=[],
                        errors=[],
                    )
                else:
                    logger.warning(
                        "Comprehensive result collection failed, falling back to basic sync"
                    )

            except Exception as e:
                logger.warning(
                    f"Error in comprehensive result collection: {e}, falling back to basic sync"
                )

        # Fallback to basic sync method
        logger.debug(f"Using basic result collection for {len(task_ids)} tasks")
        collected_tasks = []
        failed_tasks = []

        for task_id in task_ids:
            try:
                # Check if task is completed
                status_result = await self.get_task_status(task_id)
                if status_result.status not in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
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
            # Check SSH connection
            if not await self._check_connection():
                return HealthReport(
                    source_name=self.name,
                    status=HealthStatus.UNHEALTHY,
                    timestamp=datetime.now(),
                    message="SSH connection failed",
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

            # Update stats (note: active_tasks is a read-only property)
            self.stats.health_status = status
            self.stats.last_health_check = datetime.now()

            # Calculate current active tasks
            current_active_tasks = len(self.active_processes)

            return HealthReport(
                source_name=self.name,
                status=status,
                timestamp=datetime.now(),
                available_slots=self.max_concurrent_tasks - current_active_tasks,
                max_slots=self.max_concurrent_tasks,
                active_tasks=current_active_tasks,
                cpu_usage=metrics.get("cpu_percent"),
                memory_usage=metrics.get("memory_percent"),
                disk_free_gb=metrics.get("disk_free_gb"),
                load_average=metrics.get("load_average"),
                message=f"SSH compute source running {current_active_tasks}/{self.max_concurrent_tasks} tasks",
                warnings=warnings,
            )

        except Exception as e:
            logger.error(f"Health check failed for {self.name}: {e}")
            return HealthReport(
                source_name=self.name,
                status=HealthStatus.UNHEALTHY,
                timestamp=datetime.now(),
                message=f"Health check failed: {str(e)}",
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
        """Detect training script path if not already discovered."""
        # Script path should have been discovered in project discovery phase
        if self.script_path:
            return self.script_path

        # Fallback: search for common script names in project directory
        project_dir = self.ssh_config.project_dir or "~"
        common_names = ["train.py", "main.py", "run.py", "training.py"]

        logger.info("Training script not found in config, searching for common script names...")
        for name in common_names:
            script_path = f"{project_dir}/{name}"
            try:
                await self._execute_command(f"test -f {script_path}")
                logger.info(f"Auto-detected remote script: {script_path}")
                return script_path
            except:
                continue

        logger.warning("Could not detect remote training script")
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
                # Note: We can't easily track completed tasks in SSH source
                # since they're moved out of active_tasks. For now, skip this automatic sync
                # and rely on the SweepEngine's result collection instead.
                completed_tasks = []

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
                self.update_task_status(task.task_id, TaskStatus.RUNNING)
                await self._write_remote_task_status(
                    task.task_id, TaskStatus.RUNNING, start_time=datetime.now()
                )

                # Build command
                try:
                    cmd = await self._build_remote_command(task, remote_task_dir)
                    logger.debug(f"Built remote command for task {task.task_id}: {cmd}")
                except Exception as e:
                    logger.error(f"Failed to build remote command for task {task.task_id}: {e}")
                    raise

                # Start remote process
                logger.debug(f"Starting remote task {task.task_id}: {cmd}")

                try:
                    process = await self.connection.create_process(
                        cmd,
                        stdout=asyncssh.PIPE,
                        stderr=asyncssh.PIPE,
                    )
                except Exception as e:
                    logger.error(f"Failed to create remote process for task {task.task_id}: {e}")
                    raise

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
                    else:
                        final_status = TaskStatus.FAILED

                    # Update status (this will automatically update stats)
                    self.update_task_status(task.task_id, final_status)
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

                    self.update_task_status(task.task_id, TaskStatus.FAILED)
                    await self._write_remote_task_status(
                        task.task_id,
                        TaskStatus.FAILED,
                        complete_time=datetime.now(),
                        error_message="Task timed out",
                    )

            except Exception as e:
                # Task execution failed
                logger.error(f"Remote task {task.task_id} execution failed: {e}")
                self.update_task_status(task.task_id, TaskStatus.FAILED)
                await self._write_remote_task_status(
                    task.task_id,
                    TaskStatus.FAILED,
                    complete_time=datetime.now(),
                    error_message=str(e),
                )

            finally:
                # Cleanup
                if task.task_id in self.active_processes:
                    del self.active_processes[task.task_id]
                if task.task_id in self.task_futures:
                    del self.task_futures[task.task_id]

    async def _build_remote_command(self, task: Task, remote_task_dir: str) -> str:
        """Build the command to execute the task remotely."""
        cmd_parts = []

        # Change to project directory first (if specified)
        if self.ssh_config.project_dir and self.ssh_config.project_dir != "~":
            cmd_parts.append(f"cd {self.ssh_config.project_dir}")

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

        # Add task parameters as Hydra-style arguments (key=value)
        for key, value in task.params.items():
            if isinstance(value, bool):
                python_cmd.append(f"{key}={str(value).lower()}")
            else:
                python_cmd.append(f"{key}={str(value)}")

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

            # Load average
            try:
                load_cmd = 'python3 -c "import os; print(os.getloadavg()[0])"'
                load_output = await self._execute_command(load_cmd, check=False)
                metrics["load_average"] = float(load_output.strip())
            except:
                metrics["load_average"] = None

            # Process metrics
            metrics["active_tasks"] = len(self.active_processes)
            metrics["max_tasks"] = self.max_concurrent_tasks

        except Exception as e:
            logger.debug(f"Failed to get remote metrics: {e}")

        return metrics

    async def verify_project_sync(self) -> Dict[str, Any]:
        """Verify that local and remote projects are in sync.

        Returns:
            Dict with sync status information
        """
        if not self.project_state_checker:
            return {
                "in_sync": False,
                "message": "Project state checker not initialized",
                "error": True,
            }

        try:
            sync_status = await self.project_state_checker.check_project_state()
            return sync_status
        except Exception as e:
            logger.error(f"Error checking project sync: {e}")
            return {
                "in_sync": False,
                "message": f"Error checking sync status: {str(e)}",
                "error": True,
            }

    async def _discover_remote_project(self) -> bool:
        """Discover remote project configuration by searching for hsm_config.yaml."""
        try:
            logger.info("Discovering remote project configuration...")

            # Step 1: Find hsm_config.yaml on remote machine
            config_path = await self._find_remote_hsm_config()
            if not config_path:
                logger.warning("No hsm_config.yaml found on remote machine")
                return False

            # Step 2: Read and parse remote HSM config
            remote_config = await self._read_remote_hsm_config(config_path)
            if not remote_config:
                logger.error("Failed to read remote hsm_config.yaml")
                return False

            # Step 3: Extract and update paths from remote config
            self._extract_remote_paths(remote_config)

            # Step 4: Validate discovered paths
            if not await self._validate_discovered_paths():
                logger.error("Remote project validation failed")
                return False

            logger.info("✓ Remote project configuration discovered successfully")
            return True

        except Exception as e:
            logger.error(f"Error during remote project discovery: {e}")
            return False

    async def _find_remote_hsm_config(self) -> Optional[str]:
        """Find the correct hsm_config.yaml on the remote machine with project matching."""
        # Get local project information for matching
        local_script_name = None
        local_config_file = Path.cwd() / "hsm_config.yaml"
        if local_config_file.exists():
            try:
                import yaml

                with open(local_config_file) as f:
                    local_config = yaml.safe_load(f)
                local_script_path = local_config.get("paths", {}).get("training_script", "")
                if local_script_path:
                    local_script_name = Path(local_script_path).name
                    logger.debug(f"Local training script name: {local_script_name}")
            except Exception as e:
                logger.debug(f"Could not read local config: {e}")

        # Search paths in order of preference
        search_paths = [
            "sweeps/hsm_config.yaml",
            "hsm_config.yaml",
            "./sweeps/hsm_config.yaml",
            "./hsm_config.yaml",
        ]

        # If project_dir is specified, also search there first
        if self.ssh_config.project_dir and self.ssh_config.project_dir != "~":
            project_paths = [
                f"{self.ssh_config.project_dir}/hsm_config.yaml",
                f"{self.ssh_config.project_dir}/sweeps/hsm_config.yaml",
            ]
            search_paths = project_paths + search_paths

        # Try each search path with validation
        for path in search_paths:
            try:
                result = await self._execute_command(f'test -f {path} && echo "found"', check=False)
                if result.strip() == "found":
                    # Validate this is the correct project
                    if await self._validate_project_match(path, local_script_name):
                        logger.info(f"Found matching project config at: {path}")
                        return path
                    else:
                        logger.debug(f"Config at {path} doesn't match current project")
            except Exception as e:
                logger.debug(f"Error checking path {path}: {e}")
                continue

        # Find all hsm_config.yaml files and validate each one
        try:
            logger.debug("Searching all hsm_config.yaml files for project match...")
            result = await self._execute_command(
                'find . -name "hsm_config.yaml" -type f 2>/dev/null', check=False
            )

            if result.strip():
                config_files = result.strip().split("\n")
                logger.debug(f"Found {len(config_files)} hsm_config.yaml files to check")

                for config_file in config_files:
                    config_file = config_file.strip()
                    if config_file and await self._validate_project_match(
                        config_file, local_script_name
                    ):
                        logger.info(f"Found matching project config at: {config_file}")
                        return config_file

                logger.warning(
                    f"Found {len(config_files)} hsm_config.yaml files but none match the current project"
                )

        except Exception as e:
            logger.debug(f"Error during comprehensive search: {e}")

        return None

    async def _validate_project_match(
        self, config_path: str, local_script_name: Optional[str] = None
    ) -> bool:
        """Validate that a remote config file matches the current project."""
        try:
            # Read the remote config
            config_content = await self._read_remote_file(config_path)
            if not config_content:
                return False

            import yaml

            remote_config = yaml.safe_load(config_content)
            paths = remote_config.get("paths", {})

            # Get remote training script path
            remote_script_path = paths.get("training_script") or paths.get("train_script")
            if not remote_script_path:
                logger.debug(f"No training script found in {config_path}")
                return False

            # If we have a local script name, check if it matches
            if local_script_name:
                remote_script_name = Path(remote_script_path).name
                if remote_script_name == local_script_name:
                    logger.debug(f"Script name match: {local_script_name} == {remote_script_name}")

                    # Additional validation: check if the script file actually exists
                    try:
                        await self._execute_command(f"test -f {remote_script_path}")
                        logger.debug(f"Validated script exists: {remote_script_path}")
                        return True
                    except:
                        logger.debug(f"Script file doesn't exist: {remote_script_path}")
                        return False
                else:
                    logger.debug(
                        f"Script name mismatch: {local_script_name} != {remote_script_name}"
                    )
                    return False
            else:
                # No local script name to match against, just check if script exists
                try:
                    await self._execute_command(f"test -f {remote_script_path}")
                    logger.debug(f"Script exists: {remote_script_path}")
                    return True
                except:
                    logger.debug(f"Script doesn't exist: {remote_script_path}")
                    return False

        except Exception as e:
            logger.debug(f"Error validating project match for {config_path}: {e}")
            return False

    async def _read_remote_hsm_config(self, config_path: str) -> Optional[Dict[str, Any]]:
        """Read and parse remote hsm_config.yaml."""
        try:
            config_content = await self._read_remote_file(config_path)
            if not config_content:
                return None

            import yaml

            remote_config = yaml.safe_load(config_content)
            logger.debug(f"Successfully parsed remote hsm_config.yaml from {config_path}")
            return remote_config

        except Exception as e:
            logger.error(f"Failed to read remote hsm_config.yaml at {config_path}: {e}")
            return None

    def _extract_remote_paths(self, remote_config: Dict[str, Any]) -> None:
        """Extract and update paths from remote HSM configuration."""
        paths = remote_config.get("paths", {})

        # Update Python interpreter if not set
        if not self.ssh_config.python_path:
            remote_python = paths.get("python_interpreter")
            if remote_python:
                self.ssh_config.python_path = remote_python
                logger.debug(f"Using remote Python interpreter: {remote_python}")

        # Update conda environment if not set
        if not self.ssh_config.conda_env:
            remote_conda = paths.get("conda_env")
            if remote_conda:
                self.ssh_config.conda_env = remote_conda
                logger.debug(f"Using remote conda environment: {remote_conda}")

        # Update project directory - this is crucial for correct path resolution
        remote_project_root = paths.get("project_root")
        if remote_project_root:
            # Always use the remote project root since it's the authoritative source
            self.ssh_config.project_dir = remote_project_root
            logger.info(f"Using remote project directory: {remote_project_root}")

        # Update training script path if not set
        if not self.script_path:
            remote_script = paths.get("training_script")
            if remote_script:
                self.script_path = remote_script
                logger.debug(f"Using remote training script: {remote_script}")

    async def _validate_discovered_paths(self) -> bool:
        """Validate that discovered remote paths actually exist and work."""
        validation_tasks = []

        # Check Python interpreter
        if self.ssh_config.python_path:
            validation_tasks.append(
                self._validate_remote_command(
                    f"{self.ssh_config.python_path} --version", "Python interpreter"
                )
            )

        # Check project directory
        if self.ssh_config.project_dir:
            validation_tasks.append(
                self._validate_remote_path(self.ssh_config.project_dir, "Project root")
            )

        # Check training script if specified
        if self.script_path:
            script_path = self.script_path
            # If relative path, make it relative to project root
            if not script_path.startswith("/") and self.ssh_config.project_dir:
                script_path = f"{self.ssh_config.project_dir}/{script_path}"

            validation_tasks.append(self._validate_remote_path(script_path, "Training script"))

        # Check conda environment if specified
        if self.ssh_config.conda_env:
            validation_tasks.append(
                self._validate_remote_command(
                    f"conda env list | grep {self.ssh_config.conda_env}", "Conda environment"
                )
            )

        # Run all validations
        if validation_tasks:
            results = await asyncio.gather(*validation_tasks, return_exceptions=True)
            success_count = sum(1 for result in results if result is True)
            total_checks = len(validation_tasks)

            logger.debug(f"Path validation: {success_count}/{total_checks} checks passed")
            return success_count == total_checks

        return True

    async def _validate_remote_path(self, path: str, description: str) -> bool:
        """Validate that a path exists on the remote machine."""
        try:
            result = await self._execute_command(f"test -e {path}", check=False)
            if result == "":  # Command succeeded (no output expected)
                logger.debug(f"✓ {description}: {path}")
                return True
            else:
                logger.warning(f"✗ {description}: {path} (not found)")
                return False
        except Exception as e:
            logger.warning(f"✗ {description}: {path} (error: {e})")
            return False

    async def _validate_remote_command(self, command: str, description: str) -> bool:
        """Validate that a command can be executed successfully on remote."""
        try:
            result = await self._execute_command(command, check=False)
            logger.debug(f"✓ {description}: {command}")
            return True
        except Exception as e:
            logger.warning(f"✗ {description}: {command} (error: {e})")
            return False
