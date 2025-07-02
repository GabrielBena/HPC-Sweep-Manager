"""Remote job manager for executing jobs on remote machines via SSH."""

import asyncio
from datetime import datetime
import hashlib
import logging
from pathlib import Path
import signal
import sys
import tempfile
import threading
from typing import Any, Dict, List, Optional

try:
    import asyncssh

    ASYNCSSH_AVAILABLE = True
except ImportError:
    ASYNCSSH_AVAILABLE = False

from ..common.base_manager import BaseJobManager
from ..common.progress import ProgressBarManager
from ..common.templating import render_template
from .discovery import RemoteConfig, create_ssh_connection, get_ssh_client_keys
from .project_sync import ProjectStateChecker

logger = logging.getLogger(__name__)


class RemoteJobManager(BaseJobManager):
    """Manages jobs on remote machines via SSH."""

    def __init__(
        self,
        remote_config: RemoteConfig,
        local_sweep_dir: Path,
        max_parallel_jobs: int = 4,
        show_progress: bool = True,
        setup_signal_handlers: bool = True,
    ):
        """
        Initialize remote job manager.

        Args:
            remote_config: Configuration for the remote machine
            local_sweep_dir: Local sweep directory for result collection
            max_parallel_jobs: Maximum number of concurrent jobs on remote
            show_progress: Whether to show progress updates
            setup_signal_handlers: Whether to set up signal handlers for graceful shutdown
        """
        super().__init__(max_parallel_jobs, show_progress)
        self.remote_config = remote_config
        self.local_sweep_dir = local_sweep_dir
        self.system_type = "remote"

        # Remote directories
        self.remote_sweep_dir = None
        self.remote_tasks_dir = None

        # Signal handling state
        self._signal_received = False
        self._cleanup_in_progress = False

        # Rich progress tracking
        self.progress_bar = ProgressBarManager(show_progress=self.show_progress)
        self._console = self.progress_bar.console

        if not ASYNCSSH_AVAILABLE:
            raise ImportError("asyncssh is required for remote job execution")

        if setup_signal_handlers:
            # Setup signal handlers for graceful shutdown
            self._setup_signal_handlers()
            logger.debug(
                f"Signal handlers registered for remote job manager on {self.remote_config.name}"
            )

    def _setup_signal_handlers(self):
        """Setup signal handlers to catch interrupts and cleanup remote jobs."""
        # Register signal handlers for Ctrl+C and termination
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Also register cleanup on normal program exit
        import atexit

        atexit.register(self._cleanup_on_exit)

    def _signal_handler(self, signum, frame):
        """Handle signals for graceful shutdown and remote job cancellation."""
        if self._signal_received:
            # If we already received a signal and cleanup is in progress,
            # force immediate exit on second signal
            logger.warning(f"Received signal {signum} again, forcing immediate exit...")
            sys.exit(1)

        self._signal_received = True
        logger.info(
            f"🛑 Signal {signum} received - propagating to remote jobs on {self.remote_config.name}..."
        )
        logger.info(f"📡 Cancelling {len(self.running_jobs)} remote jobs and cleaning up...")

        # Start cleanup in a background thread since signal handlers can't be async
        cleanup_thread = threading.Thread(target=self._threaded_cleanup)
        cleanup_thread.daemon = True
        cleanup_thread.start()

        # Give cleanup thread time to work
        cleanup_thread.join(timeout=30)  # Wait up to 30 seconds for cleanup

        if cleanup_thread.is_alive():
            logger.warning("Cleanup thread still running after timeout, forcing exit...")
            sys.exit(1)
        else:
            logger.info("✅ Remote job cleanup completed successfully")
            sys.exit(0)

    def _threaded_cleanup(self):
        """Run async cleanup in a thread (called from signal handler)."""
        try:
            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Run the async cleanup with overall timeout
            try:
                loop.run_until_complete(
                    asyncio.wait_for(self._async_cleanup_on_signal(), timeout=25.0)
                )
                logger.info("Signal cleanup completed successfully")
            except asyncio.TimeoutError:
                logger.warning("Signal cleanup timed out after 25 seconds")
            except Exception as e:
                logger.error(f"Error during async signal cleanup: {e}")

        except Exception as e:
            logger.error(f"Error in threaded cleanup: {e}")
        finally:
            try:
                # Close the loop properly
                if loop and not loop.is_closed():
                    loop.close()
            except Exception:
                pass  # Ignore errors when closing loop

    async def _async_cleanup_on_signal(self):
        """Async cleanup method called when signal is received."""
        if self._cleanup_in_progress:
            logger.info("Cleanup already in progress, skipping duplicate cleanup")
            return

        self._cleanup_in_progress = True

        try:
            if self.running_jobs:
                logger.info(f"Starting cancellation of {len(self.running_jobs)} remote jobs...")

                # Cancel all running jobs using the dedicated method
                try:
                    results = await self.cancel_all_jobs(timeout=20)
                    logger.info(f"Job cancellation results: {results}")
                except Exception as e:
                    logger.error(f"Error during job cancellation: {e}")

                # Clean up remote environment
                if self.remote_sweep_dir:
                    logger.info("Cleaning up remote sweep directory...")
                    try:
                        await asyncio.wait_for(self.cleanup_remote_environment(), timeout=4.0)
                        logger.info("Remote environment cleanup completed")
                    except asyncio.TimeoutError:
                        logger.warning("Remote environment cleanup timed out")
                    except Exception as e:
                        logger.warning(f"Remote environment cleanup failed: {e}")
            else:
                logger.info("No running jobs to clean up")

        except Exception as e:
            logger.error(f"Error during async cleanup: {e}")
        finally:
            self._cleanup_in_progress = False

    def _cleanup_on_exit(self):
        """Clean up resources on program exit (called by atexit)."""
        if not self._signal_received and self.running_jobs:
            logger.info("Program exiting, cleaning up remote jobs...")
            # For atexit, we can only do synchronous cleanup
            # The user should use Ctrl+C for proper async cleanup
            logger.warning("Use Ctrl+C for proper async remote job cleanup")

    async def setup_remote_environment(
        self,
        sync_excludes: List[str] = None,
        verify_sync: bool = True,
        auto_sync: bool = False,
        interactive: bool = True,
    ) -> bool:
        """
        Setup remote environment for job execution.

        Args:
            sync_excludes: List of patterns to exclude from sync
            verify_sync: Whether to verify local and remote project synchronization
            auto_sync: Whether to automatically sync mismatched files without prompting
            interactive: Whether to prompt user for sync when mismatches are found

        Returns:
            True if setup successful, False otherwise
        """
        logger.info(f"Setting up remote environment on {self.remote_config.name}")

        try:
            async with await create_ssh_connection(
                self.remote_config.host,
                self.remote_config.ssh_key,
                self.remote_config.ssh_port,
            ) as conn:
                # 1. Validate that the discovered project paths still exist
                validation_success = await self._validate_existing_project(conn)
                if not validation_success:
                    logger.error("Remote project validation failed")
                    return False

                # 2. Verify project synchronization if requested
                sync_performed = False
                if verify_sync:
                    sync_checker = ProjectStateChecker(str(Path.cwd()), self.remote_config)

                    # For distributed mode, we MUST enforce strict sync
                    if hasattr(self, "is_distributed_mode") and self.is_distributed_mode:
                        logger.info("🔒 Distributed mode: enforcing strict project synchronization")
                        logger.info("   Config file mismatches will prevent execution")

                        # Use the new strict sync enforcement
                        sync_result = await sync_checker.verify_and_enforce_sync(
                            conn, auto_sync=auto_sync, interactive=interactive and not auto_sync
                        )

                        if not sync_result["in_sync"]:
                            logger.error("❌ CRITICAL: Project sync enforcement failed!")
                            logger.error(
                                "   Distributed execution requires identical configs across all sources"
                            )
                            logger.error("   Cannot proceed with mismatched project state")

                            if sync_result["errors"]:
                                for error in sync_result["errors"]:
                                    logger.error(f"   • {error}")

                            return False

                        if sync_result["sync_performed"]:
                            logger.info(
                                f"✅ Successfully synced {len(sync_result['files_synced'])} files to remote"
                            )
                            sync_performed = True

                        logger.info(
                            "✅ Strict project synchronization verified for distributed execution"
                        )

                    else:
                        # For standalone remote mode, use the legacy method but still be strict
                        logger.info("🔍 Standalone remote mode: verifying project synchronization")
                        sync_result = await sync_checker.verify_project_sync(conn)

                        if not sync_result["in_sync"]:
                            logger.warning(
                                "Project synchronization verification detected differences."
                            )

                            # Show detailed error information
                            self._show_sync_error_details(sync_result)

                            # For standalone mode, we can still be lenient if user chooses
                            if auto_sync:
                                logger.info("Auto-sync enabled, attempting to sync...")
                                sync_result = await sync_checker.verify_and_enforce_sync(
                                    conn, auto_sync=True, interactive=False
                                )

                                if sync_result["in_sync"]:
                                    logger.info("✅ Auto-sync completed successfully")
                                    sync_performed = True
                                else:
                                    logger.error("❌ Auto-sync failed")
                                    return False
                            elif interactive:
                                logger.info("Interactive mode: offering sync options...")
                                sync_result = await sync_checker.verify_and_enforce_sync(
                                    conn, auto_sync=False, interactive=True
                                )

                                if sync_result["in_sync"]:
                                    logger.info("✅ Interactive sync completed successfully")
                                    sync_performed = True
                                else:
                                    logger.error("❌ User declined sync or sync failed")
                                    return False
                            else:
                                logger.error("❌ Sync required but not in auto or interactive mode")
                                return False
                        else:
                            logger.info(
                                "✓ Project synchronization verified for standalone remote execution"
                            )
                else:
                    logger.info("✓ Project synchronization verification skipped")

                # 3. Create remote sweep directory within outputs/ subdirectory (consistent with local)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                sweep_dir_name = f"remote_sweep_{timestamp}"  # Flag as remote
                outputs_dir = f"{self.remote_config.project_root}/outputs"

                # Ensure outputs directory exists
                await conn.run(f"mkdir -p {outputs_dir}")

                self.remote_sweep_dir = f"{outputs_dir}/{sweep_dir_name}"

                await conn.run(f"mkdir -p {self.remote_sweep_dir}")
                logger.info(f"Created remote sweep directory: {self.remote_sweep_dir}")

                # 4. Create subdirectories for sweep management (not project code)
                self.remote_tasks_dir = f"{self.remote_sweep_dir}/tasks"
                await conn.run(f"mkdir -p {self.remote_tasks_dir}")
                await conn.run(f"mkdir -p {self.remote_sweep_dir}/logs")
                await conn.run(f"mkdir -p {self.remote_sweep_dir}/scripts")

                logger.info(f"Remote environment setup complete on {self.remote_config.name}")
                logger.info(f"Using existing project at: {self.remote_config.project_root}")
                if sync_performed:
                    logger.info("Local changes have been synced to remote")
                logger.info(f"Sweep artifacts will be stored in: {self.remote_sweep_dir}")
                return True

        except Exception as e:
            logger.error(f"Failed to setup remote environment on {self.remote_config.name}: {e}")
            return False

    async def _validate_existing_project(self, conn) -> bool:
        """Validate that the discovered project paths are still accessible."""
        logger.info("Validating existing remote project paths...")

        validations = []

        # Check project root
        if self.remote_config.project_root:
            validations.append(
                self._check_path(conn, self.remote_config.project_root, "Project root")
            )

        # Check train script
        if self.remote_config.train_script:
            # Handle absolute vs relative paths
            if self.remote_config.train_script.startswith("/"):
                script_path = self.remote_config.train_script
            else:
                script_path = f"{self.remote_config.project_root}/{self.remote_config.train_script}"

            validations.append(self._check_path(conn, script_path, "Training script"))

        # Check Python interpreter
        if self.remote_config.python_interpreter:
            validations.append(
                self._check_command(
                    conn,
                    f"{self.remote_config.python_interpreter} --version",
                    "Python interpreter",
                )
            )

        # Run all validations
        results = await asyncio.gather(*validations, return_exceptions=True)
        success_count = sum(1 for result in results if result is True)
        total_checks = len(validations)

        logger.info(f"Project validation: {success_count}/{total_checks} checks passed")
        return success_count == total_checks

    async def _check_path(self, conn, path: str, description: str) -> bool:
        """Check if a path exists on the remote machine."""
        try:
            result = await conn.run(f"test -e {path}", check=False)
            if result.returncode == 0:
                logger.debug(f"✓ {description}: {path}")
                return True
            else:
                logger.warning(f"✗ {description}: {path} (not found)")
                return False
        except Exception as e:
            logger.warning(f"✗ {description}: {path} (error: {e})")
            return False

    async def _check_command(self, conn, command: str, description: str) -> bool:
        """Check if a command can be executed successfully."""
        try:
            result = await conn.run(command, check=False)
            if result.returncode == 0:
                logger.debug(f"✓ {description}: {command}")
                return True
            else:
                logger.warning(f"✗ {description}: {command} (exit code: {result.returncode})")
                return False
        except Exception as e:
            logger.warning(f"✗ {description}: {command} (error: {e})")
            return False

    async def _should_sync_project(self) -> bool:
        """Determine if we need to sync the project to remote."""
        # Since we use discovery to find existing projects, we should NOT sync by default
        # Only sync if explicitly requested or if project validation fails
        logger.info("Project sync disabled - using existing remote project")
        return False

    async def submit_single_job(
        self,
        params: Dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: Optional[str] = None,
    ) -> str:
        """Submit a single job on the remote machine."""
        self.job_counter += 1
        job_id = f"remote_{self.remote_config.name}_{self.job_counter}"

        logger.debug(f"Submitting job {job_name} to remote {self.remote_config.name}")

        try:
            # Extract task number from job name (e.g., "sweep_20250625_210408_task_002" -> "task_002")
            import re

            task_match = re.search(r"task_(\d+)", job_name)
            if task_match:
                task_number = task_match.group(1)
                task_name = f"task_{task_number}"
            else:
                # Fallback to job counter if pattern not found
                task_name = f"task_{self.job_counter:03d}"
                logger.warning(
                    f"Could not extract task number from job name {job_name}, using fallback: {task_name}"
                )

            async with await create_ssh_connection(
                self.remote_config.host,
                self.remote_config.ssh_key,
                self.remote_config.ssh_port,
            ) as conn:
                # Create task directory using the extracted task name
                task_dir = f"{self.remote_tasks_dir}/{task_name}"

                # Ensure the task directory is created and accessible
                logger.debug(f"Creating task directory: {task_dir}")
                mkdir_result = await conn.run(f"mkdir -p {task_dir}", check=False)
                if mkdir_result.returncode != 0:
                    raise RuntimeError(
                        f"Failed to create task directory {task_dir}: {mkdir_result.stderr}"
                    )

                # Verify the task directory exists and is writable
                test_result = await conn.run(
                    f"test -d {task_dir} && test -w {task_dir}", check=False
                )
                if test_result.returncode != 0:
                    raise RuntimeError(f"Task directory {task_dir} is not accessible or writable")

                # Ensure scripts and logs directories exist
                scripts_dir = f"{self.remote_sweep_dir}/scripts"
                logs_dir = f"{self.remote_sweep_dir}/logs"

                for directory in [scripts_dir, logs_dir]:
                    mkdir_result = await conn.run(f"mkdir -p {directory}", check=False)
                    if mkdir_result.returncode != 0:
                        logger.warning(
                            f"Could not create directory {directory}: {mkdir_result.stderr}"
                        )

                # Create job script
                script_content = self._create_job_script(
                    params, job_name, task_dir, sweep_id, wandb_group
                )

                script_path = f"{scripts_dir}/{job_name}.sh"

                # Write script to remote (using a temporary file approach)
                with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as tmp:
                    tmp.write(script_content)
                    tmp_path = tmp.name

                try:
                    # Copy script to remote
                    await asyncssh.scp(
                        tmp_path,
                        (self.remote_config.host, script_path),
                        client_keys=get_ssh_client_keys(self.remote_config.ssh_key)
                        if self.remote_config.ssh_key
                        else None,
                    )

                    # Verify the script was copied successfully
                    verify_result = await conn.run(f"test -f {script_path}", check=False)
                    if verify_result.returncode != 0:
                        raise RuntimeError(f"Script was not copied successfully to {script_path}")

                except Exception as e:
                    raise RuntimeError(f"Failed to copy script to remote: {e}")
                finally:
                    # Clean up temporary file
                    Path(tmp_path).unlink()

                # Make script executable
                chmod_result = await conn.run(f"chmod +x {script_path}", check=False)
                if chmod_result.returncode != 0:
                    logger.warning(f"Could not make script executable: {chmod_result.stderr}")

                # Execute the job in background
                log_file = f"{logs_dir}/{job_name}.log"
                err_file = f"{logs_dir}/{job_name}.err"

                # Start job in background with proper PID tracking
                # The script will write its own process group ID to a PID file
                pid_file = f"{task_dir}/job.pid"
                cmd = f"nohup bash {script_path} > {log_file} 2> {err_file} & echo $! > {pid_file}"

                logger.debug(f"Executing command: {cmd}")
                exec_result = await conn.run(cmd, check=False)
                if exec_result.returncode != 0:
                    raise RuntimeError(f"Failed to start job: {exec_result.stderr}")

                # Wait a moment for the PID file to be written
                await asyncio.sleep(1.0)  # Increased wait time

                # Read the PID from the file
                try:
                    pid_result = await conn.run(f"cat {pid_file}")
                    remote_pid = pid_result.stdout.strip()
                    if not remote_pid:
                        logger.warning(f"PID file {pid_file} is empty for job {job_name}")
                        remote_pid = "unknown"
                except Exception as e:
                    logger.warning(f"Could not read PID file for job {job_name}: {e}")
                    remote_pid = "unknown"

                # Store job info
                job_info = {
                    "job_name": job_name,
                    "remote_pid": remote_pid,  # This is the script PID
                    "task_dir": task_dir,
                    "script_path": script_path,
                    "log_file": log_file,
                    "err_file": err_file,
                    "start_time": datetime.now(),
                    "status": "RUNNING",
                    "params": params,
                    "pid_file": pid_file,
                }

                self.running_jobs[job_id] = job_info

                logger.debug(f"Job {job_name} started on remote with PID {remote_pid}")
                return job_id

        except Exception as e:
            logger.error(
                f"Failed to submit job {job_name} to remote {self.remote_config.name}: {e}"
            )
            raise

    def _create_job_script(
        self,
        params: Dict[str, Any],
        job_name: str,
        task_dir: str,
        sweep_id: str,
        wandb_group: Optional[str] = None,
    ) -> str:
        """Create job script content for remote execution."""

        # Convert parameters to command line arguments
        params_str = self._params_to_string(params)

        # Determine effective wandb group
        effective_wandb_group = wandb_group or sweep_id

        script_content = render_template(
            "remote_job.sh.j2",
            job_name=job_name,
            generation_time=datetime.now(),
            remote_name=self.remote_config.name,
            project_root=self.remote_config.project_root,
            task_dir=task_dir,
            python_interpreter=self.remote_config.python_interpreter,
            train_script=self.remote_config.train_script,
            params_str=params_str,
            effective_wandb_group=effective_wandb_group,
        )

        return script_content

    async def get_job_status(self, job_id: str) -> str:
        """Get status of a remote job."""
        if job_id not in self.running_jobs:
            return "UNKNOWN"

        job_info = self.running_jobs[job_id]

        try:
            async with await create_ssh_connection(
                self.remote_config.host,
                self.remote_config.ssh_key,
                self.remote_config.ssh_port,
            ) as conn:
                task_dir = job_info["task_dir"]

                # Get all relevant PIDs
                pids_info = await self._get_job_pids(conn, task_dir)

                # Check if any processes are still running
                any_process_running = False

                for pid_type, pid in pids_info.items():
                    if pid and pid_type in ["script_pid", "python_pid"]:
                        try:
                            result = await conn.run(f"ps -p {pid}", check=False)
                            if result.returncode == 0:
                                any_process_running = True
                                break
                        except Exception:
                            continue

                if any_process_running:
                    # At least one process is still running
                    return "RUNNING"
                else:
                    # All processes have finished, check exit status from task info
                    try:
                        # Use grep to find the most recent Status line, not just the last line
                        result = await conn.run(
                            f"grep 'Status:' {task_dir}/task_info.txt | tail -1",
                            check=False,
                        )

                        if result.returncode == 0 and result.stdout.strip():
                            status_line = result.stdout.strip()
                            logger.debug(f"Job {job_id} status line: {status_line}")

                            if "COMPLETED" in status_line:
                                job_info["status"] = "COMPLETED"
                                return "COMPLETED"
                            elif "FAILED" in status_line:
                                job_info["status"] = "FAILED"
                                return "FAILED"
                            elif "CANCELLED" in status_line:
                                job_info["status"] = "CANCELLED"
                                return "CANCELLED"
                            elif "RUNNING" in status_line:
                                # Process finished but status still shows RUNNING
                                # This might be a race condition - check the actual exit code
                                return await self._check_job_exit_code(conn, task_dir, job_id)
                            else:
                                logger.warning(f"Unexpected status for job {job_id}: {status_line}")
                                return await self._check_job_exit_code(conn, task_dir, job_id)
                        else:
                            # No status line found or command failed
                            logger.warning(
                                f"No status line found for job {job_id}, checking exit code"
                            )
                            return await self._check_job_exit_code(conn, task_dir, job_id)

                    except Exception as e:
                        logger.warning(f"Error reading status file for job {job_id}: {e}")
                        return await self._check_job_exit_code(conn, task_dir, job_id)

        except Exception as e:
            logger.error(f"Error checking job status for {job_id}: {e}")
            return "UNKNOWN"

    async def _check_job_exit_code(self, conn, task_dir: str, job_id: str) -> str:
        """
        Fallback method to determine job status by checking if output files exist
        and trying to determine if the job completed successfully.
        """
        try:
            # Check if the command.txt file exists (it's created at the start)
            cmd_check = await conn.run(f"test -f {task_dir}/command.txt", check=False)
            if cmd_check.returncode != 0:
                logger.warning(f"Command file not found for job {job_id}, assuming FAILED")
                return "FAILED"

            # Check if there are any output files that might indicate completion
            # Look for common output patterns like final_results.csv, checkpoints, etc.
            output_check = await conn.run(
                f"find {task_dir} -name '*.csv' -o -name 'final_results*' -o -name 'outputs' -type d | head -5",
                check=False,
            )

            # Also check the logs for completion indicators
            log_check = await conn.run(
                f"find {task_dir} -name '*.log' | head -1 | xargs tail -20 2>/dev/null | grep -i 'training complete\\|final results\\|sweep complete\\|completed' || true",
                check=False,
            )

            if output_check.returncode == 0 and output_check.stdout.strip():
                logger.info(f"Output files found for job {job_id}, likely COMPLETED")
                return "COMPLETED"
            elif log_check.returncode == 0 and log_check.stdout.strip():
                logger.info(
                    f"Completion indicators found in logs for job {job_id}, likely COMPLETED"
                )
                return "COMPLETED"
            else:
                logger.warning(
                    f"No clear completion indicators for job {job_id}, marking as FAILED"
                )
                return "FAILED"

        except Exception as e:
            logger.error(f"Error in fallback status check for job {job_id}: {e}")
            return "FAILED"

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a remote job using proper process group termination."""
        if job_id not in self.running_jobs:
            logger.warning(f"Job {job_id} not found in running jobs")
            return False

        job_info = self.running_jobs[job_id]
        job_name = job_info["job_name"]
        task_dir = job_info["task_dir"]

        try:
            async with await create_ssh_connection(
                self.remote_config.host,
                self.remote_config.ssh_key,
                self.remote_config.ssh_port,
            ) as conn:
                logger.info(f"Cancelling remote job {job_name} on {self.remote_config.name}")

                # Get all relevant PIDs from the task directory
                pids_info = await self._get_job_pids(conn, task_dir)

                if not pids_info["script_pid"] and not pids_info["python_pid"]:
                    logger.info(f"Job {job_name} appears to be already terminated (no PIDs found)")
                    await self._update_cancelled_job_status(conn, job_info, "ALREADY_TERMINATED")
                    del self.running_jobs[job_id]
                    return True

                # Log the PID information for debugging
                logger.info(
                    f"Job {job_name} PIDs: script={pids_info['script_pid']}, python={pids_info['python_pid']}, pgid={pids_info['process_group']}"
                )

                # Try to terminate the script process (which should handle cancellation gracefully)
                termination_success = await self._terminate_job_gracefully(
                    conn, pids_info, job_name
                )

                if termination_success:
                    logger.info(f"Job {job_name} terminated gracefully")
                    await self._update_cancelled_job_status(conn, job_info, "CANCELLED")
                else:
                    # If graceful termination failed, use force kill
                    logger.info(f"Graceful termination failed, force killing job {job_name}")
                    force_success = await self._force_kill_job(conn, pids_info, job_name)

                    if force_success:
                        logger.info(f"Job {job_name} force-killed successfully")
                        await self._update_cancelled_job_status(conn, job_info, "FORCE_KILLED")
                    else:
                        logger.warning(f"Failed to completely terminate job {job_name}")
                        await self._update_cancelled_job_status(conn, job_info, "KILL_FAILED")

                # Remove the job from running jobs tracking
                del self.running_jobs[job_id]
                return True

        except Exception as e:
            logger.error(f"Failed to cancel remote job {job_id}: {e}")
            # Try to clean up locally even if remote cancellation failed
            if job_id in self.running_jobs:
                self.running_jobs[job_id]["status"] = "CANCEL_FAILED"
            return False

    async def _get_job_pids(self, conn, task_dir: str) -> Dict[str, Optional[str]]:
        """Get all relevant PIDs for a job from the task directory."""
        pids_info = {
            "script_pid": None,
            "python_pid": None,
            "process_group": None,
        }

        try:
            # Try to read script PID
            try:
                result = await conn.run(f"cat {task_dir}/script.pid 2>/dev/null", check=False)
                if result.returncode == 0 and result.stdout.strip():
                    pids_info["script_pid"] = result.stdout.strip()
            except Exception:
                pass

            # Try to read Python PID
            try:
                result = await conn.run(f"cat {task_dir}/python.pid 2>/dev/null", check=False)
                if result.returncode == 0 and result.stdout.strip():
                    pids_info["python_pid"] = result.stdout.strip()
            except Exception:
                pass

            # Try to read process group ID
            try:
                result = await conn.run(
                    f"cat {task_dir}/process_group.pid 2>/dev/null", check=False
                )
                if result.returncode == 0 and result.stdout.strip():
                    pids_info["process_group"] = result.stdout.strip()
            except Exception:
                pass

        except Exception as e:
            logger.warning(f"Error reading PID files from {task_dir}: {e}")

        return pids_info

    async def _verify_all_processes_dead(self, conn, pids_info: Dict[str, Optional[str]]) -> bool:
        """Verify that all job-related processes are actually dead."""
        for pid_type, pid in pids_info.items():
            if pid and pid_type in ["script_pid", "python_pid"]:
                try:
                    result = await conn.run(f"ps -p {pid}", check=False)
                    if result.returncode == 0:
                        # Process is still alive
                        logger.debug(f"Process {pid_type} {pid} is still alive")
                        return False
                except Exception:
                    # If we can't check, assume it's dead
                    continue
        return True

    async def _terminate_job_gracefully(
        self, conn, pids_info: Dict[str, Optional[str]], job_name: str
    ) -> bool:
        """Attempt graceful termination of a job by sending SIGTERM to the script process."""
        script_pid = pids_info.get("script_pid")

        if not script_pid:
            logger.warning(f"No script PID available for job {job_name}")
            return False

        try:
            # Check if the script process is still running
            check_result = await conn.run(f"ps -p {script_pid}", check=False)
            if check_result.returncode != 0:
                logger.info(f"Script process {script_pid} for job {job_name} is already dead")
                return await self._verify_all_processes_dead(conn, pids_info)

            # Send SIGTERM to the script process (which has signal handlers)
            logger.debug(f"Sending SIGTERM to script process {script_pid}")
            await conn.run(f"kill -TERM {script_pid}", check=False)

            # Wait for graceful termination and monitor all processes
            for attempt in range(12):  # Wait up to 6 seconds
                await asyncio.sleep(0.5)

                # Check if all processes are dead
                if await self._verify_all_processes_dead(conn, pids_info):
                    logger.info(f"All processes for job {job_name} terminated gracefully")
                    return True

            logger.warning(f"Some processes for job {job_name} did not terminate gracefully")
            return False

        except Exception as e:
            logger.warning(f"Error during graceful termination of job {job_name}: {e}")
            return False

    async def _force_kill_job(
        self, conn, pids_info: Dict[str, Optional[str]], job_name: str
    ) -> bool:
        """Force kill all processes associated with a job."""
        logger.info(f"Force killing all processes for job {job_name}")

        # Kill individual Python process first (most important)
        python_pid = pids_info.get("python_pid")
        if python_pid:
            try:
                logger.info(f"Force killing Python process {python_pid}")
                await conn.run(f"kill -KILL {python_pid}", check=False)
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"Failed to kill Python process {python_pid}: {e}")

        # Then kill script process
        script_pid = pids_info.get("script_pid")
        if script_pid:
            try:
                logger.debug(f"Force killing script process {script_pid}")
                await conn.run(f"kill -KILL {script_pid}", check=False)
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"Failed to kill script process {script_pid}: {e}")

        # Kill by process group as final cleanup
        process_group = pids_info.get("process_group")
        if process_group:
            try:
                logger.debug(f"Force killing process group {process_group}")
                await conn.run(f"kill -KILL -{process_group}", check=False)
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"Failed to kill process group {process_group}: {e}")

        # Final verification - check if all processes are actually dead
        await asyncio.sleep(1)
        verification_success = await self._verify_all_processes_dead(conn, pids_info)

        if not verification_success:
            logger.error(f"Some processes for job {job_name} are still alive after force kill:")
            for pid_type, pid in pids_info.items():
                if pid and pid_type in ["script_pid", "python_pid"]:
                    try:
                        check_result = await conn.run(f"ps -p {pid}", check=False)
                        if check_result.returncode == 0:
                            logger.error(f"  {pid_type} {pid} is still alive")
                    except Exception:
                        pass
        else:
            logger.info(f"All processes for job {job_name} successfully killed")

        return verification_success

    async def _update_cancelled_job_status(self, conn, job_info: dict, status: str):
        """Update the status of a cancelled job on the remote machine."""
        task_dir = job_info.get("task_dir")
        if not task_dir:
            return

        try:
            # Update task status with more informative messages
            status_messages = {
                "CANCELLED": "CANCELLED",
                "FORCE_KILLED": "CANCELLED (force killed)",
                "ALREADY_TERMINATED": "CANCELLED (already terminated)",
                "KILL_FAILED": "CANCELLED (kill failed)",
            }

            status_msg = status_messages.get(status, status)

            await conn.run(f"echo 'Status: {status_msg}' >> {task_dir}/task_info.txt", check=False)
            await conn.run(f"echo 'End Time: $(date)' >> {task_dir}/task_info.txt", check=False)

            # Update local job info
            job_info["status"] = status

        except Exception as e:
            logger.warning(f"Failed to update task status for cancelled job: {e}")

    async def cancel_all_jobs(self, timeout: int = 30) -> Dict[str, int]:
        """
        Cancel all running remote jobs.

        Args:
            timeout: Maximum time to wait for all cancellations to complete

        Returns:
            Dict with cancellation results: {'cancelled': N, 'failed': N}
        """
        if not self.running_jobs:
            logger.info("No running jobs to cancel")
            return {"cancelled": 0, "failed": 0}

        logger.info(
            f"Cancelling {len(self.running_jobs)} running remote jobs on {self.remote_config.name}..."
        )

        results = {"cancelled": 0, "failed": 0}

        # Get a copy of job IDs since we'll be modifying the dict
        job_ids = list(self.running_jobs.keys())

        # Create cancellation tasks
        cancel_tasks = []
        for job_id in job_ids:
            cancel_tasks.append(self._cancel_job_with_result(job_id))

        if cancel_tasks:
            try:
                # Wait for all cancellations with timeout
                task_results = await asyncio.wait_for(
                    asyncio.gather(*cancel_tasks, return_exceptions=True),
                    timeout=timeout,
                )

                # Count results
                for result in task_results:
                    if isinstance(result, Exception):
                        results["failed"] += 1
                        logger.warning(f"Job cancellation failed: {result}")
                    elif result:
                        results["cancelled"] += 1
                    else:
                        results["failed"] += 1

            except asyncio.TimeoutError:
                logger.warning(f"Job cancellation timeout after {timeout} seconds")
                # Count what we can
                remaining_jobs = len(self.running_jobs)
                results["failed"] += remaining_jobs
                results["cancelled"] = len(job_ids) - remaining_jobs

        logger.info(
            f"Job cancellation complete: {results['cancelled']} cancelled, {results['failed']} failed"
        )
        return results

    async def _cancel_job_with_result(self, job_id: str) -> bool:
        """Cancel a job and return True/False for success tracking."""
        try:
            return await self.cancel_job(job_id)
        except Exception as e:
            logger.error(f"Error cancelling job {job_id}: {e}")
            return False

    async def collect_results(
        self, job_ids: List[str] = None, cleanup_after_sync: bool = True
    ) -> bool:
        """
        Collect results from remote machine back to local.

        Args:
            job_ids: Specific job IDs to collect, or None for all
            cleanup_after_sync: Whether to clean up remote sweep directory after successful sync

        Returns:
            True if collection successful, False otherwise
        """
        if job_ids is None:
            job_ids = list(self.running_jobs.keys())

        logger.info(f"Collecting results from {len(job_ids)} jobs on {self.remote_config.name}")

        try:
            # Create local remote tasks directory
            local_remote_tasks = (
                self.local_sweep_dir / "tasks" / f"remote_{self.remote_config.name}"
            )
            local_remote_tasks.mkdir(parents=True, exist_ok=True)

            success_count = 0
            # Use rsync to collect all task directories
            if self.remote_tasks_dir:
                # First, check what's actually on the remote before syncing
                try:
                    async with await create_ssh_connection(
                        self.remote_config.host,
                        self.remote_config.ssh_key,
                        self.remote_config.ssh_port,
                    ) as conn:
                        # List what's in the remote tasks directory
                        check_cmd = f"ls -la {self.remote_tasks_dir}/ 2>/dev/null || echo 'EMPTY_OR_MISSING'"
                        check_result = await conn.run(check_cmd, check=False)

                        if "EMPTY_OR_MISSING" in check_result.stdout:
                            logger.warning(
                                f"Remote tasks directory is empty or missing: {self.remote_tasks_dir}"
                            )
                            logger.warning(
                                "This may explain why result collection finds no task directories"
                            )
                        else:
                            logger.debug(f"Remote tasks directory contents:\n{check_result.stdout}")

                        # Also check if any job-specific directories exist
                        for job_id in job_ids:
                            # Extract task name from job name/id
                            task_check_cmd = f"find {self.remote_tasks_dir} -name '*{job_id}*' -type d 2>/dev/null || echo 'NOT_FOUND'"
                            task_result = await conn.run(task_check_cmd, check=False)
                            if "NOT_FOUND" not in task_result.stdout and task_result.stdout.strip():
                                logger.debug(
                                    f"Found remote directories for job {job_id}:\n{task_result.stdout}"
                                )
                            else:
                                logger.warning(f"No remote directory found for job {job_id}")
                except Exception as e:
                    logger.warning(f"Failed to check remote directory contents: {e}")

                rsync_cmd = (
                    f"rsync -avz --compress-level=6 "
                    f"{self.remote_config.host}:{self.remote_tasks_dir}/ "
                    f"{local_remote_tasks}/"
                )

                logger.info(f"Result collection rsync: {rsync_cmd}")

                import subprocess

                result = subprocess.run(rsync_cmd, shell=True, capture_output=True, text=True)

                if result.returncode == 0:
                    logger.info(f"Rsync completed successfully from {self.remote_config.name}")
                    if result.stdout.strip():
                        logger.debug(f"Rsync stdout:\n{result.stdout}")
                    else:
                        logger.warning(
                            "Rsync completed but produced no output - no files may have been transferred"
                        )

                    success_count += 1

                    # Also collect error summaries if they exist
                    await self._collect_error_summaries()

                    # Show summary of what was collected
                    self._show_collection_summary(local_remote_tasks)

                    # Clean up remote sweep directory if requested and sync was successful
                    if cleanup_after_sync:
                        await self.cleanup_remote_environment()

                else:
                    logger.error(f"Result collection failed with exit code {result.returncode}")
                    logger.error(f"Rsync stderr: {result.stderr}")
                    if result.stdout:
                        logger.error(f"Rsync stdout: {result.stdout}")

            # Check if we have a failed_tasks directory that was immediately synced
            failed_tasks_dir = self.local_sweep_dir / "failed_tasks"
            if failed_tasks_dir.exists():
                failed_task_count = len([d for d in failed_tasks_dir.iterdir() if d.is_dir()])
                if failed_task_count > 0:
                    logger.info(
                        f"📁 {failed_task_count} failed task(s) already collected to: {failed_tasks_dir}"
                    )
                    success_count += 1

            # Check if we have error summaries
            error_dir = self.local_sweep_dir / "errors"
            if error_dir.exists():
                error_count = len(list(error_dir.glob("*_error.txt")))
                if error_count > 0:
                    logger.info(f"📄 {error_count} error summary(ies) available in: {error_dir}")
                    success_count += 1

            return success_count > 0

        except Exception as e:
            logger.error(f"Error collecting results from {self.remote_config.name}: {e}")
            return False

    def _show_collection_summary(self, local_tasks_dir: Path):
        """Show a summary of what was collected."""
        try:
            if not local_tasks_dir.exists():
                return

            task_dirs = [d for d in local_tasks_dir.iterdir() if d.is_dir()]
            if not task_dirs:
                logger.info(f"📁 No task directories found in {local_tasks_dir}")
                return

            completed_count = 0
            failed_count = 0

            for task_dir in task_dirs:
                task_info_file = task_dir / "task_info.txt"
                if task_info_file.exists():
                    try:
                        with open(task_info_file) as f:
                            content = f.read()
                            if "Status: COMPLETED" in content:
                                completed_count += 1
                            elif "Status: FAILED" in content:
                                failed_count += 1
                    except Exception:
                        pass

            logger.info(f"📁 Collected {len(task_dirs)} task directories from remote:")
            logger.info(f"   • ✓ {completed_count} completed")
            logger.info(f"   • ✗ {failed_count} failed")
            logger.info(f"   • ? {len(task_dirs) - completed_count - failed_count} unknown status")

        except Exception as e:
            logger.debug(f"Error showing collection summary: {e}")

    async def _collect_error_summaries(self):
        """Collect error summary files from remote to local sweep directory."""
        try:
            async with await create_ssh_connection(
                self.remote_config.host,
                self.remote_config.ssh_key,
                self.remote_config.ssh_port,
            ) as conn:
                # Check if there are error summary files on remote
                if self.remote_sweep_dir:
                    remote_error_dir = f"{self.remote_sweep_dir}/errors"
                    check_cmd = f"test -d {remote_error_dir} && ls {remote_error_dir}/*.txt 2>/dev/null || echo 'no_errors'"

                    result = await conn.run(check_cmd, check=False)

                    if result.returncode == 0 and "no_errors" not in result.stdout:
                        # Error files exist, sync them
                        local_error_dir = self.local_sweep_dir / "errors"
                        local_error_dir.mkdir(exist_ok=True)

                        rsync_cmd = (
                            f"rsync -avz --compress-level=6 "
                            f"{self.remote_config.host}:{remote_error_dir}/ "
                            f"{local_error_dir}/"
                        )

                        import subprocess

                        error_result = subprocess.run(
                            rsync_cmd, shell=True, capture_output=True, text=True
                        )

                        if error_result.returncode == 0:
                            logger.info(
                                f"📁 Collected error summaries from {self.remote_config.name}"
                            )
                        else:
                            logger.warning(
                                f"Failed to collect error summaries: {error_result.stderr}"
                            )
                    else:
                        logger.debug("No error summary files found on remote")

        except Exception as e:
            logger.warning(f"Error collecting error summaries: {e}")

    async def cleanup_remote_environment(self):
        """Clean up remote sweep directory after successful result collection."""
        if not self.remote_sweep_dir:
            logger.debug("No remote sweep directory to clean up")
            return

        logger.info(f"Cleaning up remote sweep directory on {self.remote_config.name}")

        try:
            async with await create_ssh_connection(
                self.remote_config.host,
                self.remote_config.ssh_key,
                self.remote_config.ssh_port,
            ) as conn:
                # Remove remote sweep directory from project
                await conn.run(f"rm -rf {self.remote_sweep_dir}")
                logger.info(f"Cleaned up {self.remote_sweep_dir} on {self.remote_config.name}")

                # Reset the directory path to indicate it's been cleaned up
                self.remote_sweep_dir = None
                self.remote_tasks_dir = None

        except Exception as e:
            logger.warning(f"Error cleaning up remote environment: {e}")
            logger.warning(f"You may need to manually remove: {self.remote_sweep_dir}")

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

    async def get_running_jobs_info(self) -> Dict[str, Any]:
        """Get information about currently running jobs."""
        info = {
            "remote_name": self.remote_config.name,
            "total_jobs": len(self.running_jobs),
            "running_jobs": 0,
            "completed_jobs": 0,
            "failed_jobs": 0,
            "job_details": [],
        }

        # Update job statuses
        for job_id, job_info in self.running_jobs.items():
            status = await self.get_job_status(job_id)
            job_info["status"] = status

            if status == "RUNNING":
                info["running_jobs"] += 1
            elif status == "COMPLETED":
                info["completed_jobs"] += 1
            elif status == "FAILED":
                info["failed_jobs"] += 1

            info["job_details"].append(
                {
                    "job_id": job_id,
                    "job_name": job_info["job_name"],
                    "status": status,
                    "start_time": job_info["start_time"].isoformat()
                    if job_info["start_time"]
                    else None,
                }
            )

        return info

    async def submit_sweep(
        self,
        param_combinations: List[Dict[str, Any]],
        sweep_id: str,
        wandb_group: Optional[str] = None,
    ) -> List[str]:
        """
        Submit a complete sweep with parallel job control.

        Args:
            param_combinations: List of parameter dictionaries for each job
            sweep_id: Unique identifier for this sweep
            wandb_group: Optional W&B group name

        Returns:
            List of job IDs
        """
        self.total_jobs_planned = len(param_combinations)
        self.jobs_completed = 0

        # Setup Rich progress bar if available
        if self.progress_bar.progress:
            self.progress_bar.add_task(
                f"Running remote sweep on {self.remote_config.name}",
                total=self.total_jobs_planned,
            )
            self.progress_bar.start()
        elif self.show_progress:
            logger.info(
                f"Starting {len(param_combinations)} jobs with max {self.max_parallel_jobs} parallel on {self.remote_config.name}"
            )

        try:
            job_ids = []

            # Handle completion runs where param_combinations contains task indices
            if hasattr(self, "is_completion_run") and self.is_completion_run and param_combinations:
                # For completion runs, param_combinations are dicts with 'task_index' and 'params'
                for i, task_info in enumerate(param_combinations):
                    if isinstance(task_info, dict) and "task_number" in task_info:
                        # Use task_number for proper naming in completion runs
                        task_number = task_info["task_number"]
                        params = task_info["params"]
                        job_name = f"{sweep_id}_task_{task_number:03d}"
                    elif isinstance(task_info, dict) and "task_index" in task_info:
                        # Fallback to task_index (old format)
                        task_index = task_info["task_index"]
                        params = task_info["params"]
                        job_name = f"{sweep_id}_task_{task_index + 1:03d}"
                    else:
                        # Fallback for unexpected format
                        params = task_info
                        job_name = f"{sweep_id}_task_{i + 1:03d}"

                    # Update progress for job submission
                    if self.progress_bar.progress:
                        self.progress_bar.update(
                            completed=i,
                            description=f"Submitting remote jobs ({i + 1}/{len(param_combinations)})",
                        )
                    elif self.show_progress:
                        logger.info(f"Submitting job {i + 1}/{len(param_combinations)}: {job_name}")

                    job_id = await self.submit_single_job(params, job_name, sweep_id, wandb_group)
                    job_ids.append(job_id)

                    # If we're limiting parallel jobs, wait for some to complete
                    if len(self.running_jobs) >= self.max_parallel_jobs:
                        if self.progress_bar.progress:
                            self.progress_bar.update(
                                completed=self.jobs_completed,
                                description=f"Waiting for job slots ({len(self.running_jobs)}/{self.max_parallel_jobs} running)",
                            )
                        elif self.show_progress:
                            logger.info(
                                f"Reached max parallel jobs ({self.max_parallel_jobs}), waiting for completion..."
                            )
                        await self._wait_for_job_completion()
            else:
                # Normal runs where param_combinations are just parameter dicts
                for i, params in enumerate(param_combinations):
                    job_name = f"{sweep_id}_task_{i + 1:03d}"

                    # Update progress for job submission
                    if self.progress_bar.progress:
                        self.progress_bar.update(
                            completed=i,
                            description=f"Submitting remote jobs ({i + 1}/{len(param_combinations)})",
                        )
                    elif self.show_progress:
                        logger.info(f"Submitting job {i + 1}/{len(param_combinations)}: {job_name}")

                    job_id = await self.submit_single_job(params, job_name, sweep_id, wandb_group)
                    job_ids.append(job_id)

                    # If we're limiting parallel jobs, wait for some to complete
                    if len(self.running_jobs) >= self.max_parallel_jobs:
                        if self.progress_bar.progress:
                            self.progress_bar.update(
                                completed=self.jobs_completed,
                                description=f"Waiting for job slots ({len(self.running_jobs)}/{self.max_parallel_jobs} running)",
                            )
                        elif self.show_progress:
                            logger.info(
                                f"Reached max parallel jobs ({self.max_parallel_jobs}), waiting for completion..."
                            )
                        await self._wait_for_job_completion()

            # Update progress description for monitoring phase
            if self.progress_bar.progress:
                self.progress_bar.update(
                    completed=self.jobs_completed,
                    description=f"Remote sweep running • {len(self.running_jobs)} jobs active",
                )
            elif self.show_progress:
                logger.info(f"All {len(param_combinations)} jobs submitted")

        finally:
            # Keep progress bar running - it will be stopped by wait_for_all_jobs
            pass

        return job_ids

    async def _wait_for_job_completion(self):
        """Wait for at least one job to complete."""
        while len(self.running_jobs) >= self.max_parallel_jobs:
            completed_jobs = []

            for job_id, job_info in self.running_jobs.items():
                status = await self.get_job_status(job_id)

                if status in ["COMPLETED", "FAILED", "CANCELLED"]:
                    completed_jobs.append(job_id)
                    self.jobs_completed += 1

                    if self.show_progress:
                        job_name = job_info.get("job_name", job_id)
                        logger.info(
                            f"Job {job_name} {status} ({self.jobs_completed}/{self.total_jobs_planned})"
                        )

            # Remove completed jobs
            for job_id in completed_jobs:
                del self.running_jobs[job_id]

            if completed_jobs:
                break

            # Sleep briefly before checking again
            await asyncio.sleep(2)

    async def wait_for_all_jobs(self):
        """Wait for all running jobs to complete."""
        try:
            while self.running_jobs:
                completed_jobs = []

                for job_id, job_info in self.running_jobs.items():
                    status = await self.get_job_status(job_id)

                    if status in ["COMPLETED", "FAILED", "CANCELLED"]:
                        completed_jobs.append(job_id)

                        # Only increment if not already counted
                        if not job_info.get("counted", False):
                            self.jobs_completed += 1
                            job_info["counted"] = True
                            job_info["status"] = status

                            # For failed jobs, collect error information immediately and synchronously
                            if status == "FAILED":
                                try:
                                    await self._collect_and_log_job_error_sync(job_id, job_info)
                                except Exception as e:
                                    logger.warning(
                                        f"Error collecting failure details for job {job_id}: {e}"
                                    )

                            # Update Rich progress bar
                            if self.progress_bar.progress:
                                # Count failed jobs from all completed jobs, not just running ones
                                failed_count = 0
                                for j_info in self.running_jobs.values():
                                    if j_info.get("status") == "FAILED":
                                        failed_count += 1

                                self.progress_bar.update(
                                    completed=self.jobs_completed,
                                    description=f"Remote sweep • ✓ {self.jobs_completed} • ✗ {failed_count} • {len(self.running_jobs) - len(completed_jobs)} running",
                                )
                            elif self.show_progress:
                                job_name = job_info.get("job_name", job_id)
                                logger.info(
                                    f"Job {job_name} {status} ({self.jobs_completed}/{self.total_jobs_planned})"
                                )

                # Remove completed jobs
                for job_id in completed_jobs:
                    del self.running_jobs[job_id]

                if not completed_jobs and self.running_jobs:
                    # Show periodic progress summary (only for non-Rich mode)
                    if self.show_progress and not self.progress_bar.progress:
                        await self._show_progress_summary()
                    # Sleep briefly before checking again
                    await asyncio.sleep(5)

            # Final completion message
            if self.progress_bar.progress:
                failed_count = self.total_jobs_planned - self.jobs_completed
                self.progress_bar.update(
                    completed=self.total_jobs_planned,
                    description=f"Remote sweep completed • ✓ {self.jobs_completed} • ✗ {failed_count}",
                )
                if self.progress_bar.console:
                    self.progress_bar.console.print(
                        f"[green]All {self.total_jobs_planned} remote jobs completed![/green]"
                    )
            elif self.show_progress:
                logger.info(f"All {self.total_jobs_planned} jobs completed!")

        finally:
            # Stop progress bar
            if self.progress_bar.progress:
                self.progress_bar.stop()

    async def _collect_and_log_job_error_sync(self, job_id: str, job_info: dict):
        """Collect and log error information for a failed job - synchronous version."""
        try:
            async with await create_ssh_connection(
                self.remote_config.host,
                self.remote_config.ssh_key,
                self.remote_config.ssh_port,
            ) as conn:
                task_dir = job_info.get("task_dir")
                job_name = job_info.get("job_name", job_id)

                if not task_dir:
                    logger.warning(f"No task directory found for failed job {job_name}")
                    return

                # Collect error information from various sources
                error_info = await self._gather_job_error_details(conn, task_dir, job_name)

                if error_info:
                    # Log the error details immediately
                    logger.error(f"❌ Job {job_name} failed - Error details:")

                    if error_info.get("stderr"):
                        logger.error(f"Standard Error: {error_info['stderr'][:500]}...")

                    if error_info.get("last_logs"):
                        logger.error(f"Last logs: {error_info['last_logs'][:500]}...")

                    if error_info.get("exit_code"):
                        logger.error(f"Exit code: {error_info['exit_code']}")

                    if error_info.get("error_patterns"):
                        logger.error(
                            f"Error patterns found: {error_info['error_patterns'][:300]}..."
                        )

                    # Store error info for later collection
                    job_info["error_details"] = error_info

                    # Save error summary to local file immediately
                    await self._save_error_summary_locally_sync(job_name, error_info)

                    # Also immediately sync the task directory back to local
                    await self._sync_failed_task_immediately(conn, task_dir, job_name)
                else:
                    logger.warning(f"Could not collect error details for failed job {job_name}")

        except Exception as e:
            logger.warning(f"Error collecting failure details for job {job_id}: {e}")

    async def _sync_failed_task_immediately(self, conn, remote_task_dir: str, job_name: str):
        """Immediately sync a failed task directory back to local storage."""
        try:
            # Create local directory for this specific failed task
            local_failed_tasks = self.local_sweep_dir / "failed_tasks"
            local_failed_tasks.mkdir(exist_ok=True)

            local_task_dir = local_failed_tasks / job_name
            local_task_dir.mkdir(exist_ok=True)

            # Use rsync to copy the task directory immediately
            rsync_cmd = (
                f"rsync -avz --compress-level=6 "
                f"{self.remote_config.host}:{remote_task_dir}/ "
                f"{local_task_dir}/"
            )

            logger.debug(f"Immediate failed task sync: {rsync_cmd}")

            import subprocess

            result = subprocess.run(rsync_cmd, shell=True, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info(f"📁 Immediately synced failed task {job_name} to {local_task_dir}")
            else:
                logger.warning(f"Failed to sync failed task {job_name}: {result.stderr}")

        except Exception as e:
            logger.warning(f"Error syncing failed task {job_name}: {e}")

    async def _save_error_summary_locally_sync(self, job_name: str, error_info: dict):
        """Save error summary to local file immediately - synchronous version."""
        try:
            error_dir = self.local_sweep_dir / "errors"
            error_dir.mkdir(exist_ok=True)

            error_file = error_dir / f"{job_name}_error.txt"

            with open(error_file, "w") as f:
                f.write(f"Error Summary for Job: {job_name}\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Collected at: {datetime.now()}\n")
                f.write(f"Remote machine: {self.remote_config.name}\n\n")

                # Quick diagnosis section
                if error_info.get("jax_error_type") or error_info.get("shell_error_type"):
                    f.write("QUICK DIAGNOSIS:\n")
                    f.write("=" * 16 + "\n")

                    if error_info.get("jax_error_type"):
                        f.write(f"JAX Error: {error_info['jax_error_type']}\n")
                        f.write(f"Suggestion: {error_info.get('jax_suggestion', 'Unknown')}\n")

                    if error_info.get("shell_error_type"):
                        f.write(f"Shell Error: {error_info['shell_error_type']}\n")
                        f.write(f"Suggestion: {error_info.get('shell_suggestion', 'Unknown')}\n")

                    f.write("\n")

                if error_info.get("exit_code"):
                    f.write(f"Exit Code: {error_info['exit_code']}\n\n")

                if error_info.get("error_flag"):
                    f.write(f"Error Flag: {error_info['error_flag']}\n\n")

                if error_info.get("stderr"):
                    f.write("Standard Error Output:\n")
                    f.write("-" * 25 + "\n")
                    f.write(error_info["stderr"])
                    f.write("\n\n")

                if error_info.get("captured_logs"):
                    f.write("Captured Error Logs:\n")
                    f.write("-" * 20 + "\n")
                    f.write(error_info["captured_logs"])
                    f.write("\n\n")

                if error_info.get("error_patterns"):
                    f.write("Error Patterns Found:\n")
                    f.write("-" * 20 + "\n")
                    f.write(error_info["error_patterns"])
                    f.write("\n\n")

                if error_info.get("last_logs"):
                    f.write("Last Log Entries:\n")
                    f.write("-" * 17 + "\n")
                    f.write(error_info["last_logs"])
                    f.write("\n\n")

                if error_info.get("system_info"):
                    f.write("System Info at Failure:\n")
                    f.write("-" * 23 + "\n")
                    f.write(error_info["system_info"])
                    f.write("\n\n")

                if error_info.get("task_info"):
                    f.write("Task Info:\n")
                    f.write("-" * 10 + "\n")
                    f.write(error_info["task_info"])
                    f.write("\n\n")

                if error_info.get("disk_info"):
                    f.write("Disk Space Info:\n")
                    f.write("-" * 16 + "\n")
                    f.write(error_info["disk_info"])
                    f.write("\n\n")

                if error_info.get("collection_error"):
                    f.write("Error Collection Issues:\n")
                    f.write("-" * 25 + "\n")
                    f.write(error_info["collection_error"])
                    f.write("\n\n")

                f.write("=" * 50 + "\n")
                f.write("End of Error Summary\n")
                f.write("\nTo see the full task directory with all files, check:\n")
                f.write(f"./failed_tasks/{job_name}/\n")

            logger.info(f"📄 Error summary saved: {error_file}")

        except Exception as e:
            logger.warning(f"Error saving error summary for {job_name}: {e}")

    # Keep the original async version for backwards compatibility
    async def _collect_and_log_job_error(self, job_id: str, job_info: dict):
        """Collect and log error information for a failed job."""
        return await self._collect_and_log_job_error_sync(job_id, job_info)

    async def _gather_job_error_details(self, conn, task_dir: str, job_name: str) -> dict:
        """Gather detailed error information from a failed job."""
        error_info = {}

        try:
            # First check if there's an error flag (created by our improved script)
            error_flag_cmd = f"cat {task_dir}/error_flag.txt 2>/dev/null || echo 'no_flag'"
            error_flag_result = await conn.run(error_flag_cmd, check=False)
            if error_flag_result.returncode == 0 and "no_flag" not in error_flag_result.stdout:
                error_info["error_flag"] = error_flag_result.stdout.strip()

            # Get exit code from our improved script
            exit_code_cmd = f"cat {task_dir}/exit_code.txt 2>/dev/null || echo 'unknown'"
            exit_code_result = await conn.run(exit_code_cmd, check=False)
            if exit_code_result.returncode == 0 and exit_code_result.stdout.strip() != "unknown":
                error_info["exit_code"] = exit_code_result.stdout.strip()

            # Check for stderr output from our improved script
            stderr_cmd = f"cat {task_dir}/stderr.log 2>/dev/null || echo 'no_stderr'"
            stderr_result = await conn.run(stderr_cmd, check=False)
            if stderr_result.returncode == 0 and "no_stderr" not in stderr_result.stdout:
                # Get last 50 lines to avoid overwhelming output
                stderr_content = stderr_result.stdout.strip()
                error_info["stderr"] = (
                    stderr_content[-3000:] if len(stderr_content) > 3000 else stderr_content
                )

            # Check for captured error logs from our improved script
            error_logs_cmd = f"cat {task_dir}/error_logs.txt 2>/dev/null || echo 'no_error_logs'"
            error_logs_result = await conn.run(error_logs_cmd, check=False)
            if (
                error_logs_result.returncode == 0
                and "no_error_logs" not in error_logs_result.stdout
            ):
                error_info["captured_logs"] = error_logs_result.stdout.strip()

            # Check for system info at failure
            error_system_cmd = (
                f"cat {task_dir}/error_system.txt 2>/dev/null || echo 'no_system_info'"
            )
            error_system_result = await conn.run(error_system_cmd, check=False)
            if (
                error_system_result.returncode == 0
                and "no_system_info" not in error_system_result.stdout
            ):
                error_info["system_info"] = error_system_result.stdout.strip()

            # Look for any other stderr files (fallback)
            if not error_info.get("stderr"):
                stderr_find_cmd = f"find {task_dir} -name '*.err' -o -name 'stderr*' | head -1"
                stderr_find_result = await conn.run(stderr_find_cmd, check=False)

                if stderr_find_result.returncode == 0 and stderr_find_result.stdout.strip():
                    stderr_file = stderr_find_result.stdout.strip()
                    stderr_content = await conn.run(f"tail -30 {stderr_file}", check=False)
                    if stderr_content.returncode == 0 and stderr_content.stdout.strip():
                        error_info["stderr"] = stderr_content.stdout.strip()

            # Check for log files with error patterns
            log_cmd = f"find {task_dir} -name '*.log' -not -name 'stderr.log' | head -1"
            log_result = await conn.run(log_cmd, check=False)

            if log_result.returncode == 0 and log_result.stdout.strip():
                log_file = log_result.stdout.strip()
                # Look for error patterns in logs
                error_grep = await conn.run(
                    f"grep -i 'error\\|exception\\|failed\\|traceback\\|ImportError\\|ModuleNotFoundError\\|AttributeError\\|RuntimeError' {log_file} | tail -15",
                    check=False,
                )
                if error_grep.returncode == 0 and error_grep.stdout.strip():
                    error_info["error_patterns"] = error_grep.stdout.strip()

                # Also get last 20 lines of log
                last_logs = await conn.run(f"tail -20 {log_file}", check=False)
                if last_logs.returncode == 0 and last_logs.stdout.strip():
                    error_info["last_logs"] = last_logs.stdout.strip()

            # Check task_info.txt for any recorded errors
            task_info_cmd = f"cat {task_dir}/task_info.txt 2>/dev/null || echo 'no_task_info'"
            task_info_result = await conn.run(task_info_cmd, check=False)
            if task_info_result.returncode == 0 and "no_task_info" not in task_info_result.stdout:
                error_info["task_info"] = task_info_result.stdout.strip()

            # Check for disk space issues (common cause of failures)
            disk_check_cmd = f"df -h {task_dir} | tail -1"
            disk_result = await conn.run(disk_check_cmd, check=False)
            if disk_result.returncode == 0:
                error_info["disk_info"] = disk_result.stdout.strip()

            # Look for specific JAX/CUDA errors in stderr
            if error_info.get("stderr"):
                stderr_lower = error_info["stderr"].lower()
                if "gpuallocatorconfig" in stderr_lower:
                    error_info["jax_error_type"] = "GPU Allocator Config Error"
                    error_info["jax_suggestion"] = (
                        "JAX/CUDA compatibility issue. Try setting JAX_PLATFORMS=cpu"
                    )
                elif "module 'jaxlib.xla_extension' has no attribute" in stderr_lower:
                    error_info["jax_error_type"] = "JAX Extension Error"
                    error_info["jax_suggestion"] = (
                        "JAX/JAXlib version mismatch. Update or reinstall JAX"
                    )
                elif "no such file or directory" in stderr_lower and "bc" in stderr_lower:
                    error_info["shell_error_type"] = "Missing bc command"
                    error_info["shell_suggestion"] = "Install bc package: apt-get install bc"

        except Exception as e:
            logger.debug(f"Error gathering job error details: {e}")
            error_info["collection_error"] = str(e)

        return error_info

    async def _save_error_summary_locally(self, job_name: str, error_info: dict):
        """Save error summary to local file for review."""
        try:
            error_dir = self.local_sweep_dir / "errors"
            error_dir.mkdir(exist_ok=True)

            error_file = error_dir / f"{job_name}_error.txt"

            with open(error_file, "w") as f:
                f.write(f"Error Summary for Job: {job_name}\n")
                f.write("=" * 50 + "\n\n")

                if error_info.get("exit_code"):
                    f.write(f"Exit Code: {error_info['exit_code']}\n\n")

                if error_info.get("error_patterns"):
                    f.write("Error Patterns Found:\n")
                    f.write("-" * 20 + "\n")
                    f.write(error_info["error_patterns"])
                    f.write("\n\n")

                if error_info.get("stderr"):
                    f.write("Standard Error Output:\n")
                    f.write("-" * 20 + "\n")
                    f.write(error_info["stderr"])
                    f.write("\n\n")

                if error_info.get("last_logs"):
                    f.write("Last Log Lines:\n")
                    f.write("-" * 20 + "\n")
                    f.write(error_info["last_logs"])
                    f.write("\n\n")

                if error_info.get("disk_info"):
                    f.write("Disk Information:\n")
                    f.write("-" * 20 + "\n")
                    f.write(error_info["disk_info"])
                    f.write("\n\n")

                if error_info.get("task_info"):
                    f.write("Task Information:\n")
                    f.write("-" * 20 + "\n")
                    f.write(error_info["task_info"])
                    f.write("\n")

            logger.info(f"📁 Error details saved to: {error_file}")

        except Exception as e:
            logger.warning(f"Could not save error summary for {job_name}: {e}")

    async def _show_progress_summary(self):
        """Display a summary of current progress."""
        running_count = 0
        for job_id in self.running_jobs:
            status = await self.get_job_status(job_id)
            if status == "RUNNING":
                running_count += 1

        if self.total_jobs_planned > 0:
            progress_pct = (self.jobs_completed / self.total_jobs_planned) * 100
            logger.info(
                f"Progress: {self.jobs_completed}/{self.total_jobs_planned} ({progress_pct:.1f}%) - {running_count} currently running"
            )

    # Note: Sync-related methods moved to ProjectStateChecker in project_sync.py

    def _prompt_for_sync(self, sync_result: Dict[str, Any], conn) -> bool:
        """Prompt user whether to sync local changes to remote with detailed diff display."""
        print("\n" + "=" * 80)
        print("🔄 PROJECT SYNC REQUIRED - UNCOMMITTED CHANGES DETECTED")
        print("=" * 80)

        if sync_result["method"] == "git":
            details = sync_result["details"]
            if details.get("local_status"):
                print("Local uncommitted changes detected:")
                for line in details["local_status"].split("\n"):
                    if line.strip():
                        status = line[:2]
                        filename = line[3:] if len(line) > 3 else ""
                        if status.strip() in ["M", "MM"]:
                            print(f"  📝 Modified: {filename}")
                        elif status.strip() in ["A", "AM"]:
                            print(f"  ➕ Added: {filename}")
                        elif status.strip() in ["D", "AD"]:
                            print(f"  ❌ Deleted: {filename}")
                        elif status.strip() in ["R"]:
                            print(f"  ↗️ Renamed: {filename}")
                        else:
                            print(f"  {status} {filename}")

                # Show detailed diff for config files and key files
                self._show_detailed_git_diff(details.get("local_status", ""))

        elif sync_result["method"] == "checksum":
            details = sync_result["details"]
            if details.get("mismatched_files"):
                print("Files that differ between local and remote:")
                for mismatch in details["mismatched_files"]:
                    file_path = mismatch["file"]
                    print(f"  📄 {file_path}")
                    # Show actual diff preview for important files
                    self._show_file_content_diff(file_path, mismatch)

        print("\n" + "⚠️" * 25)
        print("IMPORTANT: Review the changes above carefully!")
        print("Config changes can significantly affect experiment results.")
        print("⚠️" * 25)

        # Determine available options based on the sync situation
        details = sync_result.get("details", {})
        has_local_changes = bool(details.get("local_status"))
        has_remote_changes = bool(details.get("remote_status"))

        print("\nOptions:")
        if has_local_changes and not has_remote_changes:
            print("  ✅ [Y] Sync local changes to remote (after reviewing above)")
        elif not has_local_changes and has_remote_changes:
            print("  ✅ [Y] Bring remote changes to local (after reviewing above)")
            print("  🔄 [r] Reverse sync: pull remote changes to local")
        elif has_local_changes and has_remote_changes:
            print("  ✅ [Y] Sync local changes to remote (will overwrite remote changes)")
            print(
                "  🔄 [r] Reverse sync: bring remote changes to local (will overwrite local changes)"
            )
        else:
            print("  ✅ [Y] Proceed with sync")

        print("  ❌ [n] Cancel sweep execution")
        print("  🔍 [d] Show detailed diff for all changed files")

        while True:
            try:
                if has_remote_changes and (not has_local_changes or has_local_changes):
                    choice = input("\nChoose [Y/n/d/r]: ").strip().lower()
                else:
                    choice = input("\nChoose [Y/n/d]: ").strip().lower()

                if choice in ["", "y", "yes"]:
                    return True
                elif choice in ["n", "no"]:
                    return False
                elif choice in ["r", "reverse"] and has_remote_changes:
                    return "reverse"
                elif choice in ["d", "diff"]:
                    import asyncio

                    asyncio.run(self._show_full_detailed_diff_async(sync_result, conn))
                    continue
                else:
                    if has_remote_changes:
                        print("Please enter Y, n, d, or r")
                    else:
                        print("Please enter Y, n, or d")
            except (EOFError, KeyboardInterrupt):
                print("\nOperation cancelled by user")
                return False

    def _show_detailed_git_diff(self, git_status: str):
        """Show detailed git diff for important files."""
        import subprocess

        print("\n📋 DETAILED CHANGES PREVIEW:")
        print("-" * 60)

        # Parse git status to find modified files
        important_patterns = [
            "config",
            "Config",
            "CONFIG",
            ".yaml",
            ".yml",
            ".json",
            "train",
            "Train",
            "TRAIN",
            "requirements.txt",
            "pyproject.toml",
        ]

        for line in git_status.split("\n"):
            if not line.strip():
                continue

            status = line[:2]
            filename = line[3:] if len(line) > 3 else ""

            # Show diff for modified files that match important patterns
            if status.strip() in ["M", "MM"] and any(
                pattern in filename for pattern in important_patterns
            ):
                try:
                    # Get diff for this specific file
                    diff_result = subprocess.run(
                        ["git", "diff", "HEAD", "--", filename],
                        cwd=self.local_project_root,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )

                    if diff_result.returncode == 0 and diff_result.stdout.strip():
                        print(f"\n🔍 Changes in {filename}:")
                        # Show a condensed diff (first 15 lines)
                        diff_lines = diff_result.stdout.split("\n")
                        shown_lines = 0
                        for i, diff_line in enumerate(diff_lines):
                            if shown_lines >= 15:
                                remaining = len(diff_lines) - i
                                if remaining > 0:
                                    print(
                                        f"    ... ({remaining} more lines, use 'd' to see full diff)"
                                    )
                                break

                            if diff_line.startswith("@@"):
                                print(f"    {diff_line}")
                                shown_lines += 1
                            elif diff_line.startswith("+") and not diff_line.startswith("+++"):
                                print(f"    🟢 {diff_line}")
                                shown_lines += 1
                            elif diff_line.startswith("-") and not diff_line.startswith("---"):
                                print(f"    🔴 {diff_line}")
                                shown_lines += 1
                            elif (
                                diff_line.strip()
                                and not diff_line.startswith("diff --git")
                                and not diff_line.startswith("index ")
                            ):
                                print(f"      {diff_line}")
                                shown_lines += 1

                except Exception as e:
                    print(f"    ⚠️ Could not show diff for {filename}: {e}")

        print("-" * 60)

    def _show_file_content_diff(self, file_path: str, mismatch_info: dict):
        """Show content differences for files when using checksum method."""
        try:
            local_file = Path.cwd() / file_path
            if local_file.exists():
                print(f"\n🔍 File differs: {file_path}")
                print(
                    f"    📍 Type: {'Config file' if self._is_config_file(file_path) else 'Regular file'}"
                )
                print(
                    f"    🔢 Checksum mismatch: local={mismatch_info.get('local_checksum', 'unknown')[:8]}... vs remote={mismatch_info.get('remote_checksum', 'unknown')[:8]}..."
                )

                # For config files, show a preview of local content
                if self._is_config_file(file_path):
                    print("    📋 Local content preview:")
                    try:
                        with open(local_file, encoding="utf-8") as f:
                            content = f.read()
                            lines = content.split("\n")
                            total_lines = len(lines)

                            # Show a reasonable preview
                            max_preview_lines = 8
                            if total_lines <= max_preview_lines:
                                for i, line in enumerate(lines, 1):
                                    print(f"      {i:2d}: {line}")
                            else:
                                # Show first few and last few lines
                                show_lines = max_preview_lines // 2
                                for i in range(show_lines):
                                    if i < len(lines):
                                        print(f"      {i + 1:2d}: {lines[i]}")

                                if total_lines > max_preview_lines:
                                    print(
                                        f"      ... ({total_lines - max_preview_lines} more lines)"
                                    )

                                for i in range(total_lines - show_lines, total_lines):
                                    if i >= 0 and i < len(lines):
                                        print(f"      {i + 1:2d}: {lines[i]}")
                    except Exception as read_error:
                        print(f"      ⚠️ Could not read file content: {read_error}")

                print("    💡 Use [d] option to see detailed diff between local and remote")
            else:
                print(f"\n⚠️ Local file not found: {file_path}")
        except Exception as e:
            print(f"    ⚠️ Could not preview {file_path}: {e}")

    async def _show_file_content_diff_async(self, conn, file_path: str, mismatch_info: dict):
        """Show actual diff between local and remote file content."""
        try:
            local_file = Path.cwd() / file_path
            if not local_file.exists():
                print(f"    ⚠️ Local file not found: {file_path}")
                return

            # Read local content
            with open(local_file, encoding="utf-8") as f:
                local_content = f.read().splitlines()

            # Get remote content
            remote_file_path = f"{self.remote_config.project_root}/{file_path}"
            remote_result = await conn.run(f"cat '{remote_file_path}'", check=False)

            if remote_result.returncode != 0:
                print(f"    ⚠️ Could not read remote file: {file_path}")
                return

            remote_content = remote_result.stdout.splitlines()

            # Generate diff
            import difflib

            diff = list(
                difflib.unified_diff(
                    remote_content,
                    local_content,
                    fromfile=f"remote:{file_path}",
                    tofile=f"local:{file_path}",
                    lineterm="",
                )
            )

            if not diff:
                print(
                    "    📄 Files appear identical (checksum difference may be due to line endings)"
                )
                return

            print(f"\n🔍 Detailed changes in {file_path}:")
            print("    " + "-" * 60)

            shown_lines = 0
            max_preview_lines = 20

            for line in diff:
                if shown_lines >= max_preview_lines:
                    remaining = len(diff) - diff.index(line)
                    print(f"    ... ({remaining} more diff lines, use 'd' for complete diff)")
                    break

                if line.startswith("@@"):
                    print(f"    📍 {line}")
                elif line.startswith("+") and not line.startswith("+++"):
                    print(f"    🟢 {line}")
                elif line.startswith("-") and not line.startswith("---"):
                    print(f"    🔴 {line}")
                elif not line.startswith("---") and not line.startswith("+++"):
                    print(f"      {line}")

                shown_lines += 1

            print("    " + "-" * 60)

        except Exception as e:
            print(f"    ⚠️ Could not show diff for {file_path}: {e}")

    def _show_full_detailed_diff(self, sync_result: Dict[str, Any]):
        """Show complete detailed diff for all changed files."""
        import subprocess

        print("\n" + "=" * 80)
        print("📋 COMPLETE DETAILED DIFF")
        print("=" * 80)

        if sync_result["method"] == "git":
            details = sync_result["details"]
            git_status = details.get("local_status", "")

            for line in git_status.split("\n"):
                if not line.strip():
                    continue

                status = line[:2]
                filename = line[3:] if len(line) > 3 else ""

                if status.strip() in ["M", "MM"]:
                    try:
                        print(f"\n{'=' * 20} {filename} {'=' * 20}")
                        diff_result = subprocess.run(
                            ["git", "diff", "HEAD", "--", filename],
                            cwd=self.local_project_root,
                            capture_output=True,
                            text=True,
                            timeout=15,
                        )

                        if diff_result.returncode == 0:
                            print(diff_result.stdout)
                        else:
                            print(f"Could not get diff for {filename}")

                    except Exception as e:
                        print(f"Error showing diff for {filename}: {e}")

        print("=" * 80)
        input("Press Enter to continue...")

    async def _show_full_detailed_diff_async(self, sync_result: Dict[str, Any], conn):
        """Show complete detailed diff for all changed files (async version)."""
        import subprocess

        print("\n" + "=" * 80)
        print("📋 COMPLETE DETAILED DIFF")
        print("=" * 80)

        if sync_result["method"] == "git":
            details = sync_result["details"]
            git_status = details.get("local_status", "")

            for line in git_status.split("\n"):
                if not line.strip():
                    continue

                status = line[:2]
                filename = line[3:] if len(line) > 3 else ""

                if status.strip() in ["M", "MM"]:
                    try:
                        print(f"\n{'=' * 20} {filename} {'=' * 20}")
                        diff_result = subprocess.run(
                            ["git", "diff", "HEAD", "--", filename],
                            cwd=self.local_project_root,
                            capture_output=True,
                            text=True,
                            timeout=15,
                        )

                        if diff_result.returncode == 0:
                            print(diff_result.stdout)
                        else:
                            print(f"Could not get diff for {filename}")

                    except Exception as e:
                        print(f"Error showing diff for {filename}: {e}")

        elif sync_result["method"] == "checksum":
            details = sync_result["details"]
            mismatched_files = details.get("mismatched_files", [])

            for mismatch in mismatched_files:
                file_path = mismatch["file"]
                print(f"\n{'=' * 20} {file_path} {'=' * 20}")
                await self._show_file_content_diff_async(conn, file_path, mismatch)

        print("=" * 80)
        input("Press Enter to continue...")

    async def _sync_mismatched_files(self, conn, sync_result: Dict[str, Any]) -> bool:
        """Sync specific mismatched files to remote."""
        try:
            if sync_result["method"] == "checksum":
                details = sync_result["details"]
                mismatched_files = details.get("mismatched_files", [])

                for mismatch in mismatched_files:
                    file_path = mismatch["file"]
                    local_file = Path.cwd() / file_path

                    if not local_file.exists():
                        logger.warning(f"Local file not found: {file_path}")
                        continue

                    logger.info(f"Syncing: {file_path}")

                    # Read local file content
                    with open(local_file, encoding="utf-8") as f:
                        content = f.read()

                    # Write to remote using a here-document to handle special characters
                    remote_file_path = f"{self.remote_config.project_root}/{file_path}"

                    # Ensure remote directory exists
                    remote_dir = str(Path(remote_file_path).parent)
                    await conn.run(f"mkdir -p {remote_dir}")

                    # Write file content to remote
                    cmd = f"""cat > {remote_file_path} << 'HSMSYNCEOF'
{content}
HSMSYNCEOF"""

                    result = await conn.run(cmd, check=False)
                    if result.returncode != 0:
                        logger.error(f"Failed to sync {file_path}: {result.stderr}")
                        return False

                logger.info(f"✓ Successfully synced {len(mismatched_files)} files to remote")

                # Wait a moment for filesystem to sync
                await asyncio.sleep(1)

                # Verify sync by re-checking the files we just synced
                verification_failed = False
                for mismatch in mismatched_files:
                    file_path = mismatch["file"]
                    local_file = Path.cwd() / file_path

                    if not local_file.exists():
                        continue

                    local_checksum = hashlib.sha256()
                    with open(local_file, "rb") as f:
                        for chunk in iter(lambda: f.read(4096), b""):
                            local_checksum.update(chunk)
                    local_hash = local_checksum.hexdigest()

                    # Check remote file again
                    remote_file_path = f"{self.remote_config.project_root}/{file_path}"
                    verify_cmd = f"sha256sum '{remote_file_path}' | cut -d' ' -f1"
                    verify_result = await conn.run(verify_cmd, check=False)

                    if verify_result.returncode == 0:
                        remote_hash = verify_result.stdout.strip()
                        if local_hash == remote_hash:
                            logger.debug(f"✓ Sync verified for {file_path}")
                        else:
                            logger.warning(f"⚠ Sync verification failed for {file_path}")
                            verification_failed = True
                    else:
                        logger.warning(f"⚠ Could not verify sync for {file_path}")
                        verification_failed = True

                if verification_failed:
                    logger.warning("Some files may not have synced properly")
                else:
                    logger.info("✓ All synced files verified successfully")

                return True

            elif sync_result["method"] == "git":
                # Handle git uncommitted changes by syncing specific files
                logger.info("Syncing git uncommitted changes...")
                details = sync_result["details"]
                local_status = details.get("local_status", "")

                if not local_status:
                    logger.warning("No git changes detected to sync")
                    return True

                # Parse git status to get list of changed files
                changed_files = []
                for line in local_status.split("\n"):
                    if not line.strip():
                        continue

                    # Git status format: XY filename
                    # X = staged status, Y = unstaged status
                    status = line[:2]
                    filename = line[3:] if len(line) > 3 else ""

                    # Only sync modified and added files (not deleted)
                    if status.strip() in ["M", "MM", "A", "AM", "??"]:
                        changed_files.append(filename)

                if not changed_files:
                    logger.warning("No syncable files found in git changes")
                    return True

                logger.info(f"Syncing {len(changed_files)} changed files to remote")

                # Sync each changed file
                sync_success = True
                for file_path in changed_files:
                    try:
                        local_file = self.local_project_root / file_path

                        if not local_file.exists():
                            logger.warning(f"Local file not found: {file_path}")
                            continue

                        logger.info(f"Syncing: {file_path}")

                        # Read local file content
                        with open(local_file, encoding="utf-8", errors="replace") as f:
                            content = f.read()

                        # Write to remote using a here-document to handle special characters
                        remote_file_path = f"{self.remote_config.project_root}/{file_path}"

                        # Ensure remote directory exists
                        remote_dir = str(Path(remote_file_path).parent)
                        await conn.run(f"mkdir -p '{remote_dir}'")

                        # Write file content to remote
                        cmd = f"""cat > '{remote_file_path}' << 'HSMSYNCEOF'
{content}
HSMSYNCEOF"""

                        result = await conn.run(cmd, check=False)
                        if result.returncode != 0:
                            logger.error(f"Failed to sync {file_path}: {result.stderr}")
                            sync_success = False
                        else:
                            logger.debug(f"✓ Successfully synced {file_path}")

                    except Exception as e:
                        logger.error(f"Error syncing {file_path}: {e}")
                        sync_success = False

                if sync_success:
                    logger.info(
                        f"✓ Successfully synced all {len(changed_files)} git changes to remote"
                    )
                else:
                    logger.error("Some files failed to sync")

                return sync_success

            return False

        except Exception as e:
            logger.error(f"Error during file sync: {e}")
            return False

    async def _reverse_sync_from_remote(self, conn, sync_result: Dict[str, Any]) -> bool:
        """Sync remote changes to local (reverse sync)."""
        try:
            if sync_result["method"] == "git":
                # Handle reverse sync for git repositories
                logger.info("Performing reverse sync: bringing remote changes to local...")
                details = sync_result["details"]
                remote_status = details.get("remote_status", "")

                if not remote_status:
                    logger.warning("No remote changes detected to sync")
                    return True

                # Parse remote git status to get list of changed files
                changed_files = []
                for line in remote_status.split("\n"):
                    if not line.strip():
                        continue

                    # Git status format: XY filename
                    status = line[:2]
                    filename = line[3:] if len(line) > 3 else ""

                    # Only sync modified and added files (not deleted)
                    if status.strip() in ["M", "MM", "A", "AM", "??"]:
                        changed_files.append(filename)

                if not changed_files:
                    logger.warning("No syncable files found in remote changes")
                    return True

                logger.info(f"Bringing {len(changed_files)} changed files from remote to local")

                # Sync each changed file from remote to local
                sync_success = True
                for file_path in changed_files:
                    try:
                        logger.info(f"Reverse syncing: {file_path}")

                        # Get remote file content
                        remote_file_path = f"{self.remote_config.project_root}/{file_path}"
                        get_content_cmd = f"cat '{remote_file_path}'"

                        remote_result = await conn.run(get_content_cmd, check=False)
                        if remote_result.returncode != 0:
                            logger.error(
                                f"Failed to read remote file {file_path}: {remote_result.stderr}"
                            )
                            sync_success = False
                            continue

                        # Write to local file
                        local_file = self.local_project_root / file_path

                        # Ensure local directory exists
                        local_file.parent.mkdir(parents=True, exist_ok=True)

                        # Write content to local file
                        with open(local_file, "w", encoding="utf-8") as f:
                            f.write(remote_result.stdout)

                        logger.debug(f"✓ Successfully reverse synced {file_path}")

                    except Exception as e:
                        logger.error(f"Error reverse syncing {file_path}: {e}")
                        sync_success = False

                if sync_success:
                    logger.info(
                        f"✓ Successfully reverse synced all {len(changed_files)} files from remote"
                    )
                else:
                    logger.error("Some files failed to reverse sync")

                return sync_success

            elif sync_result["method"] == "checksum":
                # Handle reverse sync for checksum-based verification
                details = sync_result["details"]
                mismatched_files = details.get("mismatched_files", [])

                for mismatch in mismatched_files:
                    file_path = mismatch["file"]
                    logger.info(f"Reverse syncing: {file_path}")

                    # Get remote file content
                    remote_file_path = f"{self.remote_config.project_root}/{file_path}"
                    get_content_cmd = f"cat '{remote_file_path}'"

                    remote_result = await conn.run(get_content_cmd, check=False)
                    if remote_result.returncode != 0:
                        logger.error(
                            f"Failed to read remote file {file_path}: {remote_result.stderr}"
                        )
                        continue

                    # Write to local file
                    local_file = Path.cwd() / file_path
                    local_file.parent.mkdir(parents=True, exist_ok=True)

                    with open(local_file, "w", encoding="utf-8") as f:
                        f.write(remote_result.stdout)

                    logger.debug(f"✓ Successfully reverse synced {file_path}")

                logger.info(
                    f"✓ Successfully reverse synced {len(mismatched_files)} files from remote"
                )
                return True

            return False

        except Exception as e:
            logger.error(f"Error during reverse sync: {e}")
            return False

    def _show_sync_error_details(self, sync_result: Dict[str, Any]):
        """Show detailed sync error information."""
        logger.error("Project synchronization verification failed!")
        logger.error("Local and remote projects are not in the same state.")

        if sync_result["method"] == "git":
            logger.error("Git-based verification details:")
            details = sync_result["details"]
            if details.get("local_commit") != details.get("remote_commit"):
                logger.error(f"  Local commit:  {details.get('local_commit', 'unknown')[:12]}")
                logger.error(f"  Remote commit: {details.get('remote_commit', 'unknown')[:12]}")
            if details.get("local_status"):
                logger.error(f"  Local uncommitted changes:\n{details['local_status']}")
            if details.get("remote_status"):
                logger.error(f"  Remote uncommitted changes:\n{details['remote_status']}")
        elif sync_result["method"] == "checksum":
            logger.error("File checksum verification details:")
            details = sync_result["details"]
            if details.get("mismatched_files"):
                logger.error("  Mismatched files:")
                for mismatch in details["mismatched_files"]:
                    logger.error(f"    {mismatch['file']}")
            if details.get("missing_files"):
                logger.error(f"  Missing files on remote: {details['missing_files']}")

        # Check if this is being used in distributed mode and provide context
        if hasattr(self, "is_distributed_mode") and self.is_distributed_mode:
            logger.error("In distributed mode, this remote source will be skipped.")
            logger.error("Execution will continue with remaining available compute sources.")
            logger.error("To avoid this in the future:")
        else:
            logger.error(
                "Please ensure local and remote projects are synchronized before running sweep."
            )
            logger.error("Suggestions:")

        logger.error("  - Commit and push your local changes")
        logger.error("  - Pull latest changes on remote machine")
        logger.error("  - Or use --no-verify-sync to skip this check (not recommended)")
        logger.error("  - Or use --auto-sync to automatically sync mismatched files")

        # Provide specific guidance based on the type of sync failure
        if sync_result["method"] == "checksum":
            details = sync_result["details"]
            essential_missing = details.get("essential_missing", [])
            if essential_missing:
                logger.error(f"Note: Essential files missing on remote: {essential_missing}")
                logger.error(
                    "These files are critical for training and must be present on all compute sources."
                )
            else:
                logger.error(
                    "Note: Only file content differences detected - synchronization should resolve this."
                )
        elif sync_result["method"] == "git":
            details = sync_result["details"]
            if details.get("local_commit") != details.get("remote_commit"):
                logger.error(
                    "Note: Git commit differences require explicit push/pull to synchronize."
                )
            else:
                logger.error(
                    "Note: Only uncommitted changes detected - git sync or commit should resolve this."
                )

    # Note: Sync-related methods moved to ProjectStateChecker in project_sync.py

    def _is_config_file(self, filename: str) -> bool:
        """Check if a file is a configuration file (helper using ProjectStateChecker)."""
        # Create temporary checker to access helper method
        from .project_sync import ProjectStateChecker

        temp_checker = ProjectStateChecker(str(self.local_sweep_dir.parent), self.remote_config)
        return temp_checker._is_config_file(filename)
