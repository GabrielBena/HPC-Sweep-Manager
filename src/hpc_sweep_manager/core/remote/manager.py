"""Remote job manager for executing jobs on remote machines via SSH."""

import asyncio
from datetime import datetime
import hashlib
import logging
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
from typing import Any, Dict, List, Optional

try:
    import asyncssh

    ASYNCSSH_AVAILABLE = True
except ImportError:
    ASYNCSSH_AVAILABLE = False

from .discovery import RemoteConfig, create_ssh_connection, get_ssh_client_keys

logger = logging.getLogger(__name__)


class RemoteJobManager:
    """Manages jobs on remote machines via SSH."""

    def __init__(
        self,
        remote_config: RemoteConfig,
        local_sweep_dir: Path,
        max_parallel_jobs: int = 4,
        show_progress: bool = True,
    ):
        """
        Initialize remote job manager.

        Args:
            remote_config: Configuration for the remote machine
            local_sweep_dir: Local sweep directory for result collection
            max_parallel_jobs: Maximum number of concurrent jobs on remote
            show_progress: Whether to show progress updates
        """
        self.remote_config = remote_config
        self.local_sweep_dir = local_sweep_dir
        self.system_type = "remote"
        self.max_parallel_jobs = max_parallel_jobs
        self.show_progress = show_progress

        # Remote directories
        self.remote_sweep_dir = None
        self.remote_tasks_dir = None

        # Job tracking
        self.running_jobs = {}  # job_id -> job_info
        self.job_counter = 0
        self.total_jobs_planned = 0
        self.jobs_completed = 0

        # Signal handling state
        self._signal_received = False
        self._cleanup_in_progress = False

        if not ASYNCSSH_AVAILABLE:
            raise ImportError("asyncssh is required for remote job execution")

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
                    sync_result = await sync_checker.verify_project_sync(conn)

                    if not sync_result["in_sync"]:
                        logger.warning("Project synchronization verification detected differences.")

                        # Determine if we can offer sync (only for certain types of mismatches)
                        can_offer_sync = self._can_offer_sync(sync_result)

                        if can_offer_sync and auto_sync:
                            logger.info("Auto-syncing local changes to remote machine...")
                            sync_success = await self._sync_mismatched_files(conn, sync_result)
                            if sync_success:
                                logger.info("✓ Successfully synced local changes to remote")
                                sync_performed = True
                            else:
                                logger.error("Failed to sync changes to remote")
                                return False
                        elif can_offer_sync and interactive:
                            sync_choice = self._prompt_for_sync(sync_result)
                            if sync_choice is True:
                                logger.info("Syncing local changes to remote machine...")
                                sync_success = await self._sync_mismatched_files(conn, sync_result)
                                if sync_success:
                                    logger.info("✓ Successfully synced local changes to remote")
                                    sync_performed = True
                                else:
                                    logger.error("Failed to sync changes to remote")
                                    return False
                            elif sync_choice == "skip":
                                logger.warning("⚠ Skipping sync verification as requested by user")
                                # Continue without sync
                            else:
                                # User declined sync - show error and exit
                                self._show_sync_error_details(sync_result)
                                return False
                    else:
                        logger.info(
                            f"✓ Project synchronization verified ({sync_result['method']} method)"
                        )

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

        logger.info(f"Submitting job {job_name} to remote {self.remote_config.name}")

        try:
            async with await create_ssh_connection(
                self.remote_config.host,
                self.remote_config.ssh_key,
                self.remote_config.ssh_port,
            ) as conn:
                # Create task directory
                task_dir = f"{self.remote_tasks_dir}/task_{self.job_counter}"
                await conn.run(f"mkdir -p {task_dir}")

                # Create job script
                script_content = self._create_job_script(
                    params, job_name, task_dir, sweep_id, wandb_group
                )

                script_path = f"{self.remote_sweep_dir}/scripts/{job_name}.sh"

                # Write script to remote (using a temporary file approach)
                with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as tmp:
                    tmp.write(script_content)
                    tmp_path = tmp.name

                # Copy script to remote
                await asyncssh.scp(
                    tmp_path,
                    (self.remote_config.host, script_path),
                    client_keys=get_ssh_client_keys(self.remote_config.ssh_key)
                    if self.remote_config.ssh_key
                    else None,
                )

                # Clean up temporary file
                Path(tmp_path).unlink()

                # Make script executable
                await conn.run(f"chmod +x {script_path}")

                # Execute the job in background
                log_file = f"{self.remote_sweep_dir}/logs/{job_name}.log"
                err_file = f"{self.remote_sweep_dir}/logs/{job_name}.err"

                # Start job in background with proper PID tracking
                # The script will write its own process group ID to a PID file
                pid_file = f"{task_dir}/job.pid"
                cmd = f"nohup bash {script_path} > {log_file} 2> {err_file} & echo $! > {pid_file}"
                await conn.run(cmd)

                # Wait a moment for the PID file to be written
                await asyncio.sleep(0.5)

                # Read the PID from the file
                try:
                    pid_result = await conn.run(f"cat {pid_file}")
                    remote_pid = pid_result.stdout.strip()
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

                logger.info(f"Job {job_name} started on remote with PID {remote_pid}")
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

        script_content = f"""#!/bin/bash
# Remote job script for {job_name}
# Generated at {datetime.now()}
# Remote machine: {self.remote_config.name}

# Exit on any error to ensure we catch failures
set -e

# Create a new process group for this job
set -m

# Set up environment
cd {self.remote_config.project_root}

# Create task info file with initial status
echo "Job Name: {job_name}" > {task_dir}/task_info.txt
echo "Remote Machine: {self.remote_config.name}" >> {task_dir}/task_info.txt
echo "Task Directory: {task_dir}" >> {task_dir}/task_info.txt
echo "Start Time: $(date)" >> {task_dir}/task_info.txt
echo "Parameters: {params_str}" >> {task_dir}/task_info.txt
echo "Status: RUNNING" >> {task_dir}/task_info.txt

# Store the command for reference
echo "{self.remote_config.python_interpreter} {self.remote_config.train_script} {params_str} output.dir={task_dir} wandb.group={effective_wandb_group}" > {task_dir}/command.txt

# Write script PID and process group ID for cancellation
echo "$$" > {task_dir}/script.pid
echo "$(ps -o pgid= -p $$)" > {task_dir}/process_group.pid

# Function to update status safely
update_status() {{
    local status="$1"
    echo "Status: $status" >> {task_dir}/task_info.txt
    echo "End Time: $(date)" >> {task_dir}/task_info.txt
}}

# Function to handle cancellation signals
handle_cancellation() {{
    echo "Received cancellation signal at $(date)" >> {task_dir}/task_info.txt
    update_status "CANCELLED"
    
    # First, try to kill the Python process specifically
    if [ -f "{task_dir}/python.pid" ]; then
        local python_pid=$(cat {task_dir}/python.pid 2>/dev/null)
        if [ -n "$python_pid" ]; then
            echo "Terminating Python process $python_pid" >> {task_dir}/task_info.txt
            
            # Try graceful termination first
            if kill -TERM "$python_pid" 2>/dev/null; then
                echo "Sent SIGTERM to Python process $python_pid" >> {task_dir}/task_info.txt
                # Wait briefly for graceful shutdown
                for i in {{1..5}}; do
                    if ! kill -0 "$python_pid" 2>/dev/null; then
                        echo "Python process $python_pid terminated gracefully" >> {task_dir}/task_info.txt
                        break
                    fi
                    sleep 1
                done
                
                # Force kill if still alive
                if kill -0 "$python_pid" 2>/dev/null; then
                    echo "Force killing Python process $python_pid" >> {task_dir}/task_info.txt
                    kill -KILL "$python_pid" 2>/dev/null || true
                fi
            else
                echo "Failed to send SIGTERM to Python process $python_pid" >> {task_dir}/task_info.txt
            fi
        fi
    fi
    
    # Also kill the entire process group as backup
    local pgid=$(cat {task_dir}/process_group.pid 2>/dev/null || echo "$$")
    echo "Cleaning up process group $pgid" >> {task_dir}/task_info.txt
    
    # Kill the entire process group
    kill -TERM -$pgid 2>/dev/null || true
    sleep 1
    kill -KILL -$pgid 2>/dev/null || true
    
    echo "Cancellation cleanup completed at $(date)" >> {task_dir}/task_info.txt
    exit 143  # Standard exit code for SIGTERM
}}

# Set up signal handlers for graceful cancellation
trap 'handle_cancellation' TERM INT

# Start the Python training process in the background and capture its PID
echo "Starting training script at $(date)" >> {task_dir}/task_info.txt
{self.remote_config.python_interpreter} {self.remote_config.train_script} {params_str} output.dir={task_dir} wandb.group={effective_wandb_group} &

# Store the Python process PID
PYTHON_PID=$!
echo "$PYTHON_PID" > {task_dir}/python.pid
echo "Python process PID: $PYTHON_PID" >> {task_dir}/task_info.txt

# Wait for the Python process to complete
if wait $PYTHON_PID; then
    # Training completed successfully
    update_status "COMPLETED"
    echo "Training script completed successfully at $(date)" >> {task_dir}/task_info.txt
    exit 0
else
    # Training failed or was interrupted
    exit_code=$?
    if [ $exit_code -eq 143 ] || [ $exit_code -eq 130 ]; then
        # Process was terminated or interrupted
        update_status "CANCELLED"
        echo "Training script was cancelled at $(date)" >> {task_dir}/task_info.txt
    else
        # Process failed
        update_status "FAILED"
        echo "Training script failed at $(date) with exit code $exit_code" >> {task_dir}/task_info.txt
    fi
    exit $exit_code
fi
"""
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

            # Use rsync to collect all task directories
            if self.remote_tasks_dir:
                rsync_cmd = (
                    f"rsync -avz --compress-level=6 "
                    f"{self.remote_config.host}:{self.remote_tasks_dir}/ "
                    f"{local_remote_tasks}/"
                )

                logger.debug(f"Result collection rsync: {rsync_cmd}")

                import subprocess

                result = subprocess.run(rsync_cmd, shell=True, capture_output=True, text=True)

                if result.returncode == 0:
                    logger.info(f"Results collected successfully from {self.remote_config.name}")

                    # Clean up remote sweep directory if requested and sync was successful
                    if cleanup_after_sync:
                        await self.cleanup_remote_environment()

                    return True
                else:
                    logger.error(f"Result collection failed: {result.stderr}")
                    return False
            else:
                logger.warning("Remote tasks directory not set, cannot collect results")
                return False

        except Exception as e:
            logger.error(f"Error collecting results from {self.remote_config.name}: {e}")
            return False

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

        if self.show_progress:
            logger.info(
                f"Starting {len(param_combinations)} jobs with max {self.max_parallel_jobs} parallel on {self.remote_config.name}"
            )

        job_ids = []
        for i, params in enumerate(param_combinations):
            job_name = f"{sweep_id}_task_{i + 1:03d}"

            if self.show_progress:
                logger.info(f"Submitting job {i + 1}/{len(param_combinations)}: {job_name}")

            job_id = await self.submit_single_job(params, job_name, sweep_id, wandb_group)
            job_ids.append(job_id)

            # If we're limiting parallel jobs, wait for some to complete
            if len(self.running_jobs) >= self.max_parallel_jobs:
                if self.show_progress:
                    logger.info(
                        f"Reached max parallel jobs ({self.max_parallel_jobs}), waiting for completion..."
                    )
                await self._wait_for_job_completion()

        if self.show_progress:
            logger.info(f"All {len(param_combinations)} jobs submitted")

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

                        if self.show_progress:
                            job_name = job_info.get("job_name", job_id)
                            logger.info(
                                f"Job {job_name} {status} ({self.jobs_completed}/{self.total_jobs_planned})"
                            )

            # Remove completed jobs
            for job_id in completed_jobs:
                del self.running_jobs[job_id]

            if not completed_jobs and self.running_jobs:
                # Show periodic progress summary
                if self.show_progress:
                    await self._show_progress_summary()
                # Sleep briefly before checking again
                await asyncio.sleep(5)

        if self.show_progress:
            logger.info(f"All {self.total_jobs_planned} jobs completed!")

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

    def _can_offer_sync(self, sync_result: Dict[str, Any]) -> bool:
        """Determine if we can safely offer to sync based on the mismatch type."""
        # Only offer sync for checksum mismatches (not git commit differences)
        if sync_result["method"] == "git":
            details = sync_result["details"]
            # Don't offer sync if commits are different (too risky)
            if details.get("local_commit") != details.get("remote_commit"):
                return False
            # Only offer if there are just uncommitted changes
            return bool(details.get("local_status") and not details.get("remote_status"))
        elif sync_result["method"] == "checksum":
            details = sync_result["details"]
            # Offer sync if we have specific file mismatches (not missing files)
            return bool(details.get("mismatched_files")) and not details.get("missing_files")

        return False

    def _prompt_for_sync(self, sync_result: Dict[str, Any]) -> bool:
        """Prompt user whether to sync local changes to remote."""
        print("\n" + "=" * 60)
        print("🔄 PROJECT SYNC REQUIRED")
        print("=" * 60)

        if sync_result["method"] == "git":
            details = sync_result["details"]
            if details.get("local_status"):
                print("Local uncommitted changes detected:")
                for line in details["local_status"].split("\n"):
                    if line.strip():
                        print(f"  {line}")
        elif sync_result["method"] == "checksum":
            details = sync_result["details"]
            if details.get("mismatched_files"):
                print("Files that differ between local and remote:")
                for mismatch in details["mismatched_files"]:
                    print(f"  📄 {mismatch['file']}")

        print("\nOptions:")
        print("  ✅ [Y] Sync local changes to remote (recommended)")
        print("  ❌ [n] Cancel sweep execution")
        print("  ⚠️  [s] Skip sync verification (not recommended)")

        while True:
            try:
                choice = input("\nChoose [Y/n/s]: ").strip().lower()
                if choice in ["", "y", "yes"]:
                    return True
                elif choice in ["n", "no"]:
                    return False
                elif choice in ["s", "skip"]:
                    logger.warning("User chose to skip sync verification")
                    return "skip"
                else:
                    print("Please enter Y, n, or s")
            except (EOFError, KeyboardInterrupt):
                print("\nOperation cancelled by user")
                return False

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

                logger.info(f"Successfully synced {len(mismatched_files)} files")
                return True

            elif sync_result["method"] == "git":
                # For git mismatches, we could add specific git operations here
                # For now, just handle uncommitted changes by syncing them
                logger.info("Syncing uncommitted changes...")
                # This is more complex and would require careful git operations
                # For safety, we'll fall back to manual instructions
                return False

            return False

        except Exception as e:
            logger.error(f"Error during file sync: {e}")
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

        logger.error(
            "Please ensure local and remote projects are synchronized before running sweep."
        )
        logger.error("Suggestions:")
        logger.error("  - Commit and push your local changes")
        logger.error("  - Pull latest changes on remote machine")
        logger.error("  - Or use --no-verify-sync to skip this check (not recommended)")
        logger.error("  - Or use --auto-sync to automatically sync mismatched files")


class ProjectStateChecker:
    """Checks if local and remote projects are in the same state."""

    def __init__(self, local_project_root: str, remote_config: RemoteConfig):
        self.local_project_root = Path(local_project_root)
        self.remote_config = remote_config

    async def verify_project_sync(self, conn) -> Dict[str, Any]:
        """
        Verify that local and remote projects are in sync.

        Returns:
            Dict with verification results and details
        """
        logger.info("Verifying local and remote project synchronization...")

        result = {
            "in_sync": False,
            "method": None,
            "details": {},
            "warnings": [],
            "errors": [],
        }

        # Try git-based verification first
        git_result = await self._verify_git_sync(conn)
        if git_result["available"]:
            result.update(
                {
                    "in_sync": git_result["in_sync"],
                    "method": "git",
                    "details": git_result,
                }
            )

            if git_result["in_sync"]:
                logger.info("✓ Projects are in sync (git verification)")
                return result
            else:
                logger.warning("⚠ Projects are NOT in sync (git verification)")
                for warning in git_result.get("warnings", []):
                    logger.warning(f"  {warning}")
                for error in git_result.get("errors", []):
                    logger.error(f"  {error}")

        # Fall back to file checksum verification
        logger.info("Falling back to file checksum verification...")
        checksum_result = await self._verify_checksum_sync(conn)
        result.update(
            {
                "in_sync": checksum_result["in_sync"],
                "method": "checksum",
                "details": checksum_result,
            }
        )

        if checksum_result["in_sync"]:
            logger.info("✓ Projects appear to be in sync (checksum verification)")
        else:
            logger.warning("⚠ Projects may not be in sync (checksum verification)")
            for warning in checksum_result.get("warnings", []):
                logger.warning(f"  {warning}")

        return result

    async def _verify_git_sync(self, conn) -> Dict[str, Any]:
        """Verify using git status and commit hashes."""
        result = {
            "available": False,
            "in_sync": False,
            "local_commit": None,
            "remote_commit": None,
            "local_status": None,
            "remote_status": None,
            "warnings": [],
            "errors": [],
        }

        try:
            # Check if local project is a git repo
            local_git_result = subprocess.run(
                ["git", "rev-parse", "--git-dir"],
                cwd=self.local_project_root,
                capture_output=True,
                text=True,
                timeout=10,
            )

            if local_git_result.returncode != 0:
                logger.debug("Local project is not a git repository")
                return result

            # Check if remote project is a git repo
            remote_git_check = await conn.run(
                f"cd {self.remote_config.project_root} && git rev-parse --git-dir",
                check=False,
            )

            if remote_git_check.returncode != 0:
                logger.debug("Remote project is not a git repository")
                return result

            result["available"] = True

            # Get local git status
            local_status = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=self.local_project_root,
                capture_output=True,
                text=True,
                timeout=10,
            )
            result["local_status"] = local_status.stdout.strip()

            # Get local commit hash
            local_commit = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=self.local_project_root,
                capture_output=True,
                text=True,
                timeout=10,
            )
            result["local_commit"] = local_commit.stdout.strip()

            # Get remote git status
            remote_status = await conn.run(
                f"cd {self.remote_config.project_root} && git status --porcelain"
            )
            result["remote_status"] = remote_status.stdout.strip()

            # Get remote commit hash
            remote_commit = await conn.run(
                f"cd {self.remote_config.project_root} && git rev-parse HEAD"
            )
            result["remote_commit"] = remote_commit.stdout.strip()

            # Check for uncommitted changes
            if result["local_status"]:
                result["warnings"].append("Local repository has uncommitted changes")

            if result["remote_status"]:
                result["warnings"].append("Remote repository has uncommitted changes")

            # Check if commits match
            if result["local_commit"] != result["remote_commit"]:
                result["errors"].append(
                    f"Git commits differ: local={result['local_commit'][:8]} vs remote={result['remote_commit'][:8]}"
                )

            # Projects are in sync if commits match and no uncommitted changes
            result["in_sync"] = (
                result["local_commit"] == result["remote_commit"]
                and not result["local_status"]
                and not result["remote_status"]
            )

        except subprocess.TimeoutExpired:
            result["errors"].append("Git command timeout")
        except Exception as e:
            result["errors"].append(f"Git verification failed: {e}")

        return result

    async def _verify_checksum_sync(self, conn) -> Dict[str, Any]:
        """Verify using file checksums of key project files."""
        result = {
            "in_sync": True,
            "checked_files": [],
            "mismatched_files": [],
            "missing_files": [],
            "warnings": [],
        }

        # Key files to check (relative to project root)
        key_files = [
            self.remote_config.train_script,
            "configs/config.yaml",
            "requirements.txt",
            "pyproject.toml",
            "setup.py",
        ]

        # Add discovered config directory files if available
        if self.remote_config.config_dir:
            # Try to find config files
            try:
                config_base = Path(self.remote_config.config_dir).name
                key_files.extend(
                    [
                        f"{config_base}/config.yaml",
                        f"{config_base}/model/self_attention.yaml",
                        f"{config_base}/model/gnn.yaml",
                    ]
                )
            except:
                pass

        for file_path in key_files:
            if not file_path:
                continue

            try:
                # Calculate local file checksum
                local_file = self.local_project_root / file_path
                if not local_file.exists():
                    continue

                local_checksum = self._calculate_file_checksum(local_file)

                # Calculate remote file checksum
                remote_checksum_cmd = f"""
                cd {self.remote_config.project_root}
                if [ -f "{file_path}" ]; then
                    sha256sum "{file_path}" | cut -d' ' -f1
                else
                    echo "FILE_NOT_FOUND"
                fi
                """

                remote_result = await conn.run(remote_checksum_cmd)
                remote_checksum = remote_result.stdout.strip()

                if remote_checksum == "FILE_NOT_FOUND":
                    result["missing_files"].append(file_path)
                    result["in_sync"] = False
                elif local_checksum != remote_checksum:
                    result["mismatched_files"].append(
                        {
                            "file": file_path,
                            "local_checksum": local_checksum,
                            "remote_checksum": remote_checksum,
                        }
                    )
                    result["in_sync"] = False
                else:
                    result["checked_files"].append(file_path)

            except Exception as e:
                result["warnings"].append(f"Could not verify {file_path}: {e}")

        if result["missing_files"]:
            result["warnings"].append(f"Missing files on remote: {result['missing_files']}")

        if result["mismatched_files"]:
            result["warnings"].append(
                f"Mismatched files: {[f['file'] for f in result['mismatched_files']]}"
            )

        return result

    def _calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
