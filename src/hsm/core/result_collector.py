"""Result collection manager for gathering outputs from distributed sweep execution.

This module provides functionality to collect results, logs, and error information
from remote compute sources after task execution, including immediate error
collection for debugging and comprehensive result aggregation.
"""

import asyncio
from datetime import datetime
import logging
from pathlib import Path
import subprocess
import tempfile
from typing import Any, Dict, List, Optional, Union

try:
    import asyncssh

    ASYNCSSH_AVAILABLE = True
except ImportError:
    ASYNCSSH_AVAILABLE = False

logger = logging.getLogger(__name__)


class ResultCollectionError(Exception):
    """Raised when result collection fails."""

    pass


class RemoteResultCollector:
    """Collects results from remote compute sources."""

    def __init__(
        self,
        local_sweep_dir: Path,
        remote_host: str,
        ssh_key: Optional[str] = None,
        ssh_port: int = 22,
    ):
        """Initialize the result collector.

        Args:
            local_sweep_dir: Local sweep directory for storing results
            remote_host: Remote host address
            ssh_key: Optional SSH key path
            ssh_port: SSH port (default 22)
        """
        self.local_sweep_dir = Path(local_sweep_dir)
        self.remote_host = remote_host
        self.ssh_key = ssh_key
        self.ssh_port = ssh_port

    async def collect_results(
        self,
        remote_sweep_dir: str,
        job_ids: Optional[List[str]] = None,
        cleanup_after_sync: bool = True,
    ) -> bool:
        """Collect results from remote machine back to local.

        Args:
            remote_sweep_dir: Path to remote sweep directory
            job_ids: Specific job IDs to collect, or None for all
            cleanup_after_sync: Whether to clean up remote sweep directory after successful sync

        Returns:
            True if collection successful, False otherwise
        """
        logger.info(f"Collecting results from remote sweep directory: {remote_sweep_dir}")

        try:
            # Create local remote tasks directory
            local_remote_tasks = self.local_sweep_dir / "tasks" / f"remote_{self.remote_host}"
            local_remote_tasks.mkdir(parents=True, exist_ok=True)

            success_count = 0

            # Use rsync to collect all task directories
            remote_tasks_dir = f"{remote_sweep_dir}/tasks"
            rsync_cmd = (
                f"rsync -avz --compress-level=6 "
                f"{self.remote_host}:{remote_tasks_dir}/ "
                f"{local_remote_tasks}/"
            )

            logger.debug(f"Result collection rsync: {rsync_cmd}")

            result = subprocess.run(rsync_cmd, shell=True, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info(f"✓ Results collected successfully from {self.remote_host}")
                success_count += 1

                # Also collect error summaries if they exist
                await self._collect_error_summaries(remote_sweep_dir)

                # Show summary of what was collected
                self._show_collection_summary(local_remote_tasks)

                # Collect logs and reports
                await self._collect_logs_and_reports(remote_sweep_dir)

                # Clean up remote sweep directory if requested and sync was successful
                if cleanup_after_sync:
                    await self._cleanup_remote_environment(remote_sweep_dir)

            else:
                logger.error(f"Result collection failed: {result.stderr}")

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
            logger.error(f"Error collecting results from {self.remote_host}: {e}")
            return False

    async def _collect_error_summaries(self, remote_sweep_dir: str):
        """Collect error summary files from remote to local sweep directory."""
        try:
            if not ASYNCSSH_AVAILABLE:
                logger.warning("asyncssh not available, skipping error summary collection")
                return

            async with await self._create_ssh_connection() as conn:
                # Check if there are error summary files on remote
                remote_error_dir = f"{remote_sweep_dir}/errors"
                check_cmd = f"test -d {remote_error_dir} && ls {remote_error_dir}/*.txt 2>/dev/null || echo 'no_errors'"

                result = await conn.run(check_cmd, check=False)

                if result.returncode == 0 and "no_errors" not in result.stdout:
                    # Error files exist, sync them
                    local_error_dir = self.local_sweep_dir / "errors"
                    local_error_dir.mkdir(exist_ok=True)

                    rsync_cmd = (
                        f"rsync -avz --compress-level=6 "
                        f"{self.remote_host}:{remote_error_dir}/ "
                        f"{local_error_dir}/"
                    )

                    error_result = subprocess.run(
                        rsync_cmd, shell=True, capture_output=True, text=True
                    )

                    if error_result.returncode == 0:
                        logger.info(f"📁 Collected error summaries from {self.remote_host}")
                    else:
                        logger.warning(f"Failed to collect error summaries: {error_result.stderr}")
                else:
                    logger.debug("No error summary files found on remote")

        except Exception as e:
            logger.warning(f"Error collecting error summaries: {e}")

    async def _collect_logs_and_reports(self, remote_sweep_dir: str):
        """Collect logs and reports from remote sweep directory."""
        try:
            # Collect logs directory
            local_logs_dir = self.local_sweep_dir / "logs"
            local_logs_dir.mkdir(exist_ok=True)

            remote_logs_dir = f"{remote_sweep_dir}/logs"
            logs_rsync_cmd = (
                f"rsync -avz --compress-level=6 "
                f"{self.remote_host}:{remote_logs_dir}/ "
                f"{local_logs_dir}/"
            )

            logs_result = subprocess.run(logs_rsync_cmd, shell=True, capture_output=True, text=True)

            if logs_result.returncode == 0:
                logger.debug(f"📁 Collected logs from {self.remote_host}")
            else:
                logger.debug(f"No logs to collect or collection failed: {logs_result.stderr}")

            # Collect reports directory
            local_reports_dir = self.local_sweep_dir / "reports"
            local_reports_dir.mkdir(exist_ok=True)

            remote_reports_dir = f"{remote_sweep_dir}/reports"
            reports_rsync_cmd = (
                f"rsync -avz --compress-level=6 "
                f"{self.remote_host}:{remote_reports_dir}/ "
                f"{local_reports_dir}/"
            )

            reports_result = subprocess.run(
                reports_rsync_cmd, shell=True, capture_output=True, text=True
            )

            if reports_result.returncode == 0:
                logger.debug(f"📁 Collected reports from {self.remote_host}")
            else:
                logger.debug(f"No reports to collect or collection failed: {reports_result.stderr}")

        except Exception as e:
            logger.warning(f"Error collecting logs and reports: {e}")

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
            cancelled_count = 0

            for task_dir in task_dirs:
                task_info_file = task_dir / "task_info.txt"
                if task_info_file.exists():
                    try:
                        with open(task_info_file, "r") as f:
                            content = f.read()
                            if "Status: COMPLETED" in content:
                                completed_count += 1
                            elif "Status: FAILED" in content or "FAILED" in content:
                                failed_count += 1
                            elif "Status: CANCELLED" in content:
                                cancelled_count += 1
                    except Exception:
                        pass

            logger.info(f"📁 Collected {len(task_dirs)} task directories from remote:")
            logger.info(f"   • ✓ {completed_count} completed")
            logger.info(f"   • ✗ {failed_count} failed")
            logger.info(f"   • ⚠ {cancelled_count} cancelled")
            unknown_count = len(task_dirs) - completed_count - failed_count - cancelled_count
            if unknown_count > 0:
                logger.info(f"   • ? {unknown_count} unknown status")

        except Exception as e:
            logger.debug(f"Error showing collection summary: {e}")

    async def _cleanup_remote_environment(self, remote_sweep_dir: str):
        """Clean up remote sweep directory after successful result collection."""
        logger.info(f"Cleaning up remote sweep directory on {self.remote_host}")

        try:
            if not ASYNCSSH_AVAILABLE:
                logger.warning("asyncssh not available, skipping remote cleanup")
                return

            async with await self._create_ssh_connection() as conn:
                # Remove remote sweep directory
                await conn.run(f"rm -rf {remote_sweep_dir}")
                logger.info(f"✓ Cleaned up {remote_sweep_dir} on {self.remote_host}")

        except Exception as e:
            logger.warning(f"Error cleaning up remote environment: {e}")
            logger.warning(f"You may need to manually remove: {remote_sweep_dir}")

    async def _create_ssh_connection(self):
        """Create SSH connection to remote host."""
        options = {}
        if self.ssh_key:
            options["client_keys"] = [self.ssh_key]

        return await asyncssh.connect(self.remote_host, port=self.ssh_port, **options)

    async def immediately_collect_failed_task(self, conn, remote_task_dir: str, job_name: str):
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
                f"{self.remote_host}:{remote_task_dir}/ "
                f"{local_task_dir}/"
            )

            logger.debug(f"Immediate failed task sync: {rsync_cmd}")

            result = subprocess.run(rsync_cmd, shell=True, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info(f"📁 Immediately synced failed task {job_name} to {local_task_dir}")
            else:
                logger.warning(f"Failed to sync failed task {job_name}: {result.stderr}")

        except Exception as e:
            logger.warning(f"Error syncing failed task {job_name}: {e}")

    async def collect_and_analyze_task_error(
        self, conn, remote_task_dir: str, job_name: str
    ) -> Dict[str, Any]:
        """Collect and analyze error information from a failed task."""
        error_info = {}

        try:
            # First check if there's an error flag (created by improved scripts)
            error_flag_cmd = f"cat {remote_task_dir}/error_flag.txt 2>/dev/null || echo 'no_flag'"
            error_flag_result = await conn.run(error_flag_cmd, check=False)
            if error_flag_result.returncode == 0 and "no_flag" not in error_flag_result.stdout:
                error_info["error_flag"] = error_flag_result.stdout.strip()

            # Get exit code
            exit_code_cmd = f"cat {remote_task_dir}/exit_code.txt 2>/dev/null || echo 'unknown'"
            exit_code_result = await conn.run(exit_code_cmd, check=False)
            if exit_code_result.returncode == 0 and exit_code_result.stdout.strip() != "unknown":
                error_info["exit_code"] = exit_code_result.stdout.strip()

            # Check for stderr output
            stderr_cmd = f"cat {remote_task_dir}/stderr.log 2>/dev/null || echo 'no_stderr'"
            stderr_result = await conn.run(stderr_cmd, check=False)
            if stderr_result.returncode == 0 and "no_stderr" not in stderr_result.stdout:
                stderr_content = stderr_result.stdout.strip()
                error_info["stderr"] = (
                    stderr_content[-3000:] if len(stderr_content) > 3000 else stderr_content
                )

            # Check for captured error logs
            error_logs_cmd = (
                f"cat {remote_task_dir}/error_logs.txt 2>/dev/null || echo 'no_error_logs'"
            )
            error_logs_result = await conn.run(error_logs_cmd, check=False)
            if (
                error_logs_result.returncode == 0
                and "no_error_logs" not in error_logs_result.stdout
            ):
                error_info["captured_logs"] = error_logs_result.stdout.strip()

            # Check for system info at failure
            error_system_cmd = (
                f"cat {remote_task_dir}/error_system.txt 2>/dev/null || echo 'no_system_info'"
            )
            error_system_result = await conn.run(error_system_cmd, check=False)
            if (
                error_system_result.returncode == 0
                and "no_system_info" not in error_system_result.stdout
            ):
                error_info["system_info"] = error_system_result.stdout.strip()

            # Look for error patterns in any log files
            if not error_info.get("stderr"):
                stderr_find_cmd = (
                    f"find {remote_task_dir} -name '*.err' -o -name 'stderr*' | head -1"
                )
                stderr_find_result = await conn.run(stderr_find_cmd, check=False)

                if stderr_find_result.returncode == 0 and stderr_find_result.stdout.strip():
                    stderr_file = stderr_find_result.stdout.strip()
                    stderr_content = await conn.run(f"tail -30 {stderr_file}", check=False)
                    if stderr_content.returncode == 0 and stderr_content.stdout.strip():
                        error_info["stderr"] = stderr_content.stdout.strip()

            # Check for log files with error patterns
            log_cmd = f"find {remote_task_dir} -name '*.log' -not -name 'stderr.log' | head -1"
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
            task_info_cmd = (
                f"cat {remote_task_dir}/task_info.txt 2>/dev/null || echo 'no_task_info'"
            )
            task_info_result = await conn.run(task_info_cmd, check=False)
            if task_info_result.returncode == 0 and "no_task_info" not in task_info_result.stdout:
                error_info["task_info"] = task_info_result.stdout.strip()

            # Check for disk space issues
            disk_check_cmd = f"df -h {remote_task_dir} | tail -1"
            disk_result = await conn.run(disk_check_cmd, check=False)
            if disk_result.returncode == 0:
                error_info["disk_info"] = disk_result.stdout.strip()

            # Analyze for specific error types
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

    async def save_error_summary_locally(self, job_name: str, error_info: Dict[str, Any]):
        """Save error summary to local file for review."""
        try:
            error_dir = self.local_sweep_dir / "errors"
            error_dir.mkdir(exist_ok=True)

            error_file = error_dir / f"{job_name}_error.txt"

            with open(error_file, "w") as f:
                f.write(f"Error Summary for Job: {job_name}\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Collected at: {datetime.now()}\n")
                f.write(f"Remote machine: {self.remote_host}\n\n")

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


class LocalResultCollector:
    """Collects results from local compute sources."""

    def __init__(self, local_sweep_dir: Path):
        """Initialize the local result collector.

        Args:
            local_sweep_dir: Local sweep directory for storing results
        """
        self.local_sweep_dir = Path(local_sweep_dir)

    async def collect_results(self, source_name: str, task_dirs: List[Path]) -> bool:
        """Collect results from local compute source.

        Args:
            source_name: Name of the compute source
            task_dirs: List of task directories to collect

        Returns:
            True if collection successful, False otherwise
        """
        logger.info(f"Collecting results from local source: {source_name}")

        try:
            # Create local tasks directory for this source
            local_source_tasks = self.local_sweep_dir / "tasks" / f"local_{source_name}"
            local_source_tasks.mkdir(parents=True, exist_ok=True)

            success_count = 0

            for task_dir in task_dirs:
                if not task_dir.exists():
                    continue

                # Copy task directory to collection area
                task_name = task_dir.name
                dest_task_dir = local_source_tasks / task_name

                if dest_task_dir.exists():
                    # Remove existing directory first
                    import shutil

                    shutil.rmtree(dest_task_dir)

                # Copy the task directory
                import shutil

                shutil.copytree(task_dir, dest_task_dir)
                success_count += 1
                logger.debug(f"✓ Collected task: {task_name}")

            logger.info(f"✓ Collected {success_count} tasks from local source {source_name}")
            return success_count > 0

        except Exception as e:
            logger.error(f"Error collecting results from local source {source_name}: {e}")
            return False

    def analyze_local_task_error(self, task_dir: Path, job_name: str) -> Dict[str, Any]:
        """Analyze error information from a local failed task."""
        error_info = {}

        try:
            # Check for various error files
            error_files = {
                "exit_code": task_dir / "exit_code.txt",
                "stderr": task_dir / "stderr.log",
                "error_logs": task_dir / "error_logs.txt",
                "task_info": task_dir / "task_info.txt",
            }

            for info_type, file_path in error_files.items():
                if file_path.exists():
                    try:
                        with open(file_path, "r") as f:
                            content = f.read().strip()
                            if content:
                                error_info[info_type] = content
                    except Exception as e:
                        logger.debug(f"Error reading {file_path}: {e}")

            # Look for error patterns in log files
            log_files = list(task_dir.glob("*.log"))
            for log_file in log_files:
                if log_file.name == "stderr.log":
                    continue
                try:
                    with open(log_file, "r") as f:
                        content = f.read()
                        # Look for common error patterns
                        if any(
                            pattern in content.lower()
                            for pattern in ["error", "exception", "failed", "traceback"]
                        ):
                            error_info["log_errors"] = content[-2000:]  # Last 2000 chars
                            break
                except Exception:
                    continue

            # Analyze stderr for specific error types
            if error_info.get("stderr"):
                stderr_lower = error_info["stderr"].lower()
                if "gpuallocatorconfig" in stderr_lower:
                    error_info["error_type"] = "GPU Allocator Config Error"
                    error_info["suggestion"] = "JAX/CUDA compatibility issue"
                elif "importerror" in stderr_lower or "modulenotfounderror" in stderr_lower:
                    error_info["error_type"] = "Import Error"
                    error_info["suggestion"] = "Missing dependency or environment issue"

        except Exception as e:
            error_info["collection_error"] = str(e)

        return error_info

    async def save_local_error_summary(self, job_name: str, error_info: Dict[str, Any]):
        """Save error summary for a local failed task."""
        try:
            error_dir = self.local_sweep_dir / "errors"
            error_dir.mkdir(exist_ok=True)

            error_file = error_dir / f"{job_name}_local_error.txt"

            with open(error_file, "w") as f:
                f.write(f"Local Error Summary for Job: {job_name}\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Collected at: {datetime.now()}\n\n")

                if error_info.get("error_type"):
                    f.write("DIAGNOSIS:\n")
                    f.write("=" * 10 + "\n")
                    f.write(f"Error Type: {error_info['error_type']}\n")
                    f.write(f"Suggestion: {error_info.get('suggestion', 'Unknown')}\n\n")

                for key, value in error_info.items():
                    if key in ["error_type", "suggestion", "collection_error"]:
                        continue

                    f.write(f"{key.replace('_', ' ').title()}:\n")
                    f.write("-" * (len(key) + 2) + "\n")
                    f.write(str(value))
                    f.write("\n\n")

                f.write("=" * 50 + "\n")

            logger.info(f"📄 Local error summary saved: {error_file}")

        except Exception as e:
            logger.warning(f"Error saving local error summary for {job_name}: {e}")


class ResultCollectionManager:
    """Manages result collection from multiple compute sources."""

    def __init__(self, local_sweep_dir: Path):
        """Initialize the result collection manager.

        Args:
            local_sweep_dir: Local sweep directory for storing results
        """
        self.local_sweep_dir = Path(local_sweep_dir)
        self.collectors = {}

    def get_remote_collector(
        self, remote_host: str, ssh_key: Optional[str] = None, ssh_port: int = 22
    ) -> RemoteResultCollector:
        """Get or create a remote result collector for a host.

        Args:
            remote_host: Remote host address
            ssh_key: Optional SSH key path
            ssh_port: SSH port

        Returns:
            RemoteResultCollector instance
        """
        collector_key = f"remote_{remote_host}_{ssh_port}"
        if collector_key not in self.collectors:
            self.collectors[collector_key] = RemoteResultCollector(
                self.local_sweep_dir, remote_host, ssh_key, ssh_port
            )
        return self.collectors[collector_key]

    def get_local_collector(self) -> LocalResultCollector:
        """Get or create a local result collector.

        Returns:
            LocalResultCollector instance
        """
        if "local" not in self.collectors:
            self.collectors["local"] = LocalResultCollector(self.local_sweep_dir)
        return self.collectors["local"]

    async def collect_all_results(
        self, source_configs: Dict[str, Dict[str, Any]]
    ) -> Dict[str, bool]:
        """Collect results from all configured sources.

        Args:
            source_configs: Dictionary mapping source names to their configurations

        Returns:
            Dictionary mapping source names to collection success status
        """
        logger.info(f"Collecting results from {len(source_configs)} compute sources")

        results = {}
        collection_tasks = []

        for source_name, config in source_configs.items():
            if config.get("type") == "remote":
                collector = self.get_remote_collector(
                    config["host"], config.get("ssh_key"), config.get("ssh_port", 22)
                )
                task = asyncio.create_task(
                    collector.collect_results(
                        config["remote_sweep_dir"],
                        config.get("job_ids"),
                        config.get("cleanup_after_sync", True),
                    )
                )
                collection_tasks.append((source_name, task))
            elif config.get("type") == "local":
                collector = self.get_local_collector()
                task = asyncio.create_task(
                    collector.collect_results(source_name, config.get("task_dirs", []))
                )
                collection_tasks.append((source_name, task))

        # Wait for all collection tasks to complete
        for source_name, task in collection_tasks:
            try:
                result = await task
                results[source_name] = result
                if result:
                    logger.info(f"✓ Successfully collected results from {source_name}")
                else:
                    logger.warning(f"✗ Failed to collect results from {source_name}")
            except Exception as e:
                logger.error(f"✗ Error collecting results from {source_name}: {e}")
                results[source_name] = False

        success_count = sum(1 for success in results.values() if success)
        logger.info(
            f"Result collection complete: {success_count}/{len(source_configs)} sources successful"
        )

        return results
