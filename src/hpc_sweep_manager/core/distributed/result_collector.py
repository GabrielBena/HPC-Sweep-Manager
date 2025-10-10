"""Result collection and normalization component for distributed sweeps."""

import asyncio
from datetime import datetime
import logging
from pathlib import Path
import shutil
from typing import Dict, List, Optional

from ..common.compute_source import ComputeSource
from ..remote.ssh_compute_source import SSHComputeSource

logger = logging.getLogger(__name__)


class ResultCollector:
    """Handles result collection and normalization for distributed sweeps."""

    def __init__(self, sweep_dir: Path, sources: List[ComputeSource], collect_interval: int = 300):
        self.sweep_dir = sweep_dir
        self.sources = sources
        self.collect_interval = collect_interval

        # Control flags
        self._running = False
        self._collection_task = None

        # Task completion tracking
        self.completed_tasks: Dict[str, Dict] = {}  # task_name -> completion_info

    async def start_continuous_collection(self):
        """Start continuous result collection from remote sources."""
        if self._running:
            logger.warning("Result collection already running")
            return

        self._running = True
        self._collection_task = asyncio.create_task(self._collect_results_continuously())
        logger.info("Started continuous result collection")

    async def stop_collection(self):
        """Stop continuous result collection."""
        self._running = False

        if self._collection_task and not self._collection_task.done():
            self._collection_task.cancel()
            try:
                await self._collection_task
            except asyncio.CancelledError:
                pass

        logger.info("Stopped result collection")

    async def _collect_results_continuously(self):
        """Background task to continuously collect results from remote sources."""
        logger.debug("Started continuous result collection task")

        try:
            while self._running:
                try:
                    # Collect results from all SSH sources
                    ssh_sources = [s for s in self.sources if isinstance(s, SSHComputeSource)]

                    if ssh_sources:
                        collection_tasks = []
                        for source in ssh_sources:
                            # Collect results for completed jobs
                            completed_job_ids = [
                                job_id
                                for job_id, job_info in source.completed_jobs.items()
                                if job_info.status in ["COMPLETED", "FAILED"]
                            ]

                            if completed_job_ids:
                                collection_tasks.append(source.collect_results(completed_job_ids))

                        if collection_tasks:
                            await asyncio.gather(*collection_tasks, return_exceptions=True)

                    await asyncio.sleep(self.collect_interval)

                except Exception as e:
                    logger.error(f"Error in result collection loop: {e}")
                    await asyncio.sleep(10)  # Shorter sleep on error

        except Exception as e:
            logger.error(f"Critical error in result collection: {e}")
        finally:
            logger.debug("Result collection task completed")

    async def normalize_results_structure(
        self,
        task_to_source: Dict[str, str],
        is_completion_run: bool = False,
        preserve_existing_mapping: bool = False,
    ):
        """
        Normalize results to a unified structure: task_001/, task_002/, etc.
        with proper handling of completion runs and directory overrides.
        """
        logger.info("Normalizing results to unified structure...")

        try:
            # Before normalizing, do a final collection attempt to catch any missed results
            # This is especially important for completion runs where timing might be an issue
            await self._final_collection_attempt(task_to_source)

            unified_tasks_dir = self.sweep_dir / "tasks"
            unified_tasks_dir.mkdir(exist_ok=True)

            moved_count = 0
            backup_count = 0
            override_count = 0

            # Process each task and move results to the correct location
            for task_name, source_name in task_to_source.items():
                success = await self._process_task_result(
                    task_name, source_name, unified_tasks_dir, is_completion_run
                )

                if success == "moved":
                    moved_count += 1
                elif success == "overridden":
                    override_count += 1
                elif success == "backed_up":
                    backup_count += 1

            # Clean up any empty remote directories
            cleaned_dirs = await self._cleanup_empty_remote_dirs(unified_tasks_dir)

            logger.info(
                f"✓ Results normalization completed: {moved_count} moved, {override_count} overridden, "
                f"{backup_count} backed up, {cleaned_dirs} empty dirs removed"
            )

        except Exception as e:
            logger.error(f"Error normalizing results structure: {e}")
            # Don't raise - this shouldn't fail the entire sweep

    async def _final_collection_attempt(self, task_to_source: Dict[str, str]):
        """
        Perform a final collection attempt to catch any results that may have been missed
        by the continuous collection process.
        """
        logger.info("Performing final collection attempt for any missed results...")

        try:
            # Group tasks by source
            source_tasks = {}
            for task_name, source_name in task_to_source.items():
                if source_name not in source_tasks:
                    source_tasks[source_name] = []
                source_tasks[source_name].append(task_name)

            # For each remote source, attempt final collection
            ssh_sources = [s for s in self.sources if isinstance(s, SSHComputeSource)]

            for source in ssh_sources:
                if source.name in source_tasks:
                    logger.info(f"Final collection attempt for source {source.name}")

                    # Get all job IDs for this source
                    source_job_ids = []
                    for job_id, job_info in source.completed_jobs.items():
                        if any(task in job_info.job_name for task in source_tasks[source.name]):
                            source_job_ids.append(job_id)

                    if source_job_ids:
                        logger.info(
                            f"Attempting final collection of {len(source_job_ids)} jobs from {source.name}"
                        )
                        try:
                            success = await source.collect_results(source_job_ids)
                            if success:
                                logger.info(f"✓ Final collection successful for {source.name}")
                            else:
                                logger.warning(f"⚠ Final collection failed for {source.name}")
                        except Exception as e:
                            logger.warning(f"Error in final collection for {source.name}: {e}")
                    else:
                        logger.debug(f"No job IDs found for final collection from {source.name}")

        except Exception as e:
            logger.warning(f"Error in final collection attempt: {e}")
            # Don't raise - this is just a fallback attempt

    async def _process_task_result(
        self, task_name: str, source_name: str, unified_tasks_dir: Path, is_completion_run: bool
    ) -> Optional[str]:
        """Process a single task result and handle directory conflicts."""
        try:
            source_results_dir = await self._find_source_results_dir(
                task_name, source_name, unified_tasks_dir
            )

            if not source_results_dir:
                logger.warning(
                    f"No source directory found for task {task_name} from source {source_name}"
                )
                return None

            target_dir = unified_tasks_dir / task_name

            if source_results_dir == target_dir:
                logger.debug(f"Task {task_name} already in correct location: {target_dir}")
                return None

            if not target_dir.exists():
                # Simple move - no conflict
                shutil.move(str(source_results_dir), str(target_dir))
                logger.info(f"Moved {source_name}:{task_name} -> {task_name}")
                return "moved"

            # Target directory already exists - handle based on completion run status
            if is_completion_run:
                override_decision = await self._should_override_existing_task(
                    source_results_dir, target_dir, task_name, source_name
                )

                if override_decision == "override":
                    await self._backup_and_override_task(
                        source_results_dir, target_dir, task_name, source_name
                    )
                    return "overridden"
                elif override_decision == "backup_and_override":
                    await self._backup_and_override_task(
                        source_results_dir, target_dir, task_name, source_name, create_backup=True
                    )
                    return "backed_up"
                else:
                    logger.info(
                        f"Keeping existing {task_name} results (new results from {source_name} not better)"
                    )
                    # Clean up the source directory since we're not using it
                    if source_results_dir.exists():
                        shutil.rmtree(source_results_dir)
                    return None
            else:
                logger.warning(
                    f"Target directory {task_name} already exists, cannot move from {source_results_dir}"
                )
                return None

        except Exception as e:
            logger.error(f"Error processing task result for {task_name}: {e}")
            return None

    async def _find_source_results_dir(
        self, task_name: str, source_name: str, unified_tasks_dir: Path
    ) -> Optional[Path]:
        """Find the source directory for a task result."""
        potential_dirs = []

        if source_name == "local":
            # Local results are typically in tasks/ already with correct naming
            potential_dirs = [
                unified_tasks_dir / task_name,
                unified_tasks_dir / f"local_{task_name}",
            ]
        else:
            # Remote results are in tasks/remote_{source_name}/
            remote_base_dir = unified_tasks_dir / f"remote_{source_name}"
            potential_dirs = [
                remote_base_dir / task_name,
                remote_base_dir / f"{task_name}",
                unified_tasks_dir / f"remote_{source_name}_{task_name}",
            ]

        # Find the source directory
        for potential_dir in potential_dirs:
            if potential_dir.exists():
                logger.debug(f"Found source directory for {task_name}: {potential_dir}")
                return potential_dir

        # If we can't find the directory, provide comprehensive debugging information
        logger.warning(f"No source directory found for task {task_name} from source {source_name}")
        logger.warning("Searched in potential directories:")
        for i, potential_dir in enumerate(potential_dirs, 1):
            exists_status = "EXISTS" if potential_dir.exists() else "NOT_FOUND"
            logger.warning(f"  {i}. {potential_dir} - {exists_status}")

        # Show what directories actually exist in the relevant locations
        if source_name != "local":
            remote_base_dir = unified_tasks_dir / f"remote_{source_name}"
            if remote_base_dir.exists():
                try:
                    actual_dirs = [d.name for d in remote_base_dir.iterdir() if d.is_dir()]
                    if actual_dirs:
                        logger.warning(f"Actual directories in {remote_base_dir}: {actual_dirs}")
                    else:
                        logger.warning(
                            f"Remote base directory {remote_base_dir} exists but is empty"
                        )
                except Exception as e:
                    logger.warning(f"Error listing contents of {remote_base_dir}: {e}")
            else:
                logger.warning(f"Remote base directory {remote_base_dir} does not exist")

                # Check if the unified_tasks_dir has any directories that might be relevant
                try:
                    all_task_dirs = [d.name for d in unified_tasks_dir.iterdir() if d.is_dir()]
                    relevant_dirs = [d for d in all_task_dirs if source_name in d or task_name in d]
                    if relevant_dirs:
                        logger.warning(
                            f"Found potentially relevant directories in {unified_tasks_dir}: {relevant_dirs}"
                        )
                    else:
                        logger.warning(
                            f"No directories containing '{source_name}' or '{task_name}' found in {unified_tasks_dir}"
                        )
                        logger.warning(f"All directories in {unified_tasks_dir}: {all_task_dirs}")
                except Exception as e:
                    logger.warning(f"Error listing unified tasks directory: {e}")

        return None

    async def _should_override_existing_task(
        self, source_dir: Path, target_dir: Path, task_name: str, source_name: str
    ) -> str:
        """
        Determine if we should override an existing task directory with new results.
        Returns: 'override', 'backup_and_override', or 'keep_existing'
        """
        try:
            # Check the status of the new results
            new_status = await self._get_task_status_from_directory(source_dir)
            existing_status = await self._get_task_status_from_directory(target_dir)

            logger.debug(
                f"Task {task_name} status comparison: existing={existing_status}, new={new_status}"
            )

            # Priority rules for completion runs:
            # 1. Always prefer COMPLETED over any other status
            # 2. Prefer any finished status (COMPLETED/FAILED/CANCELLED) over RUNNING
            # 3. If both are the same status, keep existing (avoid unnecessary work)

            if new_status == "COMPLETED":
                if existing_status != "COMPLETED":
                    logger.info(
                        f"Overriding {existing_status} task {task_name} with completed results from {source_name}"
                    )
                    return (
                        "backup_and_override"
                        if existing_status in ["FAILED", "CANCELLED"]
                        else "override"
                    )
                else:
                    logger.debug(
                        f"Both existing and new {task_name} are completed, keeping existing"
                    )
                    return "keep_existing"

            elif new_status in ["FAILED", "CANCELLED"]:
                if existing_status == "RUNNING":
                    logger.info(
                        f"Overriding running task {task_name} with finished results from {source_name}"
                    )
                    return "override"
                elif existing_status in ["FAILED", "CANCELLED"] and new_status != existing_status:
                    logger.info(
                        f"Updating {task_name} status from {existing_status} to {new_status}"
                    )
                    return "override"
                else:
                    logger.debug(
                        f"Keeping existing {task_name} status ({existing_status}) over new ({new_status})"
                    )
                    return "keep_existing"

            elif new_status == "RUNNING":
                # Don't override with running tasks
                logger.debug(f"Not overriding {task_name} with running task from {source_name}")
                return "keep_existing"

            else:
                # Unknown status - be conservative
                logger.debug(f"Unknown new status for {task_name}: {new_status}, keeping existing")
                return "keep_existing"

        except Exception as e:
            logger.warning(f"Error checking task status for override decision on {task_name}: {e}")
            return "keep_existing"

    async def _get_task_status_from_directory(self, task_dir: Path) -> str:
        """Get the task status from its directory."""
        try:
            task_info_file = task_dir / "task_info.txt"
            if not task_info_file.exists():
                return "UNKNOWN"

            with open(task_info_file) as f:
                content = f.read()

            # Get the last status line (most recent)
            status_lines = [line for line in content.split("\n") if line.startswith("Status: ")]
            if not status_lines:
                return "UNKNOWN"

            last_status_line = status_lines[-1]

            if "COMPLETED" in last_status_line:
                return "COMPLETED"
            elif "FAILED" in last_status_line:
                return "FAILED"
            elif "CANCELLED" in last_status_line:
                return "CANCELLED"
            elif "RUNNING" in last_status_line:
                return "RUNNING"
            else:
                return "UNKNOWN"

        except Exception as e:
            logger.debug(f"Error reading task status from {task_dir}: {e}")
            return "UNKNOWN"

    async def _backup_and_override_task(
        self,
        source_dir: Path,
        target_dir: Path,
        task_name: str,
        source_name: str,
        create_backup: bool = False,
    ):
        """Backup existing task directory and override with new results."""
        try:
            if create_backup:
                # Create backup subdirectory structure
                backup_base_dir = self.sweep_dir / "backups"
                backup_base_dir.mkdir(exist_ok=True)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_dir = backup_base_dir / f"{task_name}_backup_{timestamp}"

                logger.info(f"Backing up existing {target_dir} -> {backup_dir}")
                shutil.move(str(target_dir), str(backup_dir))
            else:
                # Just remove the existing directory
                logger.info(f"Removing existing {target_dir} for override")
                shutil.rmtree(target_dir)

            # Now move the new results
            logger.info(f"Overriding {task_name} with new results from {source_name}")
            shutil.move(str(source_dir), str(target_dir))

        except Exception as e:
            logger.error(f"Error during backup and override for {task_name}: {e}")
            raise

    async def _cleanup_empty_remote_dirs(self, unified_tasks_dir: Path) -> int:
        """Clean up any empty remote directories."""
        cleaned_count = 0

        try:
            for source in self.sources:
                if source.name != "local":
                    remote_dir = unified_tasks_dir / f"remote_{source.name}"
                    if remote_dir.exists():
                        try:
                            if not any(remote_dir.iterdir()):
                                remote_dir.rmdir()
                                logger.debug(f"Removed empty directory: {remote_dir.name}")
                                cleaned_count += 1
                        except OSError as e:
                            logger.debug(f"Could not remove directory {remote_dir.name}: {e}")

        except Exception as e:
            logger.error(f"Error cleaning up empty remote directories: {e}")

        return cleaned_count

    async def collect_scripts_and_logs(self):
        """Collect scripts and logs from all compute sources."""
        logger.info("Collecting scripts and logs from all compute sources...")

        try:
            scripts_dir = self.sweep_dir / "distributed_scripts"
            logs_dir = self.sweep_dir / "logs"

            scripts_dir.mkdir(exist_ok=True)
            logs_dir.mkdir(exist_ok=True)

            # Collect from each source
            for source in self.sources:
                await self._collect_source_scripts_and_logs(source, scripts_dir, logs_dir)

            logger.info("✓ Scripts and logs collection completed")

        except Exception as e:
            logger.error(f"Error collecting scripts and logs: {e}")

    async def _collect_source_scripts_and_logs(self, source, scripts_dir: Path, logs_dir: Path):
        """Collect scripts and logs from a specific compute source."""
        try:
            if source.name == "local":
                await self._collect_local_scripts_and_logs(source, scripts_dir, logs_dir)
            else:
                await self._collect_remote_scripts_and_logs(source, scripts_dir, logs_dir)

        except Exception as e:
            logger.error(f"Error collecting scripts and logs from {source.name}: {e}")

    async def _collect_local_scripts_and_logs(self, source, scripts_dir: Path, logs_dir: Path):
        """Collect scripts and logs from local compute source."""
        try:
            # Local scripts are already in the sweep directory structure
            local_scripts_dir = self.sweep_dir / "local_scripts"

            if local_scripts_dir.exists():
                # Copy scripts to distributed_scripts with source prefix
                for script_file in local_scripts_dir.glob("*.sh"):
                    target_name = f"local_{script_file.name}"
                    target_path = scripts_dir / target_name

                    if not target_path.exists():
                        shutil.copy2(script_file, target_path)
                        logger.debug(f"Copied local script: {script_file.name} -> {target_name}")

        except Exception as e:
            logger.error(f"Error collecting local scripts and logs: {e}")

    async def _collect_remote_scripts_and_logs(self, source, scripts_dir: Path, logs_dir: Path):
        """Collect scripts and logs from remote compute source."""
        try:
            if not isinstance(source, SSHComputeSource) or not source.remote_manager:
                logger.debug(f"Source {source.name} is not an SSH source or has no remote manager")
                return

            remote_manager = source.remote_manager
            remote_config = source.remote_config

            if not remote_manager.remote_sweep_dir:
                logger.debug(
                    f"No remote sweep directory for {source.name}, skipping script/log collection"
                )
                return

            # Use rsync to collect scripts and logs from remote

            # Collect scripts
            remote_scripts_dir = f"{remote_manager.remote_sweep_dir}/scripts"
            await self._rsync_with_rename(
                f"{remote_config.host}:{remote_scripts_dir}/", scripts_dir, source.name, "*.sh"
            )

            # Collect logs
            remote_logs_dir = f"{remote_manager.remote_sweep_dir}/logs"
            await self._rsync_with_rename(
                f"{remote_config.host}:{remote_logs_dir}/", logs_dir, source.name, "*"
            )

        except Exception as e:
            logger.error(f"Error collecting remote scripts and logs from {source.name}: {e}")

    async def _rsync_with_rename(
        self, remote_path: str, local_dir: Path, source_name: str, pattern: str
    ):
        """Use rsync to collect files and rename them with source prefix."""
        try:
            import subprocess

            rsync_cmd = (
                f"rsync -avz --compress-level=6 --ignore-missing-args "
                f"--include='{pattern}' --exclude='*' "
                f"{remote_path} {local_dir}/"
            )

            result = subprocess.run(rsync_cmd, shell=True, capture_output=True, text=True)

            if result.returncode == 0:
                logger.debug(f"Successfully collected files from {source_name}")

                # Rename collected files to include source name
                for file_path in local_dir.glob(pattern):
                    if file_path.is_file() and not file_path.name.startswith(f"{source_name}_"):
                        new_name = f"{source_name}_{file_path.name}"
                        new_path = local_dir / new_name
                        if not new_path.exists():
                            file_path.rename(new_path)
                            logger.debug(f"Renamed file: {file_path.name} -> {new_name}")
            else:
                logger.debug(
                    f"No files to collect from {source_name} (rsync exit {result.returncode})"
                )

        except Exception as e:
            logger.debug(f"Error in rsync collection: {e}")
