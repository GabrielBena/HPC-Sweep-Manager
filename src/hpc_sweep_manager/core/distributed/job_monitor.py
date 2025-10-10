"""Job monitoring component for distributed sweeps with real-time status tracking."""

import asyncio
from datetime import datetime
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set

try:
    from dateutil.parser import parse as parse_date

    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False
    parse_date = None

from ..common.compute_source import ComputeSource, JobInfo

logger = logging.getLogger(__name__)


class JobMonitor:
    """Monitors job statuses across distributed compute sources with real-time updates."""

    def __init__(self, sweep_dir: Path, sources: List[ComputeSource]):
        self.sweep_dir = sweep_dir
        self.sources = sources

        # Tracking state
        self.all_jobs: Dict[str, JobInfo] = {}
        self.job_to_source: Dict[str, str] = {}
        self.task_to_source: Dict[str, str] = {}

        # Status change tracking
        self.status_change_callbacks = []
        self.last_known_statuses: Dict[str, str] = {}

        # Track which tasks have already had status mismatches logged to avoid spam
        self.status_mismatches_logged: Set[str] = set()

        # Control flags
        self._running = False
        self._monitoring_task = None

    def register_status_change_callback(self, callback):
        """Register a callback for when job statuses change."""
        self.status_change_callbacks.append(callback)

    def add_job(self, job_info: JobInfo, source_name: str, task_name: str):
        """Add a job to monitoring."""
        self.all_jobs[job_info.job_id] = job_info
        self.job_to_source[job_info.job_id] = source_name
        self.task_to_source[task_name] = source_name
        self.last_known_statuses[job_info.job_id] = job_info.status

        logger.debug(
            f"Added job {job_info.job_id} to monitoring (source: {source_name}, task: {task_name})"
        )

    async def start_monitoring(self):
        """Start the monitoring task."""
        if self._running:
            logger.warning("Job monitoring already running")
            return

        self._running = True
        self._monitoring_task = asyncio.create_task(self._monitor_jobs())
        logger.info("Started real-time job monitoring")

    async def stop_monitoring(self):
        """Stop the monitoring task."""
        self._running = False

        if self._monitoring_task and not self._monitoring_task.done():
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

        logger.info("Stopped job monitoring")

    async def _monitor_jobs(self):
        """Main monitoring loop with real-time status tracking."""
        logger.debug("Started job monitoring loop")

        try:
            while self._running:
                await self._update_job_statuses()
                await asyncio.sleep(2)  # Check every 2 seconds for responsive monitoring

        except Exception as e:
            logger.error(f"Critical error in job monitoring: {e}")
        finally:
            logger.debug("Job monitoring loop completed")

    async def _update_job_statuses(self):
        """Update job statuses from all sources and detect changes."""
        try:
            # Update job statuses for all sources
            update_tasks = []
            for source in self.sources:
                if hasattr(source, "update_all_job_statuses"):
                    update_tasks.append(source.update_all_job_statuses())

            if update_tasks:
                await asyncio.gather(*update_tasks, return_exceptions=True)

            # Check for status changes and validate against actual task directories
            await self._check_status_changes()

        except Exception as e:
            logger.error(f"Error updating job statuses: {e}")

    async def _check_status_changes(self):
        """Check for job status changes and validate against task directories."""
        changes_detected = []

        for job_id, job_info in self.all_jobs.items():
            # Get current status from source
            source_name = self.job_to_source.get(job_id)
            if not source_name:
                continue

            source = next((s for s in self.sources if s.name == source_name), None)
            if not source:
                continue

            # Get current status from the source
            current_status = self._get_job_status_from_source(source, job_id)
            last_status = self.last_known_statuses.get(job_id, "UNKNOWN")

            if current_status and current_status != last_status:
                # Status changed - validate it against task directory
                validated_status = await self._validate_status_against_directory(
                    job_id, current_status
                )

                if validated_status != last_status:
                    # Update our tracking
                    job_info.status = validated_status
                    self.last_known_statuses[job_id] = validated_status

                    # Update completion time if job finished
                    if (
                        validated_status in ["COMPLETED", "FAILED", "CANCELLED"]
                        and not job_info.complete_time
                    ):
                        job_info.complete_time = datetime.now()

                    changes_detected.append(
                        {
                            "job_id": job_id,
                            "old_status": last_status,
                            "new_status": validated_status,
                            "source": source_name,
                        }
                    )

                    logger.info(
                        f"Job status change detected: {job_id} ({source_name}) {last_status} -> {validated_status}"
                    )

        # Notify callbacks of any changes
        if changes_detected:
            for callback in self.status_change_callbacks:
                try:
                    await callback(changes_detected)
                except Exception as e:
                    logger.error(f"Error in status change callback: {e}")

    def _get_job_status_from_source(self, source: ComputeSource, job_id: str) -> Optional[str]:
        """Get the current job status from the compute source."""
        try:
            # Check active jobs first
            if job_id in source.active_jobs:
                return source.active_jobs[job_id].status

            # Check completed jobs
            if job_id in source.completed_jobs:
                return source.completed_jobs[job_id].status

            # Try the generic get_job_status method
            if hasattr(source, "get_job_status"):
                return source.get_job_status(job_id)

            return None

        except Exception as e:
            logger.debug(f"Error getting job status for {job_id} from {source.name}: {e}")
            return None

    async def _validate_status_against_directory(self, job_id: str, reported_status: str) -> str:
        """Validate the reported status against the actual task directory content."""
        try:
            # Find the task name for this job
            task_name = None
            source_name = self.job_to_source.get(job_id)

            for task, task_source in self.task_to_source.items():
                if task_source == source_name:
                    # This is a potential match - check if job_name contains task info
                    job_info = self.all_jobs.get(job_id)
                    if job_info and task in job_info.job_name:
                        task_name = task
                        break

            if not task_name:
                logger.debug(f"Could not find task name for job {job_id}, using reported status")
                return reported_status

            # Check the task directory
            task_dir = self.sweep_dir / "tasks" / task_name
            task_info_file = task_dir / "task_info.txt"

            if not task_info_file.exists():
                # No task info file yet - job might still be running or results not collected
                if reported_status == "RUNNING":
                    return reported_status
                else:
                    logger.debug(
                        f"No task_info.txt for {task_name}, but status is {reported_status}"
                    )
                    return reported_status

            # Read the task info file to get the actual status
            with open(task_info_file) as f:
                content = f.read()

            # Check if this might be an old task_info.txt file by looking at timestamps
            timestamp_mismatch = self._check_for_stale_task_info(content, job_id, task_name)

            # Get the last status line (most recent)
            status_lines = [line for line in content.split("\n") if line.startswith("Status: ")]
            if not status_lines:
                logger.debug(f"No status found in task_info.txt for {task_name}")
                return reported_status

            last_status_line = status_lines[-1]

            # Parse the actual status
            if "COMPLETED" in last_status_line:
                actual_status = "COMPLETED"
            elif "FAILED" in last_status_line:
                actual_status = "FAILED"
            elif "CANCELLED" in last_status_line:
                actual_status = "CANCELLED"
            elif "RUNNING" in last_status_line:
                actual_status = "RUNNING"
            else:
                logger.debug(f"Unknown status in task_info.txt for {task_name}: {last_status_line}")
                return reported_status

            # If there's a mismatch, be smart about which status to trust
            if actual_status != reported_status:
                # If we detected a stale task_info.txt file, trust the reported status
                if timestamp_mismatch:
                    logger.warning(
                        f"Detected stale task_info.txt for {task_name} (likely from previous run)"
                    )
                    logger.warning(
                        f"Trusting reported status '{reported_status}' over stale file status '{actual_status}'"
                    )
                    return reported_status

                # If this is a remote source and the reported status suggests success/completion
                # but the local file shows failure, it might be from a previous run
                if (
                    source_name != "local"
                    and reported_status in ["COMPLETED", "RUNNING"]
                    and actual_status == "FAILED"
                ):
                    logger.warning(
                        f"Remote task {task_name}: reported={reported_status} vs local_file={actual_status}"
                    )
                    logger.warning(
                        "Local file might be from previous run - checking result collection status"
                    )

                    # Check if we have evidence that result collection failed for this task
                    remote_dir = self.sweep_dir / "tasks" / f"remote_{source_name}"
                    if not remote_dir.exists() or not (remote_dir / task_name).exists():
                        logger.warning(
                            f"No remote results collected for {task_name} - trusting reported status"
                        )
                        return reported_status

                # Only log the warning once per task to avoid spam
                if task_name not in self.status_mismatches_logged:
                    logger.info(
                        f"Status mismatch for {task_name}: reported={reported_status}, actual={actual_status}"
                    )
                    logger.info(
                        f"Correcting {task_name} status: {reported_status} -> {actual_status}"
                    )
                    self.status_mismatches_logged.add(task_name)

                return actual_status

            return actual_status

        except Exception as e:
            logger.debug(f"Error validating status for job {job_id}: {e}")
            return reported_status

    def _check_for_stale_task_info(self, content: str, job_id: str, task_name: str) -> bool:
        """
        Check if the task_info.txt file might be stale (from a previous run).
        Returns True if the file appears to be stale.
        """
        try:
            # Get the job's start time
            job_info = self.all_jobs.get(job_id)
            if not job_info or not job_info.start_time:
                return False

            current_job_start = job_info.start_time

            # Look for start time in the task_info.txt
            start_time_lines = [
                line for line in content.split("\n") if line.startswith("Start Time:")
            ]
            if not start_time_lines:
                return False

            file_start_time_str = start_time_lines[-1].split("Start Time:", 1)[1].strip()

            # Try to parse the timestamp
            try:
                if DATEUTIL_AVAILABLE:
                    file_start_time = parse_date(file_start_time_str)
                else:
                    logger.debug("Dateutil not available, using default parsing")
                    file_start_time = datetime.fromtimestamp(float(file_start_time_str))

                # If the file's start time is more than a day before the current job's start time,
                # it's likely stale
                time_diff = current_job_start - file_start_time
                if time_diff.total_seconds() > 86400:  # 24 hours
                    logger.debug(
                        f"Task {task_name}: file start time {file_start_time} is {time_diff} before job start {current_job_start}"
                    )
                    return True

            except Exception as e:
                logger.debug(f"Could not parse timestamp from task_info.txt: {e}")
                return False

        except Exception as e:
            logger.debug(f"Error checking for stale task_info for {task_name}: {e}")

        return False

    def get_job_counts(self) -> Dict[str, int]:
        """Get current job counts by status."""
        counts = {
            "RUNNING": 0,
            "COMPLETED": 0,
            "FAILED": 0,
            "CANCELLED": 0,
            "QUEUED": 0,
            "UNKNOWN": 0,
        }

        for job_info in self.all_jobs.values():
            status = job_info.status
            if status in counts:
                counts[status] += 1
            else:
                counts["UNKNOWN"] += 1

        return counts

    def get_source_statistics(self) -> Dict[str, Dict[str, int]]:
        """Get job statistics per source."""
        source_stats = {}

        for source in self.sources:
            source_stats[source.name] = {
                "total": 0,
                "completed": 0,
                "failed": 0,
                "running": 0,
                "cancelled": 0,
            }

        for job_id, job_info in self.all_jobs.items():
            source_name = self.job_to_source.get(job_id)
            if source_name and source_name in source_stats:
                source_stats[source_name]["total"] += 1

                status = job_info.status.lower()
                if status == "completed":
                    source_stats[source_name]["completed"] += 1
                elif status == "failed":
                    source_stats[source_name]["failed"] += 1
                elif status == "running":
                    source_stats[source_name]["running"] += 1
                elif status == "cancelled":
                    source_stats[source_name]["cancelled"] += 1

        return source_stats

    def get_failed_jobs(self) -> List[Dict[str, str]]:
        """Get list of failed jobs with details."""
        failed_jobs = []

        for job_id, job_info in self.all_jobs.items():
            if job_info.status == "FAILED":
                source_name = self.job_to_source.get(job_id, "unknown")
                task_name = None

                # Find task name
                for task, task_source in self.task_to_source.items():
                    if task_source == source_name and task in job_info.job_name:
                        task_name = task
                        break

                failed_jobs.append(
                    {
                        "job_id": job_id,
                        "job_name": job_info.job_name,
                        "task_name": task_name or "unknown",
                        "source": source_name,
                        "start_time": job_info.start_time.isoformat()
                        if job_info.start_time
                        else None,
                        "complete_time": job_info.complete_time.isoformat()
                        if job_info.complete_time
                        else None,
                    }
                )

        return failed_jobs
