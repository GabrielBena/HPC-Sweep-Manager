"""HPC compute source implementation supporting PBS and Slurm schedulers."""

import asyncio
import json
import logging
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from ..config.hsm import HSMConfig
from .base import (
    ComputeSource,
    Task,
    TaskResult,
    TaskStatus,
    HealthStatus,
    HealthReport,
    SweepContext,
    ComputeSourceStats,
    CollectionResult,
)

logger = logging.getLogger(__name__)


class HPCComputeSource(ComputeSource):
    """HPC compute source supporting PBS and Slurm schedulers."""

    def __init__(
        self,
        cluster: Optional[str] = None,
        scheduler: Optional[str] = None,
        walltime: str = "04:00:00",
        resources: Optional[str] = None,
        partition: Optional[str] = None,
        account: Optional[str] = None,
        modules: Optional[List[str]] = None,
        conda_env: Optional[str] = None,
        max_concurrent_jobs: int = 100,
        poll_interval: float = 30.0,
        name: Optional[str] = None,
    ):
        """Initialize HPC compute source.

        Args:
            cluster: Cluster name (for identification)
            scheduler: Force specific scheduler ('pbs' or 'slurm'), auto-detect if None
            walltime: Job walltime
            resources: Resource specification string
            partition: Slurm partition or PBS queue
            account: Account/project for billing
            modules: Environment modules to load
            conda_env: Conda environment to activate
            max_concurrent_jobs: Maximum concurrent jobs
            poll_interval: Status polling interval in seconds
            name: Custom name for this compute source (auto-generated if None)
        """
        # Auto-detect scheduler if not specified
        scheduler = scheduler or self._detect_scheduler()

        # Generate name if not provided
        if name is None:
            name = f"hpc-{scheduler}-{cluster or 'default'}"

        super().__init__(
            name=name,
            source_type="hpc",
            max_parallel_tasks=max_concurrent_jobs,
            health_check_interval=300,
        )

        self.cluster = cluster or "default"
        self.scheduler = scheduler
        self.walltime = walltime
        self.partition = partition
        self.account = account
        self.modules = modules or []
        self.conda_env = conda_env
        self.max_concurrent_jobs = max_concurrent_jobs
        self.poll_interval = poll_interval

        # Set default resources based on scheduler
        if resources is None:
            if self.scheduler == "slurm":
                self.resources = "--nodes=1 --ntasks=4 --mem=16G"
            else:  # PBS
                self.resources = "select=1:ncpus=4:mem=16gb"
        else:
            self.resources = resources

        # State tracking
        self._sweep_context: Optional[SweepContext] = None
        self._active_jobs: Dict[str, str] = {}  # task_id -> job_id
        self._job_to_task: Dict[str, str] = {}  # job_id -> task_id
        self._scripts_dir: Optional[Path] = None
        self._monitoring_task: Optional[asyncio.Task] = None
        self._shutdown = False

    def _detect_scheduler(self) -> str:
        """Auto-detect available HPC scheduler."""
        if shutil.which("sbatch") and shutil.which("squeue"):
            return "slurm"
        elif shutil.which("qsub") and shutil.which("qstat"):
            return "pbs"
        else:
            raise RuntimeError(
                "No supported HPC scheduler detected. "
                "Please ensure PBS/Torque or Slurm tools are available."
            )

    async def setup(self, sweep_context: SweepContext) -> bool:
        """Set up the HPC compute source."""
        try:
            self._sweep_context = sweep_context

            # Create scripts directory
            self._scripts_dir = sweep_context.sweep_dir / "scripts" / self.name
            self._scripts_dir.mkdir(parents=True, exist_ok=True)

            # Validate scheduler availability
            if not await self._validate_scheduler():
                logger.error(f"Failed to validate {self.scheduler} scheduler")
                return False

            # Start background monitoring
            self._monitoring_task = asyncio.create_task(self._monitor_jobs())

            logger.info(f"HPC compute source setup complete: {self.name}")
            return True

        except Exception as e:
            logger.error(f"Failed to setup HPC compute source: {e}")
            return False

    async def submit_task(self, task: Task) -> TaskResult:
        """Submit a task to the HPC scheduler."""
        try:
            # Check if we've reached the concurrent job limit
            if len(self._active_jobs) >= self.max_concurrent_jobs:
                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.PENDING,
                    message="Waiting for job slots to become available",
                )

            # Create job script
            script_path = await self._create_job_script(task)

            # Submit job
            job_id = await self._submit_job(script_path, task)

            # Track job
            self._active_jobs[task.task_id] = job_id
            self._job_to_task[job_id] = task.task_id

            logger.info(f"Submitted HPC job {job_id} for task {task.task_id}")

            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.RUNNING,
                job_id=job_id,
                compute_source=self.name,
                message=f"Submitted to {self.scheduler} as job {job_id}",
            )

        except Exception as e:
            logger.error(f"Failed to submit task {task.task_id}: {e}")
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                message=f"Submission failed: {e}",
                error=str(e),
            )

    async def get_task_status(self, task_id: str) -> TaskStatus:
        """Get the current status of a task."""
        if task_id not in self._active_jobs:
            # Check if task completed and was cleaned up
            task_dir = self._sweep_context.sweep_dir / "tasks" / task_id
            if task_dir.exists() and (task_dir / "status.yaml").exists():
                # Load status from file
                import yaml

                with open(task_dir / "status.yaml") as f:
                    status_data = yaml.safe_load(f)
                return TaskStatus(status_data.get("status", "unknown"))
            return TaskStatus.UNKNOWN

        job_id = self._active_jobs[task_id]
        job_status = await self._get_job_status(job_id)

        # Map job status to task status
        status_map = {
            "queued": TaskStatus.PENDING,
            "running": TaskStatus.RUNNING,
            "completed": TaskStatus.COMPLETED,
            "failed": TaskStatus.FAILED,
            "cancelled": TaskStatus.CANCELLED,
        }

        return status_map.get(job_status, TaskStatus.UNKNOWN)

    async def collect_results(self, task_ids: List[str]) -> CollectionResult:
        """Collect results for completed tasks."""
        collected = []
        failed = []

        for task_id in task_ids:
            try:
                task_dir = self._sweep_context.sweep_dir / "tasks" / task_id

                if task_dir.exists():
                    # Results are already local (HPC jobs write to shared filesystem)
                    collected.append(task_id)
                else:
                    failed.append(task_id)

            except Exception as e:
                logger.error(f"Failed to collect results for task {task_id}: {e}")
                failed.append(task_id)

        return CollectionResult(
            collected_tasks=collected,
            failed_tasks=failed,
            total_size=0,  # Not tracking size for HPC
        )

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a running task."""
        if task_id not in self._active_jobs:
            return False

        job_id = self._active_jobs[task_id]

        try:
            if self.scheduler == "slurm":
                result = await self._run_command(f"scancel {job_id}")
            else:  # PBS
                result = await self._run_command(f"qdel {job_id}")

            if result.returncode == 0:
                logger.info(f"Cancelled HPC job {job_id} for task {task_id}")
                # Clean up tracking
                del self._active_jobs[task_id]
                del self._job_to_task[job_id]
                return True
            else:
                logger.error(f"Failed to cancel job {job_id}: {result.stderr}")
                return False

        except Exception as e:
            logger.error(f"Error cancelling task {task_id}: {e}")
            return False

    async def health_check(self) -> HealthReport:
        """Check the health of the HPC system."""
        issues = []
        metrics = {}

        try:
            # Check scheduler availability
            if not await self._validate_scheduler():
                issues.append(f"{self.scheduler} scheduler not available")

            # Check queue status
            try:
                if self.scheduler == "slurm":
                    result = await self._run_command("sinfo -h")
                else:  # PBS
                    result = await self._run_command("qstat -Q")

                if result.returncode != 0:
                    issues.append(f"Cannot query {self.scheduler} queue status")

            except Exception as e:
                issues.append(f"Queue status check failed: {e}")

            # Get active job count
            metrics["active_jobs"] = len(self._active_jobs)
            metrics["max_jobs"] = self.max_concurrent_jobs
            metrics["job_slots_available"] = self.max_concurrent_jobs - len(self._active_jobs)

            # Overall health status
            if issues:
                status = HealthStatus.UNHEALTHY
            elif len(self._active_jobs) >= self.max_concurrent_jobs:
                status = HealthStatus.DEGRADED
            else:
                status = HealthStatus.HEALTHY

            return HealthReport(
                source_name=self.name,
                status=status,
                last_check=datetime.now(),
                active_tasks=len(self._active_jobs),
                issues=issues,
                metrics=metrics,
            )

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return HealthReport(
                source_name=self.name,
                status=HealthStatus.UNHEALTHY,
                last_check=datetime.now(),
                issues=[f"Health check failed: {e}"],
            )

    async def cleanup(self) -> bool:
        """Clean up the HPC compute source."""
        try:
            self._shutdown = True

            # Stop monitoring task
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass

            logger.info(f"HPC compute source cleanup complete: {self.name}")
            return True

        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            return False

    def get_stats(self) -> ComputeSourceStats:
        """Get compute source statistics."""
        return ComputeSourceStats(
            source_name=self.name,
            source_type=self.source_type,
            max_parallel_tasks=self.max_concurrent_jobs,
            tasks_submitted=len(self._active_jobs),
            tasks_completed=0,  # Would need to track this
            tasks_failed=0,  # Would need to track this
            health_status=HealthStatus.HEALTHY,  # Should check actual health
        )

    # Helper methods

    async def _validate_scheduler(self) -> bool:
        """Validate that the scheduler is available and working."""
        try:
            if self.scheduler == "slurm":
                result = await self._run_command("sinfo --version")
            else:  # PBS
                result = await self._run_command("qstat --version")

            return result.returncode == 0

        except Exception:
            return False

    async def _create_job_script(self, task: Task) -> Path:
        """Create a job script for the task."""
        script_path = self._scripts_dir / f"{task.task_id}.{self.scheduler}"

        # Convert task parameters to command line arguments
        param_args = self._params_to_args(task.parameters)

        if self.scheduler == "slurm":
            script_content = self._create_slurm_script(task, param_args)
        else:  # PBS
            script_content = self._create_pbs_script(task, param_args)

        # Write script file
        with open(script_path, "w") as f:
            f.write(script_content)

        # Make executable
        script_path.chmod(0o755)

        return script_path

    def _create_slurm_script(self, task: Task, param_args: str) -> str:
        """Create a Slurm job script."""
        task_dir = self._sweep_context.sweep_dir / "tasks" / task.task_id
        logs_dir = task_dir / "logs"

        # Build script content
        lines = [
            "#!/bin/bash",
            f"#SBATCH --job-name={task.task_id}",
            f"#SBATCH --time={self.walltime}",
            f"#SBATCH {self.resources}",
            f"#SBATCH --output={logs_dir}/stdout.log",
            f"#SBATCH --error={logs_dir}/stderr.log",
        ]

        if self.partition:
            lines.append(f"#SBATCH --partition={self.partition}")
        if self.account:
            lines.append(f"#SBATCH --account={self.account}")

        lines.extend(
            [
                "",
                "# Setup environment",
                f"cd {self._sweep_context.project_root}",
            ]
        )

        # Load modules
        for module in self.modules:
            lines.append(f"module load {module}")

        # Activate conda environment
        if self.conda_env:
            lines.extend(["# Activate conda environment", f"source activate {self.conda_env}"])

        # Create output directories
        lines.extend(
            [
                "",
                "# Create output directories",
                f"mkdir -p {task_dir}",
                f"mkdir -p {logs_dir}",
                f"mkdir -p {task_dir}/results",
            ]
        )

        # Run the task
        lines.extend(
            [
                "",
                "# Run task",
                f"python {self._sweep_context.script_path} {param_args}",
            ]
        )

        return "\n".join(lines)

    def _create_pbs_script(self, task: Task, param_args: str) -> str:
        """Create a PBS job script."""
        task_dir = self._sweep_context.sweep_dir / "tasks" / task.task_id
        logs_dir = task_dir / "logs"

        # Build script content
        lines = [
            "#!/bin/bash",
            f"#PBS -N {task.task_id}",
            f"#PBS -l walltime={self.walltime}",
            f"#PBS -l {self.resources}",
            f"#PBS -o {logs_dir}/stdout.log",
            f"#PBS -e {logs_dir}/stderr.log",
        ]

        if self.partition:
            lines.append(f"#PBS -q {self.partition}")
        if self.account:
            lines.append(f"#PBS -A {self.account}")

        lines.extend(
            [
                "",
                "# Setup environment",
                f"cd {self._sweep_context.project_root}",
            ]
        )

        # Load modules
        for module in self.modules:
            lines.append(f"module load {module}")

        # Activate conda environment
        if self.conda_env:
            lines.extend(["# Activate conda environment", f"source activate {self.conda_env}"])

        # Create output directories
        lines.extend(
            [
                "",
                "# Create output directories",
                f"mkdir -p {task_dir}",
                f"mkdir -p {logs_dir}",
                f"mkdir -p {task_dir}/results",
            ]
        )

        # Run the task
        lines.extend(
            [
                "",
                "# Run task",
                f"python {self._sweep_context.script_path} {param_args}",
            ]
        )

        return "\n".join(lines)

    def _params_to_args(self, params: Dict[str, Any]) -> str:
        """Convert parameters to command line arguments."""
        args = []
        for key, value in params.items():
            if isinstance(value, (list, tuple)):
                # Handle list/tuple parameters
                value_str = str(list(value))
                args.append(f'"{key}={value_str}"')
            elif value is None:
                args.append(f'"{key}=null"')
            elif isinstance(value, bool):
                args.append(f'"{key}={str(value).lower()}"')
            elif isinstance(value, str) and (" " in value or "," in value):
                args.append(f'"{key}={value}"')
            else:
                args.append(f'"{key}={value}"')

        return " ".join(args)

    async def _submit_job(self, script_path: Path, task: Task) -> str:
        """Submit a job script to the scheduler."""
        if self.scheduler == "slurm":
            result = await self._run_command(f"sbatch {script_path}")
            if result.returncode != 0:
                raise RuntimeError(f"sbatch failed: {result.stderr}")
            # Extract job ID from "Submitted batch job 12345"
            job_id = result.stdout.strip().split()[-1]
        else:  # PBS
            result = await self._run_command(f"qsub {script_path}")
            if result.returncode != 0:
                raise RuntimeError(f"qsub failed: {result.stderr}")
            job_id = result.stdout.strip()

        return job_id

    async def _get_job_status(self, job_id: str) -> str:
        """Get the status of a job."""
        try:
            if self.scheduler == "slurm":
                result = await self._run_command(f"squeue -j {job_id} -h -o %T")
                if result.returncode != 0:
                    return "completed"  # Job not in queue, assume completed

                status = result.stdout.strip()
                status_map = {
                    "PENDING": "queued",
                    "RUNNING": "running",
                    "SUSPENDED": "held",
                    "CANCELLED": "cancelled",
                    "COMPLETING": "running",
                    "COMPLETED": "completed",
                    "FAILED": "failed",
                    "TIMEOUT": "failed",
                    "NODE_FAIL": "failed",
                }
                return status_map.get(status, "unknown")

            else:  # PBS
                result = await self._run_command(f"qstat -f {job_id}")
                if result.returncode != 0:
                    return "completed"  # Job not in queue

                output = result.stdout
                if "job_state = R" in output:
                    return "running"
                elif "job_state = Q" in output:
                    return "queued"
                elif "job_state = H" in output:
                    return "held"
                elif "job_state = C" in output:
                    return "completed"
                elif "job_state = E" in output:
                    return "running"  # Exiting
                else:
                    return "unknown"

        except Exception as e:
            logger.error(f"Failed to get status for job {job_id}: {e}")
            return "unknown"

    async def _monitor_jobs(self):
        """Background task to monitor job status."""
        while not self._shutdown:
            try:
                # Check status of all active jobs
                completed_tasks = []

                for task_id, job_id in list(self._active_jobs.items()):
                    status = await self._get_job_status(job_id)

                    if status in ["completed", "failed", "cancelled"]:
                        completed_tasks.append(task_id)

                # Clean up completed jobs
                for task_id in completed_tasks:
                    job_id = self._active_jobs.pop(task_id, None)
                    if job_id:
                        self._job_to_task.pop(job_id, None)
                        logger.info(f"Job {job_id} for task {task_id} completed")

                # Wait before next check
                await asyncio.sleep(self.poll_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in job monitoring: {e}")
                await asyncio.sleep(self.poll_interval)

    async def _run_command(self, command: str) -> subprocess.CompletedProcess:
        """Run a shell command asynchronously."""
        process = await asyncio.create_subprocess_shell(
            command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        return subprocess.CompletedProcess(
            args=command,
            returncode=process.returncode,
            stdout=stdout.decode(),
            stderr=stderr.decode(),
        )
