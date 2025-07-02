"""Slurm job manager implementation."""

import json
import logging
from pathlib import Path
import subprocess
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from .hpc_base import HPCJobManager

logger = logging.getLogger(__name__)


class SlurmJobManager(HPCJobManager):
    """Slurm job manager implementation."""

    def __init__(
        self,
        walltime: str = "04:00:00",
        resources: str = "--nodes=1 --ntasks=4 --mem=16G",
        python_path: str = "python",
        script_path: str = "",
        project_dir: str = ".",
    ):
        super().__init__(walltime, resources, python_path, script_path, project_dir)
        self.system_type = "slurm"

        # Setup Jinja2 environment
        template_dir = Path(__file__).parent.parent.parent / "templates"
        self.jinja_env = Environment(loader=FileSystemLoader(str(template_dir)))

    @property
    def scheduler_name(self) -> str:
        return "slurm"

    @property
    def submit_command(self) -> str:
        return "sbatch"

    @property
    def status_command(self) -> str:
        return "squeue"

    def _validate_system(self):
        """Validate that Slurm is available."""
        # First check if commands are available
        import shutil

        if not shutil.which("sbatch") or not shutil.which("squeue"):
            raise RuntimeError("Slurm commands not found")

        try:
            subprocess.run(["sinfo", "--version"], capture_output=True, check=True, timeout=5)
            logger.debug("Slurm system detected")
        except (
            subprocess.CalledProcessError,
            FileNotFoundError,
            subprocess.TimeoutExpired,
        ) as e:
            raise RuntimeError(f"Slurm system not available or not properly configured: {e}")

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
        """Submit a single Slurm job."""
        # Use provided directories or fallback to sweep_dir
        if logs_dir is None:
            logs_dir = sweep_dir / "logs"
        if pbs_dir is None:
            pbs_dir = sweep_dir / "scripts"

        # Ensure directories exist
        logs_dir.mkdir(parents=True, exist_ok=True)
        pbs_dir.mkdir(parents=True, exist_ok=True)

        # Create simple Slurm script
        script_content = f"""#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --time={self.walltime}
#SBATCH {self.resources}
#SBATCH --output={logs_dir}/{job_name}.out
#SBATCH --error={logs_dir}/{job_name}.err

cd {self.project_dir}

# Run the training script
{self.python_path} {self.script_path} {self._params_to_string(params)} wandb.group={wandb_group or sweep_id}
"""

        # Write job script
        job_script_path = pbs_dir / f"{job_name}.slurm"
        with open(job_script_path, "w") as f:
            f.write(script_content)

        # Submit the job
        result = self._run_command(f"sbatch {job_script_path}")
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to submit Slurm job: {result.stderr}\nScript: {job_script_path}"
            )

        # Extract job ID from output (format: "Submitted batch job 12345")
        job_id = result.stdout.strip().split()[-1]
        logger.info(f"Submitted Slurm job {job_id} for {job_name}")
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
        """Submit a Slurm array job."""
        if logs_dir is None:
            logs_dir = sweep_dir / "logs"
        if pbs_dir is None:
            pbs_dir = sweep_dir / "scripts"

        # Ensure directories exist
        logs_dir.mkdir(parents=True, exist_ok=True)
        pbs_dir.mkdir(parents=True, exist_ok=True)

        # Create tasks directory for organized task outputs
        tasks_dir = sweep_dir / "tasks"
        tasks_dir.mkdir(parents=True, exist_ok=True)

        # Save parameter combinations to JSON file
        params_file = sweep_dir / "parameter_combinations.json"
        indexed_combinations = [
            {"index": i + 1, "params": params} for i, params in enumerate(param_combinations)
        ]

        with open(params_file, "w") as f:
            json.dump(indexed_combinations, f, indent=2)

        # Render array job script from template
        template = self.jinja_env.get_template("slurm_array.sh.j2")
        script_content = template.render(
            job_name=f"{sweep_id}_array",
            sweep_id=sweep_id,
            num_jobs=len(param_combinations),
            walltime=self.walltime,
            resources=self.resources,
            sweep_dir=str(sweep_dir),
            logs_dir=str(logs_dir),
            tasks_dir=str(tasks_dir),
            params_file=str(params_file),
            python_path=self.python_path,
            script_path=self.script_path,
            project_dir=self.project_dir,
            wandb_group=wandb_group,
        )

        # Write array job script
        array_script_path = pbs_dir / f"{sweep_id}_array.slurm"
        with open(array_script_path, "w") as f:
            f.write(script_content)

        # Submit the array job
        result = self._run_command(f"sbatch {array_script_path}")
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to submit Slurm array job: {result.stderr}\nScript: {array_script_path}"
            )

        # Extract job ID from output
        array_job_id = result.stdout.strip().split()[-1]
        logger.info(
            f"Submitted Slurm array job {array_job_id} with {len(param_combinations)} tasks"
        )
        return array_job_id

    def get_job_status(self, job_id: str) -> str:
        """Get Slurm job status."""
        try:
            result = self._run_command(f"squeue -j {job_id} -h -o %T")
            if result.returncode != 0:
                # Job might be completed and out of queue
                return "completed"

            status = result.stdout.strip()

            # Map Slurm states to standard states
            status_map = {
                "PENDING": "queued",
                "RUNNING": "running",
                "SUSPENDED": "held",
                "CANCELLED": "cancelled",
                "COMPLETING": "completing",
                "COMPLETED": "completed",
                "CONFIGURING": "queued",
                "FAILED": "failed",
                "TIMEOUT": "failed",
                "PREEMPTED": "failed",
                "NODE_FAIL": "failed",
            }

            return status_map.get(status, "unknown")

        except Exception as e:
            logger.warning(f"Error checking Slurm job status for {job_id}: {e}")
            return "unknown"
