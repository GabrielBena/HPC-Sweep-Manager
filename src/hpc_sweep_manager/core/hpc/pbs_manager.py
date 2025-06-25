"""PBS/Torque job manager implementation."""

import logging
from pathlib import Path
import subprocess
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from .hpc_base import HPCJobManager

logger = logging.getLogger(__name__)


class PBSJobManager(HPCJobManager):
    """PBS/Torque job manager implementation."""

    def __init__(
        self,
        walltime: str = "04:00:00",
        resources: str = "select=1:ncpus=4:mem=16gb",
        python_path: str = "python",
        script_path: str = "",
        project_dir: str = ".",
    ):
        super().__init__(walltime, resources, python_path, script_path, project_dir)
        self.system_type = "pbs"

        # Setup Jinja2 environment
        template_dir = Path(__file__).parent.parent.parent / "templates"
        self.jinja_env = Environment(loader=FileSystemLoader(str(template_dir)))

    @property
    def scheduler_name(self) -> str:
        return "pbs"

    @property
    def submit_command(self) -> str:
        return "qsub"

    @property
    def status_command(self) -> str:
        return "qstat"

    def _validate_system(self):
        """Validate that PBS/Torque is available."""
        # First check if commands are available
        import shutil

        if not shutil.which("qsub") or not shutil.which("qstat"):
            raise RuntimeError("PBS/Torque commands not found")

        try:
            subprocess.run(["qstat", "--version"], capture_output=True, check=True, timeout=5)
            logger.debug("PBS/Torque system detected")
        except (
            subprocess.CalledProcessError,
            FileNotFoundError,
            subprocess.TimeoutExpired,
        ) as e:
            raise RuntimeError(f"PBS/Torque system not available or not properly configured: {e}")

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
        """Submit a single PBS job."""
        # Use provided directories or fallback to sweep_dir
        if logs_dir is None:
            logs_dir = sweep_dir / "logs"
        if pbs_dir is None:
            pbs_dir = sweep_dir / "scripts"

        # Ensure directories exist
        logs_dir.mkdir(parents=True, exist_ok=True)
        pbs_dir.mkdir(parents=True, exist_ok=True)

        # Render job script from template
        template = self.jinja_env.get_template("sweep_single.sh.j2")
        script_content = template.render(
            job_name=job_name,
            walltime=self.walltime,
            resources=self.resources,
            sweep_dir=str(sweep_dir),
            logs_dir=str(logs_dir),
            sweep_id=sweep_id,
            python_path=self.python_path,
            script_path=self.script_path,
            project_dir=self.project_dir,
            params=params,
            parameters=self._params_to_string(params),
            wandb_group=wandb_group,
        )

        # Write job script
        job_script_path = pbs_dir / f"{job_name}.pbs"
        with open(job_script_path, "w") as f:
            f.write(script_content)

        # Submit job
        result = self._run_command(f"qsub {job_script_path}")
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to submit PBS job: {result.stderr}\nScript: {job_script_path}"
            )

        job_id = result.stdout.strip()
        logger.info(f"Submitted PBS job {job_id} for {job_name}")
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
        """Submit a PBS array job."""
        if logs_dir is None:
            logs_dir = sweep_dir / "logs"
        if pbs_dir is None:
            pbs_dir = sweep_dir / "scripts"

        # Ensure directories exist
        logs_dir.mkdir(parents=True, exist_ok=True)
        pbs_dir.mkdir(parents=True, exist_ok=True)

        # Create tasks subdirectory for array job outputs
        tasks_dir = sweep_dir / "tasks"
        tasks_dir.mkdir(exist_ok=True)

        # Save parameter combinations to JSON file
        params_file = pbs_dir / f"{sweep_id}_params.json"
        import json

        with open(params_file, "w") as f:
            json.dump(param_combinations, f, indent=2)

        # Render array job script
        template = self.jinja_env.get_template("sweep_array.sh.j2")
        script_content = template.render(
            job_name=f"{sweep_id}_array",
            walltime=self.walltime,
            resources=self.resources,
            array_range=f"1-{len(param_combinations)}",
            sweep_dir=str(sweep_dir),
            logs_dir=str(logs_dir),
            tasks_dir=str(tasks_dir),
            params_file=str(params_file),
            sweep_id=sweep_id,
            python_path=self.python_path,
            script_path=self.script_path,
            project_dir=self.project_dir,
            wandb_group=wandb_group,
        )

        # Write array job script
        array_script_path = pbs_dir / f"{sweep_id}_array.pbs"
        with open(array_script_path, "w") as f:
            f.write(script_content)

        # Submit array job
        result = self._run_command(f"qsub {array_script_path}")
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to submit PBS array job: {result.stderr}\nScript: {array_script_path}"
            )

        array_job_id = result.stdout.strip()
        logger.info(f"Submitted PBS array job {array_job_id} with {len(param_combinations)} tasks")
        return array_job_id

    def get_job_status(self, job_id: str) -> str:
        """Get PBS job status."""
        try:
            result = self._run_command(f"qstat -f {job_id}")
            if result.returncode != 0:
                # Job might be completed and out of queue
                return "completed"

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
                return "exiting"
            else:
                return "unknown"

        except Exception as e:
            logger.warning(f"Error checking PBS job status for {job_id}: {e}")
            return "unknown"
