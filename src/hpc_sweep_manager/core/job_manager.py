"""Job management for different HPC systems."""

import subprocess
import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Any, Optional
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
from .utils import create_sweep_id


class JobManager(ABC):
    """Abstract base class for HPC job managers."""

    def __init__(
        self,
        walltime: str = "04:00:00",
        resources: str = "select=1:ncpus=4:mem=16gb",
        python_path: str = "python",
        script_path: str = "",
        project_dir: str = ".",
    ):
        self.walltime = walltime
        self.resources = resources
        self.python_path = python_path
        self.script_path = script_path
        self.project_dir = project_dir

        # Setup Jinja2 environment
        template_dir = Path(__file__).parent.parent / "templates"
        self.jinja_env = Environment(loader=FileSystemLoader(str(template_dir)))

    @abstractmethod
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
        """Submit a single job."""
        pass

    @abstractmethod
    def submit_array_job(
        self,
        param_combinations: List[Dict[str, Any]],
        sweep_id: str,
        sweep_dir: Path,
        wandb_group: Optional[str] = None,
        pbs_dir: Optional[Path] = None,
        logs_dir: Optional[Path] = None,
    ) -> str:
        """Submit an array job."""
        pass

    @abstractmethod
    def get_job_status(self, job_id: str) -> str:
        """Get job status."""
        pass

    @classmethod
    def auto_detect(cls, **kwargs) -> "JobManager":
        """Auto-detect the HPC system and return appropriate job manager."""
        # Try to detect PBS/Torque
        try:
            subprocess.run(
                ["qstat", "--version"], capture_output=True, check=True, timeout=5
            )
            return PBSJobManager(**kwargs)
        except (
            subprocess.CalledProcessError,
            FileNotFoundError,
            subprocess.TimeoutExpired,
        ):
            pass

        # Try to detect Slurm
        try:
            subprocess.run(
                ["sinfo", "--version"], capture_output=True, check=True, timeout=5
            )
            return SlurmJobManager(**kwargs)
        except (
            subprocess.CalledProcessError,
            FileNotFoundError,
            subprocess.TimeoutExpired,
        ):
            pass

        # Default to PBS if nothing else works
        return PBSJobManager(**kwargs)

    def submit_sweep(
        self,
        param_combinations: List[Dict[str, Any]],
        mode: str,
        sweep_dir: Path,
        sweep_id: str,
        wandb_group: Optional[str] = None,
        pbs_dir: Optional[Path] = None,
        logs_dir: Optional[Path] = None,
    ) -> List[str]:
        """Submit a complete sweep - either individual jobs or array job."""
        # Use provided directories or default to sweep_dir subdirectories
        if pbs_dir is None:
            pbs_dir = sweep_dir / "pbs_files"
            pbs_dir.mkdir(exist_ok=True)

        if logs_dir is None:
            logs_dir = sweep_dir / "logs"
            logs_dir.mkdir(exist_ok=True)

        if mode == "array":
            job_id = self.submit_array_job(
                param_combinations, sweep_id, sweep_dir, wandb_group, pbs_dir, logs_dir
            )
            return [job_id]
        else:  # individual mode
            job_ids = []
            for i, params in enumerate(param_combinations):
                job_name = f"{sweep_id}_job_{i + 1:03d}"
                job_id = self.submit_single_job(
                    params,
                    job_name,
                    sweep_dir,
                    sweep_id,
                    wandb_group,
                    pbs_dir,
                    logs_dir,
                )
                job_ids.append(job_id)
            return job_ids


class PBSJobManager(JobManager):
    """PBS/Torque job manager."""

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
            pbs_dir = sweep_dir / "pbs_files"

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

        # Write job script to pbs_dir instead of sweep_dir
        job_script_path = pbs_dir / f"{job_name}.pbs"
        with open(job_script_path, "w") as f:
            f.write(script_content)

        # Submit the job
        result = subprocess.run(
            ["qsub", str(job_script_path)], capture_output=True, text=True, check=True
        )
        return result.stdout.strip()

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
        # Use provided directories or fallback to sweep_dir
        if logs_dir is None:
            logs_dir = sweep_dir / "logs"
        if pbs_dir is None:
            pbs_dir = sweep_dir / "pbs_files"

        # Ensure directories exist
        logs_dir.mkdir(parents=True, exist_ok=True)
        pbs_dir.mkdir(parents=True, exist_ok=True)

        # Create tasks directory for organized task outputs
        tasks_dir = sweep_dir / "tasks"
        tasks_dir.mkdir(parents=True, exist_ok=True)

        # Save parameter combinations to JSON file
        params_file = sweep_dir / "parameter_combinations.json"
        indexed_combinations = [
            {"index": i + 1, "params": params}
            for i, params in enumerate(param_combinations)
        ]

        with open(params_file, "w") as f:
            json.dump(indexed_combinations, f, indent=2)

        # Render array job script from template
        template = self.jinja_env.get_template("sweep_array.sh.j2")
        script_content = template.render(
            sweep_id=sweep_id,
            num_jobs=len(param_combinations),
            walltime=self.walltime,
            resources=self.resources,
            sweep_dir=str(sweep_dir),
            logs_dir=str(logs_dir),
            tasks_dir=str(tasks_dir),
            python_path=self.python_path,
            script_path=self.script_path,
            project_dir=self.project_dir,
            wandb_group=wandb_group,
        )

        # Write array job script to pbs_dir instead of sweep_dir
        array_script_path = pbs_dir / f"{sweep_id}_array.pbs"
        with open(array_script_path, "w") as f:
            f.write(script_content)

        # Submit the array job
        result = subprocess.run(
            ["qsub", str(array_script_path)], capture_output=True, text=True, check=True
        )
        return result.stdout.strip()

    def get_job_status(self, job_id: str) -> str:
        """Get PBS job status."""
        try:
            result = subprocess.run(
                ["qstat", job_id], capture_output=True, text=True, check=True
            )
            lines = result.stdout.strip().split("\n")
            if len(lines) > 2:  # Skip header lines
                status_line = lines[2]
                status = status_line.split()[4]  # Job state is usually 5th column
                return status
        except subprocess.CalledProcessError:
            return "UNKNOWN"
        return "UNKNOWN"

    def _params_to_string(self, params: Dict[str, Any]) -> str:
        """Convert parameters dictionary to a readable string."""
        param_strs = []
        for key, value in params.items():
            param_strs.append(f"{key}={value}")
        return " ".join(param_strs)


class SlurmJobManager(JobManager):
    """Slurm job manager."""

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
            pbs_dir = sweep_dir / "slurm_files"  # Use slurm_files for Slurm

        # Ensure directories exist
        logs_dir.mkdir(parents=True, exist_ok=True)
        pbs_dir.mkdir(parents=True, exist_ok=True)

        # For Slurm single jobs, we can use a modified version of the array template
        # or create a separate template - for now use a simple implementation
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

        # Write job script to organized directory
        job_script_path = pbs_dir / f"{job_name}.slurm"
        with open(job_script_path, "w") as f:
            f.write(script_content)

        # Submit the job
        result = subprocess.run(
            ["sbatch", str(job_script_path)], capture_output=True, text=True, check=True
        )
        return result.stdout.strip()

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
        # Use provided directories or fallback to sweep_dir
        if logs_dir is None:
            logs_dir = sweep_dir / "logs"
        if pbs_dir is None:
            pbs_dir = sweep_dir / "slurm_files"  # Use slurm_files for Slurm

        # Ensure directories exist
        logs_dir.mkdir(parents=True, exist_ok=True)
        pbs_dir.mkdir(parents=True, exist_ok=True)

        # Create tasks directory for organized task outputs
        tasks_dir = sweep_dir / "tasks"
        tasks_dir.mkdir(parents=True, exist_ok=True)

        # Save parameter combinations to JSON file
        params_file = sweep_dir / "parameter_combinations.json"
        indexed_combinations = [
            {"index": i + 1, "params": params}
            for i, params in enumerate(param_combinations)
        ]

        with open(params_file, "w") as f:
            json.dump(indexed_combinations, f, indent=2)

        # Render array job script from template
        template = self.jinja_env.get_template("slurm_array.sh.j2")
        script_content = template.render(
            sweep_id=sweep_id,
            num_jobs=len(param_combinations),
            walltime=self.walltime,
            resources=self.resources,
            sweep_dir=str(sweep_dir),
            logs_dir=str(logs_dir),
            tasks_dir=str(tasks_dir),
            python_path=self.python_path,
            script_path=self.script_path,
            project_dir=self.project_dir,
            wandb_group=wandb_group,
        )

        # Write array job script to organized directory
        array_script_path = pbs_dir / f"{sweep_id}_array.slurm"
        with open(array_script_path, "w") as f:
            f.write(script_content)

        # Submit the array job
        result = subprocess.run(
            ["sbatch", str(array_script_path)],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def get_job_status(self, job_id: str) -> str:
        """Get Slurm job status."""
        try:
            result = subprocess.run(
                ["squeue", "-j", job_id, "-h", "-o", "%T"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError:
            return "UNKNOWN"

    def _params_to_string(self, params: Dict[str, Any]) -> str:
        """Convert parameters dictionary to command line arguments."""
        param_strs = []
        for key, value in params.items():
            param_strs.append(f'"{key}={value}"')
        return " ".join(param_strs)
