"""Base class for HPC job managers."""

from abc import ABC, abstractmethod
import logging
from pathlib import Path
import shutil
import subprocess
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class HPCJobManager(ABC):
    """Abstract base class for HPC job managers (PBS, Slurm, etc.)."""

    def __init__(
        self,
        walltime: str = "04:00:00",
        resources: str = "",
        python_path: str = "python",
        script_path: str = "",
        project_dir: str = ".",
    ):
        self.walltime = walltime
        self.resources = resources
        self.python_path = python_path
        self.script_path = script_path
        self.project_dir = project_dir
        self.system_type = "hpc"

        # Validate required executables are available
        self._validate_system()

    @property
    @abstractmethod
    def scheduler_name(self) -> str:
        """Name of the scheduler (e.g., 'pbs', 'slurm')."""
        pass

    @property
    @abstractmethod
    def submit_command(self) -> str:
        """Command to submit jobs (e.g., 'qsub', 'sbatch')."""
        pass

    @property
    @abstractmethod
    def status_command(self) -> str:
        """Command to check job status (e.g., 'qstat', 'squeue')."""
        pass

    @abstractmethod
    def _validate_system(self):
        """Validate that the scheduler is available."""
        pass

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
        """Submit a single job to the scheduler."""
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
        """Submit an array job to the scheduler."""
        pass

    @abstractmethod
    def get_job_status(self, job_id: str) -> str:
        """Get status of a job."""
        pass

    @classmethod
    def auto_detect(cls, **kwargs) -> "HPCJobManager":
        """Auto-detect the available HPC system and return appropriate manager."""
        # Check for PBS
        if shutil.which("qsub") and shutil.which("qstat"):
            from .pbs_manager import PBSJobManager

            logger.info("Detected PBS/Torque system")
            return PBSJobManager(**kwargs)

        # Check for Slurm
        elif shutil.which("sbatch") and shutil.which("squeue"):
            from .slurm_manager import SlurmJobManager

            logger.info("Detected Slurm system")
            return SlurmJobManager(**kwargs)

        else:
            raise RuntimeError(
                "No supported HPC scheduler detected. "
                "Please ensure PBS/Torque or Slurm tools are available."
            )

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
        """Submit a complete parameter sweep."""
        if mode == "individual":
            job_ids = []
            for i, params in enumerate(param_combinations):
                job_name = f"{sweep_id}_task_{i + 1:03d}"
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

        elif mode == "array":
            job_id = self.submit_array_job(
                param_combinations, sweep_id, sweep_dir, wandb_group, pbs_dir, logs_dir
            )
            return [job_id]

        else:
            raise ValueError(f"Unsupported submission mode: {mode}")

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

    def _run_command(self, command: str, **kwargs) -> subprocess.CompletedProcess:
        """Run a shell command and return the result."""
        return subprocess.run(command, shell=True, capture_output=True, text=True, **kwargs)
