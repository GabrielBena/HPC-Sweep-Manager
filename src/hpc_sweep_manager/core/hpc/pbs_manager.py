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
        max_array_size: int = 10000,
    ):
        super().__init__(walltime, resources, python_path, script_path, project_dir)
        self.system_type = "pbs"
        self.max_array_size = max_array_size

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
            raise RuntimeError(
                f"PBS/Torque system not available or not properly configured: {e}"
            ) from e

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
        """Submit PBS array job(s).

        If the number of parameter combinations exceeds max_array_size,
        this will split them into multiple array job chunks, all sharing
        the same sweep_id.

        Returns:
            A string containing the job ID(s). If multiple chunks are submitted,
            returns comma-separated job IDs.
        """
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

        total_combinations = len(param_combinations)

        # Check if this is a completion run
        is_completion_run = (
            param_combinations
            and isinstance(param_combinations[0], dict)
            and "task_number" in param_combinations[0]
            and "params" in param_combinations[0]
        )

        # Check if we need to split into chunks
        if total_combinations <= self.max_array_size:
            # Submit as a single array job
            return self._submit_single_array_job(
                param_combinations=param_combinations,
                sweep_id=sweep_id,
                sweep_dir=sweep_dir,
                wandb_group=wandb_group,
                pbs_dir=pbs_dir,
                logs_dir=logs_dir,
                tasks_dir=tasks_dir,
                chunk_id=None,
                global_offset=0,
                is_completion_run=is_completion_run,
            )

        # Split into multiple chunks
        num_chunks = (total_combinations + self.max_array_size - 1) // self.max_array_size
        logger.info(
            f"Splitting {total_combinations} tasks into {num_chunks} array job chunks "
            f"(max_array_size={self.max_array_size})"
        )

        job_ids = []
        for chunk_idx in range(num_chunks):
            start_idx = chunk_idx * self.max_array_size
            end_idx = min(start_idx + self.max_array_size, total_combinations)
            chunk_combinations = param_combinations[start_idx:end_idx]

            chunk_id = chunk_idx + 1
            logger.info(
                f"Submitting chunk {chunk_id}/{num_chunks} with {len(chunk_combinations)} tasks "
                f"(global indices {start_idx + 1}-{end_idx})"
            )

            job_id = self._submit_single_array_job(
                param_combinations=chunk_combinations,
                sweep_id=sweep_id,
                sweep_dir=sweep_dir,
                wandb_group=wandb_group,
                pbs_dir=pbs_dir,
                logs_dir=logs_dir,
                tasks_dir=tasks_dir,
                chunk_id=chunk_id,
                global_offset=start_idx,
                is_completion_run=is_completion_run,
            )
            job_ids.append(job_id)

        logger.info(f"Submitted {num_chunks} array job chunks for sweep {sweep_id}")
        return ",".join(job_ids)

    def _submit_single_array_job(
        self,
        param_combinations: List[Dict[str, Any]],
        sweep_id: str,
        sweep_dir: Path,
        wandb_group: Optional[str],
        pbs_dir: Path,
        logs_dir: Path,
        tasks_dir: Path,
        chunk_id: Optional[int] = None,
        global_offset: int = 0,
        is_completion_run: bool = False,
    ) -> str:
        """Submit a single PBS array job chunk.

        Args:
            param_combinations: Parameter combinations for this chunk
            sweep_id: Sweep identifier (shared across all chunks)
            sweep_dir: Sweep directory
            wandb_group: W&B group name
            pbs_dir: Directory for PBS scripts
            logs_dir: Directory for logs
            tasks_dir: Directory for task outputs
            chunk_id: Chunk identifier (None if not chunked)
            global_offset: Global index offset for this chunk

        Returns:
            The PBS job ID
        """
        import json

        # Determine job name and file suffix
        if chunk_id is not None:
            job_name = f"{sweep_id}_array_chunk_{chunk_id}"
            file_suffix = f"_chunk_{chunk_id}"
        else:
            job_name = f"{sweep_id}_array"
            file_suffix = ""

        # Save parameter combinations to JSON file
        # Use local indices to match PBS_ARRAY_INDEX, but also store global index
        params_file = sweep_dir / f"parameter_combinations{file_suffix}.json"

        if is_completion_run:
            # For completion runs, use the provided task_number as global_index
            indexed_combinations = [
                {
                    "index": i + 1,  # Local index for PBS_ARRAY_INDEX matching
                    "global_index": combo["task_number"],  # Use completion task number
                    "params": combo["params"],  # Extract actual params from nested dict
                }
                for i, combo in enumerate(param_combinations)
            ]
        else:
            # For regular sweeps, use sequential numbering with offset
            indexed_combinations = [
                {
                    "index": i + 1,  # Local index for PBS_ARRAY_INDEX matching
                    "global_index": global_offset + i + 1,  # Global index for unique naming
                    "params": params,
                }
                for i, params in enumerate(param_combinations)
            ]

        with open(params_file, "w") as f:
            json.dump(indexed_combinations, f, indent=2)

        # Render array job script
        template = self.jinja_env.get_template("sweep_array.sh.j2")
        script_content = template.render(
            job_name=job_name,
            walltime=self.walltime,
            resources=self.resources,
            num_jobs=len(param_combinations),
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
        array_script_path = pbs_dir / f"{job_name}.pbs"
        with open(array_script_path, "w") as f:
            f.write(script_content)

        # Submit array job
        result = self._run_command(f"qsub {array_script_path}")
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to submit PBS array job: {result.stderr}\nScript: {array_script_path}"
            )

        array_job_id = result.stdout.strip()
        logger.info(
            f"Submitted PBS job {array_job_id} ({job_name}) with {len(param_combinations)} tasks"
        )

        # Create source_mapping.yaml for consistency with distributed sweeps
        # Pass is_completion_run flag to handle task numbering correctly
        self._create_source_mapping(
            sweep_dir, sweep_id, param_combinations, global_offset, is_completion_run
        )

        return array_job_id

    def get_job_status(self, job_id: str) -> str:
        """Get PBS job status.

        If job_id contains comma-separated IDs (from chunked array jobs),
        returns a combined status based on all chunks.
        """
        # Handle comma-separated job IDs from chunked array jobs
        if "," in job_id:
            job_ids = [jid.strip() for jid in job_id.split(",")]
            statuses = [self._get_single_job_status(jid) for jid in job_ids]

            # Determine combined status
            if any(s == "running" for s in statuses):
                return "running"
            elif any(s == "queued" for s in statuses):
                return "queued"
            elif any(s == "held" for s in statuses):
                return "held"
            elif any(s == "exiting" for s in statuses):
                return "exiting"
            elif all(s == "completed" for s in statuses):
                return "completed"
            else:
                return "unknown"
        else:
            return self._get_single_job_status(job_id)

    def _get_single_job_status(self, job_id: str) -> str:
        """Get status of a single PBS job."""
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

    def _create_source_mapping(
        self,
        sweep_dir: Path,
        sweep_id: str,
        param_combinations: List[Dict[str, Any]],
        global_offset: int = 0,
        is_completion_run: bool = False,
    ):
        """Create source_mapping.yaml for PBS array jobs to unify with distributed sweeps."""
        try:
            from datetime import datetime

            import yaml

            mapping_file = sweep_dir / "source_mapping.yaml"

            # Load existing mapping if this is a completion run
            if mapping_file.exists():
                with open(mapping_file) as f:
                    mapping_data = yaml.safe_load(f) or {}
            else:
                mapping_data = {
                    "sweep_metadata": {},
                    "task_assignments": {},
                }

            # Determine total tasks and update metadata
            if is_completion_run:
                # For completion runs, preserve existing total_tasks
                total_tasks = mapping_data["sweep_metadata"].get("total_tasks", 0)
            else:
                # For new sweeps, calculate from combinations
                total_tasks = max(
                    len(param_combinations) + global_offset,
                    mapping_data["sweep_metadata"].get("total_tasks", 0),
                )

            mapping_data["sweep_metadata"].update(
                {
                    "total_tasks": total_tasks,
                    "compute_sources": ["HPC"],  # PBS array jobs run on HPC cluster
                    "strategy": "pbs_array",
                    "timestamp": datetime.now().isoformat(),
                }
            )

            # Add task assignments
            if is_completion_run:
                # For completion runs, use task_number from each combination
                for combo in param_combinations:
                    task_number = combo["task_number"]
                    task_name = f"task_{task_number:03d}"

                    # Only add if not already present
                    if task_name not in mapping_data["task_assignments"]:
                        mapping_data["task_assignments"][task_name] = {
                            "compute_source": "HPC",
                            "status": "PENDING",
                            "start_time": None,
                            "complete_time": None,
                        }
            else:
                # For regular sweeps, use sequential numbering with offset
                for i, _ in enumerate(param_combinations):
                    task_number = global_offset + i + 1
                    task_name = f"task_{task_number:03d}"

                    # Only add if not already present
                    if task_name not in mapping_data["task_assignments"]:
                        mapping_data["task_assignments"][task_name] = {
                            "compute_source": "HPC",
                            "status": "PENDING",
                            "start_time": None,
                            "complete_time": None,
                        }

            # Save to YAML file
            with open(mapping_file, "w") as f:
                yaml.dump(mapping_data, f, default_flow_style=False, indent=2)

            logger.debug(f"Created source mapping for PBS array job: {mapping_file}")

        except Exception as e:
            logger.warning(f"Failed to create source mapping: {e}")
            # Don't fail the job submission if mapping creation fails
