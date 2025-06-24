"""Job management for different HPC systems."""

import subprocess
import json
import os
import sys
import threading
import time
import signal
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

        # Default to LocalJobManager if no HPC system detected
        return LocalJobManager(**kwargs)

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


class LocalJobManager(JobManager):
    """Local job manager for running sweeps on a single machine."""

    def __init__(
        self,
        walltime: str = "04:00:00",
        resources: str = "local",
        python_path: str = "python",
        script_path: str = "",
        project_dir: str = ".",
        max_parallel_jobs: int = 1,
        show_progress: bool = True,
        show_output: bool = False,
    ):
        super().__init__(walltime, resources, python_path, script_path, project_dir)
        self.system_type = "local"
        self.max_parallel_jobs = max_parallel_jobs
        self.running_processes = {}  # job_id -> subprocess.Popen
        self.job_counter = 0
        self.show_progress = show_progress
        self.show_output = show_output
        self.total_jobs_planned = 0
        self.jobs_completed = 0

        # Validate and fix paths for cross-machine compatibility
        self._validate_and_fix_paths()

        # Register cleanup handler for graceful shutdown
        import atexit

        atexit.register(self._cleanup_on_exit)

        # Register signal handlers for Ctrl+C and termination
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _validate_and_fix_paths(self):
        """Validate and fix paths for cross-machine compatibility."""
        # Fix project directory
        if not Path(self.project_dir).exists():
            print(
                f"Warning: Project directory {self.project_dir} not found, using current directory"
            )
            self.project_dir = str(Path.cwd())
        else:
            self.project_dir = str(Path(self.project_dir).resolve())

        # Fix script path - try to resolve relative to project_dir if it's just a filename
        if self.script_path:
            script_path = Path(self.script_path)
            if not script_path.exists():
                if not script_path.is_absolute():
                    # Try relative to project directory
                    potential_script = Path(self.project_dir) / self.script_path
                    if potential_script.exists():
                        self.script_path = str(potential_script)
                    else:
                        # Try to find script in common locations
                        from .path_detector import PathDetector

                        detector = PathDetector(Path(self.project_dir))
                        detected_script = detector.detect_train_script()
                        if detected_script:
                            print(
                                f"Warning: Script {self.script_path} not found, using detected script: {detected_script}"
                            )
                            self.script_path = str(detected_script)
                        else:
                            print(
                                f"Warning: Script {self.script_path} not found and no alternative detected"
                            )
                else:
                    print(f"Warning: Script path {self.script_path} not found")

        # Fix python path - ensure it exists
        if self.python_path and self.python_path != "python":
            if not Path(self.python_path).exists():
                print(
                    f"Warning: Python path {self.python_path} not found, falling back to 'python'"
                )
                self.python_path = "python"

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
        """Submit a single local job."""
        # Use provided directories or fallback to sweep_dir
        if logs_dir is None:
            logs_dir = sweep_dir / "logs"
        if pbs_dir is None:
            pbs_dir = sweep_dir / "local_scripts"

        # Ensure directories exist
        logs_dir.mkdir(parents=True, exist_ok=True)
        pbs_dir.mkdir(parents=True, exist_ok=True)

        # Create task directory for organized outputs
        task_dir = sweep_dir / "tasks" / f"task_{self.job_counter + 1}"
        task_dir.mkdir(parents=True, exist_ok=True)

        # Increment job counter
        self.job_counter += 1
        job_id = f"local_{sweep_id}_{self.job_counter}"

        # Determine the effective wandb group
        effective_wandb_group = wandb_group or sweep_id

        # Create a shell script for the job (for consistency with HPC)
        script_content = f"""#!/bin/bash
# Local job script for {job_name}
# Generated at {datetime.now()}

# Change to project directory
cd {self.project_dir}

# Create task info file
echo "Job ID: {job_id}" > {task_dir}/task_info.txt
echo "Task Directory: {task_dir}" >> {task_dir}/task_info.txt
echo "Start Time: $(date)" >> {task_dir}/task_info.txt
echo "Parameters: {self._params_to_string(params)}" >> {task_dir}/task_info.txt
echo "Status: RUNNING" >> {task_dir}/task_info.txt

# Store the command for reference
echo "{self.python_path} {self.script_path} {self._params_to_string(params)} output.dir={task_dir} wandb.group={effective_wandb_group}" > {task_dir}/command.txt

# Run the training script and capture output (with output directory set to task directory)
{self.python_path} {self.script_path} {self._params_to_string(params)} output.dir={task_dir} wandb.group={effective_wandb_group} 2>&1

# Update status on completion
if [ $? -eq 0 ]; then
    echo "Status: COMPLETED" >> {task_dir}/task_info.txt
    echo "End Time: $(date)" >> {task_dir}/task_info.txt
else
    echo "Status: FAILED" >> {task_dir}/task_info.txt
    echo "End Time: $(date)" >> {task_dir}/task_info.txt
fi
"""

        # Write job script
        script_path = pbs_dir / f"{job_name}.sh"
        with open(script_path, "w") as f:
            f.write(script_content)
        script_path.chmod(0o755)  # Make executable

        # Start the job as a subprocess
        log_file = logs_dir / f"{job_name}.log"
        error_file = logs_dir / f"{job_name}.err"

        if self.show_output:
            # Show output in real-time while also logging to files
            from threading import Thread

            def stream_output(pipe, file_handle, prefix=""):
                """Stream output from subprocess to both console and file."""
                for line in iter(pipe.readline, b""):
                    line_str = line.decode("utf-8", errors="replace")
                    file_handle.write(line_str)
                    file_handle.flush()
                    if prefix:
                        print(f"[{prefix}] {line_str.rstrip()}")
                    else:
                        print(line_str.rstrip())
                pipe.close()

            with open(log_file, "w") as log_f, open(error_file, "w") as err_f:
                process = subprocess.Popen(
                    ["/bin/bash", str(script_path)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=self.project_dir,
                    preexec_fn=os.setsid,
                    bufsize=1,
                    universal_newlines=False,
                )

                # Start threads to handle output streaming
                stdout_thread = Thread(
                    target=stream_output, args=(process.stdout, log_f, job_name)
                )
                stderr_thread = Thread(
                    target=stream_output,
                    args=(process.stderr, err_f, f"{job_name}-ERR"),
                )
                stdout_thread.daemon = True
                stderr_thread.daemon = True
                stdout_thread.start()
                stderr_thread.start()
        else:
            # Original behavior: redirect to log files
            with open(log_file, "w") as log_f, open(error_file, "w") as err_f:
                process = subprocess.Popen(
                    ["/bin/bash", str(script_path)],
                    stdout=log_f,
                    stderr=err_f,
                    cwd=self.project_dir,
                    preexec_fn=os.setsid,  # Create new process group for clean termination
                )

        # Store process reference
        self.running_processes[job_id] = {
            "process": process,
            "task_dir": task_dir,
            "start_time": datetime.now(),
            "job_name": job_name,
        }

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
        """Submit local array job (runs sequentially or in limited parallel)."""
        # Use provided directories or fallback to sweep_dir
        if logs_dir is None:
            logs_dir = sweep_dir / "logs"
        if pbs_dir is None:
            pbs_dir = sweep_dir / "local_scripts"

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

        # Initialize progress tracking
        self.total_jobs_planned = len(param_combinations)
        self.jobs_completed = 0

        # Generate array job ID
        array_job_id = f"local_array_{sweep_id}"

        if self.show_progress:
            print(
                f"Starting {len(param_combinations)} jobs with max {self.max_parallel_jobs} parallel..."
            )

        # Submit individual jobs for each parameter combination
        job_ids = []
        for i, params in enumerate(param_combinations):
            job_name = f"{sweep_id}_task_{i + 1:03d}"

            if self.show_progress:
                print(f"Submitting job {i + 1}/{len(param_combinations)}: {job_name}")

            job_id = self.submit_single_job(
                params, job_name, sweep_dir, sweep_id, wandb_group, pbs_dir, logs_dir
            )
            job_ids.append(job_id)

            # If we're limiting parallel jobs, wait for some to complete
            if len(self.running_processes) >= self.max_parallel_jobs:
                if self.show_progress:
                    print(
                        f"Reached max parallel jobs ({self.max_parallel_jobs}), waiting for completion..."
                    )
                self._wait_for_job_completion()

        # Store array job info
        self.running_processes[array_job_id] = {
            "type": "array",
            "subjobs": job_ids,
            "start_time": datetime.now(),
        }

        if self.show_progress:
            print(
                f"All {len(param_combinations)} jobs submitted. Use 'hsm monitor {sweep_id}' to track progress."
            )

        return array_job_id

    def get_job_status(self, job_id: str) -> str:
        """Get local job status."""
        if job_id not in self.running_processes:
            return "UNKNOWN"

        job_info = self.running_processes[job_id]

        if job_info.get("type") == "array":
            # For array jobs, check status of subjobs
            completed = 0
            failed = 0
            running = 0

            for subjob_id in job_info["subjobs"]:
                status = self.get_job_status(subjob_id)
                if status == "COMPLETED":
                    completed += 1
                elif status == "FAILED":
                    failed += 1
                elif status == "RUNNING":
                    running += 1

            total = len(job_info["subjobs"])
            if completed == total:
                return "COMPLETED"
            elif failed > 0:
                return (
                    f"MIXED ({completed} completed, {failed} failed, {running} running)"
                )
            else:
                return "RUNNING"

        process = job_info["process"]
        if process.poll() is None:
            return "RUNNING"
        elif process.returncode == 0:
            return "COMPLETED"
        else:
            return "FAILED"

    def _wait_for_job_completion(self):
        """Wait for at least one job to complete."""
        while len(self.running_processes) >= self.max_parallel_jobs:
            completed_jobs = []
            for job_id, job_info in self.running_processes.items():
                if job_info.get("type") == "array":
                    continue

                process = job_info["process"]
                if process.poll() is not None:  # Process has finished
                    completed_jobs.append(job_id)
                    self.jobs_completed += 1

                    if self.show_progress:
                        job_name = job_info.get("job_name", job_id)
                        status = "COMPLETED" if process.returncode == 0 else "FAILED"
                        print(
                            f"Job {job_name} {status} ({self.jobs_completed}/{self.total_jobs_planned})"
                        )

            # Remove completed jobs
            for job_id in completed_jobs:
                del self.running_processes[job_id]

            if completed_jobs:
                break

            # Sleep briefly before checking again
            time.sleep(1)

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

    def wait_for_all_jobs(self, use_progress_bar: bool = False):
        """Wait for all running jobs to complete."""
        if use_progress_bar and self.show_progress:
            self.monitor_with_progress_bar()
            return

        while self.running_processes:
            completed_jobs = []
            for job_id, job_info in self.running_processes.items():
                if job_info.get("type") == "array":
                    # Check if all subjobs are complete
                    all_complete = True
                    for subjob_id in job_info["subjobs"]:
                        if subjob_id in self.running_processes:
                            all_complete = False
                            break
                    if all_complete:
                        completed_jobs.append(job_id)
                else:
                    process = job_info["process"]
                    if process.poll() is not None:  # Process has finished
                        completed_jobs.append(job_id)

                        # Update progress tracking
                        if not hasattr(self, "_processed_jobs"):
                            self._processed_jobs = set()
                        if job_id not in self._processed_jobs:
                            self.jobs_completed += 1
                            self._processed_jobs.add(job_id)

                            if self.show_progress:
                                job_name = job_info.get("job_name", job_id)
                                status = (
                                    "COMPLETED" if process.returncode == 0 else "FAILED"
                                )
                                print(
                                    f"Job {job_name} {status} ({self.jobs_completed}/{self.total_jobs_planned})"
                                )

            # Remove completed jobs
            for job_id in completed_jobs:
                del self.running_processes[job_id]

            if not completed_jobs and self.running_processes:
                # Show periodic progress summary
                if self.show_progress and self.total_jobs_planned > 0:
                    self.show_progress_summary()
                # Sleep briefly before checking again
                time.sleep(2)

    def cancel_job(self, job_id: str, timeout: int = 10) -> bool:
        """Cancel a local job with robust process termination.

        Args:
            job_id: The job ID to cancel
            timeout: Maximum time to wait for graceful termination (seconds)

        Returns:
            True if job was successfully cancelled, False otherwise
        """
        if job_id not in self.running_processes:
            print(f"Job {job_id} not found in running processes")
            return False

        job_info = self.running_processes[job_id]

        if job_info.get("type") == "array":
            print(
                f"Cancelling array job {job_id} with {len(job_info['subjobs'])} subjobs..."
            )
            cancelled_count = 0

            # Cancel all subjobs
            for subjob_id in job_info["subjobs"]:
                if self.cancel_job(subjob_id, timeout):
                    cancelled_count += 1

            # Remove the array job from running processes
            del self.running_processes[job_id]

            print(
                f"Array job {job_id}: cancelled {cancelled_count}/{len(job_info['subjobs'])} subjobs"
            )
            return cancelled_count > 0
        else:
            return self._cancel_single_job(job_id, job_info, timeout)

    def _cancel_single_job(self, job_id: str, job_info: dict, timeout: int) -> bool:
        """Cancel a single job process with graceful termination."""
        process = job_info["process"]
        job_name = job_info.get("job_name", job_id)

        # Check if process is already dead
        if process.poll() is not None:
            print(f"Job {job_name} ({job_id}) was already terminated")
            self._cleanup_cancelled_job(job_id, job_info, "ALREADY_DEAD")
            return True

        print(f"Cancelling job {job_name} ({job_id})...")

        try:
            # Step 1: Try graceful termination (SIGTERM)
            print(f"  Sending SIGTERM to process group {process.pid}")
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)

            # Wait for graceful termination
            start_time = time.time()
            while time.time() - start_time < timeout:
                if process.poll() is not None:
                    print(f"  Job {job_name} terminated gracefully")
                    self._cleanup_cancelled_job(job_id, job_info, "CANCELLED")
                    return True
                time.sleep(0.5)

            # Step 2: Force kill if graceful termination failed
            print(f"  Graceful termination failed, sending SIGKILL")
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)

                # Wait a bit for force kill to take effect
                for _ in range(10):  # Wait up to 5 seconds
                    if process.poll() is not None:
                        print(f"  Job {job_name} force killed")
                        self._cleanup_cancelled_job(job_id, job_info, "FORCE_KILLED")
                        return True
                    time.sleep(0.5)

                # If we get here, the process is really stubborn
                print(f"  Warning: Process {process.pid} may still be running")
                self._cleanup_cancelled_job(job_id, job_info, "KILL_FAILED")
                return False

            except (ProcessLookupError, OSError) as e:
                print(f"  Process {process.pid} already died: {e}")
                self._cleanup_cancelled_job(job_id, job_info, "CANCELLED")
                return True

        except (ProcessLookupError, OSError) as e:
            print(f"  Failed to terminate process {process.pid}: {e}")
            # Process might have died on its own
            if process.poll() is not None:
                self._cleanup_cancelled_job(job_id, job_info, "CANCELLED")
                return True
            return False

    def _cleanup_cancelled_job(self, job_id: str, job_info: dict, status: str):
        """Clean up after job cancellation."""
        # Update task status file
        task_dir = job_info.get("task_dir")
        if task_dir and Path(task_dir).exists():
            try:
                with open(task_dir / "task_info.txt", "a") as f:
                    f.write(f"Status: {status}\n")
                    f.write(f"End Time: {datetime.now()}\n")
            except Exception as e:
                print(f"  Warning: Could not update task status file: {e}")

        # Remove from running processes
        if job_id in self.running_processes:
            del self.running_processes[job_id]

    def cancel_all_jobs(self, timeout: int = 10) -> dict:
        """Cancel all running jobs.

        Args:
            timeout: Maximum time to wait for graceful termination per job

        Returns:
            Dict with cancellation results
        """
        if not self.running_processes:
            print("No running jobs to cancel")
            return {"cancelled": 0, "failed": 0, "already_dead": 0}

        print(f"Cancelling {len(self.running_processes)} running jobs...")

        results = {"cancelled": 0, "failed": 0, "already_dead": 0}

        # Get a copy of job IDs since we'll be modifying the dict
        job_ids = list(self.running_processes.keys())

        for job_id in job_ids:
            if self.cancel_job(job_id, timeout):
                results["cancelled"] += 1
            else:
                results["failed"] += 1

        print(
            f"Cancellation complete: {results['cancelled']} cancelled, {results['failed']} failed"
        )
        return results

    def get_running_process_info(self) -> dict:
        """Get information about currently running processes."""
        info = {
            "total_jobs": len(self.running_processes),
            "running_jobs": 0,
            "completed_jobs": 0,
            "array_jobs": 0,
            "individual_jobs": 0,
            "process_pids": [],
        }

        for job_id, job_info in self.running_processes.items():
            if job_info.get("type") == "array":
                info["array_jobs"] += 1
            else:
                info["individual_jobs"] += 1
                process = job_info.get("process")
                if process:
                    if process.poll() is None:
                        info["running_jobs"] += 1
                        info["process_pids"].append(process.pid)
                    else:
                        info["completed_jobs"] += 1

        return info

    def show_progress_summary(self):
        """Display a summary of current progress."""
        if not self.show_progress:
            return

        info = self.get_running_process_info()
        if self.total_jobs_planned > 0:
            progress_pct = (self.jobs_completed / self.total_jobs_planned) * 100
            print(
                f"\nProgress: {self.jobs_completed}/{self.total_jobs_planned} ({progress_pct:.1f}%)"
            )

        print(f"Currently running: {info['running_jobs']} jobs")
        print(f"Max parallel: {self.max_parallel_jobs}")

        if info["running_jobs"] > 0:
            print("Running job PIDs:", info["process_pids"])

    def monitor_with_progress_bar(self, update_interval: int = 5):
        """Monitor jobs with a rich progress bar (requires rich library)."""
        try:
            from rich.console import Console
            from rich.progress import (
                Progress,
                SpinnerColumn,
                TextColumn,
                BarColumn,
                MofNCompleteColumn,
                TimeElapsedColumn,
            )
            from rich.live import Live
            from rich.table import Table
            import time

            console = Console()

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("•"),
                TimeElapsedColumn(),
                console=console,
                transient=False,
            ) as progress:
                task = progress.add_task(
                    "Processing jobs...", total=self.total_jobs_planned
                )

                while self.running_processes:
                    # Update completed count by checking finished processes
                    self._update_completed_count()

                    # Update progress bar
                    progress.update(task, completed=self.jobs_completed)

                    # Check if all jobs are done
                    running_count = sum(
                        1
                        for job_info in self.running_processes.values()
                        if job_info.get("type") != "array"
                        and job_info.get("process")
                        and job_info["process"].poll() is None
                    )

                    if (
                        running_count == 0
                        and self.jobs_completed >= self.total_jobs_planned
                    ):
                        break

                    time.sleep(update_interval)

                progress.update(task, completed=self.total_jobs_planned)
                console.print(f"✅ All {self.total_jobs_planned} jobs completed!")

        except ImportError:
            print(
                "Rich library not available, falling back to simple progress monitoring"
            )
            self.wait_for_all_jobs()

    def _update_completed_count(self):
        """Update the completed job count by checking finished processes."""
        completed_jobs = []
        for job_id, job_info in self.running_processes.items():
            if job_info.get("type") == "array":
                continue

            process = job_info.get("process")
            if (
                process
                and process.poll() is not None
                and job_id not in getattr(self, "_processed_jobs", set())
            ):
                completed_jobs.append(job_id)

        if not hasattr(self, "_processed_jobs"):
            self._processed_jobs = set()

        for job_id in completed_jobs:
            if job_id not in self._processed_jobs:
                self.jobs_completed += 1
                self._processed_jobs.add(job_id)

    def force_cleanup_all(self) -> dict:
        """Force cleanup of all processes without graceful termination."""
        print("Force cleanup: terminating all processes immediately...")

        results = {"killed": 0, "already_dead": 0, "failed": 0}

        job_ids = list(self.running_processes.keys())

        for job_id in job_ids:
            job_info = self.running_processes[job_id]

            if job_info.get("type") == "array":
                # Handle array jobs by processing subjobs
                for subjob_id in job_info.get("subjobs", []):
                    if subjob_id in self.running_processes:
                        subjob_info = self.running_processes[subjob_id]
                        result = self._force_kill_process(subjob_id, subjob_info)
                        results[result] += 1
                # Remove array job
                del self.running_processes[job_id]
            else:
                result = self._force_kill_process(job_id, job_info)
                results[result] += 1

        print(f"Force cleanup complete: {results}")
        return results

    def _force_kill_process(self, job_id: str, job_info: dict) -> str:
        """Force kill a single process immediately."""
        process = job_info.get("process")
        if not process:
            return "already_dead"

        if process.poll() is not None:
            # Process already dead
            self._cleanup_cancelled_job(job_id, job_info, "ALREADY_DEAD")
            return "already_dead"

        try:
            print(f"  Force killing process {process.pid}")
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)

            # Give it a moment to die
            time.sleep(0.1)

            if process.poll() is not None:
                self._cleanup_cancelled_job(job_id, job_info, "FORCE_KILLED")
                return "killed"
            else:
                print(f"  Warning: Process {process.pid} did not die after SIGKILL")
                self._cleanup_cancelled_job(job_id, job_info, "KILL_FAILED")
                return "failed"

        except (ProcessLookupError, OSError) as e:
            print(f"  Process {process.pid} already dead: {e}")
            self._cleanup_cancelled_job(job_id, job_info, "ALREADY_DEAD")
            return "already_dead"

    def _cleanup_on_exit(self):
        """Clean up resources on program exit."""
        print("Cleaning up running jobs on program exit...")
        self.cancel_all_jobs()

    def _signal_handler(self, signum, frame):
        """Handle signals for graceful shutdown."""
        print(f"Received signal {signum}, cleaning up...")
        self._cleanup_on_exit()
        sys.exit(0)
