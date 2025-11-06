"""Sweep execution CLI commands."""

from datetime import datetime
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click
from rich.console import Console

if TYPE_CHECKING:
    from ..core.common.config import HSMConfig

import asyncio

from ..core.common.config import HSMConfig
from ..core.common.config_parser import SweepConfig
from ..core.common.sweep_tracker import SweepTaskTracker
from ..core.local.local_manager import LocalJobManager
from ..core.remote.discovery import RemoteDiscovery
from ..core.remote.remote_manager import RemoteJobManager
from .common import common_options

logger = logging.getLogger(__name__)


class RemoteJobManagerWrapper:
    """Wrapper to make RemoteJobManager compatible with sync JobManager interface."""

    def __init__(
        self,
        remote_job_manager: RemoteJobManager,
        verify_sync: bool = True,
        auto_sync: bool = False,
    ):
        self.remote_manager = remote_job_manager
        self.system_type = "remote"
        self.verify_sync = verify_sync
        self.auto_sync = auto_sync
        self.task_tracker = None  # Will be initialized when sweep starts
        self.is_completion_run = False

    def submit_sweep(
        self, param_combinations, mode, sweep_dir, sweep_id, wandb_group=None, **kwargs
    ):
        """Submit a complete sweep to remote machine."""
        # Run async setup and job submission
        return asyncio.run(
            self._async_submit_sweep(param_combinations, mode, sweep_dir, sweep_id, wandb_group)
        )

    async def _async_submit_sweep(self, param_combinations, mode, sweep_dir, sweep_id, wandb_group):
        """Async version of submit_sweep."""
        # Initialize task tracker for unified sweep tracking (only if not already set)
        if not hasattr(self, "task_tracker") or self.task_tracker is None:
            self.task_tracker = SweepTaskTracker(sweep_dir, sweep_id)
            logger.debug("Created new task tracker for RemoteJobManagerWrapper")
        else:
            logger.debug("Using injected task tracker for RemoteJobManagerWrapper")

        self.task_tracker.initialize_sweep(
            total_tasks=len(param_combinations), compute_source="remote", mode="remote"
        )

        # Setup remote environment first with sync verification
        setup_success = await self.remote_manager.setup_remote_environment(
            verify_sync=self.verify_sync, auto_sync=self.auto_sync
        )
        if not setup_success:
            raise Exception("Failed to setup remote environment")

        # Pass completion run flag to the remote manager
        if hasattr(self, "is_completion_run"):
            self.remote_manager.is_completion_run = self.is_completion_run

        # Submit jobs with parallel control and wait for completion
        job_ids = await self.remote_manager.submit_sweep(param_combinations, sweep_id, wandb_group)

        # Register tasks in tracker
        if self.is_completion_run:
            # For completion, param_combinations are dicts with 'task_number' or 'task_index' and 'params'
            task_names = []
            for combo in param_combinations:
                if isinstance(combo, dict) and "task_number" in combo:
                    # Use task_number for proper naming in completion runs
                    task_names.append(f"task_{combo['task_number']:03d}")
                elif isinstance(combo, dict) and "task_index" in combo:
                    # Fallback to task_index (old format)
                    task_names.append(f"task_{combo['task_index'] + 1:03d}")
                else:
                    # Fallback for unexpected format
                    task_names.append(f"task_{len(task_names) + 1:03d}")
        else:
            task_names = [f"task_{i + 1:03d}" for i in range(len(param_combinations))]

        self.task_tracker.register_task_batch(
            task_names=task_names, compute_source="remote", job_ids=job_ids
        )
        self.task_tracker.save_mapping()

        # Wait for all jobs to complete before returning
        await self.remote_manager.wait_for_all_jobs()

        # Update final task statuses by syncing with actual task directories
        self.task_tracker.sync_with_task_directories(force_update=True)
        self.task_tracker.save_mapping()

        return job_ids

    def get_job_status(self, job_id):
        """Get job status (sync wrapper)."""
        return asyncio.run(self.remote_manager.get_job_status(job_id))

    def collect_results(self):
        """Collect results from remote machine."""
        return asyncio.run(self.remote_manager.collect_results())

    def _params_to_string(self, params):
        """Convert parameters to string format (delegate to remote manager)."""
        return self.remote_manager._params_to_string(params)


def create_remote_job_manager_wrapper(
    remote_name,
    hsm_config,
    console,
    logger,
    sweep_dir,
    parallel_jobs=None,
    verify_sync=True,
    auto_sync=False,
):
    """Create a wrapper around RemoteJobManager that works with the existing sweep interface."""
    try:
        # Get remote configuration
        distributed_config = hsm_config.config_data.get("distributed", {})
        remotes = distributed_config.get("remotes", {})

        if remote_name not in remotes:
            console.print(f"[red]Error: Remote '{remote_name}' not found in hsm_config.yaml[/red]")
            console.print("Available remotes:")
            for name in remotes.keys():
                console.print(f"  - {name}")
            return None

        # Discover remote configuration
        logger.info(f"Discovering configuration for remote: {remote_name}")
        console.print(f"[cyan]Discovering configuration for remote: {remote_name}[/cyan]")

        # Run discovery
        discovery = RemoteDiscovery(hsm_config.config_data)
        remote_info = remotes[remote_name].copy()
        remote_info["name"] = remote_name

        # This is async, so we need to run it
        remote_config = asyncio.run(discovery.discover_remote_config(remote_info))

        if not remote_config:
            console.print(
                f"[red]Error: Failed to discover configuration for remote '{remote_name}'[/red]"
            )
            console.print("Make sure the remote machine is accessible and has hsm_config.yaml")
            return None

        console.print("[green]✓ Remote configuration discovered successfully[/green]")

        # Use the provided sweep directory instead of creating a new one
        # Just ensure it exists
        sweep_dir.mkdir(parents=True, exist_ok=True)

        # Determine max parallel jobs for remote execution
        max_parallel_jobs = 4  # Default
        if parallel_jobs is not None:
            max_parallel_jobs = parallel_jobs
        elif hsm_config:
            # Get from remote config or fall back to local config
            remote_max = remotes[remote_name].get("max_parallel_jobs")
            if remote_max:
                max_parallel_jobs = remote_max
            else:
                max_parallel_jobs = hsm_config.get_max_array_size() or 4

        console.print(f"[cyan]Max parallel jobs on remote: {max_parallel_jobs}[/cyan]")
        if not verify_sync:
            console.print("[yellow]⚠ Project sync verification disabled[/yellow]")
        if auto_sync:
            console.print(
                "[cyan]Auto-sync enabled: mismatched files will be automatically synced[/cyan]"
            )

        # Create RemoteJobManager with the provided sweep directory
        remote_job_manager = RemoteJobManager(
            remote_config,
            sweep_dir,
            max_parallel_jobs=max_parallel_jobs,
            show_progress=True,
        )

        # Return wrapper with sync verification setting
        return RemoteJobManagerWrapper(
            remote_job_manager, verify_sync=verify_sync, auto_sync=auto_sync
        )

    except Exception as e:
        console.print(f"[red]Error setting up remote job manager: {e}[/red]")
        logger.error(f"Remote job manager setup failed: {e}")
        return None


def _load_and_validate_config(
    config_path: Path, console: Console, logger: logging.Logger
) -> Optional["SweepConfig"]:
    """Load and validate sweep configuration.

    Returns:
        SweepConfig if valid, None if validation failed
    """
    from ..core.common.config import SweepConfig

    try:
        # Load sweep configuration
        config = SweepConfig.from_yaml(config_path)
        logger.info(f"Loaded sweep config from {config_path}")

        # Validate configuration
        errors = config.validate()
        if errors:
            console.print("[red]Configuration validation failed:[/red]")
            for error in errors:
                console.print(f"  - {error}")
            return None

        return config
    except FileNotFoundError:
        console.print(f"[red]Error: Sweep config file not found: {config_path}[/red]")
        return None
    except Exception as e:
        console.print(f"[red]Error loading configuration: {e}[/red]")
        logger.error(f"Configuration loading failed: {e}")
        return None


def _generate_parameter_combinations(
    config: "SweepConfig", max_runs: Optional[int], count_only: bool, console: Console
) -> Optional[list]:
    """Generate and display parameter combinations.

    Returns:
        List of parameter combinations, or None if count_only=True
    """
    from rich.table import Table

    from ..core.common.param_generator import ParameterGenerator

    # Generate parameter combinations
    generator = ParameterGenerator(config)

    if count_only:
        total_combinations = generator.count_combinations()
        console.print(f"[green]Total parameter combinations: {total_combinations}[/green]")
        return None

    combinations = generator.generate_combinations(max_runs)

    # Show parameter info
    info = generator.get_parameter_info()

    table = Table(title="Sweep Information")
    table.add_column("Parameter", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Values", style="green")

    for param_name, param_info in info["grid_parameters"].items():
        table.add_row(param_name, "Grid", str(param_info["values"])[:50] + "...")

    for param_name, param_info in info["paired_parameters"].items():
        table.add_row(param_name, "Paired", str(param_info["values"])[:50] + "...")

    console.print(table)
    console.print(f"\n[bold]Total combinations to run: {len(combinations)}[/bold]")

    return combinations


def _detect_project_paths(
    hsm_config: Optional["HSMConfig"], sweep_config: Optional["SweepConfig"] = None
) -> tuple[str, str, str]:
    """Detect or get project paths for execution.

    Returns:
        Tuple of (python_path, script_path, project_dir)
    """
    from ..core.common.path_detector import PathDetector

    detector = PathDetector()

    # First determine project dir and python path
    if hsm_config:
        python_path = hsm_config.get_default_python_path() or detector.detect_python_path()
        project_dir = hsm_config.get_project_root() or str(Path.cwd())
    else:
        python_path = detector.detect_python_path()
        project_dir = str(Path.cwd())

    # Priority order for script: sweep_config > hsm_config > auto-detect
    # 1. Try sweep config script first
    if sweep_config and sweep_config.script:
        script_path = sweep_config.script
        # If script path is relative, resolve it relative to project root
        script_path_obj = Path(script_path)
        if not script_path_obj.is_absolute():
            script_path = str(Path(project_dir) / script_path)
    # 2. Then try HSM config
    elif hsm_config:
        script_path = hsm_config.get_default_script_path() or detector.detect_train_script()
    # 3. Fall back to auto-detection
    else:
        script_path = detector.detect_train_script()

    return python_path, script_path, project_dir


def _create_job_manager(
    mode: str,
    walltime: str,
    resources: str,
    python_path: str,
    script_path: str,
    project_dir: str,
    parallel_jobs: Optional[int],
    no_progress: bool,
    show_output: bool,
    hsm_config: Optional["HSMConfig"],
    remote: Optional[str],
    console: Console,
) -> tuple[Optional[object], str]:
    """Create appropriate job manager based on execution mode.

    Returns:
        Tuple of (job_manager, updated_mode)
    """
    from ..core.hpc.hpc_base import HPCJobManager

    if mode == "local":
        # For local mode, determine parallel jobs from CLI, HSM config, or default
        max_parallel_jobs = 1
        if parallel_jobs is not None:
            max_parallel_jobs = parallel_jobs
        elif hsm_config:
            max_parallel_jobs = hsm_config.get_max_array_size() or 1
            # For local execution, limit to reasonable number
            max_parallel_jobs = min(max_parallel_jobs, 8)

        job_manager = LocalJobManager(
            walltime=walltime,
            resources=resources,
            python_path=python_path,
            script_path=script_path,
            project_dir=project_dir,
            max_parallel_jobs=max_parallel_jobs,
            show_progress=not no_progress,  # Enable progress tracking unless disabled
            show_output=show_output,  # Show output if requested
        )

    elif mode == "remote":
        # Remote job execution on a single machine
        if not hsm_config:
            console.print("[red]Error: hsm_config.yaml required for remote mode[/red]")
            return None, mode

        if not remote:
            console.print("[red]Error: --remote MACHINE_NAME required for remote mode[/red]")
            console.print("Available remotes:")
            remotes = hsm_config.config_data.get("distributed", {}).get("remotes", {})
            for name in remotes.keys():
                console.print(f"  - {name}")
            return None, mode

        # Remote job manager will be created later with proper sweep directory
        job_manager = None  # Will be created after sweep directory is set up

    elif mode == "distributed":
        # Distributed job execution across multiple sources
        if not hsm_config:
            console.print("[red]Error: hsm_config.yaml required for distributed mode[/red]")
            return None, mode

        # Distributed job manager will be created later with proper sweep directory
        job_manager = None  # Will be created after sweep directory is set up

    elif mode == "auto" or mode in ["individual", "array"]:
        # Auto-detect HPC system, fall back to local if none found
        try:
            job_manager = HPCJobManager.auto_detect(
                walltime=walltime,
                resources=resources,
                python_path=python_path,
                script_path=script_path,
                project_dir=project_dir,
            )
            # Update mode to reflect what was detected
            if mode == "auto":
                mode = "array"  # Default to array mode for HPC systems
                console.print(f"[green]Auto-detected HPC system, using {mode} mode[/green]")

        except RuntimeError as e:
            # No HPC system found, fall back to local mode
            if mode == "auto":
                console.print("[yellow]No HPC system detected, falling back to local mode[/yellow]")
                mode = "local"

                # For local mode, determine parallel jobs from CLI, HSM config, or default
                max_parallel_jobs = 4  # Better default for auto-detected local
                if parallel_jobs is not None:
                    max_parallel_jobs = parallel_jobs
                elif hsm_config:
                    max_parallel_jobs = hsm_config.get_max_array_size() or 4
                    # For local execution, limit to reasonable number
                    max_parallel_jobs = min(max_parallel_jobs, 8)

                job_manager = LocalJobManager(
                    walltime=walltime,
                    resources=resources,
                    python_path=python_path,
                    script_path=script_path,
                    project_dir=project_dir,
                    max_parallel_jobs=max_parallel_jobs,
                    show_progress=not no_progress,
                    show_output=show_output,
                )
            else:
                # User explicitly requested array/individual mode but no HPC found
                console.print(f"[red]Error: {e}[/red]")
                console.print("[yellow]Consider using --mode local or --mode auto[/yellow]")
                return None, mode
    else:
        console.print(f"[red]Error: Unknown mode '{mode}'[/red]")
        return None, mode

    return job_manager, mode


def _handle_dry_run(
    mode: str,
    remote: Optional[str],
    job_manager: Optional[object],
    hsm_config: Optional["HSMConfig"],
    walltime: str,
    resources: str,
    python_path: str,
    script_path: str,
    project_dir: str,
    group: Optional[str],
    combinations: list,
    console: Console,
) -> None:
    """Handle dry run execution by displaying configuration and sample commands."""
    from datetime import datetime

    console.print("\n[yellow]DRY RUN - No jobs will be submitted[/yellow]")

    # Show job configuration
    console.print("\n[bold]Job Configuration:[/bold]")
    if mode == "remote":
        console.print("  Execution System: REMOTE")
        console.print(f"  Mode: {mode} (remote execution on {remote})")
        console.print("  Note: Remote configuration will be discovered during actual execution")
    elif mode == "distributed":
        console.print("  Execution System: DISTRIBUTED")
        console.print(f"  Mode: {mode} (distributed execution across multiple sources)")
        console.print("  Note: Compute sources will be discovered during actual execution")
    elif job_manager:
        console.print(f"  Execution System: {job_manager.system_type.upper()}")
        if mode == "local":
            console.print(
                f"  Mode: {mode} (local execution with up to {job_manager.max_parallel_jobs} parallel jobs)"
            )
        else:
            console.print(
                f"  Mode: {mode} ({'array job' if mode == 'array' else 'individual jobs'})"
            )
    walltime_source = " (from hsm_config.yaml)" if hsm_config else " (default)"
    resources_source = " (from hsm_config.yaml)" if hsm_config else " (default)"
    console.print(f"  Walltime: {walltime}{walltime_source}")
    console.print(f"  Resources: {resources}{resources_source}")
    console.print(f"  Python Path: {python_path}")
    console.print(f"  Script Path: {script_path}")
    console.print(f"  Project Directory: {project_dir}")
    if group:
        console.print(f"  W&B Group: {group}")

    # Show first few combinations with their command lines
    console.print("\n[bold]First 3 parameter combinations:[/bold]")
    for i, combo in enumerate(combinations[:3], 1):
        console.print(f"  {i}. {combo}")

        # Generate the command line that would be executed
        if job_manager:
            params_str = job_manager._params_to_string(combo)
        else:
            # For remote mode, use a basic parameter conversion
            param_strs = []
            for key, value in combo.items():
                if isinstance(value, (list, tuple)):
                    value_str = str(list(value))
                    param_strs.append(f'"{key}={value_str}"')
                elif value is None:
                    param_strs.append(f'"{key}=null"')
                elif isinstance(value, bool):
                    param_strs.append(f'"{key}={str(value).lower()}"')
                elif isinstance(value, str) and (" " in value or "," in value):
                    param_strs.append(f'"{key}={value}"')
                else:
                    param_strs.append(f'"{key}={value}"')
            params_str = " ".join(param_strs)

        # Use sweep_id as fallback for wandb group (consistent with actual execution)
        effective_group = group or f"sweep_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        wandb_group_str = f"wandb.group={effective_group}"
        full_command = f"{python_path} {script_path} {params_str} {wandb_group_str}"

        console.print(f"     [dim]Command: {full_command}[/dim]")
        console.print()  # Add spacing between combinations


def _setup_sweep_directories_and_managers(
    mode: str,
    remote: Optional[str],
    job_manager: Optional[object],
    hsm_config: Optional["HSMConfig"],
    no_verify_sync: bool,
    auto_sync: bool,
    no_progress: bool,
    parallel_jobs: Optional[int],
    console: Console,
    logger: logging.Logger,
) -> tuple[str, Path, Path, Path, Optional[object]]:
    """Set up sweep directories and create remote/distributed job managers if needed.

    Returns:
        Tuple of (sweep_id, sweep_dir, scripts_dir, logs_dir, updated_job_manager)
    """
    from ..core.common.utils import create_sweep_id

    # Generate sweep ID
    sweep_id = create_sweep_id()
    console.print(f"\n[green]Sweep ID: {sweep_id}[/green]")

    # Create sweep directory
    sweep_dir = Path("sweeps") / "outputs" / sweep_id
    sweep_dir.mkdir(parents=True, exist_ok=True)
    console.print(f"Sweep directory: {sweep_dir}")

    # For remote mode, we need to update the job manager with the correct sweep directory
    if mode == "remote":
        # Create a RemoteJobManagerWrapper with the proper sweep directory
        job_manager = create_remote_job_manager_wrapper(
            remote_name=remote,
            hsm_config=hsm_config,
            console=console,
            logger=logger,
            sweep_dir=sweep_dir,  # Pass the actual sweep directory
            parallel_jobs=parallel_jobs,
            verify_sync=not no_verify_sync,
            auto_sync=auto_sync,
        )
        if not job_manager:
            return None, None, None, None, None  # Error already displayed

    elif mode == "distributed":
        # Create a DistributedSweepWrapper with the proper sweep directory
        try:
            from ..core.distributed.wrapper import create_distributed_sweep_wrapper

            job_manager = create_distributed_sweep_wrapper(
                hsm_config=hsm_config,
                console=console,
                logger=logger,
                sweep_dir=sweep_dir,
                show_progress=not no_progress,
            )
        except Exception as e:
            console.print(f"[red]Error creating distributed job manager: {e}[/red]")
            return None, None, None, None, None

    # Create subdirectories for organization
    if mode == "local":
        scripts_dir = sweep_dir / "local_scripts"
        dir_name = "Local scripts"
    elif mode == "remote":
        scripts_dir = sweep_dir / "remote_scripts"
        dir_name = "Remote scripts"
    elif mode == "distributed":
        scripts_dir = sweep_dir / "distributed_scripts"
        dir_name = "Distributed scripts"
    elif job_manager.system_type == "slurm":
        scripts_dir = sweep_dir / "slurm_files"
        dir_name = "Slurm files"
    else:
        scripts_dir = sweep_dir / "pbs_files"
        dir_name = "PBS files"

    scripts_dir.mkdir(exist_ok=True)
    logs_dir = sweep_dir / "logs"
    logs_dir.mkdir(exist_ok=True)

    console.print(f"{dir_name} will be stored in: {scripts_dir}")
    console.print(f"Job logs will be stored in: {logs_dir}")

    return sweep_id, sweep_dir, scripts_dir, logs_dir, job_manager


def _submit_and_track_jobs(
    job_manager: object,
    combinations: list,
    mode: str,
    sweep_dir: Path,
    sweep_id: str,
    group: Optional[str],
    scripts_dir: Path,
    logs_dir: Path,
    config_path: Path,
    walltime: str,
    resources: str,
    console: Console,
    logger: logging.Logger,
) -> list:
    """Submit jobs and create tracking files.

    Returns:
        List of job IDs
    """
    from datetime import datetime
    import shutil

    # Save sweep config for reference
    config_backup = sweep_dir / "sweep_config.yaml"
    shutil.copy2(config_path, config_backup)

    # Submit jobs
    console.print(f"\n[bold]Submitting {len(combinations)} jobs in {mode} mode...[/bold]")

    # Handle async submission for LocalJobManager
    if isinstance(job_manager, LocalJobManager):
        import asyncio

        # Use array mode for LocalJobManager to enable proper parallel execution control
        local_mode = "array" if job_manager.max_parallel_jobs > 1 else "individual"

        job_ids = asyncio.run(
            job_manager.submit_sweep(
                param_combinations=combinations,
                mode=local_mode,
                sweep_dir=sweep_dir,
                sweep_id=sweep_id,
                wandb_group=group,
                pbs_dir=scripts_dir,
                logs_dir=logs_dir,
            )
        )
    else:
        job_ids = job_manager.submit_sweep(
            param_combinations=combinations,
            mode=mode,
            sweep_dir=sweep_dir,
            sweep_id=sweep_id,
            wandb_group=group,
            pbs_dir=scripts_dir,
            logs_dir=logs_dir,
        )

    console.print(f"\n[green]Successfully submitted {len(job_ids)} job(s):[/green]")
    for job_id in job_ids:
        console.print(f"  - {job_id}")

    # Create submission summary
    summary_file = sweep_dir / "submission_summary.txt"
    with open(summary_file, "w") as f:
        f.write("Sweep Submission Summary\n")
        f.write("========================\n")
        f.write(f"Sweep ID: {sweep_id}\n")
        f.write(f"Submission Time: {datetime.now()}\n")
        f.write(f"Mode: {mode}\n")
        f.write(f"Total Combinations: {len(combinations)}\n")
        f.write(f"Job IDs: {', '.join(job_ids)}\n")
        f.write(f"Walltime: {walltime}\n")
        f.write(f"Resources: {resources}\n")
        if group:
            f.write(f"W&B Group: {group}\n")

    console.print(f"\nSummary saved to: {summary_file}")

    # Show source mapping information for all modes
    source_mapping_file = sweep_dir / "source_mapping.yaml"
    if source_mapping_file.exists():
        console.print(f"\n[cyan]📊 Sweep tracking initialized: {source_mapping_file}[/cyan]")
        console.print(
            "• Task progress and source assignments are tracked for reliable completion analysis"
        )
        console.print(
            "• Use 'hsm sweep status' to monitor progress and identify missing combinations"
        )

    logger.info(f"Sweep {sweep_id} submitted successfully with {len(combinations)} combinations")

    return job_ids


def _handle_results_collection(
    mode: str, job_manager: object, sweep_dir: Path, console: Console, logger: logging.Logger
):
    """Handle results collection and reporting after jobs are submitted."""
    # For remote mode, collect results after job completion
    if mode == "remote" and hasattr(job_manager, "collect_results"):
        console.print("\n[cyan]Collecting results from remote machine...[/cyan]")
        try:
            success = job_manager.collect_results()
            if success:
                console.print(
                    f"[green]✓ Results collected successfully to {sweep_dir}/tasks/[/green]"
                )

                # Check for and report on error information
                error_dir = sweep_dir / "errors"
                failed_tasks_dir = sweep_dir / "failed_tasks"

                if error_dir.exists():
                    error_files = list(error_dir.glob("*_error.txt"))
                    if error_files:
                        console.print(
                            f"[yellow]📄 {len(error_files)} error summary(ies) found in: {error_dir}/[/yellow]"
                        )
                        console.print(
                            "[yellow]• Use 'hsm sweep errors <sweep_id>' to view error details[/yellow]"
                        )

                if failed_tasks_dir.exists():
                    failed_tasks = [d for d in failed_tasks_dir.iterdir() if d.is_dir()]
                    if failed_tasks:
                        console.print(
                            f"[yellow]📁 {len(failed_tasks)} failed task(s) immediately collected to: {failed_tasks_dir}/[/yellow]"
                        )
                        console.print(
                            "[yellow]• Failed task directories contain full logs and error details[/yellow]"
                        )

                # Show comprehensive result summary
                tasks_dir = sweep_dir / "tasks"
                if tasks_dir.exists():
                    remote_tasks = [d for d in tasks_dir.iterdir() if d.is_dir()]
                    if remote_tasks:
                        console.print(
                            f"[green]📁 {len(remote_tasks)} task result(s) collected to: {tasks_dir}/[/green]"
                        )

                # Provide helpful next steps
                console.print("\n[cyan]💡 Next steps:[/cyan]")
                console.print("• Use 'hsm sweep status <sweep_id>' to check completion status")
                if error_dir.exists() and list(error_dir.glob("*_error.txt")):
                    console.print("• Use 'hsm sweep errors <sweep_id>' to analyze failures")
                    console.print(
                        "• Check specific error patterns with 'hsm sweep errors <sweep_id> --pattern <keyword>'"
                    )
            else:
                console.print("[yellow]⚠ Result collection completed with warnings[/yellow]")
                console.print(
                    "[yellow]Some results may still be available on the remote machine[/yellow]"
                )
                console.print("[yellow]Check the remote directory manually if needed[/yellow]")

        except Exception as e:
            console.print(f"[red]Error collecting results: {e}[/red]")
            logger.warning(f"Result collection failed: {e}")

            # Even if collection failed, check for locally collected error info
            error_dir = sweep_dir / "errors"
            failed_tasks_dir = sweep_dir / "failed_tasks"

            console.print(
                "\n[yellow]📋 Checking for locally collected error information...[/yellow]"
            )

            local_errors_found = False
            if error_dir.exists():
                error_files = list(error_dir.glob("*_error.txt"))
                if error_files:
                    console.print(
                        f"[green]✓ {len(error_files)} error summary(ies) available in: {error_dir}/[/green]"
                    )
                    local_errors_found = True

            if failed_tasks_dir.exists():
                failed_tasks = [d for d in failed_tasks_dir.iterdir() if d.is_dir()]
                if failed_tasks:
                    console.print(
                        f"[green]✓ {len(failed_tasks)} failed task(s) with full logs in: {failed_tasks_dir}/[/green]"
                    )
                    local_errors_found = True

            if local_errors_found:
                console.print(
                    "[cyan]💡 Error information was collected during job execution[/cyan]"
                )
                console.print("• Use 'hsm sweep errors <sweep_id>' to view details")
            else:
                console.print("[red]❌ No local error information found[/red]")
                console.print(
                    "[yellow]💡 You may need to check the remote machine manually[/yellow]"
                )

            # For distributed mode, results are normalized automatically during execution
    elif mode == "distributed":
        tasks_dir = sweep_dir / "tasks"
        scripts_dir = sweep_dir / "distributed_scripts"
        logs_dir = sweep_dir / "logs"
        source_mapping_file = sweep_dir / "source_mapping.yaml"

        if tasks_dir.exists():
            task_count = len(
                [d for d in tasks_dir.iterdir() if d.is_dir() and d.name.startswith("task_")]
            )
            console.print(f"[green]✓ {task_count} task results available in: {tasks_dir}/[/green]")

            # Show scripts and logs collection status
            scripts_count = len(list(scripts_dir.glob("*.sh"))) if scripts_dir.exists() else 0
            logs_count = (
                len([f for f in logs_dir.glob("*") if f.is_file()]) if logs_dir.exists() else 0
            )

            if scripts_count > 0:
                console.print(
                    f"[green]📜 {scripts_count} job scripts collected in: {scripts_dir}/[/green]"
                )
            if logs_count > 0:
                console.print(f"[green]📋 {logs_count} log files collected in: {logs_dir}/[/green]")

            if source_mapping_file.exists():
                console.print(f"[cyan]📊 Source assignment details: {source_mapping_file}[/cyan]")
        else:
            console.print(f"[yellow]Results will be available in: {tasks_dir}/[/yellow]")


def run_sweep(
    config_path: Path,
    mode: str,
    dry_run: bool,
    count_only: bool,
    max_runs: Optional[int],
    walltime: str,
    resources: str,
    group: Optional[str],
    priority: Optional[int],
    parallel_jobs: Optional[int],
    show_output: bool,
    no_progress: bool,
    remote: Optional[str],
    no_verify_sync: bool,
    auto_sync: bool,
    console: Console,
    logger: logging.Logger,
    hsm_config: Optional["HSMConfig"] = None,
):
    """Run parameter sweep."""

    console.print("[bold blue]HPC Sweep Manager[/bold blue]")
    console.print(f"Config: {config_path}")
    console.print(f"Mode: {mode}")

    # Show if HSM config is being used
    if hsm_config:
        hsm_config_path = None
        search_paths = [
            Path.cwd() / "sweeps" / "hsm_config.yaml",
            Path.cwd() / "hsm_config.yaml",
            Path("sweeps") / "hsm_config.yaml",
            Path("hsm_config.yaml"),
        ]
        for path in search_paths:
            if path.exists():
                hsm_config_path = path
                break

        if hsm_config_path:
            console.print(f"[green]Using HSM config: {hsm_config_path}[/green]")
    else:
        console.print("[yellow]No hsm_config.yaml found - using default values[/yellow]")

    try:
        # Load and validate sweep configuration
        config = _load_and_validate_config(config_path, console, logger)
        if config is None:
            return

        # Generate parameter combinations and display info
        combinations = _generate_parameter_combinations(config, max_runs, count_only, console)
        if combinations is None:  # count_only was True
            return

        # Detect project paths for execution (with script from sweep config if specified)
        python_path, script_path, project_dir = _detect_project_paths(hsm_config, config)

        # Create appropriate job manager based on mode
        job_manager, mode = _create_job_manager(
            mode=mode,
            walltime=walltime,
            resources=resources,
            python_path=python_path,
            script_path=script_path,
            project_dir=project_dir,
            parallel_jobs=parallel_jobs,
            no_progress=no_progress,
            show_output=show_output,
            hsm_config=hsm_config,
            remote=remote,
            console=console,
        )
        if job_manager is None and mode not in ["remote", "distributed"]:
            return

        if job_manager:
            console.print(f"Detected/Selected execution system: {job_manager.system_type}")

        if dry_run:
            _handle_dry_run(
                mode=mode,
                remote=remote,
                job_manager=job_manager,
                hsm_config=hsm_config,
                walltime=walltime,
                resources=resources,
                python_path=python_path,
                script_path=script_path,
                project_dir=project_dir,
                group=group,
                combinations=combinations,
                console=console,
            )
            return

        # Set up sweep directories and update job managers for remote/distributed modes
        setup_result = _setup_sweep_directories_and_managers(
            mode=mode,
            remote=remote,
            job_manager=job_manager,
            hsm_config=hsm_config,
            no_verify_sync=no_verify_sync,
            auto_sync=auto_sync,
            no_progress=no_progress,
            parallel_jobs=parallel_jobs,
            console=console,
            logger=logger,
        )
        if setup_result[0] is None:  # Error occurred
            return

        sweep_id, sweep_dir, scripts_dir, logs_dir, job_manager = setup_result

        # Submit jobs and create tracking files
        try:
            job_ids = _submit_and_track_jobs(
                job_manager=job_manager,
                combinations=combinations,
                mode=mode,
                sweep_dir=sweep_dir,
                sweep_id=sweep_id,
                group=group,
                scripts_dir=scripts_dir,
                logs_dir=logs_dir,
                config_path=config_path,
                walltime=walltime,
                resources=resources,
                console=console,
                logger=logger,
            )

            # Handle results collection and reporting
            _handle_results_collection(
                mode=mode,
                job_manager=job_manager,
                sweep_dir=sweep_dir,
                console=console,
                logger=logger,
            )

        except Exception as e:
            console.print(f"[red]Error submitting jobs: {e}[/red]")
            logger.error(f"Job submission failed: {e}")
            raise

    except FileNotFoundError:
        console.print(f"[red]Error: Sweep config file not found: {config_path}[/red]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.error(f"Sweep execution failed: {e}")
        raise


# Make this a group to support subcommands
@click.group("sweep")
@click.pass_context
def sweep_cmd(ctx):
    """Run and manage parameter sweeps."""
    pass


@sweep_cmd.command("run")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default=Path("sweeps/sweep.yaml"),
    help="Path to sweep configuration file",
)
@click.option(
    "--mode",
    type=click.Choice(["auto", "individual", "array", "local", "remote", "distributed"]),
    default="auto",
    help="Job submission mode",
)
@click.option("--dry-run", "-d", is_flag=True, help="Show what would be executed without running")
@click.option("--count-only", is_flag=True, help="Count combinations and exit")
@click.option("--max-runs", type=int, help="Maximum number of runs to execute")
@click.option("--walltime", help="Job walltime (overrides config default)")
@click.option("--resources", help="Job resources (overrides config default)")
@click.option("--group", help="W&B group name for this sweep")
@click.option("--priority", type=int, help="Job priority")
@click.option("--parallel-jobs", "-p", type=int, help="Maximum parallel jobs")
@click.option("--show-output", is_flag=True, help="Show job output in real-time (local mode only)")
@click.option("--no-progress", is_flag=True, help="Disable progress tracking")
@click.option("--remote", help="Remote machine name for remote mode")
@click.option("--no-verify-sync", is_flag=True, help="Skip project synchronization verification")
@click.option(
    "--auto-sync",
    is_flag=True,
    help="Automatically sync mismatched files to remote without prompting",
)
@common_options
@click.pass_context
def run_cmd(
    ctx,
    config,
    mode,
    dry_run,
    count_only,
    max_runs,
    walltime,
    resources,
    group,
    priority,
    parallel_jobs,
    show_output,
    no_progress,
    remote,
    no_verify_sync,
    auto_sync,
    verbose,
    quiet,
):
    """Run parameter sweep."""
    # Load HSM config for defaults
    hsm_config = HSMConfig.load()

    # Use HSM config defaults if not provided via CLI
    if walltime is None:
        walltime = hsm_config.get_default_walltime() if hsm_config else "23:59:59"

    if resources is None:
        resources = (
            hsm_config.get_default_resources() if hsm_config else "select=1:ncpus=4:mem=64gb"
        )

    config_path = Path(config)

    run_sweep(
        config_path=config_path,
        mode=mode,
        dry_run=dry_run,
        count_only=count_only,
        max_runs=max_runs,
        walltime=walltime,
        resources=resources,
        group=group,
        priority=priority,
        parallel_jobs=parallel_jobs,
        show_output=show_output,
        no_progress=no_progress,
        remote=remote,
        no_verify_sync=no_verify_sync,
        auto_sync=auto_sync,
        console=ctx.obj["console"],
        logger=ctx.obj["logger"],
        hsm_config=hsm_config,
    )


@sweep_cmd.command("complete")
@click.argument("sweep_id")
@click.option(
    "--mode",
    type=click.Choice(["auto", "local", "remote", "distributed", "array"]),
    default="auto",
    help="Job submission mode for completion (auto=detect from original sweep)",
)
@click.option("--dry-run", "-d", is_flag=True, help="Show what would be completed without running")
@click.option(
    "--no-retry-failed", is_flag=True, help="Don't retry failed combinations, only missing ones"
)
@click.option("--max-runs", type=int, help="Maximum number of runs to execute for completion")
@click.option("--walltime", help="Job walltime for completion jobs")
@click.option("--resources", help="Job resources for completion jobs")
@click.option("--group", help="W&B group name for completion jobs")
@click.option("--parallel-jobs", "-p", type=int, help="Maximum parallel jobs for completion")
@click.option("--show-output", is_flag=True, help="Show job output in real-time (local mode only)")
@click.option("--no-progress", is_flag=True, help="Disable progress tracking")
@click.option("--remote", help="Remote machine name for remote completion")
@click.option("--no-verify-sync", is_flag=True, help="Skip project synchronization verification")
@click.option(
    "--auto-sync",
    is_flag=True,
    help="Automatically sync mismatched files to remote without prompting",
)
@click.option("--complete-baselines", is_flag=True, help="Complete all baseline combinations")
@click.option("--complete-main-training", is_flag=True, help="Complete main training combinations")
@click.option(
    "--baselines-only",
    is_flag=True,
    help="Run ONLY baseline completion (skip regular missing/failed runs)",
)
@click.option(
    "--overwrite-source-mapping", is_flag=True, help="Overwrite source mapping with new analysis"
)
@common_options
@click.pass_context
def complete_cmd(
    ctx,
    sweep_id,
    mode,
    dry_run,
    no_retry_failed,
    max_runs,
    walltime,
    resources,
    group,
    parallel_jobs,
    show_output,
    no_progress,
    remote,
    no_verify_sync,
    auto_sync,
    verbose,
    quiet,
    complete_baselines,
    complete_main_training,
    baselines_only,
    overwrite_source_mapping,
):
    """Complete a partially finished sweep by running missing/failed combinations."""
    from rich.table import Table

    from ..core.common.completion import SweepCompletor

    console = ctx.obj["console"]
    logger = ctx.obj["logger"]

    # Find sweep directory
    sweep_dir = Path("sweeps/outputs") / sweep_id
    if not sweep_dir.exists():
        console.print(f"[red]Error: Sweep directory not found: {sweep_dir}[/red]")
        return

    console.print("[bold blue]HPC Sweep Manager - Completion[/bold blue]")
    console.print(f"Sweep: {sweep_id}")
    console.print(f"Directory: {sweep_dir}")
    console.print(f"Baselines only: {baselines_only}")
    console.print(f"Overwrite source mapping: {overwrite_source_mapping}")

    # Create completor and analyze
    completor = SweepCompletor(sweep_dir)
    analysis = completor.analyzer.analyze_completion_status(
        overwrite_source_mapping=overwrite_source_mapping
    )

    if "error" in analysis:
        console.print(f"[red]Error analyzing sweep: {analysis['error']}[/red]")
        return

    # Show current status
    console.print("\n[bold]Current Sweep Status:[/bold]")
    table = Table()
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="green")
    table.add_column("Percentage", style="yellow")

    total_expected = analysis["total_expected"]
    total_completed = analysis["total_completed"]
    total_failed = analysis["total_failed"]
    total_cancelled = analysis.get("total_cancelled", 0)  # Handle older mappings without cancelled
    total_missing = analysis["total_missing"]
    total_running = analysis["total_running"]

    table.add_row("Expected Combinations", str(total_expected), "100.0%")
    table.add_row("Completed", str(total_completed), f"{analysis['completion_rate']:.1f}%")
    table.add_row(
        "Failed",
        str(total_failed),
        f"{total_failed / total_expected * 100:.1f}%" if total_expected > 0 else "0%",
    )
    if total_cancelled > 0:
        table.add_row(
            "Cancelled",
            str(total_cancelled),
            f"{total_cancelled / total_expected * 100:.1f}%" if total_expected > 0 else "0%",
        )
    table.add_row(
        "Missing",
        str(total_missing),
        f"{total_missing / total_expected * 100:.1f}%" if total_expected > 0 else "0%",
    )
    if total_running > 0:
        table.add_row("Running", str(total_running), f"{total_running / total_expected * 100:.1f}%")

    console.print(table)

    # Show status fixes if any were made
    if analysis.get("status_fixes_made", False):
        console.print("\n[yellow]🔧 Status corrections were made:[/yellow]")
        console.print("• Some tasks had incorrect status in the tracking file")
        console.print("• The status was corrected by checking actual task directories")
        console.print("• This ensures accurate completion analysis")
        console.print("• The corrected status is now saved to source_mapping.yaml")

    if not analysis["needs_completion"]:
        console.print("\n[green]✓ Sweep is already complete![/green]")
        return

    # Load HSM config for defaults
    hsm_config = HSMConfig.load()

    # Use HSM config defaults if not provided via CLI
    if walltime is None:
        walltime = hsm_config.get_default_walltime() if hsm_config else "23:59:59"

    if resources is None:
        resources = (
            hsm_config.get_default_resources() if hsm_config else "select=1:ncpus=4:mem=64gb"
        )

    # If baselines_only is set, ensure complete_baselines is also set
    if baselines_only and not complete_baselines:
        complete_baselines = True
        console.print("[yellow]--baselines-only implies --complete-baselines[/yellow]")

    # Execute completion
    result = completor.execute_completion(
        mode=mode,
        dry_run=dry_run,
        retry_failed=not no_retry_failed,
        max_runs=max_runs,
        walltime=walltime,
        resources=resources,
        group=group,
        parallel_jobs=parallel_jobs,
        show_output=show_output,
        no_progress=no_progress,
        remote=remote,
        no_verify_sync=no_verify_sync,
        auto_sync=auto_sync,
        complete_baselines=complete_baselines,
        complete_main_training=complete_main_training,
        baselines_only=baselines_only,
        console=console,
        logger=logger,
    )

    if "error" in result:
        console.print(f"[red]Error: {result['error']}[/red]")
        return

    if result["status"] == "dry_run":
        console.print("\n[yellow]DRY RUN - Completion Plan:[/yellow]")

        # Show execution mode prominently
        exec_mode = result.get("execution_mode", mode)
        console.print(f"\n[bold cyan]Execution Mode: {exec_mode.upper()}[/bold cyan]")
        if exec_mode == "array":
            console.print("[green]✓ Will submit as PBS/Slurm array job (HPC cluster)[/green]")
        elif exec_mode == "distributed":
            console.print("[green]✓ Will distribute across multiple compute sources[/green]")
        elif exec_mode == "remote":
            console.print("[green]✓ Will execute on remote machine[/green]")
        elif exec_mode == "local":
            console.print("[yellow]⚠ Will execute locally (not recommended for HPC)[/yellow]")

        # Show script path from sweep_config
        sweep_config_path = sweep_dir / "sweep_config.yaml"
        if sweep_config_path.exists():
            try:
                import yaml

                with open(sweep_config_path) as f:
                    sweep_cfg = yaml.safe_load(f)
                    if sweep_cfg and "script" in sweep_cfg:
                        console.print(f"[green]✓ Training script: {sweep_cfg['script']}[/green]")
            except Exception:
                pass

        console.print(f"\nMissing combinations to run: {result['missing_count']}")
        if not no_retry_failed:
            console.print(f"Failed combinations to retry: {result['failed_count']}")
            cancelled_count = result.get("cancelled_count", 0)
            if cancelled_count > 0:
                console.print(f"Cancelled combinations to retry: {cancelled_count}")
        if complete_baselines:
            baseline_count = result.get("baseline_count", 0)
            if baseline_count > 0:
                console.print(
                    f"[cyan]Baseline completion runs: {baseline_count} "
                    f"(completed tasks missing baseline results)[/cyan]"
                )
        console.print(f"Total combinations to run: {result['total_to_run']}")

        # Detailed combinations and commands are already shown in completion.py dry_run output
        # Only show summary here if there are more than 3 of each type
        regular_runs = result.get("regular_runs", [])
        baseline_runs = result.get("baseline_runs", [])
        if regular_runs and len(regular_runs) > 3:
            console.print(
                f"\n[dim]Showing first 3 of {len(regular_runs)} regular completion runs "
                f"(see above for details)[/dim]"
            )
        if baseline_runs and len(baseline_runs) > 3:
            console.print(
                f"[dim]Showing first 3 of {len(baseline_runs)} baseline completion runs "
                f"(see above for details)[/dim]"
            )

        console.print(
            "\n[dim]To execute the completion, run the same command without --dry-run[/dim]"
        )

    elif result["status"] == "complete":
        console.print(f"\n[green]{result['message']}[/green]")

    elif result["status"] == "submitted":
        console.print(f"\n[green]{result['message']}[/green]")
        console.print(f"Job IDs: {', '.join(result['job_ids'])}")
        console.print(f"Combinations submitted: {result['combinations_count']}")

        # Create completion summary
        summary_file = sweep_dir / "completion_summary.txt"
        with open(summary_file, "w") as f:
            f.write("Sweep Completion Summary\n")
            f.write("========================\n")
            f.write(f"Completion Time: {datetime.now()}\n")
            f.write(f"Mode: {mode}\n")
            f.write(f"Combinations Submitted: {result['combinations_count']}\n")
            f.write(f"Job IDs: {', '.join(result['job_ids'])}\n")
            f.write(f"Retry Failed: {not no_retry_failed}\n")

        console.print(f"\nCompletion summary saved to: {summary_file}")

        # For remote mode, provide additional guidance about error monitoring
        if mode == "remote":
            console.print("\n[cyan]💡 Remote Mode Tips:[/cyan]")
            console.print("• Error details are automatically collected when jobs fail")
            console.print(
                "• Check the errors/ directory in your sweep folder for detailed error logs"
            )
            console.print("• Use 'hsm monitor' to track job progress in real-time")
            console.print("• Error summaries will be available after job completion")

            error_dir = sweep_dir / "errors"
            if error_dir.exists() and list(error_dir.glob("*_error.txt")):
                console.print(
                    f"\n[yellow]📁 Error summaries already available: {error_dir}/[/yellow]"
                )

        # Show source mapping information for all modes
        source_mapping_file = sweep_dir / "source_mapping.yaml"
        if source_mapping_file.exists():
            console.print(f"\n[cyan]📊 Sweep tracking updated: {source_mapping_file}[/cyan]")
            console.print(
                "• Task progress and source assignments are maintained across all execution modes"
            )
            console.print("• Use 'hsm sweep status' to see detailed completion status")
        else:
            console.print(
                f"\n[yellow]⚠ Creating sweep tracking file: {source_mapping_file}[/yellow]"
            )


@sweep_cmd.command("status")
@click.argument("sweep_id", required=False)
@click.option("--all", "-a", is_flag=True, help="Show status of all sweeps")
@click.option("--incomplete-only", is_flag=True, help="Show only incomplete sweeps")
@common_options
@click.pass_context
def status_cmd(ctx, sweep_id, all, incomplete_only, verbose, quiet):
    """Show completion status of sweep(s)."""
    from rich.table import Table

    from ..core.common.completion import SweepCompletionAnalyzer, find_incomplete_sweeps

    console = ctx.obj["console"]

    if all or incomplete_only:
        # Show status of multiple sweeps
        sweeps_dir = Path("sweeps/outputs")
        if not sweeps_dir.exists():
            console.print("[yellow]No sweeps directory found.[/yellow]")
            return

        sweeps_to_show = []

        if incomplete_only:
            incomplete_sweeps = find_incomplete_sweeps(sweeps_dir)
            sweeps_to_show = incomplete_sweeps
        else:
            # Show all sweeps
            for sweep_dir in sweeps_dir.iterdir():
                if sweep_dir.is_dir() and sweep_dir.name.startswith("sweep_"):
                    analyzer = SweepCompletionAnalyzer(sweep_dir)
                    analysis = analyzer.analyze_completion_status()
                    if "error" not in analysis:
                        sweeps_to_show.append(
                            {
                                "sweep_id": sweep_dir.name,
                                "completion_rate": analysis["completion_rate"],
                                "total_expected": analysis["total_expected"],
                                "total_completed": analysis["total_completed"],
                                "total_missing": analysis["total_missing"],
                                "total_failed": analysis["total_failed"],
                                "total_cancelled": analysis.get("total_cancelled", 0),
                            }
                        )

        if not sweeps_to_show:
            if incomplete_only:
                console.print("[green]No incomplete sweeps found![/green]")
            else:
                console.print("[yellow]No sweeps found.[/yellow]")
            return

        # Create table
        table = Table(title="Sweep Status Summary")
        table.add_column("Sweep ID", style="cyan")
        table.add_column("Progress", style="green")
        table.add_column("Completion %", style="yellow")
        table.add_column("Missing", style="red")
        table.add_column("Failed", style="magenta")
        table.add_column("Cancelled", style="orange")
        table.add_column("Status", style="blue")

        for sweep_info in sorted(sweeps_to_show, key=lambda x: x["sweep_id"]):
            completion_rate = sweep_info["completion_rate"]
            status = "COMPLETE" if completion_rate >= 100.0 else "INCOMPLETE"
            status_style = "green" if status == "COMPLETE" else "red"

            table.add_row(
                sweep_info["sweep_id"],
                f"{sweep_info['total_completed']}/{sweep_info['total_expected']}",
                f"{completion_rate:.1f}%",
                str(sweep_info["total_missing"]),
                str(sweep_info["total_failed"]),
                str(sweep_info["total_cancelled"]),
                f"[{status_style}]{status}[/{status_style}]",
            )

        console.print(table)

    elif sweep_id:
        # Show detailed status of specific sweep
        sweep_dir = Path("sweeps/outputs") / sweep_id
        if not sweep_dir.exists():
            console.print(f"[red]Error: Sweep directory not found: {sweep_dir}[/red]")
            return

        analyzer = SweepCompletionAnalyzer(sweep_dir)
        analysis = analyzer.analyze_completion_status()

        if "error" in analysis:
            console.print(f"[red]Error analyzing sweep: {analysis['error']}[/red]")
            return

        console.print(f"[bold blue]Sweep Status: {sweep_id}[/bold blue]")
        console.print(f"Directory: {sweep_dir}")
        console.print()

        # Detailed table
        table = Table()
        table.add_column("Metric", style="cyan")
        table.add_column("Count", style="green")
        table.add_column("Percentage", style="yellow")

        total_expected = analysis["total_expected"]

        table.add_row("Expected Combinations", str(analysis["total_expected"]), "100.0%")
        table.add_row(
            "Completed", str(analysis["total_completed"]), f"{analysis['completion_rate']:.1f}%"
        )
        table.add_row(
            "Failed",
            str(analysis["total_failed"]),
            f"{analysis['total_failed'] / total_expected * 100:.1f}%"
            if total_expected > 0
            else "0%",
        )
        total_cancelled = analysis.get("total_cancelled", 0)
        if total_cancelled > 0:
            table.add_row(
                "Cancelled",
                str(total_cancelled),
                f"{total_cancelled / total_expected * 100:.1f}%" if total_expected > 0 else "0%",
            )
        table.add_row(
            "Missing",
            str(analysis["total_missing"]),
            f"{analysis['total_missing'] / total_expected * 100:.1f}%"
            if total_expected > 0
            else "0%",
        )

        if analysis["total_running"] > 0:
            table.add_row(
                "Running",
                str(analysis["total_running"]),
                f"{analysis['total_running'] / total_expected * 100:.1f}%",
            )

        console.print(table)

        # Show status fixes if any were made
        if analysis.get("status_fixes_made", False):
            console.print("\n[yellow]🔧 Status corrections were made:[/yellow]")
            console.print("• Some tasks had incorrect status in the tracking file")
            console.print("• The status was corrected by checking actual task directories")
            console.print("• This ensures accurate completion analysis")
            console.print("• The corrected status is now saved to source_mapping.yaml")

        if not analysis["needs_completion"]:
            console.print("\n[green]✓ Sweep is already complete![/green]")
            return

        console.print(
            f"\n[yellow]⚠ Sweep needs completion. Run 'hsm sweep complete {sweep_id}' to finish it.[/yellow]"
        )

    else:
        console.print("[red]Error: Please specify a sweep ID or use --all/--incomplete-only[/red]")


@sweep_cmd.command("report")
@click.argument("sweep_id")
@click.option(
    "--scan-tasks", is_flag=True, help="Force scanning task directories (useful for PBS array jobs)"
)
@click.option("--save-json", is_flag=True, help="Save detailed report to JSON file")
@common_options
@click.pass_context
def report_cmd(ctx, sweep_id, scan_tasks, save_json, verbose, quiet):
    """Generate detailed completion report for a sweep."""
    import json

    from rich.table import Table

    from ..core.common.completion import SweepCompletionAnalyzer

    console = ctx.obj["console"]

    # Find sweep directory
    sweep_dir = Path("sweeps/outputs") / sweep_id
    if not sweep_dir.exists():
        console.print(f"[red]Error: Sweep directory not found: {sweep_dir}[/red]")
        return

    console.print(f"[bold blue]Sweep Completion Report: {sweep_id}[/bold blue]")
    console.print(f"Directory: {sweep_dir}")

    # Create analyzer and run analysis
    analyzer = SweepCompletionAnalyzer(sweep_dir)

    if scan_tasks:
        console.print("\n[cyan]Scanning task directories...[/cyan]")
        analysis = analyzer.analyze_from_task_directories()
    else:
        console.print("\n[cyan]Analyzing completion status...[/cyan]")
        analysis = analyzer.analyze_completion_status()

    if "error" in analysis:
        console.print(f"[red]Error analyzing sweep: {analysis['error']}[/red]")
        return

    # Display summary table
    console.print("\n[bold]Summary:[/bold]")
    table = Table()
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="green")
    table.add_column("Percentage", style="yellow")

    total_expected = analysis["total_expected"]
    total_completed = analysis["total_completed"]
    total_failed = analysis["total_failed"]
    total_missing = analysis["total_missing"]
    total_running = analysis.get("total_running", 0)

    table.add_row("Expected Combinations", str(total_expected), "100.0%")
    table.add_row("Completed", str(total_completed), f"{analysis['completion_rate']:.1f}%")
    table.add_row(
        "Failed",
        str(total_failed),
        f"{total_failed / total_expected * 100:.1f}%" if total_expected > 0 else "0%",
    )
    table.add_row(
        "Missing",
        str(total_missing),
        f"{total_missing / total_expected * 100:.1f}%" if total_expected > 0 else "0%",
    )
    if total_running > 0:
        table.add_row(
            "Running",
            str(total_running),
            f"{total_running / total_expected * 100:.1f}%" if total_expected > 0 else "0%",
        )

    console.print(table)

    # Show detailed task status if available
    if "task_statuses" in analysis and analysis["task_statuses"]:
        console.print(f"\n[bold]Task Status Details:[/bold]")
        console.print(f"Total tasks found: {len(analysis['task_statuses'])}")

        # Group by status
        status_groups = {}
        for task_id, status in analysis["task_statuses"].items():
            if status not in status_groups:
                status_groups[status] = []
            status_groups[status].append(task_id)

        for status, tasks in sorted(status_groups.items()):
            console.print(f"  {status}: {len(tasks)} tasks")

    # Show missing task numbers
    if "missing_task_numbers" in analysis and analysis["missing_task_numbers"]:
        missing_numbers = analysis["missing_task_numbers"]
        if len(missing_numbers) <= 20:
            console.print(f"\n[yellow]Missing task numbers: {missing_numbers}[/yellow]")
        else:
            console.print(
                f"\n[yellow]Missing task numbers (first 20): {missing_numbers[:20]}...[/yellow]"
            )
            console.print(f"[yellow]Total missing: {len(missing_numbers)}[/yellow]")

    # Show failed task numbers
    if analysis.get("failed_tasks"):
        failed_tasks = analysis["failed_tasks"]
        if len(failed_tasks) <= 20:
            console.print(f"\n[red]Failed tasks: {failed_tasks}[/red]")
        else:
            console.print(f"\n[red]Failed tasks (first 20): {failed_tasks[:20]}...[/red]")
            console.print(f"[red]Total failed: {len(failed_tasks)}[/red]")

    # Save detailed report if requested
    if save_json:
        report_file = sweep_dir / "detailed_completion_report.json"
        try:
            # Create a JSON-serializable version
            json_report = {
                "sweep_id": sweep_id,
                "sweep_dir": str(sweep_dir),
                "timestamp": analysis.get("timestamp", datetime.now().isoformat()),
                "scan_method": analysis.get("scan_method", "source_mapping"),
                "summary": {
                    "total_expected": total_expected,
                    "total_completed": total_completed,
                    "total_failed": total_failed,
                    "total_missing": total_missing,
                    "total_running": total_running,
                    "completion_rate": analysis["completion_rate"],
                },
                "completed_tasks": analysis.get("completed_tasks", []),
                "failed_tasks": analysis.get("failed_tasks", []),
                "running_tasks": analysis.get("running_tasks", []),
                "missing_task_numbers": analysis.get("missing_task_numbers", []),
                "task_statuses": analysis.get("task_statuses", {}),
            }

            with open(report_file, "w") as f:
                json.dump(json_report, f, indent=2)

            console.print(f"\n[green]✓ Detailed report saved to: {report_file}[/green]")
        except Exception as e:
            console.print(f"[red]Error saving report: {e}[/red]")

    # Show next steps
    if analysis.get("needs_completion"):
        console.print(
            f"\n[yellow]⚠ Sweep needs completion. Run 'hsm sweep complete {sweep_id}' to finish it.[/yellow]"
        )
    else:
        console.print("\n[green]✓ Sweep is complete![/green]")

    # Show where source mapping is stored (single source of truth)
    source_mapping = sweep_dir / "source_mapping.yaml"
    if source_mapping.exists():
        console.print(f"\n[dim]Task tracking: {source_mapping}[/dim]")


@sweep_cmd.command("errors")
@click.argument("sweep_id")
@click.option("--all", "-a", is_flag=True, help="Show all error details")
@click.option("--pattern", help="Filter errors by pattern (e.g., 'ImportError')")
@common_options
@click.pass_context
def errors_cmd(ctx, sweep_id, all, pattern, verbose, quiet):
    """Show error summaries for a specific sweep."""
    console = ctx.obj["console"]

    # Find sweep directory
    sweep_dir = Path("sweeps/outputs") / sweep_id
    if not sweep_dir.exists():
        console.print(f"[red]Error: Sweep directory not found: {sweep_dir}[/red]")
        return

    error_dir = sweep_dir / "errors"
    if not error_dir.exists():
        console.print(f"[yellow]No error directory found for sweep {sweep_id}[/yellow]")
        console.print("This means either:")
        console.print("• No jobs have failed yet")
        console.print("• The sweep was not run in remote mode")
        console.print("• Error collection is not yet implemented for this execution mode")
        return

    error_files = list(error_dir.glob("*_error.txt"))
    if not error_files:
        console.print("[green]No error files found - all jobs may have succeeded![/green]")
        return

    console.print(f"[bold blue]Error Summary for Sweep: {sweep_id}[/bold blue]")
    console.print(f"Found {len(error_files)} error files in: {error_dir}")

    # Filter by pattern if provided
    if pattern:
        filtered_files = []
        for error_file in error_files:
            try:
                with open(error_file) as f:
                    content = f.read()
                    if pattern.lower() in content.lower():
                        filtered_files.append(error_file)
            except Exception:
                pass
        error_files = filtered_files
        console.print(f"Filtered to {len(error_files)} files containing '{pattern}'")

    if not error_files:
        console.print(f"[yellow]No error files match the pattern '{pattern}'[/yellow]")
        return

    # Show errors
    for i, error_file in enumerate(error_files):
        console.print(f"\n[bold red]Error {i + 1}: {error_file.stem}[/bold red]")
        try:
            with open(error_file) as f:
                content = f.read()
                if all:
                    # Show full content
                    console.print(content)
                else:
                    # Show preview (first 400 chars)
                    preview = content[:400]
                    if len(content) > 400:
                        preview += "\n... (use --all to see full content)"
                    console.print(preview)

        except Exception as e:
            console.print(f"[red]Could not read error file: {e}[/red]")

        if not all and i >= 4:  # Limit to first 5 errors unless --all is used
            remaining = len(error_files) - i - 1
            if remaining > 0:
                console.print(
                    f"\n[yellow]... and {remaining} more errors (use --all to see all)[/yellow]"
                )
            break

    console.print("\n[cyan]💡 Use --all to see full error details[/cyan]")
    console.print("[cyan]💡 Use --pattern to filter by specific error types[/cyan]")


# Add legacy alias for backwards compatibility
@click.command("sweep-legacy", hidden=True)
@click.pass_context
def sweep_legacy_cmd(ctx):
    """Legacy sweep command (use 'hsm sweep run' instead)."""
    ctx.invoke(run_cmd)


# Keep the old group-based interface for backwards compatibility, but hidden
@click.group(hidden=True)
def sweep():
    """Run and manage parameter sweeps."""
    pass


@sweep.command("run", hidden=True)
@click.pass_context
def run_cmd(ctx):
    """Run parameter sweep (legacy interface)."""
    # This could redirect to the main sweep command if needed for backwards compatibility
    ctx.invoke(sweep_cmd)


if __name__ == "__main__":
    sweep()
