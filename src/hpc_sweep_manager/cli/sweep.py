"""Sweep execution CLI commands."""

from datetime import datetime
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click
from rich.console import Console
from rich.table import Table

if TYPE_CHECKING:
    from ..core.common.config import HSMConfig

import asyncio

from ..core.common.config import HSMConfig
from ..core.common.config_parser import SweepConfig
from ..core.common.param_generator import ParameterGenerator
from ..core.common.utils import create_sweep_id
from ..core.distributed.wrapper import create_distributed_sweep_wrapper
from ..core.hpc.hpc_base import HPCJobManager
from ..core.remote.discovery import RemoteDiscovery
from ..core.remote.manager import RemoteJobManager
from .common import common_options


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
        # Setup remote environment first with sync verification
        setup_success = await self.remote_manager.setup_remote_environment(
            verify_sync=self.verify_sync, auto_sync=self.auto_sync
        )
        if not setup_success:
            raise Exception("Failed to setup remote environment")

        # Submit jobs with parallel control and wait for completion
        job_ids = await self.remote_manager.submit_sweep(param_combinations, sweep_id, wandb_group)

        # Wait for all jobs to complete before returning
        await self.remote_manager.wait_for_all_jobs()

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
        # Load sweep configuration
        config = SweepConfig.from_yaml(config_path)
        logger.info(f"Loaded sweep config from {config_path}")

        # Validate configuration
        errors = config.validate()
        if errors:
            console.print("[red]Configuration validation failed:[/red]")
            for error in errors:
                console.print(f"  - {error}")
            return

        # Generate parameter combinations
        generator = ParameterGenerator(config)

        if count_only:
            total_combinations = generator.count_combinations()
            console.print(f"[green]Total parameter combinations: {total_combinations}[/green]")
            return

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

        # Create job manager with project configuration (needed for both dry run and actual run)
        from ..core.common.path_detector import PathDetector

        detector = PathDetector()

        # Try to get paths from HSM config first, then auto-detect
        if hsm_config:
            python_path = hsm_config.get_default_python_path() or detector.detect_python_path()
            script_path = hsm_config.get_default_script_path() or detector.detect_train_script()
            project_dir = hsm_config.get_project_root() or str(Path.cwd())
        else:
            python_path = detector.detect_python_path()
            script_path = detector.detect_train_script()
            project_dir = str(Path.cwd())

        # Create appropriate job manager based on mode
        if mode == "local":
            from ..core.local.manager import LocalJobManager

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
                return

            if not remote:
                console.print("[red]Error: --remote MACHINE_NAME required for remote mode[/red]")
                console.print("Available remotes:")
                remotes = hsm_config.config_data.get("distributed", {}).get("remotes", {})
                for name in remotes.keys():
                    console.print(f"  - {name}")
                return

            # Remote job manager will be created later with proper sweep directory
            job_manager = None  # Will be created after sweep directory is set up
        elif mode == "distributed":
            # Distributed job execution across multiple sources
            if not hsm_config:
                console.print("[red]Error: hsm_config.yaml required for distributed mode[/red]")
                return

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
                    console.print(
                        "[yellow]No HPC system detected, falling back to local mode[/yellow]"
                    )
                    mode = "local"

                    from ..core.local.manager import LocalJobManager

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
                    return
        else:
            console.print(f"[red]Error: Unknown mode '{mode}'[/red]")
            return

        if job_manager:
            console.print(f"Detected/Selected execution system: {job_manager.system_type}")

        if dry_run:
            console.print("\n[yellow]DRY RUN - No jobs will be submitted[/yellow]")

            # Show job configuration
            console.print("\n[bold]Job Configuration:[/bold]")
            if mode == "remote":
                console.print("  Execution System: REMOTE")
                console.print(f"  Mode: {mode} (remote execution on {remote})")
                console.print(
                    "  Note: Remote configuration will be discovered during actual execution"
                )
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

            return

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
                return  # Error already displayed by create_remote_job_manager_wrapper
        elif mode == "distributed":
            # Create a DistributedSweepWrapper with the proper sweep directory
            try:
                job_manager = create_distributed_sweep_wrapper(
                    hsm_config=hsm_config,
                    console=console,
                    logger=logger,
                    sweep_dir=sweep_dir,
                    show_progress=not no_progress,
                )
            except Exception as e:
                console.print(f"[red]Error creating distributed job manager: {e}[/red]")
                return

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

        # Save sweep config for reference
        config_backup = sweep_dir / "sweep_config.yaml"
        import shutil

        shutil.copy2(config_path, config_backup)

        # Submit jobs
        console.print(f"\n[bold]Submitting {len(combinations)} jobs in {mode} mode...[/bold]")

        try:
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
            logger.info(
                f"Sweep {sweep_id} submitted successfully with {len(combinations)} combinations"
            )

            # For remote mode, collect results after job completion
            if mode == "remote" and hasattr(job_manager, "collect_results"):
                console.print("\n[cyan]Collecting results from remote machine...[/cyan]")
                try:
                    success = job_manager.collect_results()
                    if success:
                        console.print(
                            f"[green]✓ Results collected successfully to {sweep_dir}/tasks/[/green]"
                        )
                    else:
                        console.print(
                            "[yellow]⚠ Result collection completed with warnings[/yellow]"
                        )
                except Exception as e:
                    console.print(f"[red]Error collecting results: {e}[/red]")
                    logger.warning(f"Result collection failed: {e}")

                    # For distributed mode, results are normalized automatically during execution
            elif mode == "distributed":
                tasks_dir = sweep_dir / "tasks"
                scripts_dir = sweep_dir / "distributed_scripts"
                logs_dir = sweep_dir / "logs"
                source_mapping_file = sweep_dir / "source_mapping.yaml"

                if tasks_dir.exists():
                    task_count = len(
                        [
                            d
                            for d in tasks_dir.iterdir()
                            if d.is_dir() and d.name.startswith("task_")
                        ]
                    )
                    console.print(
                        f"[green]✓ {task_count} task results available in: {tasks_dir}/[/green]"
                    )

                    # Show scripts and logs collection status
                    scripts_count = (
                        len(list(scripts_dir.glob("*.sh"))) if scripts_dir.exists() else 0
                    )
                    logs_count = (
                        len([f for f in logs_dir.glob("*") if f.is_file()])
                        if logs_dir.exists()
                        else 0
                    )

                    if scripts_count > 0:
                        console.print(
                            f"[green]📜 {scripts_count} job scripts collected in: {scripts_dir}/[/green]"
                        )
                    if logs_count > 0:
                        console.print(
                            f"[green]📋 {logs_count} log files collected in: {logs_dir}/[/green]"
                        )

                    if source_mapping_file.exists():
                        console.print(
                            f"[cyan]📊 Source assignment details: {source_mapping_file}[/cyan]"
                        )
                else:
                    console.print(f"[yellow]Results will be available in: {tasks_dir}/[/yellow]")

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


# CLI command - make this a direct command instead of a group
@click.command("sweep")
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
def sweep_cmd(
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
