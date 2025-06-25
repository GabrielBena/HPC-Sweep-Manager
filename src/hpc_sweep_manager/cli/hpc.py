"""CLI commands for HPC cluster execution (PBS/Torque and Slurm)."""

from pathlib import Path
import shutil
import subprocess
from typing import Optional

import click
from rich.console import Console
from rich.table import Table

from ..core.common.config import SweepConfig
from ..core.common.param_generator import ParameterGenerator
from ..core.hpc.hpc_base import HPCJobManager
from .common import (
    common_options,
    create_status_panel,
    display_sweep_summary,
    handle_cli_error,
    load_hsm_config_with_validation,
    setup_cli_context,
)


@click.group()
def hpc():
    """HPC cluster execution commands."""
    pass


@hpc.command("submit")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default=Path("sweeps/sweep.yaml"),
    help="Path to sweep configuration file",
)
@click.option(
    "--mode",
    type=click.Choice(["individual", "array"]),
    default="array",
    help="Job submission mode",
)
@click.option("--max-runs", type=int, help="Maximum number of runs to execute")
@click.option("--walltime", help="Job walltime (overrides config default)")
@click.option("--resources", help="Job resources (overrides config default)")
@click.option("--group", help="W&B group name for this sweep")
@click.option("--priority", type=int, help="Job priority")
@click.option("--dry-run", is_flag=True, help="Show what would be executed without running")
@common_options
@click.pass_context
def submit_hpc(
    ctx,
    config: Path,
    mode: str,
    max_runs: Optional[int],
    walltime: Optional[str],
    resources: Optional[str],
    group: Optional[str],
    priority: Optional[int],
    dry_run: bool,
    verbose: bool,
    quiet: bool,
):
    """Submit a parameter sweep to HPC cluster."""
    logger, console = setup_cli_context(ctx, verbose, quiet)

    try:
        console.print("[bold blue]🏛️  HPC Cluster Execution[/bold blue]")

        # Load and validate configurations
        hsm_config = load_hsm_config_with_validation(console, logger)
        if not hsm_config:
            return

        sweep_config = SweepConfig.from_yaml(config)

        # Generate parameter combinations
        generator = ParameterGenerator(sweep_config)
        param_combinations = generator.generate_combinations(max_runs)

        if not param_combinations:
            console.print(
                "[red]No parameter combinations generated. Check your sweep configuration.[/red]"
            )
            return

        # Display summary
        display_sweep_summary(console, len(param_combinations), f"hpc-{mode}", config, max_runs)

        # Auto-detect HPC system and create job manager
        try:
            hpc_manager = HPCJobManager.auto_detect(
                walltime=walltime or hsm_config.get_default_walltime(),
                resources=resources or hsm_config.get_default_resources(),
                python_path=hsm_config.get_default_python_path() or "python",
                script_path=hsm_config.get_default_script_path() or "train.py",
                project_dir=hsm_config.get_project_root() or str(Path.cwd()),
            )

            console.print(
                f"[green]✓ Detected HPC system: {hpc_manager.scheduler_name.upper()}[/green]"
            )

        except RuntimeError as e:
            console.print(
                create_status_panel(
                    "HPC System Not Available",
                    str(e)
                    + "\n\nEnsure you're running on a system with PBS/Torque or Slurm installed.",
                    "error",
                )
            )
            return

        if dry_run:
            console.print("\n[bold]Dry Run - Jobs that would be submitted:[/bold]")

            # Show submission details
            console.print("\n[bold]HPC Submission Details:[/bold]")
            console.print(f"  Scheduler: [yellow]{hpc_manager.scheduler_name.upper()}[/yellow]")
            console.print(f"  Mode: [yellow]{mode}[/yellow]")
            console.print(f"  Walltime: [yellow]{hpc_manager.walltime}[/yellow]")
            console.print(f"  Resources: [yellow]{hpc_manager.resources}[/yellow]")
            console.print(f"  Total jobs: [yellow]{len(param_combinations)}[/yellow]")

            if mode == "array":
                console.print(f"  Array job with {len(param_combinations)} tasks")
            else:
                console.print(f"  {len(param_combinations)} individual jobs")

            # Show first few parameter combinations
            preview_count = min(3, len(param_combinations))
            console.print("\n[bold]Sample parameter combinations:[/bold]")
            for i, params in enumerate(param_combinations[:preview_count]):
                console.print(f"  [cyan]{i + 1:3d}.[/cyan] {params}")

            if len(param_combinations) > preview_count:
                console.print(f"  ... and {len(param_combinations) - preview_count} more")

            return

        # Create sweep directory
        from ..core.common.utils import create_sweep_id

        sweep_id = create_sweep_id(f"hpc_{mode}")
        sweep_dir = Path("sweeps") / "outputs" / sweep_id
        sweep_dir.mkdir(parents=True, exist_ok=True)

        console.print(f"\n[bold]Submitting to HPC cluster:[/bold] [cyan]{sweep_id}[/cyan]")
        console.print(f"Output directory: [dim]{sweep_dir}[/dim]")

        # Submit the sweep
        job_ids = hpc_manager.submit_sweep(
            param_combinations=param_combinations,
            mode=mode,
            sweep_dir=sweep_dir,
            sweep_id=sweep_id,
            wandb_group=group,
        )

        console.print("\n[green]✓ HPC sweep submitted successfully![/green]")
        console.print(f"Job ID(s): [cyan]{', '.join(job_ids)}[/cyan]")
        console.print(f"Monitor with: [dim]hsm monitor {sweep_id}[/dim]")

    except Exception as e:
        handle_cli_error(console, logger, e, "HPC submission")


@hpc.command("queue")
@click.option("--user", help="Show jobs for specific user (default: current user)")
@click.option("--watch", "-w", is_flag=True, help="Watch mode - continuously update")
@click.option("--refresh", "-r", type=int, default=30, help="Refresh interval for watch mode")
@common_options
@click.pass_context
def queue_status(ctx, user: Optional[str], watch: bool, refresh: int, verbose: bool, quiet: bool):
    """Show HPC queue status."""
    logger, console = setup_cli_context(ctx, verbose, quiet)

    try:
        console.print("[bold blue]🏛️  HPC Queue Status[/bold blue]")

        # Detect HPC system
        try:
            hpc_manager = HPCJobManager.auto_detect()
            scheduler = hpc_manager.scheduler_name.upper()
        except RuntimeError:
            console.print("[red]No HPC scheduler detected.[/red]")
            return

        if watch:
            console.print(f"[dim]Monitoring {scheduler} queue (Ctrl+C to stop)...[/dim]")
            import time

            try:
                while True:
                    console.clear()
                    console.print(f"[bold blue]🏛️  {scheduler} Queue Status[/bold blue]")
                    _display_queue_status(console, hpc_manager, user)
                    time.sleep(refresh)
            except KeyboardInterrupt:
                console.print("\n[yellow]Queue monitoring stopped.[/yellow]")
        else:
            _display_queue_status(console, hpc_manager, user)

    except Exception as e:
        handle_cli_error(console, logger, e, "queue status")


def _display_queue_status(console: Console, hpc_manager: HPCJobManager, user: Optional[str]):
    """Display the current queue status."""
    try:
        if hpc_manager.scheduler_name == "pbs":
            cmd = ["qstat", "-u", user] if user else ["qstat"]
        else:  # slurm
            cmd = ["squeue", "-u", user] if user else ["squeue"]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

        if result.returncode == 0:
            output = result.stdout.strip()
            if output:
                # Simple display - could be enhanced with parsing and formatting
                console.print(f"[dim]{output}[/dim]")
            else:
                console.print("[green]No jobs in queue.[/green]")
        else:
            console.print(
                f"[red]Error querying {hpc_manager.scheduler_name} queue: {result.stderr}[/red]"
            )

    except subprocess.TimeoutExpired:
        console.print("[red]Queue query timed out.[/red]")
    except Exception as e:
        console.print(f"[red]Error querying queue: {e}[/red]")


@hpc.command("status")
@common_options
@click.pass_context
def hpc_status(ctx, verbose: bool, quiet: bool):
    """Show HPC system status and configuration."""
    logger, console = setup_cli_context(ctx, verbose, quiet)

    try:
        console.print("[bold blue]🏛️  HPC System Status[/bold blue]")

        # Detect available schedulers
        has_pbs = shutil.which("qsub") and shutil.which("qstat")
        has_slurm = shutil.which("sbatch") and shutil.which("squeue")

        # Create status table
        table = Table(title="HPC Environment")
        table.add_column("Component", style="cyan", width=20)
        table.add_column("Status", style="bold", width=15)
        table.add_column("Details", style="dim", width=50)

        # PBS/Torque
        if has_pbs:
            try:
                result = subprocess.run(
                    ["qstat", "--version"], capture_output=True, text=True, timeout=5
                )
                version = result.stdout.strip() if result.returncode == 0 else "Unknown version"
                table.add_row("PBS/Torque", "[green]✓[/green]", version)
            except:
                table.add_row("PBS/Torque", "[yellow]?[/yellow]", "Available but version unknown")
        else:
            table.add_row("PBS/Torque", "[red]✗[/red]", "Not available")

        # Slurm
        if has_slurm:
            try:
                result = subprocess.run(
                    ["sinfo", "--version"], capture_output=True, text=True, timeout=5
                )
                version = result.stdout.strip() if result.returncode == 0 else "Unknown version"
                table.add_row("Slurm", "[green]✓[/green]", version)
            except:
                table.add_row("Slurm", "[yellow]?[/yellow]", "Available but version unknown")
        else:
            table.add_row("Slurm", "[red]✗[/red]", "Not available")

        # HSM Config
        hsm_config = load_hsm_config_with_validation(console, logger)
        if hsm_config:
            table.add_row("HSM Config", "[green]✓[/green]", "Configuration loaded")

            # Show HPC settings
            hpc_config = hsm_config.config_data.get("hpc", {})
            default_walltime = hpc_config.get("default_walltime", "04:00:00")
            default_resources = hpc_config.get("default_resources", "select=1:ncpus=4:mem=16gb")

            table.add_row("Default Walltime", "[green]✓[/green]", default_walltime)
            table.add_row("Default Resources", "[green]✓[/green]", default_resources)
        else:
            table.add_row("HSM Config", "[red]✗[/red]", "Not configured")

        console.print(table)

        # Show current queue status
        if has_pbs or has_slurm:
            console.print("\n[bold]Current Queue Summary:[/bold]")
            try:
                # Try to get a quick queue summary
                if has_pbs:
                    result = subprocess.run(
                        ["qstat", "-Q"], capture_output=True, text=True, timeout=5
                    )
                    if result.returncode == 0:
                        console.print(f"[dim]{result.stdout.strip()}[/dim]")
                elif has_slurm:
                    result = subprocess.run(
                        ["sinfo", "-s"], capture_output=True, text=True, timeout=5
                    )
                    if result.returncode == 0:
                        console.print(f"[dim]{result.stdout.strip()}[/dim]")
            except:
                console.print("[yellow]Could not query queue status[/yellow]")

    except Exception as e:
        handle_cli_error(console, logger, e, "HPC status")


@hpc.command("cancel")
@click.argument("job_id")
@click.option("--force", "-f", is_flag=True, help="Force cancellation without confirmation")
@common_options
@click.pass_context
def cancel_job(ctx, job_id: str, force: bool, verbose: bool, quiet: bool):
    """Cancel a specific HPC job."""
    logger, console = setup_cli_context(ctx, verbose, quiet)

    try:
        console.print("[bold blue]🏛️  Cancel HPC Job[/bold blue]")

        # Detect HPC system
        try:
            hpc_manager = HPCJobManager.auto_detect()
            scheduler = hpc_manager.scheduler_name
        except RuntimeError:
            console.print("[red]No HPC scheduler detected.[/red]")
            return

        # Confirm cancellation
        if not force:
            from .common import confirm_action

            if not confirm_action(console, f"Cancel job {job_id}?"):
                console.print("[yellow]Cancellation aborted.[/yellow]")
                return

        # Cancel the job
        if scheduler == "pbs":
            cmd = ["qdel", job_id]
        else:  # slurm
            cmd = ["scancel", job_id]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            console.print(f"[green]✓ Job {job_id} cancelled successfully.[/green]")
        else:
            console.print(f"[red]Failed to cancel job {job_id}: {result.stderr}[/red]")

    except Exception as e:
        handle_cli_error(console, logger, e, "job cancellation")


if __name__ == "__main__":
    hpc()
