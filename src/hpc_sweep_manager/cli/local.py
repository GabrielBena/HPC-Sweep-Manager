"""CLI commands for local execution mode."""

from pathlib import Path
from typing import Optional

import click
from rich.table import Table

from ..core.common.config import SweepConfig
from ..core.common.param_generator import ParameterGenerator
from ..core.local.local_manager import LocalJobManager
from .common import (
    common_options,
    display_sweep_summary,
    handle_cli_error,
    load_hsm_config_with_validation,
    setup_cli_context,
)


@click.group()
def local():
    """Local execution mode commands."""
    pass


@local.command("run")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default=Path("sweeps/sweep.yaml"),
    help="Path to sweep configuration file",
)
@click.option("--parallel-jobs", "-p", type=int, default=2, help="Maximum parallel jobs")
@click.option("--max-runs", type=int, help="Maximum number of runs to execute")
@click.option("--show-output", is_flag=True, help="Show job output in real-time")
@click.option("--dry-run", is_flag=True, help="Show what would be executed without running")
@click.option("--group", help="W&B group name for this sweep")
@common_options
@click.pass_context
def run_local(
    ctx,
    config: Path,
    parallel_jobs: int,
    max_runs: Optional[int],
    show_output: bool,
    dry_run: bool,
    group: Optional[str],
    verbose: bool,
    quiet: bool,
):
    """Run a parameter sweep locally with parallel execution."""
    logger, console = setup_cli_context(ctx, verbose, quiet)

    try:
        console.print("[bold blue]🖥️  Local Execution Mode[/bold blue]")

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
        display_sweep_summary(console, len(param_combinations), "local", config, max_runs)

        if dry_run:
            console.print("\n[bold]Dry Run - Jobs that would be executed:[/bold]")

            # Show first few parameter combinations
            preview_count = min(5, len(param_combinations))
            for i, params in enumerate(param_combinations[:preview_count]):
                console.print(f"  [cyan]{i + 1:3d}.[/cyan] {params}")

            if len(param_combinations) > preview_count:
                console.print(f"  ... and {len(param_combinations) - preview_count} more")

            console.print("\n[bold]Local Execution Details:[/bold]")
            console.print(f"  Max parallel jobs: [yellow]{parallel_jobs}[/yellow]")
            console.print(f"  Show output: [yellow]{'Yes' if show_output else 'No'}[/yellow]")
            console.print(f"  Total jobs: [yellow]{len(param_combinations)}[/yellow]")
            return

        # Create local job manager
        local_manager = LocalJobManager(
            python_path=hsm_config.get_default_python_path() or "python",
            script_path=hsm_config.get_default_script_path() or "train.py",
            project_dir=hsm_config.get_project_root() or str(Path.cwd()),
            max_parallel_jobs=parallel_jobs,
            show_progress=not quiet,
            show_output=show_output,
        )

        # Create sweep directory
        from ..core.common.utils import create_sweep_id

        sweep_id = create_sweep_id("local_sweep")
        sweep_dir = Path("sweeps") / "outputs" / sweep_id
        sweep_dir.mkdir(parents=True, exist_ok=True)

        console.print(f"\n[bold]Starting local sweep:[/bold] [cyan]{sweep_id}[/cyan]")
        console.print(f"Output directory: [dim]{sweep_dir}[/dim]")

        # Run the sweep
        job_ids = local_manager.submit_sweep(
            param_combinations=param_combinations,
            mode="array",  # Use array mode for parallel execution
            sweep_dir=sweep_dir,
            sweep_id=sweep_id,
            wandb_group=group,
        )

        console.print("\n[green]✓ Local sweep completed successfully![/green]")
        console.print(f"Results saved to: [cyan]{sweep_dir}[/cyan]")

    except Exception as e:
        handle_cli_error(console, logger, e, "local execution")


@local.command("status")
@common_options
@click.pass_context
def local_status(ctx, verbose: bool, quiet: bool):
    """Show status of local execution environment."""
    logger, console = setup_cli_context(ctx, verbose, quiet)

    try:
        console.print("[bold blue]🖥️  Local Execution Status[/bold blue]")

        # Load HSM config
        hsm_config = load_hsm_config_with_validation(console, logger)
        if not hsm_config:
            return

        # Create status table
        table = Table(title="Local Execution Environment")
        table.add_column("Component", style="cyan", width=20)
        table.add_column("Status", style="bold", width=15)
        table.add_column("Value", style="dim", width=50)

        # Python interpreter
        python_path = hsm_config.get_default_python_path() or "python"
        table.add_row("Python Interpreter", "[green]✓[/green]", python_path)

        # Training script
        script_path = hsm_config.get_default_script_path()
        if script_path and Path(script_path).exists():
            table.add_row("Training Script", "[green]✓[/green]", script_path)
        else:
            table.add_row("Training Script", "[yellow]?[/yellow]", "Not configured or not found")

        # Project directory
        project_dir = hsm_config.get_project_root() or str(Path.cwd())
        table.add_row("Project Directory", "[green]✓[/green]", project_dir)

        # Check system resources
        try:
            import psutil

            cpu_count = psutil.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024**3)
            table.add_row("CPU Cores", "[green]✓[/green]", f"{cpu_count} cores")
            table.add_row("Memory", "[green]✓[/green]", f"{memory_gb:.1f} GB")
        except ImportError:
            table.add_row("System Info", "[yellow]?[/yellow]", "psutil not available")

        console.print(table)

        # Show recent local sweeps
        sweeps_dir = Path("sweeps/outputs")
        if sweeps_dir.exists():
            local_sweeps = [d for d in sweeps_dir.iterdir() if d.is_dir() and "local" in d.name]

            if local_sweeps:
                console.print("\n[bold]Recent Local Sweeps:[/bold]")

                sweep_table = Table()
                sweep_table.add_column("Sweep ID", style="cyan")
                sweep_table.add_column("Created", style="dim")
                sweep_table.add_column("Tasks", style="yellow")

                # Sort by creation time (newest first)
                local_sweeps.sort(key=lambda x: x.stat().st_mtime, reverse=True)

                for sweep_dir in local_sweeps[:5]:  # Show last 5
                    import datetime

                    created = datetime.datetime.fromtimestamp(sweep_dir.stat().st_mtime)

                    # Count tasks
                    tasks_dir = sweep_dir / "tasks"
                    task_count = len(list(tasks_dir.iterdir())) if tasks_dir.exists() else 0

                    sweep_table.add_row(
                        sweep_dir.name,
                        created.strftime("%Y-%m-%d %H:%M"),
                        str(task_count),
                    )

                console.print(sweep_table)

    except Exception as e:
        handle_cli_error(console, logger, e, "local status")


@local.command("clean")
@click.option("--days", "-d", type=int, default=7, help="Clean sweeps older than N days")
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be cleaned without actually cleaning",
)
@click.option("--force", "-f", is_flag=True, help="Force cleanup without confirmation")
@common_options
@click.pass_context
def clean_local(ctx, days: int, dry_run: bool, force: bool, verbose: bool, quiet: bool):
    """Clean up old local sweep outputs."""
    logger, console = setup_cli_context(ctx, verbose, quiet)

    try:
        console.print("[bold blue]🧹 Local Cleanup[/bold blue]")

        sweeps_dir = Path("sweeps/outputs")
        if not sweeps_dir.exists():
            console.print("[yellow]No sweeps directory found.[/yellow]")
            return

        import datetime

        cutoff_time = datetime.datetime.now() - datetime.timedelta(days=days)

        # Find old local sweeps
        old_sweeps = []
        for sweep_dir in sweeps_dir.iterdir():
            if (
                sweep_dir.is_dir()
                and "local" in sweep_dir.name
                and datetime.datetime.fromtimestamp(sweep_dir.stat().st_mtime) < cutoff_time
            ):
                old_sweeps.append(sweep_dir)

        if not old_sweeps:
            console.print(f"[green]No local sweeps older than {days} days found.[/green]")
            return

        console.print(
            f"[yellow]Found {len(old_sweeps)} local sweeps older than {days} days:[/yellow]"
        )

        total_size = 0
        for sweep_dir in old_sweeps:
            created = datetime.datetime.fromtimestamp(sweep_dir.stat().st_mtime)

            # Calculate directory size
            size = sum(f.stat().st_size for f in sweep_dir.rglob("*") if f.is_file())
            total_size += size

            size_mb = size / (1024 * 1024)
            console.print(
                f"  [cyan]{sweep_dir.name}[/cyan] - {created.strftime('%Y-%m-%d')} ({size_mb:.1f} MB)"
            )

        total_mb = total_size / (1024 * 1024)
        console.print(f"\nTotal size: [yellow]{total_mb:.1f} MB[/yellow]")

        if dry_run:
            console.print("\n[bold]Dry run - no files were deleted.[/bold]")
            return

        # Confirm deletion
        if not force:
            from .common import confirm_action

            if not confirm_action(console, f"Delete {len(old_sweeps)} old sweep directories?"):
                console.print("[yellow]Cleanup cancelled.[/yellow]")
                return

        # Delete old sweeps
        import shutil

        deleted_count = 0
        for sweep_dir in old_sweeps:
            try:
                shutil.rmtree(sweep_dir)
                deleted_count += 1
                if verbose:
                    console.print(f"  [dim]Deleted {sweep_dir.name}[/dim]")
            except Exception as e:
                console.print(f"  [red]Failed to delete {sweep_dir.name}: {e}[/red]")

        console.print(
            f"[green]✓ Cleaned up {deleted_count} old local sweeps ({total_mb:.1f} MB freed)[/green]"
        )

    except Exception as e:
        handle_cli_error(console, logger, e, "local cleanup")


if __name__ == "__main__":
    local()
