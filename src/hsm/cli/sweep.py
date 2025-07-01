"""Sweep execution CLI commands."""

import asyncio
from pathlib import Path
from typing import List, Optional

import click
from rich.console import Console
from rich.table import Table

from ..config.hsm import HSMConfig
from ..config.sweep import SweepConfig
from ..core.engine import SweepEngine
from ..utils.common import create_sweep_id
from ..utils.logging import get_logger
from .utils import create_compute_source, format_duration, parse_compute_sources


@click.group("sweep")
@click.pass_context
def sweep_cmd(ctx):
    """Sweep management commands."""
    pass


@sweep_cmd.command("run")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default=Path("sweeps/sweep_config.yaml"),
    help="Path to sweep configuration file",
)
@click.option(
    "--sources",
    "-s",
    type=str,
    default="local",
    help="Comma-separated list of compute sources (local,ssh:hostname,hpc:cluster)",
)
@click.option("--dry-run", "-d", is_flag=True, help="Show what would be executed")
@click.option("--count-only", is_flag=True, help="Count combinations and exit")
@click.option("--max-tasks", type=int, help="Maximum number of tasks to execute")
@click.option("--parallel-limit", "-p", type=int, help="Maximum parallel tasks per source")
@click.option(
    "--output-dir", type=click.Path(path_type=Path), help="Output directory for sweep results"
)
@click.option("--sweep-id", help="Custom sweep ID (auto-generated if not provided)")
@click.pass_context
def run_cmd(
    ctx,
    config: Path,
    sources: str,
    dry_run: bool,
    count_only: bool,
    max_tasks: Optional[int],
    parallel_limit: Optional[int],
    output_dir: Optional[Path],
    sweep_id: Optional[str],
):
    """Run a parameter sweep."""
    console: Console = ctx.obj["console"]
    logger = get_logger()

    try:
        # Load configuration
        console.print(f"[cyan]Loading sweep configuration from {config}[/cyan]")
        sweep_config = SweepConfig.from_yaml(config)

        # Load HSM configuration
        hsm_config = HSMConfig.load()

        # Parse compute sources
        try:
            source_specs = parse_compute_sources(sources)
            console.print(f"[cyan]Parsed {len(source_specs)} compute source(s)[/cyan]")
        except ValueError as e:
            console.print(f"[red]Error parsing compute sources: {e}[/red]")
            raise click.Abort()

        # Count parameter combinations
        from ..utils.params import ParameterGenerator

        param_gen = ParameterGenerator(sweep_config)
        total_combinations = param_gen.count_combinations()

        console.print(f"[cyan]Total parameter combinations: {total_combinations}[/cyan]")

        if count_only:
            return

        # Apply max_tasks limit
        if max_tasks and max_tasks < total_combinations:
            console.print(
                f"[yellow]Limiting to {max_tasks} tasks (out of {total_combinations})[/yellow]"
            )

        if dry_run:
            console.print("[yellow]Dry run mode - showing planned execution[/yellow]")
            _show_dry_run_info(console, sweep_config, source_specs, total_combinations, max_tasks)
            return

        # Create compute sources
        compute_sources = []
        for source_type, source_config in source_specs:
            try:
                source = create_compute_source(source_type, source_config)
                compute_sources.append(source)
                console.print(f"[green]✓ Created {source_type} compute source[/green]")
            except Exception as e:
                console.print(f"[red]Failed to create {source_type} compute source: {e}[/red]")
                raise click.Abort()

        # Run the sweep
        console.print("[cyan]Starting sweep execution...[/cyan]")
        result = asyncio.run(
            _run_sweep_async(
                sweep_config,
                compute_sources,
                max_tasks,
                parallel_limit,
                output_dir,
                sweep_id,
                console,
                logger,
            )
        )

        # Display results
        console.print(f"[green]✓ Sweep completed: {result.sweep_id}[/green]")
        console.print(f"  Total tasks: {result.total_tasks}")
        console.print(f"  Completed: {result.completed_tasks}")
        console.print(f"  Failed: {result.failed_tasks}")
        console.print(f"  Duration: {format_duration(result.duration)}")

    except Exception as e:
        logger.error(f"Sweep execution failed: {e}")
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@sweep_cmd.command("complete")
@click.argument("sweep_id")
@click.option(
    "--sources",
    "-s",
    type=str,
    default="local",
    help="Comma-separated list of compute sources for completion",
)
@click.option("--dry-run", "-d", is_flag=True, help="Show what would be completed")
@click.option("--retry-failed", is_flag=True, help="Retry failed tasks in addition to missing ones")
@click.pass_context
def complete_cmd(
    ctx,
    sweep_id: str,
    sources: str,
    dry_run: bool,
    retry_failed: bool,
):
    """Complete a partial sweep."""
    asyncio.run(complete_sweep_impl(ctx, sweep_id, sources, dry_run, retry_failed))


@sweep_cmd.command("status")
@click.argument("sweep_id", required=False)
@click.option("--all", "-a", is_flag=True, help="Show status of all sweeps")
@click.pass_context
def status_cmd(ctx, sweep_id: Optional[str], all: bool):
    """Show sweep status."""
    asyncio.run(show_sweep_status_impl(ctx, sweep_id, all))


@sweep_cmd.command("cancel")
@click.argument("sweep_id")
@click.option("--force", "-f", is_flag=True, help="Force cancellation without confirmation")
@click.pass_context
def cancel_cmd(ctx, sweep_id: str, force: bool):
    """Cancel a running sweep."""
    asyncio.run(cancel_sweep_impl(ctx, sweep_id, force))


# Helper functions


async def _run_sweep_async(
    sweep_config: SweepConfig,
    compute_sources: List,
    max_tasks: Optional[int],
    parallel_limit: Optional[int],
    output_dir: Optional[Path],
    sweep_id: Optional[str],
    console: Console,
    logger,
):
    """Run sweep asynchronously."""
    from pathlib import Path

    from ..compute.base import SweepContext
    from ..utils.params import ParameterGenerator

    # Generate sweep ID if not provided
    if not sweep_id:
        sweep_id = create_sweep_id()

    # Setup output directory (use sweeps/outputs structure)
    if not output_dir:
        output_dir = Path.cwd() / "sweeps" / "outputs" / sweep_id

    output_dir.mkdir(parents=True, exist_ok=True)

    # Create sweep context
    sweep_context = SweepContext(
        sweep_id=sweep_id,
        sweep_dir=output_dir,
        config=sweep_config.to_dict(),
        max_parallel_tasks=parallel_limit,
    )

    # Create SweepEngine with required arguments
    engine = SweepEngine(
        sweep_context=sweep_context, sources=compute_sources, max_concurrent_tasks=max_tasks
    )

    # Setup sources
    setup_success = await engine.setup_sources()
    if not setup_success:
        raise RuntimeError("Failed to setup compute sources")

    # Generate tasks
    param_gen = ParameterGenerator(sweep_config)
    task_params = param_gen.generate_combinations()

    if max_tasks:
        task_params = task_params[:max_tasks]

    # Create Task objects with simple task IDs
    from ..compute.base import Task

    tasks = []
    for i, params in enumerate(task_params):
        task = Task(task_id=f"task_{i + 1:03d}", params=params, sweep_id=sweep_id)
        tasks.append(task)

    # Run the sweep
    return await engine.run_sweep(tasks)


async def complete_sweep_impl(
    ctx: click.Context,
    sweep_id: str,
    sources: str,
    dry_run: bool,
    retry_failed: bool,
) -> None:
    """Implementation for sweep completion."""
    from ..core.engine import SweepEngine
    from ..core.tracker import SweepTracker
    from ..config.hsm import HSMConfig

    console = ctx.obj["console"]
    logger = ctx.obj["logger"]

    try:
        # Load HSM configuration
        hsm_config = HSMConfig.load()
        if hsm_config is None:
            console.print("[red]No HSM configuration found. Run 'hsm config init' first.[/red]")
            raise click.Abort()

        # Find the sweep directory
        sweep_dir = Path("sweeps/outputs") / sweep_id
        if not sweep_dir.exists():
            # Try alternative locations
            alt_locations = [
                Path("sweeps") / "outputs" / sweep_id,
                Path("outputs") / sweep_id,
                Path(sweep_id),
            ]
            for alt_dir in alt_locations:
                if alt_dir.exists():
                    sweep_dir = alt_dir
                    break
            else:
                console.print(f"[red]Sweep directory not found: {sweep_id}[/red]")
                raise click.Abort()

        # Load sweep tracker to check completion status
        tracker = SweepTracker(sweep_dir)
        await tracker.load_from_disk()

        completion_status = tracker.get_completion_status()
        missing_tasks = tracker.get_missing_tasks()
        failed_tasks = tracker.get_failed_tasks() if retry_failed else []

        tasks_to_complete = missing_tasks + failed_tasks

        if not tasks_to_complete:
            console.print(f"[green]✓ Sweep {sweep_id} is already complete![/green]")
            return

        # Show completion plan
        with console.status(f"Planning completion for sweep {sweep_id}..."):
            console.print(f"\n[bold]Sweep Completion Plan[/bold]")
            console.print(f"Missing tasks: {len(missing_tasks)}")
            if retry_failed:
                console.print(f"Failed tasks to retry: {len(failed_tasks)}")
            console.print(f"Total tasks to complete: {len(tasks_to_complete)}")

        if dry_run:
            console.print("\n[yellow]DRY RUN - No tasks will be executed[/yellow]")
            for i, task in enumerate(tasks_to_complete[:5]):
                console.print(f"  Task {task.task_id}: {task.params}")
            if len(tasks_to_complete) > 5:
                console.print(f"  ... and {len(tasks_to_complete) - 5} more tasks")
            return

        # Parse and create compute sources
        compute_sources = parse_compute_sources(sources, hsm_config)
        if not compute_sources:
            console.print("[red]No valid compute sources specified[/red]")
            raise click.Abort()

        # Load original sweep context
        sweep_config_file = sweep_dir / "sweep_config.yaml"
        if not sweep_config_file.exists():
            console.print(f"[red]Sweep configuration not found: {sweep_config_file}[/red]")
            raise click.Abort()

        with open(sweep_config_file) as f:
            import yaml

            original_config = yaml.safe_load(f)

        from ..compute.base import SweepContext

        sweep_context = SweepContext(
            sweep_id=sweep_id,
            sweep_dir=sweep_dir,
            config=original_config,
        )

        # Create and run completion engine
        engine = SweepEngine(
            sweep_context=sweep_context,
            sources=compute_sources,
        )

        console.print(f"\n[green]Starting completion of {len(tasks_to_complete)} tasks...[/green]")

        # Setup sources
        if not await engine.setup_sources():
            console.print("[red]Failed to setup compute sources[/red]")
            raise click.Abort()

        # Execute completion
        result = await engine.run_sweep(tasks_to_complete)

        # Display results
        console.print(f"\n[bold]Completion Results[/bold]")
        console.print(f"Status: {result.status.value}")
        console.print(f"Completed: {result.completed_tasks}/{result.total_tasks}")
        console.print(f"Success rate: {result.success_rate:.1f}%")
        if result.duration:
            console.print(f"Duration: {result.duration:.1f}s")

    except Exception as e:
        logger.error(f"Error completing sweep: {e}")
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


async def show_sweep_status_impl(
    ctx: click.Context,
    sweep_id: Optional[str],
    show_all: bool,
) -> None:
    """Implementation for showing sweep status."""
    from ..core.tracker import SweepTracker

    console = ctx.obj["console"]
    logger = ctx.obj["logger"]

    try:
        if show_all:
            # Show status of all sweeps
            outputs_dir = Path("sweeps/outputs")
            if not outputs_dir.exists():
                console.print("[yellow]No sweeps directory found[/yellow]")
                return

            sweep_dirs = [d for d in outputs_dir.iterdir() if d.is_dir()]
            if not sweep_dirs:
                console.print("[yellow]No sweeps found[/yellow]")
                return

            console.print("[bold]All Sweeps Status[/bold]\n")

            table = Table(show_header=True, header_style="bold blue")
            table.add_column("Sweep ID", style="cyan")
            table.add_column("Status", style="green")
            table.add_column("Progress", justify="right")
            table.add_column("Completed", justify="right")
            table.add_column("Failed", justify="right")
            table.add_column("Duration")

            for sweep_dir in sorted(sweep_dirs, key=lambda x: x.stat().st_mtime, reverse=True):
                try:
                    tracker = SweepTracker(sweep_dir)
                    await tracker.load_from_disk()

                    status = tracker.get_completion_status()
                    total = status["total_tasks"]
                    completed = status["completed_tasks"]
                    failed = status["failed_tasks"]
                    progress_pct = (completed + failed) / total * 100 if total > 0 else 0

                    # Get duration if available
                    metadata_file = sweep_dir / "sweep_metadata.yaml"
                    duration = "Unknown"
                    if metadata_file.exists():
                        with open(metadata_file) as f:
                            import yaml

                            metadata = yaml.safe_load(f)
                            start_time = metadata.get("start_time")
                            end_time = metadata.get("end_time")
                            if start_time and end_time:
                                from datetime import datetime

                                start = datetime.fromisoformat(start_time)
                                end = datetime.fromisoformat(end_time)
                                duration_sec = (end - start).total_seconds()
                                duration = (
                                    f"{duration_sec / 3600:.1f}h"
                                    if duration_sec > 3600
                                    else f"{duration_sec / 60:.1f}m"
                                )

                    status_color = (
                        "green"
                        if completed == total and failed == 0
                        else "yellow"
                        if completed + failed == total
                        else "blue"
                    )
                    status_text = (
                        "Complete"
                        if completed == total and failed == 0
                        else "Partial"
                        if completed + failed == total
                        else "Running"
                    )

                    table.add_row(
                        sweep_dir.name,
                        f"[{status_color}]{status_text}[/{status_color}]",
                        f"{progress_pct:.1f}%",
                        str(completed),
                        str(failed),
                        duration,
                    )
                except Exception as e:
                    table.add_row(sweep_dir.name, "[red]Error[/red]", "N/A", "N/A", "N/A", "N/A")

            console.print(table)

        else:
            # Show status of specific sweep
            if not sweep_id:
                # Try to find the most recent sweep
                outputs_dir = Path("sweeps/outputs")
                if outputs_dir.exists():
                    sweep_dirs = [d for d in outputs_dir.iterdir() if d.is_dir()]
                    if sweep_dirs:
                        # Get most recent
                        latest_dir = max(sweep_dirs, key=lambda x: x.stat().st_mtime)
                        sweep_id = latest_dir.name
                        console.print(
                            f"[yellow]No sweep ID specified, using most recent: {sweep_id}[/yellow]\n"
                        )
                    else:
                        console.print("[yellow]No sweeps found and no sweep ID specified[/yellow]")
                        return
                else:
                    console.print(
                        "[yellow]No sweeps directory found and no sweep ID specified[/yellow]"
                    )
                    return

            # Find the sweep directory
            sweep_dir = Path("sweeps/outputs") / sweep_id
            if not sweep_dir.exists():
                console.print(f"[red]Sweep not found: {sweep_id}[/red]")
                raise click.Abort()

            # Load and display detailed status
            tracker = SweepTracker(sweep_dir)
            await tracker.load_from_disk()

            status = tracker.get_completion_status()

            console.print(f"[bold]Sweep Status: {sweep_id}[/bold]\n")

            # Main status table
            table = Table(show_header=True, header_style="bold blue")
            table.add_column("Metric", style="cyan")
            table.add_column("Value")

            total = status["total_tasks"]
            completed = status["completed_tasks"]
            failed = status["failed_tasks"]
            active = status["active_tasks"]
            pending = status["pending_tasks"]
            progress_pct = (completed + failed) / total * 100 if total > 0 else 0

            table.add_row(
                "Status",
                "Running"
                if active > 0 or pending > 0
                else "Complete"
                if failed == 0
                else "Partial",
            )
            table.add_row("Progress", f"{completed + failed}/{total} ({progress_pct:.1f}%)")
            table.add_row("Completed", f"[green]{completed}[/green]")
            table.add_row("Failed", f"[red]{failed}[/red]" if failed > 0 else "0")
            table.add_row("Active", f"[blue]{active}[/blue]" if active > 0 else "0")
            table.add_row("Pending", f"[yellow]{pending}[/yellow]" if pending > 0 else "0")

            # Add duration and ETA if available
            metadata_file = sweep_dir / "sweep_metadata.yaml"
            if metadata_file.exists():
                with open(metadata_file) as f:
                    import yaml

                    metadata = yaml.safe_load(f)
                    start_time = metadata.get("start_time")
                    if start_time:
                        from datetime import datetime

                        start = datetime.fromisoformat(start_time)
                        duration_sec = (datetime.now() - start).total_seconds()
                        duration = (
                            f"{duration_sec / 3600:.1f}h"
                            if duration_sec > 3600
                            else f"{duration_sec / 60:.1f}m"
                        )
                        table.add_row("Duration", duration)

                        # Calculate ETA if still running
                        if active > 0 or pending > 0:
                            remaining = pending + active
                            if completed > 0:
                                avg_time_per_task = duration_sec / completed
                                eta_sec = remaining * avg_time_per_task
                                eta = (
                                    f"{eta_sec / 3600:.1f}h"
                                    if eta_sec > 3600
                                    else f"{eta_sec / 60:.1f}m"
                                )
                                table.add_row("ETA", eta)

            console.print(table)

            # Show recent errors if any
            errors_dir = sweep_dir / "errors"
            if errors_dir.exists():
                error_files = list(errors_dir.glob("*_error.txt"))
                if error_files:
                    console.print(f"\n[yellow]Recent Errors ({len(error_files)} found):[/yellow]")
                    for error_file in sorted(error_files)[-3:]:  # Show last 3
                        console.print(f"  • {error_file.stem}")
                    if len(error_files) > 3:
                        console.print(f"  ... and {len(error_files) - 3} more errors")
                    console.print(
                        f"\nUse 'hsm monitor errors {sweep_id}' for detailed error analysis"
                    )

    except Exception as e:
        logger.error(f"Error showing sweep status: {e}")
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


async def cancel_sweep_impl(
    ctx: click.Context,
    sweep_id: str,
    force: bool,
) -> None:
    """Implementation for sweep cancellation."""
    console = ctx.obj["console"]
    logger = ctx.obj["logger"]

    try:
        # Find the sweep directory
        sweep_dir = Path("sweeps/outputs") / sweep_id
        if not sweep_dir.exists():
            console.print(f"[red]Sweep not found: {sweep_id}[/red]")
            raise click.Abort()

        # Check if sweep is actually running
        from ..core.tracker import SweepTracker

        tracker = SweepTracker(sweep_dir)
        await tracker.load_from_disk()

        status = tracker.get_completion_status()
        if status["active_tasks"] == 0 and status["pending_tasks"] == 0:
            console.print(f"[yellow]Sweep {sweep_id} is not currently running[/yellow]")
            return

        # Confirm cancellation unless force flag is used
        if not force:
            console.print(f"[yellow]About to cancel sweep: {sweep_id}[/yellow]")
            console.print(f"Active tasks: {status['active_tasks']}")
            console.print(f"Pending tasks: {status['pending_tasks']}")

            if not click.confirm("Are you sure you want to cancel this sweep?"):
                console.print("Cancellation aborted")
                return

        console.print(f"[red]Cancelling sweep {sweep_id}...[/red]")

        # Create a cancellation marker file
        cancel_file = sweep_dir / ".cancelled"
        with open(cancel_file, "w") as f:
            from datetime import datetime

            f.write(f"Cancelled at: {datetime.now().isoformat()}\n")
            f.write(f"Cancelled by: CLI\n")

        # Try to find and cancel running engine if possible
        # This is best-effort - the actual cancellation will be handled by the running engine

        console.print(f"[green]✓ Cancellation signal sent for sweep {sweep_id}[/green]")
        console.print("Note: Active tasks may take some time to respond to cancellation")

    except Exception as e:
        logger.error(f"Error cancelling sweep: {e}")
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


def _show_dry_run_info(
    console: Console,
    sweep_config: SweepConfig,
    source_specs: List,
    total_combinations: int,
    max_tasks: Optional[int],
):
    """Show dry run information."""
    console.print("\n[bold]Dry Run Information[/bold]")

    # Show parameter info
    table = Table(title="Parameter Combinations")
    table.add_column("Parameter", style="cyan")
    table.add_column("Values", style="green")

    # Show grid parameters
    for param_name, values in sweep_config.grid.items():
        if isinstance(values, list):
            values_str = f"{len(values)} values"
        else:
            values_str = str(values)
        table.add_row(param_name, values_str)

    # Show paired parameters
    for group_idx, group in enumerate(sweep_config.paired):
        for group_name, params in group.items():
            for param_name, values in params.items():
                if isinstance(values, list):
                    values_str = f"{len(values)} values (paired group {group_idx})"
                else:
                    values_str = f"{values} (paired group {group_idx})"
                table.add_row(f"{group_name}.{param_name}", values_str)

    console.print(table)

    # Show compute sources
    console.print("\n[bold]Compute Sources:[/bold]")
    for source_type, config in source_specs:
        console.print(f"  • {source_type}: {config}")

    # Show execution plan
    actual_tasks = min(max_tasks or total_combinations, total_combinations)
    console.print("\n[bold]Execution Plan:[/bold]")
    console.print(f"  Total combinations: {total_combinations}")
    console.print(f"  Tasks to execute: {actual_tasks}")
    console.print(f"  Tasks per source: ~{actual_tasks // len(source_specs)}")
