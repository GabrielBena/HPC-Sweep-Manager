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
from ..utils.logging import get_logger
from ..utils.common import create_sweep_id
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
@click.option("--parallel-limit", "-p", type=int, help="Maximum parallel tasks per source")
@click.pass_context
def complete_cmd(
    ctx,
    sweep_id: str,
    sources: str,
    dry_run: bool,
    retry_failed: bool,
    parallel_limit: Optional[int],
):
    """Complete a partial sweep."""
    console: Console = ctx.obj["console"]
    logger = get_logger()

    try:
        # Parse compute sources
        try:
            source_specs = parse_compute_sources(sources)
        except ValueError as e:
            console.print(f"[red]Error parsing compute sources: {e}[/red]")
            raise click.Abort()

        # Create compute sources
        compute_sources = []
        for source_type, source_config in source_specs:
            try:
                source = create_compute_source(source_type, source_config)
                compute_sources.append(source)
            except Exception as e:
                console.print(f"[red]Failed to create {source_type} compute source: {e}[/red]")
                raise click.Abort()

        if dry_run:
            console.print("[yellow]Dry run mode - showing planned completion[/yellow]")
            # TODO: Implement dry run for completion
            return

        # Complete the sweep
        console.print(f"[cyan]Completing sweep {sweep_id}...[/cyan]")
        result = asyncio.run(
            _complete_sweep_async(
                sweep_id, compute_sources, retry_failed, parallel_limit, console, logger
            )
        )

        # Display results
        console.print("[green]✓ Sweep completion finished[/green]")
        console.print(f"  Completed tasks: {result.completed_tasks}")
        console.print(f"  Duration: {format_duration(result.duration)}")

    except Exception as e:
        logger.error(f"Sweep completion failed: {e}")
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@sweep_cmd.command("status")
@click.argument("sweep_id", required=False)
@click.option("--all", "-a", is_flag=True, help="Show status of all sweeps")
@click.option("--incomplete-only", is_flag=True, help="Show only incomplete sweeps")
@click.pass_context
def status_cmd(ctx, sweep_id: Optional[str], all: bool, incomplete_only: bool):
    """Show sweep status."""
    console: Console = ctx.obj["console"]

    try:
        if sweep_id:
            _show_sweep_status(console, sweep_id)
        else:
            _show_all_sweeps_status(console, all, incomplete_only)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@sweep_cmd.command("cancel")
@click.argument("sweep_id")
@click.option("--force", "-f", is_flag=True, help="Force cancellation without confirmation")
@click.pass_context
def cancel_cmd(ctx, sweep_id: str, force: bool):
    """Cancel a running sweep."""
    console: Console = ctx.obj["console"]
    logger = get_logger()

    try:
        if not force:
            from .utils import confirm_action

            if not confirm_action(f"Cancel sweep {sweep_id}?"):
                console.print("Cancelled.")
                return

        # Cancel the sweep
        console.print(f"[cyan]Cancelling sweep {sweep_id}...[/cyan]")
        success = asyncio.run(_cancel_sweep_async(sweep_id, console, logger))

        if success:
            console.print(f"[green]✓ Sweep {sweep_id} cancelled[/green]")
        else:
            console.print(
                f"[yellow]Sweep {sweep_id} could not be cancelled (may not be running)[/yellow]"
            )

    except Exception as e:
        logger.error(f"Sweep cancellation failed: {e}")
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


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


async def _complete_sweep_async(
    sweep_id: str,
    compute_sources: List,
    retry_failed: bool,
    parallel_limit: Optional[int],
    console: Console,
    logger,
):
    """Complete sweep asynchronously."""
    from pathlib import Path

    from ..compute.base import SweepContext

    # Find sweep directory (use sweeps/outputs structure)
    sweep_dir = Path.cwd() / "sweeps" / "outputs" / sweep_id
    if not sweep_dir.exists():
        raise RuntimeError(f"Sweep directory not found: {sweep_dir}")

    # Create sweep context
    sweep_context = SweepContext(
        sweep_id=sweep_id,
        sweep_dir=sweep_dir,
        config={},  # Will be loaded from sweep directory
        max_parallel_tasks=parallel_limit,
    )

    # Create SweepEngine with required arguments
    engine = SweepEngine(sweep_context=sweep_context, sources=compute_sources)

    # Setup sources
    setup_success = await engine.setup_sources()
    if not setup_success:
        raise RuntimeError("Failed to setup compute sources")

    # TODO: Implement sweep completion logic
    console.print("[yellow]Sweep completion not yet fully implemented[/yellow]")

    # For now, return a basic result
    from datetime import datetime

    from ..core.engine import SweepResult, SweepStatus

    return SweepResult(
        sweep_id=sweep_id,
        status=SweepStatus.COMPLETED,
        total_tasks=0,
        completed_tasks=0,
        failed_tasks=0,
        cancelled_tasks=0,
        start_time=datetime.now(),
        end_time=datetime.now(),
    )


async def _cancel_sweep_async(sweep_id: str, console: Console, logger) -> bool:
    """Cancel sweep asynchronously."""
    from pathlib import Path

    from ..compute.base import SweepContext

    # Find sweep directory (use sweeps/outputs structure)
    sweep_dir = Path.cwd() / "sweeps" / "outputs" / sweep_id
    if not sweep_dir.exists():
        logger.warning(f"Sweep directory not found: {sweep_dir}")
        return False

    # Create sweep context
    sweep_context = SweepContext(sweep_id=sweep_id, sweep_dir=sweep_dir, config={})

    # Create SweepEngine with minimal sources (we just need to cancel)
    engine = SweepEngine(
        sweep_context=sweep_context,
        sources=[],  # No sources needed for cancellation
    )

    return await engine.cancel_sweep()


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


def _show_sweep_status(console: Console, sweep_id: str):
    """Show status for a specific sweep."""
    # TODO: Implement sweep status display
    console.print(f"[yellow]Status display for sweep {sweep_id} not yet implemented[/yellow]")


def _show_all_sweeps_status(console: Console, show_all: bool, incomplete_only: bool):
    """Show status for all sweeps."""
    # TODO: Implement all sweeps status display
    console.print("[yellow]All sweeps status display not yet implemented[/yellow]")
