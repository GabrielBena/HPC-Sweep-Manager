"""Main CLI entry point for HPC Sweep Manager."""

import click
from pathlib import Path
from rich.console import Console

from ..core.utils import setup_logging

console = Console()


@click.group()
@click.version_option(version="0.1.0", prog_name="hsm")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option("--quiet", "-q", is_flag=True, help="Suppress non-error output")
@click.pass_context
def cli(ctx: click.Context, verbose: bool, quiet: bool):
    """HPC Sweep Manager - Automated hyperparameter sweeps on HPC systems."""

    # Ensure that ctx.obj exists and is a dict
    ctx.ensure_object(dict)

    # Set up logging
    if quiet:
        log_level = "ERROR"
    elif verbose:
        log_level = "DEBUG"
    else:
        log_level = "INFO"

    logger = setup_logging(log_level)
    ctx.obj["logger"] = logger
    ctx.obj["console"] = console


@cli.command("init")
@click.option("--interactive", "-i", is_flag=True, help="Interactive setup mode")
@click.option(
    "--project-root", type=click.Path(exists=True), help="Project root directory"
)
@click.pass_context
def init_cmd(ctx, interactive: bool, project_root: str):
    """Initialize sweep infrastructure in a project."""
    from .init import init_project

    project_path = Path(project_root) if project_root else Path.cwd()
    init_project(project_path, interactive, ctx.obj["console"], ctx.obj["logger"])


@cli.command("configure")
@click.option(
    "--from-file",
    type=click.Path(exists=True),
    help="Build from existing Hydra config file",
)
@click.option("--output", "-o", type=click.Path(), help="Output sweep config file")
@click.pass_context
def configure_cmd(ctx, from_file: str, output: str):
    """Interactive sweep configuration builder."""
    from .configure import configure_sweep

    config_file = Path(from_file) if from_file else None
    output_file = Path(output) if output else None

    configure_sweep(config_file, output_file, ctx.obj["console"], ctx.obj["logger"])


@cli.command("sweep")
@click.option(
    "--config",
    "-c",
    type=click.Path(),
    default="sweeps/sweep.yaml",
    show_default=True,
    help="Sweep configuration file",
)
@click.option(
    "--mode",
    type=click.Choice(["individual", "array", "local"]),
    default="array",
    show_default=True,
    help="Job submission mode",
)
@click.option("--dry-run", "-d", is_flag=True, help="Preview jobs without submitting")
@click.option("--count", is_flag=True, help="Count combinations and exit")
@click.option("--max-runs", "-n", type=int, help="Maximum number of runs to submit")
@click.option(
    "--walltime",
    "-w",
    help="Job walltime (auto-detected from hsm_config.yaml or defaults to 23:59:59)",
)
@click.option(
    "--resources",
    help="HPC resources (auto-detected from hsm_config.yaml or defaults to select=1:ncpus=4:mem=64gb)",
)
@click.option("--group", help="W&B group name")
@click.option("--priority", type=int, help="Job priority")
@click.option(
    "--parallel-jobs",
    "-j",
    type=int,
    help="Maximum parallel jobs for local mode (default: 1)",
)
@click.pass_context
def sweep_cmd(
    ctx,
    config: str,
    mode: str,
    dry_run: bool,
    count: bool,
    max_runs: int,
    walltime: str,
    resources: str,
    group: str,
    priority: int,
    parallel_jobs: int,
):
    """Run parameter sweep."""
    from .sweep import run_sweep
    from ..core.hsm_config import HSMConfig

    # Load HSM config for defaults
    hsm_config = HSMConfig.load()

    # Use HSM config defaults if not provided via CLI
    if walltime is None:
        walltime = hsm_config.get_default_walltime() if hsm_config else "23:59:59"

    if resources is None:
        resources = (
            hsm_config.get_default_resources()
            if hsm_config
            else "select=1:ncpus=4:mem=64gb"
        )

    config_path = Path(config)

    run_sweep(
        config_path=config_path,
        mode=mode,
        dry_run=dry_run,
        count_only=count,
        max_runs=max_runs,
        walltime=walltime,
        resources=resources,
        group=group,
        priority=priority,
        parallel_jobs=parallel_jobs,
        console=ctx.obj["console"],
        logger=ctx.obj["logger"],
        hsm_config=hsm_config,
    )


@cli.command("monitor")
@click.argument("sweep_id", required=False)
@click.option(
    "--watch", "-w", is_flag=True, help="Watch mode - continuously update status"
)
@click.option(
    "--refresh",
    "-r",
    type=int,
    default=30,
    help="Refresh interval in seconds for watch mode",
)
@click.option(
    "--detailed",
    "-d",
    is_flag=True,
    help="Show detailed array job subjob status breakdown",
)
@click.pass_context
def monitor_cmd(ctx, sweep_id: str, watch: bool, refresh: int, detailed: bool):
    """Monitor sweep progress."""
    from .monitor import monitor_sweep

    monitor_sweep(
        sweep_id, watch, refresh, detailed, ctx.obj["console"], ctx.obj["logger"]
    )


@cli.command()
@click.pass_context
def status(ctx):
    """Show status of all active sweeps."""
    from .monitor import show_status

    show_status(ctx.obj["console"], ctx.obj["logger"])


@cli.command("recent")
@click.option("--days", "-d", type=int, default=7, help="Show sweeps from last N days")
@click.option(
    "--watch", "-w", is_flag=True, help="Watch mode - continuously update status"
)
@click.option(
    "--refresh",
    "-r",
    type=int,
    default=60,
    help="Refresh interval in seconds for watch mode",
)
@click.pass_context
def recent_cmd(ctx, days: int, watch: bool, refresh: int):
    """Show recent sweeps from the last N days."""
    from .monitor import show_recent_sweeps

    show_recent_sweeps(days, watch, refresh, ctx.obj["console"], ctx.obj["logger"])


@cli.command("queue")
@click.option(
    "--watch", "-w", is_flag=True, help="Watch mode - continuously update status"
)
@click.option(
    "--refresh",
    "-r",
    type=int,
    default=30,
    help="Refresh interval in seconds for watch mode",
)
@click.pass_context
def queue_cmd(ctx, watch: bool, refresh: int):
    """Show current queue status for all user jobs."""
    from .monitor import show_queue_status

    show_queue_status(watch, refresh, ctx.obj["console"], ctx.obj["logger"])


@cli.command()
@click.argument("sweep_id")
@click.option(
    "--force", "-f", is_flag=True, help="Force cancellation without confirmation"
)
@click.pass_context
def cancel(ctx, sweep_id: str, force: bool):
    """Cancel a running sweep."""
    from .monitor import cancel_sweep

    cancel_sweep(sweep_id, force, ctx.obj["console"], ctx.obj["logger"])


@cli.command("delete-jobs")
@click.argument("sweep_id")
@click.option(
    "--pattern", "-p", help="Filter jobs by name pattern (e.g., 'job_001', '*_002')"
)
@click.option("--state", "-s", help="Filter jobs by state (e.g., 'R', 'Q', 'H')")
@click.option(
    "--dry-run",
    "-d",
    is_flag=True,
    help="Show what would be deleted without actually deleting",
)
@click.option("--force", "-f", is_flag=True, help="Force deletion without confirmation")
@click.option(
    "--all-states",
    is_flag=True,
    help="Include jobs in all states (even completed ones)",
)
@click.pass_context
def delete_jobs_cmd(
    ctx,
    sweep_id: str,
    pattern: str,
    state: str,
    dry_run: bool,
    force: bool,
    all_states: bool,
):
    """Delete specific jobs from a sweep with filtering options."""
    from .monitor import delete_sweep_jobs

    delete_sweep_jobs(
        sweep_id=sweep_id,
        pattern=pattern,
        state=state,
        dry_run=dry_run,
        force=force,
        all_states=all_states,
        console=ctx.obj["console"],
        logger=ctx.obj["logger"],
    )


@cli.command("cleanup")
@click.option(
    "--days",
    "-d",
    type=int,
    default=7,
    help="Delete jobs from sweeps older than N days",
)
@click.option(
    "--states",
    "-s",
    multiple=True,
    help="Only delete jobs in these states (e.g., 'C', 'F')",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be deleted without actually deleting",
)
@click.option("--force", "-f", is_flag=True, help="Force deletion without confirmation")
@click.pass_context
def cleanup_cmd(ctx, days: int, states: tuple, dry_run: bool, force: bool):
    """Clean up old sweep jobs based on age and state."""
    from .monitor import cleanup_old_sweeps

    cleanup_old_sweeps(
        days=days,
        states=list(states) if states else None,
        dry_run=dry_run,
        force=force,
        console=ctx.obj["console"],
        logger=ctx.obj["logger"],
    )


@cli.command()
@click.argument("sweep_id")
@click.option(
    "--output-dir", "-o", type=click.Path(), help="Output directory for results"
)
@click.option(
    "--format",
    type=click.Choice(["csv", "json", "xlsx"]),
    default="csv",
    help="Output format",
)
@click.pass_context
def results(ctx, sweep_id: str, output_dir: str, format: str):
    """Collect and analyze sweep results."""
    from .monitor import collect_results

    output_path = Path(output_dir) if output_dir else None
    collect_results(
        sweep_id, output_path, format, ctx.obj["console"], ctx.obj["logger"]
    )


def main():
    """Main entry point for the CLI."""
    try:
        cli()
    except KeyboardInterrupt:
        console.print("\n[yellow]Operation cancelled by user.[/yellow]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise


if __name__ == "__main__":
    main()
