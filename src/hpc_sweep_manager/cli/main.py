"""Main CLI entry point for HPC Sweep Manager."""

import click
from rich.console import Console

from ..core.common.utils import setup_logging
from .analyze import analyze  # Code analysis and usage tracking
from .collect import results  # Results: collect
from .distributed import distributed  # Distributed execution
from .hpc import hpc  # HPC execution

# Import consolidated command groups (no more wrappers!)
from .init import setup  # Project setup: init, configure
from .local import local  # Local execution
from .monitor import (
    monitor,
)  # Monitoring: watch, status, recent, queue, cancel, cleanup, delete-jobs, collect-results
from .remote import remote  # Remote management
from .sweep import sweep_cmd  # Direct sweep command

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


# Register all command groups
cli.add_command(setup)  # hsm setup init, hsm setup configure
cli.add_command(sweep_cmd)  # hsm sweep (direct command)
cli.add_command(
    monitor
)  # hsm monitor watch, status, recent, queue, cancel, cleanup, delete-jobs, collect-results
cli.add_command(results)  # hsm results collect
cli.add_command(local)  # hsm local run, status, clean
cli.add_command(hpc)  # hsm hpc submit, queue, status, cancel
cli.add_command(remote)  # hsm remote add, list, test, health, remove
cli.add_command(distributed)  # hsm distributed init, add, list, test, health, remove
cli.add_command(
    analyze
)  # hsm analyze enable-tracking, report, dead-code, complexity, dependencies, coverage-gaps


# Core workflow shortcuts for common commands (as shown in README)
@cli.command("init")
@click.option("--interactive", "-i", is_flag=True, help="Interactive setup mode")
@click.option("--project-root", type=click.Path(exists=True), help="Project root directory")
@click.pass_context
def init_compat(ctx, interactive: bool, project_root: str):
    """Initialize sweep infrastructure."""
    ctx.invoke(setup.commands["init"], interactive=interactive, project_root=project_root)


@cli.command("monitor")
@click.argument("sweep_id", required=False)
@click.option("--watch", "-w", is_flag=True, help="Watch mode")
@click.option("--refresh", "-r", type=int, default=30, help="Refresh interval in seconds")
@click.option("--days", "-d", type=int, default=7, help="Show sweeps from last N days")
@click.pass_context
def monitor_compat(ctx, sweep_id: str, watch: bool, refresh: int, days: int):
    """Monitor sweep progress."""
    if sweep_id:
        # Monitor specific sweep
        ctx.invoke(
            monitor.commands["watch"], sweep_id=sweep_id, watch=watch, refresh=refresh, days=days
        )
    else:
        # Show recent sweeps
        ctx.invoke(monitor.commands["recent"], watch=watch, refresh=refresh, days=days)


@cli.command("collect-results")
@click.argument("sweep_id")
@click.option("--remote", help="Remote machine name to collect from (for remote sweeps)")
@click.pass_context
def collect_compat(ctx, sweep_id: str, remote: str):
    """Collect results from remote machines."""
    ctx.invoke(results.commands["collect"], sweep_id=sweep_id, remote=remote)


# Additional shortcuts for common operations
@cli.command("cancel")
@click.argument("sweep_id")
@click.option("--force", "-f", is_flag=True, help="Force cancellation without confirmation")
@click.pass_context
def cancel_compat(ctx, sweep_id: str, force: bool):
    """Cancel a running sweep."""
    ctx.invoke(monitor.commands["cancel"], sweep_id=sweep_id, force=force)


@cli.command("queue")
@click.option("--watch", "-w", is_flag=True, help="Watch mode")
@click.option("--refresh", "-r", type=int, default=30, help="Refresh interval")
@click.pass_context
def queue_compat(ctx, watch: bool, refresh: int):
    """Show queue status."""
    ctx.invoke(monitor.commands["queue"], watch=watch, refresh=refresh)


@cli.command("recent")
@click.option("--days", "-d", type=int, default=7, help="Show sweeps from last N days")
@click.option("--watch", "-w", is_flag=True, help="Watch mode")
@click.pass_context
def recent_compat(ctx, days: int, watch: bool):
    """Show recent sweeps."""
    ctx.invoke(monitor.commands["recent"], days=days, watch=watch)


@cli.command("cleanup")
@click.option("--days", "-d", type=int, default=30, help="Clean jobs older than N days")
@click.option("--dry-run", is_flag=True, help="Show what would be cleaned")
@click.option("--force", "-f", is_flag=True, help="Force cleanup without confirmation")
@click.pass_context
def cleanup_compat(ctx, days: int, dry_run: bool, force: bool):
    """Clean up old sweep jobs."""
    ctx.invoke(monitor.commands["cleanup"], days=days, dry_run=dry_run, force=force)


# Add backwards compatibility for the original sweep command
@cli.command("run")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    default="sweeps/sweep.yaml",
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
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option("--quiet", "-q", is_flag=True, help="Suppress non-error output")
@click.pass_context
def run_compat(ctx, **kwargs):
    """Run parameter sweep (shortcut for 'hsm sweep run')."""
    ctx.invoke(sweep_cmd.commands["run"], **kwargs)


# Add shortcuts for new completion functionality
@cli.command("complete")
@click.argument("sweep_id")
@click.option(
    "--mode",
    type=click.Choice(["auto", "local", "remote", "distributed"]),
    default="local",
    help="Job submission mode for completion",
)
@click.option("--dry-run", "-d", is_flag=True, help="Show what would be completed without running")
@click.option(
    "--no-retry-failed", is_flag=True, help="Don't retry failed combinations, only missing ones"
)
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
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option("--quiet", "-q", is_flag=True, help="Suppress non-error output")
@click.pass_context
def complete_compat(ctx, **kwargs):
    """Complete a partially finished sweep (shortcut for 'hsm sweep complete')."""
    ctx.invoke(sweep_cmd.commands["complete"], **kwargs)


@cli.command("status")
@click.argument("sweep_id", required=False)
@click.option("--all", "-a", is_flag=True, help="Show status of all sweeps")
@click.option("--incomplete-only", is_flag=True, help="Show only incomplete sweeps")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option("--quiet", "-q", is_flag=True, help="Suppress non-error output")
@click.pass_context
def status_compat(ctx, **kwargs):
    """Show completion status of sweep(s) (shortcut for 'hsm sweep status')."""
    ctx.invoke(sweep_cmd.commands["status"], **kwargs)


@cli.command("errors")
@click.argument("sweep_id")
@click.option("--all", "-a", is_flag=True, help="Show all error details")
@click.option("--pattern", help="Filter errors by pattern (e.g., 'ImportError')")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option("--quiet", "-q", is_flag=True, help="Suppress non-error output")
@click.pass_context
def errors_compat(ctx, **kwargs):
    """Show error summaries for a specific sweep (shortcut for 'hsm sweep errors')."""
    ctx.invoke(sweep_cmd.commands["errors"], **kwargs)


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
