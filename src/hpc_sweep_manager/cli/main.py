"""Main CLI entry point for HPC Sweep Manager."""

import click
from rich.console import Console

from ..core.common.utils import setup_logging
from .analyze import analyze  # Code analysis and usage tracking
from .hpc import hpc  # HPC execution

# Import consolidated command groups (no more wrappers!)
from .init import setup  # Project setup: init, configure
from .local import local  # Local execution
from .monitor import (
    monitor,
)  # Monitoring: watch, status, recent, queue, cancel, cleanup, delete-jobs, collect-results
from .remote import remote  # Remote management
from .sweep import sweep_cmd  # Direct sweep command
from .sync_commands import sync  # Sync: init, list, run, to

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
cli.add_command(sync)  # hsm sync init, list, run, to
cli.add_command(
    monitor
)  # hsm monitor watch, status, recent, queue, cancel, cleanup, delete-jobs, collect-results
cli.add_command(local)  # hsm local run, status, clean
cli.add_command(hpc)  # hsm hpc submit, queue, status, cancel
cli.add_command(remote)  # hsm remote add, list, test, health, remove
cli.add_command(
    analyze
)  # hsm analyze enable-tracking, report, dead-code, complexity, dependencies, coverage-gaps


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
