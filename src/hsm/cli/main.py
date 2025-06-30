"""Main CLI entry point for HPC Sweep Manager v2."""

import click
from rich.console import Console

from ..utils.logging import setup_logging
from .config import config_cmd
from .monitor import monitor
from .sweep import sweep_cmd
from .utils import validate_environment

console = Console()


@click.group()
@click.version_option(version="2.0.0", prog_name="hsm")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option("--quiet", "-q", is_flag=True, help="Suppress non-error output")
@click.pass_context
def cli(ctx: click.Context, verbose: bool, quiet: bool):
    """HPC Sweep Manager v2 - Unified sweep execution across compute sources."""

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

    # Validate environment on startup
    validation_result = validate_environment()
    if not validation_result.is_valid:
        console.print("[yellow]Warning: Environment validation issues detected[/yellow]")
        for warning in validation_result.warnings:
            console.print(f"  [yellow]• {warning}[/yellow]")


# Register command groups
cli.add_command(sweep_cmd)  # hsm sweep run/complete/status/cancel
cli.add_command(monitor)  # hsm monitor watch/status/recent
cli.add_command(config_cmd)  # hsm config init/validate/show


# Shortcuts for common commands
@cli.command("run")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    default="sweeps/sweep.yaml",
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
@click.pass_context
def run_shortcut(ctx, **kwargs):
    """Run a parameter sweep (shortcut for 'hsm sweep run')."""
    ctx.invoke(sweep_cmd.commands["run"], **kwargs)


@cli.command("complete")
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
def complete_shortcut(ctx, **kwargs):
    """Complete a partial sweep (shortcut for 'hsm sweep complete')."""
    ctx.invoke(sweep_cmd.commands["complete"], **kwargs)


@cli.command("status")
@click.argument("sweep_id", required=False)
@click.option("--all", "-a", is_flag=True, help="Show status of all sweeps")
@click.option("--watch", "-w", is_flag=True, help="Watch mode with live updates")
@click.pass_context
def status_shortcut(ctx, **kwargs):
    """Show sweep status (shortcut for 'hsm sweep status')."""
    if kwargs.get("watch"):
        # Remove 'watch' parameter and pass rest to monitor watch command
        watch_kwargs = {k: v for k, v in kwargs.items() if k != "watch"}
        ctx.invoke(monitor.commands["watch"], **watch_kwargs)
    else:
        # Remove 'watch' parameter and pass rest to sweep status command
        status_kwargs = {k: v for k, v in kwargs.items() if k != "watch"}
        ctx.invoke(sweep_cmd.commands["status"], **status_kwargs)


@cli.command("cancel")
@click.argument("sweep_id")
@click.option("--force", "-f", is_flag=True, help="Force cancellation without confirmation")
@click.pass_context
def cancel_shortcut(ctx, **kwargs):
    """Cancel a running sweep (shortcut for 'hsm sweep cancel')."""
    ctx.invoke(sweep_cmd.commands["cancel"], **kwargs)


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
