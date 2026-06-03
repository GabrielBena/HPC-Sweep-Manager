"""Main CLI entry point for HPC Sweep Manager."""

import click
from rich.console import Console

from .. import __version__
from ..core.common.utils import setup_logging
from .docs import docs  # `hsm docs`: where the documentation lives
from .init import init_cmd, setup  # Project setup: init, configure
from .queue import queue  # Cluster queue inspection
from .remote import remote  # Remote management
from .sweep import sweep_cmd  # Sweep: run/status/report/errors/watch/recent/queue/cancel/cleanup

console = Console()


@click.group()
@click.version_option(version=__version__, prog_name="hsm")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option("--quiet", "-q", is_flag=True, help="Suppress non-error output")
@click.pass_context
def cli(ctx: click.Context, verbose: bool, quiet: bool):
    """HPC Sweep Manager - Automated hyperparameter sweeps on HPC systems."""

    # Ensure that ctx.obj exists and is a dict
    ctx.ensure_object(dict)

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
cli.add_command(init_cmd, name="init")  # top-level alias: `hsm init` == `hsm setup init`
cli.add_command(sweep_cmd)  # hsm sweep run/status/report/errors/watch/recent/queue/cancel/cleanup
cli.add_command(remote)  # hsm remote add/list/test/health/gpus/clean/remove
cli.add_command(queue)  # hsm queue mine/position/gpus/reservations
cli.add_command(docs)  # hsm docs — pointers to the documentation


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
