"""Result collection CLI commands."""

import asyncio
import logging
from pathlib import Path
from typing import Optional

import click
from rich.console import Console

from ..core.common.config import HSMConfig
from ..core.remote.discovery import RemoteDiscovery
from ..core.remote.remote_manager import RemoteJobManager
from .common import common_options


def collect_results(
    sweep_id: str,
    remote: Optional[str],
    console: Console,
    logger: logging.Logger,
):
    """Collect results from remote machines."""

    console.print(f"[bold blue]Collecting Results for Sweep: {sweep_id}[/bold blue]")

    # Load HSM config
    hsm_config = HSMConfig.load()
    if not hsm_config:
        console.print("[red]Error: hsm_config.yaml required for result collection[/red]")
        return

    # Find sweep directory
    sweep_dir = Path("sweeps") / "outputs" / sweep_id
    if not sweep_dir.exists():
        console.print(f"[red]Error: Sweep directory not found: {sweep_dir}[/red]")
        return

    console.print(f"Found sweep directory: {sweep_dir}")

    if remote:
        # Collect from specific remote
        console.print(f"[cyan]Collecting results from remote: {remote}[/cyan]")
        success = asyncio.run(collect_from_remote(remote, sweep_dir, hsm_config, console, logger))

        if success:
            console.print(f"[green]✓ Results collected successfully from {remote}[/green]")
        else:
            console.print(f"[red]✗ Failed to collect results from {remote}[/red]")
    else:
        # Auto-detect remote sweeps and collect from all
        console.print("[cyan]Auto-detecting remote sweeps and collecting results...[/cyan]")

        # Look for remote-related directories or files
        remotes = hsm_config.config_data.get("distributed", {}).get("remotes", {})

        if not remotes:
            console.print("[yellow]No remote machines configured for collection[/yellow]")
            return

        collected_any = False
        for remote_name in remotes:
            console.print(f"[dim]Attempting collection from {remote_name}...[/dim]")
            success = asyncio.run(
                collect_from_remote(remote_name, sweep_dir, hsm_config, console, logger)
            )
            if success:
                collected_any = True
                console.print(f"[green]✓ Collected from {remote_name}[/green]")
            else:
                console.print(f"[dim]⊘ No results to collect from {remote_name}[/dim]")

        if collected_any:
            console.print("[green]✓ Result collection completed[/green]")
        else:
            console.print("[yellow]No results collected from any remote machines[/yellow]")


async def collect_from_remote(
    remote_name: str,
    sweep_dir: Path,
    hsm_config: HSMConfig,
    console: Console,
    logger: logging.Logger,
) -> bool:
    """Collect results from a specific remote machine."""

    try:
        # Get remote configuration
        distributed_config = hsm_config.config_data.get("distributed", {})
        remotes = distributed_config.get("remotes", {})

        if remote_name not in remotes:
            logger.warning(f"Remote '{remote_name}' not found in configuration")
            return False

        # Discover remote configuration
        discovery = RemoteDiscovery(hsm_config.config_data)
        remote_info = remotes[remote_name].copy()
        remote_info["name"] = remote_name

        remote_config = await discovery.discover_remote_config(remote_info)

        if not remote_config:
            logger.warning(f"Failed to discover configuration for remote '{remote_name}'")
            return False

        # Create RemoteJobManager for collection
        remote_job_manager = RemoteJobManager(remote_config, sweep_dir)

        # Collect results
        success = await remote_job_manager.collect_results()

        if success:
            logger.info(f"Successfully collected results from {remote_name}")

        return success

    except Exception as e:
        logger.error(f"Error collecting from remote {remote_name}: {e}")
        return False


# CLI command group structure
@click.group()
def results():
    """Collect and analyze sweep results."""
    pass


@results.command("collect")
@click.argument("sweep_id")
@click.option(
    "--remote",
    help="Remote machine name to collect from (for remote sweeps)",
)
@common_options
@click.pass_context
def collect_cmd(ctx, sweep_id: str, remote: str, verbose: bool, quiet: bool):
    """Collect results from remote machines."""
    collect_results(
        sweep_id=sweep_id,
        remote=remote,
        console=ctx.obj["console"],
        logger=ctx.obj["logger"],
    )


if __name__ == "__main__":
    results()
