"""Result collection CLI commands."""

from pathlib import Path
from rich.console import Console
import logging
from typing import Optional
import asyncio

from ..core.hsm_config import HSMConfig
from ..core.remote_discovery import RemoteDiscovery
from ..core.remote_job_manager import RemoteJobManager


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
        console.print(
            "[red]Error: hsm_config.yaml required for result collection[/red]"
        )
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
        success = asyncio.run(
            collect_from_remote(remote, sweep_dir, hsm_config, console, logger)
        )

        if success:
            console.print(
                f"[green]✓ Results collected successfully from {remote}[/green]"
            )
        else:
            console.print(f"[red]✗ Failed to collect results from {remote}[/red]")
    else:
        # Auto-detect remote sweeps and collect from all
        console.print(
            "[cyan]Auto-detecting remote sweeps and collecting results...[/cyan]"
        )

        # Look for remote-related directories or files
        remotes = hsm_config.config_data.get("distributed", {}).get("remotes", {})

        if not remotes:
            console.print(
                "[yellow]No remote machines configured for collection[/yellow]"
            )
            return

        collected_any = False
        for remote_name in remotes.keys():
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
            console.print(f"[green]✓ Result collection completed[/green]")
        else:
            console.print(
                f"[yellow]No results collected from any remote machines[/yellow]"
            )


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
            logger.warning(
                f"Failed to discover configuration for remote '{remote_name}'"
            )
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
