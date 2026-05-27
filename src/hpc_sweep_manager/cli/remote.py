"""CLI commands for remote machine management."""

import asyncio
import datetime
import logging
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich.tree import Tree
import yaml

from ..core.common.config import HSMConfig
from ..core.remote.discovery import RemoteDiscovery, RemoteValidator
from ..core.remote.gpu_probe import probe_gpus

# Set up more detailed logging for debugging
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@click.group()
def remote():
    """Manage remote machines for distributed sweeps."""
    pass


def _config_write_path() -> Path:
    """Pick where to persist remote config edits.

    Prefers an existing config file in the standard search order; otherwise
    bootstraps a new ``.hsm/config.yaml`` (the primary location).
    """
    candidates = [
        Path.cwd() / ".hsm" / "config.yaml",
        Path.cwd() / "sweeps" / "hsm_config.yaml",
        Path.cwd() / "hsm_config.yaml",
    ]
    for path in candidates:
        if path.exists():
            return path
    return candidates[0]  # bootstrap a fresh .hsm/config.yaml


def _resolve_remotes_for_action(names: tuple, all_flag: bool, console: Console):
    """Resolve which remotes to act on, supporting bare ~/.ssh/config aliases.

    Registered remotes come from hsm_config's ``distributed.remotes``; a name
    that isn't registered is treated as a bare ssh-config alias (empty config →
    host defaults to the alias name). ``--all`` only spans registered remotes.

    Returns ``(remotes_dict, config_data)`` or ``(None, None)`` on error/empty.
    """
    hsm_config = HSMConfig.load()
    config_data = hsm_config.config_data if hsm_config else {}
    registered = config_data.get("distributed", {}).get("remotes", {})

    if all_flag:
        if not registered:
            console.print(
                "[yellow]No remotes registered. Add one with 'hsm remote add', "
                "or name a ~/.ssh/config alias directly.[/yellow]"
            )
            return None, None
        return {name: dict(cfg) for name, cfg in registered.items()}, config_data

    if not names:
        console.print("[red]Specify remote name(s) or use --all.[/red]")
        return None, None

    selected: dict = {}
    for name in names:
        if name in registered:
            selected[name] = dict(registered[name])
        else:
            console.print(
                f"[dim]{name}: not in hsm_config — treating as a ~/.ssh/config alias[/dim]"
            )
            selected[name] = {}
    return selected, config_data


@remote.command()
@click.argument("name")
@click.argument("host", required=False)
@click.option("--key", help="SSH key path (overrides ~/.ssh/config)")
@click.option("--port", type=int, default=None, help="SSH port (overrides ~/.ssh/config)")
@click.option("--max-jobs", type=int, help="Max parallel jobs (overrides remote default)")
@click.option("--enabled/--disabled", default=True, help="Enable/disable this remote")
def add(name: str, host: str, key: str, port: int, max_jobs: int, enabled: bool):
    """Add a new remote machine configuration.

    HOST is optional: if omitted, NAME is treated as a ~/.ssh/config alias and
    all connection details (hostname, user, port, key, proxy) come from there.
    Provide HOST / --key / --port only to override the ssh-config entry.
    """
    console = Console()

    # Load existing config, or bootstrap a fresh one — adding a remote is
    # exactly the moment to create the file, so don't demand 'hsm init' first.
    hsm_config = HSMConfig.load()
    config_data = hsm_config.config_data if hsm_config else {}

    distributed = config_data.setdefault(
        "distributed",
        {"enabled": False, "strategy": "round_robin", "sync_method": "rsync"},
    )
    distributed.setdefault("remotes", {})

    # Only persist connection fields that were explicitly given — a bare entry
    # resolves entirely from ~/.ssh/config via the alias.
    remote_config = {"enabled": enabled}
    if host:
        remote_config["host"] = host
    if port:
        remote_config["ssh_port"] = port
    if key:
        remote_config["ssh_key"] = key
    if max_jobs:
        remote_config["max_parallel_jobs"] = max_jobs

    distributed["remotes"][name] = remote_config

    config_path = _config_write_path()
    try:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)

        where = host or f"{name} (via ~/.ssh/config)"
        console.print(f"[green]✓ Added remote '{name}' ({where})[/green]")
        console.print(f"Configuration saved to: {config_path}")
        console.print(f"Run 'hsm remote test {name}' to verify the connection.")

    except Exception as e:
        console.print(f"[red]Failed to save configuration: {e}[/red]")


@remote.command()
def list():
    """List all configured remote machines."""
    console = Console()

    hsm_config = HSMConfig.load()
    remotes = (
        hsm_config.config_data.get("distributed", {}).get("remotes", {})
        if hsm_config
        else {}
    )

    if not remotes:
        console.print("[yellow]No remotes registered yet.[/yellow]")
        console.print(
            "Register one with 'hsm remote add <name>' (uses your ~/.ssh/config alias), "
            "or ping an alias directly with 'hsm remote test <alias>'."
        )
        return

    table = Table(title="Configured Remote Machines")
    table.add_column("Name", style="cyan")
    table.add_column("Host", style="green")
    table.add_column("Port", style="magenta")
    table.add_column("SSH Key", style="blue")
    table.add_column("Max Jobs", style="yellow")
    table.add_column("Status", style="red")

    for name, config in remotes.items():
        status = "✓ Enabled" if config.get("enabled", True) else "✗ Disabled"

        # A bare entry resolves from ~/.ssh/config; show the alias + that hint.
        table.add_row(
            name,
            config.get("host", f"{name} (ssh config)"),
            str(config.get("ssh_port", "ssh config")),
            config.get("ssh_key", "ssh config") or "ssh config",
            str(config.get("max_parallel_jobs", "auto")),
            status,
        )

    console.print(table)


@remote.command()
@click.argument("names", nargs=-1)
@click.option("--all", is_flag=True, help="Probe all registered remotes")
def gpus(names: tuple, all: bool):
    """Show GPU availability on remote machines (via nvidia-smi).

    NAMES may be registered remotes or bare ~/.ssh/config aliases. Needs
    nothing on the remote except nvidia-smi — handy for deciding where to send
    a sweep. Example: ``hsm remote gpus anahita`` or ``hsm remote gpus --all``.
    """
    console = Console()

    targets, _ = _resolve_remotes_for_action(names, all, console)
    if not targets:
        return

    async def probe_all():
        results = {}
        for name, config in targets.items():
            host = config.get("host", name)
            try:
                results[name] = await probe_gpus(
                    host, config.get("ssh_key"), config.get("ssh_port")
                )
            except Exception as e:  # noqa: BLE001 - report unreachable per host
                results[name] = e
        return results

    try:
        results = asyncio.run(probe_all())
    except Exception as e:
        console.print(f"[red]Error probing GPUs: {e}[/red]")
        return

    table = Table(title="Remote GPU Availability")
    table.add_column("Machine", style="cyan")
    table.add_column("GPU", style="magenta", justify="right")
    table.add_column("Name", style="green")
    table.add_column("Memory", style="blue")
    table.add_column("Util", style="yellow", justify="right")
    table.add_column("State")

    for name, result in results.items():
        if isinstance(result, Exception):
            table.add_row(name, "-", f"[red]unreachable: {result}[/red]", "-", "-", "")
            continue
        if not result:
            table.add_row(name, "-", "[dim]no GPUs / nvidia-smi not found[/dim]", "-", "-", "")
            continue
        for i, gpu in enumerate(result):
            state = "[green]○ free[/green]" if gpu.is_free else "[red]● busy[/red]"
            table.add_row(
                name if i == 0 else "",
                str(gpu.index),
                gpu.name,
                f"{gpu.mem_used_gb:.1f}/{gpu.mem_total_gb:.0f} GB",
                f"{gpu.util_pct:.0f}%",
                state,
            )

    console.print(table)

    # Quick summary of free capacity to aid targeting decisions.
    free_by_host = {
        name: sum(1 for g in res if g.is_free)
        for name, res in results.items()
        if not isinstance(res, Exception) and res
    }
    if free_by_host:
        summary = ", ".join(f"{name}: {n} free" for name, n in free_by_host.items())
        console.print(f"\n[bold]Free GPUs[/bold] — {summary}")


@remote.command()
@click.argument("names", nargs=-1)
@click.option("--all", is_flag=True, help="Test all remotes")
def test(names: tuple, all: bool):
    """Test connection and configuration discovery for remote machines."""
    console = Console()

    test_remotes, config_data = _resolve_remotes_for_action(names, all, console)
    if not test_remotes:
        return

    console.print(f"[bold]Testing {len(test_remotes)} remote machine(s)...[/bold]")

    # Run async test
    async def run_tests():
        discovery = RemoteDiscovery(config_data)

        results = {}
        for name, config in test_remotes.items():
            console.print(f"\n[cyan]Testing {name} ({config.get('host', name)})...[/cyan]")

            config["name"] = name
            remote_config = await discovery.discover_remote_config(config)

            if remote_config:
                results[name] = {"status": "success", "config": remote_config}
                console.print(f"[green]✓ {name}: Configuration discovered successfully[/green]")
            else:
                results[name] = {"status": "failed", "config": None}
                console.print(f"[red]✗ {name}: Failed to discover configuration[/red]")

        return results

    # Execute async tests
    try:
        results = asyncio.run(run_tests())

        # Display detailed results
        console.print("\n[bold]Test Results Summary:[/bold]")

        for name, result in results.items():
            if result["status"] == "success":
                config = result["config"]

                # Create a tree for the configuration
                tree = Tree(f"[green]✓ {name}[/green]")
                tree.add(f"Host: {config.host}")
                tree.add(f"Python: {config.python_interpreter}")
                tree.add(f"Project Root: {config.project_root}")
                tree.add(f"Train Script: {config.train_script}")
                tree.add(f"Max Jobs: {config.max_parallel_jobs}")

                if config.wandb_config:
                    wandb_branch = tree.add("W&B Config")
                    wandb_branch.add(f"Project: {config.wandb_config.get('project', 'N/A')}")
                    wandb_branch.add(f"Entity: {config.wandb_config.get('entity', 'N/A')}")

                console.print(tree)
            else:
                console.print(f"[red]✗ {name}: Test failed[/red]")

        # Summary
        success_count = sum(1 for r in results.values() if r["status"] == "success")
        total_count = len(results)

        if success_count == total_count:
            console.print(
                f"\n[green]All {total_count} remote(s) configured successfully! 🎉[/green]"
            )
        else:
            console.print(
                f"\n[yellow]{success_count}/{total_count} remote(s) configured successfully[/yellow]"
            )

    except Exception as e:
        console.print(f"[red]Error during testing: {e}[/red]")


@remote.command()
@click.argument("names", nargs=-1)
@click.option("--all", is_flag=True, help="Check health of all remotes")
@click.option("--watch", is_flag=True, help="Continuous monitoring mode")
@click.option("--refresh", default=30, help="Refresh interval in seconds for watch mode")
def health(names: tuple, all: bool, watch: bool, refresh: int):
    """Check health status of remote machines."""
    console = Console()

    check_remotes, config_data = _resolve_remotes_for_action(names, all, console)
    if not check_remotes:
        return

    async def run_health_check():
        # First discover configurations
        discovery = RemoteDiscovery(config_data)
        discovered = {}

        for name, config in check_remotes.items():
            config["name"] = name
            remote_config = await discovery.discover_remote_config(config)
            if remote_config:
                discovered[name] = remote_config

        if not discovered:
            console.print("[red]No healthy remotes found for health check.[/red]")
            return

        # Run health checks
        validator = RemoteValidator(discovered)
        health_report = await validator.health_check_all()

        return health_report

    def display_health_report(health_report):
        table = Table(title="Remote Machine Health Status")
        table.add_column("Machine", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Connection", style="blue")
        table.add_column("Load", style="yellow")
        table.add_column("Python", style="magenta")
        table.add_column("Timestamp", style="dim")

        for name, health in health_report.items():
            status_style = "green" if health.get("status") == "healthy" else "red"
            status = f"[{status_style}]{health.get('status', 'unknown')}[/{status_style}]"

            table.add_row(
                name,
                status,
                health.get("connection", "?"),
                health.get("load", "N/A")[:30] + "..."
                if health.get("load") and len(health.get("load", "")) > 30
                else health.get("load", "N/A"),
                health.get("python_version", "N/A"),
                health.get("timestamp", "N/A"),
            )

        console.print(table)

        # Show any errors
        for name, health in health_report.items():
            if "error" in health:
                console.print(f"[red]✗ {name}: {health['error']}[/red]")

    if watch:
        console.print(
            f"[bold]Monitoring remote health every {refresh} seconds (Ctrl+C to stop)...[/bold]"
        )

        try:
            while True:
                console.clear()
                console.print(
                    f"[bold]Remote Health Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/bold]\n"
                )

                try:
                    health_report = asyncio.run(run_health_check())
                    if health_report:
                        display_health_report(health_report)
                    else:
                        console.print("[red]Failed to get health report[/red]")
                except Exception as e:
                    console.print(f"[red]Error during health check: {e}[/red]")

                import time

                time.sleep(refresh)

        except KeyboardInterrupt:
            console.print("\n[yellow]Health monitoring stopped.[/yellow]")
    else:
        # Single health check
        try:
            health_report = asyncio.run(run_health_check())
            if health_report:
                display_health_report(health_report)
            else:
                console.print("[red]Failed to get health report[/red]")
        except Exception as e:
            console.print(f"[red]Error during health check: {e}[/red]")


@remote.command()
@click.argument("name")
@click.confirmation_option(prompt="Are you sure you want to remove this remote?")
def remove(name: str):
    """Remove a remote machine configuration."""
    console = Console()

    hsm_config = HSMConfig.load()
    if not hsm_config:
        console.print("[yellow]No remotes registered (no hsm_config found).[/yellow]")
        return

    config_data = hsm_config.config_data
    remotes = config_data.get("distributed", {}).get("remotes", {})

    if name not in remotes:
        console.print(f"[red]Remote '{name}' not found in hsm_config.[/red]")
        return

    # Remove the remote
    del remotes[name]

    # Save updated configuration
    config_path = _config_write_path()

    try:
        with open(config_path, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)

        console.print(f"[green]✓ Removed remote '{name}'[/green]")

    except Exception as e:
        console.print(f"[red]Failed to save configuration: {e}[/red]")


if __name__ == "__main__":
    remote()
