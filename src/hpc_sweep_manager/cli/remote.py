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

# Set up more detailed logging for debugging
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@click.group()
def remote():
    """Manage remote machines for distributed sweeps."""
    pass


@remote.command()
@click.argument("name")
@click.argument("host")
@click.option("--key", help="SSH key path")
@click.option("--port", default=22, help="SSH port")
@click.option("--max-jobs", type=int, help="Max parallel jobs (overrides remote default)")
@click.option("--enabled/--disabled", default=True, help="Enable/disable this remote")
def add(name: str, host: str, key: str, port: int, max_jobs: int, enabled: bool):
    """Add a new remote machine configuration."""
    console = Console()

    # Load existing HSM config
    hsm_config = HSMConfig.load()
    if not hsm_config:
        console.print("[red]No hsm_config.yaml found. Run 'hsm init' first.[/red]")
        return

    # Add remote to configuration
    config_data = hsm_config.config_data

    # Initialize distributed config if it doesn't exist
    if "distributed" not in config_data:
        config_data["distributed"] = {
            "enabled": False,
            "remotes": {},
            "strategy": "round_robin",
            "sync_method": "rsync",
        }

    if "remotes" not in config_data["distributed"]:
        config_data["distributed"]["remotes"] = {}

    # Add the new remote
    remote_config = {"host": host, "ssh_port": port, "enabled": enabled}

    if key:
        remote_config["ssh_key"] = key
    if max_jobs:
        remote_config["max_parallel_jobs"] = max_jobs

    config_data["distributed"]["remotes"][name] = remote_config

    # Save updated configuration
    config_path = Path.cwd() / "sweeps" / "hsm_config.yaml"
    if not config_path.exists():
        config_path = Path.cwd() / "hsm_config.yaml"

    try:
        with open(config_path, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)

        console.print(f"[green]✓ Added remote '{name}' ({host})[/green]")
        console.print(f"Configuration saved to: {config_path}")
        console.print(f"Run 'hsm remote test {name}' to verify the connection.")

    except Exception as e:
        console.print(f"[red]Failed to save configuration: {e}[/red]")


@remote.command()
def list():
    """List all configured remote machines."""
    console = Console()

    hsm_config = HSMConfig.load()
    if not hsm_config:
        console.print("[red]No hsm_config.yaml found. Run 'hsm init' first.[/red]")
        return

    distributed_config = hsm_config.config_data.get("distributed", {})
    remotes = distributed_config.get("remotes", {})

    if not remotes:
        console.print("[yellow]No remote machines configured.[/yellow]")
        console.print("Use 'hsm remote add <name> <host>' to add a remote machine.")
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

        table.add_row(
            name,
            config.get("host", "N/A"),
            str(config.get("ssh_port", 22)),
            config.get("ssh_key", "default") or "default",
            str(config.get("max_parallel_jobs", "auto")),
            status,
        )

    console.print(table)


@remote.command()
@click.argument("names", nargs=-1)
@click.option("--all", is_flag=True, help="Test all remotes")
def test(names: tuple, all: bool):
    """Test connection and configuration discovery for remote machines."""
    console = Console()

    hsm_config = HSMConfig.load()
    if not hsm_config:
        console.print("[red]No hsm_config.yaml found. Run 'hsm init' first.[/red]")
        return

    distributed_config = hsm_config.config_data.get("distributed", {})
    remotes = distributed_config.get("remotes", {})

    if not remotes:
        console.print("[yellow]No remote machines configured.[/yellow]")
        return

    # Determine which remotes to test
    if all:
        test_remotes = remotes
    elif names:
        test_remotes = {name: remotes[name] for name in names if name in remotes}
        missing = [name for name in names if name not in remotes]
        if missing:
            console.print(f"[red]Unknown remotes: {', '.join(missing)}[/red]")
    else:
        console.print("[red]Specify remote names or use --all[/red]")
        return

    if not test_remotes:
        console.print("[yellow]No remotes to test.[/yellow]")
        return

    console.print(f"[bold]Testing {len(test_remotes)} remote machine(s)...[/bold]")

    # Run async test
    async def run_tests():
        discovery = RemoteDiscovery(hsm_config.config_data)

        results = {}
        for name, config in test_remotes.items():
            console.print(f"\n[cyan]Testing {name} ({config['host']})...[/cyan]")

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

    hsm_config = HSMConfig.load()
    if not hsm_config:
        console.print("[red]No hsm_config.yaml found.[/red]")
        return

    distributed_config = hsm_config.config_data.get("distributed", {})
    remotes = distributed_config.get("remotes", {})

    if not remotes:
        console.print("[yellow]No remote machines configured.[/yellow]")
        return

    # Determine which remotes to check
    if all:
        check_remotes = remotes
    elif names:
        check_remotes = {name: remotes[name] for name in names if name in remotes}
    else:
        console.print("[red]Specify remote names or use --all[/red]")
        return

    async def run_health_check():
        # First discover configurations
        discovery = RemoteDiscovery(hsm_config.config_data)
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
        console.print("[red]No hsm_config.yaml found.[/red]")
        return

    config_data = hsm_config.config_data
    remotes = config_data.get("distributed", {}).get("remotes", {})

    if name not in remotes:
        console.print(f"[red]Remote '{name}' not found.[/red]")
        return

    # Remove the remote
    del remotes[name]

    # Save updated configuration
    config_path = Path.cwd() / "sweeps" / "hsm_config.yaml"
    if not config_path.exists():
        config_path = Path.cwd() / "hsm_config.yaml"

    try:
        with open(config_path, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)

        console.print(f"[green]✓ Removed remote '{name}'[/green]")

    except Exception as e:
        console.print(f"[red]Failed to save configuration: {e}[/red]")


if __name__ == "__main__":
    remote()
