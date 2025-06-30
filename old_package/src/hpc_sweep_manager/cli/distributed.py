"""CLI commands for distributed computing management."""

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
from ..core.common.path_detector import PathDetector
from ..core.local.local_compute_source import LocalComputeSource
from ..core.remote.discovery import RemoteDiscovery
from ..core.remote.ssh_compute_source import SSHComputeSource

logger = logging.getLogger(__name__)


@click.group()
def distributed():
    """Manage distributed computing across multiple sources."""
    pass


@distributed.command()
@click.option("--interactive", "-i", is_flag=True, help="Interactive setup mode")
def init(interactive: bool):
    """Initialize distributed computing configuration."""
    console = Console()

    hsm_config = HSMConfig.load()
    if not hsm_config:
        console.print("[red]No hsm_config.yaml found. Run 'hsm init' first.[/red]")
        return

    config_data = hsm_config.config_data

    # Initialize distributed config if it doesn't exist
    if "distributed" not in config_data:
        console.print("[cyan]Initializing distributed computing configuration...[/cyan]")

        config_data["distributed"] = {
            "enabled": True,
            "strategy": "round_robin",
            "collect_interval": 300,
            "health_check_interval": 60,
            "max_retries": 3,
            "remotes": {},
            "sync": {
                "exclude_patterns": [
                    "*.git*",
                    "__pycache__",
                    "*.pyc",
                    "wandb/run-*",
                    "outputs/sweep_*",
                    "checkpoints/*.pkl",
                ],
                "compression": True,
                "bandwidth_limit": None,
            },
        }

        # Save updated configuration
        config_path = Path.cwd() / "sweeps" / "hsm_config.yaml"
        if not config_path.exists():
            config_path = Path.cwd() / "hsm_config.yaml"

        with open(config_path, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)

        console.print(f"[green]✓ Distributed computing initialized in {config_path}[/green]")
        console.print("Use 'hsm distributed add' to add remote compute sources.")
    else:
        console.print("[yellow]Distributed computing already initialized.[/yellow]")


@distributed.command()
@click.argument("name")
@click.argument("host")
@click.option("--key", help="SSH key path")
@click.option("--port", default=22, help="SSH port")
@click.option("--max-jobs", type=int, help="Max parallel jobs")
@click.option("--enabled/--disabled", default=True, help="Enable/disable this source")
def add(name: str, host: str, key: str, port: int, max_jobs: int, enabled: bool):
    """Add a compute source for distributed execution."""
    console = Console()

    hsm_config = HSMConfig.load()
    if not hsm_config:
        console.print("[red]No hsm_config.yaml found. Run 'hsm init' first.[/red]")
        return

    config_data = hsm_config.config_data

    # Ensure distributed config exists
    if "distributed" not in config_data:
        console.print(
            "[red]Distributed computing not initialized. Run 'hsm distributed init' first.[/red]"
        )
        return

    if "remotes" not in config_data["distributed"]:
        config_data["distributed"]["remotes"] = {}

    # Add the compute source
    source_config = {"host": host, "ssh_port": port, "enabled": enabled}

    if key:
        source_config["ssh_key"] = key
    if max_jobs:
        source_config["max_parallel_jobs"] = max_jobs

    config_data["distributed"]["remotes"][name] = source_config

    # Save updated configuration
    config_path = Path.cwd() / "sweeps" / "hsm_config.yaml"
    if not config_path.exists():
        config_path = Path.cwd() / "hsm_config.yaml"

    try:
        with open(config_path, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)

        console.print(f"[green]✓ Added compute source '{name}' ({host})[/green]")
        console.print(f"Configuration saved to: {config_path}")
        console.print(f"Run 'hsm distributed test {name}' to verify the connection.")

    except Exception as e:
        console.print(f"[red]Failed to save configuration: {e}[/red]")


@distributed.command()
def list():
    """List all configured compute sources."""
    console = Console()

    hsm_config = HSMConfig.load()
    if not hsm_config:
        console.print("[red]No hsm_config.yaml found.[/red]")
        return

    distributed_config = hsm_config.config_data.get("distributed", {})

    if not distributed_config.get("enabled", False):
        console.print("[yellow]Distributed computing is not enabled.[/yellow]")
        return

    remotes = distributed_config.get("remotes", {})

    table = Table(title="Configured Compute Sources")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="blue")
    table.add_column("Host", style="green")
    table.add_column("Port", style="magenta")
    table.add_column("Max Jobs", style="yellow")
    table.add_column("Status", style="red")

    # Add local compute source (always available)
    table.add_row(
        "local",
        "Local",
        "localhost",
        "-",
        str(distributed_config.get("local_max_jobs", 1)),
        "✓ Available",
    )

    # Add remote sources
    for name, config in remotes.items():
        status = "✓ Enabled" if config.get("enabled", True) else "✗ Disabled"

        table.add_row(
            name,
            "SSH Remote",
            config.get("host", "N/A"),
            str(config.get("ssh_port", 22)),
            str(config.get("max_parallel_jobs", "auto")),
            status,
        )

    console.print(table)

    # Show strategy and settings
    strategy = distributed_config.get("strategy", "round_robin")
    collect_interval = distributed_config.get("collect_interval", 300)

    console.print("\n[bold]Distribution Settings:[/bold]")
    console.print(f"Strategy: {strategy}")
    console.print(f"Result collection interval: {collect_interval}s")


@distributed.command()
@click.argument("names", nargs=-1)
@click.option("--all", is_flag=True, help="Test all compute sources")
def test(names: tuple, all: bool):
    """Test connections and discover configurations for compute sources."""
    console = Console()

    hsm_config = HSMConfig.load()
    if not hsm_config:
        console.print("[red]No hsm_config.yaml found.[/red]")
        return

    distributed_config = hsm_config.config_data.get("distributed", {})
    remotes = distributed_config.get("remotes", {})

    # Determine which sources to test
    if all:
        test_sources = ["local"] + list(remotes.keys())
    elif names:
        test_sources = list(names)
        # Validate source names
        invalid = [name for name in names if name != "local" and name not in remotes]
        if invalid:
            console.print(f"[red]Unknown compute sources: {', '.join(invalid)}[/red]")
            return
    else:
        console.print("[red]Specify source names or use --all[/red]")
        return

    console.print(f"[bold]Testing {len(test_sources)} compute source(s)...[/bold]")

    async def run_tests():
        discovery = RemoteDiscovery(hsm_config.config_data)

        results = {}

        for source_name in test_sources:
            console.print(f"\n[cyan]Testing {source_name}...[/cyan]")

            if source_name == "local":
                # Test local compute source
                try:
                    detector = PathDetector()
                    python_path = (
                        hsm_config.get_default_python_path() or detector.detect_python_path()
                    )
                    script_path = (
                        hsm_config.get_default_script_path() or detector.detect_train_script()
                    )

                    local_source = LocalComputeSource(
                        name="local",
                        max_parallel_jobs=1,
                        python_path=python_path,
                        script_path=script_path,
                        project_dir=str(Path.cwd()),
                    )

                    # Simulate setup test
                    test_dir = Path.cwd() / "test_setup"
                    test_dir.mkdir(exist_ok=True)
                    setup_success = await local_source.setup(test_dir, "test_sweep")
                    test_dir.rmdir()

                    if setup_success:
                        results[source_name] = {"status": "success", "type": "local"}
                        console.print(
                            f"[green]✓ {source_name}: Local source configured successfully[/green]"
                        )
                    else:
                        results[source_name] = {"status": "failed", "type": "local"}
                        console.print(f"[red]✗ {source_name}: Local source setup failed[/red]")

                except Exception as e:
                    results[source_name] = {
                        "status": "failed",
                        "type": "local",
                        "error": str(e),
                    }
                    console.print(f"[red]✗ {source_name}: {e}[/red]")
            else:
                # Test SSH remote source
                if source_name not in remotes:
                    console.print(f"[red]✗ {source_name}: Not found in configuration[/red]")
                    continue

                remote_info = remotes[source_name].copy()
                remote_info["name"] = source_name

                remote_config = await discovery.discover_remote_config(remote_info)

                if remote_config:
                    results[source_name] = {
                        "status": "success",
                        "config": remote_config,
                    }
                    console.print(
                        f"[green]✓ {source_name}: SSH source configured successfully[/green]"
                    )
                else:
                    results[source_name] = {"status": "failed", "config": None}
                    console.print(f"[red]✗ {source_name}: SSH source setup failed[/red]")

        return results

    # Execute async tests
    try:
        results = asyncio.run(run_tests())

        # Display detailed results
        console.print("\n[bold]Test Results Summary:[/bold]")

        success_count = 0
        for name, result in results.items():
            if result["status"] == "success":
                success_count += 1

                if result.get("type") == "local":
                    tree = Tree(f"[green]✓ {name} (Local)[/green]")
                    tree.add("Type: Local compute source")
                    tree.add("Ready for distributed execution")
                else:
                    config = result["config"]
                    tree = Tree(f"[green]✓ {name} (SSH Remote)[/green]")
                    tree.add(f"Host: {config.host}")
                    tree.add(f"Python: {config.python_interpreter}")
                    tree.add(f"Project Root: {config.project_root}")
                    tree.add(f"Train Script: {config.train_script}")
                    tree.add(f"Max Jobs: {config.max_parallel_jobs}")

                console.print(tree)
            else:
                console.print(f"[red]✗ {name}: Test failed[/red]")
                if "error" in result:
                    console.print(f"   Error: {result['error']}")

        # Summary
        total_count = len(results)
        if success_count == total_count:
            console.print(
                f"\n[green]All {total_count} source(s) configured successfully! 🎉[/green]"
            )
        else:
            console.print(
                f"\n[yellow]{success_count}/{total_count} source(s) configured successfully[/yellow]"
            )

    except Exception as e:
        console.print(f"[red]Error during testing: {e}[/red]")


@distributed.command()
@click.argument("names", nargs=-1)
@click.option("--all", is_flag=True, help="Check health of all sources")
@click.option("--watch", is_flag=True, help="Continuous monitoring mode")
@click.option("--refresh", default=30, help="Refresh interval in seconds for watch mode")
def health(names: tuple, all: bool, watch: bool, refresh: int):
    """Check health status of compute sources."""
    console = Console()

    hsm_config = HSMConfig.load()
    if not hsm_config:
        console.print("[red]No hsm_config.yaml found.[/red]")
        return

    distributed_config = hsm_config.config_data.get("distributed", {})
    remotes = distributed_config.get("remotes", {})

    # Determine which sources to check
    if all:
        check_sources = ["local"] + list(remotes.keys())
    elif names:
        check_sources = list(names)
    else:
        console.print("[red]Specify source names or use --all[/red]")
        return

    async def run_health_check():
        # Create compute sources for health checking
        sources = []

        for source_name in check_sources:
            if source_name == "local":
                detector = PathDetector()
                python_path = hsm_config.get_default_python_path() or detector.detect_python_path()
                script_path = hsm_config.get_default_script_path() or detector.detect_train_script()

                local_source = LocalComputeSource(
                    name="local",
                    max_parallel_jobs=4,
                    python_path=python_path,
                    script_path=script_path,
                    project_dir=str(Path.cwd()),
                )
                sources.append(local_source)
            else:
                if source_name in remotes:
                    # Discover and create SSH source
                    discovery = RemoteDiscovery(hsm_config.config_data)
                    remote_info = remotes[source_name].copy()
                    remote_info["name"] = source_name

                    remote_config = await discovery.discover_remote_config(remote_info)
                    if remote_config:
                        ssh_source = SSHComputeSource(source_name, remote_config)
                        sources.append(ssh_source)

        # Run health checks
        health_report = {}
        for source in sources:
            try:
                health_info = await source.health_check()
                health_report[source.name] = health_info
            except Exception as e:
                health_report[source.name] = {
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.datetime.now().isoformat(),
                }

        return health_report

    def display_health_report(health_report):
        table = Table(title="Compute Source Health Status")
        table.add_column("Source", style="cyan")
        table.add_column("Type", style="blue")
        table.add_column("Status", style="green")
        table.add_column("Disk Space", style="yellow")
        table.add_column("System", style="magenta")
        table.add_column("Failures", style="red")
        table.add_column("Details", style="dim")

        for name, health in health_report.items():
            source_type = "Local" if name == "local" else "SSH"

            # Status with appropriate styling
            status = health.get("status", "unknown")
            if status == "healthy":
                status_display = f"[green]✓ {status}[/green]"
            elif status == "degraded":
                status_display = f"[yellow]⚠ {status}[/yellow]"
            elif status == "unhealthy":
                status_display = f"[red]✗ {status}[/red]"
            else:
                status_display = f"[dim]{status}[/dim]"

            # Disk space information
            disk_info = "N/A"
            if "disk_status" in health:
                disk_status = health["disk_status"]
                disk_message = health.get("disk_message", "")

                if disk_status == "critical":
                    disk_info = f"[red]⚠ {disk_message}[/red]"
                elif disk_status == "warning":
                    disk_info = f"[yellow]⚠ {disk_message}[/yellow]"
                elif disk_status == "healthy":
                    disk_info = f"[green]✓ {disk_message}[/green]"
                else:
                    disk_info = f"[dim]{disk_message}[/dim]"

            # System information
            system_info = "N/A"
            if "system" in health:
                sys_data = health["system"]
                cpu = sys_data.get("cpu_percent", "N/A")
                ram = sys_data.get("memory_percent", "N/A")
                system_info = f"CPU: {cpu}%, RAM: {ram}%"
            elif "load_average" in health and "memory_percent" in health:
                load = health.get("load_average", "N/A")
                ram = health.get("memory_percent", "N/A")
                system_info = f"Load: {load:.1f}, RAM: {ram:.1f}%"
            elif "remote_time" in health:
                system_info = f"Connected"

            # Failure rate information
            failure_info = "N/A"
            if "disk_failure_rate" in health:
                failure_rate = health["disk_failure_rate"]
                if failure_rate > 0:
                    if failure_rate > 0.3:
                        failure_info = f"[red]{failure_rate:.1%}[/red]"
                    elif failure_rate > 0.1:
                        failure_info = f"[yellow]{failure_rate:.1%}[/yellow]"
                    else:
                        failure_info = f"{failure_rate:.1%}"
                else:
                    failure_info = "0%"

            # Additional details
            details = ""
            if "warning" in health:
                details = (
                    health["warning"][:40] + "..."
                    if len(health["warning"]) > 40
                    else health["warning"]
                )
            elif "error" in health:
                details = (
                    health["error"][:40] + "..." if len(health["error"]) > 40 else health["error"]
                )
            elif health.get("utilization"):
                details = f"Utilization: {health['utilization']}"

            table.add_row(
                name, source_type, status_display, disk_info, system_info, failure_info, details
            )

        console.print(table)

        # Show summary of issues
        issues = []
        for name, health in health_report.items():
            if health.get("status") == "unhealthy":
                issues.append(f"[red]✗ {name}: {health.get('error', 'Unknown error')}[/red]")
            elif health.get("status") == "degraded":
                issues.append(
                    f"[yellow]⚠ {name}: {health.get('warning', 'Degraded performance')}[/yellow]"
                )
            elif health.get("disk_status") == "critical":
                issues.append(
                    f"[red]⚠ {name}: Critical disk space - {health.get('disk_message', '')}[/red]"
                )

        if issues:
            console.print("\n[bold]Issues detected:[/bold]")
            for issue in issues:
                console.print(f"  {issue}")
        else:
            console.print("\n[green]✓ All sources appear healthy[/green]")

    if watch:
        console.print(
            f"[bold]Monitoring compute source health every {refresh} seconds (Ctrl+C to stop)...[/bold]"
        )

        try:
            while True:
                console.clear()
                console.print(
                    f"[bold]Distributed Health Monitor - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/bold]\n"
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


@distributed.command()
@click.argument("name")
@click.confirmation_option(prompt="Are you sure you want to remove this compute source?")
def remove(name: str):
    """Remove a compute source from distributed configuration."""
    console = Console()

    if name == "local":
        console.print("[red]Cannot remove the local compute source.[/red]")
        return

    hsm_config = HSMConfig.load()
    if not hsm_config:
        console.print("[red]No hsm_config.yaml found.[/red]")
        return

    config_data = hsm_config.config_data
    remotes = config_data.get("distributed", {}).get("remotes", {})

    if name not in remotes:
        console.print(f"[red]Compute source '{name}' not found.[/red]")
        return

    # Remove the source
    del remotes[name]

    # Save updated configuration
    config_path = Path.cwd() / "sweeps" / "hsm_config.yaml"
    if not config_path.exists():
        config_path = Path.cwd() / "hsm_config.yaml"

    try:
        with open(config_path, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)

        console.print(f"[green]✓ Removed compute source '{name}'[/green]")

    except Exception as e:
        console.print(f"[red]Failed to save configuration: {e}[/red]")


if __name__ == "__main__":
    distributed()
