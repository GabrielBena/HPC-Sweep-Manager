"""CLI commands for syncing sweep results."""

import logging
from pathlib import Path

import click
from rich.panel import Panel
from rich.table import Table

from ..sync.config import SyncConfig
from ..sync.sync_manager import SyncManager
from .common import common_options

logger = logging.getLogger(__name__)


@click.group()
def sync():
    """Sync sweep results and wandb runs to remote targets."""
    pass


@sync.command("init")
@click.option("--project-name", help="Project name for template paths", default="my-project")
@common_options
@click.pass_context
def sync_init(ctx, project_name: str, verbose: bool, quiet: bool):
    """Initialize sync configuration with template."""
    console = ctx.obj["console"]
    logger = ctx.obj["logger"]

    console.print(
        Panel.fit(
            "[bold blue]Initialize Sync Configuration[/bold blue]",
            border_style="blue",
        )
    )

    # Determine output path
    config_path = Path.cwd() / ".hsm" / "sync_config.yaml"

    if config_path.exists():
        console.print(f"[yellow]Sync config already exists at: {config_path}[/yellow]")
        if not click.confirm("Overwrite existing config?", default=False):
            console.print("[dim]Cancelled[/dim]")
            return

    # Create template
    if SyncConfig.create_template(config_path, project_name):
        console.print("\n[green]✅ Created sync configuration template:[/green]")
        console.print(f"   {config_path}")
        console.print("\n[bold]Next steps:[/bold]")
        console.print("1. Edit the configuration file to add your sync targets")
        console.print("2. Test with: hsm sync --list")
        console.print("3. Sync with: hsm sync <sweep-id> --target <target-name>")
    else:
        console.print("[red]❌ Failed to create sync configuration[/red]")


@sync.command("list")
@common_options
@click.pass_context
def sync_list(ctx, verbose: bool, quiet: bool):
    """List configured sync targets."""
    console = ctx.obj["console"]
    logger = ctx.obj["logger"]

    # Load sync config
    sync_config = SyncConfig()

    if not sync_config.config_path or not sync_config.config_path.exists():
        console.print("[yellow]No sync configuration found[/yellow]")
        console.print("Run 'hsm sync init' to create a configuration template")
        return

    # Create sync manager and list targets
    manager = SyncManager(sync_config, console)
    manager.list_targets()


@sync.command("run")
@click.argument("sweep_id")
@click.option(
    "--target",
    "-t",
    multiple=True,
    help="Sync target name(s). Can specify multiple times.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be synced without actually syncing",
)
@click.option(
    "--sweep-only",
    is_flag=True,
    help="Only sync sweep metadata (not wandb runs)",
)
@click.option(
    "--wandb-only",
    is_flag=True,
    help="Only sync wandb runs (not sweep metadata)",
)
@click.option(
    "--all-versions",
    is_flag=True,
    help="Sync all artifact versions (not just latest)",
)
@common_options
@click.pass_context
def sync_run(
    ctx,
    sweep_id: str,
    target: tuple,
    dry_run: bool,
    sweep_only: bool,
    wandb_only: bool,
    all_versions: bool,
    verbose: bool,
    quiet: bool,
):
    """Sync a sweep to one or more targets.

    Examples:
        hsm sync run sweep_20251020_212836 --target desktop
        hsm sync run sweep_20251020_212836 --target desktop --dry-run
        hsm sync run sweep_20251020_212836 --target desktop --target laptop
        hsm sync run sweep_20251020_212836 --target desktop --sweep-only
    """
    console = ctx.obj["console"]
    logger = ctx.obj["logger"]

    # Validate options
    if sweep_only and wandb_only:
        console.print("[red]Error: Cannot specify both --sweep-only and --wandb-only[/red]")
        return

    # Load sync config
    sync_config = SyncConfig()

    if not sync_config.config_path or not sync_config.config_path.exists():
        console.print("[red]No sync configuration found[/red]")
        console.print("Run 'hsm sync init' to create a configuration template")
        return

    # Get target names
    target_names = list(target) if target else [sync_config.default_target]

    if not target_names or target_names == [None]:
        console.print("[red]Error: No sync target specified and no default configured[/red]")
        console.print("Specify target with --target or set default_target in sync_config.yaml")
        return

    # Show header
    console.print(
        Panel.fit(
            f"[bold blue]Sync Sweep: {sweep_id}[/bold blue]",
            border_style="blue",
        )
    )

    if dry_run:
        console.print("[yellow]DRY RUN MODE - No files will be transferred[/yellow]\n")

    # Create sync manager and run sync
    manager = SyncManager(sync_config, console)

    success = manager.sync_sweep(
        sweep_id=sweep_id,
        target_names=target_names,
        dry_run=dry_run,
        sweep_only=sweep_only,
        wandb_only=wandb_only,
        latest_only=not all_versions,
    )

    if success:
        console.print("\n[green]✅ Sync completed successfully[/green]")
    else:
        console.print("\n[red]❌ Sync completed with errors[/red]")
        ctx.exit(1)


# Add shorter alias for "sync run"
@sync.command()
@click.argument("sweep_id")
@click.option("--target", "-t", multiple=True, help="Sync target name(s)")
@click.option("--dry-run", is_flag=True, help="Show what would be synced")
@click.option("--sweep-only", is_flag=True, help="Only sync sweep metadata")
@click.option("--wandb-only", is_flag=True, help="Only sync wandb runs")
@click.option("--all-versions", is_flag=True, help="Sync all artifact versions")
@common_options
@click.pass_context
def to(
    ctx,
    sweep_id: str,
    target: tuple,
    dry_run: bool,
    sweep_only: bool,
    wandb_only: bool,
    all_versions: bool,
    verbose: bool,
    quiet: bool,
):
    """Shorthand for 'sync run' - sync a sweep to target(s).

    Examples:
        hsm sync to sweep_20251020_212836 --target desktop
        hsm sync to sweep_20251020_212836 -t desktop --dry-run
    """
    ctx.invoke(
        sync_run,
        sweep_id=sweep_id,
        target=target,
        dry_run=dry_run,
        sweep_only=sweep_only,
        wandb_only=wandb_only,
        all_versions=all_versions,
        verbose=verbose,
        quiet=quiet,
    )


@sync.command("cache")
@click.option(
    "--sweep-id",
    help="Sweep ID to show cache for (or clear if using --clear)",
)
@click.option(
    "--clear",
    is_flag=True,
    help="Clear cache instead of showing it",
)
@click.option(
    "--all",
    "clear_all",
    is_flag=True,
    help="Clear entire cache (all sweeps)",
)
@common_options
@click.pass_context
def cache_management(ctx, sweep_id: str, clear: bool, clear_all: bool, verbose: bool, quiet: bool):
    """Manage wandb sync cache.

    The cache stores three types of information:
    1. Which runs belong to which sweep (metadata scan results)
    2. Whether runs are finished or still running
    3. Which finished runs have been successfully synced

    Examples:
        # Show cache summary
        hsm sync cache

        # Show cache for specific sweep
        hsm sync cache --sweep-id sweep_20251020_212836

        # Clear cache for specific sweep
        hsm sync cache --sweep-id sweep_20251020_212836 --clear

        # Clear entire cache
        hsm sync cache --clear --all
    """
    console = ctx.obj["console"]

    # Get cache directory path (per-sweep files)
    cache_dir = Path.cwd() / ".hsm" / "cache" / "wandb_sweeps"

    if clear:
        if clear_all:
            # Clear entire cache directory
            if cache_dir.exists():
                if click.confirm("Clear entire wandb sync cache?", default=False):
                    import shutil

                    shutil.rmtree(cache_dir)
                    cache_dir.mkdir(parents=True, exist_ok=True)
                    console.print("[green]✅ Cleared entire cache[/green]")
                else:
                    console.print("[dim]Cancelled[/dim]")
            else:
                console.print("[yellow]No cache directory found[/yellow]")
        elif sweep_id:
            # Clear specific sweep file
            import json

            safe_sweep_id = sweep_id.replace("/", "_").replace("\\", "_")
            cache_file = cache_dir / f"{safe_sweep_id}.json"

            if cache_file.exists():
                if click.confirm(f"Clear cache for sweep {sweep_id}?", default=False):
                    cache_file.unlink()
                    console.print(f"[green]✅ Cleared cache for sweep {sweep_id}[/green]")
                else:
                    console.print("[dim]Cancelled[/dim]")
            else:
                console.print(f"[yellow]No cache found for sweep {sweep_id}[/yellow]")
        else:
            console.print("[red]Error: Specify --sweep-id or --all when using --clear[/red]")
    else:
        # Show cache info
        if not cache_dir.exists() or not list(cache_dir.glob("*.json")):
            console.print("[yellow]No cache files found[/yellow]")
            console.print(f"Cache will be created at: {cache_dir}")
            return

        import json

        if sweep_id:
            # Show specific sweep
            safe_sweep_id = sweep_id.replace("/", "_").replace("\\", "_")
            cache_file = cache_dir / f"{safe_sweep_id}.json"

            if not cache_file.exists():
                console.print(f"[yellow]No cache found for sweep {sweep_id}[/yellow]")
                return

            with open(cache_file) as f:
                sweep_cache = json.load(f)

            console.print(f"\n[bold]Cache for {sweep_id}:[/bold]")
            console.print(f"  Cache file: {cache_file.name}")
            total_runs = len(sweep_cache.get("runs", []))
            console.print(f"  Total runs: {total_runs}")

            run_status = sweep_cache.get("run_status", {})
            finished_count = sum(1 for r in run_status.values() if r.get("finished", False))
            running_count = len(run_status) - finished_count

            console.print(f"  Finished runs: {finished_count}")
            console.print(f"  Running runs: {running_count}")

            synced = sweep_cache.get("synced", {})
            synced_count = sum(1 for v in synced.values() if v)
            console.print(f"  Synced runs: {synced_count}")

            # Show some example runs
            runs = sweep_cache.get("runs", [])[:5]
            if runs:
                console.print("\n  Example runs:")
                for run_name in runs:
                    status_parts = []
                    if run_name in run_status:
                        if run_status[run_name].get("finished"):
                            status_parts.append("finished")
                        else:
                            status_parts.append("running")
                    if synced.get(run_name):
                        status_parts.append("synced")
                    status_str = f" ({', '.join(status_parts)})" if status_parts else ""
                    console.print(f"    - {run_name}{status_str}")
        else:
            # Show summary of all sweeps
            table = Table(title="Wandb Sync Cache Summary")
            table.add_column("Sweep ID", style="cyan")
            table.add_column("Total Runs", justify="right")
            table.add_column("Finished", justify="right", style="green")
            table.add_column("Running", justify="right", style="yellow")
            table.add_column("Synced", justify="right", style="blue")

            cache_files = sorted(cache_dir.glob("*.json"))
            for cache_file in cache_files:
                try:
                    with open(cache_file) as f:
                        sweep_cache = json.load(f)

                    sid = sweep_cache.get("sweep_id", cache_file.stem)
                    total = len(sweep_cache.get("runs", []))
                    run_status = sweep_cache.get("run_status", {})
                    finished = sum(1 for r in run_status.values() if r.get("finished", False))
                    running = len(run_status) - finished
                    synced_dict = sweep_cache.get("synced", {})
                    synced = sum(1 for v in synced_dict.values() if v)

                    table.add_row(sid, str(total), str(finished), str(running), str(synced))
                except Exception as e:
                    console.print(f"[red]Error reading {cache_file.name}: {e}[/red]")

            console.print()
            console.print(table)
            console.print()
            console.print(f"Cache location: {cache_dir}")


if __name__ == "__main__":
    sync()
