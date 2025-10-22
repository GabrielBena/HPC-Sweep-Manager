"""CLI commands for syncing sweep results."""

import logging
from pathlib import Path

import click
from rich.console import Console
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
@click.option(
    "--project-name",
    help="Project name for template paths",
    default="my-project"
)
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
        console.print(f"\n[green]✅ Created sync configuration template:[/green]")
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
def to(ctx, sweep_id: str, target: tuple, dry_run: bool, sweep_only: bool, wandb_only: bool, all_versions: bool, verbose: bool, quiet: bool):
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


if __name__ == "__main__":
    sync()

