"""Main sync manager for coordinating sweep and wandb syncing."""

import logging
from typing import List, Optional

from rich.console import Console

from .config import SyncConfig, SyncTarget
from .sweep_sync import SweepSyncer
from .wandb_sync import WandbSyncer

logger = logging.getLogger(__name__)


class SyncManager:
    """Manages syncing of sweep results and wandb runs to remote targets."""

    def __init__(self, sync_config: SyncConfig, console: Optional[Console] = None):
        """
        Initialize sync manager.

        Args:
            sync_config: Sync configuration
            console: Rich console for output (optional)
        """
        self.sync_config = sync_config
        self.console = console or Console()

    def sync_sweep(
        self,
        sweep_id: str,
        target_names: List[str],
        dry_run: bool = False,
        sweep_only: bool = False,
        wandb_only: bool = False,
        latest_only: bool = True,
    ) -> bool:
        """
        Sync a sweep to one or more targets.

        Args:
            sweep_id: Sweep ID to sync
            target_names: List of target names to sync to
            dry_run: If True, only show what would be synced
            sweep_only: If True, only sync sweep metadata
            wandb_only: If True, only sync wandb runs
            latest_only: If True, only sync latest artifact versions

        Returns:
            True if all syncs successful, False otherwise
        """
        if not target_names:
            self.console.print("[red]Error: No sync targets specified[/red]")
            return False

        all_success = True

        for target_name in target_names:
            target = self.sync_config.get_target(target_name)
            if not target:
                self.console.print(f"[red]Error: Unknown sync target '{target_name}'[/red]")
                all_success = False
                continue

            self.console.print(
                f"\n[bold cyan]Syncing sweep {sweep_id} to {target_name}[/bold cyan]"
            )
            self.console.print(f"  Target: {target.ssh_host}")

            success = self._sync_to_target(
                sweep_id=sweep_id,
                target=target,
                dry_run=dry_run,
                sweep_only=sweep_only,
                wandb_only=wandb_only,
                latest_only=latest_only,
            )

            if not success:
                all_success = False

        return all_success

    def _sync_to_target(
        self,
        sweep_id: str,
        target: SyncTarget,
        dry_run: bool,
        sweep_only: bool,
        wandb_only: bool,
        latest_only: bool,
    ) -> bool:
        """Sync sweep to a single target."""
        success = True

        # Sync sweep metadata unless wandb_only
        if not wandb_only and target.get_option("sync_sweep_metadata", True):
            self.console.print("\n[yellow]Syncing sweep metadata...[/yellow]")
            sweep_syncer = SweepSyncer(target, self.console)
            if not sweep_syncer.sync_sweep_metadata(sweep_id, dry_run=dry_run):
                self.console.print("[red]✗ Sweep metadata sync failed[/red]")
                success = False
            else:
                self.console.print("[green]✓ Sweep metadata synced[/green]")

        # Sync wandb runs unless sweep_only
        if not sweep_only and target.get_option("sync_wandb_runs", True):
            self.console.print("\n[yellow]Syncing wandb runs...[/yellow]")
            wandb_syncer = WandbSyncer(target, self.console)
            if not wandb_syncer.sync_wandb_runs(sweep_id, dry_run=dry_run, latest_only=latest_only):
                self.console.print("[red]✗ Wandb runs sync failed[/red]")
                success = False
            else:
                self.console.print("[green]✓ Wandb runs synced[/green]")

        return success

    def list_targets(self) -> None:
        """List all configured sync targets."""
        if not self.sync_config.has_targets():
            self.console.print("[yellow]No sync targets configured[/yellow]")
            self.console.print("Run 'hsm sync init' to create a configuration template")
            return

        self.console.print("\n[bold]Configured Sync Targets:[/bold]\n")

        for target_name in self.sync_config.list_targets():
            target = self.sync_config.get_target(target_name)
            is_default = target_name == self.sync_config.default_target

            self.console.print(
                f"[cyan]{target_name}[/cyan]{'  [dim](default)[/dim]' if is_default else ''}"
            )
            self.console.print(f"  Host: {target.ssh_host}")
            self.console.print(f"  Sweeps: {target.get_path('sweeps')}")
            self.console.print(f"  Wandb: {target.get_path('wandb')}")

            # Show key options
            options = []
            if target.get_option("sync_sweep_metadata"):
                options.append("metadata")
            if target.get_option("sync_wandb_runs"):
                options.append("wandb")
            if target.get_option("latest_only"):
                options.append("latest-only")

            self.console.print(f"  Options: {', '.join(options)}\n")
