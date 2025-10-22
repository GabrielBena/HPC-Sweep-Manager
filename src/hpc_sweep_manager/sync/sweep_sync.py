"""Sweep metadata syncing functionality."""

import logging
import subprocess
from pathlib import Path
from typing import Optional

from rich.console import Console

from .config import SyncTarget

logger = logging.getLogger(__name__)


class SweepSyncer:
    """Handles syncing of sweep metadata and configuration files."""

    def __init__(self, target: SyncTarget, console: Optional[Console] = None):
        """
        Initialize sweep syncer.

        Args:
            target: Sync target configuration
            console: Rich console for output (optional)
        """
        self.target = target
        self.console = console or Console()

    def sync_sweep_metadata(self, sweep_id: str, dry_run: bool = False) -> bool:
        """
        Sync sweep metadata (config, logs, etc.) to target.

        Args:
            sweep_id: Sweep ID to sync
            dry_run: If True, only show what would be synced

        Returns:
            True if successful, False otherwise
        """
        # Find sweep output directory
        sweep_dir = self._find_sweep_dir(sweep_id)
        if not sweep_dir:
            self.console.print(f"[red]Error: Sweep directory not found for {sweep_id}[/red]")
            return False

        # Get sync depth
        max_depth = self.target.get_option("max_depth", 1)

        # Build rsync command
        rsync_cmd = [
            "rsync",
            "-avz",
            "--progress",
        ]

        if dry_run:
            rsync_cmd.append("--dry-run")

        # Add depth limiting if specified
        if max_depth:
            rsync_cmd.extend([
                f"--max-depth={max_depth}",
                "--exclude=wandb/",  # Don't sync wandb runs here
                "--exclude=*.ckpt",  # Exclude large checkpoint files
                "--exclude=*.pth",
            ])

        # Source and destination
        sweep_dir_name = sweep_dir.name
        remote_sweeps_path = self.target.get_path("sweeps")
        
        # Ensure trailing slash for directory sync
        source = str(sweep_dir) + "/"
        destination = f"{self.target.ssh_host}:{remote_sweeps_path}/{sweep_dir_name}/"

        rsync_cmd.extend([source, destination])

        # Execute rsync
        try:
            self.console.print(f"  Source: {sweep_dir}")
            self.console.print(f"  Destination: {destination}")
            
            if dry_run:
                self.console.print("  [dim](dry run)[/dim]")

            result = subprocess.run(
                rsync_cmd,
                capture_output=False,
                text=True,
                check=True
            )

            logger.info(f"Successfully synced sweep metadata for {sweep_id}")
            return True

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to sync sweep metadata: {e}")
            return False

    def _find_sweep_dir(self, sweep_id: str) -> Optional[Path]:
        """
        Find the sweep output directory.

        Args:
            sweep_id: Sweep ID to find

        Returns:
            Path to sweep directory if found, None otherwise
        """
        # Try common locations
        search_paths = [
            Path.cwd() / "sweeps" / "outputs" / sweep_id,
            Path.cwd() / "outputs" / sweep_id,
            Path("sweeps") / "outputs" / sweep_id,
        ]

        for path in search_paths:
            if path.exists() and path.is_dir():
                logger.debug(f"Found sweep directory: {path}")
                return path

        logger.warning(f"Sweep directory not found for {sweep_id}")
        return None

