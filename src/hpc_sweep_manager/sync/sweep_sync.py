"""Sweep metadata syncing functionality."""

import logging
from pathlib import Path
import subprocess
import tempfile
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
        max_depth = self.target.get_option("max_depth", None)

        # Source and destination
        sweep_dir_name = sweep_dir.name
        remote_sweeps_path = self.target.get_path("sweeps")
        destination = f"{self.target.ssh_host}:{remote_sweeps_path}/{sweep_dir_name}/"

        self.console.print(f"  Source: {sweep_dir}")
        self.console.print(f"  Destination: {destination}")

        if dry_run:
            self.console.print("  [dim](dry run)[/dim]")

        # Execute rsync (with or without depth limiting)
        try:
            if max_depth:
                # Use find + rsync --files-from for depth limiting (like rsync.sh)
                self._sync_with_depth_limit(sweep_dir, destination, max_depth, dry_run)
            else:
                # Simple rsync without depth limiting
                self._sync_simple(sweep_dir, destination, dry_run)

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

    def _sync_simple(self, sweep_dir: Path, destination: str, dry_run: bool) -> None:
        """Sync sweep directory without depth limiting."""
        rsync_cmd = [
            "rsync",
            "-avz",
            "--progress",
            "--exclude=wandb/",  # Don't sync wandb runs here
            "--exclude=*.ckpt",  # Exclude large checkpoint files
            "--exclude=*.pth",
        ]

        if dry_run:
            rsync_cmd.append("--dry-run")

        # Source with trailing slash for directory sync
        source = str(sweep_dir) + "/"
        rsync_cmd.extend([source, destination])

        subprocess.run(rsync_cmd, capture_output=False, text=True, check=True)

    def _sync_with_depth_limit(
        self, sweep_dir: Path, destination: str, max_depth: int, dry_run: bool
    ) -> None:
        """Sync sweep directory with depth limiting using find + rsync --files-from."""
        # Create temporary file for file list (like rsync.sh)
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as temp_file:
            temp_file_path = temp_file.name

            # Use find to generate file list within depth limit
            find_cmd = [
                "find",
                str(sweep_dir),
                "-maxdepth",
                str(max_depth),
                "-type",
                "f",
                "!",
                "-path",
                "*/wandb/*",  # Exclude wandb
                "!",
                "-name",
                "*.ckpt",  # Exclude checkpoints
                "!",
                "-name",
                "*.pth",
            ]

            try:
                # Run find and write to temp file
                result = subprocess.run(find_cmd, capture_output=True, text=True, check=True)

                # Write relative paths to temp file
                with open(temp_file_path, "w") as f:
                    for file_path in result.stdout.strip().split("\n"):
                        if file_path:
                            # Convert to relative path from sweep_dir parent
                            rel_path = Path(file_path).relative_to(sweep_dir.parent)
                            f.write(str(rel_path) + "\n")

                # Run rsync with --files-from
                rsync_cmd = [
                    "rsync",
                    "-avz",
                    "--progress",
                    "--files-from",
                    temp_file_path,
                    str(sweep_dir.parent) + "/",  # Source base directory
                    destination.rsplit("/", 1)[0] + "/",  # Destination base directory
                ]

                if dry_run:
                    rsync_cmd.append("--dry-run")

                subprocess.run(rsync_cmd, capture_output=False, text=True, check=True)

            finally:
                # Clean up temp file
                Path(temp_file_path).unlink(missing_ok=True)
