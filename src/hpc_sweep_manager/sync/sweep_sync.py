"""Sweep metadata syncing functionality."""

import logging
from pathlib import Path
import subprocess
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

        # Handle depth limiting using find + --files-from (like rsync.sh)
        temp_file_path = None
        if max_depth:
            # Create temporary file with files to sync (respecting depth limit)
            temp_file_path = self._create_depth_limited_file_list(sweep_dir, max_depth)
            if temp_file_path:
                rsync_cmd.extend(
                    [
                        "--files-from",
                        temp_file_path,
                        "--dirs",  # Create directories as needed
                    ]
                )
                # Source is the parent directory for --files-from
                source = str(sweep_dir.parent) + "/"
            else:
                # Fallback to regular sync if file list creation failed
                source = str(sweep_dir) + "/"
        else:
            # Regular sync without depth limiting
            source = str(sweep_dir) + "/"

        # Add exclusions
        rsync_cmd.extend(
            [
                "--exclude=wandb/",  # Don't sync wandb runs here
                "--exclude=*.ckpt",  # Exclude large checkpoint files
                "--exclude=*.pth",
            ]
        )

        # Destination
        sweep_dir_name = sweep_dir.name
        remote_sweeps_path = self.target.get_path("sweeps")
        destination = f"{self.target.ssh_host}:{remote_sweeps_path}/{sweep_dir_name}/"

        rsync_cmd.extend([source, destination])

        # Execute rsync
        try:
            self.console.print(f"  Source: {sweep_dir}")
            self.console.print(f"  Destination: {destination}")

            if dry_run:
                self.console.print("  [dim](dry run)[/dim]")

            result = subprocess.run(rsync_cmd, capture_output=False, text=True, check=True)

            logger.info(f"Successfully synced sweep metadata for {sweep_id}")
            return True

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to sync sweep metadata: {e}")
            return False
        finally:
            # Clean up temporary file
            if temp_file_path and Path(temp_file_path).exists():
                try:
                    Path(temp_file_path).unlink()
                except Exception as e:
                    logger.debug(f"Failed to remove temp file {temp_file_path}: {e}")

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

    def _create_depth_limited_file_list(self, sweep_dir: Path, max_depth: int) -> Optional[str]:
        """
        Create a temporary file list for rsync with depth limiting.

        Args:
            sweep_dir: Sweep directory to sync
            max_depth: Maximum directory depth

        Returns:
            Path to temporary file with file list, or None if failed
        """
        import tempfile

        try:
            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt")

            # Use find to get files within depth limit, preserving folder structure
            # Like rsync.sh: find "$folder_name" -maxdepth "$max_depth" -type f -printf "$folder_name/%P\n"
            find_cmd = [
                "find",
                str(sweep_dir),
                "-maxdepth",
                str(max_depth),
                "-type",
                "f",
                "-printf",
                f"{sweep_dir.name}/%P\\n",
            ]

            result = subprocess.run(find_cmd, capture_output=True, text=True, check=True)

            # Write the paths directly (they already include the folder prefix)
            for file_path in result.stdout.strip().split("\n"):
                if file_path:  # Skip empty lines
                    temp_file.write(file_path + "\n")

            temp_file.flush()
            temp_file.close()

            logger.debug(f"Created depth-limited file list: {temp_file.name}")
            return temp_file.name

        except Exception as e:
            logger.error(f"Failed to create depth-limited file list: {e}")
            return None
