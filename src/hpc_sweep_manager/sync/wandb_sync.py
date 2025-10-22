"""Wandb runs syncing functionality with optimization."""

from concurrent.futures import ThreadPoolExecutor
import logging
from pathlib import Path
import subprocess
from typing import List, Optional, Set

from rich.console import Console

from .config import SyncTarget

logger = logging.getLogger(__name__)


class WandbSyncer:
    """Handles syncing of wandb run directories with optimizations."""

    def __init__(self, target: SyncTarget, console: Optional[Console] = None):
        """
        Initialize wandb syncer.

        Args:
            target: Sync target configuration
            console: Rich console for output (optional)
        """
        self.target = target
        self.console = console or Console()

    def sync_wandb_runs(
        self,
        sweep_id: str,
        dry_run: bool = False,
        latest_only: bool = True,
    ) -> bool:
        """
        Sync wandb runs for a sweep to target.

        Args:
            sweep_id: Sweep ID to sync runs for
            dry_run: If True, only show what would be synced
            latest_only: If True, only sync latest artifact versions

        Returns:
            True if successful, False otherwise
        """
        # Find wandb directory
        wandb_dir = self._find_wandb_dir()
        if not wandb_dir:
            self.console.print("[red]Error: Wandb directory not found[/red]")
            return False

        # Find runs belonging to this sweep
        run_dirs = self._find_sweep_runs(wandb_dir, sweep_id)
        if not run_dirs:
            self.console.print(f"[yellow]No wandb runs found for sweep {sweep_id}[/yellow]")
            return True  # Not an error, just no runs

        self.console.print(f"  Found {len(run_dirs)} wandb runs for sweep")

        # Check remote to see what already exists (if enabled)
        existing_runs = set()
        if self.target.get_option("check_remote_first", True) and not dry_run:
            self.console.print("  Checking remote for existing runs...")
            existing_runs = self._check_remote_runs()
            if existing_runs:
                self.console.print(f"  Found {len(existing_runs)} runs already on remote")

        # Filter to runs that need syncing
        runs_to_sync = [r for r in run_dirs if r.name not in existing_runs]

        if not runs_to_sync:
            self.console.print("[green]  All runs already synced![/green]")
            return True

        self.console.print(f"  Need to sync {len(runs_to_sync)} runs")

        # Sync runs (parallel if enabled)
        parallel_transfers = self.target.get_option("parallel_transfers", 1)

        if parallel_transfers > 1:
            return self._sync_runs_parallel(
                runs_to_sync,
                dry_run=dry_run,
                latest_only=latest_only,
                max_workers=parallel_transfers,
            )
        else:
            return self._sync_runs_sequential(
                runs_to_sync,
                dry_run=dry_run,
                latest_only=latest_only,
            )

    def _find_wandb_dir(self) -> Optional[Path]:
        """Find the wandb directory."""
        search_paths = [
            Path.cwd() / "wandb",
            Path.cwd() / "outputs" / "wandb",
            Path("wandb"),
        ]

        for path in search_paths:
            if path.exists() and path.is_dir():
                # Check if it contains run directories
                run_dirs = list(path.glob("run-*"))
                if run_dirs:
                    logger.debug(f"Found wandb directory: {path}")
                    return path

        return None

    def _find_sweep_runs(self, wandb_dir: Path, sweep_id: str) -> List[Path]:
        """
        Find all wandb runs belonging to a sweep.

        This is a simplified version - in production, you'd want to use
        the more sophisticated logic from sync_wandb_runs.py
        """
        # For now, use a simple heuristic: check run directories by date pattern
        import re

        # Extract date from sweep ID if possible
        date_match = re.search(r"(\d{8})", sweep_id)
        if date_match:
            sweep_date = date_match.group(1)
            run_dirs = list(wandb_dir.glob(f"run-{sweep_date}_*"))
        else:
            # Fall back to all runs (will be slow for large wandb dirs)
            run_dirs = list(wandb_dir.glob("run-*"))

        # TODO: In future, read metadata to confirm sweep membership
        # For now, return all runs matching the date pattern
        return run_dirs

    def _check_remote_runs(self) -> Set[str]:
        """
        Check which runs already exist on remote.

        Returns:
            Set of run directory names that exist on remote
        """
        try:
            remote_wandb_path = self.target.get_path("wandb")
            ssh_host = self.target.ssh_host

            # Use SSH to list remote run directories
            cmd = ["ssh", ssh_host, f"ls -1 {remote_wandb_path} 2>/dev/null | grep '^run-' || true"]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,  # Don't fail if directory doesn't exist
            )

            if result.returncode == 0 and result.stdout:
                # Parse output into set of run names
                existing = set(result.stdout.strip().split("\n"))
                existing = {r for r in existing if r.startswith("run-")}
                return existing

            return set()

        except Exception as e:
            logger.warning(f"Failed to check remote runs: {e}")
            return set()

    def _sync_runs_sequential(
        self,
        run_dirs: List[Path],
        dry_run: bool,
        latest_only: bool,
    ) -> bool:
        """Sync runs sequentially (one at a time)."""
        success = True

        for i, run_dir in enumerate(run_dirs, 1):
            self.console.print(f"\n  [{i}/{len(run_dirs)}] Syncing {run_dir.name}...")

            if not self._sync_single_run(run_dir, dry_run, latest_only):
                success = False
                # Continue with other runs even if one fails

        return success

    def _sync_runs_parallel(
        self,
        run_dirs: List[Path],
        dry_run: bool,
        latest_only: bool,
        max_workers: int,
    ) -> bool:
        """Sync runs in parallel."""
        self.console.print(f"  Using {max_workers} parallel transfers...")

        success = True
        completed = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._sync_single_run, run_dir, dry_run, latest_only): run_dir
                for run_dir in run_dirs
            }

            for future in futures:
                run_dir = futures[future]
                try:
                    if not future.result():
                        success = False
                        self.console.print(f"[red]  ✗ Failed: {run_dir.name}[/red]")
                    else:
                        completed += 1
                        progress_str = f"({completed}/{len(run_dirs)})"
                        self.console.print(
                            f"[green]  ✓ Completed: {run_dir.name}[/green] {progress_str}"
                        )
                except Exception as e:
                    logger.error(f"Error syncing {run_dir.name}: {e}")
                    success = False

        return success

    def _sync_single_run(
        self,
        run_dir: Path,
        dry_run: bool,
        latest_only: bool,
    ) -> bool:
        """Sync a single run directory."""
        try:
            rsync_cmd = [
                "rsync",
                "-az",  # archive, compress (no verbose for parallel)
                "--update",  # Skip files that are newer on receiver
            ]

            if dry_run:
                rsync_cmd.append("--dry-run")

            if latest_only:
                # Add filters to skip old artifact versions
                rsync_cmd.extend(
                    [
                        "--exclude=*:v[0-9]*",  # Exclude versioned artifacts except latest
                        "--exclude=*_[0-9]*_*.table.json",  # Exclude old table versions
                    ]
                )

            # Source and destination
            remote_wandb_path = self.target.get_path("wandb")
            source = str(run_dir) + "/"
            destination = f"{self.target.ssh_host}:{remote_wandb_path}/{run_dir.name}/"

            rsync_cmd.extend([source, destination])

            # Execute rsync
            subprocess.run(
                rsync_cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout per run
                check=True,
            )

            return True

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to sync {run_dir.name}: {e}")
            return False
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout syncing {run_dir.name}")
            return False
