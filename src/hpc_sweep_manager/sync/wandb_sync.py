"""Wandb runs syncing functionality with optimization."""

from concurrent.futures import ThreadPoolExecutor
import json
import logging
from pathlib import Path
import re
import subprocess
from typing import Dict, List, Optional, Set

from rich.console import Console
import yaml

from .config import SyncTarget

logger = logging.getLogger(__name__)


class WandbSyncer:
    """
    Handles syncing of wandb run directories with optimizations.

    Three-State Caching System:
    ---------------------------
    This syncer tracks three independent states for each run:

    1. **Cached Metadata (Sweep Membership)**
       - Determines which runs belong to which sweep
       - Built by scanning wandb metadata files (group field)
       - Updated during both dry-run and actual sync
       - Expensive to compute, so cached for reuse

    2. **Run Status (Finished vs Running)**
       - Determines if a run is finished or still running
       - Detected by presence of wandb-summary.json
       - Cached along with metadata
       - Updated during both dry-run and actual sync

    3. **Sync Status (Already Rsynced)**
       - Tracks which runs have been successfully rsynced
       - ONLY updated after successful actual rsync (not dry-run)
       - Used to skip re-syncing unchanged finished runs

    Skip Logic:
    -----------
    - Skip ONLY if: (finished=True AND synced=True)
    - Always sync if: running OR (finished but not synced)
    - Running runs are never marked as synced (to get updates)

    Dry-Run Behavior:
    -----------------
    - Builds/updates States 1 & 2 (metadata and run status)
    - Does NOT update State 3 (sync status)
    - Shows what would be synced without actual transfer
    - Useful for populating expensive metadata cache
    """

    def __init__(self, target: SyncTarget, console: Optional[Console] = None):
        """
        Initialize wandb syncer.

        Args:
            target: Sync target configuration
            console: Rich console for output (optional)
        """
        self.target = target
        self.console = console or Console()

        # Cache directory in .hsm folder
        self._cache_dir = Path.cwd() / ".hsm" / "cache"
        self._cache_dir.mkdir(parents=True, exist_ok=True)

        # Master cache file for all sweeps
        self._cache_file = self._cache_dir / "wandb_sync_cache.json"
        self._cache: Optional[Dict] = None

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

        # Filter out runs that are finished and already synced
        runs_to_sync = self._filter_runs_to_sync(run_dirs, sweep_id, dry_run)

        if not runs_to_sync:
            self.console.print("[green]  All runs already synced![/green]")
            return True

        self.console.print(f"  Need to sync {len(runs_to_sync)} runs")

        # Sync runs (parallel if enabled)
        parallel_transfers = self.target.get_option("parallel_transfers", 1)

        success = False
        if parallel_transfers > 1:
            success = self._sync_runs_parallel(
                runs_to_sync,
                sweep_id=sweep_id,
                dry_run=dry_run,
                latest_only=latest_only,
                max_workers=parallel_transfers,
            )
        else:
            success = self._sync_runs_sequential(
                runs_to_sync,
                sweep_id=sweep_id,
                dry_run=dry_run,
                latest_only=latest_only,
            )

        # Mark successfully synced runs in cache (only for finished runs)
        if success and not dry_run:
            self._mark_runs_as_synced(sweep_id, runs_to_sync)

        return success

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

    def _find_sweep_runs(
        self, wandb_dir: Path, sweep_id: str, force_refresh: bool = False
    ) -> List[Path]:
        """
        Find all wandb runs belonging to a sweep using cached metadata.

        Args:
            wandb_dir: Path to wandb directory
            sweep_id: Sweep ID to filter by
            force_refresh: If True, ignore cache and rescan all runs

        Returns:
            List of run directory paths belonging to the sweep
        """
        # Load cache
        cache = self._load_cache()

        # Check if we have cached results for this sweep
        if not force_refresh and sweep_id in cache.get("sweeps", {}):
            cached_data = cache["sweeps"][sweep_id]
            run_names = cached_data.get("runs", [])

            # Validate cached runs still exist
            cached_runs = [wandb_dir / name for name in run_names if (wandb_dir / name).exists()]

            logger.debug(f"Loaded {len(cached_runs)} runs from cache for sweep {sweep_id}")

            # Check for new runs incrementally
            new_runs = self._find_new_runs(wandb_dir, sweep_id, cached_runs)

            if new_runs:
                logger.debug(f"Found {len(new_runs)} new runs for sweep {sweep_id}")
                all_runs = cached_runs + new_runs
                self._update_cache_for_sweep(sweep_id, all_runs)
                return all_runs

            return cached_runs

        # Full scan if no cache or force refresh
        logger.debug(f"Performing full scan for sweep {sweep_id}")
        all_runs = self._scan_all_runs_for_sweep(wandb_dir, sweep_id)

        # Update cache
        self._update_cache_for_sweep(sweep_id, all_runs)

        return all_runs

    def _load_cache(self) -> Dict:
        """Load sweep-to-runs cache from disk."""
        if self._cache is not None:
            return self._cache

        if not self._cache_file.exists():
            self._cache = {"sweeps": {}, "version": 1}
            return self._cache

        try:
            with open(self._cache_file) as f:
                self._cache = json.load(f)
                return self._cache
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}, starting fresh")
            self._cache = {"sweeps": {}, "version": 1}
            return self._cache

    def _save_cache(self) -> None:
        """Save cache to disk."""
        if self._cache is None:
            return

        try:
            with open(self._cache_file, "w") as f:
                json.dump(self._cache, f, indent=2)
            logger.debug(f"Cache saved to {self._cache_file}")
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")

    def clear_cache(self, sweep_id: Optional[str] = None) -> None:
        """
        Clear the wandb run cache.

        Args:
            sweep_id: If provided, only clear cache for this sweep.
                     If None, clear entire cache.
        """
        if sweep_id:
            # Clear cache for specific sweep
            cache = self._load_cache()
            if "sweeps" in cache and sweep_id in cache["sweeps"]:
                del cache["sweeps"][sweep_id]
                self._save_cache()
                self.console.print(f"[green]Cleared cache for sweep {sweep_id}[/green]")
            else:
                self.console.print(f"[yellow]No cache found for sweep {sweep_id}[/yellow]")
        else:
            # Clear entire cache
            self._cache = {"sweeps": {}, "version": 1}
            self._save_cache()
            self.console.print("[green]Cleared entire wandb sync cache[/green]")

    def _update_cache_for_sweep(self, sweep_id: str, run_dirs: List[Path]) -> None:
        """
        Update cache with run directories and their status for a sweep.

        This stores:
        - Which runs belong to the sweep (metadata scan result)
        - Whether each run is finished or running
        - Which runs have been successfully synced (separate from this method)
        """
        cache = self._load_cache()

        if "sweeps" not in cache:
            cache["sweeps"] = {}

        # Preserve existing sync status if it exists
        existing_synced = {}
        if sweep_id in cache["sweeps"] and "synced" in cache["sweeps"][sweep_id]:
            existing_synced = cache["sweeps"][sweep_id]["synced"]

        # Build run status map
        run_status = {}
        for run_dir in run_dirs:
            is_finished = self._is_run_finished(run_dir)
            run_status[run_dir.name] = {
                "finished": is_finished,
                "last_checked": None,  # Could add timestamp here if needed
            }

        cache["sweeps"][sweep_id] = {
            "runs": [r.name for r in run_dirs],
            "count": len(run_dirs),
            "run_status": run_status,
            "synced": existing_synced,  # Preserve sync status
        }

        self._save_cache()

    def _extract_sweep_id_from_run(self, run_dir: Path, target_sweep_id: str) -> Optional[str]:
        """
        Extract sweep ID from a wandb run directory.

        Based on logic from sync_wandb_runs.py - checks multiple metadata files
        and locations for sweep/group information.

        Args:
            run_dir: Path to wandb run directory
            target_sweep_id: Target sweep ID for matching

        Returns:
            Sweep ID if found and matches target, None otherwise
        """
        # Try different metadata files - check both root and files/ subdirectory
        metadata_locations = [
            run_dir / "wandb-metadata.json",
            run_dir / "files" / "wandb-metadata.json",
            run_dir / "config.yaml",
            run_dir / "files" / "config.yaml",
            run_dir / "wandb-summary.json",
            run_dir / "files" / "wandb-summary.json",
        ]

        for metadata_path in metadata_locations:
            if not metadata_path.exists():
                continue

            try:
                if metadata_path.name.endswith(".json"):
                    with open(metadata_path) as f:
                        metadata = json.load(f)
                elif metadata_path.name.endswith(".yaml"):
                    with open(metadata_path) as f:
                        metadata = yaml.safe_load(f)
                else:
                    continue

                # Look for sweep ID in various possible locations
                sweep_id_keys = [
                    "sweep_id",
                    "sweep",
                    ["args", "sweep_id"],
                    ["config", "sweep_id"],
                    ["wandb", "sweep_id"],
                    "group",  # Most common location for sweep ID
                ]

                for key in sweep_id_keys:
                    if isinstance(key, list):
                        # Nested key access
                        value = metadata
                        for k in key:
                            if isinstance(value, dict) and k in value:
                                value = value[k]
                            else:
                                value = None
                                break
                    else:
                        value = metadata.get(key)

                    if value:
                        value_str = str(value)

                        # Try exact match first
                        if value_str == target_sweep_id:
                            return value_str

                        # For group field, try more flexible matching
                        if key == "group":
                            # Check if target sweep is contained in the group value
                            if target_sweep_id in value_str:
                                return target_sweep_id
                            # Check if group value is contained in target sweep
                            if value_str in target_sweep_id:
                                return value_str

                # Special handling for wandb-metadata.json args format
                if metadata_path.name == "wandb-metadata.json" and "args" in metadata:
                    args_list = metadata["args"]
                    if isinstance(args_list, list):
                        for arg in args_list:
                            if isinstance(arg, str) and "wandb.group=" in arg:
                                group_value = arg.split("wandb.group=")[1]
                                if group_value == target_sweep_id or target_sweep_id in group_value:
                                    return target_sweep_id

            except Exception as e:
                logger.debug(f"Error reading {metadata_path}: {e}")
                continue

        return None

    def _scan_all_runs_for_sweep(self, wandb_dir: Path, sweep_id: str) -> List[Path]:
        """
        Scan all run directories to find those belonging to a sweep.

        Uses date filtering as an optimization when possible.

        Args:
            wandb_dir: Path to wandb directory
            sweep_id: Sweep ID to filter by

        Returns:
            List of run directory paths
        """
        # Try to extract date from sweep ID for more efficient filtering
        date_match = re.search(r"(\d{8})", sweep_id)

        if date_match:
            sweep_date = date_match.group(1)
            # Start with date-filtered runs as optimization
            run_dirs = list(wandb_dir.glob(f"run-{sweep_date}_*"))
            logger.debug(f"Found {len(run_dirs)} runs matching date pattern {sweep_date}")

            # If no runs found with date filter, fall back to all runs
            if not run_dirs:
                logger.debug("No runs with date pattern, scanning all runs...")
                run_dirs = list(wandb_dir.glob("run-*"))
        else:
            # No date in sweep ID, scan all runs
            run_dirs = list(wandb_dir.glob("run-*"))

        matching_runs = []

        for run_dir in run_dirs:
            if not run_dir.is_dir():
                continue

            try:
                run_sweep_id = self._extract_sweep_id_from_run(run_dir, sweep_id)
                if run_sweep_id:
                    matching_runs.append(run_dir)
            except Exception as e:
                logger.debug(f"Error processing run {run_dir.name}: {e}")
                continue

        logger.debug(f"Found {len(matching_runs)} runs for sweep {sweep_id}")
        return matching_runs

    def _find_new_runs(
        self, wandb_dir: Path, sweep_id: str, existing_runs: List[Path]
    ) -> List[Path]:
        """
        Find new runs for a sweep that aren't in the existing list.

        Only checks recently modified run directories for efficiency.

        Args:
            wandb_dir: Path to wandb directory
            sweep_id: Sweep ID to filter by
            existing_runs: List of already known run directories

        Returns:
            List of new run directory paths
        """
        existing_names = {r.name for r in existing_runs}

        # Get all run directories
        all_run_dirs = [d for d in wandb_dir.glob("run-*") if d.is_dir()]

        # Filter to only new ones (not in existing)
        new_run_dirs = [d for d in all_run_dirs if d.name not in existing_names]

        if not new_run_dirs:
            return []

        # Check which new runs belong to this sweep
        matching_new_runs = []

        for run_dir in new_run_dirs:
            try:
                run_sweep_id = self._extract_sweep_id_from_run(run_dir, sweep_id)
                if run_sweep_id:
                    matching_new_runs.append(run_dir)
            except Exception as e:
                logger.debug(f"Error processing new run {run_dir.name}: {e}")
                continue

        return matching_new_runs

    def _is_run_finished(self, run_dir: Path) -> bool:
        """
        Check if a wandb run is finished (not still running).

        A run is considered finished if it has a wandb-summary.json file,
        which is written when the run completes.

        Args:
            run_dir: Path to wandb run directory

        Returns:
            True if run is finished, False if still running
        """
        # Check for wandb-summary.json in common locations
        summary_paths = [
            run_dir / "wandb-summary.json",
            run_dir / "files" / "wandb-summary.json",
        ]

        for summary_path in summary_paths:
            if summary_path.exists():
                # Additionally check file is not empty and has valid content
                try:
                    with open(summary_path) as f:
                        data = json.load(f)
                        # If we can load it and it has content, run is finished
                        if data:
                            return True
                except Exception:
                    continue

        # No valid summary file found, assume still running
        return False

    def _filter_runs_to_sync(
        self, run_dirs: List[Path], sweep_id: str, dry_run: bool = False
    ) -> List[Path]:
        """
        Filter runs to only those that need syncing.

        Three states are tracked independently:
        1. Cached metadata: Whether we know this run belongs to the sweep
        2. Run status: Whether the run is finished or still running
        3. Sync status: Whether we've successfully rsynced this run

        Skip logic:
        - Skip ONLY if: (finished=True AND synced=True)
        - Sync if: running OR (finished but not synced)

        Note: In dry-run mode, we still filter to show what WOULD be synced,
        but we don't actually sync or mark as synced.

        Args:
            run_dirs: All run directories for the sweep
            sweep_id: Sweep ID
            dry_run: If True, still filter but for display only

        Returns:
            Filtered list of runs that need syncing
        """
        cache = self._load_cache()
        sweep_cache = cache.get("sweeps", {}).get(sweep_id, {})
        run_status_cache = sweep_cache.get("run_status", {})
        synced_runs = sweep_cache.get("synced", {})

        runs_to_sync = []
        skipped_finished = 0
        running_count = 0
        finished_not_synced = 0

        for run_dir in run_dirs:
            run_name = run_dir.name

            # State 1: Cached metadata (already known - we have run_dir)

            # State 2: Run status (finished or running)
            # Use cached status if available, otherwise check now
            if run_name in run_status_cache:
                is_finished = run_status_cache[run_name].get("finished", False)
            else:
                is_finished = self._is_run_finished(run_dir)

            # State 3: Sync status
            is_synced = synced_runs.get(run_name, False)

            # Decision logic
            if is_finished and is_synced:
                # Skip: finished and already synced
                logger.debug(f"Skipping {run_name}: finished and already synced")
                skipped_finished += 1
            else:
                # Sync: either running OR not yet synced
                if not is_finished:
                    logger.debug(f"Including {run_name}: still running")
                    running_count += 1
                else:
                    logger.debug(f"Including {run_name}: finished but not synced")
                    finished_not_synced += 1
                runs_to_sync.append(run_dir)

        # Show summary of filtering decisions
        if skipped_finished > 0:
            self.console.print(
                f"  [dim]Skipping {skipped_finished} already-synced finished runs[/dim]"
            )
        if running_count > 0:
            self.console.print(f"  [cyan]Including {running_count} running runs[/cyan]")
        if finished_not_synced > 0:
            self.console.print(
                f"  [yellow]Including {finished_not_synced} finished "
                f"but not-yet-synced runs[/yellow]"
            )

        return runs_to_sync

    def _mark_runs_as_synced(self, sweep_id: str, run_dirs: List[Path]) -> None:
        """
        Mark runs as successfully synced in the cache (State 3: Sync status).

        IMPORTANT: This should ONLY be called after actual rsync, NOT during dry-run!

        Only marks runs that are finished - running runs should be
        re-synced next time to get updates.

        The three states:
        1. Cached metadata (sweep membership) - updated during any scan
        2. Run status (finished/running) - updated during any scan
        3. Sync status (rsynced) - ONLY updated after successful actual rsync

        Args:
            sweep_id: Sweep ID
            run_dirs: Run directories that were successfully synced
        """
        cache = self._load_cache()

        if "sweeps" not in cache:
            cache["sweeps"] = {}
        if sweep_id not in cache["sweeps"]:
            cache["sweeps"][sweep_id] = {"runs": [], "run_status": {}, "synced": {}}
        if "synced" not in cache["sweeps"][sweep_id]:
            cache["sweeps"][sweep_id]["synced"] = {}

        finished_synced = 0
        running_not_marked = 0

        for run_dir in run_dirs:
            is_finished = self._is_run_finished(run_dir)
            if is_finished:
                # Only mark finished runs as synced
                cache["sweeps"][sweep_id]["synced"][run_dir.name] = True
                logger.debug(f"Marked {run_dir.name} as synced")
                finished_synced += 1
            else:
                logger.debug(f"Not marking {run_dir.name} as synced (still running)")
                running_not_marked += 1

        if finished_synced > 0:
            self.console.print(
                f"  [green]Marked {finished_synced} finished runs as synced in cache[/green]"
            )
        if running_not_marked > 0:
            self.console.print(
                f"  [dim]Not caching {running_not_marked} running runs "
                f"(will re-sync next time)[/dim]"
            )

        self._save_cache()

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
        sweep_id: str,
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
        sweep_id: str,
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
