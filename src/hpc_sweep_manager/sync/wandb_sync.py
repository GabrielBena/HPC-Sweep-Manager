"""Wandb runs syncing functionality with optimization."""

from concurrent.futures import ThreadPoolExecutor
import json
import logging
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from typing import Dict, List, Optional, Set, Tuple

from rich.console import Console
from rich.prompt import Confirm
import yaml

from .config import SyncTarget

logger = logging.getLogger(__name__)


def extract_version_from_filename(filename: str) -> Tuple[str, Optional[int]]:
    """
    Extract base filename and version from a versioned filename.

    Examples:
        'model:v3' -> ('model', 3)
        'artifact_v2.json' -> ('artifact.json', 2)
        'results_latest.csv' -> ('results.csv', float('inf'))
        'ood_accuracy_results_best_70_hash.table.json' ->
            ('ood_accuracy_results_best.table.json', 70)
        'val_accuracy_results_latest_33_hash.table.json' ->
            ('val_accuracy_results_latest.table.json', 33)
        'config.yaml' -> ('config.yaml', None)  # no version

    Args:
        filename: Name of the file

    Returns:
        Tuple of (base_filename, version_number)
    """
    # Handle wandb table format: metric_results_{best|latest}_number_hash.table.json
    wandb_table_match = re.match(
        r"^(.+_results_(?:best|latest))_(\d+)_[a-f0-9]+(\.\w+\.\w+)$", filename
    )
    if wandb_table_match:
        base_name = wandb_table_match.group(1)  # e.g., "ood_accuracy_results_best"
        version = int(wandb_table_match.group(2))  # e.g., 70
        ext = wandb_table_match.group(3)  # e.g., ".table.json"
        return (base_name + ext, version)

    # Handle wandb artifact format: name:v[number] or name:latest
    artifact_match = re.match(r"^(.+):(v(\d+)|latest)$", filename)
    if artifact_match:
        base_name = artifact_match.group(1)
        if artifact_match.group(2) == "latest":
            return (base_name, float("inf"))  # latest is treated as highest version
        else:
            version = int(artifact_match.group(3))
            return (base_name, version)

    # Handle _v[number] or _version[number] patterns
    version_patterns = [
        r"^(.+)_v(\d+)(\..+)?$",  # file_v2.ext
        r"^(.+)_version(\d+)(\..+)?$",  # file_version2.ext
        r"^(.+)\.(v\d+)\.(\w+)$",  # file.v2.ext
    ]

    for pattern in version_patterns:
        match = re.match(pattern, filename)
        if match:
            base_name = match.group(1)
            version_str = match.group(2)
            ext = match.group(3) if len(match.groups()) > 2 and match.group(3) else ""

            # Extract numeric version
            version_num_match = re.search(r"(\d+)", version_str)
            if version_num_match:
                version = int(version_num_match.group(1))
                # Reconstruct base filename without version
                if ext:
                    base_name += ext
                return (base_name, version)

    # Handle 'latest' suffix
    latest_patterns = [
        r"^(.+)_latest(\..+)?$",  # file_latest.ext
        r"^(.+)\.latest\.(\w+)$",  # file.latest.ext
    ]

    for pattern in latest_patterns:
        match = re.match(pattern, filename)
        if match:
            base_name = match.group(1)
            ext = match.group(2) if match.group(2) else ""
            if ext:
                base_name += ext
            return (base_name, float("inf"))  # latest treated as highest version

    # No version found
    return (filename, None)


def get_latest_versions(file_paths: List[Path]) -> Dict[str, Path]:
    """
    Filter a list of file paths to keep only the latest versions of each file.

    Args:
        file_paths: List of file paths to filter

    Returns:
        Dictionary mapping base filename to the path of its latest version
    """
    file_versions: Dict[str, List[Tuple[Path, Optional[int]]]] = {}

    # Group files by base name
    for file_path in file_paths:
        base_name, version = extract_version_from_filename(file_path.name)

        if base_name not in file_versions:
            file_versions[base_name] = []
        file_versions[base_name].append((file_path, version))

    # Select latest version for each base name
    latest_files = {}

    for base_name, versions in file_versions.items():
        if len(versions) == 1:
            # Only one version, keep it
            latest_files[base_name] = versions[0][0]
        else:
            # Multiple versions, find the latest
            # Separate versioned and unversioned files
            versioned = [(path, ver) for path, ver in versions if ver is not None]
            unversioned = [(path, ver) for path, ver in versions if ver is None]

            if versioned:
                # If we have versioned files, pick the highest version
                latest_path = max(versioned, key=lambda x: x[1])[0]
                latest_files[base_name] = latest_path
            elif unversioned:
                # If only unversioned files, pick the first one (arbitrary choice)
                latest_files[base_name] = unversioned[0][0]

    return latest_files


def create_filtered_file_list(run_dir: Path, latest_only: bool = False) -> List[Path]:
    """
    Create a list of files to sync from a run directory.

    Args:
        run_dir: Path to wandb run directory
        latest_only: If True, only include latest versions of files

    Returns:
        List of file paths to sync
    """
    if not latest_only:
        # Return all files
        all_files = []
        for file_path in run_dir.rglob("*"):
            if file_path.is_file():
                all_files.append(file_path)
        return all_files

    # Group files by directory and filter each directory separately
    files_to_sync = []

    # Process each subdirectory separately to maintain directory structure
    directories_to_process = [run_dir]

    # Add all subdirectories
    for item in run_dir.rglob("*"):
        if item.is_dir():
            directories_to_process.append(item)

    for directory in directories_to_process:
        # Get direct files in this directory (not recursive)
        direct_files = [f for f in directory.iterdir() if f.is_file()]

        if not direct_files:
            continue

        # Filter to latest versions within this directory
        latest_files = get_latest_versions(direct_files)
        files_to_sync.extend(latest_files.values())

    return files_to_sync


def create_rsync_file_list(files_to_sync: List[Path], run_dir: Path) -> str:
    """
    Create a temporary file containing the list of files to sync for rsync --files-from.

    Args:
        files_to_sync: List of absolute file paths to sync
        run_dir: Base run directory path

    Returns:
        Path to temporary file containing relative paths
    """
    # Create temporary file with context manager
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as temp_file:
        for file_path in files_to_sync:
            # Convert to relative path from run_dir
            try:
                rel_path = file_path.relative_to(run_dir)
                temp_file.write(str(rel_path) + "\n")
            except ValueError:
                # Path is not relative to run_dir, skip it
                logger.debug(f"Skipping file outside run directory: {file_path}")
                continue

        temp_file.flush()
        return temp_file.name


class WandbSyncer:
    """
    Handles syncing of wandb run directories with optimizations.

    Pre-Organized Directory Optimization:
    -------------------------------------
    If your wandb runs are organized in sweep-specific subdirectories:
      wandb/{sweep_id}/wandb/run-*

    The syncer automatically detects this and:
    - Skips ALL metadata scanning (much faster!)
    - Skips ALL caching (not needed)
    - Directly uses runs from the organized structure
    - Still applies sync status filtering (finished+synced runs)

    Example structure:
      wandb/
        sweep_20251020_212836/
          wandb/
            run-20251020_123456/
            run-20251020_123457/

    Three-State Caching System (for non-organized runs):
    -----------------------------------------------------
    When runs are NOT pre-organized, this syncer tracks three independent states:

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

        # Cache directory in .hsm folder - use per-sweep files for efficiency
        self._cache_dir = Path.cwd() / ".hsm" / "cache" / "wandb_sweeps"
        self._cache_dir.mkdir(parents=True, exist_ok=True)

        # Per-sweep cache (set when loading for a specific sweep)
        self._current_sweep_id: Optional[str] = None
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

    def clean_wandb_runs(
        self,
        sweep_id: str,
        dry_run: bool = False,
        confirm: bool = True,
    ) -> bool:
        """
        Clean (delete) local wandb runs for a sweep.

        Uses cached metadata to efficiently locate runs without full scan.

        Args:
            sweep_id: Sweep ID to clean runs for
            dry_run: If True, only show what would be deleted without actually deleting
            confirm: If True, prompt for confirmation before deletion (ignored in dry_run)

        Returns:
            True if successful, False otherwise
        """
        # Find wandb directory
        wandb_dir = self._find_wandb_dir()
        if not wandb_dir:
            self.console.print("[red]Error: Wandb directory not found[/red]")
            return False

        # Find runs belonging to this sweep (reuse cache)
        run_dirs = self._find_sweep_runs(wandb_dir, sweep_id)
        if not run_dirs:
            self.console.print(f"[yellow]No wandb runs found for sweep {sweep_id}[/yellow]")
            return True  # Not an error, just no runs

        # Calculate total size
        total_size = 0
        for run_dir in run_dirs:
            total_size += sum(f.stat().st_size for f in run_dir.rglob("*") if f.is_file())

        size_mb = total_size / (1024 * 1024)
        size_gb = total_size / (1024 * 1024 * 1024)

        # Display what will be deleted
        self.console.print(f"\n[bold]Found {len(run_dirs)} wandb runs for sweep {sweep_id}:[/bold]")
        for run_dir in run_dirs:
            run_size = sum(f.stat().st_size for f in run_dir.rglob("*") if f.is_file())
            run_size_mb = run_size / (1024 * 1024)
            self.console.print(f"  • {run_dir.name} ({run_size_mb:.1f} MB)")

        if size_gb >= 1:
            self.console.print(f"\n[bold]Total size: {size_gb:.2f} GB[/bold]")
        else:
            self.console.print(f"\n[bold]Total size: {size_mb:.1f} MB[/bold]")

        if dry_run:
            self.console.print("\n[yellow]DRY RUN: No files will be deleted[/yellow]")
            return True

        # Confirm deletion
        if confirm:
            self.console.print()
            confirmed = Confirm.ask(
                f"[bold red]Delete {len(run_dirs)} runs ({size_gb:.2f} GB)?[/bold red]",
                default=False,
            )
            if not confirmed:
                self.console.print("[yellow]Deletion cancelled[/yellow]")
                return False

        # Delete runs
        self.console.print(f"\n[bold]Deleting {len(run_dirs)} runs...[/bold]")
        deleted = 0
        failed = 0

        for run_dir in run_dirs:
            try:
                shutil.rmtree(run_dir)
                self.console.print(f"  [green]✓ Deleted {run_dir.name}[/green]")
                deleted += 1
            except Exception as e:
                self.console.print(f"  [red]✗ Failed to delete {run_dir.name}: {e}[/red]")
                logger.error(f"Failed to delete {run_dir.name}: {e}")
                failed += 1

        # Summary
        self.console.print()
        if deleted > 0:
            self.console.print(f"[green]Successfully deleted {deleted} runs[/green]")
        if failed > 0:
            self.console.print(f"[red]Failed to delete {failed} runs[/red]")

        # Clear cache for this sweep since runs are deleted
        if deleted > 0:
            self.console.print("[dim]Clearing cache for deleted sweep...[/dim]")
            self.clear_cache(sweep_id)

        return failed == 0

    def _find_wandb_dir(self) -> Optional[Path]:
        """Find the wandb directory."""
        search_paths = [
            Path.cwd() / "wandb",
            Path.cwd() / "outputs" / "wandb",
            Path("wandb"),
        ]

        for path in search_paths:
            if path.exists() and path.is_dir():
                # Check if it contains run directories (either directly or in subdirs)
                run_dirs = list(path.glob("run-*"))
                # Also check for sweep-organized structure: wandb/sweep_id/wandb/run-*
                sweep_subdirs = list(path.glob("*/wandb/run-*"))

                if run_dirs or sweep_subdirs:
                    logger.debug(f"Found wandb directory: {path}")
                    return path

        return None

    def _find_sweep_runs(
        self, wandb_dir: Path, sweep_id: str, force_refresh: bool = False
    ) -> List[Path]:
        """
        Find all wandb runs belonging to a sweep.

        Uses optimized detection if runs are pre-organized in sweep subdirectories:
          wandb/{sweep_id}/wandb/run-*

        Otherwise falls back to cached metadata scanning.

        Args:
            wandb_dir: Path to wandb directory
            sweep_id: Sweep ID to filter by
            force_refresh: If True, ignore cache and rescan all runs

        Returns:
            List of run directory paths belonging to the sweep
        """
        # OPTIMIZATION: Check for pre-organized sweep directory structure
        # Structure: wandb/{sweep_id}/wandb/run-*
        sweep_wandb_dir = wandb_dir / sweep_id / "wandb"

        if sweep_wandb_dir.exists() and sweep_wandb_dir.is_dir():
            # Found pre-organized structure - no metadata scanning needed!
            run_dirs = list(sweep_wandb_dir.glob("run-*"))
            run_dirs = [d for d in run_dirs if d.is_dir()]

            if run_dirs:
                self.console.print(
                    f"  [green]Found pre-organized sweep directory: {sweep_id}/wandb/[/green]"
                )
                self.console.print(
                    f"  [green]Skipping metadata scan - using {len(run_dirs)} runs "
                    f"from organized structure[/green]"
                )
                logger.info(
                    f"Using pre-organized structure for {sweep_id}, found {len(run_dirs)} runs"
                )
                return run_dirs

        # Fall back to cache-based metadata scanning
        self.console.print("  [dim]No pre-organized sweep directory found, using cache...[/dim]")
        cache = self._load_cache(sweep_id)

        # Check if we have cached results for this sweep
        run_names = cache.get("runs", [])
        if not force_refresh and run_names:
            self.console.print(f"  [cyan]Found cached data for sweep {sweep_id}[/cyan]")

            self.console.print(f"  [dim]Validating {len(run_names)} cached runs...[/dim]")

            # Validate cached runs still exist
            cached_runs = [wandb_dir / name for name in run_names if (wandb_dir / name).exists()]

            removed = len(run_names) - len(cached_runs)
            if removed > 0:
                logger.debug(f"Removed {removed} non-existent runs from cache")
                self.console.print(f"  [dim]Removed {removed} deleted runs from cache[/dim]")

            logger.debug(f"Loaded {len(cached_runs)} valid runs from cache for sweep {sweep_id}")
            self.console.print(f"  [green]Loaded {len(cached_runs)} runs from cache[/green]")

            # Check for new runs incrementally
            self.console.print("  [dim]Checking for new runs...[/dim]")
            new_runs = self._find_new_runs(wandb_dir, sweep_id, cached_runs)

            if new_runs:
                logger.debug(f"Found {len(new_runs)} new runs for sweep {sweep_id}")
                self.console.print(
                    f"  [cyan]Found {len(new_runs)} new runs, updating cache...[/cyan]"
                )
                all_runs = cached_runs + new_runs
                self._update_cache_for_sweep(sweep_id, all_runs)
                return all_runs
            else:
                self.console.print("  [dim]No new runs found[/dim]")

            return cached_runs

        # Full scan if no cache or force refresh
        if force_refresh:
            self.console.print("  [yellow]Force refresh enabled, performing full scan...[/yellow]")
        else:
            self.console.print(
                f"  [yellow]No cache for {sweep_id}, performing full scan...[/yellow]"
            )
        logger.debug(f"Performing full scan for sweep {sweep_id}")
        all_runs = self._scan_all_runs_for_sweep(wandb_dir, sweep_id)

        # Update cache
        self.console.print(f"  [cyan]Updating cache with {len(all_runs)} runs...[/cyan]")
        self._update_cache_for_sweep(sweep_id, all_runs)

        return all_runs

    def _get_cache_file(self, sweep_id: str) -> Path:
        """Get the cache file path for a specific sweep."""
        # Sanitize sweep_id for filename
        safe_sweep_id = sweep_id.replace("/", "_").replace("\\", "_")
        return self._cache_dir / f"{safe_sweep_id}.json"

    def _load_cache(self, sweep_id: str) -> Dict:
        """Load cache for a specific sweep from disk."""
        # If already loaded for this sweep, return it
        if self._cache is not None and self._current_sweep_id == sweep_id:
            logger.debug(f"Using already-loaded cache for {sweep_id}")
            return self._cache

        cache_file = self._get_cache_file(sweep_id)

        if not cache_file.exists():
            logger.debug(f"No cache file found for {sweep_id}, creating new cache")
            self.console.print("  [dim]No cache found, will perform full scan[/dim]")
            self._cache = {
                "sweep_id": sweep_id,
                "runs": [],
                "run_status": {},
                "synced": {},
                "version": 1,
            }
            self._current_sweep_id = sweep_id
            return self._cache

        try:
            logger.debug(f"Loading cache from {cache_file}")
            self.console.print(f"  [dim]Loading cache for {sweep_id}...[/dim]")
            with open(cache_file) as f:
                self._cache = json.load(f)
                run_count = len(self._cache.get("runs", []))
                logger.debug(f"Loaded cache with {run_count} runs for {sweep_id}")
                self.console.print(f"  [dim]Cache loaded ({run_count} runs)[/dim]")
                self._current_sweep_id = sweep_id
                return self._cache
        except Exception as e:
            logger.warning(f"Failed to load cache for {sweep_id}: {e}, starting fresh")
            self.console.print("  [yellow]Failed to load cache, starting fresh[/yellow]")
            self._cache = {
                "sweep_id": sweep_id,
                "runs": [],
                "run_status": {},
                "synced": {},
                "version": 1,
            }
            self._current_sweep_id = sweep_id
            return self._cache

    def _save_cache(self) -> None:
        """Save cache to disk for current sweep."""
        if self._cache is None or self._current_sweep_id is None:
            return

        cache_file = self._get_cache_file(self._current_sweep_id)

        try:
            with open(cache_file, "w") as f:
                json.dump(self._cache, f, indent=2)
            logger.debug(f"Cache saved to {cache_file}")
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")

    def clear_cache(self, sweep_id: Optional[str] = None) -> None:
        """
        Clear the wandb run cache.

        Args:
            sweep_id: If provided, only clear cache for this sweep.
                     If None, clear entire cache directory.
        """
        if sweep_id:
            # Clear cache for specific sweep
            cache_file = self._get_cache_file(sweep_id)
            if cache_file.exists():
                cache_file.unlink()
                self.console.print(f"[green]Cleared cache for sweep {sweep_id}[/green]")
                # Clear in-memory cache if it matches
                if self._current_sweep_id == sweep_id:
                    self._cache = None
                    self._current_sweep_id = None
            else:
                self.console.print(f"[yellow]No cache found for sweep {sweep_id}[/yellow]")
        else:
            # Clear entire cache directory
            import shutil

            if self._cache_dir.exists():
                shutil.rmtree(self._cache_dir)
                self._cache_dir.mkdir(parents=True, exist_ok=True)
                self._cache = None
                self._current_sweep_id = None
                self.console.print("[green]Cleared entire wandb sync cache[/green]")
            else:
                self.console.print("[yellow]No cache directory found[/yellow]")

    def _update_cache_for_sweep(self, sweep_id: str, run_dirs: List[Path]) -> None:
        """
        Update cache with run directories and their status for a sweep.

        This stores:
        - Which runs belong to the sweep (metadata scan result)
        - Whether each run is finished or running
        - Which runs have been successfully synced (separate from this method)
        """
        cache = self._load_cache(sweep_id)

        # Preserve existing sync status
        existing_synced = cache.get("synced", {})

        # Build run status map
        run_status = {}
        for run_dir in run_dirs:
            is_finished = self._is_run_finished(run_dir)
            run_status[run_dir.name] = {
                "finished": is_finished,
                "last_checked": None,  # Could add timestamp here if needed
            }

        # Update cache
        cache["runs"] = [r.name for r in run_dirs]
        cache["run_status"] = run_status
        cache["synced"] = existing_synced  # Preserve sync status

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
            run_dir / "wandb-config.yaml",
            run_dir / "files" / "wandb-config.yaml",
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

                        # For debugging: log what we found
                        if target_sweep_id:
                            logger.debug(f"Run {run_dir.name}: Found {key} = '{value_str}'")

                        # Try exact match first
                        if value_str == target_sweep_id:
                            return value_str

                        # For group field, try more flexible matching
                        if key == "group" and target_sweep_id:
                            # Check if target sweep is contained in the group value
                            if target_sweep_id in value_str:
                                return target_sweep_id
                            # Check if group value is contained in target sweep
                            if value_str in target_sweep_id:
                                return value_str

                        # Return any non-empty value as potential sweep ID
                        if not target_sweep_id:
                            return value_str

                # Special handling for wandb-metadata.json args format
                if metadata_path.name == "wandb-metadata.json" and "args" in metadata:
                    args_list = metadata["args"]
                    if isinstance(args_list, list):
                        for arg in args_list:
                            if isinstance(arg, str) and "wandb.group=" in arg:
                                group_value = arg.split("wandb.group=")[1]
                                if target_sweep_id:
                                    logger.debug(
                                        f"Run {run_dir.name}: "
                                        f"Found wandb.group in args = '{group_value}'"
                                    )
                                    if (
                                        group_value == target_sweep_id
                                        or target_sweep_id in group_value
                                    ):
                                        return target_sweep_id
                                else:
                                    return group_value

            except Exception as e:
                logger.debug(f"Error reading {metadata_path}: {e}")
                continue

        # If no sweep ID found in metadata, try to extract from run name or group
        # This is a fallback method
        try:
            summary_path = run_dir / "wandb-summary.json"
            if summary_path.exists():
                with open(summary_path) as f:
                    summary = json.load(f)
                    # Sometimes the run name contains the sweep ID
                    run_name = summary.get("_wandb", {}).get("run_name", "")
                    if "sweep_" in run_name:
                        # Extract sweep ID pattern
                        match = re.search(r"sweep_\w+", run_name)
                        if match:
                            found_sweep = match.group()
                            if target_sweep_id and target_sweep_id in found_sweep:
                                return target_sweep_id
                            return found_sweep
        except Exception:
            pass

        return None

    def _scan_all_runs_for_sweep(self, wandb_dir: Path, sweep_id: str) -> List[Path]:
        """
        Scan all run directories to find those belonging to a sweep.

        Uses date filtering as an optimization when possible.
        Parallelizes metadata scanning for faster processing.

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

        # Filter to only directories
        run_dirs = [d for d in run_dirs if d.is_dir()]

        if not run_dirs:
            return []

        # Parallelize metadata scanning for speed
        logger.debug(f"Scanning {len(run_dirs)} runs in parallel...")
        matching_runs = []

        def check_run(run_dir: Path) -> Optional[Path]:
            """Check if run belongs to sweep, return path if it does."""
            try:
                run_sweep_id = self._extract_sweep_id_from_run(run_dir, sweep_id)
                if run_sweep_id:
                    return run_dir
            except Exception as e:
                logger.debug(f"Error processing run {run_dir.name}: {e}")
            return None

        # Use ThreadPoolExecutor for parallel I/O
        with ThreadPoolExecutor(max_workers=min(20, len(run_dirs))) as executor:
            results = executor.map(check_run, run_dirs)
            matching_runs = [r for r in results if r is not None]

        logger.debug(f"Found {len(matching_runs)} runs for sweep {sweep_id}")
        return matching_runs

    def _find_new_runs(
        self, wandb_dir: Path, sweep_id: str, existing_runs: List[Path]
    ) -> List[Path]:
        """
        Find new runs for a sweep that aren't in the existing list.

        Only checks recently modified run directories for efficiency.
        Parallelizes metadata scanning for faster processing.

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

        logger.debug(f"Checking {len(new_run_dirs)} new runs in parallel...")

        # Check which new runs belong to this sweep (parallelized)
        def check_run(run_dir: Path) -> Optional[Path]:
            """Check if run belongs to sweep, return path if it does."""
            try:
                run_sweep_id = self._extract_sweep_id_from_run(run_dir, sweep_id)
                if run_sweep_id:
                    return run_dir
            except Exception as e:
                logger.debug(f"Error processing new run {run_dir.name}: {e}")
            return None

        # Use ThreadPoolExecutor for parallel I/O
        with ThreadPoolExecutor(max_workers=min(20, len(new_run_dirs))) as executor:
            results = executor.map(check_run, new_run_dirs)
            matching_new_runs = [r for r in results if r is not None]

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
        cache = self._load_cache(sweep_id)
        run_status_cache = cache.get("run_status", {})
        synced_runs = cache.get("synced", {})

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
        cache = self._load_cache(sweep_id)

        if "synced" not in cache:
            cache["synced"] = {}

        finished_synced = 0
        running_not_marked = 0

        for run_dir in run_dirs:
            is_finished = self._is_run_finished(run_dir)
            if is_finished:
                # Only mark finished runs as synced
                cache["synced"][run_dir.name] = True
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
        file_list_path = None
        try:
            rsync_cmd = [
                "rsync",
                "-az",  # archive, compress (no verbose for parallel)
                "--update",  # Skip files that are newer on receiver
            ]

            if dry_run:
                rsync_cmd.append("--dry-run")

            if latest_only:
                # Create filtered file list instead of using exclude patterns
                # This properly handles versioned files (including WandB tables with hashes)
                files_to_sync = create_filtered_file_list(run_dir, latest_only=True)
                file_list_path = create_rsync_file_list(files_to_sync, run_dir)

                # Use --files-from to sync only the filtered files
                rsync_cmd.extend(
                    [
                        "--files-from",
                        file_list_path,
                        "--relative",  # Preserve directory structure
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
        finally:
            # Clean up temporary file list
            if file_list_path and Path(file_list_path).exists():
                try:
                    Path(file_list_path).unlink()
                except Exception as e:
                    logger.debug(f"Failed to clean up temp file {file_list_path}: {e}")
