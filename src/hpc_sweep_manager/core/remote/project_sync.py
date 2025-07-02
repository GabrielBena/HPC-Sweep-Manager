"""Module for handling project state checking and synchronization with strict sync enforcement."""

import hashlib
import logging
from pathlib import Path
import subprocess
from typing import Any, Dict, List, Optional

from .discovery import RemoteConfig

logger = logging.getLogger(__name__)


class ProjectStateChecker:
    """
    Checks and enforces strict project synchronization between local and remote.

    Key principles:
    - Local changes always take priority
    - Only hsm_config.yaml is allowed to differ between local/remote
    - All config files must be identical for distributed execution
    - User confirmation required before overwriting remote files
    """

    def __init__(self, local_project_root: str, remote_config: RemoteConfig):
        self.local_project_root = Path(local_project_root)
        self.remote_config = remote_config

        # Files that are allowed to be different (source/remote specific)
        self.allowed_different_files = {
            "hsm_config.yaml",
            "sweeps/hsm_config.yaml",
            "./hsm_config.yaml",
            "./sweeps/hsm_config.yaml",
        }

    async def verify_and_enforce_sync(
        self, conn, auto_sync: bool = False, interactive: bool = True
    ) -> Dict[str, Any]:
        """
        Verify project sync and enforce strict synchronization if needed.

        Args:
            conn: SSH connection to remote
            auto_sync: If True, automatically sync without confirmation (dangerous!)
            interactive: If True, ask for user confirmation before syncing

        Returns:
            Dict with sync results and whether sync was successful
        """
        logger.info("🔍 Verifying strict project synchronization...")

        result = {
            "in_sync": False,
            "sync_performed": False,
            "files_synced": [],
            "files_skipped": [],
            "errors": [],
            "warnings": [],
        }

        try:
            # Step 1: Get list of all files that need to be checked
            files_to_check = await self._discover_all_project_files(conn)

            if not files_to_check:
                result["warnings"].append("No files found to check")
                result["in_sync"] = True
                return result

            logger.info(f"📁 Checking {len(files_to_check)} project files for sync...")

            # Step 2: Check each file for differences
            files_needing_sync = []
            files_checked = 0

            for file_path in files_to_check:
                try:
                    needs_sync = await self._check_file_needs_sync(conn, file_path)
                    if needs_sync:
                        # Check if this file is allowed to be different
                        if self._is_allowed_different_file(file_path):
                            logger.debug(f"⏭️  Skipping {file_path} (allowed to be different)")
                            result["files_skipped"].append(file_path)
                        else:
                            files_needing_sync.append(file_path)
                            logger.debug(f"❌ {file_path} needs sync")
                    else:
                        logger.debug(f"✅ {file_path} in sync")
                    files_checked += 1
                except Exception as e:
                    logger.warning(f"⚠️  Could not check {file_path}: {e}")
                    result["warnings"].append(f"Could not verify {file_path}: {e}")

            logger.info(
                f"📊 Sync check complete: {files_checked} files checked, {len(files_needing_sync)} need sync"
            )

            # Step 3: Handle files that need syncing
            if not files_needing_sync:
                logger.info("✅ All essential files are in sync!")
                result["in_sync"] = True
                return result

            # Log what files need syncing
            logger.warning(f"❌ {len(files_needing_sync)} files are out of sync:")
            for file_path in files_needing_sync:
                logger.warning(f"   • {file_path}")

            # Check if any config files are out of sync (critical!)
            config_files_needing_sync = [f for f in files_needing_sync if self._is_config_file(f)]
            if config_files_needing_sync:
                logger.error(
                    "🚨 CRITICAL: Config files are out of sync! This will cause inconsistent results."
                )
                logger.error("   Config files needing sync:")
                for config_file in config_files_needing_sync:
                    logger.error(f"   • {config_file}")
                logger.error("   Distributed execution cannot proceed with mismatched configs!")

            # Step 4: Offer to sync (local -> remote)
            if auto_sync:
                logger.info("🔄 Auto-sync enabled, syncing files...")
                should_sync = True
            elif interactive:
                should_sync = await self._confirm_sync_operation(
                    files_needing_sync, config_files_needing_sync
                )
            else:
                logger.error("❌ Sync required but not in interactive mode. Cannot proceed.")
                result["errors"].append("Sync required but not in interactive mode")
                return result

            if not should_sync:
                logger.error(
                    "❌ User declined sync operation. Cannot proceed with distributed execution."
                )
                result["errors"].append("User declined sync operation")
                return result

            # Step 5: Perform the sync (local -> remote)
            logger.info("🔄 Syncing local changes to remote (local takes priority)...")
            sync_success = await self._sync_files_to_remote(conn, files_needing_sync)

            if sync_success:
                logger.info("✅ Successfully synced all files to remote!")
                result["in_sync"] = True
                result["sync_performed"] = True
                result["files_synced"] = files_needing_sync
            else:
                logger.error("❌ Failed to sync files to remote")
                result["errors"].append("Failed to sync files to remote")

            return result

        except Exception as e:
            logger.error(f"💥 Error during sync verification: {e}")
            result["errors"].append(f"Sync verification failed: {e}")
            return result

    async def _discover_all_project_files(self, conn) -> List[str]:
        """Discover all project files that need to be kept in sync."""
        files_to_check = []

        try:
            # 1. Add training script
            if self.remote_config.train_script:
                train_script = self._make_relative_path(self.remote_config.train_script)
                if train_script:
                    files_to_check.append(train_script)

            # 2. Add all config files
            config_files = await self._discover_config_files(conn)
            files_to_check.extend(config_files)

            # 3. Add common Python project files
            common_files = [
                "requirements.txt",
                "pyproject.toml",
                "setup.py",
                "environment.yml",
                "conda.yml",
            ]

            for common_file in common_files:
                if (self.local_project_root / common_file).exists():
                    files_to_check.append(common_file)

            # 4. Add Python source files (recursively find .py files)
            python_files = await self._discover_python_files(conn)
            files_to_check.extend(python_files)

            # Remove duplicates while preserving order
            seen = set()
            files_to_check = [f for f in files_to_check if f and f not in seen and not seen.add(f)]

            logger.debug(f"Discovered {len(files_to_check)} files to check for sync")
            return files_to_check

        except Exception as e:
            logger.error(f"Error discovering project files: {e}")
            return []

    async def _discover_config_files(self, conn) -> List[str]:
        """Discover all configuration files in the project."""
        config_files = []

        if not self.remote_config.config_dir:
            return config_files

        try:
            config_dir_relative = self._make_relative_path(self.remote_config.config_dir)
            if not config_dir_relative:
                return config_files

            # Find all config files (.yaml, .yml, .json) in config directory
            find_cmd = f"""
            cd {self.remote_config.project_root}
            if [ -d "{config_dir_relative}" ]; then
                find "{config_dir_relative}" -type f \\( -name "*.yaml" -o -name "*.yml" -o -name "*.json" \\) 2>/dev/null || true
            fi
            """

            result = await conn.run(find_cmd, check=False)
            if result.returncode == 0 and result.stdout.strip():
                for config_file in result.stdout.strip().split("\n"):
                    config_file = config_file.strip()
                    if config_file and (self.local_project_root / config_file).exists():
                        config_files.append(config_file)

        except Exception as e:
            logger.debug(f"Error discovering config files: {e}")

        return config_files

    async def _discover_python_files(self, conn) -> List[str]:
        """Discover key Python source files."""
        python_files = []

        try:
            # Find Python files but exclude certain directories
            find_cmd = f"""
            cd {self.remote_config.project_root}
            find . -name "*.py" -type f \\
                ! -path "./.git/*" \\
                ! -path "./wandb/*" \\
                ! -path "./outputs/*" \\
                ! -path "./__pycache__/*" \\
                ! -path "./.*" \\
                -not -name ".*" \\
                2>/dev/null | head -50 | sed 's|^./||'
            """

            result = await conn.run(find_cmd, check=False)
            if result.returncode == 0 and result.stdout.strip():
                for py_file in result.stdout.strip().split("\n"):
                    py_file = py_file.strip()
                    if py_file and (self.local_project_root / py_file).exists():
                        python_files.append(py_file)

        except Exception as e:
            logger.debug(f"Error discovering Python files: {e}")

        return python_files

    async def _check_file_needs_sync(self, conn, file_path: str) -> bool:
        """Check if a specific file needs syncing."""
        try:
            # Check if file exists locally
            local_file = self.local_project_root / file_path
            if not local_file.exists():
                logger.debug(f"Local file {file_path} doesn't exist, skipping")
                return False

            # Calculate local file checksum
            local_checksum = self._calculate_file_checksum(local_file)

            # Calculate remote file checksum
            remote_checksum_cmd = f"""
            cd {self.remote_config.project_root}
            if [ -f "{file_path}" ]; then
                sha256sum "{file_path}" | cut -d' ' -f1
            else
                echo "FILE_NOT_FOUND"
            fi
            """

            result = await conn.run(remote_checksum_cmd, check=False)
            remote_checksum = result.stdout.strip()

            if remote_checksum == "FILE_NOT_FOUND":
                logger.debug(f"Remote file {file_path} doesn't exist, needs sync")
                return True
            elif local_checksum != remote_checksum:
                logger.debug(f"File {file_path} checksums differ, needs sync")
                return True
            else:
                return False

        except Exception as e:
            logger.warning(f"Error checking file {file_path}: {e}")
            return False

    async def _confirm_sync_operation(
        self, files_needing_sync: List[str], config_files: List[str]
    ) -> bool:
        """Ask user for confirmation before syncing files."""

        print("\n" + "=" * 80)
        print("🔄 PROJECT SYNC REQUIRED")
        print("=" * 80)
        print(f"📁 {len(files_needing_sync)} files need to be synced from local to remote")
        print(f"🎛️  {len(config_files)} of these are config files (CRITICAL for consistency)")
        print()

        if config_files:
            print("⚠️  CONFIG FILES OUT OF SYNC (this will cause inconsistent results!):")
            for config_file in config_files:
                print(f"   • {config_file}")
            print()

        print("📝 All files that will be synced:")
        for file_path in files_needing_sync:
            file_type = "CONFIG" if self._is_config_file(file_path) else "CODE"
            print(f"   • {file_path} ({file_type})")
        print()

        print("📋 SYNC OPERATION DETAILS:")
        print("   • Local files will OVERWRITE remote files")
        print("   • Remote backup will be created automatically")
        print("   • Only files shown above will be modified")
        print("   • hsm_config.yaml will be skipped (allowed to differ)")
        print()

        if config_files:
            print("⚠️  WARNING: Config mismatches will cause inconsistent results!")
            print("   This sync is ESSENTIAL for reliable distributed execution.")
            print()

        while True:
            response = input("🤔 Proceed with sync operation? [y/N]: ").strip().lower()
            if response in ["y", "yes"]:
                return True
            elif response in ["n", "no", ""]:
                return False
            else:
                print("Please enter 'y' for yes or 'n' for no.")

    async def _sync_files_to_remote(self, conn, files_to_sync: List[str]) -> bool:
        """Sync specified files from local to remote."""

        try:
            synced_count = 0
            failed_count = 0

            for file_path in files_to_sync:
                try:
                    # Create backup of remote file if it exists
                    backup_cmd = f"""
                    cd {self.remote_config.project_root}
                    if [ -f "{file_path}" ]; then
                        cp "{file_path}" "{file_path}.backup_$(date +%Y%m%d_%H%M%S)" 2>/dev/null || true
                    fi
                    """
                    await conn.run(backup_cmd, check=False)

                    # Ensure remote directory exists
                    remote_dir = str(Path(file_path).parent)
                    if remote_dir != ".":
                        mkdir_cmd = f"cd {self.remote_config.project_root} && mkdir -p {remote_dir}"
                        await conn.run(mkdir_cmd)

                    # Copy file using rsync for reliability
                    local_file = self.local_project_root / file_path
                    remote_file = (
                        f"{self.remote_config.host}:{self.remote_config.project_root}/{file_path}"
                    )

                    rsync_cmd = ["rsync", "-avz", "--progress", str(local_file), remote_file]

                    result = subprocess.run(rsync_cmd, capture_output=True, text=True, timeout=60)

                    if result.returncode == 0:
                        logger.info(f"✅ Synced: {file_path}")
                        synced_count += 1
                    else:
                        logger.error(f"❌ Failed to sync {file_path}: {result.stderr}")
                        failed_count += 1

                except Exception as e:
                    logger.error(f"❌ Error syncing {file_path}: {e}")
                    failed_count += 1

            logger.info(f"📊 Sync complete: {synced_count} synced, {failed_count} failed")
            return failed_count == 0

        except Exception as e:
            logger.error(f"💥 Error during file sync: {e}")
            return False

    def _make_relative_path(self, path: str) -> Optional[str]:
        """Convert absolute path to relative path from project root."""
        if not path:
            return None

        if path.startswith("/"):
            # Absolute path - make relative to project root if possible
            if self.remote_config.project_root:
                try:
                    path_obj = Path(path)
                    project_root_obj = Path(self.remote_config.project_root)
                    if path_obj.is_relative_to(project_root_obj):
                        return str(path_obj.relative_to(project_root_obj))
                except Exception:
                    pass
            return None
        else:
            # Already relative
            return path

    def _is_allowed_different_file(self, file_path: str) -> bool:
        """Check if a file is allowed to be different between local and remote."""
        # Normalize path for comparison
        normalized_path = file_path.replace("./", "").replace("\\", "/")

        for allowed_pattern in self.allowed_different_files:
            allowed_normalized = allowed_pattern.replace("./", "").replace("\\", "/")
            if normalized_path == allowed_normalized or normalized_path.endswith(
                f"/{allowed_normalized}"
            ):
                return True
        return False

    def _is_config_file(self, filename: str) -> bool:
        """Check if a file is a configuration file."""
        config_patterns = [
            "config",
            "Config",
            "CONFIG",
            ".yaml",
            ".yml",
            ".json",
            "hyperparams",
            "experiment",
            "train_config",
            "model_config",
        ]
        return any(pattern in filename for pattern in config_patterns)

    def _calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    # Legacy methods for backwards compatibility (simplified)
    async def verify_project_sync(self, conn) -> Dict[str, Any]:
        """Legacy method - now just calls the strict sync enforcement."""
        logger.warning("Using legacy verify_project_sync - consider using verify_and_enforce_sync")
        result = await self.verify_and_enforce_sync(conn, auto_sync=False, interactive=False)

        # Convert to legacy format
        return {
            "in_sync": result["in_sync"],
            "method": "strict_sync",
            "details": result,
            "warnings": result["warnings"],
            "errors": result["errors"],
        }

    def can_offer_sync(self, sync_result: Dict[str, Any]) -> bool:
        """Legacy method - always return True since we can always sync local -> remote."""
        return not sync_result["in_sync"]

    def is_minor_sync_issue(self, sync_result: Dict[str, Any]) -> bool:
        """Legacy method - always return False since we enforce strict sync."""
        return False
