"""Project synchronization manager for ensuring local and remote projects are in sync.

This module provides functionality to verify that local and remote projects are
synchronized before running distributed sweeps, and can automatically sync
mismatched files when safe to do so.
"""

import asyncio
from datetime import datetime
import hashlib
import logging
from pathlib import Path
import subprocess
import tempfile
from typing import Any, Dict, List, Optional, Union

try:
    import asyncssh

    ASYNCSSH_AVAILABLE = True
except ImportError:
    ASYNCSSH_AVAILABLE = False

logger = logging.getLogger(__name__)


class ProjectSyncError(Exception):
    """Raised when project synchronization fails."""

    pass


class ProjectStateChecker:
    """Checks if local and remote projects are in the same state."""

    def __init__(
        self,
        local_project_root: Union[str, Path],
        remote_host: str,
        remote_project_root: str,
        ssh_key: Optional[str] = None,
        ssh_port: int = 22,
    ):
        """Initialize the project state checker.

        Args:
            local_project_root: Path to local project root
            remote_host: Remote host address
            remote_project_root: Path to remote project root
            ssh_key: Optional SSH key path
            ssh_port: SSH port (default 22)
        """
        self.local_project_root = Path(local_project_root)
        self.remote_host = remote_host
        self.remote_project_root = remote_project_root
        self.ssh_key = ssh_key
        self.ssh_port = ssh_port

    async def verify_project_sync(self, conn=None) -> Dict[str, Any]:
        """Verify that local and remote projects are in sync.

        Args:
            conn: Optional existing SSH connection

        Returns:
            Dict with verification results and details
        """
        logger.info("Verifying local and remote project synchronization...")

        result = {
            "in_sync": False,
            "method": None,
            "details": {},
            "warnings": [],
            "errors": [],
        }

        # Use provided connection or create new one
        if conn is None:
            if not ASYNCSSH_AVAILABLE:
                raise ProjectSyncError("asyncssh is required for remote sync verification")

            async with await self._create_ssh_connection() as conn:
                return await self._perform_verification(conn, result)
        else:
            return await self._perform_verification(conn, result)

    async def _perform_verification(self, conn, result: Dict[str, Any]) -> Dict[str, Any]:
        """Perform the actual verification with an established connection."""
        # Try git-based verification first
        git_result = await self._verify_git_sync(conn)
        if git_result["available"]:
            result.update(
                {
                    "in_sync": git_result["in_sync"],
                    "method": "git",
                    "details": git_result,
                }
            )

            if git_result["in_sync"]:
                logger.info("✓ Projects are in sync (git verification)")
                return result
            else:
                logger.warning("⚠ Projects are NOT in sync (git verification)")
                for warning in git_result.get("warnings", []):
                    logger.warning(f"  {warning}")
                for error in git_result.get("errors", []):
                    logger.error(f"  {error}")

        # Fall back to file checksum verification
        logger.info("Falling back to file checksum verification...")
        checksum_result = await self._verify_checksum_sync(conn)
        result.update(
            {
                "in_sync": checksum_result["in_sync"],
                "method": "checksum",
                "details": checksum_result,
            }
        )

        if checksum_result["in_sync"]:
            logger.info("✓ Projects appear to be in sync (checksum verification)")
        else:
            logger.warning("⚠ Projects may not be in sync (checksum verification)")
            for warning in checksum_result.get("warnings", []):
                logger.warning(f"  {warning}")

        return result

    async def _create_ssh_connection(self):
        """Create SSH connection to remote host."""
        options = {}
        if self.ssh_key:
            options["client_keys"] = [self.ssh_key]

        return await asyncssh.connect(self.remote_host, port=self.ssh_port, **options)

    async def _verify_git_sync(self, conn) -> Dict[str, Any]:
        """Verify using git status and commit hashes."""
        result = {
            "available": False,
            "in_sync": False,
            "local_commit": None,
            "remote_commit": None,
            "local_status": None,
            "remote_status": None,
            "warnings": [],
            "errors": [],
        }

        try:
            # Check if local project is a git repo
            local_git_result = subprocess.run(
                ["git", "rev-parse", "--git-dir"],
                cwd=self.local_project_root,
                capture_output=True,
                text=True,
                timeout=10,
            )

            if local_git_result.returncode != 0:
                logger.debug("Local project is not a git repository")
                return result

            # Check if remote project is a git repo
            remote_git_check = await conn.run(
                f"cd {self.remote_project_root} && git rev-parse --git-dir",
                check=False,
            )

            if remote_git_check.returncode != 0:
                logger.debug("Remote project is not a git repository")
                return result

            result["available"] = True

            # Get local git status
            local_status = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=self.local_project_root,
                capture_output=True,
                text=True,
                timeout=10,
            )
            result["local_status"] = local_status.stdout.strip()

            # Get local commit hash
            local_commit = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=self.local_project_root,
                capture_output=True,
                text=True,
                timeout=10,
            )
            result["local_commit"] = local_commit.stdout.strip()

            # Get remote git status
            remote_status = await conn.run(
                f"cd {self.remote_project_root} && git status --porcelain"
            )
            result["remote_status"] = remote_status.stdout.strip()

            # Get remote commit hash
            remote_commit = await conn.run(f"cd {self.remote_project_root} && git rev-parse HEAD")
            result["remote_commit"] = remote_commit.stdout.strip()

            # Analyze sync status
            commits_match = result["local_commit"] == result["remote_commit"]
            has_local_changes = bool(result["local_status"])
            has_remote_changes = bool(result["remote_status"])

            # Parse changed files to understand the nature of changes
            local_files = (
                self._parse_git_status_files(result.get("local_status", ""))
                if has_local_changes
                else []
            )
            remote_files = (
                self._parse_git_status_files(result.get("remote_status", ""))
                if has_remote_changes
                else []
            )

            local_config_files = [f for f in local_files if self._is_config_file(f)]
            remote_config_files = [f for f in remote_files if self._is_config_file(f)]

            # Determine sync status
            if commits_match and not has_local_changes and not has_remote_changes:
                result["in_sync"] = True
            elif commits_match and has_local_changes and not has_remote_changes:
                result["in_sync"] = False
                result["errors"].append("Local uncommitted changes detected - sync required")
            elif commits_match and not has_local_changes and has_remote_changes:
                if len(remote_config_files) == len(remote_files) and len(remote_files) > 0:
                    result["in_sync"] = False
                    result["warnings"].append(
                        "Remote has config changes (likely from previous sync)"
                    )
                else:
                    result["in_sync"] = False
                    result["errors"].append("Remote uncommitted changes detected")
            elif commits_match and has_local_changes and has_remote_changes:
                files_overlap = set(local_files) & set(remote_files)
                overlap_ratio = len(files_overlap) / max(len(remote_files), 1)

                if (
                    len(remote_config_files) == len(remote_files)
                    and len(local_config_files) > 0
                    and overlap_ratio >= 0.5
                ):
                    result["in_sync"] = False
                    result["warnings"].append(
                        "Both sides have config changes - iterative development detected"
                    )
                else:
                    result["in_sync"] = False
                    result["errors"].append("Both local and remote have uncommitted changes")
            else:
                result["in_sync"] = False
                if not commits_match:
                    result["errors"].append(
                        f"Git commits differ: local={result['local_commit'][:8]} vs remote={result['remote_commit'][:8]}"
                    )

        except subprocess.TimeoutExpired:
            result["errors"].append("Git command timeout")
        except Exception as e:
            result["errors"].append(f"Git verification failed: {e}")

        return result

    async def _verify_checksum_sync(self, conn) -> Dict[str, Any]:
        """Verify using file checksums of key project files."""
        result = {
            "in_sync": True,
            "checked_files": [],
            "mismatched_files": [],
            "missing_files": [],
            "warnings": [],
        }

        # Key files to check
        key_files = [
            "requirements.txt",
            "pyproject.toml",
            "setup.py",
            "config.yaml",
            "config.yml",
            "configs/config.yaml",
            "configs/config.yml",
            "config/config.yaml",
            "train_config.yaml",
            "model_config.yaml",
            "experiment_config.yaml",
            "hyperparameters.yaml",
        ]

        # Add any .yaml/.yml files in common config directories
        for config_dir in ["configs", "config", "experiments"]:
            config_path = self.local_project_root / config_dir
            if config_path.exists():
                for config_file in config_path.rglob("*.yaml"):
                    rel_path = config_file.relative_to(self.local_project_root)
                    key_files.append(str(rel_path))
                for config_file in config_path.rglob("*.yml"):
                    rel_path = config_file.relative_to(self.local_project_root)
                    key_files.append(str(rel_path))

        # Remove duplicates
        key_files = list(dict.fromkeys(key_files))

        logger.info(f"Checking sync for {len(key_files)} key files")

        for file_path in key_files:
            if not file_path:
                continue

            try:
                local_file = self.local_project_root / file_path
                if not local_file.exists():
                    continue

                local_checksum = self._calculate_file_checksum(local_file)

                # Calculate remote file checksum
                remote_checksum_cmd = f"""
                cd {self.remote_project_root}
                if [ -f "{file_path}" ]; then
                    sha256sum "{file_path}" | cut -d' ' -f1
                else
                    echo "FILE_NOT_FOUND"
                fi
                """

                remote_result = await conn.run(remote_checksum_cmd)
                remote_checksum = remote_result.stdout.strip()

                if remote_checksum == "FILE_NOT_FOUND":
                    result["missing_files"].append(file_path)
                    result["in_sync"] = False
                elif local_checksum != remote_checksum:
                    result["mismatched_files"].append(
                        {
                            "file": file_path,
                            "local_checksum": local_checksum,
                            "remote_checksum": remote_checksum,
                        }
                    )
                    result["in_sync"] = False
                else:
                    result["checked_files"].append(file_path)

            except Exception as e:
                result["warnings"].append(f"Could not verify {file_path}: {e}")

        return result

    def _parse_git_status_files(self, git_status: str) -> List[str]:
        """Parse git status output to extract list of changed files."""
        files = []
        for line in git_status.split("\n"):
            if not line.strip():
                continue
            # Git status format: XY filename
            if len(line) > 3:
                filename = line[3:].strip()
                if filename:
                    files.append(filename)
        return files

    def _is_config_file(self, filename: str) -> bool:
        """Check if a file is a configuration file."""
        config_patterns = [
            "config",
            "Config",
            "CONFIG",
            ".yaml",
            ".yml",
            ".json",
            "requirements.txt",
            "pyproject.toml",
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


class ProjectSyncManager:
    """Manages project synchronization between local and remote environments."""

    def __init__(self, local_project_root: Union[str, Path]):
        """Initialize the sync manager.

        Args:
            local_project_root: Path to local project root
        """
        self.local_project_root = Path(local_project_root)

    async def sync_files_to_remote(
        self, conn, sync_result: Dict[str, Any], remote_project_root: str
    ) -> bool:
        """Sync specific files from local to remote.

        Args:
            conn: SSH connection to remote
            sync_result: Result from project sync verification
            remote_project_root: Remote project root path

        Returns:
            True if sync successful, False otherwise
        """
        try:
            if sync_result["method"] == "checksum":
                return await self._sync_checksum_files(conn, sync_result, remote_project_root)
            elif sync_result["method"] == "git":
                return await self._sync_git_files(conn, sync_result, remote_project_root)
            return False
        except Exception as e:
            logger.error(f"Error during file sync: {e}")
            return False

    async def _sync_checksum_files(
        self, conn, sync_result: Dict[str, Any], remote_project_root: str
    ) -> bool:
        """Sync files identified by checksum mismatch."""
        details = sync_result["details"]
        mismatched_files = details.get("mismatched_files", [])

        for mismatch in mismatched_files:
            file_path = mismatch["file"]
            local_file = self.local_project_root / file_path

            if not local_file.exists():
                logger.warning(f"Local file not found: {file_path}")
                continue

            logger.info(f"Syncing: {file_path}")

            # Read local file content
            with open(local_file, encoding="utf-8") as f:
                content = f.read()

            # Write to remote using a here-document to handle special characters
            remote_file_path = f"{remote_project_root}/{file_path}"

            # Ensure remote directory exists
            remote_dir = str(Path(remote_file_path).parent)
            await conn.run(f"mkdir -p {remote_dir}")

            # Write file content to remote
            cmd = f"""cat > {remote_file_path} << 'HSMSYNCEOF'
{content}
HSMSYNCEOF"""

            result = await conn.run(cmd, check=False)
            if result.returncode != 0:
                logger.error(f"Failed to sync {file_path}: {result.stderr}")
                return False

        logger.info(f"✓ Successfully synced {len(mismatched_files)} files to remote")
        return True

    async def _sync_git_files(
        self, conn, sync_result: Dict[str, Any], remote_project_root: str
    ) -> bool:
        """Sync files identified by git status."""
        details = sync_result["details"]
        local_status = details.get("local_status", "")

        if not local_status:
            logger.warning("No git changes detected to sync")
            return True

        # Parse git status to get list of changed files
        changed_files = []
        for line in local_status.split("\n"):
            if not line.strip():
                continue

            # Git status format: XY filename
            status = line[:2]
            filename = line[3:] if len(line) > 3 else ""

            # Only sync modified and added files (not deleted)
            if status.strip() in ["M", "MM", "A", "AM", "??"]:
                changed_files.append(filename)

        if not changed_files:
            logger.warning("No syncable files found in git changes")
            return True

        logger.info(f"Syncing {len(changed_files)} changed files to remote")

        # Sync each changed file
        sync_success = True
        for file_path in changed_files:
            try:
                local_file = self.local_project_root / file_path

                if not local_file.exists():
                    logger.warning(f"Local file not found: {file_path}")
                    continue

                logger.info(f"Syncing: {file_path}")

                # Read local file content
                with open(local_file, encoding="utf-8", errors="replace") as f:
                    content = f.read()

                # Write to remote
                remote_file_path = f"{remote_project_root}/{file_path}"
                remote_dir = str(Path(remote_file_path).parent)
                await conn.run(f"mkdir -p '{remote_dir}'")

                cmd = f"""cat > '{remote_file_path}' << 'HSMSYNCEOF'
{content}
HSMSYNCEOF"""

                result = await conn.run(cmd, check=False)
                if result.returncode != 0:
                    logger.error(f"Failed to sync {file_path}: {result.stderr}")
                    sync_success = False
                else:
                    logger.debug(f"✓ Successfully synced {file_path}")

            except Exception as e:
                logger.error(f"Error syncing {file_path}: {e}")
                sync_success = False

        if sync_success:
            logger.info(f"✓ Successfully synced all {len(changed_files)} git changes to remote")
        else:
            logger.error("Some files failed to sync")

        return sync_success

    async def reverse_sync_from_remote(
        self, conn, sync_result: Dict[str, Any], remote_project_root: str
    ) -> bool:
        """Sync remote changes to local (reverse sync).

        Args:
            conn: SSH connection to remote
            sync_result: Result from project sync verification
            remote_project_root: Remote project root path

        Returns:
            True if reverse sync successful, False otherwise
        """
        try:
            if sync_result["method"] == "git":
                return await self._reverse_sync_git_files(conn, sync_result, remote_project_root)
            elif sync_result["method"] == "checksum":
                return await self._reverse_sync_checksum_files(
                    conn, sync_result, remote_project_root
                )
            return False
        except Exception as e:
            logger.error(f"Error during reverse sync: {e}")
            return False

    async def _reverse_sync_git_files(
        self, conn, sync_result: Dict[str, Any], remote_project_root: str
    ) -> bool:
        """Reverse sync files from git status."""
        logger.info("Performing reverse sync: bringing remote changes to local...")
        details = sync_result["details"]
        remote_status = details.get("remote_status", "")

        if not remote_status:
            logger.warning("No remote changes detected to sync")
            return True

        # Parse remote git status to get list of changed files
        changed_files = []
        for line in remote_status.split("\n"):
            if not line.strip():
                continue

            status = line[:2]
            filename = line[3:] if len(line) > 3 else ""

            # Only sync modified and added files (not deleted)
            if status.strip() in ["M", "MM", "A", "AM", "??"]:
                changed_files.append(filename)

        if not changed_files:
            logger.warning("No syncable files found in remote changes")
            return True

        logger.info(f"Bringing {len(changed_files)} changed files from remote to local")

        # Sync each changed file from remote to local
        sync_success = True
        for file_path in changed_files:
            try:
                logger.info(f"Reverse syncing: {file_path}")

                # Get remote file content
                remote_file_path = f"{remote_project_root}/{file_path}"
                get_content_cmd = f"cat '{remote_file_path}'"

                remote_result = await conn.run(get_content_cmd, check=False)
                if remote_result.returncode != 0:
                    logger.error(f"Failed to read remote file {file_path}: {remote_result.stderr}")
                    sync_success = False
                    continue

                # Write to local file
                local_file = self.local_project_root / file_path
                local_file.parent.mkdir(parents=True, exist_ok=True)

                with open(local_file, "w", encoding="utf-8") as f:
                    f.write(remote_result.stdout)

                logger.debug(f"✓ Successfully reverse synced {file_path}")

            except Exception as e:
                logger.error(f"Error reverse syncing {file_path}: {e}")
                sync_success = False

        if sync_success:
            logger.info(f"✓ Successfully reverse synced all {len(changed_files)} files from remote")
        else:
            logger.error("Some files failed to reverse sync")

        return sync_success

    async def _reverse_sync_checksum_files(
        self, conn, sync_result: Dict[str, Any], remote_project_root: str
    ) -> bool:
        """Reverse sync files from checksum mismatch."""
        details = sync_result["details"]
        mismatched_files = details.get("mismatched_files", [])

        for mismatch in mismatched_files:
            file_path = mismatch["file"]
            logger.info(f"Reverse syncing: {file_path}")

            # Get remote file content
            remote_file_path = f"{remote_project_root}/{file_path}"
            get_content_cmd = f"cat '{remote_file_path}'"

            remote_result = await conn.run(get_content_cmd, check=False)
            if remote_result.returncode != 0:
                logger.error(f"Failed to read remote file {file_path}: {remote_result.stderr}")
                continue

            # Write to local file
            local_file = self.local_project_root / file_path
            local_file.parent.mkdir(parents=True, exist_ok=True)

            with open(local_file, "w", encoding="utf-8") as f:
                f.write(remote_result.stdout)

            logger.debug(f"✓ Successfully reverse synced {file_path}")

        logger.info(f"✓ Successfully reverse synced {len(mismatched_files)} files from remote")
        return True

    def can_offer_sync(self, sync_result: Dict[str, Any]) -> bool:
        """Determine if we can safely offer to sync based on the mismatch type."""
        if sync_result["method"] == "git":
            details = sync_result["details"]

            # Don't offer sync if commits are different (too risky)
            if details.get("local_commit") != details.get("remote_commit"):
                return False

            # Check change patterns to determine safety
            has_local_changes = bool(details.get("local_status"))
            has_remote_changes = bool(details.get("remote_status"))
            commits_match = details.get("local_commit") == details.get("remote_commit")

            if commits_match and has_local_changes and not has_remote_changes:
                return True
            elif commits_match and has_local_changes and has_remote_changes:
                # Check if changes are mostly config files
                remote_files = self._parse_git_status_files(details.get("remote_status", ""))
                local_files = self._parse_git_status_files(details.get("local_status", ""))

                remote_config_files = [f for f in remote_files if self._is_config_file(f)]
                local_config_files = [f for f in local_files if self._is_config_file(f)]

                # Allow sync if remote only has config changes or significant overlap
                if (len(remote_config_files) == len(remote_files) and len(remote_files) > 0) or (
                    len(set(remote_files) & set(local_files)) >= len(remote_files) * 0.5
                ):
                    return True
            elif commits_match and not has_local_changes and has_remote_changes:
                # Check if remote changes are config files (likely from previous sync)
                remote_files = self._parse_git_status_files(details.get("remote_status", ""))
                remote_config_files = [f for f in remote_files if self._is_config_file(f)]

                if len(remote_config_files) == len(remote_files) and len(remote_files) > 0:
                    return True

            return False

        elif sync_result["method"] == "checksum":
            details = sync_result["details"]
            # Offer sync if we have specific file mismatches (not missing files)
            return bool(details.get("mismatched_files")) and not details.get("missing_files")

        return False

    def _parse_git_status_files(self, git_status: str) -> List[str]:
        """Parse git status output to extract list of changed files."""
        files = []
        for line in git_status.split("\n"):
            if not line.strip():
                continue
            if len(line) > 3:
                filename = line[3:].strip()
                if filename:
                    files.append(filename)
        return files

    def _is_config_file(self, filename: str) -> bool:
        """Check if a file is a configuration file."""
        config_patterns = [
            "config",
            "Config",
            "CONFIG",
            ".yaml",
            ".yml",
            ".json",
            "requirements.txt",
            "pyproject.toml",
            "hyperparams",
            "experiment",
            "train_config",
            "model_config",
        ]
        return any(pattern in filename for pattern in config_patterns)
