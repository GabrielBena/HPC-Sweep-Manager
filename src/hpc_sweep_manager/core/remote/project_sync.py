"""Module for handling project state checking and synchronization."""

import hashlib
import logging
from pathlib import Path
import subprocess
from typing import Any, Dict, List

from .discovery import RemoteConfig

logger = logging.getLogger(__name__)


class ProjectStateChecker:
    """Checks if local and remote projects are in the same state."""

    def __init__(self, local_project_root: str, remote_config: RemoteConfig):
        self.local_project_root = Path(local_project_root)
        self.remote_config = remote_config

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

    async def verify_project_sync(self, conn) -> Dict[str, Any]:
        """
        Verify that local and remote projects are in sync.

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
                f"cd {self.remote_config.project_root} && git rev-parse --git-dir",
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
                f"cd {self.remote_config.project_root} && git status --porcelain"
            )
            result["remote_status"] = remote_status.stdout.strip()

            # Get remote commit hash
            remote_commit = await conn.run(
                f"cd {self.remote_config.project_root} && git rev-parse HEAD"
            )
            result["remote_commit"] = remote_commit.stdout.strip()

            # Check for uncommitted changes
            if result["local_status"]:
                result["warnings"].append("Local repository has uncommitted changes")

            if result["remote_status"]:
                result["warnings"].append("Remote repository has uncommitted changes")

            # Check if commits match
            if result["local_commit"] != result["remote_commit"]:
                result["errors"].append(
                    f"Git commits differ: local={result['local_commit'][:8]} vs remote={result['remote_commit'][:8]}"
                )

            # For distributed sweeps, analyze the nature of uncommitted changes
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

            # More nuanced sync determination
            if commits_match and not has_local_changes and not has_remote_changes:
                # Perfect sync
                result["in_sync"] = True
            elif commits_match and has_local_changes and not has_remote_changes:
                # Only local changes - standard case requiring sync
                result["in_sync"] = False
                result["errors"].append(
                    "Local uncommitted changes detected - sync required for distributed execution"
                )
            elif commits_match and not has_local_changes and has_remote_changes:
                # Only remote changes - check if they're config files from previous sync
                if len(remote_config_files) == len(remote_files) and len(remote_files) > 0:
                    # Remote only has config changes - likely from previous HSM sync
                    result["in_sync"] = False  # Still offer sync options
                    result["warnings"].append(
                        "Remote has config changes (likely from previous sync) - sync options available"
                    )
                else:
                    # Remote has non-config changes - more concerning
                    result["in_sync"] = False
                    result["errors"].append(
                        "Remote uncommitted changes detected - review needed before sync"
                    )
            elif commits_match and has_local_changes and has_remote_changes:
                # Both have changes - analyze overlap and file types
                files_overlap = set(local_files) & set(remote_files)
                overlap_ratio = len(files_overlap) / max(len(remote_files), 1)

                if (
                    len(remote_config_files) == len(remote_files)
                    and len(local_config_files) > 0
                    and overlap_ratio >= 0.5
                ):
                    # Both sides have config changes with good overlap - iterative development
                    result["in_sync"] = False  # Still offer sync
                    result["warnings"].append(
                        "Both local and remote have config changes - iterative development detected"
                    )
                else:
                    # More complex changes - be cautious
                    result["in_sync"] = False
                    result["errors"].append(
                        "Both local and remote have uncommitted changes - careful review required"
                    )
            else:
                # Commits don't match or other edge cases
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
        """Verify using file checksums based on discovered remote configuration."""
        result = {
            "in_sync": True,
            "checked_files": [],
            "mismatched_files": [],
            "missing_files": [],
            "warnings": [],
        }

        # Use discovered paths from remote config instead of hardcoding
        essential_files = []
        optional_files = ["requirements.txt", "pyproject.toml", "setup.py"]

        # Get files to check based on discovered remote configuration
        files_to_check = await self._discover_files_to_check(conn)
        essential_files = files_to_check["essential"]
        optional_files = files_to_check["optional"]

        # Combine all files for checking
        key_files = essential_files + optional_files

        # Remove duplicates while preserving order
        seen = set()
        key_files = [f for f in key_files if f and f not in seen and not seen.add(f)]

        # Track essential vs optional separately
        essential_set = set(essential_files)

        logger.info(
            f"Checking sync for {len(key_files)} key files ({len(essential_files)} essential, {len(optional_files)} optional)"
        )
        logger.debug(f"Essential files: {essential_files}")
        logger.debug(f"Optional files: {optional_files}")

        essential_missing = []
        optional_missing = []

        for file_path in key_files:
            if not file_path:
                continue

            try:
                # Check if file exists locally first
                local_file = self.local_project_root / file_path
                if not local_file.exists():
                    logger.debug(f"Skipping {file_path} - not found locally")
                    continue

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

                remote_result = await conn.run(remote_checksum_cmd)
                remote_checksum = remote_result.stdout.strip()

                if remote_checksum == "FILE_NOT_FOUND":
                    if file_path in essential_set:
                        essential_missing.append(file_path)
                        result["in_sync"] = False
                        logger.error(
                            f"Essential file {file_path} exists locally but missing on remote"
                        )
                    else:
                        optional_missing.append(file_path)
                        logger.warning(
                            f"Optional file {file_path} exists locally but missing on remote"
                        )
                    result["missing_files"].append(file_path)
                elif local_checksum != remote_checksum:
                    result["mismatched_files"].append(
                        {
                            "file": file_path,
                            "local_checksum": local_checksum,
                            "remote_checksum": remote_checksum,
                        }
                    )
                    if file_path in essential_set:
                        result["in_sync"] = False
                        logger.error(f"Essential file {file_path} has different checksums")
                    else:
                        logger.warning(f"Optional file {file_path} has different checksums")
                else:
                    result["checked_files"].append(file_path)
                    logger.debug(f"File {file_path} matches between local and remote")

            except Exception as e:
                result["warnings"].append(f"Could not verify {file_path}: {e}")
                logger.warning(f"Error checking {file_path}: {e}")

        # Update result details
        result["essential_missing"] = essential_missing
        result["optional_missing"] = optional_missing

        if essential_missing:
            result["warnings"].append(f"Essential files missing on remote: {essential_missing}")

        if optional_missing:
            result["warnings"].append(
                f"Optional files missing on remote (may be OK): {optional_missing}"
            )

        if result["mismatched_files"]:
            essential_mismatched = [
                f for f in result["mismatched_files"] if f["file"] in essential_set
            ]
            optional_mismatched = [
                f for f in result["mismatched_files"] if f["file"] not in essential_set
            ]

            if essential_mismatched:
                result["warnings"].append(
                    f"Essential files mismatched: {[f['file'] for f in essential_mismatched]}"
                )
            if optional_mismatched:
                result["warnings"].append(
                    f"Optional files mismatched: {[f['file'] for f in optional_mismatched]}"
                )

        return result

    async def _discover_files_to_check(self, conn) -> Dict[str, List[str]]:
        """
        Discover which files to check for sync based on the remote configuration.
        Uses the same discovery pattern as the remote HSM config discovery.
        """
        essential_files = []
        optional_files = ["requirements.txt", "pyproject.toml", "setup.py"]

        # 1. Add training script from discovered config
        if self.remote_config.train_script:
            # Handle both absolute and relative train script paths
            if self.remote_config.train_script.startswith("/"):
                # Absolute path - try to make it relative to project root
                try:
                    train_script_path = Path(self.remote_config.train_script)
                    if self.remote_config.project_root and train_script_path.is_relative_to(
                        Path(self.remote_config.project_root)
                    ):
                        rel_train_script = train_script_path.relative_to(
                            Path(self.remote_config.project_root)
                        )
                        essential_files.append(str(rel_train_script))
                    else:
                        logger.debug(
                            f"Training script {self.remote_config.train_script} is absolute and not under project root"
                        )
                except Exception as e:
                    logger.debug(f"Could not process absolute train script path: {e}")
            else:
                # Relative path - use as is
                essential_files.append(self.remote_config.train_script)

        # 2. Add config files from discovered config directory
        if self.remote_config.config_dir:
            config_files = await self._discover_config_files(conn)
            essential_files.extend(config_files)

        # 3. Add any additional files from remote HSM config if available
        additional_files = await self._discover_additional_files_from_remote_config(conn)
        essential_files.extend(additional_files["essential"])
        optional_files.extend(additional_files["optional"])

        return {
            "essential": list(set(essential_files)),  # Remove duplicates
            "optional": list(set(optional_files)),
        }

    async def _discover_config_files(self, conn) -> List[str]:
        """Discover actual config files in the remote config directory."""
        config_files = []

        if not self.remote_config.config_dir:
            return config_files

        try:
            # Get the relative path from project root for remote verification
            if self.remote_config.config_dir.startswith("/"):
                # Absolute path - make relative to project root if possible
                if self.remote_config.project_root:
                    try:
                        config_dir_path = Path(self.remote_config.config_dir)
                        project_root_path = Path(self.remote_config.project_root)
                        if config_dir_path.is_relative_to(project_root_path):
                            config_dir_relative = str(
                                config_dir_path.relative_to(project_root_path)
                            )
                        else:
                            logger.debug(
                                f"Config dir {self.remote_config.config_dir} not under project root"
                            )
                            return config_files
                    except Exception as e:
                        logger.debug(f"Could not make config dir relative: {e}")
                        return config_files
                else:
                    logger.debug("No project root to make config dir relative")
                    return config_files
            else:
                # Already relative
                config_dir_relative = self.remote_config.config_dir

            # Find all .yaml and .yml files in the config directory on remote
            find_cmd = f"""
            cd {self.remote_config.project_root}
            if [ -d "{config_dir_relative}" ]; then
                find "{config_dir_relative}" -type f \\( -name "*.yaml" -o -name "*.yml" \\) 2>/dev/null || true
            fi
            """

            result = await conn.run(find_cmd, check=False)
            if result.returncode == 0 and result.stdout.strip():
                remote_config_files = result.stdout.strip().split("\n")

                # Filter out empty lines and validate that files exist locally too
                for config_file in remote_config_files:
                    config_file = config_file.strip()
                    if config_file:
                        local_config_file = self.local_project_root / config_file
                        if local_config_file.exists():
                            config_files.append(config_file)
                        else:
                            logger.debug(
                                f"Config file {config_file} exists on remote but not locally"
                            )

        except Exception as e:
            logger.debug(f"Error discovering config files: {e}")

        return config_files

    async def _discover_additional_files_from_remote_config(self, conn) -> Dict[str, List[str]]:
        """Discover additional files to check based on remote HSM configuration."""
        additional = {"essential": [], "optional": []}

        try:
            # Try to read the remote HSM config to get additional file patterns
            # First find the HSM config file on remote
            hsm_config_cmd = f"""
            cd {self.remote_config.project_root}
            for path in "sweeps/hsm_config.yaml" "hsm_config.yaml" "./sweeps/hsm_config.yaml" "./hsm_config.yaml"; do
                if [ -f "$path" ]; then
                    echo "$path"
                    break
                fi
            done
            """

            result = await conn.run(hsm_config_cmd, check=False)
            if result.returncode == 0 and result.stdout.strip():
                hsm_config_path = result.stdout.strip()

                # Read the remote HSM config
                cat_result = await conn.run(
                    f"cd {self.remote_config.project_root} && cat {hsm_config_path}", check=False
                )
                if cat_result.returncode == 0:
                    import yaml

                    try:
                        remote_hsm_config = yaml.safe_load(cat_result.stdout)

                        # Extract any additional paths that might be important
                        paths = remote_hsm_config.get("paths", {})

                        # Check for any additional script or config paths
                        for key, path in paths.items():
                            if path and isinstance(path, str) and not path.startswith("/"):
                                # Only include relative paths that exist locally
                                local_file = self.local_project_root / path
                                if local_file.exists():
                                    if key in ["train_script", "config_dir"]:
                                        # Already handled above
                                        continue
                                    elif "config" in key.lower() or "script" in key.lower():
                                        additional["essential"].append(path)
                                    else:
                                        additional["optional"].append(path)

                    except yaml.YAMLError as e:
                        logger.debug(f"Could not parse remote HSM config: {e}")

        except Exception as e:
            logger.debug(f"Error discovering additional files from remote config: {e}")

        return additional

    def can_offer_sync(self, sync_result: Dict[str, Any]) -> bool:
        """
        Determine if we can safely offer sync options to the user.
        Moved from remote_manager.py for better separation of concerns.
        """
        try:
            if sync_result["method"] == "git":
                details = sync_result["details"]

                # Check for scenarios where sync would be risky or impossible
                local_status = details.get("local_status", "")
                remote_status = details.get("remote_status", "")
                commits_match = details.get("local_commit") == details.get("remote_commit")

                # Parse status to understand types of changes
                local_files = self._parse_git_status_files(local_status) if local_status else []
                remote_files = self._parse_git_status_files(remote_status) if remote_status else []

                # Check for conflicting scenarios
                files_overlap = set(local_files) & set(remote_files)
                has_file_conflicts = len(files_overlap) > 0

                # Safe scenarios for sync:
                # 1. Only local changes, no remote changes, same commit
                if commits_match and local_status and not remote_status:
                    return True

                # 2. Only remote changes, no local changes, same commit
                if commits_match and not local_status and remote_status:
                    return True

                # 3. Both have changes but no file overlap and same commit
                if commits_match and local_status and remote_status and not has_file_conflicts:
                    return True

                # 4. Only config file changes on remote side (likely from previous HSM runs)
                if commits_match and remote_status and not local_status:
                    remote_config_files = [f for f in remote_files if self._is_config_file(f)]
                    if len(remote_config_files) == len(remote_files):
                        return True

                # Risky scenarios - don't offer sync
                return False

            elif sync_result["method"] == "checksum":
                # For checksum verification, we can offer sync if there are just file differences
                # but no missing essential files
                details = sync_result["details"]
                essential_missing = details.get("essential_missing", [])

                # Don't offer sync if essential files are missing - this suggests structural differences
                if essential_missing:
                    return False

                # Can offer sync for file content differences
                mismatched_files = details.get("mismatched_files", [])
                if mismatched_files:
                    return True

                return False

        except Exception as e:
            logger.warning(f"Error determining sync safety: {e}")
            return False

        return False

    def is_minor_sync_issue(self, sync_result: Dict[str, Any]) -> bool:
        """
        Determine if sync differences are minor and can be tolerated in distributed mode.
        Moved from remote_manager.py for better separation of concerns.
        """
        try:
            if sync_result["method"] == "git":
                details = sync_result["details"]
                local_status = details.get("local_status", "")
                remote_status = details.get("remote_status", "")
                commits_match = details.get("local_commit") == details.get("remote_commit")

                # If commits match and only one side has changes, it might be minor
                if commits_match:
                    if local_status and not remote_status:
                        # Only local changes - check if they're just config files
                        local_files = self._parse_git_status_files(local_status)
                        config_files = [f for f in local_files if self._is_config_file(f)]
                        # If most changes are config files, this is minor
                        if len(config_files) >= len(local_files) * 0.7:
                            return True

                    elif remote_status and not local_status:
                        # Only remote changes - check if they're config files from previous runs
                        remote_files = self._parse_git_status_files(remote_status)
                        config_files = [f for f in remote_files if self._is_config_file(f)]
                        # If all remote changes are config files, this is likely from previous HSM runs
                        if len(config_files) == len(remote_files) and len(remote_files) > 0:
                            return True

                # Different commits suggest more significant changes
                return False

            elif sync_result["method"] == "checksum":
                details = sync_result["details"]
                essential_missing = details.get("essential_missing", [])
                optional_missing = details.get("optional_missing", [])
                mismatched_files = details.get("mismatched_files", [])

                # If no essential files are missing and only optional files differ, it's minor
                if not essential_missing:
                    # Check if mismatched files are mostly config files
                    if mismatched_files:
                        config_mismatches = [
                            m for m in mismatched_files if self._is_config_file(m["file"])
                        ]
                        non_config_mismatches = [
                            m for m in mismatched_files if not self._is_config_file(m["file"])
                        ]

                        # If most mismatches are config files and no critical files are affected, it's minor
                        if (
                            len(non_config_mismatches) <= 2
                            and len(config_mismatches) >= len(mismatched_files) * 0.6
                        ):
                            return True

                    # If only optional files are missing, it's minor
                    if optional_missing and not mismatched_files:
                        return True

                # Essential missing files or significant mismatches indicate major issues
                return False

        except Exception as e:
            logger.warning(f"Error determining if sync issue is minor: {e}")
            return False

        return False

    def _calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
