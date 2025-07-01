"""Remote HSM configuration discovery and validation."""

import asyncio
from dataclasses import dataclass
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

try:
    import asyncssh

    ASYNCSSH_AVAILABLE = True
except ImportError:
    ASYNCSSH_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class RemoteConfig:
    """Configuration for a remote machine."""

    name: str
    host: str
    ssh_key: Optional[str] = None
    ssh_port: int = 22
    max_parallel_jobs: Optional[int] = None
    enabled: bool = True

    # Auto-discovered paths (populated by discovery)
    python_interpreter: Optional[str] = None
    train_script: Optional[str] = None
    project_root: Optional[str] = None
    config_dir: Optional[str] = None
    output_dir: Optional[str] = None
    wandb_config: Optional[Dict] = None
    hpc_config: Optional[Dict] = None


def expand_ssh_key_path(ssh_key_path: str) -> Optional[str]:
    """Expand SSH key path and verify it exists."""
    if not ssh_key_path:
        return None

    # Expand ~ and environment variables
    expanded_path = os.path.expanduser(os.path.expandvars(ssh_key_path))
    expanded_path = Path(expanded_path).resolve()

    if expanded_path.exists():
        return str(expanded_path)
    else:
        logger.debug(f"SSH key not found at: {expanded_path}")
        return None


def get_ssh_client_keys(ssh_key_path: Optional[str]) -> List[str]:
    """Get list of SSH client keys for asyncssh."""
    keys = []

    # Try user-specified key first
    if ssh_key_path:
        expanded_key = expand_ssh_key_path(ssh_key_path)
        if expanded_key:
            keys.append(expanded_key)
            logger.debug(f"Added user-specified SSH key: {expanded_key}")
        else:
            logger.debug(f"User-specified SSH key not found: {ssh_key_path}")

    # Always try default SSH keys as fallback (but only add if they exist)
    default_keys = [
        "~/.ssh/id_ed25519",
        "~/.ssh/id_rsa",
        "~/.ssh/id_ecdsa",
        "~/.ssh/id_dsa",
    ]

    found_default_keys = []
    for default_key in default_keys:
        expanded_default = expand_ssh_key_path(default_key)
        if expanded_default and expanded_default not in keys:
            keys.append(expanded_default)
            found_default_keys.append(expanded_default)

    if found_default_keys:
        logger.debug(f"Added default SSH keys: {found_default_keys}")
    elif not keys:
        logger.debug("No SSH keys found, using SSH agent or password authentication")

    return keys


async def create_ssh_connection(host: str, ssh_key: Optional[str] = None, ssh_port: int = 22):
    """Create SSH connection with proper key handling."""
    logger.debug(f"Attempting SSH connection to {host}:{ssh_port}")

    # Build connection arguments
    connection_kwargs = {
        "host": host,
        "port": ssh_port,
        "known_hosts": None,  # Accept any host key for now (like -o StrictHostKeyChecking=no)
    }

    # If user specified an SSH key, try to use it
    if ssh_key:
        expanded_key = expand_ssh_key_path(ssh_key)
        if expanded_key:
            logger.debug(f"Using specified SSH key: {expanded_key}")
            connection_kwargs["client_keys"] = [expanded_key]
        else:
            logger.warning(f"Specified SSH key not found: {ssh_key}, using default SSH behavior")
    else:
        logger.debug("No SSH key specified, using default SSH behavior")

    # Let asyncssh auto-detect username from host string or use current user
    if "@" not in host:
        connection_kwargs["username"] = os.getenv("USER") or os.getenv("USERNAME")
        logger.debug(f"No username in host, using: {connection_kwargs['username']}")
    else:
        logger.debug("Username included in host string")

    logger.debug(f"Connection arguments: {connection_kwargs}")

    try:
        conn = await asyncssh.connect(**connection_kwargs)
        logger.debug(f"✓ SSH connection established to {host}")
        return conn
    except asyncssh.PermissionDenied as e:
        logger.error(f"SSH permission denied to {host}: {e}")
        logger.error("Try running: ssh-copy-id {host}")
        raise
    except asyncssh.ConnectionLost as e:
        logger.error(f"SSH connection lost to {host}: {e}")
        raise
    except Exception as e:
        logger.error(f"SSH connection failed to {host}: {e}")
        logger.error(f"Full error details: {type(e).__name__}: {e}")

        # Try a simpler connection without client_keys
        if "client_keys" in connection_kwargs:
            logger.info("Retrying connection without explicit client keys...")
            simple_kwargs = {k: v for k, v in connection_kwargs.items() if k != "client_keys"}
            try:
                conn = await asyncssh.connect(**simple_kwargs)
                logger.debug(f"✓ SSH connection established to {host} (fallback mode)")
                return conn
            except Exception as e2:
                logger.error(f"Fallback connection also failed: {e2}")

        raise


class RemoteDiscovery:
    """Discovers and validates remote HSM configurations."""

    def __init__(self, local_hsm_config: Dict[str, Any]):
        """Initialize with local HSM configuration for reference."""
        self.local_config = local_hsm_config
        self.discovered_remotes = {}

        if not ASYNCSSH_AVAILABLE:
            raise ImportError(
                "asyncssh is required for distributed sweeps. Install with: pip install asyncssh"
            )

    async def discover_remote_config(self, remote_info: Dict[str, Any]) -> Optional[RemoteConfig]:
        """
        Discover HSM configuration on a remote machine.

        Args:
            remote_info: Basic connection info from local hsm_config.yaml

        Returns:
            RemoteConfig with discovered paths, or None if discovery fails
        """
        remote_name = remote_info.get("name", "unknown")
        logger.debug(f"Discovering HSM configuration on remote: {remote_name}")

        try:
            # Create RemoteConfig with basic info
            remote_config = RemoteConfig(
                name=remote_name,
                host=remote_info["host"],
                ssh_key=remote_info.get("ssh_key"),
                ssh_port=remote_info.get("ssh_port", 22),
                max_parallel_jobs=remote_info.get("max_parallel_jobs"),
                enabled=remote_info.get("enabled", True),
            )

            # Establish SSH connection and discover configuration
            async with await create_ssh_connection(
                remote_config.host, remote_config.ssh_key, remote_config.ssh_port
            ) as conn:
                # 1. Try to find matching project structure first
                local_project_name = Path.cwd().name
                logger.info(f"Looking for project '{local_project_name}' on remote {remote_name}")

                matched_project_root = await self._find_matching_project(conn, local_project_name)

                # 2. Find hsm_config.yaml on remote (with project matching if available)
                hsm_config_path = await self._find_hsm_config(conn, matched_project_root)
                if not hsm_config_path:
                    logger.error(f"No hsm_config.yaml found on remote {remote_name}")
                    if matched_project_root:
                        logger.error(f"Searched in matched project: {matched_project_root}")
                    logger.error("Make sure the project exists on the remote machine")
                    return None

                # 3. Read and parse remote HSM config
                remote_hsm_config = await self._read_remote_hsm_config(conn, hsm_config_path)
                if not remote_hsm_config:
                    logger.error(f"Failed to read hsm_config.yaml on remote {remote_name}")
                    return None

                # 4. Extract paths and configuration
                remote_config = self._extract_remote_paths(remote_config, remote_hsm_config)

                # 5. Set project root based on matched project or auto-detection
                if matched_project_root:
                    remote_config.project_root = matched_project_root
                    logger.info(
                        f"Using matched project root for {remote_name}: {matched_project_root}"
                    )
                elif not remote_config.project_root:
                    detected_root = await self._auto_detect_project_root(conn, hsm_config_path)
                    if detected_root:
                        remote_config.project_root = detected_root
                        logger.info(
                            f"Auto-detected project root for {remote_name}: {detected_root}"
                        )
                    else:
                        logger.warning(f"Could not auto-detect project root for {remote_name}")

                # 6. Validate remote environment
                is_valid = await self._validate_remote_environment(conn, remote_config)
                if not is_valid:
                    logger.error(f"Remote environment validation failed for {remote_name}")
                    return None

                # 7. Test basic commands
                test_success = await self._test_basic_commands(conn, remote_config)
                if not test_success:
                    logger.error(f"Basic command tests failed for {remote_name}")
                    return None

                logger.debug(f"Successfully discovered configuration for remote: {remote_name}")
                return remote_config

        except Exception as e:
            logger.error(f"Failed to discover configuration for remote {remote_name}: {e}")
            return None

    async def _find_matching_project(self, conn, local_project_name: str) -> Optional[str]:
        """Find a project directory on remote that matches the local project structure."""
        try:
            logger.debug(f"Searching for project matching '{local_project_name}' on remote")

            # Phase 1: Search in common predictable locations
            search_locations = [
                ".",  # Current directory
                "Code",
                "code",
                "projects",
                "Projects",
                "work",
                "workspace",
                "src",
                "dev",
                "development",
                f"Code/{local_project_name}",
                f"code/{local_project_name}",
                f"projects/{local_project_name}",
                f"Projects/{local_project_name}",
                local_project_name,  # Direct project name in current dir
            ]

            # Also search in common nested structures
            nested_patterns = [
                f"Code/packages/{local_project_name}",
                f"code/packages/{local_project_name}",
                f"Code/repos/{local_project_name}",
                f"code/repos/{local_project_name}",
                f"Code/NCA/{local_project_name}",  # Common research directory structure
                f"code/nca/{local_project_name}",
                f"Code/ML/{local_project_name}",
                f"code/ml/{local_project_name}",
                f"packages/{local_project_name}",
                f"repos/{local_project_name}",
            ]

            search_locations.extend(nested_patterns)

            for location in search_locations:
                # Check if this directory exists and has project indicators
                try:
                    # Check if directory exists
                    result = await conn.run(f"test -d {location}", check=False)
                    if result.returncode != 0:
                        continue

                    # Check for HSM project indicators
                    indicators_cmd = f"""
                    cd {location} 2>/dev/null && (
                        test -f hsm_config.yaml || test -f sweeps/hsm_config.yaml
                    ) && echo "found"
                    """

                    result = await conn.run(indicators_cmd, check=False)
                    if result.returncode == 0 and "found" in result.stdout:
                        # Convert to absolute path
                        abs_path_result = await conn.run(f"cd {location} && pwd", check=False)
                        if abs_path_result.returncode == 0:
                            abs_path = abs_path_result.stdout.strip()
                            logger.info(f"Found matching project at: {abs_path}")
                            return abs_path

                except Exception as e:
                    logger.debug(f"Error checking location {location}: {e}")
                    continue

            # Phase 2: Use find command to search for directories with the project name that contain HSM configs
            logger.debug(
                "Predictable locations failed, searching for project directories with HSM configs"
            )

            # Search for directories with the project name that contain hsm_config.yaml files
            find_project_cmd = f"""
            find ~ -maxdepth 4 -type d -name "{local_project_name}" 2>/dev/null | while read dir; do
                if [ -f "$dir/hsm_config.yaml" ] || [ -f "$dir/sweeps/hsm_config.yaml" ]; then
                    echo "$dir"
                fi
            done | head -1
            """

            result = await conn.run(find_project_cmd, check=False)
            if result.returncode == 0 and result.stdout.strip():
                found_project = result.stdout.strip()
                # Convert to absolute path
                abs_path_result = await conn.run(f"cd {found_project} && pwd", check=False)
                if abs_path_result.returncode == 0:
                    abs_path = abs_path_result.stdout.strip()
                    logger.info(f"Found project via directory search at: {abs_path}")
                    return abs_path

            # Phase 3: Broader search for any HSM projects (fallback)
            logger.debug("Project-specific search failed, searching for any HSM projects")
            find_cmd = 'find ~ -maxdepth 4 -name "hsm_config.yaml" -type f 2>/dev/null | head -10'

            result = await conn.run(find_cmd, check=False)
            if result.returncode == 0 and result.stdout.strip():
                found_configs = result.stdout.strip().split("\n")
                logger.warning(f"Found {len(found_configs)} HSM projects on remote:")

                # Check if any of these match our project name
                project_matches = []
                for config_path in found_configs:
                    config_dir = str(Path(config_path).parent)
                    logger.warning(f"  - {config_dir}")

                    # Check if the directory name or parent directory name matches our project
                    dir_parts = Path(config_dir).parts
                    if local_project_name in dir_parts:
                        project_matches.append(config_dir)

                if len(project_matches) == 1:
                    logger.info(f"Found unique HSM project match: {project_matches[0]}")
                    return project_matches[0]
                elif len(project_matches) > 1:
                    logger.warning(f"Multiple HSM projects match '{local_project_name}':")
                    for match in project_matches:
                        logger.warning(f"  - {match}")
                    logger.warning("Specify remote project_root explicitly in hsm_config.yaml")
                    return None
                else:
                    logger.warning(f"No HSM projects match '{local_project_name}'")
                    logger.warning(
                        "Consider specifying remote project_root explicitly in hsm_config.yaml"
                    )
                    return None

            logger.warning(f"No matching project found for '{local_project_name}' on remote")
            return None

        except Exception as e:
            logger.warning(f"Error finding matching project: {e}")
            return None

    async def _find_hsm_config(self, conn, project_root: Optional[str] = None) -> Optional[str]:
        """Find hsm_config.yaml on the remote machine, optionally within a specific project."""
        if project_root:
            # Search within the specified project root
            search_paths = [
                f"{project_root}/sweeps/hsm_config.yaml",
                f"{project_root}/hsm_config.yaml",
            ]

            for path in search_paths:
                try:
                    result = await conn.run(f'test -f {path} && echo "found"', check=False)
                    if result.stdout.strip() == "found":
                        logger.debug(f"Found hsm_config.yaml at: {path}")
                        return path
                except Exception as e:
                    logger.debug(f"Error checking path {path}: {e}")
                    continue

            logger.warning(f"No hsm_config.yaml found in project root: {project_root}")
            return None

        # Fallback: search in current directory and common locations
        search_paths = [
            "sweeps/hsm_config.yaml",
            "hsm_config.yaml",
            "./sweeps/hsm_config.yaml",
            "./hsm_config.yaml",
        ]

        for path in search_paths:
            try:
                result = await conn.run(f'test -f {path} && echo "found"', check=False)
                if result.stdout.strip() == "found":
                    logger.debug(f"Found hsm_config.yaml at: {path}")
                    return path
            except Exception as e:
                logger.debug(f"Error checking path {path}: {e}")
                continue

        # Try to find in current directory tree as last resort
        try:
            result = await conn.run(
                'find . -maxdepth 3 -name "hsm_config.yaml" -type f 2>/dev/null | head -1',
                check=False,
            )
            if result.stdout.strip():
                found_path = result.stdout.strip()
                logger.debug(f"Found hsm_config.yaml via find: {found_path}")
                return found_path
        except Exception as e:
            logger.debug(f"Error during find search: {e}")

        return None

    async def _read_remote_hsm_config(self, conn, config_path: str) -> Optional[Dict[str, Any]]:
        """Read and parse remote hsm_config.yaml."""
        try:
            result = await conn.run(f"cat {config_path}")
            config_content = result.stdout

            # Parse YAML
            config_data = yaml.safe_load(config_content)
            return config_data

        except Exception as e:
            logger.error(f"Failed to read remote hsm_config.yaml at {config_path}: {e}")
            return None

    def _extract_remote_paths(
        self, remote_config: RemoteConfig, hsm_config: Dict[str, Any]
    ) -> RemoteConfig:
        """Extract paths from remote HSM config."""
        paths = hsm_config.get("paths", {})
        project = hsm_config.get("project", {})
        wandb = hsm_config.get("wandb", {})
        hpc = hsm_config.get("hpc", {})

        # Update remote config with discovered paths
        remote_config.python_interpreter = paths.get("python_interpreter")
        remote_config.train_script = paths.get("train_script")
        remote_config.project_root = project.get("root")
        remote_config.config_dir = paths.get("config_dir")
        remote_config.output_dir = paths.get("output_dir", "outputs")
        remote_config.wandb_config = wandb
        remote_config.hpc_config = hpc

        # Use remote's max_parallel_jobs if not overridden locally
        if remote_config.max_parallel_jobs is None:
            remote_config.max_parallel_jobs = hpc.get("max_array_size", 4)

        # If project_root is None or empty, try to auto-detect it from other paths
        if not remote_config.project_root:
            logger.warning(
                f"Project root not set in remote HSM config for {remote_config.name}, attempting auto-detection"
            )

            # Try to infer from train_script path
            if remote_config.train_script and not remote_config.train_script.startswith("/"):
                # Relative train script suggests current directory is project root
                # Since hsm_config.yaml was found, use its directory as project root
                # This will be resolved during validation when we have the hsm_config path
                pass

            # Try to infer from config_dir path
            if remote_config.config_dir and not remote_config.config_dir.startswith("/"):
                # Relative config directory suggests current directory is project root
                pass

            # If we still don't have a project root, we'll set it during validation
            # based on where we found the hsm_config.yaml file

        return remote_config

    async def _auto_detect_project_root(self, conn, hsm_config_path: str) -> Optional[str]:
        """Auto-detect project root based on common project indicators."""
        try:
            # Start from the directory containing hsm_config.yaml
            config_dir = str(Path(hsm_config_path).parent)

            # Check for common project root indicators
            project_indicators = [
                ".git",
                "setup.py",
                "pyproject.toml",
                "requirements.txt",
                "Dockerfile",
                ".gitignore",
                "README.md",
                "README.rst",
            ]

            # Check current directory (where hsm_config.yaml was found)
            for indicator in project_indicators:
                check_path = f"{config_dir}/{indicator}"
                result = await conn.run(f"test -e {check_path}", check=False)
                if result.returncode == 0:
                    logger.debug(f"Found project indicator {indicator} in {config_dir}")
                    return config_dir

            # If not found in config directory, check parent directories up to 3 levels
            current_dir = config_dir
            for level in range(3):
                parent_dir = str(Path(current_dir).parent)
                if parent_dir == current_dir:  # Reached filesystem root
                    break

                for indicator in project_indicators:
                    check_path = f"{parent_dir}/{indicator}"
                    result = await conn.run(f"test -e {check_path}", check=False)
                    if result.returncode == 0:
                        logger.debug(f"Found project indicator {indicator} in {parent_dir}")
                        return parent_dir

                current_dir = parent_dir

            # Fallback: use the directory containing hsm_config.yaml
            logger.warning(
                f"No project indicators found, using hsm_config.yaml directory as project root: {config_dir}"
            )
            return config_dir

        except Exception as e:
            logger.warning(f"Error auto-detecting project root: {e}")
            return None

    async def _validate_remote_environment(self, conn, remote_config: RemoteConfig) -> bool:
        """Validate that remote environment is properly set up."""
        validation_tasks = []

        # Check Python interpreter
        if remote_config.python_interpreter:
            validation_tasks.append(
                self._check_command(
                    conn,
                    f"{remote_config.python_interpreter} --version",
                    "Python interpreter",
                )
            )

        # Check project directory
        if remote_config.project_root:
            validation_tasks.append(
                self._check_path(conn, remote_config.project_root, "Project root")
            )

        # Check train script (handle absolute vs relative paths correctly)
        if remote_config.train_script and remote_config.project_root:
            # If train_script is absolute path, use it directly
            if remote_config.train_script.startswith("/"):
                script_path = remote_config.train_script
            else:
                # If relative, combine with project root
                script_path = f"{remote_config.project_root}/{remote_config.train_script}"

            validation_tasks.append(self._check_path(conn, script_path, "Training script"))

        # Check config directory (handle absolute vs relative paths correctly)
        if remote_config.config_dir:
            # If config_dir is absolute path, use it directly
            if remote_config.config_dir.startswith("/"):
                config_path = remote_config.config_dir
            else:
                # If relative, combine with project root (if available)
                if remote_config.project_root:
                    config_path = f"{remote_config.project_root}/{remote_config.config_dir}"
                else:
                    config_path = remote_config.config_dir

            validation_tasks.append(self._check_path(conn, config_path, "Config directory"))

        # Run all validations concurrently
        results = await asyncio.gather(*validation_tasks, return_exceptions=True)

        # Check if all validations passed
        success_count = sum(1 for result in results if result is True)
        total_checks = len(validation_tasks)

        logger.debug(f"Environment validation: {success_count}/{total_checks} checks passed")
        return success_count == total_checks

    async def _check_command(self, conn, command: str, description: str) -> bool:
        """Check if a command can be executed successfully."""
        try:
            result = await conn.run(command, check=False)
            if result.returncode == 0:
                logger.debug(f"✓ {description}: {command}")
                return True
            else:
                logger.warning(f"✗ {description}: {command} (exit code: {result.returncode})")
                return False
        except Exception as e:
            logger.warning(f"✗ {description}: {command} (error: {e})")
            return False

    async def _check_path(self, conn, path: str, description: str) -> bool:
        """Check if a path exists on the remote machine."""
        try:
            result = await conn.run(f"test -e {path}", check=False)
            if result.returncode == 0:
                logger.debug(f"✓ {description}: {path}")
                return True
            else:
                logger.warning(f"✗ {description}: {path} (not found)")
                return False
        except Exception as e:
            logger.warning(f"✗ {description}: {path} (error: {e})")
            return False

    async def _test_basic_commands(self, conn, remote_config: RemoteConfig) -> bool:
        """Test basic commands to ensure remote environment works."""
        test_tasks = []

        # Test Python version
        if remote_config.python_interpreter:
            test_tasks.append(
                self._check_command(
                    conn,
                    f"{remote_config.python_interpreter} --version",
                    "Python version check",
                )
            )

        # Test import of basic modules
        if remote_config.python_interpreter:
            test_tasks.append(
                self._check_command(
                    conn,
                    f"{remote_config.python_interpreter} -c 'import sys; print(sys.version)'",
                    "Python import test",
                )
            )

        # Test wandb if available (don't fail if not present)
        if remote_config.python_interpreter:
            # This is optional, so we'll allow it to fail
            try:
                result = await conn.run(
                    f"{remote_config.python_interpreter} -c 'import wandb; print(wandb.__version__)'",
                    check=False,
                )
                if result.returncode == 0:
                    logger.debug("✓ W&B available on remote")
                else:
                    logger.debug("W&B not available on remote (optional)")
            except:
                logger.debug("W&B not available on remote (optional)")

        # Run command tests
        if test_tasks:
            results = await asyncio.gather(*test_tasks, return_exceptions=True)
            success_count = sum(1 for result in results if result is True)
            total_tests = len(test_tasks)

            logger.debug(f"Basic command tests: {success_count}/{total_tests} passed")
            return success_count == total_tests

        return True

    async def discover_all_remotes(self, remotes_config: Dict[str, Any]) -> Dict[str, RemoteConfig]:
        """Discover configurations for all configured remote machines."""
        discovery_tasks = []

        for remote_name, remote_info in remotes_config.items():
            if remote_info.get("enabled", True):
                remote_info["name"] = remote_name
                discovery_tasks.append(self.discover_remote_config(remote_info))

        # Run discoveries concurrently
        results = await asyncio.gather(*discovery_tasks, return_exceptions=True)

        # Collect successful discoveries
        discovered = {}
        for i, result in enumerate(results):
            if isinstance(result, RemoteConfig):
                discovered[result.name] = result
                logger.info(f"✓ Remote '{result.name}' configured successfully")
            else:
                remote_name = list(remotes_config.keys())[i]
                logger.error(f"✗ Failed to configure remote '{remote_name}': {result}")

        return discovered


class RemoteValidator:
    """Validates remote configurations and provides health checks."""

    def __init__(self, remote_configs: Dict[str, RemoteConfig]):
        self.remote_configs = remote_configs

    async def health_check(self, remote_name: str) -> Dict[str, Any]:
        """Perform health check on a specific remote machine."""
        if remote_name not in self.remote_configs:
            return {"status": "unknown", "error": "Remote not configured"}

        remote_config = self.remote_configs[remote_name]

        try:
            async with await create_ssh_connection(
                remote_config.host, remote_config.ssh_key, remote_config.ssh_port
            ) as conn:
                health_info = {
                    "status": "healthy",
                    "connection": "✓",
                    "timestamp": None,
                    "load": None,
                    "disk_space": None,
                    "python_version": None,
                }

                # Get system info
                try:
                    # System load
                    result = await conn.run("uptime")
                    health_info["load"] = result.stdout.strip()

                    # Disk space
                    result = await conn.run("df -h . | tail -1")
                    health_info["disk_space"] = result.stdout.strip()

                    # Python version
                    if remote_config.python_interpreter:
                        result = await conn.run(f"{remote_config.python_interpreter} --version")
                        health_info["python_version"] = result.stdout.strip()

                    # Timestamp
                    result = await conn.run("date")
                    health_info["timestamp"] = result.stdout.strip()

                except Exception as e:
                    health_info["warning"] = f"Could not gather all system info: {e}"

                return health_info

        except Exception as e:
            return {"status": "unhealthy", "error": str(e), "connection": "✗"}

    async def health_check_all(self) -> Dict[str, Dict[str, Any]]:
        """Perform health checks on all remote machines."""
        health_tasks = [
            self.health_check(remote_name) for remote_name in self.remote_configs.keys()
        ]

        results = await asyncio.gather(*health_tasks, return_exceptions=True)

        health_report = {}
        for i, result in enumerate(results):
            remote_name = list(self.remote_configs.keys())[i]
            if isinstance(result, dict):
                health_report[remote_name] = result
            else:
                health_report[remote_name] = {"status": "error", "error": str(result)}

        return health_report
