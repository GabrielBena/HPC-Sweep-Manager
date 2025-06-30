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
                # 1. Find hsm_config.yaml on remote
                hsm_config_path = await self._find_hsm_config(conn)
                if not hsm_config_path:
                    logger.error(f"No hsm_config.yaml found on remote {remote_name}")
                    return None

                # 2. Read and parse remote HSM config
                remote_hsm_config = await self._read_remote_hsm_config(conn, hsm_config_path)
                if not remote_hsm_config:
                    logger.error(f"Failed to read hsm_config.yaml on remote {remote_name}")
                    return None

                # 3. Extract paths and configuration
                remote_config = self._extract_remote_paths(remote_config, remote_hsm_config)

                # 4. Validate remote environment
                is_valid = await self._validate_remote_environment(conn, remote_config)
                if not is_valid:
                    logger.error(f"Remote environment validation failed for {remote_name}")
                    return None

                # 5. Test basic commands
                test_success = await self._test_basic_commands(conn, remote_config)
                if not test_success:
                    logger.error(f"Basic command tests failed for {remote_name}")
                    return None

                logger.debug(f"Successfully discovered configuration for remote: {remote_name}")
                return remote_config

        except Exception as e:
            logger.error(f"Failed to discover configuration for remote {remote_name}: {e}")
            return None

    async def _find_hsm_config(self, conn) -> Optional[str]:
        """Find hsm_config.yaml on the remote machine."""
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

        # Try to find in common project structures
        try:
            # Look for any hsm_config.yaml in current directory tree
            result = await conn.run(
                'find . -name "hsm_config.yaml" -type f 2>/dev/null | head -1',
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

        return remote_config

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
