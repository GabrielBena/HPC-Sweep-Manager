"""CLI utility functions."""

from typing import List, Tuple

from rich.console import Console

from ..compute.base import ComputeSource
from ..config.validation import SystemValidator, ValidationResult


def validate_environment() -> ValidationResult:
    """Validate the system environment for HSM.

    Returns:
        ValidationResult with environment check details
    """
    # Combine python environment and system resource validation
    python_result = SystemValidator.validate_python_environment()
    system_result = SystemValidator.validate_system_resources()

    # Merge results
    combined_result = ValidationResult()
    combined_result.errors.extend(python_result.errors)
    combined_result.warnings.extend(python_result.warnings)
    combined_result.info.extend(python_result.info)

    combined_result.errors.extend(system_result.errors)
    combined_result.warnings.extend(system_result.warnings)
    combined_result.info.extend(system_result.info)

    return combined_result


def parse_compute_sources(sources_str: str) -> List[Tuple[str, dict]]:
    """Parse compute sources string into source specifications.

    Args:
        sources_str: Comma-separated list of compute sources
                    Format: "local", "ssh:hostname", "hpc:cluster_name"

    Returns:
        List of (source_type, config) tuples
    """
    sources = []

    for source_spec in sources_str.split(","):
        source_spec = source_spec.strip()

        if ":" in source_spec:
            source_type, config_str = source_spec.split(":", 1)
            source_type = source_type.strip()
            config_str = config_str.strip()

            if source_type == "ssh":
                sources.append(("ssh", {"hostname": config_str}))
            elif source_type == "hpc":
                sources.append(("hpc", {"cluster": config_str}))
            else:
                raise ValueError(f"Unknown compute source type: {source_type}")
        else:
            if source_spec == "local":
                sources.append(("local", {}))
            else:
                raise ValueError(f"Unknown compute source: {source_spec}")

    return sources


def create_compute_source(source_type: str, config: dict) -> ComputeSource:
    """Create a compute source instance from type and config.

    Args:
        source_type: Type of compute source ("local", "ssh", "hpc")
        config: Configuration dictionary for the source

    Returns:
        ComputeSource instance

    Raises:
        ValueError: If source type is unknown
        ImportError: If required dependencies are missing
    """
    if source_type == "local":
        from ..compute.local import LocalComputeSource

        # Add default name if not provided
        if "name" not in config:
            config["name"] = f"local-{config.get('hostname', 'default')}"

        return LocalComputeSource(**config)

    elif source_type == "ssh":
        from ..compute.ssh import SSHComputeSource, SSHConfig

        # Extract hostname and create SSHConfig
        hostname = config.get("hostname")
        if not hostname:
            raise ValueError("SSH compute source requires 'hostname' in config")

        ssh_config = SSHConfig(
            host=hostname,
            username=config.get("username"),
            port=config.get("port", 22),
            key_file=config.get("key_file"),
            password=config.get("password"),
            known_hosts=config.get("known_hosts"),
            project_dir=config.get("project_dir"),
            python_path=config.get("python_path"),
            conda_env=config.get("conda_env"),
        )

        # Create source config
        source_config = {
            "name": config.get("name", f"ssh-{hostname}"),
            "ssh_config": ssh_config,
            "max_concurrent_tasks": config.get("max_concurrent_tasks", 4),
            "script_path": config.get("script_path"),
            "timeout": config.get("timeout", 3600),
            "sync_interval": config.get("sync_interval", 30),
        }

        return SSHComputeSource(**source_config)

    elif source_type == "hpc":
        from ..compute.hpc import HPCComputeSource

        # Add default name if not provided
        cluster = config.get("cluster", "default")
        if "name" not in config:
            config["name"] = f"hpc-{cluster}"

        return HPCComputeSource(**config)

    else:
        raise ValueError(f"Unknown compute source type: {source_type}")


def check_dependencies() -> List[str]:
    """Check for optional dependencies and return missing ones.

    Returns:
        List of missing dependency names
    """
    missing = []

    # Check for SSH dependencies
    try:
        import asyncssh
    except ImportError:
        missing.append("asyncssh (for SSH compute sources)")

    # Check for HPC dependencies
    try:
        import paramiko
    except ImportError:
        missing.append("paramiko (for HPC compute sources)")

    # Check for monitoring dependencies
    try:
        import psutil
    except ImportError:
        missing.append("psutil (for system monitoring)")

    return missing


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def format_size(bytes_count: int) -> str:
    """Format byte count to human-readable string.

    Args:
        bytes_count: Number of bytes

    Returns:
        Formatted size string
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_count < 1024:
            return f"{bytes_count:.1f}{unit}"
        bytes_count /= 1024
    return f"{bytes_count:.1f}PB"


def confirm_action(message: str, default: bool = False) -> bool:
    """Prompt user for confirmation.

    Args:
        message: Confirmation message
        default: Default response if user just presses enter

    Returns:
        True if user confirms, False otherwise
    """
    console = Console()

    default_str = "[Y/n]" if default else "[y/N]"
    prompt = f"{message} {default_str}: "

    try:
        response = console.input(prompt).strip().lower()
        if not response:
            return default
        return response in ("y", "yes", "true", "1")
    except (KeyboardInterrupt, EOFError):
        console.print("\nCancelled.")
        return False
