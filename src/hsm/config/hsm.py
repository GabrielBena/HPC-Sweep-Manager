"""HSM configuration management.

This module provides configuration management for HSM-specific settings
including default paths, resource settings, and system preferences.
"""

from dataclasses import dataclass, field
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

logger = logging.getLogger(__name__)


@dataclass
class HPCConfig:
    """HPC-specific configuration settings."""

    default_walltime: str = "23:59:59"
    default_resources: str = "select=1:ncpus=4:mem=64gb"
    default_queue: Optional[str] = None
    max_array_size: Optional[int] = None
    system: Optional[str] = None
    module_commands: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "default_walltime": self.default_walltime,
            "default_resources": self.default_resources,
            "default_queue": self.default_queue,
            "max_array_size": self.max_array_size,
            "system": self.system,
            "module_commands": self.module_commands,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HPCConfig":
        """Create from dictionary."""
        return cls(
            default_walltime=data.get("default_walltime", "23:59:59"),
            default_resources=data.get("default_resources", "select=1:ncpus=4:mem=64gb"),
            default_queue=data.get("default_queue"),
            max_array_size=data.get("max_array_size"),
            system=data.get("system"),
            module_commands=data.get("module_commands", []),
        )


@dataclass
class PathsConfig:
    """Path configuration settings."""

    python_interpreter: Optional[str] = None
    training_script: Optional[str] = None
    project_root: Optional[str] = None
    conda_env: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "python_interpreter": self.python_interpreter,
            "training_script": self.training_script,
            "project_root": self.project_root,
            "conda_env": self.conda_env,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PathsConfig":
        """Create from dictionary."""
        return cls(
            python_interpreter=data.get("python_interpreter"),
            training_script=data.get("training_script"),
            project_root=data.get("project_root"),
            conda_env=data.get("conda_env"),
        )


@dataclass
class LoggingConfig:
    """Logging configuration settings."""

    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_logging: bool = True
    console_logging: bool = True
    max_log_size: str = "100MB"
    backup_count: int = 5

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "level": self.level,
            "format": self.format,
            "file_logging": self.file_logging,
            "console_logging": self.console_logging,
            "max_log_size": self.max_log_size,
            "backup_count": self.backup_count,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LoggingConfig":
        """Create from dictionary."""
        return cls(
            level=data.get("level", "INFO"),
            format=data.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
            file_logging=data.get("file_logging", True),
            console_logging=data.get("console_logging", True),
            max_log_size=data.get("max_log_size", "100MB"),
            backup_count=data.get("backup_count", 5),
        )


@dataclass
class ExperimentTrackingConfig:
    """Experiment tracking configuration (W&B, MLflow, etc.)."""

    wandb_enabled: bool = False
    wandb_project: Optional[str] = None
    wandb_entity: Optional[str] = None
    mlflow_enabled: bool = False
    mlflow_tracking_uri: Optional[str] = None
    tensorboard_enabled: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "wandb_enabled": self.wandb_enabled,
            "wandb_project": self.wandb_project,
            "wandb_entity": self.wandb_entity,
            "mlflow_enabled": self.mlflow_enabled,
            "mlflow_tracking_uri": self.mlflow_tracking_uri,
            "tensorboard_enabled": self.tensorboard_enabled,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExperimentTrackingConfig":
        """Create from dictionary."""
        return cls(
            wandb_enabled=data.get("wandb_enabled", False),
            wandb_project=data.get("wandb_project"),
            wandb_entity=data.get("wandb_entity"),
            mlflow_enabled=data.get("mlflow_enabled", False),
            mlflow_tracking_uri=data.get("mlflow_tracking_uri"),
            tensorboard_enabled=data.get("tensorboard_enabled", False),
        )


@dataclass
class ComputeConfig:
    """Compute-related configuration settings."""

    max_concurrent_local: int = 4
    max_concurrent_remote: int = 10
    default_timeout: int = 3600  # 1 hour
    retry_attempts: int = 3
    retry_delay: int = 60  # 1 minute
    health_check_interval: int = 300  # 5 minutes
    result_collection_interval: int = 60  # 1 minute

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "max_concurrent_local": self.max_concurrent_local,
            "max_concurrent_remote": self.max_concurrent_remote,
            "default_timeout": self.default_timeout,
            "retry_attempts": self.retry_attempts,
            "retry_delay": self.retry_delay,
            "health_check_interval": self.health_check_interval,
            "result_collection_interval": self.result_collection_interval,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ComputeConfig":
        """Create from dictionary."""
        return cls(
            max_concurrent_local=data.get("max_concurrent_local", 4),
            max_concurrent_remote=data.get("max_concurrent_remote", 10),
            default_timeout=data.get("default_timeout", 3600),
            retry_attempts=data.get("retry_attempts", 3),
            retry_delay=data.get("retry_delay", 60),
            health_check_interval=data.get("health_check_interval", 300),
            result_collection_interval=data.get("result_collection_interval", 60),
        )


@dataclass
class HSMConfig:
    """Main HSM configuration class.

    This class manages all HSM-specific configuration settings and provides
    methods for loading, saving, and accessing configuration data.
    """

    hpc: HPCConfig = field(default_factory=HPCConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    experiment_tracking: ExperimentTrackingConfig = field(default_factory=ExperimentTrackingConfig)
    compute: ComputeConfig = field(default_factory=ComputeConfig)

    @classmethod
    def load(cls, config_path: Optional[Path] = None) -> Optional["HSMConfig"]:
        """Load HSM configuration from file.

        Args:
            config_path: Path to hsm_config.yaml file. If None, searches standard locations.

        Returns:
            HSMConfig instance or None if not found
        """
        if config_path is None:
            # Search for hsm_config.yaml in standard locations
            search_paths = [
                Path.cwd() / "sweeps" / "hsm_config.yaml",
                Path.cwd() / "hsm_config.yaml",
                Path("sweeps") / "hsm_config.yaml",
                Path("hsm_config.yaml"),
                Path.home() / ".hsm" / "config.yaml",
            ]

            for path in search_paths:
                if path.exists():
                    config_path = path
                    break

        if config_path is None or not config_path.exists():
            logger.debug("No hsm_config.yaml found in standard locations")
            return None

        try:
            with open(config_path) as f:
                config_data = yaml.safe_load(f)

            if config_data is None:
                config_data = {}

            logger.debug(f"Loaded HSM config from {config_path}")
            return cls.from_dict(config_data)

        except Exception as e:
            logger.warning(f"Failed to load HSM config from {config_path}: {e}")
            return None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HSMConfig":
        """Create HSMConfig from dictionary.

        Args:
            data: Dictionary containing configuration data

        Returns:
            HSMConfig instance
        """
        return cls(
            hpc=HPCConfig.from_dict(data.get("hpc", {})),
            paths=PathsConfig.from_dict(data.get("paths", {})),
            logging=LoggingConfig.from_dict(data.get("logging", {})),
            experiment_tracking=ExperimentTrackingConfig.from_dict(
                data.get("experiment_tracking", {})
            ),
            compute=ComputeConfig.from_dict(data.get("compute", {})),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation.

        Returns:
            Dictionary representation of the HSM config
        """
        return {
            "hpc": self.hpc.to_dict(),
            "paths": self.paths.to_dict(),
            "logging": self.logging.to_dict(),
            "experiment_tracking": self.experiment_tracking.to_dict(),
            "compute": self.compute.to_dict(),
        }

    def save(self, output_path: Union[str, Path]) -> None:
        """Save HSM config to YAML file.

        Args:
            output_path: Path where to save the configuration
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, indent=2)

        logger.debug(f"Saved HSM config to {output_path}")

    def get_default_walltime(self) -> str:
        """Get default walltime from HPC config."""
        return self.hpc.default_walltime

    def get_default_resources(self) -> str:
        """Get default resources from HPC config."""
        return self.hpc.default_resources

    def get_default_python_path(self) -> Optional[str]:
        """Get default Python interpreter path."""
        return self.paths.python_interpreter

    def get_default_script_path(self) -> Optional[str]:
        """Get default training script path."""
        return self.paths.training_script

    def get_project_root(self) -> Optional[str]:
        """Get project root path."""
        return self.paths.project_root

    def get_conda_env(self) -> Optional[str]:
        """Get conda environment name."""
        return self.paths.conda_env

    def get_wandb_config(self) -> Dict[str, Any]:
        """Get W&B configuration."""
        return {
            "enabled": self.experiment_tracking.wandb_enabled,
            "project": self.experiment_tracking.wandb_project,
            "entity": self.experiment_tracking.wandb_entity,
        }

    def get_hpc_system(self) -> Optional[str]:
        """Get HPC system name."""
        return self.hpc.system

    def get_max_array_size(self) -> Optional[int]:
        """Get maximum HPC array job size."""
        return self.hpc.max_array_size

    def get_module_commands(self) -> List[str]:
        """Get HPC module commands."""
        return self.hpc.module_commands.copy()

    def create_default_config(self, output_path: Union[str, Path]) -> None:
        """Create a default configuration file with comments.

        Args:
            output_path: Path where to save the default configuration
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Create a configuration file with comments
        config_content = """# HSM Configuration File
# This file contains default settings for the HPC Sweep Manager

# HPC-specific settings
hpc:
  default_walltime: "23:59:59"  # Default job walltime
  default_resources: "select=1:ncpus=4:mem=64gb"  # Default resource allocation
  default_queue: null  # Default job queue (null for system default)
  max_array_size: null  # Maximum array job size (null for unlimited)
  system: null  # HPC system name (e.g., "pbs", "slurm")
  module_commands: []  # Module commands to run before job execution

# Path settings
paths:
  python_interpreter: null  # Path to Python interpreter (null for auto-detect)
  training_script: null  # Default training script path
  project_root: null  # Project root directory
  conda_env: null  # Conda environment name

# Logging configuration
logging:
  level: "INFO"  # Log level (DEBUG, INFO, WARNING, ERROR)
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file_logging: true  # Enable file logging
  console_logging: true  # Enable console logging
  max_log_size: "100MB"  # Maximum log file size
  backup_count: 5  # Number of backup log files

# Experiment tracking
experiment_tracking:
  wandb_enabled: false  # Enable Weights & Biases
  wandb_project: null  # W&B project name
  wandb_entity: null  # W&B entity/team name
  mlflow_enabled: false  # Enable MLflow
  mlflow_tracking_uri: null  # MLflow tracking URI
  tensorboard_enabled: false  # Enable TensorBoard

# Compute settings
compute:
  max_concurrent_local: 4  # Maximum concurrent local tasks
  max_concurrent_remote: 10  # Maximum concurrent remote tasks
  default_timeout: 3600  # Default task timeout (seconds)
  retry_attempts: 3  # Number of retry attempts for failed tasks
  retry_delay: 60  # Delay between retries (seconds)
  health_check_interval: 300  # Health check interval (seconds)
  result_collection_interval: 60  # Result collection interval (seconds)
"""

        with open(output_path, "w") as f:
            f.write(config_content)

        logger.info(f"Created default HSM config at {output_path}")

    def __str__(self) -> str:
        """String representation of the HSM configuration."""
        return f"HSMConfig(hpc_system={self.hpc.system}, paths_configured={bool(self.paths.python_interpreter)})"

    def __repr__(self) -> str:
        """Detailed representation of the HSM configuration."""
        return f"HSMConfig({self.to_dict()})"


# Convenience function for backward compatibility
def load_hsm_config(config_path: Optional[Path] = None) -> Optional[HSMConfig]:
    """Load HSM configuration from file.

    Args:
        config_path: Path to configuration file

    Returns:
        HSMConfig instance or None if not found
    """
    return HSMConfig.load(config_path)
