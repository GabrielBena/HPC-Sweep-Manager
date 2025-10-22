"""Sync configuration management for HPC Sweep Manager.

Handles loading, validation, and management of sync targets for results syncing.
"""

from dataclasses import dataclass, field
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


@dataclass
class SyncTarget:
    """Configuration for a sync target (destination machine)."""

    name: str
    host: str
    user: str
    paths: Dict[str, str] = field(default_factory=dict)
    options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate and set defaults for sync target."""
        # Ensure required paths exist
        required_paths = ["sweeps", "wandb"]
        for path_key in required_paths:
            if path_key not in self.paths:
                raise ValueError(f"Missing required path '{path_key}' for target '{self.name}'")

        # Set default options
        default_options = {
            "sync_sweep_metadata": True,
            "sync_wandb_runs": True,
            "latest_only": True,
            "max_depth": 1,
            "parallel_transfers": 4,
            "check_remote_first": True,
        }
        for key, value in default_options.items():
            if key not in self.options:
                self.options[key] = value

    @property
    def ssh_host(self) -> str:
        """Get full SSH host string (user@host)."""
        return f"{self.user}@{self.host}"

    def get_path(self, path_type: str, default: Optional[str] = None) -> str:
        """Get path by type with optional default."""
        return self.paths.get(path_type, default or "")

    def get_option(self, option_name: str, default: Any = None) -> Any:
        """Get option by name with optional default."""
        return self.options.get(option_name, default)


class SyncConfig:
    """Manages sync configuration for the project."""

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize sync configuration.

        Args:
            config_path: Path to sync_config.yaml. If None, searches standard locations.
        """
        self.config_path = config_path or self._find_config_file()
        self.targets: Dict[str, SyncTarget] = {}
        self.default_target: Optional[str] = None

        if self.config_path and self.config_path.exists():
            self._load_config()

    @staticmethod
    def _find_config_file() -> Optional[Path]:
        """Find sync_config.yaml in standard locations."""
        search_paths = [
            Path.cwd() / ".hsm" / "sync_config.yaml",
            Path.cwd() / "sweeps" / "sync_config.yaml",  # Legacy location
            Path(".hsm") / "sync_config.yaml",
        ]

        for path in search_paths:
            if path.exists():
                logger.debug(f"Found sync_config.yaml at: {path}")
                return path

        logger.debug("No sync_config.yaml found in standard locations")
        return None

    def _load_config(self):
        """Load configuration from YAML file."""
        try:
            with open(self.config_path) as f:
                config_data = yaml.safe_load(f)

            if not config_data:
                logger.warning(f"Empty sync config file: {self.config_path}")
                return

            # Load sync targets
            targets_data = config_data.get("sync_targets", {})
            for target_name, target_config in targets_data.items():
                try:
                    self.targets[target_name] = SyncTarget(
                        name=target_name,
                        host=target_config["host"],
                        user=target_config["user"],
                        paths=target_config.get("paths", {}),
                        options=target_config.get("options", {}),
                    )
                    logger.debug(f"Loaded sync target: {target_name}")
                except (KeyError, ValueError) as e:
                    logger.error(f"Invalid sync target '{target_name}': {e}")

            # Load default target
            self.default_target = config_data.get("default_target")

            logger.info(f"Loaded {len(self.targets)} sync target(s) from {self.config_path}")

        except Exception as e:
            logger.error(f"Failed to load sync config from {self.config_path}: {e}")
            raise

    def get_target(self, target_name: Optional[str] = None) -> Optional[SyncTarget]:
        """
        Get a sync target by name.

        Args:
            target_name: Name of the target. If None, returns default target.

        Returns:
            SyncTarget if found, None otherwise.
        """
        if target_name is None:
            target_name = self.default_target

        if target_name is None:
            logger.warning("No target specified and no default target configured")
            return None

        return self.targets.get(target_name)

    def list_targets(self) -> List[str]:
        """Get list of all configured target names."""
        return list(self.targets.keys())

    def has_targets(self) -> bool:
        """Check if any targets are configured."""
        return len(self.targets) > 0

    @staticmethod
    def create_template(output_path: Path, project_name: str = "my-project") -> bool:
        """
        Create a template sync_config.yaml file.

        Args:
            output_path: Path where to create the template
            project_name: Project name for default paths

        Returns:
            True if successful, False otherwise
        """
        template = {
            "# Sync configuration for HPC Sweep Manager": None,
            "# Define sync targets (destination machines) for sweep results": None,
            "default_target": "desktop",
            "sync_targets": {
                "desktop": {
                    "host": "your-desktop.example.com",
                    "user": "your_username",
                    "paths": {
                        "sweeps": f"/home/your_username/projects/{project_name}/sweeps",
                        "wandb": f"/home/your_username/projects/{project_name}/wandb",
                        "project_root": f"/home/your_username/projects/{project_name}",
                    },
                    "options": {
                        "sync_sweep_metadata": True,
                        "sync_wandb_runs": True,
                        "latest_only": True,
                        "max_depth": 1,
                        "parallel_transfers": 4,
                        "check_remote_first": True,
                    },
                },
                "laptop": {
                    "host": "laptop.local",
                    "user": "your_username",
                    "paths": {
                        "sweeps": f"/Users/your_username/projects/{project_name}/sweeps",
                        "wandb": f"/Users/your_username/projects/{project_name}/wandb",
                        "project_root": f"/Users/your_username/projects/{project_name}",
                    },
                    "options": {
                        "sync_sweep_metadata": True,
                        "sync_wandb_runs": False,  # Limited space on laptop
                        "latest_only": True,
                        "max_depth": 1,
                    },
                },
            },
        }

        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w") as f:
                # Write header comments
                f.write("# Sync Configuration for HPC Sweep Manager\n")
                f.write("#\n")
                f.write("# Define sync targets (destination machines) for sweep results\n")
                f.write("#\n")
                f.write("# Options:\n")
                f.write("#   sync_sweep_metadata: Sync sweep config and metadata\n")
                f.write("#   sync_wandb_runs: Sync wandb run directories\n")
                f.write("#   latest_only: Only sync latest artifact versions\n")
                f.write("#   max_depth: Maximum directory depth for sweep metadata sync\n")
                f.write("#   parallel_transfers: Number of parallel rsync transfers\n")
                f.write("#   check_remote_first: Check what exists on remote before syncing\n")
                f.write("\n")

                # Write config (skip comment keys)
                config_to_write = {k: v for k, v in template.items() if not k.startswith("#")}
                yaml.dump(config_to_write, f, default_flow_style=False, indent=2, sort_keys=False)

            logger.info(f"Created sync config template at: {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to create sync config template: {e}")
            return False
