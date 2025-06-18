"""HSM configuration loading utilities."""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class HSMConfig:
    """HSM configuration loader and manager."""

    def __init__(self, config_data: Dict[str, Any]):
        """Initialize with configuration data."""
        self.config_data = config_data

    @classmethod
    def load(cls, config_path: Optional[Path] = None) -> Optional["HSMConfig"]:
        """
        Load HSM configuration from file.

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
            ]

            for path in search_paths:
                if path.exists():
                    config_path = path
                    break

        if config_path is None or not config_path.exists():
            logger.debug("No hsm_config.yaml found in standard locations")
            return None

        try:
            with open(config_path, "r") as f:
                config_data = yaml.safe_load(f)

            logger.debug(f"Loaded HSM config from {config_path}")
            return cls(config_data)

        except Exception as e:
            logger.warning(f"Failed to load HSM config from {config_path}: {e}")
            return None

    def get_default_walltime(self) -> str:
        """Get default walltime from config."""
        return self.config_data.get("hpc", {}).get("default_walltime", "23:59:59")

    def get_default_resources(self) -> str:
        """Get default resources from config."""
        return self.config_data.get("hpc", {}).get(
            "default_resources", "select=1:ncpus=4:mem=64gb"
        )

    def get_default_python_path(self) -> Optional[str]:
        """Get default Python interpreter path from config."""
        return self.config_data.get("paths", {}).get("python_interpreter")

    def get_default_script_path(self) -> Optional[str]:
        """Get default training script path from config."""
        return self.config_data.get("paths", {}).get("train_script")

    def get_project_root(self) -> Optional[str]:
        """Get project root directory from config."""
        return self.config_data.get("project", {}).get("root")

    def get_wandb_config(self) -> Dict[str, Any]:
        """Get wandb configuration from config."""
        return self.config_data.get("wandb", {})

    def get_hpc_system(self) -> Optional[str]:
        """Get HPC system type from config."""
        return self.config_data.get("hpc", {}).get("system")

    def get_max_array_size(self) -> Optional[int]:
        """Get maximum array size from config."""
        return self.config_data.get("hpc", {}).get("max_array_size")
