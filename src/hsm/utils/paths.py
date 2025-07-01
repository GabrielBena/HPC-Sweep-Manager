"""Path detection and project structure utilities."""

import logging
import os
from pathlib import Path
import shutil
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class PathDetector:
    """Auto-detect project structure and paths."""

    def __init__(self, project_root: Optional[Path] = None):
        if project_root is None:
            project_root = Path.cwd()
        self.project_root = Path(project_root)

    def detect_config_dir(self) -> Optional[Path]:
        """Find Hydra config directory."""
        candidates = ["configs", "conf", "config", "cfg"]
        for candidate in candidates:
            path = self.project_root / candidate
            if path.exists() and path.is_dir():
                # Check if it contains YAML files
                if any(path.glob("*.yaml")) or any(path.glob("**/*.yaml")):
                    return path
        return None

    def detect_training_script(self) -> Optional[Path]:
        """Find training script."""
        candidates = [
            "scripts/train.py",
            "src/train.py",
            "train.py",
            "main.py",
            "run.py",
            "scripts/main.py",
            "src/main.py",
        ]

        for candidate in candidates:
            path = self.project_root / candidate
            if path.exists() and path.is_file():
                return path

        # Also search for any python files with common training script patterns
        for pattern in ["*train*.py", "*main*.py", "*run*.py"]:
            matches = list(self.project_root.rglob(pattern))
            if matches:
                # Return the first match in a reasonable location
                for match in matches:
                    if not any(part.startswith(".") for part in match.parts):
                        return match

        return None

    def detect_python_interpreter(self) -> Optional[Path]:
        """Detect Python interpreter."""
        # First, try current environment
        current_python = shutil.which("python")
        if current_python:
            return Path(current_python)

        # Try python3
        python3 = shutil.which("python3")
        if python3:
            return Path(python3)

        # Check common conda/mamba environment paths
        home = Path.home()
        conda_paths = [
            home / "miniconda3" / "bin" / "python",
            home / "anaconda3" / "bin" / "python",
            home / "mambaforge" / "bin" / "python",
            home / "miniforge3" / "bin" / "python",
        ]

        for path in conda_paths:
            if path.exists():
                return path

        # Check for environment-specific Python
        conda_env = os.environ.get("CONDA_DEFAULT_ENV")
        if conda_env and conda_env != "base":
            for base_path in [
                home / "miniconda3",
                home / "anaconda3",
                home / "mambaforge",
            ]:
                env_python = base_path / "envs" / conda_env / "bin" / "python"
                if env_python.exists():
                    return env_python

        return None

    def detect_conda_environment(self) -> Optional[str]:
        """Detect current conda environment."""
        # Check environment variables
        conda_env = os.environ.get("CONDA_DEFAULT_ENV")
        if conda_env:
            return conda_env

        # Try to get from conda command
        try:
            import subprocess

            result = subprocess.run(
                ["conda", "info", "--envs"], capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                # Look for the active environment marked with *
                for line in result.stdout.split("\n"):
                    if "*" in line:
                        env_name = line.split()[0]
                        if env_name != "base":
                            return env_name
        except (subprocess.SubprocessError, FileNotFoundError):
            pass

        return None

    def detect_hpc_system(self) -> str:
        """Detect HPC system type."""
        # Check for PBS/Torque
        if shutil.which("qstat") and shutil.which("qsub"):
            return "pbs"

        # Check for Slurm
        if shutil.which("sinfo") or shutil.which("sbatch"):
            return "slurm"

        # Check for SGE
        if shutil.which("qstat") and shutil.which("qsub"):
            # Need to distinguish from PBS - check for SGE-specific commands
            if shutil.which("qhost"):
                return "sge"

        # Default to local if no HPC system found
        return "local"

    def detect_output_dir(self) -> Path:
        """Detect or suggest output directory."""
        candidates = ["outputs", "results", "experiments", "logs", "sweeps/outputs"]

        for candidate in candidates:
            path = self.project_root / candidate
            if path.exists() and path.is_dir():
                return path

        # Default to creating 'sweeps/outputs' directory
        return self.project_root / "sweeps" / "outputs"

    def detect_storage_paths(self) -> Dict[str, Optional[Path]]:
        """Detect common HPC storage paths."""
        paths = {}

        # Check for common HPC storage patterns
        user = os.environ.get("USER", "unknown")

        # Imperial College CX3/RDS patterns
        rds_patterns = [
            f"/rds/general/user/{user}/home",
            f"/rds/general/user/{user}/projects",
            f"/rds/general/user/{user}/ephemeral",
        ]

        for pattern in rds_patterns:
            path = Path(pattern)
            if path.exists():
                paths[f"rds_{path.name}"] = path

        # Common scratch directories
        scratch_patterns = [
            f"/scratch/{user}",
            f"/tmp/{user}",
            f"/local/scratch/{user}",
        ]

        for pattern in scratch_patterns:
            path = Path(pattern)
            if path.exists():
                paths["scratch"] = path
                break

        return paths

    def detect_hydra_configs(self) -> List[Path]:
        """Find all Hydra config files in the project."""
        config_files = []

        # Look in config directory first
        config_dir = self.detect_config_dir()
        if config_dir:
            # Main config files
            for pattern in ["config.yaml", "config.yml", "main.yaml", "main.yml"]:
                config_file = config_dir / pattern
                if config_file.exists():
                    config_files.append(config_file)

            # Also check subdirectories for more configs
            for config_file in config_dir.rglob("*.yaml"):
                if config_file not in config_files:
                    config_files.append(config_file)
            for config_file in config_dir.rglob("*.yml"):
                if config_file not in config_files:
                    config_files.append(config_file)

        # Also look for configs in common locations
        common_locations = [
            self.project_root / "config.yaml",
            self.project_root / "config.yml",
            self.project_root / "conf" / "config.yaml",
            self.project_root / "cfg" / "config.yaml",
        ]

        for config_file in common_locations:
            if config_file.exists() and config_file not in config_files:
                config_files.append(config_file)

        return config_files

    def extract_config_parameters(self) -> Dict[str, Any]:
        """Extract parameters from existing Hydra config files."""
        import yaml

        config_files = self.detect_hydra_configs()
        extracted_params = {}

        for config_file in config_files:
            try:
                with open(config_file) as f:
                    config_data = yaml.safe_load(f)

                if config_data:
                    # Extract interesting parameters for sweep potential
                    params = self._extract_sweep_candidates(config_data, str(config_file))
                    extracted_params[str(config_file.relative_to(self.project_root))] = params

            except Exception as e:
                logger.debug(f"Could not parse config file {config_file}: {e}")

        return extracted_params

    def _extract_sweep_candidates(
        self, config_data: Dict[str, Any], source_file: str
    ) -> Dict[str, Any]:
        """Extract parameters that are good candidates for sweeping."""
        candidates = {}

        # Flatten the config to find numeric/categorical parameters
        flattened = self._flatten_dict(config_data)

        # Look for common parameters that are often swept
        sweep_worthy_patterns = [
            "lr",
            "learning_rate",
            "learning_rates",
            "batch_size",
            "batch_sizes",
            "hidden_dim",
            "hidden_size",
            "embed_dim",
            "dropout",
            "dropout_rate",
            "weight_decay",
            "l2_reg",
            "epochs",
            "max_epochs",
            "seed",
            "random_seed",
            "gamma",
            "alpha",
            "beta",
            "temperature",
            "tau",
            "momentum",
            "optimizer",
            "scheduler",
            "activation",
            "loss_function",
        ]

        for key, value in flattened.items():
            # Skip certain keys
            if any(skip in key.lower() for skip in ["dir", "path", "file", "name", "id"]):
                continue

            # Include if it matches common patterns
            if (
                any(pattern in key.lower() for pattern in sweep_worthy_patterns)
                or isinstance(value, (int, float))
                and not isinstance(value, bool)
            ):
                candidates[key] = {
                    "current_value": value,
                    "type": type(value).__name__,
                    "source": source_file,
                }
            # Include string values that look like choices
            elif isinstance(value, str) and value.lower() in [
                "adam",
                "sgd",
                "adamw",
                "rmsprop",  # optimizers
                "relu",
                "gelu",
                "tanh",
                "sigmoid",  # activations
                "mse",
                "cross_entropy",
                "bce",  # losses
                "cosine",
                "step",
                "exponential",  # schedulers
            ]:
                candidates[key] = {
                    "current_value": value,
                    "type": "categorical",
                    "source": source_file,
                }

        return candidates

    def suggest_sweep_parameters(self) -> Dict[str, Any]:
        """Suggest parameters for sweeping based on detected configs."""
        extracted = self.extract_config_parameters()
        suggestions = {}

        for config_file, params in extracted.items():
            for param_name, param_info in params.items():
                current_value = param_info["current_value"]
                param_type = param_info["type"]

                # Suggest sweep values based on type and current value
                if param_type in ["int", "float"]:
                    if "lr" in param_name.lower() or "learning_rate" in param_name.lower():
                        # Learning rate suggestions
                        if isinstance(current_value, float):
                            suggestions[param_name] = [
                                current_value * 0.1,
                                current_value * 0.5,
                                current_value,
                                current_value * 2,
                                current_value * 10,
                            ]
                    elif "batch_size" in param_name.lower():
                        # Batch size suggestions
                        if isinstance(current_value, int):
                            suggestions[param_name] = [
                                max(1, current_value // 2),
                                current_value,
                                current_value * 2,
                            ]
                    elif "seed" in param_name.lower():
                        # Multiple seeds
                        suggestions[param_name] = [1, 2, 3, 4, 5]
                    elif "dropout" in param_name.lower():
                        # Dropout rates
                        suggestions[param_name] = [0.0, 0.1, 0.2, 0.3, 0.5]
                    else:
                        # Generic numeric suggestions
                        if isinstance(current_value, (int, float)):
                            suggestions[param_name] = [
                                current_value * 0.5,
                                current_value,
                                current_value * 2,
                            ]
                elif param_type == "categorical":
                    # Add common alternatives for categorical values
                    if "optimizer" in param_name.lower():
                        suggestions[param_name] = ["adam", "sgd", "adamw"]
                    elif "activation" in param_name.lower():
                        suggestions[param_name] = ["relu", "gelu", "tanh"]

        return suggestions

    def get_project_info(self) -> Dict[str, Any]:
        """Get comprehensive project information."""
        info = {
            "project_root": self.project_root,
            "project_name": self.project_root.name,
            "config_dir": self.detect_config_dir(),
            "training_script": self.detect_training_script(),
            "python_interpreter": self.detect_python_interpreter(),
            "conda_env": self.detect_conda_environment(),
            "output_dir": self.detect_output_dir(),
            "hpc_system": self.detect_hpc_system(),
            "storage_paths": self.detect_storage_paths(),
        }

        # Add some metadata
        info["has_git"] = (self.project_root / ".git").exists()
        info["has_requirements"] = (self.project_root / "requirements.txt").exists()
        info["has_pyproject"] = (self.project_root / "pyproject.toml").exists()
        info["has_conda_env"] = (self.project_root / "environment.yml").exists()

        # Add config detection
        info["hydra_configs"] = self.detect_hydra_configs()
        info["config_parameters"] = self.extract_config_parameters()
        info["sweep_suggestions"] = self.suggest_sweep_parameters()

        return info

    def validate_paths(self) -> List[str]:
        """Validate detected paths and return any issues."""
        issues = []

        config_dir = self.detect_config_dir()
        if not config_dir:
            issues.append("No Hydra config directory found (expected 'configs', 'conf', etc.)")

        training_script = self.detect_training_script()
        if not training_script:
            issues.append("No training script found (expected 'train.py', 'main.py', etc.)")

        python_path = self.detect_python_interpreter()
        if not python_path:
            issues.append("No Python interpreter found")
        elif not python_path.exists():
            issues.append(f"Python interpreter not found at: {python_path}")

        hpc_system = self.detect_hpc_system()
        if hpc_system == "local":
            issues.append("No HPC system detected - only local execution available")

        return issues

    def suggest_setup(self) -> Dict[str, str]:
        """Suggest setup commands based on detected environment."""
        suggestions = {}

        info = self.get_project_info()

        if not info["has_requirements"] and not info["has_pyproject"] and not info["has_conda_env"]:
            suggestions["dependencies"] = (
                "Consider creating requirements.txt, environment.yml, or pyproject.toml"
            )

        if not info["config_dir"]:
            suggestions["configs"] = "Create a 'configs' directory with Hydra configuration files"

        if not info["training_script"]:
            suggestions["training"] = (
                "Create a training script (e.g., scripts/train.py or train.py)"
            )

        if info["hpc_system"] == "local":
            suggestions["hpc"] = (
                "No HPC system detected - install PBS/Torque or Slurm for cluster execution"
            )

        return suggestions

    def get_default_resources(self, hpc_system: str) -> str:
        """Get default HPC resource specifications."""
        if hpc_system == "pbs":
            return "select=1:ncpus=4:mem=16gb"
        elif hpc_system == "slurm":
            return "--nodes=1 --ntasks=4 --mem=16G"
        else:
            return ""

    def _flatten_dict(
        self, d: Dict[str, Any], parent_key: str = "", sep: str = "."
    ) -> Dict[str, Any]:
        """Flatten nested dictionary with dot notation."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
