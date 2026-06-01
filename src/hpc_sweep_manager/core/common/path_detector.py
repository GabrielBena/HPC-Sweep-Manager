"""Auto-detect project structure and paths."""

import os
from pathlib import Path
import shutil
from typing import Any, Dict, List, Optional


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

    # Non-hidden directories that match the entrypoint patterns but never hold
    # the user's training script — vendored deps and caches. (Hidden dirs like
    # .venv/.git/.tox are already excluded by the leading-dot check.) We do NOT
    # hard-code 'env'/'venv'/'build'/'dist': those collide with legitimate
    # source dirs — e.g. RL repos keep gym environments under env/ — so a
    # name-match there would silently hide a real entrypoint. Actual
    # virtualenvs are recognized structurally instead (see _is_virtualenv_dir).
    _SCRIPT_SKIP_DIRS = frozenset({"__pycache__", "node_modules", "site-packages"})

    @staticmethod
    def _is_virtualenv_dir(path: Path) -> bool:
        """True if ``path`` looks like a Python virtualenv (regardless of name)."""
        return (path / "pyvenv.cfg").exists() or (path / "bin" / "activate").exists()

    def _is_skippable_script(self, match: Path) -> bool:
        """Whether an rglob hit lives somewhere we should never auto-detect from.

        Skips hidden dirs, vendored/cache dirs, and anything inside a detected
        virtualenv — but not legitimately-named source dirs.
        """
        rel_parts = match.relative_to(self.project_root).parts
        if any(p.startswith(".") for p in rel_parts):
            return True
        ancestor = self.project_root
        for part in rel_parts[:-1]:
            ancestor = ancestor / part
            if part in self._SCRIPT_SKIP_DIRS or self._is_virtualenv_dir(ancestor):
                return True
        return False

    def detect_train_script_candidates(self) -> List[Path]:
        """Return all plausible training-script entrypoints, best guess first.

        ``candidates[0]`` is exactly what :meth:`detect_train_script` returns.
        Callers that drive setup (``hsm setup init``) should warn when more
        than one candidate exists — otherwise a repo that ships both
        ``scripts/train.py`` and ``scripts/train_2d.py`` silently points the
        user at whichever sorts first, against the wrong Hydra config.
        """
        seen: set = set()
        found: List[Path] = []

        def _add(path: Path) -> None:
            if not path.is_file():
                return
            resolved = path.resolve()
            if resolved in seen:
                return
            seen.add(resolved)
            found.append(path)

        # Priority candidates in canonical locations (ordered).
        for candidate in [
            "scripts/train.py",
            "src/train.py",
            "train.py",
            "main.py",
            "run.py",
            "scripts/main.py",
            "src/main.py",
        ]:
            _add(self.project_root / candidate)

        # Then anything else matching common entrypoint patterns. Sorted for
        # determinism; vendored/cache/hidden paths and virtualenvs are skipped.
        for pattern in ["*train*.py", "*main*.py", "*run*.py"]:
            for match in sorted(self.project_root.rglob(pattern)):
                if self._is_skippable_script(match):
                    continue
                _add(match)

        return found

    def detect_train_script(self) -> Optional[Path]:
        """Find the single best-guess training script (first candidate)."""
        candidates = self.detect_train_script_candidates()
        return candidates[0] if candidates else None

    def detect_python_path(self) -> Optional[Path]:
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

    def detect_output_dir(self) -> Path:
        """Detect or suggest output directory."""
        candidates = ["outputs", "results", "experiments", "logs"]

        for candidate in candidates:
            path = self.project_root / candidate
            if path.exists() and path.is_dir():
                return path

        # Default to creating 'outputs' directory
        return self.project_root / "outputs"

    def detect_hpc_system(self) -> str:
        """Detect HPC system type."""
        # Check for PBS/Torque
        if shutil.which("qstat"):
            return "pbs"

        # Check for Slurm
        if shutil.which("sinfo") or shutil.which("sbatch"):
            return "slurm"

        # Check for SGE
        if shutil.which("qstat") and shutil.which("qsub"):
            return "sge"

        # Default to PBS
        return "pbs"

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

    def get_project_info(self) -> Dict[str, Any]:
        """Get comprehensive project information."""
        info = {
            "project_root": self.project_root,
            "config_dir": self.detect_config_dir(),
            "train_script": self.detect_train_script(),
            "train_script_candidates": self.detect_train_script_candidates(),
            "python_path": self.detect_python_path(),
            "output_dir": self.detect_output_dir(),
            "hpc_system": self.detect_hpc_system(),
            "storage_paths": self.detect_storage_paths(),
        }

        # Add some metadata
        info["has_git"] = (self.project_root / ".git").exists()
        info["has_requirements"] = (self.project_root / "requirements.txt").exists()
        info["has_pyproject"] = (self.project_root / "pyproject.toml").exists()
        info["has_conda_env"] = (self.project_root / "environment.yml").exists()

        return info

    def validate_paths(self) -> List[str]:
        """Validate detected paths and return any issues."""
        issues = []

        config_dir = self.detect_config_dir()
        if not config_dir:
            issues.append("No Hydra config directory found")

        train_script = self.detect_train_script()
        if not train_script:
            issues.append("No training script found")

        python_path = self.detect_python_path()
        if not python_path:
            issues.append("No Python interpreter found")
        elif not python_path.exists():
            issues.append(f"Python interpreter not found at: {python_path}")

        return issues

    def suggest_setup(self) -> Dict[str, str]:
        """Suggest setup commands based on detected environment."""
        suggestions = {}

        info = self.get_project_info()

        if not info["has_requirements"] and not info["has_pyproject"] and not info["has_conda_env"]:
            suggestions["dependencies"] = "Consider creating requirements.txt or environment.yml"

        if not info["config_dir"]:
            suggestions["configs"] = "Create a 'configs' directory with Hydra configuration files"

        if not info["train_script"]:
            suggestions["training"] = (
                "Create a training script (e.g., scripts/train.py or train.py)"
            )

        if info["hpc_system"] == "unknown":
            suggestions["hpc"] = "HPC system not detected - manual configuration may be needed"

        return suggestions
