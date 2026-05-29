"""HSM configuration loading utilities."""

from dataclasses import dataclass, field
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

try:
    from omegaconf import DictConfig, OmegaConf

    OMEGACONF_AVAILABLE = True
except ImportError:
    OMEGACONF_AVAILABLE = False
    DictConfig = None


logger = logging.getLogger(__name__)


MACHINE_CONFIG_PATH = Path.home() / ".hsm" / "config.yaml"
"""Per-machine HSM defaults — sibling to the per-project ``.hsm/config.yaml``.

Lives outside any project so machine-specific facts (filesystem layout,
GPU inventory, default conda env) can be set once and reused by every
project on this user account. Only the ``local:`` block is honored;
``slurm:``/``distributed:`` here are dropped with a warning (project
concerns).
"""

# Top-level keys the machine config is allowed to set. Anything else is
# considered project scope and dropped with a warning at load time —
# putting `distributed:` here would mean "every project on this machine
# silently sees the same remotes," which is surprising.
_MACHINE_CONFIG_ALLOWED_TOP_LEVEL = {"local"}


def _load_yaml_dict(path: Path) -> Optional[Dict[str, Any]]:
    """Read a YAML file as a dict, returning ``None`` on any failure.

    Empty / comment-only files load as ``None`` from yaml; we coerce to
    ``{}`` so downstream merge logic doesn't need to special-case them.
    """
    try:
        with open(path) as f:
            data = yaml.safe_load(f)
    except Exception as e:
        logger.warning(f"Failed to load HSM config from {path}: {e}")
        return None
    if data is None:
        return {}
    if not isinstance(data, dict):
        logger.warning(
            f"HSM config at {path} is not a YAML mapping (got {type(data).__name__}); ignoring."
        )
        return None
    return data


def _scope_machine_config(data: Dict[str, Any], path: Path) -> Dict[str, Any]:
    """Drop non-``local:`` top-level keys from the machine config with a warning."""
    rejected = set(data) - _MACHINE_CONFIG_ALLOWED_TOP_LEVEL
    if rejected:
        logger.warning(
            f"Machine config at {path} has top-level keys {sorted(rejected)!r} "
            f"that are not honored — only `local:` is read from the machine file. "
            f"Move project-level settings to <project>/.hsm/config.yaml."
        )
    return {k: v for k, v in data.items() if k in _MACHINE_CONFIG_ALLOWED_TOP_LEVEL}


def _merge_machine_and_project(
    machine: Dict[str, Any], project: Dict[str, Any]
) -> Dict[str, Any]:
    """Merge machine (base) + project (overrides) by per-block rule.

    - ``local:`` — deep-merged field-by-field; project fields win on collisions
      (so a project can override the machine's ``sweeps_root`` without
      clobbering ``visible_gpus`` it didn't set).
    - Every other block — whole-block replace (project wins). The machine
      config shouldn't carry these anyway (see ``_scope_machine_config``),
      but the rule is here in case someone widens
      ``_MACHINE_CONFIG_ALLOWED_TOP_LEVEL`` later.
    """
    result: Dict[str, Any] = dict(machine)
    for key, project_val in project.items():
        machine_val = result.get(key)
        if (
            key == "local"
            and isinstance(machine_val, dict)
            and isinstance(project_val, dict)
        ):
            merged_local = dict(machine_val)
            merged_local.update(project_val)
            result[key] = merged_local
        else:
            result[key] = project_val
    return result


@dataclass
class SweepConfig:
    """Configuration for parameter sweeps."""

    grid: Dict[str, List[Any]] = field(default_factory=dict)
    paired: List[Dict[str, Dict[str, List[Any]]]] = field(default_factory=list)
    defaults: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    script: str = None  # Training script path (optional)
    complete: str = None  # Completion sweep ID (optional)

    @classmethod
    def from_yaml(cls, config_path: Union[str, Path]) -> "SweepConfig":
        """Load sweep config from YAML file."""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Sweep config not found: {config_path}")

        with open(config_path) as f:
            raw_config = yaml.safe_load(f)

        return cls.from_dict(raw_config)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "SweepConfig":
        """Create sweep config from dictionary."""
        # Extract sweep section if it exists, otherwise treat entire dict as sweep config
        sweep_config = config_dict.get("sweep", config_dict)

        return cls(
            grid=sweep_config.get("grid", {}),
            paired=sweep_config.get("paired", []),
            defaults=config_dict.get("defaults", {}),
            metadata=config_dict.get("metadata", {}),
            script=config_dict.get("script"),  # Extract script from top level
            complete=config_dict.get("complete"),  # Extract completion sweep ID from top level
        )

    @classmethod
    def from_hydra_config(
        cls,
        hydra_config: Union[DictConfig, Dict[str, Any]],
        selected_params: Dict[str, List[Any]],
    ) -> "SweepConfig":
        """Create sweep config from Hydra config with selected parameters."""
        if not OMEGACONF_AVAILABLE:
            raise ImportError("omegaconf is required for Hydra config support")

        if isinstance(hydra_config, DictConfig):
            hydra_config = OmegaConf.to_container(hydra_config, resolve=True)

        return cls(
            grid=selected_params,
            paired=[],
            defaults=hydra_config.get("defaults", {}),
            metadata={
                "source": "hydra_config",
                "hydra_config_keys": list(hydra_config.keys()),
            },
        )

    def validate(self) -> List[str]:
        """Validate the sweep configuration and return any errors."""
        errors = []

        # Validate grid parameters
        for key, values in self.grid.items():
            if not isinstance(values, list):
                errors.append(f"Grid parameter '{key}' must be a list, got {type(values)}")
            elif len(values) == 0:
                errors.append(f"Grid parameter '{key}' cannot be empty")

        # Validate paired parameters
        for i, group in enumerate(self.paired):
            if not isinstance(group, dict):
                errors.append(f"Paired group {i} must be a dictionary")
                continue

            # Check that all parameters in a group have the same length
            lengths = []
            for group_name, params in group.items():
                for param_name, values in params.items():
                    if not isinstance(values, list):
                        errors.append(
                            f"Paired parameter '{param_name}' in group '{group_name}' must be a list"
                        )
                    else:
                        lengths.append(len(values))

            if lengths and not all(length == lengths[0] for length in lengths):
                errors.append(
                    f"All parameters in paired group {i} must have the same length. Found lengths: {lengths}"
                )

        return errors

    def get_total_combinations(self) -> int:
        """Calculate total number of parameter combinations."""
        from .param_generator import ParameterGenerator

        generator = ParameterGenerator(self)
        return generator.count_combinations()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {
            "sweep": {"grid": self.grid, "paired": self.paired},
            "defaults": self.defaults,
            "metadata": self.metadata,
        }
        if self.script:
            result["script"] = self.script
        return result

    def save(self, output_path: Union[str, Path]) -> None:
        """Save sweep config to YAML file."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, indent=2)


class HSMConfig:
    """HSM configuration loader and manager."""

    def __init__(self, config_data: Dict[str, Any]):
        """Initialize with configuration data."""
        self.config_data = config_data

    @classmethod
    def load(
        cls,
        config_path: Optional[Path] = None,
        machine_config_path: Optional[Path] = None,
    ) -> Optional["HSMConfig"]:
        """Load HSM configuration, merging the machine-wide base with the project file.

        Precedence (low → high):

        1. ``~/.hsm/config.yaml`` — per-machine defaults. Only the ``local:``
           block is honored (so machine-specific facts like ``sweeps_root``
           or ``visible_gpus`` can live here without leaking remote configs
           across projects). Other top-level keys are dropped with a warning.
        2. ``<project>/.hsm/config.yaml`` (or legacy ``sweeps/hsm_config.yaml``)
           — per-project. Wins on collisions; ``local:`` fields are
           deep-merged so a project can override individual fields without
           clobbering the machine's other ``local:`` settings.

        Returns ``None`` only when BOTH files are missing.

        Args:
            config_path: Explicit project config path. If ``None``, searches
                standard locations under ``Path.cwd()``.
            machine_config_path: Override the machine config location.
                Defaults to :data:`MACHINE_CONFIG_PATH`. Mostly for tests.
        """
        if config_path is None:
            # Search for the project config in standard locations.
            search_paths = [
                Path.cwd() / ".hsm" / "config.yaml",  # primary location
                Path(".hsm") / "config.yaml",  # relative .hsm/
                Path.cwd() / "sweeps" / "hsm_config.yaml",  # Legacy
                Path.cwd() / "hsm_config.yaml",  # Legacy
                Path("sweeps") / "hsm_config.yaml",  # Legacy
                Path("hsm_config.yaml"),  # Legacy
            ]

            for path in search_paths:
                if path.exists():
                    config_path = path
                    logger.debug(f"Found HSM project config at: {path}")
                    break

        project_data: Optional[Dict[str, Any]] = None
        if config_path is not None and config_path.exists():
            project_data = _load_yaml_dict(config_path)
            if project_data is not None:
                logger.debug(f"Loaded HSM project config from {config_path}")

        # Machine-wide base config.
        machine_path = machine_config_path or MACHINE_CONFIG_PATH
        machine_data: Optional[Dict[str, Any]] = None
        if machine_path.exists():
            machine_data = _load_yaml_dict(machine_path)
            if machine_data is not None:
                machine_data = _scope_machine_config(machine_data, machine_path)
                logger.debug(f"Loaded HSM machine config from {machine_path}")

        if not project_data and not machine_data:
            logger.debug("No HSM config found (machine or project).")
            return None

        merged = _merge_machine_and_project(machine_data or {}, project_data or {})

        # Deprecation warning: paths.python_interpreter superseded by paths.conda_env.
        paths = merged.get("paths") or {}
        if isinstance(paths, dict) and paths.get("python_interpreter"):
            if paths.get("conda_env"):
                logger.warning(
                    "paths.python_interpreter is set AND paths.conda_env is set — "
                    "conda_env wins, python_interpreter is ignored. Remove "
                    "python_interpreter from your config to silence this warning."
                )
            else:
                logger.warning(
                    "paths.python_interpreter is DEPRECATED — use paths.conda_env "
                    "to name your project's conda/mamba env. HSM will activate it "
                    "for every backend (local/slurm/ssh) via `conda run -n <env>`."
                )
        return cls(merged)

    def get_default_python_path(self) -> Optional[str]:
        """Get default Python interpreter path from config."""
        return self.config_data.get("paths", {}).get("python_interpreter")

    def get_conda_env(self) -> Optional[str]:
        """Project-level conda/mamba env name. The single source of truth.

        Read from ``paths.conda_env``. When set, every backend that runs
        ``python`` for this project activates this env: local (via
        ``conda run -n <env> python``), native Slurm (same), SSH and
        SSH-Slurm (via the existing ``resolve_run_prefix`` path).
        Per-remote ``distributed.remotes.<alias>.conda_env`` still
        overrides for the remote that names it (rare).

        Returns ``None`` when unset or set to an empty string.

        Convention: pin one env name per project, create it with the
        same name on every machine the project will run on. The whole
        config stays portable across machines this way.
        """
        value = self.config_data.get("paths", {}).get("conda_env")
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            logger.warning(
                f"`paths.conda_env` must be a non-empty string; "
                f"got {type(value).__name__}. Ignoring."
            )
            return None
        return value.strip()

    def get_default_script_path(self) -> Optional[str]:
        """Get default training script path from config."""
        return self.config_data.get("paths", {}).get("train_script")

    def get_project_root(self) -> Optional[str]:
        """Get project root directory from config."""
        return self.config_data.get("project", {}).get("root")

    def get_wandb_config(self) -> Dict[str, Any]:
        """Get wandb configuration from config."""
        return self.config_data.get("wandb", {})

    def get_max_array_size(self) -> Optional[int]:
        """Get the Slurm-array size cap from the ``slurm:`` block.

        Slurm's default ceiling is 10000; this lets users lower it for
        clusters with a stricter cap. Returns ``None`` when unset.
        """
        return self.config_data.get("slurm", {}).get("max_array_size")

    def get_slurm_spec(self):
        """Read the typed ``slurm:`` block as a :class:`ResourceSpec`, or ``None``.

        The ``slurm:`` block in ``.hsm/config.yaml`` is the canonical place to
        express HPC fields the opaque ``--resources`` CLI string can't reach:
        ``gpu_type``, ``modules``, ``pre_script``, ``account``,
        ``extra_directives``. All keys are optional. Extra (non-ResourceSpec)
        keys like ``qos_whitelist`` are stripped before construction.

        Example::

            slurm:
              walltime: "01:00:00"
              cpus_per_task: 4
              mem: "16gb"
              gpus: 1
              gpu_type: "h100"
              qos: "normal"
              modules: [h100]
              pre_script:
                - "source ~/.bashrc"
                - "conda activate my-env"
              extra_directives:
                mail-type: FAIL
                mail-user: me@example.com
              qos_whitelist: [normal, medium, long]  # consumed separately
        """
        from .resource_spec import ResourceSpec

        block = self.config_data.get("slurm")
        if not isinstance(block, dict) or not block:
            return None
        # Strip orchestrator-/scheduler-only keys before handing to ResourceSpec.
        _NON_SPEC_KEYS = {"qos_whitelist", "max_array_size"}
        filtered = {k: v for k, v in block.items() if k not in _NON_SPEC_KEYS}
        try:
            return ResourceSpec.from_dict(filtered)
        except (TypeError, ValueError) as e:
            logger.warning(f"Invalid `slurm:` block in HSM config: {e}")
            return None

    def get_local_spec(self):
        """Read the typed ``local:`` block as a :class:`ResourceSpec`, or ``None``.

        Applies *only* to ``--mode local`` — never to Slurm or remote/distributed.
        Restricted to fields that make sense outside a batch scheduler:
        ``walltime``, ``cpus_per_task``, ``mem``, ``gpus``, ``pre_script``.
        Slurm-only fields (``gpu_type``, ``modules``, ``qos``, ``account``,
        ``extra_directives``) belong in the ``slurm:`` block; if they appear here
        they are silently dropped with a warning. ``visible_gpus`` is consumed
        separately by :meth:`get_local_visible_gpus` (it's a GPU allowlist,
        not a per-job ResourceSpec field).

        Example::

            local:
              walltime: "04:00:00"
              cpus_per_task: 4
              mem: "16gb"
              gpus: 1               # per-task GPU count; LocalComputeSource partitions
                                    # nvidia-smi -L into slots of this size
              visible_gpus: [1, 2, 3]  # allowlist; CLI --gpus overrides (see get_local_visible_gpus)
              pre_script:
                - "conda activate my-env"
        """
        from .resource_spec import ResourceSpec

        block = self.config_data.get("local")
        if not isinstance(block, dict) or not block:
            return None
        _LOCAL_SPEC_FIELDS = {"walltime", "cpus_per_task", "mem", "gpus", "pre_script"}
        # Consumed by sibling accessors, not ResourceSpec:
        #   visible_gpus  → get_local_visible_gpus
        #   sweeps_root   → get_local_sweeps_root
        _NON_SPEC_LOCAL_KEYS = {"visible_gpus", "sweeps_root"}
        rejected = set(block) - _LOCAL_SPEC_FIELDS - _NON_SPEC_LOCAL_KEYS
        if rejected:
            logger.warning(
                f"`local:` block has Slurm-only or unknown fields {sorted(rejected)!r}; "
                f"move them to the `slurm:` block. Ignoring."
            )
        filtered = {k: v for k, v in block.items() if k in _LOCAL_SPEC_FIELDS}
        try:
            return ResourceSpec.from_dict(filtered)
        except (TypeError, ValueError) as e:
            logger.warning(f"Invalid `local:` block in HSM config: {e}")
            return None

    def get_local_visible_gpus(self):
        """Read ``local.visible_gpus`` as a list of int indices, or ``None`` if unset.

        Same allowlist semantics as the ``--gpus`` CLI flag (which overrides
        this when both are set). Useful for shared GPU boxes where (e.g.)
        ``GPU:0`` is reserved for interactive work — set
        ``visible_gpus: [1, 2, 3]`` and ``hsm sweep run --mode local`` will
        never schedule on GPU:0.

        Mirrors :meth:`get_slurm_qos_whitelist`'s shape: read separately
        from :meth:`get_local_spec` because it's a per-source filter, not
        a per-job resource.
        """
        block = self.config_data.get("local")
        if not isinstance(block, dict):
            return None
        value = block.get("visible_gpus")
        if value is None:
            return None
        if not isinstance(value, (list, tuple)):
            logger.warning(
                f"`local.visible_gpus` must be a list of GPU indices "
                f"(e.g. [1, 2, 3]); got {type(value).__name__}. Ignoring."
            )
            return None
        try:
            indices = [int(x) for x in value]
        except (TypeError, ValueError) as e:
            logger.warning(f"Invalid `local.visible_gpus`: {e}. Ignoring.")
            return None
        if not indices:
            return None
        return indices

    def get_local_sweeps_root(self) -> Optional[str]:
        """Read ``local.sweeps_root`` — a directory where sweep dirs live, or ``None``.

        When set, sweep dirs are created at ``<sweeps_root>/<sweep_id>/`` (with
        ``~`` / ``$HOME`` / ``$USER`` expanded by :func:`resolve_sweep_dir`), and
        a symlink is dropped at ``<cwd>/sweeps/outputs/<sweep_id>`` pointing to
        it — so existing tooling (``hsm sweep status``, ``hsm sweep report``)
        keeps finding sweeps where it expects.

        Use case: a fat workstation whose system disk is tight but with a
        large secondary mount (e.g., ``/mnt/8TB_HDD/<user>/sweeps``). Setting
        ``sweeps_root`` keeps ``/`` breathing while preserving the
        project-local discovery pattern.

        Returns the raw string (caller expands); ``None`` when unset.
        """
        block = self.config_data.get("local")
        if not isinstance(block, dict):
            return None
        value = block.get("sweeps_root")
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            logger.warning(
                f"`local.sweeps_root` must be a non-empty string; "
                f"got {type(value).__name__}. Ignoring."
            )
            return None
        return value

    def get_slurm_qos_whitelist(self) -> Optional[frozenset]:
        """Read ``slurm.qos_whitelist`` as a frozenset, or ``None`` if unset.

        Returned from the same ``slurm:`` block as :meth:`get_slurm_spec`,
        but consumed separately by ``SlurmComputeSource`` (it's a per-source
        constraint, not a per-job resource).
        """
        block = self.config_data.get("slurm")
        if not isinstance(block, dict):
            return None
        whitelist = block.get("qos_whitelist")
        if not whitelist:
            return None
        if not isinstance(whitelist, (list, tuple, set, frozenset)):
            logger.warning(
                f"`slurm.qos_whitelist` must be a list of strings; got {type(whitelist).__name__}"
            )
            return None
        return frozenset(str(q) for q in whitelist)


def resolve_sweep_dir(
    hsm_config: Optional["HSMConfig"],
    sweep_id: str,
    project_dir: Optional[Path] = None,
) -> Path:
    """Resolve and create the sweep dir for ``sweep_id``.

    Default: ``<project_dir>/sweeps/outputs/<sweep_id>`` (where
    ``project_dir`` falls back to the current working directory).

    When ``hsm_config.local.sweeps_root`` is set, sweep dirs are created
    at ``<sweeps_root>/<sweep_id>/`` instead (with ``~`` / ``$HOME`` /
    ``$USER`` expanded), and a symlink at
    ``<project_dir>/sweeps/outputs/<sweep_id>`` points to it — so
    project-local tooling keeps working transparently.

    The returned ``Path`` is the *target* (where data actually lives),
    not the symlink, so callers using it for ``mkdir``, ``glob``, etc.
    operate on the canonical location.

    Raises:
        FileNotFoundError: when ``local.sweeps_root`` is set but resolves
            to a directory that doesn't exist on this machine. Catches
            the "shared `.hsm/config.yaml` references a path that exists
            on machine A but not on machine B" footgun — HSM refuses to
            silently `mkdir -p` a path that almost certainly should have
            been a mount point.
    """
    project_dir = project_dir or Path.cwd()
    default = project_dir / "sweeps" / "outputs" / sweep_id

    sweeps_root = (
        hsm_config.get_local_sweeps_root() if hsm_config is not None else None
    )
    if not sweeps_root:
        default.mkdir(parents=True, exist_ok=True)
        return default

    expanded = Path(os.path.expandvars(os.path.expanduser(sweeps_root)))
    if not expanded.exists():
        raise FileNotFoundError(
            f"`local.sweeps_root` is set to {sweeps_root!r} "
            f"(resolved to {expanded}), but that directory does not "
            f"exist on this machine. Either create it "
            f"(`mkdir -p {expanded}`) or remove the `local.sweeps_root` "
            f"field from your HSM config (machine: {MACHINE_CONFIG_PATH}, "
            f"or this project's `.hsm/config.yaml`)."
        )
    expanded = expanded.resolve()
    target = expanded / sweep_id
    target.mkdir(parents=True, exist_ok=True)

    link_parent = project_dir / "sweeps" / "outputs"
    link_parent.mkdir(parents=True, exist_ok=True)
    link = link_parent / sweep_id

    # If a stale symlink already exists at the link path (e.g., from a
    # collision on sweep_id), replace it with one pointing at the new
    # target. Never touch a real directory living at the link path —
    # that would be data loss.
    if link.is_symlink():
        link.unlink()
    if not link.exists():
        try:
            link.symlink_to(target, target_is_directory=True)
            logger.info(f"Sweep dir at {target} (symlinked from {link})")
        except OSError as e:
            logger.warning(
                f"Could not symlink {link} -> {target}: {e}. Sweep data "
                f"lives at the target; project-local discovery via "
                f"`sweeps/outputs/` will not see it."
            )
    else:
        logger.warning(
            f"Cannot create discovery symlink at {link}: a non-symlink "
            f"entry already exists there. Sweep data lives at {target}."
        )

    return target


class HydraConfigParser:
    """Parser for Hydra configuration files."""

    def __init__(self, config_dir: Path):
        self.config_dir = config_dir

    def discover_configs(self) -> Dict[str, Path]:
        """Discover all configuration files in the config directory."""
        configs = {}

        if not self.config_dir.exists():
            return configs

        # Look for YAML files
        for yaml_file in self.config_dir.rglob("*.yaml"):
            relative_path = yaml_file.relative_to(self.config_dir)
            config_name = str(relative_path).replace("/", ".").replace(".yaml", "")
            configs[config_name] = yaml_file

        return configs

    def load_config(self, config_path: Path) -> Dict[str, Any]:
        """Load a configuration file."""
        with open(config_path) as f:
            return yaml.safe_load(f)

    def extract_parameters(self, config: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        """Extract parameters from a nested configuration."""
        params = {}

        for key, value in config.items():
            full_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                params.update(self.extract_parameters(value, full_key))
            else:
                params[full_key] = value

        return params

    def suggest_sweep_ranges(self, parameters: Dict[str, Any]) -> Dict[str, List[Any]]:
        """Suggest sweep ranges for parameters based on their types and values."""
        suggestions = {}

        for param_name, value in parameters.items():
            if isinstance(value, (int, float)):
                if isinstance(value, int):
                    suggestions[param_name] = [value // 2, value, value * 2]
                else:
                    suggestions[param_name] = [value * 0.5, value, value * 2.0]
            elif isinstance(value, bool):
                suggestions[param_name] = [True, False]
            elif isinstance(value, str):
                suggestions[param_name] = [value]

        return suggestions
