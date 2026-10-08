"""Utilities for rendering Jinja2 templates and serializing parameters."""

from __future__ import annotations

import logging
import shlex
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader, Undefined

logger = logging.getLogger(__name__)


class _PrintFailsUndefined(Undefined):
    """A missing variable can still be tested (``{% if x %}``) but never printed: printed as "",
    it built commands with holes, gotcha #11's class of bug (G2)."""

    __str__ = Undefined._fail_with_undefined_error


def params_to_hydra_args(params: dict[str, Any]) -> str:
    """Render params as Hydra ``"key=value"`` tokens, space-joined, for a shell to eval::

        python train.py "model.hidden_size=128" "layers=[64, 64]" "seed=null"

    Lists take Hydra's bracket form, bools are lowercased and ``None`` is ``null``. Each
    token escapes ``\\`` and ``"``, so any value survives its double quotes; ``$`` still
    expands, as a sweep may rely on it. ``slurm_array.sh.j2`` has a copy: keep the two alike.
    """

    def value(v: Any) -> str:
        if isinstance(v, (list, tuple)):
            return str(list(v))
        return "null" if v is None else str(v).lower() if isinstance(v, bool) else str(v)

    def token(k: str, v: Any) -> str:
        return '"' + f"{k}={value(v)}".replace("\\", "\\\\").replace('"', '\\"') + '"'

    return " ".join(token(k, v) for k, v in params.items())


# The overrides HSM appends to every task command, in this order (R3). The project config's
# `hydra_overrides:` picks a subset; `hydra.run.dir` gives each task its own Hydra run dir, so
# tasks that start in the same second no longer share `outputs/<date>/<time>`.
HYDRA_OVERRIDES = {
    "wandb.group": "{group}",
    "output.dir": "{dir}",
    "hydra.run.dir": "{dir}/.hydra_run",
}
LEGACY_HYDRA_OVERRIDES = ("wandb.group", "output.dir")  # before R3: for chains launched without it


def task_overrides(keys, group: str, task_dir: str) -> str:
    """The ``key=value`` override of each of ``keys`` for one task, space-joined."""
    return " ".join(f"{k}={HYDRA_OVERRIDES[k].format(group=group, dir=task_dir)}" for k in keys)


def params_to_yaml(params: dict[str, Any]) -> str:
    """Serialize a task's parameter dict to YAML for a self-describing
    ``params.yaml`` dropped into each task dir.

    After a ``tasks/``-only pull, a synced checkpoint would otherwise be
    orphaned from the overrides that produced it (Hydra's ``.hydra/config.yaml``
    lands outside ``tasks/`` unless ``hydra.run.dir`` is passed). Writing the exact per-task
    overrides next to the checkpoint makes it self-describing — pair it with the
    project code to rebuild the model. Always ends with a trailing newline so it
    drops cleanly into a heredoc.
    """
    return yaml.safe_dump(params, default_flow_style=False, sort_keys=True)


def strftime_filter(value, format="%Y-%m-%d %H:%M:%S"):
    """Custom Jinja2 filter for strftime formatting."""
    if value == "now":
        return datetime.now().strftime(format)
    elif isinstance(value, datetime):
        return value.strftime(format)
    else:
        return str(value)


def render_template(template_name: str, **kwargs) -> str:
    """
    Render a Jinja2 template with the given context.

    Args:
        template_name: The name of the template file.
        **kwargs: The context variables to pass to the template.

    Returns:
        The rendered template as a string.
    """
    # Get the path to the templates directory
    template_dir = Path(__file__).parent.parent.parent / "templates"

    if not template_dir.exists():
        logger.error(f"Template directory not found at: {template_dir}")
        raise FileNotFoundError(f"Template directory not found at: {template_dir}")

    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=False,  # shell scripts, not HTML
        undefined=_PrintFailsUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )

    # Add custom filters
    env.filters["strftime"] = strftime_filter
    env.globals["task_overrides"] = task_overrides
    env.filters["shquote"] = shlex.quote
    kwargs.setdefault("hydra_overrides", tuple(HYDRA_OVERRIDES))  # never an empty Undefined

    try:
        template = env.get_template(template_name)
        return template.render(**kwargs)
    except Exception as e:
        logger.error(f"Error rendering template {template_name}: {e}")
        raise
