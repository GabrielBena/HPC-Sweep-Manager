"""Utilities for rendering Jinja2 templates and serializing parameters."""

from __future__ import annotations

from datetime import datetime
import logging
from pathlib import Path
from typing import Any

try:
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False

logger = logging.getLogger(__name__)


def params_to_hydra_args(params: dict[str, Any]) -> str:
    """Convert a parameter dict into a Hydra-style command-line argument string.

    Each key/value becomes a quoted ``"key=value"`` token. Lists and tuples are
    rendered in Hydra's bracket form (``[a, b, c]``), bools are lowercased, and
    ``None`` becomes ``null``. The returned tokens are space-joined and intended
    to be appended verbatim to a Python invocation::

        python train.py "model.hidden_size=128" "seed=null"

    This is the canonical implementation. Older copies live in each manager
    class and will be removed as the unified ComputeSource architecture lands.
    """
    tokens: list[str] = []
    for key, value in params.items():
        if isinstance(value, (list, tuple)):
            value_str = str(list(value))
            tokens.append(f'"{key}={value_str}"')
        elif value is None:
            tokens.append(f'"{key}=null"')
        elif isinstance(value, bool):
            tokens.append(f'"{key}={str(value).lower()}"')
        elif isinstance(value, str) and (" " in value or "," in value):
            tokens.append(f'"{key}={value}"')
        else:
            tokens.append(f'"{key}={value}"')
    return " ".join(tokens)


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
    if not JINJA2_AVAILABLE:
        raise ImportError(
            "Jinja2 is required for template rendering. Please install it with 'pip install jinja2'"
        )

    # Get the path to the templates directory
    template_dir = Path(__file__).parent.parent.parent / "templates"

    if not template_dir.exists():
        logger.error(f"Template directory not found at: {template_dir}")
        raise FileNotFoundError(f"Template directory not found at: {template_dir}")

    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(["html", "xml", "sh"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )

    # Add custom filters
    env.filters["strftime"] = strftime_filter

    try:
        template = env.get_template(template_name)
        return template.render(**kwargs)
    except Exception as e:
        logger.error(f"Error rendering template {template_name}: {e}")
        raise
