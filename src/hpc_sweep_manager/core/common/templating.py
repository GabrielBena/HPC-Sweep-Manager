"""Utility for rendering Jinja2 templates."""

from datetime import datetime
import logging
from pathlib import Path

try:
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False

logger = logging.getLogger(__name__)


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
