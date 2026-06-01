"""HPC Sweep Manager - Automated hyperparameter sweeps on HPC systems."""

from pathlib import Path
from typing import Optional

from .core.common.config import SweepConfig
from .core.common.param_generator import ParameterGenerator
from .core.common.path_detector import PathDetector

__author__ = "Gabriel Bena"
__email__ = "gabriel.bena@gmail.com"

# Release version; the runtime ``__version__`` appends a short git SHA when
# imported from a source checkout (see _resolve_version) so users can tell
# dev builds apart in bug reports during the active refactor.
_BASE_VERSION = "0.1.0"


def _git_short_sha() -> Optional[str]:
    """Short git SHA when running from a source checkout, else ``None``.

    Reads ``.git`` directly (no subprocess) so importing the package stays
    cheap and side-effect-free. Installed wheels have no ``.git`` alongside the
    source and fall through to ``None`` → the plain ``_BASE_VERSION``.
    """
    try:
        git_dir = Path(__file__).resolve().parents[2] / ".git"
        if not git_dir.is_dir():
            return None
        head = (git_dir / "HEAD").read_text().strip()
        if head.startswith("ref:"):
            ref = head.split(":", 1)[1].strip()
            ref_file = git_dir / ref
            if ref_file.is_file():
                return ref_file.read_text().strip()[:7]
            # Branch ref may be packed instead of a loose file.
            packed = git_dir / "packed-refs"
            if packed.is_file():
                for line in packed.read_text().splitlines():
                    if line and not line.startswith(("#", "^")) and line.endswith(ref):
                        return line.split(maxsplit=1)[0][:7]
            return None
        # Detached HEAD: the file holds the raw SHA.
        return head[:7] or None
    except Exception:
        return None


def _resolve_version() -> str:
    sha = _git_short_sha()
    return f"{_BASE_VERSION}+g{sha}" if sha else _BASE_VERSION


__version__ = _resolve_version()

__all__ = [
    "SweepConfig",
    "ParameterGenerator",
    "PathDetector",
]
