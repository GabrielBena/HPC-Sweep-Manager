"""Query GPU availability on a host via ``nvidia-smi``.

Used by ``hsm remote gpus`` to decide where to target a sweep. The parsing is
a pure function (easy to unit-test); the probe wraps it around an SSH
connection opened through :func:`create_ssh_connection`, so it honors
``~/.ssh/config`` aliases and needs nothing installed on the remote beyond
``nvidia-smi`` itself.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import List, Optional

from .discovery import create_ssh_connection

logger = logging.getLogger(__name__)

# Columns we request, in order. Kept in one place so the query string and the
# parser can't drift apart.
_QUERY_FIELDS = ("index", "name", "memory.used", "memory.total", "utilization.gpu")
NVIDIA_SMI_QUERY = (
    "nvidia-smi --query-gpu="
    + ",".join(_QUERY_FIELDS)
    + " --format=csv,noheader,nounits"
)

# A GPU counts as "free" when it's essentially idle and nearly empty.
_FREE_UTIL_PCT = 5.0
_FREE_MEM_MB = 500.0


@dataclass(frozen=True)
class GpuInfo:
    index: int
    name: str
    mem_used_mb: float
    mem_total_mb: float
    util_pct: float

    @property
    def is_free(self) -> bool:
        return self.util_pct < _FREE_UTIL_PCT and self.mem_used_mb < _FREE_MEM_MB

    @property
    def mem_used_gb(self) -> float:
        return self.mem_used_mb / 1024.0

    @property
    def mem_total_gb(self) -> float:
        return self.mem_total_mb / 1024.0


def parse_nvidia_smi_csv(text: str) -> List[GpuInfo]:
    """Parse ``nvidia-smi --query-gpu=...,--format=csv,noheader,nounits`` output.

    Each non-empty line is ``index, name, mem_used, mem_total, util``. Lines
    that don't parse (unexpected column count / non-numeric) are skipped with a
    debug log rather than raising — a half-readable probe still beats none.
    """
    gpus: List[GpuInfo] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != len(_QUERY_FIELDS):
            logger.debug(f"Skipping unparseable nvidia-smi line: {line!r}")
            continue
        try:
            gpus.append(
                GpuInfo(
                    index=int(parts[0]),
                    name=parts[1],
                    mem_used_mb=float(parts[2]),
                    mem_total_mb=float(parts[3]),
                    util_pct=float(parts[4]),
                )
            )
        except ValueError:
            logger.debug(f"Skipping non-numeric nvidia-smi line: {line!r}")
            continue
    return gpus


async def probe_gpus(
    host: str, ssh_key: Optional[str] = None, ssh_port: Optional[int] = None
) -> List[GpuInfo]:
    """Connect to ``host`` and return its GPUs.

    Returns an empty list if the box has no NVIDIA GPUs / no ``nvidia-smi``
    (a CPU node), and raises only on connection failure — so callers can
    distinguish "reached it, no GPUs" from "couldn't reach it".
    """
    async with await create_ssh_connection(host, ssh_key, ssh_port) as conn:
        result = await conn.run(NVIDIA_SMI_QUERY, check=False)
        if result.returncode != 0:
            logger.debug(
                f"nvidia-smi unavailable on {host} (rc={result.returncode}): "
                f"{(result.stderr or '').strip()}"
            )
            return []
        return parse_nvidia_smi_csv(result.stdout or "")
