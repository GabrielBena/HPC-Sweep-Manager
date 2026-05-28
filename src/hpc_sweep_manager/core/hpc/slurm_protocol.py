"""Pure Slurm protocol helpers shared by local and SSH-driven Slurm sources.

These are *format* helpers — no I/O, no shell, no asyncio. The two callers
(:class:`SlurmComputeSource` runs ``sbatch`` locally; :class:`SSHSlurmComputeSource`
runs it over an asyncssh connection) need exactly the same directive shape
and state translation, so deduplicating saves the only real risk of drift
between them.
"""

from __future__ import annotations

from typing import Dict, List

from ..common.resource_spec import ResourceSpec


# Map raw Slurm states (from ``squeue -h -o %T``) to the canonical states
# ComputeSource.update_job_status expects. Unknown states default to RUNNING
# at the call site — safer than treating them as terminal.
SLURM_STATE_MAP: Dict[str, str] = {
    "PENDING": "PENDING",
    "RUNNING": "RUNNING",
    "SUSPENDED": "PENDING",
    "CANCELLED": "CANCELLED",
    "COMPLETING": "RUNNING",
    "COMPLETED": "COMPLETED",
    "CONFIGURING": "PENDING",
    "FAILED": "FAILED",
    "TIMEOUT": "FAILED",
    "PREEMPTED": "FAILED",
    "NODE_FAIL": "FAILED",
    "OUT_OF_MEMORY": "FAILED",
    "BOOT_FAIL": "FAILED",
    "DEADLINE": "FAILED",
}


def render_sbatch_directives(spec: ResourceSpec) -> str:
    """Render a ``ResourceSpec`` into the ``#SBATCH ...`` directive block.

    Returned as a newline-joined string ready to drop into the wrapper
    template (between the ``#!/bin/bash`` shebang and the body). The
    caller decides where the ``--job-name`` / ``--output`` / ``--error``
    / ``--array=`` directives live — those are template-level, not
    spec-level.
    """
    lines: List[str] = []
    if spec.walltime:
        lines.append(f"#SBATCH --time={spec.walltime}")
    if spec.cpus_per_task:
        lines.append(f"#SBATCH --cpus-per-task={spec.cpus_per_task}")
    if spec.mem:
        lines.append(f"#SBATCH --mem={spec.mem}")
    if spec.mem_per_cpu:
        lines.append(f"#SBATCH --mem-per-cpu={spec.mem_per_cpu}")
    if spec.gpus is not None and spec.gpus > 0:
        if spec.gpu_type:
            lines.append(f"#SBATCH --gres=gpu:{spec.gpu_type}:{spec.gpus}")
        else:
            lines.append(f"#SBATCH --gpus={spec.gpus}")
    if spec.partition:
        lines.append(f"#SBATCH --partition={spec.partition}")
    if spec.qos:
        lines.append(f"#SBATCH --qos={spec.qos}")
    if spec.account:
        lines.append(f"#SBATCH --account={spec.account}")
    for key, value in spec.extra_directives:
        if value:
            lines.append(f"#SBATCH {key}={value}")
        else:
            lines.append(f"#SBATCH {key}")
    return "\n".join(lines)


def parse_sbatch_job_id(stdout: str) -> str:
    """Extract the job id from sbatch's stdout.

    Slurm prints ``Submitted batch job <id>`` on success. We tolerate
    trailing whitespace and array submissions where the id includes
    array bracket notation (handled upstream — Slurm itself reports
    just the integer parent id here).
    """
    text = (stdout or "").strip()
    if not text:
        raise ValueError("sbatch produced no output to parse a job id from")
    return text.splitlines()[-1].strip().split()[-1]
