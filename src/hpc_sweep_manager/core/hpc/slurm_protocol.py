"""Pure Slurm protocol helpers shared by local and SSH-driven Slurm sources.

These are *format* helpers — no I/O, no shell, no asyncio. The two callers
(:class:`SlurmComputeSource` runs ``sbatch`` locally; :class:`SSHSlurmComputeSource`
runs it over an asyncssh connection) need exactly the same directive shape
and state translation, so deduplicating saves the only real risk of drift
between them.
"""

from __future__ import annotations

import logging
import re

from ..common.resource_spec import ResourceSpec

logger = logging.getLogger(__name__)


# Map raw Slurm states (from ``squeue -h -o %T``) to the canonical states
# ComputeSource.update_job_status expects. Unknown states default to RUNNING
# at the call site — safer than treating them as terminal.
SLURM_STATE_MAP: dict[str, str] = {
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


def directive_flag(key: str) -> str:
    """``exclude`` → ``--exclude``: an ``extra_directives`` key without its dashes was
    rendered verbatim (``#SBATCH exclude=…``), which Slurm ignores (tracker S6)."""
    return key if key.startswith("-") else f"--{key}"


def format_signal(grace: int) -> str:
    """The ``--signal`` token HSM uses for the pre-walltime save (issue #12).

    ``B:TERM@<grace>`` signals the *batch shell* ``<grace>`` seconds before the
    walltime. The ``B:`` prefix is REQUIRED: with no ``srun`` step, a bare
    ``TERM@N`` targets job steps (there are none), so the batch script never
    sees it. The rendered template forwards this SIGTERM to the python child.
    """
    return f"B:TERM@{grace}"


def render_sbatch_directives(
    spec: ResourceSpec,
    *,
    dependency: str | None = None,
    signal: str | None = None,
) -> str:
    """Render a ``ResourceSpec`` into the ``#SBATCH ...`` directive block.

    Returned as a newline-joined string ready to drop into the wrapper
    template (between the ``#!/bin/bash`` shebang and the body). The
    caller decides where the ``--job-name`` / ``--output`` / ``--error``
    / ``--array=`` directives live — those are template-level, not
    spec-level.

    ``dependency`` (e.g. ``"afterany:12345"``) and ``signal`` (e.g.
    ``"B:TERM@120"``, from :func:`format_signal`) are the resumable-chain
    additions (issue #12) — both default ``None`` so every existing call site
    renders byte-identically. They append AFTER ``extra_directives`` so a
    chain directive wins a duplicate (Slurm takes the last ``--signal``).
    """
    lines: list[str] = []
    if spec.walltime:
        lines.append(f"#SBATCH --time={spec.walltime}")
    if spec.cpus_per_task:
        lines.append(f"#SBATCH --cpus-per-task={spec.cpus_per_task}")
    if spec.mem:
        lines.append(f"#SBATCH --mem={spec.mem}")
    if spec.mem_per_cpu:
        lines.append(f"#SBATCH --mem-per-cpu={spec.mem_per_cpu}")
    if spec.gpus is not None and spec.gpus > 0:
        if isinstance(spec.gpu_type, tuple):
            # Multi-type specs must be split into one sub-array per type
            # (core/hpc/gpu_planner.plan_gpu_split) and scalarized BEFORE
            # rendering — refusing here catches any path that forgot.
            raise ValueError(
                "render_sbatch_directives: gpu_type is a multi-type list "
                f"({list(spec.gpu_type)}); scalarize via gpu_planner before "
                "rendering (array mode splits one sub-array per type)"
            )
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
        key = directive_flag(key)
        if value:
            lines.append(f"#SBATCH {key}={value}")
        else:
            lines.append(f"#SBATCH {key}")
    # Resumable-chain directives (issue #12) — appended last so they win a
    # duplicate against anything a user stuck in extra_directives.
    if dependency:
        if any(k == "--dependency" for k, _ in spec.extra_directives):
            logger.warning(
                "render_sbatch_directives: --dependency set both via the chain "
                "and extra_directives; the chain dependency (%s) wins.",
                dependency,
            )
        lines.append(f"#SBATCH --dependency={dependency}")
    if signal:
        if any(k == "--signal" for k, _ in spec.extra_directives):
            logger.warning(
                "render_sbatch_directives: --signal set both via the chain and "
                "extra_directives; the chain signal (%s) wins.",
                signal,
            )
        lines.append(f"#SBATCH --signal={signal}")
    return "\n".join(lines)


def parse_sbatch_job_id(stdout: str) -> str:
    """The job id in sbatch's ``Submitted batch job <id>`` line.

    Anything else raises: a guessed id (the last word of an extra notice line, say) would read as
    unknown to sacct, so the job would be taken as finished and its sweep dir cleaned.
    """
    m = re.search(r"Submitted batch job (\d+)", stdout or "")
    if not m:
        raise ValueError(f"no job id in sbatch output: {stdout!r}")
    return m.group(1)
