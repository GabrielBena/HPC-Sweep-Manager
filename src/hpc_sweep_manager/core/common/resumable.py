"""Config surface for resumable chained runs (issue #12).

``ResumableConfig`` is the typed, validated knob block; ``ResumableContext`` is
the tiny per-chunk bundle the Slurm sources thread into the rendered template;
``resolve_resumable_config`` layers the three config sources
(per-remote ← sweep YAML ← CLI) into one resolved config.

Pure (no I/O) — mirrors ``resource_spec.py``. Done-detection semantics and the
runaway guards live in :mod:`.chain`; this module is purely the configuration.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .chain import ChainConfig
from .utils import parse_walltime

logger = logging.getLogger(__name__)

# Keys ResumableConfig understands; anything else in a config block is dropped
# with a warning (tolerant, like ResourceSpec.from_dict).
_KNOWN_KEYS = frozenset(
    {
        "enabled",
        "chunk_walltime",
        "signal_grace",
        "resume_arg",
        "done_sentinel",
        "checkpoint_subdir",
        "max_chunks",
        "max_consecutive_failures",
    }
)

# A per-remote ``resumable:`` block may only carry CLUSTER-bound knobs (the
# 24h cap is a fact about the pool, not the workload). Workload guards
# (enabled / max_chunks / ...) belong in the sweep YAML — warn if misplaced.
_REMOTE_ALLOWED_KEYS = frozenset({"chunk_walltime", "signal_grace", "checkpoint_subdir"})


def is_hms(walltime: str) -> bool:
    """True iff ``walltime`` is ``HH:MM:SS`` (three numeric parts).

    Deliberately stricter than :func:`parse_walltime`, which also accepts the
    two-part ``MM:SS`` form — a chunk cap written ``23:00`` would silently mean
    23 *minutes*, the exact gpu_planner trap. Slurm ``--time`` wants HH:MM:SS.
    """
    parts = (walltime or "").split(":")
    return len(parts) == 3 and all(p.isdigit() for p in parts)


@dataclass(frozen=True)
class ResumableConfig:
    """Resolved resumable knobs. ``chunk_walltime`` is REQUIRED when enabled."""

    enabled: bool = False
    chunk_walltime: Optional[str] = None  # HH:MM:SS — the pool's QOS cap
    signal_grace: int = 120  # seconds before walltime -> --signal=B:TERM@<grace>
    resume_arg: Optional[str] = "training.resume_from"  # hydra key; None = env-only
    done_sentinel: str = ".hsm_done"  # script writes this under HSM_WORKDIR when complete
    checkpoint_subdir: str = "resume"  # per-task persistent ckpt dir under the workdir
    max_chunks: int = 10  # runaway guard: chain length cap
    max_consecutive_failures: int = 2  # no-progress strikes -> mark the chain FAILED

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "ResumableConfig":
        """Build from a plain dict, dropping unknown keys with a warning."""
        if not data:
            return cls()
        clean: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in _KNOWN_KEYS:
                logger.warning("resumable: ignoring unknown key %r", k)
                continue
            if v is None:
                continue
            clean[k] = v
        # light coercion for the int knobs (YAML may hand us strings)
        for int_key in ("signal_grace", "max_chunks", "max_consecutive_failures"):
            if int_key in clean:
                clean[int_key] = int(clean[int_key])
        if "enabled" in clean:
            clean["enabled"] = bool(clean["enabled"])
        return cls(**clean)

    def validate(self) -> list[str]:
        """Return a list of human-readable errors ([] when valid)."""
        errors: list[str] = []
        if self.enabled and not self.chunk_walltime:
            errors.append(
                "resumable.enabled is true but chunk_walltime is unset — set it "
                "to the pool's QOS walltime cap (e.g. 23:00:00)"
            )
        if self.chunk_walltime is not None and not is_hms(self.chunk_walltime):
            errors.append(
                f"resumable.chunk_walltime {self.chunk_walltime!r} is not HH:MM:SS "
                f"(a two-part value like '23:00' would mean 23 MINUTES)"
            )
        if self.signal_grace < 0:
            errors.append("resumable.signal_grace must be >= 0")
        if (
            self.chunk_walltime is not None
            and is_hms(self.chunk_walltime)
            and self.signal_grace >= parse_walltime(self.chunk_walltime)
        ):
            errors.append(
                f"resumable.signal_grace ({self.signal_grace}s) must be smaller than "
                f"chunk_walltime ({self.chunk_walltime}) — the signal fires that many "
                f"seconds BEFORE the walltime"
            )
        if self.max_chunks < 1:
            errors.append("resumable.max_chunks must be >= 1")
        if self.max_consecutive_failures < 1:
            errors.append("resumable.max_consecutive_failures must be >= 1")
        return errors

    def chain_config(self) -> ChainConfig:
        """The subset the pure state machine needs."""
        return ChainConfig(
            max_chunks=self.max_chunks,
            max_consecutive_failures=self.max_consecutive_failures,
        )

    def to_manifest(self) -> Dict[str, Any]:
        """Serialize for ``.hsm_manifest.json`` (so ``advance`` can reconstruct)."""
        return {
            "enabled": self.enabled,
            "chunk_walltime": self.chunk_walltime,
            "signal_grace": self.signal_grace,
            "resume_arg": self.resume_arg,
            "done_sentinel": self.done_sentinel,
            "checkpoint_subdir": self.checkpoint_subdir,
            "max_chunks": self.max_chunks,
            "max_consecutive_failures": self.max_consecutive_failures,
        }

    @classmethod
    def from_manifest(cls, data: Optional[Dict[str, Any]]) -> "ResumableConfig":
        return cls.from_dict(data)


@dataclass(frozen=True)
class ChunkProgress:
    """What a Slurm source observed about a chunk's progress (issue #12).

    Returned by ``ComputeSource.chunk_progress`` — the I/O probe that stats the
    persistent per-task dirs HSM owns. The orchestrator turns successive
    observations into the ``progressed`` bool the pure state machine consumes,
    so HSM never parses checkpoint CONTENTS, only a path's mtime.

    * ``done_indices`` — global task indices whose ``.hsm_done`` sentinel exists.
    * ``checkpoint_mtime`` — newest mtime (epoch seconds) of any file under any
      task's checkpoint subdir, or ``None`` if nothing has been written yet.
    """

    done_indices: frozenset[int]
    checkpoint_mtime: Optional[float]


@dataclass(frozen=True)
class ResumableContext:
    """Per-chunk bundle the Slurm sources thread into a chunk submission.

    Deliberately tiny — the source derives the ``--signal`` token at render
    time via ``slurm_protocol.format_signal`` (keeps the Slurm grammar in the
    hpc layer; this ``common`` dataclass stays pure data)."""

    chunk_index: int  # 0-based
    config: ResumableConfig

    @property
    def resume_from_present(self) -> bool:
        """Chunk >=2 gets a resume pointer; chunk 0 starts fresh."""
        return self.chunk_index > 0


def _filtered_remote_block(remote_block: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Keep only cluster-bound keys from a per-remote ``resumable:`` block."""
    if not remote_block:
        return {}
    out: Dict[str, Any] = {}
    for k, v in remote_block.items():
        if k in _REMOTE_ALLOWED_KEYS:
            out[k] = v
        elif k in _KNOWN_KEYS:
            logger.warning(
                "resumable: per-remote block may only set %s — ignoring %r "
                "(put workload guards like that in the sweep YAML)",
                ", ".join(sorted(_REMOTE_ALLOWED_KEYS)),
                k,
            )
    return out


def resolve_resumable_config(
    *,
    sweep_block: Optional[Dict[str, Any]] = None,
    remote_block: Optional[Dict[str, Any]] = None,
    cli_enabled: Optional[bool] = None,
    cli_chunk_walltime: Optional[str] = None,
) -> ResumableConfig:
    """Layer the three config sources into one resolved config.

    Precedence (lowest to highest): dataclass defaults ← per-remote block
    (cluster-bound knobs only) ← sweep YAML block ← CLI flags. Merging is done
    at the DICT level so a layer overrides only the keys it actually sets
    (constructing the dataclass per-layer would conflate "defaulted" with
    "explicitly set").
    """
    merged: Dict[str, Any] = {}
    merged.update(_filtered_remote_block(remote_block))
    if sweep_block:
        merged.update({k: v for k, v in sweep_block.items() if v is not None})
    if cli_chunk_walltime is not None:
        merged["chunk_walltime"] = cli_chunk_walltime
    if cli_enabled is not None:
        merged["enabled"] = cli_enabled
    return ResumableConfig.from_dict(merged)
