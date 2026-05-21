"""Typed resource specification for compute jobs.

A single :class:`ResourceSpec` describes the resources one job needs. Backends
translate it into their native syntax (Slurm ``#SBATCH`` directives, PBS
``select=...`` strings, local subprocess env, etc.).

Fields left as ``None`` mean "use the scheduler/backend default" — the backend
is free to omit the corresponding directive.

The dataclass is frozen so it can be reused safely across jobs in a batch and
hashed/compared. Use :func:`dataclasses.replace` or :meth:`ResourceSpec.merge`
to derive a modified spec.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any


@dataclass(frozen=True)
class ResourceSpec:
    walltime: str | None = None
    cpus_per_task: int | None = None
    mem: str | None = None
    mem_per_cpu: str | None = None
    gpus: int | None = None
    gpu_type: str | None = None
    partition: str | None = None
    qos: str | None = None
    account: str | None = None
    modules: tuple[str, ...] = ()
    pre_script: tuple[str, ...] = ()
    extra_directives: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.mem is not None and self.mem_per_cpu is not None:
            raise ValueError("ResourceSpec: cannot set both 'mem' and 'mem_per_cpu'")
        if self.cpus_per_task is not None and self.cpus_per_task < 1:
            raise ValueError(
                f"ResourceSpec: cpus_per_task must be >= 1, got {self.cpus_per_task}"
            )
        if self.gpus is not None and self.gpus < 0:
            raise ValueError(f"ResourceSpec: gpus must be >= 0, got {self.gpus}")
        if self.gpu_type is not None and (self.gpus is None or self.gpus < 1):
            raise ValueError("ResourceSpec: gpu_type requires gpus >= 1")
        for mod in self.modules:
            if not isinstance(mod, str) or not mod:
                raise ValueError(f"ResourceSpec: modules must be non-empty strings, got {mod!r}")
        for line in self.pre_script:
            if not isinstance(line, str):
                raise ValueError(f"ResourceSpec: pre_script entries must be strings, got {line!r}")

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> ResourceSpec:
        """Build a spec from a plain dict, converting list/dict fields to tuples."""
        if not data:
            return cls()
        normalized: dict[str, Any] = {}
        for k, v in data.items():
            if v is None:
                continue
            if k == "modules":
                normalized[k] = tuple(v) if not isinstance(v, str) else (v,)
            elif k == "pre_script":
                normalized[k] = tuple(v) if not isinstance(v, str) else (v,)
            elif k == "extra_directives":
                if isinstance(v, dict):
                    normalized[k] = tuple((str(kk), str(vv)) for kk, vv in v.items())
                else:
                    normalized[k] = tuple((str(kk), str(vv)) for kk, vv in v)
            else:
                normalized[k] = v
        return cls(**normalized)

    def merge(self, other: ResourceSpec | dict[str, Any] | None) -> ResourceSpec:
        """Return a new spec with non-``None`` fields of ``other`` overriding ``self``.

        Collections (``modules``, ``pre_script``, ``extra_directives``) override
        wholesale. If you need to extend them, construct the union yourself
        before passing it in the override.
        """
        if other is None:
            return self
        if isinstance(other, dict):
            other = ResourceSpec.from_dict(other)
        changes: dict[str, Any] = {}
        for k, v in other.__dict__.items():
            if v is None:
                continue
            if isinstance(v, tuple) and len(v) == 0:
                continue
            changes[k] = v
        if not changes:
            return self
        return replace(self, **changes)

    def to_dict(self) -> dict[str, Any]:
        """Render the spec as a plain dict, converting tuples back to lists/dict.

        Useful for templating contexts that don't deal well with tuples.
        """
        out: dict[str, Any] = {}
        for k, v in self.__dict__.items():
            if k == "extra_directives":
                out[k] = dict(v) if v else {}
            elif isinstance(v, tuple):
                out[k] = list(v)
            else:
                out[k] = v
        return out


# Tiny convenience factory used by config loaders and CLI option parsers.
def spec_from_legacy_resources(
    resources: str | None, scheduler: str | None = None
) -> ResourceSpec:
    """Best-effort parse of the legacy opaque ``resources`` string into a ResourceSpec.

    The legacy field accepted either Slurm-style ``--flag=value`` segments or
    PBS-style ``select=1:ncpus=4:mem=16gb`` strings depending on the backend.
    This helper extracts what it can; unknown fragments land in
    ``extra_directives``. Pass ``scheduler="slurm"`` or ``"pbs"`` for slightly
    smarter parsing.

    For greenfield code, prefer constructing a :class:`ResourceSpec` directly.
    """
    if not resources:
        return ResourceSpec()
    scheduler_norm = (scheduler or "").lower()

    fields: dict[str, Any] = {}
    extras: list[tuple[str, str]] = []

    text = resources.strip()
    if scheduler_norm == "pbs" or ("select=" in text and "--" not in text):
        for chunk in text.split(":"):
            if "=" not in chunk:
                continue
            key, val = chunk.split("=", 1)
            key = key.strip()
            val = val.strip()
            if key in ("ncpus", "cpus"):
                fields["cpus_per_task"] = int(val)
            elif key == "mem":
                fields["mem"] = val
            elif key == "ngpus":
                fields["gpus"] = int(val)
            elif key != "select":
                extras.append((key, val))
    else:
        for token in text.split():
            if not token.startswith("--"):
                extras.append((token, ""))
                continue
            if "=" in token:
                key, val = token.split("=", 1)
            else:
                key, val = token, ""
            key_stripped = key.lstrip("-")
            if key_stripped in ("cpus-per-task", "ntasks"):
                fields["cpus_per_task"] = int(val)
            elif key_stripped == "mem":
                fields["mem"] = val
            elif key_stripped == "mem-per-cpu":
                fields["mem_per_cpu"] = val
            elif key_stripped == "gpus":
                fields["gpus"] = int(val)
            elif key_stripped == "partition":
                fields["partition"] = val
            elif key_stripped == "qos":
                fields["qos"] = val
            elif key_stripped == "account":
                fields["account"] = val
            elif key_stripped == "time":
                fields["walltime"] = val
            else:
                extras.append((key, val))

    if extras:
        fields["extra_directives"] = tuple(extras)
    return ResourceSpec(**fields)
