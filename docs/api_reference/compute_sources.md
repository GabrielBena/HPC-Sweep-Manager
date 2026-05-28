# Compute sources API

The `ComputeSource` ABC + its four implementations are the live backend
interface. Every CLI mode (`local`, `array`, `individual`, `remote`,
`distributed`, `auto`) routes through one of these via the
`SweepOrchestrator`.

For the broader design context see
[../../ARCHITECTURE.md](../../ARCHITECTURE.md). For agent on-boarding
see [../../CLAUDE.md](../../CLAUDE.md).

## `ComputeSource` (abstract)

**Source:** [`core/common/compute_source.py`](../../src/hpc_sweep_manager/core/common/compute_source.py)

The async ABC every backend implements. Lifecycle: `setup` →
`submit_job` (or `submit_batch`) → `wait_for_all` → `collect_results`
→ `cleanup`.

```python
class ComputeSource(ABC):
    def __init__(self, name: str, source_type: str, max_parallel_jobs: int): ...

    @abstractmethod
    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool: ...

    @abstractmethod
    async def submit_job(
        self,
        params: Dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: Optional[str] = None,
        spec: Optional[ResourceSpec] = None,
    ) -> str: ...

    async def submit_batch(
        self,
        params_list: List[Dict[str, Any]],
        sweep_id: str,
        mode: SubmissionMode = "individual",  # or "array"
        spec: Optional[ResourceSpec] = None,
        wandb_group: Optional[str] = None,
        job_name_prefix: Optional[str] = None,
    ) -> List[str]: ...

    @abstractmethod
    async def get_job_status(self, job_id: str) -> str: ...
    @abstractmethod
    async def cancel_job(self, job_id: str) -> bool: ...
    @abstractmethod
    async def collect_results(self, job_ids: Optional[List[str]] = None) -> bool: ...
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]: ...
    @abstractmethod
    async def cleanup(self) -> None: ...

    async def wait_for_all(
        self,
        poll_interval: float = 5.0,
        on_progress: Optional[ProgressCallback] = None,
    ) -> Dict[str, str]: ...  # default polls get_job_status; override for stronger signals
```

`JobInfo` (same file) is the dataclass each job is tracked as. The
canonical statuses are `PENDING / RUNNING / COMPLETED / FAILED /
CANCELLED`. `update_job_status(job_id, new_status)` moves jobs between
`active_jobs` and `completed_jobs`.

## `LocalComputeSource`

**Source:** [`core/local/local_compute_source.py`](../../src/hpc_sweep_manager/core/local/local_compute_source.py)

Self-contained local executor — does not wrap any legacy job manager.

- Probes `nvidia-smi -L` at `setup()`; partitions GPUs into slots of
  size `spec.gpus`.
- One async subprocess per task via `asyncio.create_subprocess_exec`.
- Per-task monitor coroutine awaits `proc.wait()`, releases its slot.
- Overrides `wait_for_all` to await the monitor tasks directly (avoids
  a poll/status race with the default ABC polling loop).
- Falls back to `max_parallel_jobs` CPU slots when no GPUs or
  `spec.gpus = 0`.

Used by `--mode local` and as a child of `--mode distributed`.

## `SlurmComputeSource`

**Source:** [`core/hpc/slurm_compute_source.py`](../../src/hpc_sweep_manager/core/hpc/slurm_compute_source.py)

Native Slurm backend. Replaces the legacy `SlurmJobManager` (deleted in Pass B-heavy).

- Renders [`slurm_single.sh.j2`](../../src/hpc_sweep_manager/templates/slurm_single.sh.j2)
  per task (`--mode individual`) or
  [`slurm_array.sh.j2`](../../src/hpc_sweep_manager/templates/slurm_array.sh.j2)
  once (`--mode array`).
- Translates `ResourceSpec` fields to `#SBATCH` directives — including
  `gpu_type` and `modules` (which are unreachable from the CLI
  `--resources` string today; see
  [HPC_EXECUTION.md](../user_guide/HPC_EXECUTION.md#known-limitation--what---resources-cant-express)).
- Polls `squeue` for status; uses the default ABC `wait_for_all`.
- Optional `qos_whitelist` constructor arg gates QOS strings at submit
  time (handy if your cluster has a tier list).

Used by `--mode array`, `--mode individual`, and `--mode auto` (when
`sbatch` is on PATH).

## `SSHComputeSource`

**Source:** [`core/remote/ssh_compute_source.py`](../../src/hpc_sweep_manager/core/remote/ssh_compute_source.py)

Push-model SSH backend — self-contained, doesn't depend on anything on
the remote besides bash + rsync + optionally nvidia-smi.

- `setup()`: opens one persistent asyncssh connection, probes `$HOME`,
  rsyncs the local project to `{remote_root}/{project}/code/`,
  `nvidia-smi`-probes the remote, partitions GPU slots, computes the
  run prefix (`conda run -n <env> python` or bare interpreter).
- `submit_job()`: acquires a slot, renders
  [`ssh_compute_source.sh.j2`](../../src/hpc_sweep_manager/templates/ssh_compute_source.sh.j2),
  writes it remotely via `conn.run("cat > script", input=...)`, launches
  with `conn.create_process("bash script")`, spawns a monitor coro.
- `collect_results()`: rsync-pull `tasks/` back; `rm -rf` the remote
  per-sweep dir on full success (preserves the code cache for next sweep).
- `cleanup()`: closes the asyncssh connection.

Used by `--mode remote` (single host) and as a child of
`--mode distributed` (many hosts).

### Companion helpers

```python
from hpc_sweep_manager.core.remote.ssh_compute_source import (
    SSHComputeSource,
    build_ssh_source,    # config-driven factory (precedence: override > per-remote > global > default)
    parse_gpus_arg,      # parses --gpus value ("all" | "cpu" | "N" | "i,j,k") → None | int | list[int]
)
```

`build_ssh_source(name, remote_cfg, distributed_cfg, project_dir,
script_path, ..., gpus_override=, conda_env_override=)` is the entry
the orchestrator + the distributed child builder both use.

`core/remote/push_exec.py` holds the pure helpers everything else
composes from:
- `DEFAULT_RSYNC_EXCLUDES` — the always-excluded patterns.
- `normalize_gpu_allowlist(gpus, detected)` — `None`/`int`/`list` → list[int].
- `partition_gpu_slots(allowed, gpus_per_job, cpu_slots)` → slot list.
- `resolve_run_prefix(conda_env, python_path)` → command prefix string.
- `build_rsync_push_cmd / build_rsync_pull_cmd` → argv lists.

## `DistributedComputeSource`

**Source:** [`core/distributed/distributed_compute_source.py`](../../src/hpc_sweep_manager/core/distributed/distributed_compute_source.py)

Wraps the legacy 1736-LOC `DistributedJobManager` (still the interior
fan-out engine) behind the unified ABC. Used by `--mode distributed`.

- Constructor takes either an explicit `child_sources=[...]` list or
  an `hsm_config=...` which it uses to build children in `setup()`
  (one `LocalComputeSource` for `local`, one `SSHComputeSource` per
  enabled entry in `distributed.remotes`).
- `submit_batch()` is a **fused** submit+wait+collect (delegates to the
  manager's blocking `submit_distributed_sweep`); `wait_for_all` returns
  the already-final statuses. A future refactor will split these.
- Strategies (round-robin, least-loaded) come from
  `DistributedSweepConfig` in `distributed_manager.py`.

## Boundary objects

- **`JobInfo`** in `core/common/compute_source.py` — per-task dataclass
  (job_id, job_name, params, source_name, status, submit/start/complete
  times, task_dir).
- **`SweepResult`** in `core/common/sweep_orchestrator.py` —
  `(sweep_id, sweep_dir, job_ids, final_statuses, submission_mode, source_type)`.
- **`ResourceSpec`** in `core/common/resource_spec.py` — the typed
  resource description. `merge(other)` for layered defaults;
  `spec_from_legacy_resources(string)` parses the CLI `--resources`
  string.

## Lifecycle from Python (no CLI)

```python
import asyncio
from pathlib import Path
from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.common.sweep_orchestrator import (
    build_compute_source,
    run_sweep_async,
)

source, mode, sub_mode = build_compute_source(
    mode="remote",
    python_path="python",
    script_path="train.py",
    project_dir=".",
    default_spec=ResourceSpec(gpus=1),
    remote_alias="my-box",
    gpus_override=[0, 1],     # only use GPUs 0 and 1
)

params_list = [{"seed": s, "lr": lr} for s in (1, 2, 3) for lr in (1e-3, 1e-2)]

result = asyncio.run(
    run_sweep_async(
        source=source,
        sweep_dir=Path("sweeps/outputs/my_sweep"),
        sweep_id="my_sweep",
        params_list=params_list,
        submission_mode=sub_mode,
        wandb_group="my_sweep",
    )
)
# result: SweepResult with final_statuses, job_ids, etc.
```

The orchestrator calls `setup → submit_batch → wait_for_all →
collect_results → cleanup` in order; `run_sweep_async(wait=False)`
returns early after submission if you want to drive the lifecycle
yourself.

## See also

- [`../user_guide/SSH_EXECUTION.md`](../user_guide/SSH_EXECUTION.md) — push-model recipe.
- [`../user_guide/HPC_EXECUTION.md`](../user_guide/HPC_EXECUTION.md) — Slurm/PBS recipe.
- [`examples/smoke_slurm.py`](../../examples/smoke_slurm.py) — direct-Python Slurm example.
- [`examples/smoke_ssh_cli.sh`](../../examples/smoke_ssh_cli.sh) — push-SSH smoke test.
