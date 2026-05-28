# Architecture

How HPC-Sweep-Manager (HSM) is wired internally. For an agent on-boarding
view, start with [CLAUDE.md](CLAUDE.md). For user-facing recipes see
[docs/user_guide/](docs/user_guide/).

## Top-level shape

```
   user
    │
    ▼
   CLI    (cli/sweep.py, cli/remote.py, cli/local.py, cli/hpc.py, ...)
    │
    ▼
   SweepOrchestrator                       ← async lifecycle driver
    │   (core/common/sweep_orchestrator.py)
    │
    ▼
   ComputeSource (ABC, async)              ← unified backend interface
    │   (core/common/compute_source.py)
    │
    ┌──────────────┬──────────────┬──────────────────┐
    ▼              ▼              ▼                  ▼
  Local          Slurm           SSH              Distributed
  (process       (sbatch)        (asyncssh +      (fan-out wrapper
   pool +                         rsync +          over child
   slots)                         create_process)  ComputeSources)
```

The CLI is the only sync boundary; everything below it is `async` and is
driven from one `asyncio.run(run_sweep_async(...))` per sweep.

A separate **legacy tier** still lives in the repo (the `BaseJobManager`
hierarchy + `DistributedSweepWrapper`), reached only by the completion
code path. It will go when completion migrates to the orchestrator.

## Why a unified `ComputeSource` ABC

Earlier HSM had three parallel manager hierarchies (`HPCJobManager`,
`LocalJobManager`, `RemoteJobManager`) plus an over-grown 2.6k-line
`remote_manager.py`. Adding a new backend meant duplicating slot logic,
result collection, GPU pinning, etc. four times.

The ABC at
[`core/common/compute_source.py`](src/hpc_sweep_manager/core/common/compute_source.py)
collapses that to seven async methods every backend implements the same
way:

- `setup(sweep_dir, sweep_id) -> bool` — open connection, allocate slots,
  create dirs. Idempotent: failure leaves no half-state.
- `submit_job(params, job_name, sweep_id, wandb_group=, spec=) -> job_id` —
  acquire a slot, launch one task.
- `submit_batch(params_list, ..., mode='individual'|'array')` — default
  loops `submit_job`; backends override for true scheduler-side array
  submission.
- `get_job_status(job_id) -> str` — current status string.
- `cancel_job(job_id) -> bool` — terminate a running job.
- `wait_for_all(poll_interval=, on_progress=)` — block until all active
  jobs reach a terminal state.
- `collect_results(job_ids=None) -> bool` — rsync results / no-op /
  whatever the backend needs to surface outputs locally.
- `health_check()` — single backend self-check.
- `cleanup()` — release resources (close ssh conn, kill stragglers).

`JobInfo` (same file) is the dataclass carried through the system: status
string, params dict, source name, timestamps, task_dir.

[`ResourceSpec`](src/hpc_sweep_manager/core/common/resource_spec.py) is
the typed resource description: `walltime`, `cpus_per_task`, `mem`,
`gpus`, `gpu_type`, `partition`, `qos`, `account`, `modules`,
`pre_script`, `extra_directives`. Frozen dataclass — reused across jobs
in a batch. Backends translate to their native syntax (Slurm
`#SBATCH ...`, PBS `select=...`, local subprocess env, etc.).

## Push-SSH lifecycle

The `SSHComputeSource`
([source](src/hpc_sweep_manager/core/remote/ssh_compute_source.py)) is
self-contained: the remote needs only `bash + rsync + optionally
nvidia-smi`. HSM doesn't need to be installed there. One persistent
asyncssh connection is opened in `setup()` and reused for the whole
sweep.

```
┌─────────────────────── setup(sweep_dir, sweep_id) ────────────────────┐
│ open asyncssh conn  ──►  echo $HOME (resolve ~)  ──►  mkdir layout    │
│ rsync push local project → {remote_root}/{project}/code/              │
│ nvidia-smi probe → list[gpu_indices]                                  │
│ normalize_gpu_allowlist + partition_gpu_slots → asyncio.Queue         │
│ resolve_run_prefix(conda_env, python_path) → "conda run -n env python"│
└───────────────────────────────────────────────────────────────────────┘
                                  │
            ┌─────────────────────┴─────────────────────┐
            ▼                                           ▼
┌── submit_job (per task) ──┐               ┌── wait_for_all ──┐
│ slot = await queue.get()  │               │ await each       │
│ render ssh_compute_source │               │ monitor task →   │
│   .sh.j2 (CUDA pin, conda │               │ final_statuses   │
│   init, modules, command) │               └──────────────────┘
│ conn.run("cat > script",  │
│   input=script_text)      │                         │
│ proc = conn.create_process│                         ▼
│   (f"bash {script}")      │              ┌── collect_results ──┐
│ spawn monitor coro:       │              │ rsync pull tasks/ ← │
│   await proc.wait()       │              │ if no FAILED jobs:  │
│   → status COMPLETED|FAIL │              │   rm -rf remote     │
│   release slot to queue   │              │   per-sweep dir     │
└───────────────────────────┘              └─────────────────────┘
                                                      │
                                                      ▼
                                            ┌── cleanup ──┐
                                            │ close conn  │
                                            │ wait_closed │
                                            └─────────────┘
```

Key properties:

- **Rolling code mirror.** rsync push uses `--delete`; the code cache at
  `{remote_root}/{project}/code/` is reused across sweeps (only diffs go
  over the wire). Per-sweep work lives at
  `{remote_root}/{project}/sweeps/{sweep_id}/`.

- **Slot-based back-pressure.** `setup()` builds a fixed-size
  `asyncio.Queue` of slots (GPU index lists for GPU mode, `None` for CPU
  mode). `submit_job` blocks on `queue.get()` if all slots are busy —
  so submitting `N >> slots` tasks transparently throttles concurrency
  to the slot count.

- **Auto-cleanup on success.** `collect_results` rsyncs `tasks/` back
  and then `rm -rf`'s the remote per-sweep dir IF all jobs COMPLETED
  (and `keep_remote_on_success=False`). On any FAILED job the dir
  stays for debugging — use `hsm remote clean <alias>` to nuke it
  manually after.

- **Two narrow I/O seams.** `_open_connection()` and `_run_rsync()` are
  overridable methods so unit tests inject fakes (see
  `tests/unit/test_ssh_compute_source.py`). No real SSH endpoint needed
  for the test suite.

## Slot back-pressure (detail)

`partition_gpu_slots(allowed, gpus_per_job, cpu_slots)` in
[`core/remote/push_exec.py`](src/hpc_sweep_manager/core/remote/push_exec.py)
returns the slot list:

- 4 GPUs allowed, `gpus_per_job=1` → `[[0], [1], [2], [3]]` (4 slots, one
  GPU each).
- 4 GPUs allowed, `gpus_per_job=2` → `[[0, 1], [2, 3]]` (2 slots, two
  GPUs each).
- 3 GPUs allowed, `gpus_per_job=2` → `[[0, 1]]` (1 slot, GPU 2 unused —
  remainder dropped).
- No GPUs (or `gpus_per_job=0`) → `[None] * cpu_slots` (CPU fallback).

The same pattern drives `LocalComputeSource`. The `--gpus` CLI flag
populates `allowed` (via `parse_gpus_arg` in `ssh_compute_source.py`);
`spec.gpus` (from `--resources --gpus=N`) is `gpus_per_job`.

## Slurm path

`SlurmComputeSource`
([source](src/hpc_sweep_manager/core/hpc/slurm_compute_source.py))
renders two templates:

- [`slurm_single.sh.j2`](src/hpc_sweep_manager/templates/slurm_single.sh.j2)
  for `--mode individual` (one `sbatch` per task).
- [`slurm_array.sh.j2`](src/hpc_sweep_manager/templates/slurm_array.sh.j2)
  for `--mode array` (one `sbatch --array=1-N`).

The orchestrator's `spec_from_cli()` parses the legacy `--resources`
string into a `ResourceSpec`. The Slurm template emits `#SBATCH`
directives from the spec fields. **Known limitation:** the CLI string
can express `cpus_per_task`, `mem`, `gpus`, `qos`, `partition`,
`account`, and `walltime`, but NOT `gpu_type`, `modules`,
`pre_script`, or `extra_directives`. For those, drive
`SlurmComputeSource` directly from Python — see
[`examples/smoke_slurm.py`](examples/smoke_slurm.py).

## Distributed path

`DistributedComputeSource`
([source](src/hpc_sweep_manager/core/distributed/distributed_compute_source.py))
fans a sweep across multiple child `ComputeSource`s. Children are built
from `hsm_config.yaml`'s `distributed:` block (one local +
N SSH push sources via `build_ssh_source(...)`). The internal
`DistributedJobManager` (the 1736-LOC monolith in
`core/distributed/distributed_manager.py`) handles
round-robin/least-loaded dispatch, monitoring, and result normalization.
It's wrapped behind the ABC; decomposing it further is future work.

## Legacy tier (what's still alive and why)

These classes are still imported but only the **completion path** uses
them:

- `core/common/base_manager.py:BaseJobManager` — common job dataclass +
  sync `wait_for_all`.
- `core/hpc/hpc_base.py:HPCJobManager` (+ `slurm_manager.py`,
  `pbs_manager.py`).
- `core/local/local_manager.py:LocalJobManager`.
- `core/distributed/wrapper.py:DistributedSweepWrapper`.
- `core/common/completion.py` — the completion runner itself.

Plus three CLI commands that still use the legacy tier directly:
`cli/hpc.py` (`hsm hpc submit|queue|status|cancel`), `cli/local.py`
(`hsm local run|status|clean`).

Migrating `completion.py` to the unified orchestrator requires a new
`task_number_offset` knob on `ComputeSource` (so completion runs can
continue numbering from where the original sweep left off). When that
lands, **Pass B-heavy** deletes all of the above in one commit.

## Test architecture

No docker. No real cluster needed.

- **PATH-stub fixtures** under
  [`tests/fixtures/fake_slurm/`](tests/fixtures/fake_slurm/) provide
  fake `sbatch`, `squeue`, `scancel`, `sinfo` scripts that maintain a
  jobs.jsonl state file with configurable PENDING/RUNNING/COMPLETED
  transitions. Fixture installs them into `$PATH` for the test.
- **Fake `nvidia-smi`** at `tests/fixtures/fake_nvidia_smi/` for GPU
  detection tests.
- **Fake asyncssh conn** for `SSHComputeSource` tests — a tiny class
  that records `run()` and `create_process()` calls so tests can
  assert on what would have gone over the wire. See
  `tests/unit/test_ssh_compute_source.py:FakeConn`.

Integration tests under `tests/integration/` exercise the full
setup→submit→wait→collect→cleanup lifecycle against the fake
fixtures. Real-hardware validation lives in `examples/smoke_*.sh`.

## Configuration

- **`.hsm/config.yaml`** (project-level) — auto-generated by
  `hsm setup init`. Contains `project`, `paths`, `hpc`, `wandb` blocks.
  An optional `distributed:` block enables `--mode distributed` and
  `--mode remote` (see
  [`docs/user_guide/SSH_EXECUTION.md`](docs/user_guide/SSH_EXECUTION.md)
  for that block's schema).
- **`sweeps/*.yaml`** — sweep configs with `grid:`, `paired:`,
  `defaults:`, `script:` keys. See
  [`docs/user_guide/getting_started.md`](docs/user_guide/getting_started.md).

The config is read by `HSMConfig.load()` in
[`core/common/config.py`](src/hpc_sweep_manager/core/common/config.py).

## Known limitations

- **CLI can't express full `ResourceSpec`.** `--resources` is opaque text;
  `gpu_type`, `modules`, `pre_script`, `account`, `extra_directives`
  require direct-Python use of `SlurmComputeSource`. Phase 3.4 (typed
  `slurm:` block in `hsm_config.yaml`) will fix this.
- **Completion runs (`hsm sweep complete`) bypass the orchestrator.**
  They use the legacy job manager path because `ComputeSource` doesn't
  expose a starting-task-number knob. Pass B-heavy blocked on this.
- **Slurm array progress shows `1/1` not `N/N`.** `SlurmComputeSource`
  inserts one `JobInfo` per `sbatch` invocation (including arrays), so
  the progress callback can't see per-task progress. Per-task truth is
  in `tasks/*/task_info.txt`. Acceptable until users complain.
- **`params_to_hydra_args` quoting** for values with spaces/commas
  works but produces nested-quote sequences. Fix requires touching
  every wrapper template (`slurm_*.sh.j2`, `local_compute_source.sh.j2`,
  `ssh_compute_source.sh.j2`).
- **Distributed dispatch is fused.** `DistributedComputeSource.submit_batch`
  blocks until completion (delegates to the manager's
  `submit_distributed_sweep`). A future cross-host queueing UX would
  split submission from waiting.

## References

- [CLAUDE.md](CLAUDE.md) — short-form agent on-boarding.
- [docs/user_guide/SSH_EXECUTION.md](docs/user_guide/SSH_EXECUTION.md) —
  push-model recipe.
- [docs/user_guide/HPC_EXECUTION.md](docs/user_guide/HPC_EXECUTION.md) —
  Slurm recipe + the `--resources` gap workaround.
- [docs/api_reference/compute_sources.md](docs/api_reference/compute_sources.md) —
  per-class API surface.
