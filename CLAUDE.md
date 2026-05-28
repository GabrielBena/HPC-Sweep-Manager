# CLAUDE.md — Agent on-boarding for HPC-Sweep-Manager

This file orients an AI coding agent landing on this repo cold. Read it first;
then dip into [ARCHITECTURE.md](ARCHITECTURE.md) for the design rationale and
[docs/user_guide/](docs/user_guide/) for user-facing recipes.

## Quick identity

- **What:** HPC-Sweep-Manager (HSM) — a Python package + CLI for running
  hyperparameter sweeps over Hydra-style training scripts.
- **Where it runs:** local machines, Slurm/PBS HPC clusters, and remote
  Linux boxes via SSH (push-model rsync — no remote install required).
- **Language / floor:** Python 3.11+ (`pyproject.toml`).
- **Entry point:** `hsm` (installed via `pip install -e .`).

## Execution model — the 6 modes

`hsm sweep run --mode <X>` (or `--mode auto` to autodetect) routes through
the **SweepOrchestrator** in
[`core/common/sweep_orchestrator.py`](src/hpc_sweep_manager/core/common/sweep_orchestrator.py):

| Mode | Backend | Where |
|---|---|---|
| `local` | `LocalComputeSource` | this machine (slot queue + per-process monitor) |
| `array` | `SlurmComputeSource` | submits one Slurm `sbatch --array=...` job |
| `individual` | `SlurmComputeSource` | one `sbatch` per parameter combo |
| `remote` | `SSHComputeSource` | push-model rsync to a single SSH host |
| `distributed` | `DistributedComputeSource` | fans across multiple SSH hosts + local |
| `auto` | resolved at runtime | `array` if `sbatch` on PATH, else `local` |

`hsm sweep run --remote <alias>` implies `--mode remote` (you don't pass
both). `--gpus all\|cpu\|N\|i,j,k` is the per-remote GPU allowlist
(separate from `spec.gpus`, which is per-job count — see "Gotchas" below).

## Architecture in three tiers

```
                    CLI (cli/sweep.py, cli/remote.py, ...)
                                 ↓
              SweepOrchestrator (build_compute_source → run_sweep_async)
                                 ↓
                 ┌─────── ComputeSource ABC (async) ───────┐
                 │                                         │
     LocalComputeSource    SlurmComputeSource    SSHComputeSource    DistributedComputeSource
                                                                     (wraps DistributedJobManager — legacy interior)

  Below is the LEGACY tier — still alive but only the completion path uses it.
  Slated for deletion in Pass B-heavy (once completion.py migrates to the
  orchestrator):

     BaseJobManager → {HPCJobManager → {SlurmJobManager, PBSJobManager},
                       LocalJobManager}
     DistributedSweepWrapper
```

**Live path (use these):**
- [`core/common/sweep_orchestrator.py`](src/hpc_sweep_manager/core/common/sweep_orchestrator.py) —
  `build_compute_source(mode, ...)`, `run_sweep_async(...)`, `spec_from_cli(...)`.
- [`core/common/compute_source.py`](src/hpc_sweep_manager/core/common/compute_source.py) —
  `ComputeSource` ABC + `JobInfo` dataclass.
- [`core/common/resource_spec.py`](src/hpc_sweep_manager/core/common/resource_spec.py) —
  typed `ResourceSpec` (walltime, cpus_per_task, mem, gpus, gpu_type, modules,
  qos, pre_script, extra_directives).
- [`core/local/local_compute_source.py`](src/hpc_sweep_manager/core/local/local_compute_source.py).
- [`core/hpc/slurm_compute_source.py`](src/hpc_sweep_manager/core/hpc/slurm_compute_source.py).
- [`core/remote/ssh_compute_source.py`](src/hpc_sweep_manager/core/remote/ssh_compute_source.py) —
  push-model class + `build_ssh_source(...)` factory + `parse_gpus_arg(...)` helper.
- [`core/remote/push_exec.py`](src/hpc_sweep_manager/core/remote/push_exec.py) —
  pure helpers (`DEFAULT_RSYNC_EXCLUDES`, `normalize_gpu_allowlist`,
  `partition_gpu_slots`, `resolve_run_prefix`, `build_rsync_push_cmd`,
  `build_rsync_pull_cmd`).
- [`core/distributed/distributed_compute_source.py`](src/hpc_sweep_manager/core/distributed/distributed_compute_source.py).

**Legacy tier (do not extend, only fix-in-place if blocked):**
- `core/common/base_manager.py:BaseJobManager`.
- `core/hpc/hpc_base.py:HPCJobManager` + `core/hpc/slurm_manager.py`/`pbs_manager.py`.
- `core/local/local_manager.py:LocalJobManager`.
- `core/distributed/wrapper.py:DistributedSweepWrapper`.
- `core/common/completion.py` — the completion code path (`hsm sweep complete`)
  still wires these up. Migrating it to the orchestrator is the gating task
  for deleting the legacy tier.

## Do NOT reintroduce

These were deliberately removed; resist resurrecting them.

- **`--mode remote --remote <alias>` shape.** The flag was renamed: now it's
  just `--remote <alias>` (implies `--mode remote`). See `cli/sweep.py:run_cmd`.
- **`hsm distributed init|add|test|health|remove`.** Deleted — duplicated
  `hsm remote` entirely. Multi-host execution is `--mode distributed` driven
  by `hsm_config.yaml`'s `distributed:` block.
- **`hsm results collect` / `hsm collect-results`.** Deleted — the push-SSH
  `SSHComputeSource.collect_results()` does rsync-pull automatically as part
  of `run_sweep_async()`.
- **`RemoteJobManager` / `RemoteDiscovery` / `RemoteValidator` /
  `discover_remote_config`.** Auto-discovery of remote project structure is
  gone for good. The push model rsyncs the local tree up; the remote is a
  dumb compute target (bash + rsync + optional nvidia-smi).
- **`SSHComputeSource.from_remote_config`.** The transitional adapter
  classmethod was deleted in Pass B-medium; use `build_ssh_source(...)`.
- **`source.remote_manager` attribute access.** Push-model
  `SSHComputeSource` has no `.remote_manager` — that was a wrapper of the
  deleted `RemoteJobManager`. The sentinel that kept legacy code paths
  short-circuiting is also gone.

## Gotchas — patched, do not re-discover

These are real bugs that have been fixed; if you see code that looks like
it's trying to reintroduce them, push back.

1. **Non-interactive SSH shells skip `~/.bashrc` / `~/.zshrc`** → `conda`
   is usually not on PATH. The wrapper template
   ([`templates/ssh_compute_source.sh.j2`](src/hpc_sweep_manager/templates/ssh_compute_source.sh.j2))
   sources `conda.sh` from `~/miniconda3`/`~/anaconda3`/`~/.miniconda3`/
   `/opt/conda` automatically when `conda_env` is set.

2. **`~` in `output.dir=~/path` does NOT tilde-expand** inside double-quoted
   COMMAND strings (bash only expands `~` in cd/mkdir tokens). `SSHComputeSource.setup()`
   probes `echo $HOME` once and substitutes if `remote_root` starts with `~`.

3. **`script_path` must be relative-to-project on the remote.** After
   `cd <remote_code_dir>` on the rsync'd mirror, an absolute LOCAL path
   won't exist. `SSHComputeSource.__init__` normalizes it.

4. **`run_sweep_async` MUST call `collect_results()` + `cleanup()`** after
   `wait_for_all()`. No-op for Local/Slurm; load-bearing for SSH (where
   `collect_results` IS the rsync-pull). Don't remove these calls.

5. **Two-level GPU semantics:** `--gpus` (CLI) = which GPUs are *visible*
   on the box (allowlist). `spec.gpus` (from `--resources --gpus=N`
   slurm-style) = how many GPUs *per task*. Both must be set for GPU mode;
   the slot queue falls back to CPU slots if either is missing.

6. **S3IT-style Slurm needs BOTH `gpu_type` AND `modules`.** Setting
   `--gpus=1` alone produces `#SBATCH --gpus=1` which does NOT allocate a
   GPU on S3IT (confirmed empirically). You need `gpus=1, gpu_type="h100",
   modules=("h100",)` → emits `#SBATCH --gres=gpu:h100:1` + `module load h100`.
   See "Known CLI limitations" below.

7. **Array-job progress reports `1/1` not `N/N`.** `SlurmComputeSource`
   inserts one `JobInfo` for the whole array; `wait_for_all` reports `1 done`
   even when `N` tasks succeeded. The per-task truth lives in
   `tasks/*/task_info.txt`. Acceptable for now — defer until users complain.

8. **`params_to_hydra_args` quoting:** values with spaces/commas may render
   with nested quotes that bash collapses gracefully, but path-as-value
   params with spaces are fragile. Known limitation across all templates.

## Known CLI limitations

- The opaque `--resources "..."` string can only express a subset of
  `ResourceSpec`. **Not expressible from CLI:** `modules`, `gpu_type`,
  `pre_script`, `account`, `extra_directives`. For these you must either
  (a) put them in the legacy `--resources` string as raw scheduler flags,
  or (b) use the direct-Python entry point in
  [`examples/smoke_slurm.py`](examples/smoke_slurm.py) with a typed
  `ResourceSpec`. A future Phase 3.4 will add a typed `slurm:` block in
  `hsm_config.yaml` to close this gap.

- **Completion runs (`hsm sweep complete <id>`)** still flow through the
  legacy `BaseJobManager` path — they need `starting_task_number`
  propagation that the unified `ComputeSource` doesn't have yet. Blocker
  for Pass B-heavy deletion of the legacy tier.

## Testing model

- **`pytest tests/unit tests/cli tests/integration`** — full suite. There
  is a baseline of ~6 pre-existing failures (Click 8 + `log_cli=true` +
  Python 3.13 interaction in `tests/cli/test_sweep_command.py` plus one
  stale `LocalJobManager.show_output` assertion in
  `tests/unit/test_local_manager.py`). **Do not chase these as regressions.**
- **PATH-stub fixtures** under `tests/fixtures/` provide fake
  `sbatch/squeue/scancel/sinfo` and a fake `nvidia-smi` so unit tests
  don't need a real cluster. No docker, no integration containers.
- **Real-hardware smoke tests** in `examples/` are runnable against a real
  Slurm cluster ([`smoke_cli.sh`](examples/smoke_cli.sh)) and a real SSH
  remote ([`smoke_ssh_cli.sh`](examples/smoke_ssh_cli.sh)).

## When you add a new feature

1. Decide which tier it lives in: ComputeSource path (default) vs legacy
   manager path (only if you're fixing a completion-path bug).
2. If GPU-related: respect the two-level semantics. `--gpus` is the
   allowlist, `spec.gpus` is per-task.
3. If SSH-related: read the wire-level details in
   [ARCHITECTURE.md](ARCHITECTURE.md#push-ssh-lifecycle) before assuming
   asyncssh behavior.
4. Add unit tests under `tests/unit/` (fake-conn / fake-Slurm if applicable).
5. Run the appropriate `examples/smoke_*.sh` for live validation if you
   have hardware access.
6. If you touch the rendered wrapper templates
   ([`templates/`](src/hpc_sweep_manager/templates/)), the existing
   smoke-script outputs (`sentinel.txt`, `task_info.txt`) are the contract
   to preserve.

## Cross-references

- **Users:** [README.md](README.md) (quickstart),
  [docs/user_guide/getting_started.md](docs/user_guide/getting_started.md),
  [docs/user_guide/SSH_EXECUTION.md](docs/user_guide/SSH_EXECUTION.md),
  [docs/user_guide/HPC_EXECUTION.md](docs/user_guide/HPC_EXECUTION.md).
- **API:** [docs/api_reference/](docs/api_reference/) — the
  `compute_sources.md` doc is the live one; the others are legacy with banners.
- **Design rationale:** [ARCHITECTURE.md](ARCHITECTURE.md).
- **Project layout:** [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md).
- **CLI reference:** [docs/cli/README.md](docs/cli/README.md).
