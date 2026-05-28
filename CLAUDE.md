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

## Architecture (one tier, post Pass B-heavy)

```
                    CLI (cli/sweep.py, cli/remote.py, ...)
                                 ↓
              SweepOrchestrator (build_compute_source → run_sweep_async)
                                 ↓
                 ┌─────── ComputeSource ABC (async) ───────┐
                 │                                         │
     LocalComputeSource    SlurmComputeSource    SSHComputeSource    DistributedComputeSource
                                                                     (fans across child ComputeSources)
```

The old `BaseJobManager` hierarchy + `DistributedSweepWrapper` +
`completion.py:SweepCompletor` were all deleted in Pass B-heavy. The
analyzer half of completion (`SweepCompletionAnalyzer` — pure on-disk
inspection) survived in `core/common/sweep_analysis.py` and still
backs `hsm sweep status` / `hsm sweep report`.

**Key files:**
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
- [`core/common/sweep_analysis.py`](src/hpc_sweep_manager/core/common/sweep_analysis.py) —
  `SweepCompletionAnalyzer`, `find_incomplete_sweeps`, `get_sweep_completion_summary`.
  Read-only on-disk analysis; used by `hsm sweep status` and `hsm sweep report`.

## Do NOT reintroduce

These were deliberately removed; resist resurrecting them.

- **`BaseJobManager` / `HPCJobManager` / `SlurmJobManager` / `PBSJobManager`
  / `LocalJobManager` / `DistributedSweepWrapper`.** The entire pre-async
  manager hierarchy was deleted in Pass B-heavy. Inheriting from
  `BaseJobManager` or wrapping a `JobManager` is the wrong shape — new
  backends inherit from `ComputeSource` (in `core/common/compute_source.py`).
- **`completion.py:SweepCompletor`** (the bloated re-runner). Was 1376 LOC
  of glue between the legacy manager hierarchy and a re-submit loop. The
  read-only `SweepCompletionAnalyzer` half survives in `sweep_analysis.py`.
  A clean `hsm sweep complete` will return one day as ~200-400 LOC built
  directly on `ComputeSource` + a `task_number_offset` knob — do NOT port
  the old shape forward.
- **`hsm hpc submit|queue|status|cancel`** + **`hsm local run|status|clean`.**
  Deleted CLI groups — duplicated `hsm sweep run` with different flags.
  Use `hsm sweep run --mode array|individual|local` instead.
- **`hsm sweep complete <sweep_id>`.** Deleted alongside `SweepCompletor`.
  `cli/sweep.py:run_sweep` errors clearly if it sees `config.complete: <id>`
  in a sweep yaml. Inspect with `hsm sweep status` / `hsm sweep report`,
  then re-submit manually with a filtered sweep config for now.
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
- **`hpc:` block in `.hsm/config.yaml` + its three accessors
  (`get_default_walltime` / `get_default_resources` / `get_hpc_system`).**
  Deleted — `default_walltime` and `default_resources` were opaque-string
  fallbacks the CLI applied *before* the typed `local:` / `slurm:` blocks,
  which meant they silently overrode the typed block's `walltime` field
  (a real bleed bug). `get_hpc_system` was dead. `max_array_size` moved
  into the `slurm:` block where it belongs (it's slurm-specific).
  Resource defaults now live exclusively in the typed `local:` /
  `slurm:` / per-remote `spec:` blocks. Don't bring `hpc:` back.
- **`hsm sync init|list|run|to|cache|clean` + the `sync/` module.**
  Deleted — SSH and distributed `ComputeSource`s already rsync results
  back via `collect_results()`, so the standalone sync tooling was
  redundant. The `.hsm/sync_config.yaml` it consumed is no longer
  generated by `hsm setup init`. If a user wants post-hoc rsync of a
  sweep dir to another machine, point them at plain `rsync` or a
  one-shot script — don't resurrect the module.

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
   slurm-style, OR from the typed `local:` / `slurm:` / per-remote `spec:`
   blocks) = how many GPUs *per task*. Both must be set for GPU mode;
   the slot queue falls back to CPU slots if either is missing.

5b. **Mode-scoped config blocks — no cross-mode bleed.** As of the post-
   sync-deletion refactor, `spec_from_cli(mode=...)` reads:
   - `local:` block → only for `--mode local`
   - `slurm:` block → only for `--mode array|individual`
   - **neither** for `--mode remote` / `--mode distributed` — per-remote
     spec lives at `distributed.remotes.<alias>.spec` and is layered in
     `build_ssh_source`.

   Before this refactor, every backend silently read the `slurm:` block
   for its default ResourceSpec. Do NOT restore that bleed; the user-
   surprise factor was high (a `slurm: { gpus: 4 }` block secretly
   changed local-mode behavior). `mode='auto'` is resolved by
   `resolve_auto_mode()` *before* spec lookup so the block matches the
   backend that will actually run.

6. **S3IT-style Slurm: `gpu_type` + uppercase GRES, NO flavour modules in script.**
   Two distinct quirks empirically + per [S3IT docs](https://docs.s3it.uzh.ch/cluster/job_submission/):

   - **Bare `--gpus=N` doesn't pin a type.** `#SBATCH --gpus=1` allocates "any GPU"
     and may land on the wrong family or fail with "Requested node configuration
     is not available" if your QoS limits aren't compatible with the default. You
     usually want `gpu_type: H100` (or L4/A100/H200) → HSM emits
     `#SBATCH --gres=gpu:H100:1`.
   - **GRES casing is uppercase on S3IT** — `sinfo -o "%P %G"` reports
     `gpu:H100:N` / `gpu:L4:N`. Use `gpu_type: H100`, not `h100`. Slurm GRES
     names are case-sensitive; lowercase → "Requested node configuration is
     not available."
   - **DO NOT put flavour modules (`h100`, `l4`, `multigpu`, etc.) in the rendered
     script.** Earlier versions of this gotcha said you needed BOTH `gpu_type` AND
     `modules: [h100]` — that was wrong. Per S3IT docs: flavour modules belong on
     the command line before `sbatch`, not in the batch script ("they set Slurm
     constraints that may conflict with job directives, causing allocation errors").
     The `modules:` field of the typed `slurm:` block is still useful for
     non-flavour modules (e.g. `module load matlab`); just don't put `h100` in it.
   - CUDA / cudnn usually NOT needed via modules — "containers, conda bundle
     CUDA" (S3IT docs). Only load `cuda` when compiling with `nvcc`.

   The opaque CLI `--resources` string can't express `gpu_type`; use the typed
   `slurm:` block in `.hsm/config.yaml`. See
   [docs/user_guide/HPC_EXECUTION.md](docs/user_guide/HPC_EXECUTION.md#the-typed-slurm-block--reach-fields---resources-cant).

7. **Array-job progress reports `1/1` not `N/N`.** `SlurmComputeSource`
   inserts one `JobInfo` for the whole array; `wait_for_all` reports `1 done`
   even when `N` tasks succeeded. The per-task truth lives in
   `tasks/*/task_info.txt`. Acceptable for now — defer until users complain.

8. **`params_to_hydra_args` quoting:** values with spaces/commas may render
   with nested quotes that bash collapses gracefully, but path-as-value
   params with spaces are fragile. Known limitation across all templates.

## Known limitations

- **No `hsm sweep complete` command in this build.** The bloated v0.1
  re-runner was deleted in Pass B-heavy. The read-only
  `SweepCompletionAnalyzer` survives so `hsm sweep status` /
  `hsm sweep report` still tell you which tasks failed; manual
  re-submission with a filtered sweep config is the current path. A
  clean rebuild on top of `ComputeSource` + a future `task_number_offset`
  knob is planned.

- The CLI `--resources` opaque string covers common Slurm fields
  (cpus/mem/gpus/qos/partition/walltime) but **cannot** express
  `gpu_type`, `modules`, `pre_script`, `account`, or
  `extra_directives`. Use the typed `slurm:` block in
  `.hsm/config.yaml` for those. See
  [`HPC_EXECUTION.md`](docs/user_guide/HPC_EXECUTION.md#the-typed-slurm-block--reach-fields---resources-cant).

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
