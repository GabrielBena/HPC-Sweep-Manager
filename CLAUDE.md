# CLAUDE.md — Agent on-boarding for HPC-Sweep-Manager

This file orients an AI coding agent landing on this repo cold. Read it first;
then dip into [ARCHITECTURE.md](ARCHITECTURE.md) for the design rationale and
[docs/user_guide/](docs/user_guide/) for user-facing recipes.

## Workflow rule — dual-location project (experimental)

This project is actively worked from **two machines**: the user's laptop
and **anahita** (a lab workstation; see the `hq-topology` memory note).
To keep memory, plans, and session transcripts coherent across both:

**Run `bash scripts/sync-claude-state.sh` at session boundaries** — once
at the start of a session (pulls the other machine's edits) and once
before ending a session that wrote anything worth preserving (pushes
yours). The script is bidirectional `rsync --update`; safe to re-run;
exits non-zero (with a clear message) if the remote is unreachable
over the VPN. See the `dual-location-workflow` memory entry for the
full rule + edge cases.

If you skip this and edit memory, the next agent on the other machine
will start from a stale snapshot and re-derive things that were
already settled. Don't be that agent.

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
| `array` | `SlurmComputeSource` | submits one Slurm `sbatch --array=...` job (locally) |
| `individual` | `SlurmComputeSource` | one `sbatch` per parameter combo (locally) |
| `remote` | `SSHComputeSource` *or* `SSHSlurmComputeSource` | push-model rsync to a single SSH host — bash by default, `sbatch`/`squeue` over SSH when the remote's config has `backend: slurm` |
| `distributed` | `DistributedComputeSource` | fans across mixed children (local + any combo of `backend: ssh` and `backend: slurm` remotes) |
| `auto` | resolved at runtime | `array` if `sbatch` on PATH, else `local` |

`hsm sweep run --remote <alias>` implies `--mode remote` (you don't pass
both). `--gpus all\|cpu\|N\|i,j,k` is the per-remote GPU allowlist
(separate from `spec.gpus`, which is per-job count — see "Gotchas" below).

**Backend dispatch lives at the remote level**, NOT inside `spec:`.
`distributed.remotes.<alias>.backend` is either `ssh` (default, →
`SSHComputeSource`) or `slurm` (→ `SSHSlurmComputeSource`). Keep this
out of `ResourceSpec` — `spec:` is per-job (walltime/gpus/...);
`backend`/`workdir`/`archive_dir` are per-source.

## Architecture (one tier, post Pass B-heavy)

```
                    CLI (cli/sweep.py, cli/remote.py, ...)
                                 ↓
              SweepOrchestrator (build_compute_source → run_sweep_async)
                                 ↓
                 ┌─────── ComputeSource ABC (async) ───────┐
                 │                                         │
     LocalComputeSource    SlurmComputeSource    SSHComputeSource    SSHSlurmComputeSource    DistributedComputeSource
                                                  (bash/SSH)         (sbatch/SSH)              (fans across mixed children)
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
  bash-over-SSH push class + `build_ssh_source(...)` factory + `parse_gpus_arg(...)` helper.
- [`core/remote/ssh_slurm_compute_source.py`](src/hpc_sweep_manager/core/remote/ssh_slurm_compute_source.py) —
  sbatch-over-SSH class + `build_ssh_slurm_source(...)` factory. Composes push_exec rsync helpers + slurm_protocol directive rendering. Supports `workdir` (overrides `remote_root`) + `archive_dir` (server-side rsync to a durable target after `wait_for_all`) + `archive_on: completed|always|never` + `.archived` sentinel.
- [`core/hpc/slurm_protocol.py`](src/hpc_sweep_manager/core/hpc/slurm_protocol.py) —
  pure shared helpers: `SLURM_STATE_MAP`, `render_sbatch_directives(spec)`, `parse_sbatch_job_id(stdout)`. Used by both Slurm sources to avoid drift.
- [`core/remote/push_exec.py`](src/hpc_sweep_manager/core/remote/push_exec.py) —
  pure helpers (`DEFAULT_RSYNC_EXCLUDES`, `normalize_gpu_allowlist`,
  `partition_gpu_slots`, `resolve_run_prefix`, `build_rsync_push_cmd`,
  `build_rsync_pull_cmd`).
- [`core/distributed/distributed_compute_source.py`](src/hpc_sweep_manager/core/distributed/distributed_compute_source.py) —
  `_build_ssh_children` dispatches per-remote on `backend:` (`ssh`/`slurm`) so a single distributed run can mix both.
- [`core/common/config.py`](src/hpc_sweep_manager/core/common/config.py) —
  `HSMConfig.load()` merges `~/.hsm/config.yaml` (machine) with `<project>/.hsm/config.yaml` (project); `local:` is deep-merged field-by-field with project winning on collisions, other blocks only honored from the project file (machine-level `slurm:`/`distributed:` are dropped with a warning). `get_local_sweeps_root()` + `resolve_sweep_dir(hsm_config, sweep_id, project_dir)` — when `local.sweeps_root` is set, sweep dirs land there with a discovery symlink in the project dir; **hard-errors** if the path doesn't exist on the current machine (catches the "config copied to a machine where the mount isn't present" footgun).
- [`core/common/sweep_analysis.py`](src/hpc_sweep_manager/core/common/sweep_analysis.py) —
  `SweepCompletionAnalyzer`, `find_incomplete_sweeps`, `get_sweep_completion_summary`.
  Read-only on-disk analysis; used by `hsm sweep status` and `hsm sweep report`.
- [`core/hpc/scheduler_queue.py`](src/hpc_sweep_manager/core/hpc/scheduler_queue.py) —
  `SlurmQueue` (+ `QueueJob` / `Reservation` dataclasses). Read-only wrappers
  around `squeue` / `sprio` / `scontrol`. Backs `hsm queue mine|position|gpus|reservations`.

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

1. **Non-interactive SSH shells AND sbatch compute nodes skip `~/.bashrc`** →
   `conda` / `MAMBA_EXE` is not on PATH. All three rendered-script templates
   (`ssh_compute_source.sh.j2`, `slurm_array.sh.j2`, `slurm_single.sh.j2`)
   include the shared partial
   [`templates/_conda_init.sh.j2`](src/hpc_sweep_manager/templates/_conda_init.sh.j2)
   when `uses_conda=True`. The partial probes standard conda paths
   (`~/miniconda3`/`~/anaconda3`/`~/miniforge3`/`/opt/conda`), then falls back
   to micromamba via `$MAMBA_EXE` + common locations (including
   `~/code/packages/HPC-Sweep-Manager/bin/micromamba` for the S3IT layout
   where the binary lives inside an HSM clone), sources the right
   shell-hook, and defines `conda() { micromamba "$@"; }` so the rest of
   the script can keep emitting `conda run -n <env> python ...` uniformly.
   SSHComputeSource + SSHSlurmComputeSource pass `uses_conda` based on
   `bool(self.conda_env)`; native SlurmComputeSource uses
   `_python_needs_conda_init(python_path)` (`True` if `python_path`
   starts with `conda `/`mamba `/`micromamba `).

2. **`~` in `output.dir=~/path` does NOT tilde-expand** inside double-quoted
   COMMAND strings (bash only expands `~` in cd/mkdir tokens). `SSHComputeSource.setup()`
   probes `echo $HOME` once and substitutes if `remote_root` starts with `~`.

3. **`script_path` must be relative-to-project on the remote.** After
   `cd <remote_code_dir>` on the rsync'd mirror, an absolute LOCAL path
   won't exist. `SSHComputeSource.__init__` normalizes it.

4. **`run_sweep_async` MUST call `collect_results()` + `cleanup()`** after
   `wait_for_all()`. No-op for Local/Slurm; load-bearing for SSH (where
   `collect_results` IS the rsync-pull, and the archive step for SSH-Slurm
   piggybacks on it). Don't remove these calls.

4b. **SSH-Slurm: archive runs BEFORE the local pull.** For
   `SSHSlurmComputeSource.collect_results()`, the sequence is: (a)
   server-side rsync `workdir → archive_dir/<sweep_id>/` + write
   `.archived` sentinel (only when `archive_dir` set and `archive_on`
   allows), then (b) the local rsync pull of `tasks/`, then (c)
   `rm -rf` the remote sweep dir on full success. Server-side rsync
   uses the cluster's internal network (fast); the pull goes through
   the SSH connection (slow). Reversing the order would have us pulling
   data we're about to delete, while the durable safety net is still
   missing. Don't reorder.

4c. **Backend dispatch lives at the remote level, NOT in `ResourceSpec`.**
   `distributed.remotes.<alias>.backend` is `ssh` (default) or `slurm`.
   `workdir` / `archive_dir` / `archive_on` also live at the remote
   level. Per-job concerns (walltime, gpus, gpu_type, modules, ...) stay
   inside `spec:`. Conflating them invites a `slurm:` style bleed-bug
   (see gotcha 5b). The orchestrator dispatches in `build_compute_source`
   (`--mode remote`) and `_build_ssh_children` (`--mode distributed`).

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
   - **Flavour modules (`h100`, `l4`, `multigpu`, ...) in the rendered script:
     S3IT docs recommend against, empirically both forms work.** Earlier
     versions of this gotcha said you NEEDED `modules: [h100]` alongside
     `gpu_type` — that was wrong; `gpu_type: H100` alone is sufficient.
     S3IT docs say flavour modules belong on the command line before
     `sbatch` because they set Slurm constraints that *can* conflict with
     directives. In practice (verified 2026-05-28), having `module load
     h100` inside the script alongside `--gres=gpu:H100:1` works fine
     because both set consistent H100 constraints — no conflict.
     **Safer default: omit flavour modules from the typed `slurm:` block.**
     The `modules:` field is still the right home for non-flavour modules
     (`module load matlab`, `module load openmpi`); just be aware that
     mixing a flavour module with a contradictory `gpu_type` will hit the
     conflict path the docs warn about.
   - Also note: "Requested node configuration is not available" can be a
     *transient* error on a busy cluster (no matching node currently idle),
     not just a permanent config mismatch. Retry after a wait before
     concluding the directives are wrong.
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

9. **Two HSM configs, not one — machine + project.** `HSMConfig.load()` reads
   `~/.hsm/config.yaml` (machine-wide) AND `<project>/.hsm/config.yaml`
   (project) and merges them. Per-block rule: `local:` is deep-merged
   field-by-field (project wins on collisions); every other block
   (`slurm:`, `distributed:`, `paths:`, `project:`, `wandb:`) is only
   honored from the project file — putting them in the machine config
   warns and drops. Rationale: machine-specific facts like
   `local.sweeps_root` belong in the machine file (the path
   `/mnt/8TB_HDD` exists on the workstation but not the laptop);
   `distributed.remotes.*` is project scope (different projects
   target different clusters). The machine file is auto-created by
   `hsm setup init` if absent — commented stub by default, or active
   with `sweeps_root` set if the run was on a TTY with candidate disks
   under `/mnt`/`/data`/`/scratch`. `resolve_sweep_dir` hard-errors if
   `sweeps_root` resolves to a non-existent directory on the current
   machine (catches "config copied across machines, mount isn't
   present" — common when shared via git).

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

## Recently landed (2026-05-28) — Pieces A–D

Four small features that together unlock the "HQ workstation drives
sweeps across {local, SSH boxes, SSH-Slurm clusters}" workflow. Brief
plan + decisions: `/home/gbena/.claude/plans/we-are-making-misty-moth.md`.

| Piece | What | Where |
|---|---|---|
| A | `local.sweeps_root` — redirect sweep dirs to a different filesystem with a discovery symlink in the project | `core/common/config.py` (`get_local_sweeps_root`, `resolve_sweep_dir`); `cli/sweep.py` |
| B | `SSHSlurmComputeSource` — sbatch over SSH; reuses push_exec rsync + slurm_protocol directive rendering | `core/remote/ssh_slurm_compute_source.py`, `core/hpc/slurm_protocol.py` (new shared module) |
| C | `workdir` / `archive_dir` / `archive_on` on SSH-Slurm — `/scratch → /shares` server-side rsync with `.archived` sentinel | same file as B (layered on collect_results) |
| D | Mixed-backend distributed — `_build_ssh_children` dispatches on `backend:` so `--mode distributed` can fan across `ssh` + `slurm` children in one sweep | `core/distributed/distributed_compute_source.py` |

Smoke driver for the new SSH-Slurm path:
[`examples/smoke_ssh_slurm_cli.sh`](examples/smoke_ssh_slurm_cli.sh).
User-facing docs: [docs/user_guide/MULTI_CLUSTER.md](docs/user_guide/MULTI_CLUSTER.md)
is the canonical place; [SSH_EXECUTION.md](docs/user_guide/SSH_EXECUTION.md#driving-slurm-over-ssh-backend-slurm)
has the per-feature reference.

## Cross-references

- **Users:** [README.md](README.md) (quickstart),
  [docs/user_guide/getting_started.md](docs/user_guide/getting_started.md),
  [docs/user_guide/SSH_EXECUTION.md](docs/user_guide/SSH_EXECUTION.md),
  [docs/user_guide/HPC_EXECUTION.md](docs/user_guide/HPC_EXECUTION.md),
  [docs/user_guide/MULTI_CLUSTER.md](docs/user_guide/MULTI_CLUSTER.md).
- **API:** [docs/api_reference/](docs/api_reference/) — the
  `compute_sources.md` doc is the live one; the others are legacy with banners.
- **Design rationale:** [ARCHITECTURE.md](ARCHITECTURE.md).
- **Project layout:** [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md).
- **CLI reference:** [docs/cli/README.md](docs/cli/README.md).
