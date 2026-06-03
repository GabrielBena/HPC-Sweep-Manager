# SSH (push-model) remote execution

Run a sweep on a remote Linux box over SSH. HSM rsyncs your project up,
runs the tasks there with GPU pinning, rsyncs the results back, and
auto-cleans the per-sweep dir on success. Nothing needs to be installed
on the remote besides `bash`, `rsync`, and optionally `nvidia-smi`.

Two backends share this push lifecycle:

- **`backend: ssh`** (default) — run wrapped `bash` directly on the
  remote. For workstation-style boxes with a few GPUs.
- **`backend: slurm`** — push code, then `sbatch` jobs over SSH. For
  driving a real HPC scheduler (e.g., S3IT) from off-cluster. See
  [Driving Slurm over SSH](#driving-slurm-over-ssh-backend-slurm)
  below.

This guide covers the single-host case (`--remote <alias>`). For the
multi-host fan-out (`--mode distributed`) see
[MULTI_CLUSTER.md](MULTI_CLUSTER.md).

## Prerequisites

- HSM installed locally: `pip install -e ".[dev]"` from this repo (or
  `pip install hpc-sweep-manager` once a release is published).
- A working SSH alias for the remote in `~/.ssh/config`. Test it once
  interactively: `ssh my-box`. HSM uses the same alias resolution
  (HostName / User / Port / IdentityFile / ProxyJump all honored).
- Optionally, `nvidia-smi` on the remote if you want GPU detection +
  pinning. CPU-only boxes work fine; HSM falls back to CPU slots.

## 5-minute recipe

In your project directory (the one containing your `train.py`):

```bash
# 1. Initialize HSM scaffolding (one-time per project).
hsm setup init

# 2. (Optional) Register the remote in .hsm/config.yaml. You can skip
#    this and just use a bare ~/.ssh/config alias instead — see below.
hsm remote add my-box

# 3. Confirm you can reach it + see its GPUs.
hsm remote test my-box
hsm remote gpus my-box

# 4. Edit sweeps/example_sweep.yaml to your liking, then submit.
hsm sweep run --remote my-box --gpus 1 --resources "--gpus=1"

# 5. Inspect outputs (rsync'd back automatically).
ls sweeps/outputs/<sweep-id>/tasks/
```

Replace `my-box` with whatever your alias is. HSM will:

1. rsync your project to `~/.hsm/runs/<project-name>/code/` on the remote
   (rolling mirror — diffs only, excludes `.git`, `__pycache__`, etc.).
2. Probe `nvidia-smi` to discover GPUs.
3. Partition them into per-task slots (1 task per GPU here, since
   `--gpus 1` is the allowlist and `--resources --gpus=1` is per-task).
4. Submit your sweep tasks via SSH, one per slot (with back-pressure if
   you have more tasks than slots).
5. rsync `tasks/` back to your local `sweeps/outputs/<sweep-id>/`.
6. `rm -rf` the remote per-sweep dir on full success (code cache stays).
   On any FAILED task, the remote dir is kept for debugging.

## The `--gpus` flag — allowlist vs per-task count

Two **different** concepts, both required for real GPU sweeps:

- **`--gpus <spec>`** (CLI flag): which GPUs on the remote are *visible*
  to the sweep. Allowlist.
  - `all` (default): use every GPU `nvidia-smi` reports.
  - `cpu` or `0`: CPU-only, ignore GPUs even if present.
  - `N` (single int): take the first `N` detected GPUs.
  - `i,j,k` (comma-list): explicit GPU indices.
- **`spec.gpus = N`** (in `--resources "--gpus=N"`): how many GPUs each
  task gets. Set this if you want CUDA_VISIBLE_DEVICES exported per
  task. Without it the slot queue falls back to CPU slots.

Example combos:

| `--gpus` | `--resources "--gpus=N"` | Result |
|---|---|---|
| `all` (default) | not set | CPU slots (no GPU isolation) |
| `1` | `--gpus=1` | 1 slot, GPU `[0]` only |
| `0,1` | `--gpus=1` | 2 slots, `[0]` and `[1]` |
| `0,1,2,3` | `--gpus=2` | 2 slots, `[0,1]` and `[2,3]` |
| `cpu` | (anything) | CPU slots, ignore GPUs |

## Conda env vs explicit Python path

The wrapper script's interpreter is resolved in this order:

1. `conda_env` (from `--conda-env` CLI flag / `distributed.conda_env` in
   `.hsm/config.yaml`) → renders `conda run -n <env> python`.
   The script also sources `conda.sh` from the standard locations
   (`~/miniconda3`, `~/anaconda3`, `~/.miniconda3`, `/opt/conda`)
   before invoking, since non-interactive SSH shells skip `~/.bashrc`.
2. `python_path` (per-remote `python_path` in config) → renders that
   absolute path.
3. Bare `python` on the remote PATH (whatever the non-interactive shell
   finds).

Quick reachability check before your first sweep:

```bash
ssh my-box "conda run -n my-env python -c 'import sys; print(sys.executable)'"
```

Should print a python path. If you see `command not found: conda`, the
template's auto-source will catch the most common installs, but if your
conda lives somewhere exotic you may need to set `python_path` instead.

## Your project's own package on the remote

If your training script does `import yourpkg` and `yourpkg` is an
*editable* install on your local machine (`pip install -e .`), the
rsynced tree alone will **not** make that import work on the remote:
`python scripts/train.py` puts `scripts/` on `sys.path`, not the project
root, and the remote conda env has never seen your package. The failure
is silent at submit time — every task dies mid-sweep on the compute node
with `ModuleNotFoundError: No module named 'yourpkg'`.

Two fixes; pick one:

1. **Install the package into the remote env** (durable — survives HSM
   re-pushes; do it once per env):

   ```bash
   ssh my-box "cd ~/.hsm/runs/<project-name>/code && conda run -n my-env pip install -e ."
   ```

2. **Point `PYTHONPATH` at the pushed code dir** (zero-install — lives in
   config, applies to every task):

   ```yaml
   distributed:
     remotes:
       my-box:
         pre_script:
           - export PYTHONPATH=$HOME/.hsm/runs/<project-name>/code:$PYTHONPATH
   ```

Option 2 always imports exactly the code that was just pushed (no stale
installed copy), which is usually what you want for active development.
Note option 1's editable install points at the *rolling* code dir — HSM
re-pushes into the same path, so the install stays current too; it only
goes stale if you change the project name or `remote_root`.

## `.hsm/config.yaml` — the `distributed:` block

You can submit a sweep with **just** a bare `~/.ssh/config` alias and no
HSM-side registration (`hsm sweep run --remote my-box` works as long as
`ssh my-box` works). Registering the remote in `.hsm/config.yaml` lets
you set per-remote knobs:

```yaml
distributed:
  enabled: true
  remote_root: ~/.hsm/runs          # global default
  conda_env: my-env                 # global default
  rsync_excludes:                   # adds to DEFAULT_RSYNC_EXCLUDES
    - data/raw/
    - outputs/                      # see note below
    - "*.pt"
  remotes:
    my-box:
      max_parallel_jobs: 4          # cap concurrent tasks on this box
      gpus: [0, 1, 2, 3]            # default allowlist (CLI overrides)
      conda_env: my-env-cpu         # override the global env
      # All optional below — fall back to ~/.ssh/config defaults:
      # host: actual-hostname
      # ssh_key: ~/.ssh/id_special
      # ssh_port: 2222
      # python_path: /opt/python/bin/python3
    other-box:
      max_parallel_jobs: 2
      conda_env: my-env-cpu
```

CLI flags (`--gpus`, `--conda-env`) override per-remote config; per-remote
config overrides global `distributed.*`; global overrides defaults.

`DEFAULT_RSYNC_EXCLUDES` already skips the usual ML artifacts from the code
push — `.git`, `__pycache__`, `*.pyc`/`*.pt`/`*.pth`/`*.ckpt`,
`/checkpoints`, `/multirun`, `.hydra`, `/wandb`. The output-dir names are
**anchored to the project root** (the leading `/`): an unanchored `wandb`
would also match a `configs/wandb/` Hydra config *group* and silently strip
it from the push — every task then fails with `MissingConfigException`.
Nested same-name dirs (`sub/wandb/`) therefore *do* ship; the weight globs
still catch the heavy files, and you can add an unanchored `wandb` to your
per-remote `rsync_excludes` if you want the broader match back. Your
`rsync_excludes` is **layered on top** of the defaults (it extends, doesn't
replace — so you keep `.git` etc. for free). A bare `outputs/` and `*.pkl`
are **not** excluded by default (an `outputs/` source dir is easy to clobber,
and `.pkl` is as often input data as output); add them to `rsync_excludes` as
shown above if they're artifacts in your project. There's no way yet to
*un-exclude* a default, so if you commit a pretrained `/checkpoints` as a
training INPUT, rename it.

## Driving Slurm over SSH (`backend: slurm`)

For driving a real HPC scheduler from off-cluster (e.g., kicking off
S3IT jobs from your lab workstation), declare a remote with
`backend: slurm` instead of the default `ssh`. HSM still rsyncs your
project up the same way; the difference is that submissions go through
`sbatch` on the remote (over the same persistent SSH connection) and
status polls hit `squeue`. The login node needs `sbatch`/`squeue`/
`scancel` on `$PATH`.

```yaml
distributed:
  enabled: true
  remotes:
    uzh:                                          # ~/.ssh/config alias
      backend: slurm                              # → SSHSlurmComputeSource
      host: uzh
      conda_env: cpvr
      workdir: "/scratch/$USER/hsm-runs"         # active sweep lives here
      archive_dir: "/shares/payvand.ini.uzh/hsm-archive"  # durable copy
      archive_on: completed                       # completed | always | never
      qos_whitelist: [normal, medium]
      spec:
        walltime: "06:00:00"
        cpus_per_task: 4
        mem: "32G"
        gpus: 1
        gpu_type: H100                            # case-sensitive on S3IT
```

Then:

```bash
hsm sweep run --remote uzh -c sweeps/sweep.yaml              # one sbatch per combo
hsm sweep run --remote uzh -c sweeps/sweep.yaml --mode array # one sbatch --array
```

`--remote` implies remote execution; the optional `--mode array|individual`
picks the **submission style**. Default is `individual` (one `sbatch` per
parameter combo). `--mode array` packs the whole sweep into a single
`sbatch --array` — fewer scheduler entries, faster to queue. (`--mode array`
is ignored for `backend: ssh` bash remotes, which have no scheduler.)

The CLI flags `--walltime` / `--resources` still override `spec:` per
run, same as the local-Slurm path. Add `--dry-run` to preview the merged
per-remote `spec:` (the resulting `#SBATCH --gres=…`, account, qos) before
submitting.

### `workdir` vs `remote_root` — what changes

`SSHComputeSource` (default `backend: ssh`) uses a single
`remote_root` (default `~/.hsm/runs`) as both the code mirror's home
*and* where sweep dirs land. That's fine on workstation-style boxes
with one user-writable filesystem.

Clusters typically split storage tiers. On S3IT:

- `$HOME` (400 GB) — permanent, but small.
- `/scratch/$USER` (20 TB) — ephemeral; **files unread for 30 days
  are deleted**. Use this for in-flight runs.
- `/shares/<group>/` — durable group storage.

`backend: slurm` adds two optional fields to express that split:

| Field | What it does | Default |
|---|---|---|
| `workdir` | Overrides `remote_root` for THIS run's sweep dir. Set to `/scratch/$USER/...` for the ephemeral tier. | `remote_root` |
| `archive_dir` | On completion, runs a server-side `rsync` from `workdir → archive_dir/<sweep_id>/`, then writes a `.archived` sentinel. The archive runs *before* the local `tasks/` pull and uses the cluster's internal network, so it's fast. | unset (no archive) |
| `archive_on` | `completed` (default — only on full success), `always` (incl. partial failures, useful for forensics), `never` (explicit opt-out). | `completed` |

Sweeps with any FAILED tasks stay on `workdir` for inspection regardless
of the archive setting. The local `tasks/` pull still happens.

### S3IT-specific recipe

Replace the alias, conda env, group share path. The S3IT login node lives
at `cluster.s3it.uzh.ch`:

```yaml
distributed:
  enabled: true
  remotes:
    uzh:
      backend: slurm
      host: uzh
      conda_env: cpvr
      workdir: "/scratch/$USER/hsm-runs"
      archive_dir: "/shares/payvand.ini.uzh/hsm-archive"
      qos_whitelist: [normal, medium, long]
      spec:
        walltime: "06:00:00"
        cpus_per_task: 4
        mem: "32G"
        gpus: 1
        gpu_type: H100              # uppercase; check `sinfo -o "%P %G"`
        pre_script:
          - module load miniforge3 # canonical S3IT conda recipe (see below)
```

GRES names are case-sensitive on S3IT (`H100`/`L4`/`A100`/`H200`). See
[HPC_EXECUTION.md](HPC_EXECUTION.md#the-typed-slurm-block--reach-fields---resources-cant)
for the wider Slurm field reference.

**`$USER` / `~` / `$HOME` in `workdir` / `archive_dir` expand** — HSM resolves
them once on the remote at connect time, so `/scratch/$USER/hsm-runs` becomes
`/scratch/<you>/hsm-runs` (not a literal `$USER` directory).

**Conda from a module (the S3IT way).** S3IT provides conda via
`module load miniforge3` (prefix under `/apps/...`), not a fixed `~/miniconda3`.
Put it in `pre_script:` as above — HSM renders `module load` *before* its
conda-init probe and defers to the module's conda when it's on PATH, so
`conda run -n <env>` resolves the right env and your GPU job stays on GPU. (No
`ln -sfn … ~/miniforge3` symlink needed — that old workaround is obsolete.)

### Smoke test

[`examples/smoke_ssh_slurm_cli.sh`](../../examples/smoke_ssh_slurm_cli.sh)
bootstraps a throwaway project, submits a 2-task array via SSH-Slurm,
and verifies the round-trip + archive sentinel:

```bash
REMOTE=uzh CONDA_ENV=cpvr \
    WORKDIR=/scratch/$USER/hsm-runs \
    ARCHIVE_DIR=/shares/payvand.ini.uzh/hsm-archive \
    bash examples/smoke_ssh_slurm_cli.sh
```

### Troubleshooting `backend: slurm`

- **`sbatch not found` at setup:** the non-interactive SSH shell skips
  `~/.bashrc`. Either install `sbatch` system-wide on the login node
  (true for S3IT), or load it via `pre_script:` inside the `spec:` block.
- **`Requested node configuration is not available`:** can be a
  *transient* sbatch error on a busy cluster, not just a permanent
  config mismatch. Retry after a wait before assuming the directives
  are wrong.
- **Empty `archive_dir`:** the `.archived` sentinel only lands when
  `archive_dir` is set AND `archive_on` allows. If you want forensics
  on partial-failure runs, use `archive_on: always`.
- **Job trains on CPU though a GPU was allocated:** almost always the conda
  env wasn't the one you think (a micromamba bridge shadowed a module conda).
  Use the `pre_script: [module load miniforge3]` recipe above; the rendered
  script now defers to a module-provided conda. Verify with a one-off
  `srun … python -c "import jax; print(jax.devices())"`.
- **A failed sweep prints `FAILED`, not `COMPLETED`.** Once a job leaves the
  queue HSM asks `sacct` for the real terminal state, and `hsm sweep run` exits
  non-zero if anything failed — so check the exit code in scripts. Failing task
  dirs + the logs dir are printed; `hsm sweep report <id> --scan-tasks` and
  `hsm sweep errors <id>` give detail.

### Recovering a sweep whose launcher died — `hsm sweep collect`

A long sweep where the `hsm sweep run` process exits before every task finishes
(overnight runs, a task stuck behind a **maintenance reservation**, a dropped
SSH session) no longer strands results:

- As each task reaches a terminal state mid-flight, HSM pulls its `tasks/<t>/`
  dir back immediately — a single stuck task can't hold the others hostage.
- At submit, HSM warns if the cluster has a Slurm reservation whose window
  could outlast this launcher.
- Submit writes a `.hsm_manifest.json` (locally + on the remote). Re-attach any
  time with:

  ```bash
  hsm sweep collect <sweep_id>
  ```

  It classifies each job via `sacct`, pulls everything terminal, and runs the
  `/scratch → /shares` archive once all tasks are done. Idempotent — re-run as
  more tasks finish. No dependence on the original process.

Every task dir also carries a `params.yaml` with that task's exact overrides,
so a synced checkpoint is self-describing (pair it with the project code to
rebuild the model) even from a partial pull.

## Housekeeping

The rolling code cache at `~/.hsm/runs/<project>/code/` is reused across
sweeps. Per-sweep dirs are auto-cleaned on success, kept on failure.

To wipe everything HSM left on a remote:

```bash
hsm remote clean my-box                  # wipe ~/.hsm/runs/<this-project>/
hsm remote clean my-box --all-projects   # wipe ~/.hsm/runs/ (every project)
```

To list / probe / health-check your remotes:

```bash
hsm remote list
hsm remote gpus my-box           # nvidia-smi summary + which GPUs are free
hsm remote health my-box         # connection + uptime + disk + python check
```

## Runnable example

[`examples/smoke_ssh_cli.sh`](../../examples/smoke_ssh_cli.sh) is a
self-bootstrapping smoke driver that runs the full lifecycle against a
real remote. Set `REMOTE=my-box` (required) and optionally
`CONDA_ENV=my-env`, then run it:

```bash
REMOTE=my-box CONDA_ENV=my-env bash examples/smoke_ssh_cli.sh --dry-only
REMOTE=my-box CONDA_ENV=my-env bash examples/smoke_ssh_cli.sh
```

It writes a throwaway project to `/tmp/hsm-ssh-smoke/`, submits a 2-task
sweep, and verifies sentinel files round-trip + the remote per-sweep dir
was auto-cleaned.

For the SSH-Slurm variant, see
[`examples/smoke_ssh_slurm_cli.sh`](../../examples/smoke_ssh_slurm_cli.sh).

## Troubleshooting

- **`ssh my-box` works but HSM hangs:** check `~/.ssh/known_hosts` —
  HSM honors strict host-key checking. Connect once interactively to
  record the host key.
- **`conda: command not found` in task output:** your conda install is
  in a non-standard location. Either set `python_path:
  /full/path/to/python` per-remote, or symlink your `conda.sh` into one
  of the standard spots.
- **Tasks succeed but `output.dir` is empty locally:** check that your
  `train.py` actually honors the `output.dir` Hydra arg HSM passes in.
  See [`examples/test_train.py`](../../examples/test_train.py) for the
  canonical contract.
- **`CUDA_VISIBLE_DEVICES=<unset>` in sentinel:** you ran with `--gpus`
  but no `--resources "--gpus=N"`, so the slot queue picked CPU slots.
  Pass both.

## See also

- [MULTI_CLUSTER.md](MULTI_CLUSTER.md) — the HQ-plus-burst workflow that
  combines `backend: ssh` and `backend: slurm` remotes under
  `--mode distributed`.
- [getting_started.md](getting_started.md) — broader quickstart covering
  local / array / individual modes too.
- [HPC_EXECUTION.md](HPC_EXECUTION.md) — Slurm / `--mode array` recipe
  (for when you're on the cluster directly, not driving it over SSH).
- [../api_reference/compute_sources.md](../api_reference/compute_sources.md) —
  Python API surface (`SSHComputeSource`, `SSHSlurmComputeSource`,
  `build_ssh_source`, `build_ssh_slurm_source`, `parse_gpus_arg`).
- [../../ARCHITECTURE.md](../../ARCHITECTURE.md#push-ssh-lifecycle) —
  full lifecycle internals.
