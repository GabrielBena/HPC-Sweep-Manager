# SSH (push-model) remote execution

Run a sweep on a remote Linux box over SSH. HSM rsyncs your project up,
runs the tasks there with GPU pinning, rsyncs the results back, and
auto-cleans the per-sweep dir on success. Nothing needs to be installed
on the remote besides `bash`, `rsync`, and optionally `nvidia-smi`.

This guide covers the single-host case (`--remote <alias>`). For the
multi-host fan-out (`--mode distributed`) see
[getting_started.md](getting_started.md#multi-host-distributed-mode).

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

- [getting_started.md](getting_started.md) — broader quickstart covering
  local / array / individual modes too.
- [HPC_EXECUTION.md](HPC_EXECUTION.md) — Slurm / `--mode array` recipe.
- [../api_reference/compute_sources.md](../api_reference/compute_sources.md) —
  Python API surface (`SSHComputeSource`, `build_ssh_source`, `parse_gpus_arg`).
- [../../ARCHITECTURE.md](../../ARCHITECTURE.md#push-ssh-lifecycle) —
  full lifecycle internals.
