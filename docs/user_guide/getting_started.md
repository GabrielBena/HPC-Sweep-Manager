# Getting started with HPC Sweep Manager

This guide walks through installing HSM, initializing it in your ML
project, and running a sweep in each of the four execution modes.

For deeper recipes:
- [SSH (push-model) execution](SSH_EXECUTION.md) — the `--remote <alias>` path in depth, including `backend: slurm` to drive a remote Slurm cluster (e.g., S3IT) over SSH.
- [HPC (Slurm / PBS) execution](HPC_EXECUTION.md) — the `--mode array|individual` path + the typed `slurm:` block for advanced Slurm features.
- [MULTI_CLUSTER.md](MULTI_CLUSTER.md) — fanning a single sweep across {local GPUs, SSH-Slurm cluster, SSH workstation} from one HQ box.

If you're an AI agent landing in this repo, start with
[../../CLAUDE.md](../../CLAUDE.md) instead.

## Installation

```bash
# Editable install with dev deps (pytest + ruff + mypy):
git clone <this-repo> && cd HPC-Sweep-Manager
pip install -e ".[dev]"
```

Requirements:
- Python 3.11+.
- For HPC mode: a Slurm or PBS cluster with `sbatch` / `qsub` on PATH.
- For SSH `--remote` mode: a working `~/.ssh/config` alias to your
  remote box. Nothing needs installing on the remote.

## Pick a mode

| Mode | What it does | When to use |
|---|---|---|
| `local` | Run on this machine; parallel processes with optional GPU pinning. | Dev / quick experiments / no cluster nearby. |
| `array` | Submit one Slurm `sbatch --array=1-N` job. | HPC, many tasks, scheduler-friendly. |
| `individual` | Submit one `sbatch` per parameter combination. | HPC when you want per-task scheduling control. |
| `remote` | Push-model SSH: rsync project to a single remote box, run there. | Lab box / shared GPU server / "scp it over and run it" use cases. |
| `distributed` | Fan across N SSH boxes + optional local, via `.hsm/config.yaml`. | Multi-lab-host workloads, ad-hoc compute pool. |
| `auto` | Detected at runtime — `array` if `sbatch` exists, else `local`. | When you don't want to think about it. |

## Step 0 — initialize HSM in your project

In the directory containing your `train.py` (and Hydra `configs/`, if
any):

```bash
hsm setup init
```

This creates:

```
.hsm/
└── config.yaml          # HSM project config (paths, defaults, optional slurm:/distributed: blocks)
sweeps/
├── example_sweep.yaml   # Starter sweep config
├── outputs/             # Per-sweep result dirs land here
├── logs/
└── README.md
```

Edit `.hsm/config.yaml` to point at your interpreter + training script.
HSM auto-detects most of this on first run.

## Step 1 — define your sweep

Edit `sweeps/example_sweep.yaml` (or write a fresh `sweeps/sweep.yaml`):

```yaml
script: train.py             # path (relative to project root)
sweep:
  grid:                      # Cartesian product
    seed: [1, 2, 3]
    lr: [0.001, 0.01, 0.1]
  paired:                    # zipped within group, then Cartesian with grid
    - schedule_pair:
        optimizer: ["adam", "sgd"]
        momentum: [0.9, 0.95]
defaults:                    # applied to every combination
  batch_size: 32
metadata:
  description: "smoke test for new arch"
```

Quick stats without running anything:

```bash
hsm sweep run --config sweeps/example_sweep.yaml --count-only
hsm sweep run --config sweeps/example_sweep.yaml --dry-run
```

## Step 2 — your training script's contract

HSM passes parameters as Hydra-style `key=value` tokens, plus two
HSM-injected args:

- `wandb.group=<sweep_id>` — pass to wandb if you use it.
- `output.dir=<path>` — your script must `mkdir -p` it and write any
  outputs there. HSM rsyncs this dir back for `--remote` mode.

Minimum viable Python:

```python
import os, sys
from pathlib import Path

params = {}
for tok in sys.argv[1:]:
    if "=" in tok:
        k, v = tok.split("=", 1)
        params[k] = v

output_dir = Path(params.get("output.dir", "."))
output_dir.mkdir(parents=True, exist_ok=True)

# ... your training loop here, writing checkpoints/metrics to output_dir ...
```

See [`examples/test_train.py`](../../examples/test_train.py) for a more
complete reference (sentinel files, progress prints, optional failure
mode for testing).

## Step 3 — run

### Local mode (default fallback)

```bash
hsm sweep run --mode local --parallel-jobs 4
# Or, with GPU pinning if you have N GPUs and want one per task:
hsm sweep run --mode local --parallel-jobs 4 --resources "--gpus=1"
```

`--show-output` streams stdout/stderr live (handy for small runs).

On a box where the system disk is tight but you have a larger secondary
mount, redirect sweep outputs there via the `local:` block:

```yaml
# .hsm/config.yaml
local:
  sweeps_root: "/mnt/8TB_HDD/$USER/hsm-sweeps"
```

Data lives at `<sweeps_root>/<sweep_id>/`; a symlink at
`<project>/sweeps/outputs/<sweep_id>` keeps `hsm sweep status` /
`hsm sweep report` working transparently. See
[HPC_EXECUTION.md#localsweeps_root](HPC_EXECUTION.md#localsweeps_root--put-sweep-dirs-on-a-different-filesystem).

### HPC array mode

```bash
hsm sweep run --mode array \
    --walltime 01:00:00 \
    --resources "--cpus-per-task=4 --mem=16gb --qos=normal"
```

This submits one `sbatch --array=1-N` job. For per-task `sbatch` calls,
use `--mode individual`. For advanced Slurm fields the opaque
`--resources` string can't reach (`gpu_type`, `modules`, `pre_script`,
`account`, `extra_directives`), add a typed `slurm:` block to
`.hsm/config.yaml` — see
[HPC_EXECUTION.md](HPC_EXECUTION.md#the-typed-slurm-block--reach-fields---resources-cant).

### SSH push-model (single remote)

```bash
# One-time: register the remote (uses ~/.ssh/config for the alias).
hsm remote add my-box
hsm remote test my-box
hsm remote gpus my-box      # sanity check: what GPUs are visible?

# Real submission.
hsm sweep run --remote my-box --gpus 1 --resources "--gpus=1"
```

Replace `my-box` with your `~/.ssh/config` alias. HSM rsyncs your
project up, runs the sweep with GPU pinning, rsyncs results back, and
cleans up the remote on success. Full guide:
[SSH_EXECUTION.md](SSH_EXECUTION.md).

### Multi-host distributed mode

Add a `distributed:` block to `.hsm/config.yaml`:

```yaml
distributed:
  enabled: true
  local_max_jobs: 4
  remote_root: ~/.hsm/runs
  conda_env: my-env
  remotes:
    box-1:                       # default backend: ssh
      max_parallel_jobs: 4
      gpus: [0, 1, 2, 3]
    box-2:                       # default backend: ssh
      max_parallel_jobs: 2
      conda_env: my-env-cpu
    cluster:                     # SSH-driven Slurm — sbatch jobs over SSH
      backend: slurm
      host: cluster-login
      conda_env: my-env
      workdir: "/scratch/$USER/hsm-runs"
      archive_dir: "/shares/<group>/hsm-archive"
      spec:
        walltime: "06:00:00"
        cpus_per_task: 4
        mem: "32G"
        gpus: 1
        gpu_type: H100
```

Then:

```bash
hsm sweep run --mode distributed
```

HSM fans the parameter combinations across all sources (local + every
enabled remote — including mixed SSH-bash and SSH-Slurm backends in one
sweep) using a round-robin or least-loaded strategy. Schema details in
[SSH_EXECUTION.md](SSH_EXECUTION.md#hsmconfigyaml--the-distributed-block).
For the full HQ-on-workstation pattern (laptop → workstation → S3IT)
see [MULTI_CLUSTER.md](MULTI_CLUSTER.md).

## Step 4 — inspect

Each sweep lands at `sweeps/outputs/<sweep_id>/`:

```
sweeps/outputs/sweep_20260601_143022/
├── sweep_config.yaml            # snapshot of what you ran
├── submission_summary.txt       # job IDs + final statuses
├── tasks/
│   ├── task_001/
│   │   ├── task_info.txt        # node, params, started/finished, status
│   │   ├── command.txt          # exact command that ran
│   │   └── <your training outputs here>
│   ├── task_002/
│   └── ...
├── logs/
└── scripts/
```

Quick status:

```bash
hsm sweep status                          # all sweeps
hsm sweep status <sweep_id>               # one sweep, detail
hsm sweep report <sweep_id>               # completion report
hsm sweep errors <sweep_id>               # error summaries for failed tasks
```

## Step 5 — partial sweeps + retries

`hsm sweep complete` (the v0.1 re-runner) was deleted in Pass B-heavy
alongside the legacy job manager hierarchy — the 2000-LOC bloat made
clean rebuild the better path. A small replacement built on top of
`ComputeSource` is planned. For now:

```bash
hsm sweep status <sweep_id>     # see which tasks failed or are missing
hsm sweep report <sweep_id>     # detailed per-task breakdown
hsm sweep errors <sweep_id>     # tail the error logs
```

To re-run failed tasks: write a smaller `sweep.yaml` containing only the
parameter combinations you want to retry (or filter via `--max-runs`)
and submit it as a fresh sweep with `hsm sweep run`.

## Common environment + tooling notes

- HSM's CLI is the only sync boundary; everything below it is async.
  If you're using HSM from Python, see
  [`../api_reference/compute_sources.md`](../api_reference/compute_sources.md)
  for the `run_sweep_async()` + `ComputeSource` API.
- `~/.ssh/config` is the single source of truth for SSH host resolution.
  Aliases, `ProxyJump`, port, identity files — all honored.
- `hsm remote gpus <alias>` is a quick way to see what's free on a box
  before submitting a sweep there.
- `hsm remote clean <alias>` removes HSM's scratch on a remote.

## Where to next

- [SSH_EXECUTION.md](SSH_EXECUTION.md) — the push-model recipe + the
  `distributed:` config schema.
- [HPC_EXECUTION.md](HPC_EXECUTION.md) — Slurm / PBS + the `--resources`
  gap workaround.
- [../api_reference/compute_sources.md](../api_reference/compute_sources.md) — Python API.
- [../../ARCHITECTURE.md](../../ARCHITECTURE.md) — design rationale.
