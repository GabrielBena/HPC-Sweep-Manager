# HPC Sweep Manager (HSM)

A Python package + CLI for running hyperparameter sweeps over Hydra-style
training scripts. Targets local machines, Slurm/PBS HPC clusters, and
remote Linux boxes over SSH — same CLI, same sweep config, four
execution modes.

> **Status:** alpha. Active refactor; semantics stable for the modes
> documented below. See [ARCHITECTURE.md](ARCHITECTURE.md) for what's
> live vs legacy and [CLAUDE.md](CLAUDE.md) if you're an AI agent
> landing here cold.

## Why HSM

- One CLI runs sweeps locally, on a Slurm/PBS cluster, or on remote SSH
  boxes — without changing your training script.
- **Push-model SSH:** rsync your project to a remote, run with per-task
  GPU pinning, rsync results back. No HSM install needed on the remote.
- **Typed `ResourceSpec`** for HPC resources — no more opaque PBS strings
  in Python; templates handle the per-scheduler translation.
- **Unified async `ComputeSource` interface** — same lifecycle (setup →
  submit → wait → collect → cleanup) for every backend.

## Quick start

```bash
# 1. Install (editable, with dev deps).
git clone <this-repo> && cd HPC-Sweep-Manager
pip install -e ".[dev]"

# 2. Initialize HSM in YOUR ML project.
cd /path/to/your/project
hsm setup init                # creates .hsm/config.yaml + sweeps/example_sweep.yaml

# 3. Edit sweeps/example_sweep.yaml — a simple grid:
#    sweep:
#      grid:
#        seed: [1, 2, 3]
#        lr: [0.001, 0.01]

# 4. Pick a backend.
hsm sweep run --mode local                       # parallel local processes
hsm sweep run --mode array                       # one Slurm sbatch --array=
hsm sweep run --remote my-box --gpus 1 \
    --resources "--gpus=1"                        # push to a single SSH box
hsm sweep run --mode distributed                 # fan across many SSH hosts
```

Your `train.py` just needs to accept Hydra-style `key=value` args and
write its outputs to `output.dir` (HSM passes it in). See
[`examples/test_train.py`](examples/test_train.py) for the canonical
contract.

## Execution modes

| `--mode` | Backend | What it does |
|---|---|---|
| `local` | `LocalComputeSource` | Run on this machine. Per-process slot queue; optional GPU pinning via `--resources "--gpus=N"`. |
| `array` | `SlurmComputeSource` | One `sbatch --array=1-N` job to the cluster. |
| `individual` | `SlurmComputeSource` | One `sbatch` per parameter combination. |
| `remote` | `SSHComputeSource` (push-model) | rsync project to a single SSH alias, run there, rsync back. Auto-cleanup on success. |
| `distributed` | `DistributedComputeSource` | Fan across N SSH boxes + optional local (driven by `.hsm/config.yaml`'s `distributed:` block). |
| `auto` | resolved at runtime | `array` if `sbatch` on PATH, else `local`. |

`hsm sweep run --remote <alias>` is a shorthand that implies `--mode remote`.

## Architecture in 30 seconds

```
                 cli/sweep.py
                     │
                     ▼
             SweepOrchestrator                ← core/common/sweep_orchestrator.py
                     │
                     ▼  build_compute_source(mode, ...)
                     │
        ┌────────────┼─────────────┬───────────────┐
        ▼            ▼             ▼               ▼
  LocalCompute  SlurmCompute   SSHCompute    DistributedCompute
    Source        Source        Source           Source
```

All `ComputeSource` implementations share one async interface (`setup`,
`submit_job`, `wait_for_all`, `collect_results`, `cleanup`). The CLI
is the only sync boundary. See [ARCHITECTURE.md](ARCHITECTURE.md) for
the full lifecycle.

## CLI surface

```
hsm setup    init configure         # project bootstrap
hsm sweep    run | status | report | errors | watch | recent | queue | cancel | cleanup
hsm remote   add | list | test | health | gpus | clean | remove
hsm queue    mine | position | gpus | reservations   # cluster queue inspection (Slurm)
hsm analyze  enable-tracking | report | dead-code | complexity | dependencies | coverage-gaps
```

Full reference: [docs/cli/README.md](docs/cli/README.md).

> **Note:** `hsm sweep complete` (resuming partial sweeps) is not in this
> build. The bloated v0.1 completion code was deleted in Pass B-heavy
> alongside the legacy job manager hierarchy. `hsm sweep status` and
> `hsm sweep report` still tell you which tasks failed; manual
> re-submission with a filtered sweep config is the current path. A
> clean rebuild is planned.

## Docs

- **Quickstart (this README)** — install + first sweep.
- [docs/user_guide/getting_started.md](docs/user_guide/getting_started.md) — broader tutorial with per-mode walkthroughs.
- [docs/user_guide/SSH_EXECUTION.md](docs/user_guide/SSH_EXECUTION.md) — the push-model SSH recipe in depth.
- [docs/user_guide/HPC_EXECUTION.md](docs/user_guide/HPC_EXECUTION.md) — Slurm / PBS recipe + the `--resources` gap workaround.
- [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md) — `.hsm/` layout + paths HSM expects.
- [docs/api_reference/](docs/api_reference/) — Python API for embedding HSM in your own scripts.
- [ARCHITECTURE.md](ARCHITECTURE.md) — design rationale + known limitations.
- [CLAUDE.md](CLAUDE.md) — agent on-boarding (read this if you're Claude / Cursor / etc.).

## Runnable examples

- [`examples/test_train.py`](examples/test_train.py) — canonical training-script contract.
- [`examples/test_sweep.yaml`](examples/test_sweep.yaml) — minimal sweep config.
- [`examples/smoke_cli.sh`](examples/smoke_cli.sh) — end-to-end smoke for `--mode array` on real Slurm.
- [`examples/smoke_ssh_cli.sh`](examples/smoke_ssh_cli.sh) — end-to-end smoke for `--remote` on a real SSH box.
- [`examples/smoke_slurm.py`](examples/smoke_slurm.py) — direct-Python entry point for Slurm features the CLI can't express yet.

## Configuration

`hsm setup init` writes `.hsm/config.yaml` with:

```yaml
project:
  name: your-project
  root: /path/to/your/project

paths:
  python_interpreter: /path/to/python
  train_script: train.py
  config_dir: configs
  output_dir: outputs

wandb:
  project: your-project
  entity: ""
```

Resource defaults live in three independent, mode-scoped typed blocks
appended to the file as commented scaffolds (and uncommented when GPUs
are detected for `local:`). Each `--mode` reads only its own block:

| Block | Read by | Holds |
|---|---|---|
| `local:` | `--mode local` | `walltime` / `cpus_per_task` / `mem` / `gpus` / `pre_script` |
| `slurm:` | `--mode array` / `--mode individual` | full Slurm spec including `gpu_type` / `modules` / `qos` / `account` / `max_array_size` |
| `distributed.remotes.<alias>.spec:` | `--remote <alias>` / `--mode distributed` | per-remote default `ResourceSpec` |

See [HPC_EXECUTION.md](docs/user_guide/HPC_EXECUTION.md#mode-scoped-config-blocks--no-cross-mode-bleed)
for the full schema, and [SSH_EXECUTION.md](docs/user_guide/SSH_EXECUTION.md)
for the `distributed:` block layout.

## Requirements

- Python 3.11+
- For HPC: a Slurm or PBS cluster with `sbatch`/`qsub` on PATH.
- For remote SSH: a working `~/.ssh/config` alias to your remote (no
  HSM install needed on the remote).
- Dependencies: see `pyproject.toml`. Install with `pip install -e ".[dev]"`
  to also get pytest + ruff + mypy.

## Contributing

This is alpha and under active refactor. Before adding a feature, read:

- [ARCHITECTURE.md](ARCHITECTURE.md) — what's live vs legacy and what's
  about to be deleted in Pass B-heavy.
- [CLAUDE.md](CLAUDE.md) — the "do not reintroduce" list (auto-discovery,
  `RemoteJobManager`, `--mode remote --remote X` flag shape, etc.).

Tests: `pytest tests/unit tests/cli tests/integration --no-cov`.
There's a ~6-test pre-existing baseline failure list (Click + log_cli
interaction, plus one stale legacy assertion); don't chase those as
regressions.

## License

MIT. See `LICENSE`.
