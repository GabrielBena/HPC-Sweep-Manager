# CLI reference

Every `hsm <group> <command>` documented here is verified against
`hsm --help` output. If you spot drift, regenerate this doc by running
each group's `--help` and comparing.

For broader tutorials (sweep config, training-script contract, GPU
pinning, etc.) see the [user guides](../user_guide/).

## Top-level groups

```
hsm setup    init configure                       # bootstrap
hsm sweep    run | complete | status | report | errors
hsm remote   add | list | test | health | gpus | clean | remove
hsm local    run | status | clean                 # legacy backend (kept until completion migrates)
hsm hpc      submit | queue | status | cancel     # legacy backend (kept until completion migrates)
hsm monitor  watch | status | recent | queue | cancel | cleanup | delete-jobs
hsm sync     init | list | run | to               # rsync results to a target
hsm analyze  enable-tracking | report | dead-code | complexity | dependencies | coverage-gaps
```

Global options (apply to every command):

- `-v, --verbose` — DEBUG-level logging.
- `-q, --quiet` — ERROR-level only.
- `--version` — print version and exit.

## `hsm setup`

Project bootstrap. Run once per ML project.

| Subcommand | What it does |
|---|---|
| `hsm setup init` | Auto-detect paths, write `.hsm/config.yaml`, `sweeps/example_sweep.yaml`, etc. Idempotent — re-running migrates an old `sweeps/hsm_config.yaml` to the new `.hsm/` layout. |
| `hsm setup configure` | Interactive sweep-config builder. |

## `hsm sweep`

The main workflow. `hsm sweep run` is the entry point you'll use most.

### `hsm sweep run`

```
hsm sweep run [OPTIONS]

  -c, --config PATH                  Path to sweep config (default: sweeps/sweep.yaml)
  --mode [auto|local|array|individual|distributed|remote]
                                     Default: 'remote' if --remote given, else 'auto'
  --remote TEXT                      ssh-config alias to push the sweep to. Implies --mode remote.
  --gpus TEXT                        Allowlist on the remote: 'all' (default), 'cpu', int N, or '0,1,3'.
  -d, --dry-run                      Render scripts + show resolved spec; no submission.
  --count-only                       Count parameter combinations and exit.
  --max-runs INTEGER                 Cap N for testing.
  --walltime TEXT                    HH:MM:SS (overrides hpc.default_walltime).
  --resources TEXT                   Scheduler resource string (slurm `--flag=value` or PBS `select=...`).
  --group TEXT                       W&B group name for this sweep.
  --priority INTEGER                 Job priority (HPC schedulers that support it).
  -p, --parallel-jobs INTEGER        Max concurrent jobs (local / remote slot count).
  --show-output                      Stream stdout/stderr live (local mode only).
  --no-progress                      Suppress progress callback prints.
```

Examples:

```bash
hsm sweep run --mode local --parallel-jobs 4
hsm sweep run --mode array --walltime 01:00:00 --resources "--cpus-per-task=4 --mem=16gb"
hsm sweep run --remote my-box --gpus 0,1 --resources "--gpus=1"
hsm sweep run --mode distributed     # uses .hsm/config.yaml's distributed: block
```

### `hsm sweep complete <sweep_id>`

Resume an incomplete sweep by re-running missing/failed task numbers.
Same flags as `run` plus `--task-ids "1,3,5-9"`, `--no-retry-failed`,
`--complete-baselines`, `--verify-running`, etc.

See [../user_guide/COMPLETION_RUNS.md](../user_guide/COMPLETION_RUNS.md).

### `hsm sweep status [sweep_id]`

Show completion status. With no argument, summarizes every sweep under
`sweeps/outputs/`. With a sweep ID, shows per-task state.

### `hsm sweep report <sweep_id>` / `errors <sweep_id>`

Detailed completion report; error summaries for FAILED tasks.

## `hsm remote`

Manage SSH remote registrations. Many of these work with a bare
`~/.ssh/config` alias (no `hsm remote add` required first).

| Subcommand | What it does |
|---|---|
| `hsm remote add <name> [host]` | Register a remote in `.hsm/config.yaml`. Host defaults to alias. Optional flags: `--key`, `--port`, `--max-jobs`, `--enabled/--disabled`. |
| `hsm remote list` | Table of registered remotes. |
| `hsm remote test <name> [more...]` / `--all` | Quick SSH ping (date + python --version + uptime). |
| `hsm remote health <name> [more...]` / `--all` / `--watch` | Detailed health (load + disk + python). `--watch` polls. |
| `hsm remote gpus <name> [more...]` / `--all` | `nvidia-smi` probe; shows memory + util + free/busy per GPU. |
| `hsm remote clean <name>` / `--all-projects` / `-y` | `rm -rf` HSM's scratch on the remote (this project's dir or the whole `~/.hsm/runs/`). |
| `hsm remote remove <name>` | Unregister from config. |

See [../user_guide/SSH_EXECUTION.md](../user_guide/SSH_EXECUTION.md) for the full SSH workflow.

## `hsm monitor`

Live sweep monitoring + job management.

| Subcommand | What it does |
|---|---|
| `hsm monitor watch [sweep_id]` | Live progress display. |
| `hsm monitor status` | One-shot status snapshot. |
| `hsm monitor recent [--days N]` | Recent sweeps from the last N days. |
| `hsm monitor queue` | All your jobs across schedulers. |
| `hsm monitor cancel <sweep_id>` | Cancel a running sweep. |
| `hsm monitor cleanup` | Tidy old job artifacts by age/state. |
| `hsm monitor delete-jobs` | Delete specific jobs with filters. |

## `hsm sync`

rsync sweep outputs to a destination machine (or pull them from one).
Separate from the auto-rsync inside `--remote` execution; this is for
moving completed sweep dirs around after the fact.

| Subcommand | What it does |
|---|---|
| `hsm sync init` | Write `.hsm/sync_config.yaml` template. |
| `hsm sync list` | Show configured sync targets. |
| `hsm sync run <sweep_id> [--target X]` | Sync one sweep. |
| `hsm sync to <target>` | Sync the most recent sweep to a target. |
| `hsm sync cache` | Manage wandb sync cache. |
| `hsm sync clean` | Delete local sweep data including wandb runs + metadata. |

## `hsm local` (legacy)

Direct local execution via the legacy `LocalJobManager`. Kept until
completion runs migrate to `LocalComputeSource`.

| Subcommand | What it does |
|---|---|
| `hsm local run` | Run a sweep locally (legacy path). |
| `hsm local status` | Local environment info. |
| `hsm local clean` | Clean old local sweep outputs. |

Prefer `hsm sweep run --mode local` for new work — it goes through the
unified orchestrator.

## `hsm hpc` (legacy)

Direct HPC submission via the legacy `HPCJobManager`. Kept until
completion runs migrate to `SlurmComputeSource` / `PBSComputeSource`.

| Subcommand | What it does |
|---|---|
| `hsm hpc submit` | Submit a sweep to Slurm/PBS (legacy path). |
| `hsm hpc queue` | List your queued jobs. |
| `hsm hpc status` | HPC system status. |
| `hsm hpc cancel` | Cancel a specific HPC job. |

Prefer `hsm sweep run --mode array|individual|auto` for new work.

## `hsm analyze`

Codebase analysis tooling — for HSM developers, not sweep users.

| Subcommand | What it does |
|---|---|
| `hsm analyze enable-tracking` | Turn on usage tracking. |
| `hsm analyze report` | Generate a usage-analysis report. |
| `hsm analyze dead-code` | Detect unused code. |
| `hsm analyze complexity` | Code complexity report. |
| `hsm analyze dependencies` | Module-dependency graph. |
| `hsm analyze coverage-gaps` | Identify test coverage gaps. |

## Environment variables

- `HSM_FAKE_STATE_DIR`, `HSM_FAKE_PENDING_S`, `HSM_FAKE_RUNNING_S`,
  `HSM_FAKE_GPU_COUNT` — internal use, populated by test fixtures. Not
  for normal usage.

## Exit codes

- `0` — success.
- non-zero — error (specific code depends on subcommand).

## See also

- [../user_guide/getting_started.md](../user_guide/getting_started.md)
- [../user_guide/SSH_EXECUTION.md](../user_guide/SSH_EXECUTION.md)
- [../user_guide/HPC_EXECUTION.md](../user_guide/HPC_EXECUTION.md)
- [../api_reference/](../api_reference/)
