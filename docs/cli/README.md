# CLI reference

Every `hsm <group> <command>` documented here is verified against
`hsm --help` output. If you spot drift, regenerate this doc by running
each group's `--help` and comparing.

For broader tutorials (sweep config, training-script contract, GPU
pinning, etc.) see the [user guides](../user_guide/).

## Top-level groups

```
hsm setup    init configure                       # bootstrap
hsm sweep    run | status | report | errors | watch | recent | queue | cancel | cleanup
hsm remote   add | list | test | health | gpus | clean | remove
hsm queue    mine | position | gpus | reservations
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
  --gpus TEXT                        GPU allowlist (local or remote): 'all' (default), 'cpu', int N, or '0,1,3'.
  -d, --dry-run                      Show resolved spec + the exact task-1 command; no submission.
  --count-only                       Count parameter combinations and exit.
  --max-runs INTEGER                 Cap N for testing.
  --walltime TEXT                    HH:MM:SS (overrides the typed `local:` / `slurm:` block walltime).
  --resources TEXT                   Scheduler resource string (slurm `--flag=value` or PBS `select=...`).
  --group TEXT                       W&B group name for this sweep.
  -p, --parallel-jobs INTEGER        Max concurrent jobs (local / remote slot count).
  --no-progress                      Suppress progress callback prints.
  -v, --verbose                      Enable verbose (DEBUG) logging.
  -q, --quiet                        Suppress non-error output.
```

Examples:

```bash
hsm sweep run --mode local --parallel-jobs 4
hsm sweep run --mode array --walltime 01:00:00 --resources "--cpus-per-task=4 --mem=16gb"
hsm sweep run --remote my-box --gpus 0,1 --resources "--gpus=1"
hsm sweep run --mode distributed     # uses .hsm/config.yaml's distributed: block
```

### `hsm sweep status [sweep_id]`

Show completion status. With no argument, summarizes every sweep under
`sweeps/outputs/`. With a sweep ID, shows per-task state.

### `hsm sweep report <sweep_id>` / `errors <sweep_id>`

Detailed completion report; error summaries for FAILED tasks.

### `hsm sweep watch <sweep_id>` / `recent` / `queue` / `cancel` / `cleanup`

Sweep lifecycle commands — all built on `SweepCompletionAnalyzer` so
they're backend-agnostic (work for local / Slurm / SSH push / distributed
the same way).

| Subcommand | What it does |
|---|---|
| `hsm sweep watch <sweep_id>` | Live progress display for one sweep. Polls `tasks/*/task_info.txt` and redraws every `--refresh` seconds. Exits automatically when the sweep finishes. Use `--once` for a one-shot snapshot. |
| `hsm sweep recent [-d N]` | Table of sweeps from the last N days (default 7). Shows backend, progress, completion %, and COMPLETE/INCOMPLETE status. |
| `hsm sweep queue` | Auto-detects `squeue` or `qstat` on PATH and shows the cluster's job queue for $USER. Reports cleanly when no scheduler is installed. |
| `hsm sweep cancel <sweep_id>` | Cancel a running sweep. Reads `Backend:` from `submission_summary.txt` and dispatches: `scancel` (Slurm), `qdel` (PBS), or `pkill -f <sweep_id>` (local). For SSH push and distributed sweeps, prints a clear "kill the local `hsm sweep run` process or `ssh <alias> 'pkill -f <sweep_id>'`" message — no reliable remote-pid tracking exists for those backends. |
| `hsm sweep cleanup [-d N] [--keep-incomplete] [--dry-run]` | Delete sweep output dirs older than N days (default 30). `--keep-incomplete` spares sweeps with failed/missing tasks. `--dry-run` lists candidates. |

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

## `hsm queue`

Cluster-wide Slurm queue inspection. Read-only — wraps `squeue` /
`scontrol show reservations` with richer output (including sweep-ID
linkage for your jobs). Runs locally on a login node, or **over SSH**
with `--remote <alias>` (every subcommand takes it) so you can monitor
the cluster from the workstation that drives the sweeps. Pending arrays
are counted per-task (`squeue -r` for totals/positions; a `×N` Tasks
column for compact views).

| Subcommand | What it does |
|---|---|
| `hsm queue mine [--flat] [--watch]` | Your jobs **grouped by array**: one row per array with `▶ running ⏳ pending` (squeue) and `✓ completed ✗ failed` + progress-out-of-total (sacct accounting, gracefully omitted when unavailable), plus sweep IDs cross-referenced from `sweeps/outputs/*/submission_summary.txt` AND `.hsm_manifest.json` (SSH-Slurm sweeps). `--flat` for the per-task rows. |
| `hsm queue position [<job_id>]` | Position of your pending GPU task(s) in the cluster-wide priority-sorted GPU queue. With a `job_id` (exact task or array base), reports just that job; without, reports every pending GPU job of yours, plus a note for CPU-only pending tasks. |
| `hsm queue gpus [--no-mine] [--watch]` | Per-GPU-type capacity + queue: VRAM per GPU (cluster-reported `GPUMEM` feature tags, `~`-marked model-typical fallback), Total / In use / Free from `sinfo` allocation accounting (excludes down/drained nodes; degrades to queue-only when sinfo is absent), Pending demand from `squeue -r`, and your contribution (Mine, on by default — `--no-mine` to hide). |
| `hsm queue reservations` | Upcoming Slurm maintenance windows. |

`--watch [--refresh N]` (on `mine` and `gpus`) keeps the view
refreshing; over SSH the connection is opened once and reused. With no
local `squeue` and exactly one `backend: slurm` remote registered, the
remote is used automatically (a note is printed); otherwise the error
lists the available `--remote` choices. A failing remote exits non-zero
— never a silently empty table. PBS isn't covered yet. See
[../user_guide/QUEUE.md](../user_guide/QUEUE.md) for output samples and
the underlying API.

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
