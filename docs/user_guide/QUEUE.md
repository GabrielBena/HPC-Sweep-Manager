# Cluster queue inspection

`hsm queue` is a small read-only group of subcommands that wrap
`squeue` / `scontrol show reservations` with richer output than raw
stdout — most importantly, it cross-references job IDs against the
sweeps you have under `sweeps/outputs/` so you can see which job goes
with which sweep.

It runs over one of **two transports**:

- **local** — `squeue`/`scontrol` on this machine (a cluster login node);
- **SSH** — the *same* commands on a remote login node, via
  `--remote <alias>`. The alias is a `backend: slurm` entry from your
  `.hsm/config.yaml` `distributed.remotes` block, or failing that a plain
  `~/.ssh/config` alias. This is the "monitor the cluster from the
  workstation that drives the sweeps" mode — which is also where the
  job→sweep linkage data lives, so the `Sweep` column actually populates
  there (see below).

**Auto-fallback:** when this machine has no `squeue` and exactly ONE
slurm-backend remote is registered in the current project, `hsm queue ...`
uses it automatically and prints a one-line note:

```bash
$ cd ~/code/my-project        # has distributed.remotes.uzh: {backend: slurm}
$ hsm queue mine
No local squeue — using remote 'uzh' (sole slurm-backend remote).
...
```

Zero or several candidates → a clear error telling you which `--remote`
values are available. A failing remote (unreachable host, no `squeue` on
its PATH) exits non-zero with a message — never a silently empty table.

PBS isn't covered (the underlying `core/hpc/scheduler_queue` is
Slurm-only); contributions welcome.

## Arrays are counted as tasks

A *pending* Slurm array job is a single `squeue` row
(`3710878_[690-1920%4]` — 1231 queued tasks; the `%4` is a concurrency
throttle, not a count). `hsm queue` handles this in both directions:

- **counting paths** (`position`, `gpus`) query with `squeue -r` so Slurm
  expands per-task rows — totals and positions count *tasks*;
- **display paths** (`mine`) keep the compact collapsed row and show a
  `Tasks` column (`×1231`).

## `hsm queue mine [--remote ALIAS] [--watch [--refresh N]]`

Your jobs (any state) in a single rich table with:

| Column | Meaning |
|---|---|
| Job ID | Slurm job id, including `_N` (array task) or `_[a-b%t]` (collapsed pending range). |
| State | `PENDING` / `RUNNING` / `COMPLETING` / `FAILED` / ... with color. |
| Name | Slurm job name (HSM uses `<sweep_id>_array` for array submissions). |
| Reason / Node | `(Resources)` = next-up; `(Priority)` = waiting on others; `(QOSMaxJobsPerUserLimit)` = your own QoS cap; or the running node list. |
| Tasks | `×N` for collapsed pending array rows. |
| GPU | `1×H100` etc.; blank for CPU-only jobs. |
| Sweep | The local sweep ID this job belongs to. Looked up from `sweeps/outputs/*/submission_summary.txt` (local/array submissions) **and** `.hsm_manifest.json` (SSH-Slurm submissions). Blank if no match — e.g. when the sweep was driven from a different project or machine. |

A footer totals it up (`2 queue rows · 1216 tasks`). Run it from the
project directory that launched the sweeps to get the `Sweep` linkage.

## `hsm queue position [JOB_ID] [--remote ALIAS]`

> Note: this is best-effort. Slurm is dynamic — newly-submitted
> high-priority jobs can push your job back, and the
> [S3IT job-management docs](https://docs.s3it.uzh.ch/cluster/job_management/)
> explicitly say there's no definitive "when will my job run?" answer.

**With a `JOB_ID`** — exact task ids match directly; an array *base* id
reports the array's best-placed pending task:

```bash
$ hsm queue position 3703585 --remote uzh
3703585: 4 pending GPU task(s) — first at position 6 / 80 pending GPU tasks cluster-wide
```

**Without `JOB_ID`** — one row per pending GPU job of yours (arrays
collapsed, task-counted):

```
GPU queue position — 4 task(s) of yours / 80 total pending GPU tasks
┏━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ Job ID          ┃ Tasks ┃ First position ┃ Reason     ┃    GPU ┃ Expected Start ┃
┡━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ 3703585_[19-22] │    ×4 │         6 / 80 │ (Priority) │ 1×A100 │ N/A            │
└─────────────────┴───────┴────────────────┴────────────┴────────┴────────────────┘
Plus 1212 pending CPU-only task(s) of yours, not part of the GPU queue.
```

Position uses priority-descending order (`squeue -r -S '-Q'`), the same
order Slurm uses to decide what runs next. CPU-only pending jobs are not
*in* the GPU queue; they're surfaced as a note instead of silently
vanishing.

## `hsm queue gpus [--mine] [--remote ALIAS] [--watch [--refresh N]]`

Per-GPU-type queue depth, cluster-wide (GPUs, task-weighted):

```
GPU queue depth by type
┏━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━┓
┃ Type      ┃ Running ┃ Pending ┃ Other ┃ Mine (R/P) ┃
┡━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━┩
│ <untyped> │      26 │      74 │       │        0/0 │
│ A100      │      12 │       4 │       │        9/4 │
│ H100      │       7 │       4 │       │        0/0 │
│ L4        │       7 │       0 │       │        0/0 │
└───────────┴─────────┴─────────┴───────┴────────────┘
```

With `--mine` the extra `Mine (R/P)` column shows your contribution to
each row — useful for "am I overcommitting on H100s?" sanity checks.

Jobs requesting GPUs without a type (`--gpus=1`, no `--gres=gpu:TYPE:N`)
are aggregated under `<untyped>`.

## `hsm queue reservations [--remote ALIAS]`

Upcoming maintenance windows from `scontrol show reservations`:

```
Upcoming reservations
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Name          ┃ Start               ┃ End                 ┃ Duration ┃ Nodes ┃ Node spec             ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━┩
│ maint_2026_06 │ 2026-06-01T06:00:00 │ 2026-06-01T18:00:00 │ 12:00:00 │     2 │ u24-chiihm0-[621-622] │
└───────────────┴─────────────────────┴─────────────────────┴──────────┴───────┴───────────────────────┘
```

Useful as a "should I bother submitting this overnight" pre-flight check.

## Watch mode

`mine` and `gpus` take `--watch` (with `--refresh N`, default 30s) for a
continuously refreshing view — the live dashboard you keep open while a
sweep runs:

```bash
hsm queue gpus --remote uzh --mine --watch --refresh 20
```

In SSH mode the connection is opened **once** and reused across
refreshes (no per-cycle handshake). `Ctrl+C` stops cleanly.

## Relationship to `hsm sweep queue`

The older `hsm sweep queue` (raw `squeue -u $USER` dump) still works
but is unstructured. It now prints a `Tip:` footer pointing at
`hsm queue mine` for the richer view. Both will coexist for now;
`hsm queue` is the recommended path going forward.

## API

The CLI is a thin wrapper around
[`core/hpc/scheduler_queue`](../../src/hpc_sweep_manager/core/hpc/scheduler_queue.py),
which ships both transports over shared pure parsers (so they can't
drift). If you want to script against the data directly:

```python
from hpc_sweep_manager.core.hpc.scheduler_queue import SlurmQueue, slurm_available

if slurm_available():
    q = SlurmQueue()
    my_pending = [j for j in q.list_user_jobs("alice") if j.state == "PENDING"]
    pos = q.position_in_gpu_queue("3553026_7")  # -> (7, 42) or None
    depth = q.gpu_summary()                      # {"H100": {"RUNNING": 31, ...}, ...}
    res = q.reservations()
```

Over SSH (async; bring your own connection):

```python
from hpc_sweep_manager.core.hpc.scheduler_queue import SSHSlurmQueue
from hpc_sweep_manager.core.remote.discovery import create_ssh_connection

conn = await create_ssh_connection("uzh")
q = SSHSlurmQueue(conn)
user = await q.whoami()
jobs = await q.list_user_jobs(user)   # same QueueJob dataclasses
```

`SSHSlurmQueue` raises `QueueCommandError` on any failure (missing
`squeue`, non-zero exit, timeout) rather than returning an empty list —
over a network hop, "no rows" must mean "no jobs".

Methods return frozen dataclasses (`QueueJob`, `Reservation`); see the
module docstrings for fields. Note `QueueJob.tres_per_node`: squeue's
`%b` is TRES **per node** and on Slurm ≥ 23 uses colon-count GRES syntax
(`gres/gpu:A100:1`); the parser also accepts the `=`-count accounting
grammar (`gres/gpu:h100=1`).
