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

## `hsm queue mine [--flat] [--remote ALIAS] [--watch [--refresh N]]`

**One row per array** (running array tasks would otherwise be one squeue
row *each* — hundreds of near-identical lines mid-sweep):

```
                                 My queue (gbena) — grouped by array
┏━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━┓
┃ Job ID  ┃ Name                        ┃ Tasks            ┃ Progress            ┃    GPU ┃ Where / Why ┃
┡━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━┩
│ 3703585 │ sweep_20260603_224456_array │ ▶9 ⏳4 ✓8 ✗1     │ ▰▰▰▰▱▱▱▱▱▱ 9/22     │ 1×A100 │ 3 nodes     │
│ 3710878 │ sweep_20260604_092647_array │ ▶360 ⏳1062 ✓498 │ ▰▰▰▱▱▱▱▱▱▱ 498/1920 │        │ 65 nodes    │
└─────────┴─────────────────────────────┴──────────────────┴─────────────────────┴────────┴─────────────┘
2 array(s) · 1435 task(s) in queue (369 running, 1066 pending) · 507 finished · 1 FAILED
```

(A `Sweep` column — elided above for width — links each job to its local
sweep dir; see below.)

| Cell | Meaning / source |
|---|---|
| Tasks | `▶` running + `⏳` pending (squeue, live, task-weighted) · `✓` completed + `✗` failed/cancelled (**sacct accounting** — these tasks already left the queue and are invisible to squeue). `✗` is omitted when 0; both are omitted when accounting is unavailable. |
| Progress | 10-segment bar + `finished/total`. Total is the array's true size from sacct (or, failing that, the sweep's `num_tasks` from local metadata). No known total → `—`, never a guess. |
| Where / Why | Running → node name (or `N nodes`); pending-only → squeue reason (`(Priority)`, `(QOSMaxJobsPerUserLimit)`, ...). |
| GPU | Per-task GPU spec, `1×A100` etc. |
| Sweep | The local sweep ID, from `sweeps/outputs/*/submission_summary.txt` (local/array submissions) **and** `.hsm_manifest.json` (SSH-Slurm). Blank when the sweep was driven from a different project/machine. |

The footer aggregates everything and shouts `N FAILED` in red when
accounting reports failures — a failed array task no longer hides among
hundreds of running siblings.

**`--flat`** restores the per-task view (one row per running task,
collapsed pending ranges with a `×N` Tasks column) when you need to find
a *specific* task.

**Degradation:** clusters without accounting (`sacct` missing or
disabled) lose only ✓/✗ and exact totals — the view falls back to sweep
metadata for totals and notes `no accounting data`. Run from the project
directory that launched the sweeps to get the `Sweep` linkage and the
metadata fallback.

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

## `hsm queue gpus [--no-mine] [--remote ALIAS] [--watch [--refresh N]]`

Per-GPU-type **capacity and** queue depth, cluster-wide:

```
                       GPU capacity & queue by type
┏━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━┳━━━━━━━━┳━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━┓
┃ Type      ┃ VRAM/GPU ┃ Total ┃ In use ┃ Free ┃ Pending ┃ Mine (R/P) ┃
┡━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━╇━━━━━━━━╇━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━┩
│ <untyped> │          │       │        │      │      83 │        0/0 │
│ A100      │      80G │    40 │     34 │    6 │       4 │        9/4 │
│ H100      │   80/96G │    28 │     28 │    0 │      14 │        0/0 │
│ H200      │     140G │    16 │     16 │    0 │         │        0/0 │
│ L4        │      24G │    16 │      9 │    7 │       4 │        0/0 │
│ V100      │      32G │    48 │      3 │   45 │         │        0/0 │
└───────────┴──────────┴───────┴────────┴──────┴─────────┴────────────┘
58 GPU(s) free right now
<untyped> = jobs requesting a GPU without a type (e.g. --gpus=1) — demand
only; once running, their GPUs are attributed to the physical type in the
In-use column.
```

- **Total / In use / Free** come from `sinfo`'s per-node allocation
  accounting (`Gres` / `GresUsed`), so GPUs consumed by *untyped* job
  requests are attributed to their physical type and **Free is real, not
  an estimate**. GPUs on down/drained nodes are excluded from totals (a
  footer notes how many). Types with zero queue demand (idle V100s
  above) still show — that's the point of a Free column.
- **VRAM/GPU** is cluster-reported when nodes carry `GPUMEM<N>GB`
  feature tags (S3IT does) — mixed node groups list every variant
  (`80/96G` above is real: two H100 flavors). When the cluster doesn't
  report it, a model-typical value is shown with a `~` marker (and a
  legend), only for models with a single common configuration —
  ambiguous ones (A100 40/80, V100 16/32) show `?` rather than a
  confident guess.
- **Pending** is demand from `squeue -r` (per-task). Jobs requesting
  GPUs without a type (`--gpus=1`, no `--gres=gpu:TYPE:N`) aggregate
  under `<untyped>` — demand-only, no physical inventory (explained by
  an automatic legend whenever the row appears).
- **Mine (R/P)** — your contribution per type — is **on by default**
  (`--no-mine` to hide). Useful for "am I overcommitting on H100s?" and
  for spotting QoS-capped pendings: pending tasks *despite* free GPUs of
  that type usually means `(QOSMaxJobsPerUserLimit)`, not capacity.
- Clusters where `sinfo` is unavailable degrade to the queue-only
  Running/Pending table with a one-line note — never silently.

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
