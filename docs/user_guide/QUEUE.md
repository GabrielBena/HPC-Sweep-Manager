# Cluster queue inspection

`hsm queue` is a small read-only group of subcommands that wrap
`squeue` / `sprio` / `scontrol show reservations` with richer output
than raw stdout — most importantly, it cross-references job IDs against
the sweeps you have under `sweeps/outputs/` so you can see which job
goes with which sweep.

All four subcommands need a Slurm scheduler on PATH; on machines without
one they print a one-line `No Slurm scheduler detected` and exit
cleanly. PBS isn't covered yet (the underlying
`core/hpc/scheduler_queue.SlurmQueue` is Slurm-only); contributions
welcome.

## `hsm queue mine`

Your jobs (any state) in a single rich table with:

| Column | Meaning |
|---|---|
| Job ID | Slurm job id, including the `_N` array-task suffix when applicable. |
| State | `PENDING` / `RUNNING` / `COMPLETING` / `FAILED` / ... with color. |
| Name | Slurm job name (HSM uses `<sweep_id>_array` for array submissions). |
| Reason / Node | `(Resources)` = next-up; `(Priority)` = waiting on others; or the running node list. |
| GPU | `1×H100` etc.; blank for CPU-only jobs. |
| Sweep | The local sweep ID this job belongs to, looked up via `sweeps/outputs/*/submission_summary.txt`. Blank if no match. |

This is what `hsm sweep queue` should be — it's the recommended view going forward.

## `hsm queue position [JOB_ID]`

> Note: this is best-effort. Slurm is dynamic — newly-submitted
> high-priority jobs can push your job back, and the
> [S3IT job-management docs](https://docs.s3it.uzh.ch/cluster/job_management/)
> explicitly say there's no definitive "when will my job run?" answer.

**With a `JOB_ID`:**

```bash
$ hsm queue position 3553026
3553026: position 7 / 42 pending GPU jobs cluster-wide
```

**Without `JOB_ID`** — reports position of every one of your pending
GPU jobs as a table:

```
GPU queue position — 4 of yours / 42 total pending
┏━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
┃ Job ID     ┃ Position ┃ Reason      ┃ GPU  ┃ Expected Start      ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
│ 3553026_1  │ 7  / 42  │ (Priority)  │ 1×H100│ 2026-05-28T16:00:00 │
│ 3553026_2  │ 8  / 42  │ (Priority)  │ 1×H100│ 2026-05-28T16:00:00 │
│ ...        │          │             │       │                     │
└────────────┴──────────┴─────────────┴───────┴─────────────────────┘
```

Position uses priority-descending order (`squeue -S '-Q'`), the same
order Slurm uses to decide what runs next. CPU-only pending jobs are
filtered out — they're not in the same queue.

## `hsm queue gpus [--mine]`

Per-GPU-type queue depth, cluster-wide:

```
GPU queue depth by type
┏━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━┓
┃ Type ┃ Running ┃ Pending ┃ Other ┃
┡━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━┩
│ A100 │ 12      │  3      │       │
│ H100 │ 31      │ 47      │       │
│ H200 │  8      │  0      │       │
│ L4   │ 16      │ 12      │       │
└──────┴─────────┴─────────┴───────┘
```

With `--mine` an extra `Mine (R/P)` column shows your contribution to
each row — useful for "am I overcommitting on H100s?" sanity checks.

Jobs requesting GPUs without a type (`--gpus=1`, no `--gres=gpu:TYPE:N`)
are aggregated under `<untyped>`.

## `hsm queue reservations`

Upcoming maintenance windows from `scontrol show reservations`:

```
Upcoming reservations
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━┓
┃ Name          ┃ Start               ┃ End                 ┃ Duration  ┃ Nodes ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━┩
│ maint_2026_06 │ 2026-06-01T06:00:00 │ 2026-06-01T18:00:00 │ 12:00:00  │ 2     │
└───────────────┴─────────────────────┴─────────────────────┴───────────┴───────┘
```

Useful as a "should I bother submitting this overnight" pre-flight check.

## Relationship to `hsm sweep queue`

The older `hsm sweep queue` (raw `squeue -u $USER` dump) still works
but is unstructured. It now prints a `Tip:` footer pointing at
`hsm queue mine` for the richer view. Both will coexist for now;
`hsm queue` is the recommended path going forward.

## API

The CLI is a thin wrapper around
[`core/hpc/scheduler_queue.SlurmQueue`](../../src/hpc_sweep_manager/core/hpc/scheduler_queue.py).
If you want to script against the data directly:

```python
from hpc_sweep_manager.core.hpc.scheduler_queue import SlurmQueue, slurm_available

if slurm_available():
    q = SlurmQueue()
    my_pending = [j for j in q.list_user_jobs("alice") if j.state == "PENDING"]
    pos = q.position_in_gpu_queue("3553026")  # -> (7, 42) or None
    depth = q.gpu_summary()                    # {"H100": {"RUNNING": 31, ...}, ...}
    res = q.reservations()
```

Methods return frozen dataclasses (`QueueJob`, `Reservation`); see the
module docstrings for fields.
