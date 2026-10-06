# Field notes — Comp-PVR on the `uzh` ssh-slurm remote, 2026-08-17 → 08-27

Consumer-side feedback only; no source patches were kept. Three items, in order of how much they
cost an autonomous consumer. All reproduced on HSM `main` at 5f87299.

## 1. `--mode remote` submits one `sbatch` per task; nothing warns that this will lock you out

**What happened.** `hsm sweep run -c <cfg> --remote uzh --mode remote` on a 600-run, a 160-run, a
180-run and then a 576-run sweep. Each submission is one ssh + one `sbatch`. ~1500 ssh sessions in
a few minutes tripped the login node's sshd `MaxStartups`; every ssh (including the ones HSM itself
needed for status and collect) was refused for ~20 min, with the sweep half-submitted and no
consumer-side way to tell which tasks had made it.

**Repro.** Any sweep of a few hundred tasks with `--remote <slurm host> --mode remote`.

**What is already there.** `cli/sweep.py:1050-1056` supports `--remote uzh --mode array`, which
submits one job array (one ssh). It is not the default for a Slurm remote, and the docs I read
(SSH_EXECUTION.md / MULTI_CLUSTER.md) did not steer me to it.

**Suggested fix.** For a Slurm remote, default to array submission (or refuse `--mode remote` above
some task count without `--force`), and print the submission style before the first sbatch. A
one-line "submitting N individual jobs — consider --mode array" would have been enough.

## 2. `hsm sweep collect` fans out one ssh per task and dies on big sweeps

**What happened.** `collect` on the 600-run sweep raised `ConnectionResetError` part-way through
(one rsync/ssh per task dir; the same MaxStartups budget as above). A single
`rsync -a uzh:<workdir>/<project>/sweeps/<sweep>/tasks/ <local>/tasks/` pulls the whole sweep in
one connection and is what I used instead (`Comp-PVR/scripts/rescue_checkpoints.sh`).

**Suggested fix.** One rsync per sweep (or per N tasks), with `--partial` and a backoff on
`ConnectionReset`/`kex_exchange_identification` so a transient refusal does not abort the collect.

## 3. Task directory naming depends on submission mode

Array mode writes `tasks/task_<n>`; individual mode writes `tasks/<sweep_id>_task_<nnn>`. Every
consumer glob (`tasks/task_*`) silently matched zero directories after switching modes — three
analysis scripts reported "0 runs" with exit 0. Suggest one naming scheme, or a documented helper
that lists task dirs regardless of mode. Consumer-side workaround: `task_dirs()` in
`Comp-PVR/scripts/analyze_survival.py` accepts both and sorts on the trailing integer.

## Minor

- `hsm sweep run` reports "submitted" before Slurm has accepted the array; a task count from
  `squeue` immediately after would catch the partial-submission case in item 1.
- Remote `results.csv` leaves OOD/val cells blank on non-eval epochs (that is the consumer script's
  doing, not HSM's) — noting it here only because `collect`'s summary CSV inherits the object dtype.
