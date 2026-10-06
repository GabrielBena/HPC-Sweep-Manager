# HSM field report: the Comp-PVR replay campaign (2026-09-29 → 10-06)

*Consumer: the Claude session `modularity` (agent-deck 9363550e), driving HSM from anahita (editable `main` at
`5f87299`, the `cpvr` env, asyncssh 2.23.0). Written 2026-10-06 for the HSM maintenance pass. It consolidates the
two field-notes files in this folder ([`2026-08-27-comp-pvr-mode-remote-collect.md`](2026-08-27-comp-pvr-mode-remote-collect.md),
[`2026-09-29-comp-pvr-replay.md`](2026-09-29-comp-pvr-replay.md)) and adds what they don't cover. No HSM source was changed by the consumer.*

## Usage this covers

- **uzh (S3IT Slurm, `backend: slurm`, array mode):** about 20 arrays, 2 to 600 tasks each, about 2,400 tasks in
  total. The tasks were CPU-only (4 cpus, 8G, medium QoS, up to 47.5 h), launched as `hsm sweep run … --remote uzh
  --mode array`, with launchers living 1–3 days.
- **athena (INI workstation, `backend: ssh`, first real use):** 2 × 12 CPU tasks of about 30 h each
  (`--remote athena --gpus cpu`), currently running.
- **Config of record:** `~/code/active/modularity/Comp-PVR/.hsm/config.yaml` (untracked). Its comments document every
  workaround below.

## Findings, by severity

### P0: correctness and safety

1. **ssh backend: more than ~9 concurrent tasks per host kills the sweep and orphans tasks.**
   - With `max_parallel_jobs: 12`, the 11th task failed with `asyncssh.misc.ChannelOpenError: open failed`.
   - The orchestrator then aborted the sweep ("Sweep execution failed") and left the 10 started tasks running with no
     launcher.
   - Cause: sshd's `MaxSessions 10` (the default) per connection, with one channel held per running task.
   - Fixes:
     - launch tasks detached (`setsid nohup … &`, record the pid) and poll them all over one channel;
     - or cap concurrency at a probed or configured `max_sessions`, and warn at submit time;
     - and never orphan: on a submit failure, keep supervising what already started.
   - The consumer's workaround: ≤ 8 slots per remote entry, plus a second remote entry with the same `host:`.
2. **The shared remote code dir is re-synced at every launch, so pending tasks of earlier sweeps run newer code.**
   - Every sweep's tasks run from `<workdir>/<project>/code`, and each `hsm sweep run` rsyncs the current tree there.
   - A queued array task from sweep A that starts after sweep B's launch runs B's code. The consumer had to hold
     merges in the project until the uzh queue was empty.
   - Fix: a per-sweep code snapshot (`code/<sweep_id>/`, made cheap with `rsync --link-dest` to the previous one),
     with tasks `cd`ing into their own snapshot. That is also a reproducibility guarantee worth documenting.
3. **Silent auth hang on a stale forwarded agent** (09-29 notes §1).
   - asyncssh queries `SSH_AUTH_SOCK` first and hangs until the server resets the connection; HSM reports a
     connection reset.
   - Fix: `asyncio.wait_for` around connect, with a clear "auth timed out (agent?)" message, and a retry with
     `agent_path=None`; or honour `IdentityAgent none`.
   - The consumer runs every command as `env -u SSH_AUTH_SOCK hsm …`.
4. **Slurm `--mode remote` submits one `sbatch` per task** (08-27 notes §1). About 1,500 ssh sessions tripped the
   login node's `MaxStartups` and locked the user out for about 20 min, leaving a half-submitted sweep.
   - Fix: make array mode the default for Slurm remotes, or refuse above N tasks without `--force`.
5. **`hsm sweep collect` opens one ssh per task** and dies on big sweeps (08-27 notes §2).
   - Fix: one rsync per sweep, with `--partial` and a backoff.

### P1: being a good citizen on a shared cluster account

6. **No array throttle** (09-29 notes, addendum 2).
   - The `--array=1-K` is rendered without `%N`, so the consumer runs `scontrol update JobId=<id>
     ArrayTaskThrottle=N` after every submission.
   - A 1,320-task replay pushed the lab account to ~17× its fair share and a co-worker waited days.
   - Fix: `spec.array_throttle` / `--throttle N` → `--array=1-K%N`.
   - Also clarify what `max_parallel_jobs` means for Slurm array mode: 350 was set and not rendered as a throttle.
7. **No `nice` knob.** Used manually: `scontrol update JobId=<id> Nice=10000`. Warning for the docs: on uzh, Nice=10000
   drops the job to priority 1, behind every account in the partition, not just co-workers. Fix: `spec.nice`.
8. **CPU jobs land on GPU nodes** when `gpus` is unset: 24 of ~300 tasks did.
   - `extra_directives: {"--exclude": "<hostlist>"}` works; the key needs its `--`, and the value is rendered as
     `#SBATCH --exclude=…`.
   - Fix: document it, or add `cpu_only_nodes: true`, which resolves GPU nodes from
     `sinfo -p <part> -N -o "%N %G"` at submit time.
9. **Maintenance reservations: HSM warns, but not about what matters.**
   - At submission HSM printed the reservation `maint-u24-2026-10` (10-07 06:00–18:00), which was good. But it didn't
     check whether `now + walltime` crosses it.
   - The W5 arrays (47.5 h) sat pending for about 20 h, first shown as "Priority", later as "ReqNodeNotAvail,
     Reserved for maintenance", with 1,600 idle CPUs.
   - Fix: at submit time, if the walltime overlaps a reservation on the target nodes, say "won't start before <end>;
     a walltime ≤ X would start now".
10. **Fair-share awareness (optional).** The consumer wrote `uzh-share` (claude-config `scripts/common/uzh-share`):
    one ssh call, `sshare`/`squeue`, with the account usage vs its share, our part of it, and who's waiting. An
    `hsm remote share <alias>`, or a pre-submit warning above 2× share, would bring that into HSM.

### P2: ergonomics and robustness

11. **Re-running failed or timed-out tasks needs hand-written sweep files.** Walltime timeouts happened (W3 R2: 6 tasks,
    R3: 13 at 4096 epochs), and so did start-up failures. Each re-run was a custom YAML (`replay_w3_r2_rerun7.yaml`,
    `replay_w3_fill_january_rerun2.yaml`).
    - Fix: `hsm sweep rerun <sweep_id> --failed --timeout [--walltime X]` → a new array over exactly those
      combinations.
    - Also report TIMEOUT separately from FAILED in the summary and in `hsm sweep errors`.
12. **Non-product designs need several sweep files.** `sweep.grid` is a full product. W5 needed 5 files for one stage
    (arm × λ subsets, a width split across two remotes).
    - Fix: `exclude:` / `include:` filters, or an explicit `combinations:` list.
13. **Infrastructure failures aren't distinguished.** A task died in 4 s in `conda run` on one node: `PermissionError:
    '$XDG_CONFIG_HOME/conda/.condarc'`, with the variable unexpanded on that node.
    - Fix: classify very early failures in the interpreter's start-up as infrastructure, and offer a re-submit with
      `--exclude=<node>`.
14. **Hydra run-dir race** (09-29 notes, 10-01 addendum). Tasks starting in the same second share
    `outputs/<date>/<time>` in the shared code dir; one task died with `PermissionError` on `mkdir`.
    - Fix: pass a per-task `hydra.run.dir`, or give each task its own cwd, or stagger starts.
15. **Conda discovery on ssh remotes.**
    - (a) The wrapper sources `conda.sh` only from `~/miniconda3`, `~/anaconda3`, `~/.miniconda3` and `/opt/conda`, not
      from `~/miniforge3` (Athena's).
    - (b) Precedence surprise: with a per-remote `python_path` set and no per-remote `conda_env`, the run prefix was
      still `conda run -n cpvr python`. The project-level `paths.conda_env` appears to win over the remote's
      `python_path`.
    - Fix: add miniforge3/mambaforge to the search list, and document or decide the precedence (per-remote should
      win).
16. **Scientific notation in sweep YAML becomes strings.** PyYAML (YAML 1.1) parses `1e-1` and `5e-2` (no dot) as
    strings, so `params.yaml` stores `"deepr.l1": "1e-1"` in some sweeps and `0.0146` in others. Hydra copes, but every
    consumer analysis has to `float()` defensively.
    - Fix: resolve YAML 1.2-style floats when loading sweep configs, or warn.
17. **A `pre_script` at the remote level is silently ignored.** It has to live inside `spec:`.
    - Fix: validate the config and warn on unknown or misplaced keys.
18. **Task directory naming depends on the submission mode** (08-27 notes §3): `task_<n>` vs `<sweep_id>_task_<nnn>`.
19. **Paths aren't validated before submission** (09-29 notes §2): stale `project.root`/`train_script` after a repo
    move only fails late.
20. **Results arrive only when the whole sweep finishes.** The launcher collects at the end, so a 1–3-day array gives
    no local results until its last task is done. The consumer read partial results remotely instead.
    - Fix: an incremental `hsm sweep collect --finished` (one rsync with a filter), or periodic pulls by the launcher.
21. **The launcher's life is the sweep's life.** If anahita reboots or the launcher dies, ssh-backend tasks are
    orphaned; Slurm ones survive, and collect works later, as HSM's own warning says.
    - Fix: detached execution plus `hsm sweep attach <sweep_id>` to resume supervision.

### What worked well (keep it)

- **Array mode on Slurm:** one `sbatch`, polling, archiving to `/shares`, a single-rsync pull and scratch cleanup, over
  roughly 20 arrays.
- **`extra_directives`:** `--exclude` was rendered verbatim and correctly.
- **The ssh backend's slot model:** `--gpus cpu` for CPU slots; `pre_script` in `spec:` carried `PYTHONPATH` and the
  OMP threads correctly; the run was bit-identical to the same task on uzh (verified at the byte level).
- **Error and diagnostics output:**
  - `submission_summary.txt` and `parameter_combinations.json` made a live dashboard easy to build;
  - the "kept for inspection" behaviour on FAILED tasks is useful;
  - the reservation warning, even if incomplete, was useful.

## Not yet verified

- **Collection on the ssh backend:** the first real Athena sweeps finish around 8 Oct.
