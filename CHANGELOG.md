# Changelog

All notable changes to HPC-Sweep-Manager are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/); this project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

Two field reports drove this cycle: SSH-Slurm → S3IT first use
([`2026-06-02-s3it-first-use.md`](docs/dev/field-reports/2026-06-02-s3it-first-use.md))
and the first blind agent-driven consumer run from Comp-PVR
([`2026-06-03-comp-pvr-first-run.md`](docs/dev/field-reports/2026-06-03-comp-pvr-first-run.md)).

### Fixed (queue inspection audit, 2026-06-04)

Live audit against S3IT (Slurm 25.05) with two sweeps in flight found
`hsm queue` blind on real clusters:

- **GPU parsing missed the colon-count GRES grammar.** `squeue %b` emits
  `gres/gpu:A100:1` / `gres/gpu:3` on Slurm 25.05; the parser only knew the
  `=`-count accounting style (`gres/gpu:h100=1`), so every job parsed as
  0 GPUs — `gpus` reported an empty GPU queue, `position` denied pending GPU
  jobs existed, `mine`'s GPU column was blank. Both grammars are accepted
  now, with the live cluster census as test fixtures. Also corrected the
  field's semantics: `%b` is TRES per **node** (`QueueJob.tres_per_node`).
- **Pending arrays counted as 1 task.** A pending array is one squeue row
  (`123_[690-1920%4]` = 1231 tasks). Counting paths (`position`, `gpus`)
  now query with `squeue -r` so totals/positions count tasks; `mine` keeps
  the compact row and shows a `×N` Tasks column.
- **`position` silently hid CPU-only pending jobs** — now surfaced as a
  note; the reason-code legend covers `(QOSMaxJobsPerUserLimit)`.

### Added (heterogeneous GPU-type scheduling, issue #7 v0+v1, 2026-06-04)

- **`spec.gpu_type` accepts a list** (array mode): the sweep splits into one
  Slurm array per type via greedy LPT — costliest tasks first, each to the
  type minimizing `(load + cost) × factor`. With uniform costs this
  degenerates to counts ∝ 1/factor. Pure planner shared by the local and
  SSH-Slurm sources (`core/hpc/gpu_planner.py`); a multi-type spec reaching
  `render_sbatch_directives` raises (unplanned-path guard). Singleton lists
  degenerate to the plain scalar path.
- **`speed_factors`** (per-remote key, or in the `slurm:` block): GPU type →
  relative runtime multiplier; per-sub-array walltime = base × factor ×
  (bin max cost / global max cost), ceiled to the minute, floored at 10 min,
  uncapped. Workload-specific — measure, don't trust spec sheets; houses in
  the key a future `hsm calibrate` will write.
- **Per-task cost hints in the sweep YAML**: `cost_param` names a swept
  param; optional `cost_map` translates values to measured costs (ratios
  matter). Unusable costs default to 1.0 with a loud warning. Costs never
  enter the hydra override string.
- **`--dry-run` shows the exact split plan** (same planner call as
  submission): type / factor / tasks / Σcost / max cost / scaled walltime.
- Task layout unchanged (`tasks/task_%04d` stays globally numbered via
  per-sub-array params files with original `global_index`); `task_info.txt`
  records `GPU Type:` per task (arch-confound flag); manifest gains per-job
  `jobs:` entries so `hsm queue mine` shows per-type sub-arrays with correct
  per-array progress.

### Changed (`hsm queue gpus` capacity view, 2026-06-04)

- **`hsm queue gpus` shows Total / In use / Free per GPU type**, from
  `sinfo`'s per-node allocation accounting (`Gres`/`GresUsed`) — which also
  attributes GPUs consumed by *untyped* job requests to their physical
  type, making Free exact rather than an estimate. Down/drained nodes are
  excluded from totals (disclosed in a footer); idle types with no queue
  demand are now visible. sinfo is optional enrichment with the same
  None-on-failure contract as sacct — clusters without it degrade to the
  queue-only table with a note. Parser handles the live S3IT trap of
  commas inside `GresUsed` index decorations (`gpu:A100:6(IDX:0-1,4-7)`).
- **`--mine` is now the default** on `gpus` (`--no-mine` to hide) — there
  was no good reason to hide your own footprint.
- **VRAM/GPU column**: cluster-reported via `GPUMEM<N>GB` node-feature
  tags when available (mixed node groups list every variant — live S3IT
  H100s really are `80/96G`); model-typical fallback marked with `~` and
  restricted to unambiguous models; `?` otherwise — never a confident
  guess. An automatic legend explains the `<untyped>` demand row.

### Changed (grouped `hsm queue mine`, 2026-06-04)

- **`hsm queue mine` groups by array by default.** Mid-sweep, every running
  array task is its own squeue row — the old view printed hundreds of
  near-identical lines. Now: one row per array with `▶ running ⏳ pending`
  (squeue, live, task-weighted) plus `✓ completed ✗ failed` and a
  progress-bar-out-of-true-total from **sacct accounting** — tasks that
  already left the queue (including failures) were previously invisible in
  every queue view. sacct is optional enrichment: clusters without
  accounting degrade to sweep-metadata totals or `—`, never a guessed
  number, and a missing sacct never errors (asymmetric with squeue, which
  stays loud-on-failure). `--flat` restores the per-task rows. Failed-task
  counts are surfaced in red per-row and in the footer.

### Added (queue inspection from the driving workstation, 2026-06-04)

- **`--remote <alias>` on all four `hsm queue` subcommands** — runs the same
  queries over SSH (registered `backend: slurm` remote or bare `~/.ssh/config`
  alias). New `SSHSlurmQueue` async twin shares the pure command
  builders/parsers with the local `SlurmQueue` (the `slurm_protocol.py`
  anti-drift idiom); failures raise instead of rendering an empty table.
- **Auto-fallback:** with no local `squeue` and exactly one slurm-backend
  remote registered, `hsm queue` uses it automatically (note printed).
- **`--watch [--refresh N]` on `mine` and `gpus`** — live-refreshing view;
  one SSH connection reused across cycles.
- **Sweep linkage for SSH-Slurm sweeps:** `mine` resolves job→sweep from
  `.hsm_manifest.json` too (such sweeps have no `submission_summary.txt`),
  so the from-HQ view shows which sweep each job belongs to.

### Fixed (Comp-PVR consumer report, 2026-06-03)
- **Array tasks silently ran the default config under `conda run`.** The
  per-task params extraction fed Python over stdin (`python - <<heredoc`);
  `conda run` doesn't forward heredoc stdin inside `$()` on some clusters, so
  tasks got zero overrides and reported SUCCESS. The snippet now runs from a
  tempfile by path, and an empty extraction fails fast instead of training
  the default config.
- **`configs/wandb/` was silently stripped from the push.** Output-dir rsync
  excludes are now anchored to the project root (`/wandb`, `/checkpoints`,
  `/multirun`) so they can't shadow a same-named Hydra config group
  (`MissingConfigException` on every task, with no per-remote workaround).
- **`hsm setup init` reported failure after succeeding** (crash after all
  files were written). Also hardened: exits non-zero on *real* failure
  (previously exited 0), re-runs back up `.hsm/config.yaml` to
  `.hsm/config.yaml.bak` before regenerating, and non-interactive runs never
  prompt (migration path included).

### Added (Comp-PVR consumer report, 2026-06-03)
- `hsm init` — top-level alias for `hsm setup init`.
- The init re-run/overwrite contract is documented in `hsm setup init --help`,
  README, and getting_started.md.
- SSH_EXECUTION.md: "Your project's own package on the remote" — editable
  installs don't import from the rsynced tree; `pip install -e` into the
  remote env or a `PYTHONPATH` `pre_script`.
- `hsm docs` prints URLs unwrapped (copy-pasteable at any width) and notes
  that `main` is the canonical branch.

### Added
- `hsm docs` — prints the documentation URLs (+ local path on a source checkout),
  so consumers/agents can find the docs without spelunking the installed package.
- `hsm setup init` now writes a **self-contained** `sweeps/README.md` (execution
  modes, output layout, failure/recovery semantics, config knobs) instead of
  pointing at `docs/user_guide/*` paths that aren't shipped with `pip install`.
  It can also create an `AGENTS.md` pointer for coding agents — only on explicit
  interactive opt-in, and it never modifies an existing `AGENTS.md`/`CLAUDE.md`.
- `hsm sweep collect <sweep_id>` — re-attach to an SSH-Slurm sweep whose
  launching process died (long run / overnight / maintenance window) and
  pull + archive whatever's terminal, via a submit-time `.hsm_manifest.json`.
  Idempotent; classifies jobs with `squeue` then `sacct`.
- Continuous mid-flight result pull: completed task dirs are rsync'd back as
  soon as each task finishes, so one stuck task can't strand the rest.
- Self-describing checkpoints: every `tasks/<t>/` now contains a `params.yaml`
  with that task's exact overrides.
- Array submission over SSH-Slurm: `--remote <alias> --mode array` packs one
  `sbatch --array` instead of one job per combo.
- Maintenance-reservation warning at submit (`scontrol show reservations`).

### Fixed
- **FAILED Slurm jobs were reported COMPLETED.** Terminal state now comes from
  `sacct` once a job leaves `squeue` (queue-absence is not success); `hsm sweep
  run` exits non-zero on any failure and lists failing task dirs.
- **Module-provided conda was silently shadowed → jobs trained on CPU.** Rendered
  scripts now run `module load` / `pre_script` before the conda-init probe, which
  defers to a real conda on PATH instead of a micromamba bridge.
- `$USER` / `$HOME` / `~` are now expanded in remote `workdir` / `archive_dir` /
  `remote_root` (rsync got a literal `$USER` dir before).
- `--dry-run` for `--remote` now shows the merged per-remote `spec:` (gres /
  account / qos), not an empty preview.
- `hsm setup init` no longer mis-detects PBS on a box with no local scheduler
  (defaults to `unknown`; checks Slurm → SGE → PBS in order).

### Changed
- `DEFAULT_RSYNC_EXCLUDES` adds `*.pth` / `/checkpoints` / `/multirun` /
  `.hydra` (dir names root-anchored — see Fixed above); per-remote
  `rsync_excludes` now **extends** the defaults instead of replacing them
  (so adding `outputs/` can't accidentally start pushing `.git`).

## [0.1.0]
- Initial release: `local` / `array` / `individual` / `remote` / `distributed`
  execution over a unified `ComputeSource` API; SSH push-model + SSH-Slurm
  backends; `local.sweeps_root` redirection; `workdir` / `archive_dir` storage
  tiers.
