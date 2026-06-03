# Changelog

All notable changes to HPC-Sweep-Manager are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/); this project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

First-use field report from driving SSH-Slurm → S3IT (UZH). Full account:
[`docs/dev/field-reports/2026-06-02-s3it-first-use.md`](docs/dev/field-reports/2026-06-02-s3it-first-use.md).

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
- `DEFAULT_RSYNC_EXCLUDES` adds `*.pth` / `checkpoints/` / `multirun/` /
  `.hydra/`; per-remote `rsync_excludes` now **extends** the defaults instead of
  replacing them (so adding `outputs/` can't accidentally start pushing `.git`).

## [0.1.0]
- Initial release: `local` / `array` / `individual` / `remote` / `distributed`
  execution over a unified `ComputeSource` API; SSH push-model + SSH-Slurm
  backends; `local.sweeps_root` redirection; `workdir` / `archive_dir` storage
  tiers.
