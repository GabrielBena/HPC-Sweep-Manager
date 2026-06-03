# Contributing to HPC-Sweep-Manager

Small, lab-internal project — this is intentionally lightweight. The one hard
rule: **`main` stays green.** CI runs the test suite on every PR; don't merge red.

## Setup

```bash
git clone git@github.com:GabrielBena/HPC-Sweep-Manager.git
cd HPC-Sweep-Manager
pip install -e ".[dev]"     # editable install + pytest/ruff/mypy
```

Pick one conda env name per project and create it with that exact name on every
machine you run on — HSM activates it from `paths.conda_env`, not shell state
(see CLAUDE.md gotcha #10).

## Running tests

```bash
pytest tests/unit tests/cli tests/integration
```

Fully hermetic — PATH-stub fakes stand in for `sbatch`/`squeue`/`sacct`/
`nvidia-smi` and an SSH connection, so no real cluster is needed. This is the
same suite CI runs.

> **On anahita:** the repo dir auto-activates the `hsm` conda env, which has no
> pytest. Use the base interpreter: `/home/gbena/miniconda3/bin/python -m pytest …`.

Live, real-hardware smoke tests live in `examples/smoke_*.sh` (run manually
against a real Slurm/SSH target; not part of CI).

## Workflow

1. Branch off `main` (`git switch -c your-feature`).
2. Make the change **with tests** — unit tests under `tests/unit/` using the
   fake-conn / PATH-stub fixtures in `tests/conftest.py`.
3. Open a PR. CI must pass.
4. Merge: self-merge is fine for small/obvious changes; for anything touching
   the execution paths (job submission, result collection, terminal-state
   detection) get a second pair of eyes — those are where silent bugs hide
   (see the field report under `docs/dev/field-reports/`).

Keep `CHANGELOG.md` updated under `## [Unreleased]` for user-facing changes.

## Style

`ruff` and `mypy` are configured (`pip install -e ".[dev]"` pulls them in):

```bash
ruff check src/ tests/      # lint
ruff format src/ tests/     # format
```

Not yet enforced in CI (the codebase predates the config and isn't clean — a
cleanup PR is welcome before we turn the gate on).

## Where things live

- **Architecture, gotchas, "do not reintroduce":** `CLAUDE.md` (read it first).
- **User recipes:** `docs/user_guide/` (SSH / HPC / multi-cluster execution).
- **Design rationale:** `ARCHITECTURE.md`.
- **Field reports** (real first-use accounts — genuinely useful, please add
  yours): `docs/dev/field-reports/`.

## Reporting bugs / ideas

Open a GitHub issue (templates provided). For execution bugs, include the
backend (`local` / `array` / `remote` / SSH-Slurm), the command, and the
relevant `tasks/*/task_info.txt` + `logs/*.err`.
