# Contributing to HPC-Sweep-Manager

Small, lab-internal project — this is intentionally lightweight. The one hard
rule: **`main` stays green.** CI runs the test suite on every PR; don't merge red.

## Setup

```bash
git clone git@github.com:GabrielBena/HPC-Sweep-Manager.git
cd HPC-Sweep-Manager
pip install -e ".[dev]"     # editable install + pytest/ruff/pre-commit
pre-commit install          # ruff on commit, the no-push-to-main guard on push
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

> Use an interpreter that has the `dev` extra (`pip install -e '.[dev]'`); an env
> that only runs sweeps may not have pytest.

Live, real-hardware smoke tests live in `examples/smoke_*.sh` (run manually
against a real Slurm/SSH target; not part of CI).

## Workflow

HSM follows the PR discipline of Gabriel's research program (loom's `DISCIPLINE.md`):

1. **Branch** off `main`; never commit or push to it. The pre-push guard (`scripts/guard_push.sh`) refuses a push
   to `main`. Install it with `pre-commit install`, which sets up both hook types: ruff on commit, the guard on push.
2. **Make one change per PR, with its tests.** Unit tests go under `tests/unit/`, using the fake-conn and
   PATH-stub fixtures. The PR fills in `.github/pull_request_template.md`.
   - A PR's scope freezes when it opens: review fixes only. New work waits for the next branch.
   - **The chunk cap:** at most 150 hand-written changed lines (tests and prose excluded;
     `scripts/chunk_size.sh`). Only Gabriel's `oversize-approved` label lets a larger PR through.
3. **Open a draft PR.** Gabriel reviews it inline and submits the review. A review round goes:
   1. fix on the branch;
   2. run the gates locally;
   3. push (a round never waits for CI; only a merge does);
   4. reply on each thread and resolve it;
   5. post one round-closing digest.

   Agent-posted comments start with `🤖 **claude-code** · automated reply (via Gabriel's token, not Gabriel)`.
   The PR stays a draft until Gabriel says "mark ready".
4. **Merge** with `gh pr merge --merge`, a true merge commit (never squash or rebase), once every check is green.
   There is one open PR per lane (`slurm`, `ssh`, `cli`).
   - Anything touching an execution path (job submission, result collection, terminal-state detection) gets a
     cold review before merge. Those paths are where silent bugs hide (`docs/dev/field-reports/`).
5. **The chore lane** covers encapsulated changes that make no claim: tooling, docs, formatting, dead code,
   dependency pins.
   - A chore PR's first line reads `chore lane — self-merged on green CI`, and the author merges it the moment
     CI is green. Chores are exempt from the one-PR count.
   - Never for behaviour.

Keep `CHANGELOG.md` updated under `## [Unreleased]` for user-facing changes.

## Gates

ruff is the style SSOT. `[tool.ruff]` in `pyproject.toml` and the pinned `ruff==0.15.13` match loom's. CI's
`gates` job runs the same pre-commit hooks as a local commit, plus the chunk cap:

```bash
pre-commit run --all-files                       # ruff + ruff-format, exactly as CI runs them
scripts/chunk_size.sh origin/main 150            # this branch's hand-written lines
git config blame.ignoreRevsFile .git-blame-ignore-revs   # once per clone: blame skips the format commit
```

`.pre-commit-config.yaml`, `scripts/guard_push.sh` and `scripts/chunk_size.sh` are vendored verbatim from loom.
Bump ruff in `pyproject.toml` and `.pre-commit-config.yaml` together.

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
