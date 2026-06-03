<!-- Keep it short. Delete sections that don't apply. -->

## What & why


## Checklist
- [ ] Tests added/updated and `pytest tests/unit tests/cli tests/integration` is green locally
- [ ] CI is green
- [ ] `CHANGELOG.md` updated under `## [Unreleased]` (if user-facing)
- [ ] Docs touched if behavior changed (`docs/user_guide/`, `CLAUDE.md` gotchas)

## Execution-path changes (delete if N/A)
Touches job submission / result collection / terminal-state detection? If so,
note how it was validated (which fake fixtures, and any live `examples/smoke_*.sh`
run) — these are where silent bugs hide.
