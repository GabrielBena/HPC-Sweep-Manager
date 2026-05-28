# HSM documentation

The repo root has the quickstart ([README.md](../README.md)) and design
docs ([CLAUDE.md](../CLAUDE.md), [ARCHITECTURE.md](../ARCHITECTURE.md)).
The rest of the docs live here.

## User guides

- **[Getting started](user_guide/getting_started.md)** — install, `hsm setup init`, sweep config, first run. The broad tutorial.
- **[SSH (push-model) execution](user_guide/SSH_EXECUTION.md)** — `hsm sweep run --remote <alias>` in depth: GPU pinning, conda env handling, rsync excludes, `hsm remote clean`.
- **[HPC (Slurm / PBS) execution](user_guide/HPC_EXECUTION.md)** — `--mode array|individual|auto` recipe, the `--resources` string format, and the typed `slurm:` block in `.hsm/config.yaml` for advanced fields.

## Reference

- **[CLI reference](cli/README.md)** — every `hsm` command and subcommand.
- **[API reference](api_reference/README.md)** — Python entry points if you want to embed HSM in your own scripts. See [`compute_sources.md`](api_reference/compute_sources.md) for the live tier; the `job_manager.md` / `hpc/` / `local/` subtrees are legacy (banners explain).
- **[Project structure](PROJECT_STRUCTURE.md)** — what `.hsm/` contains and how HSM finds your config + scripts.

## Design

- **[../ARCHITECTURE.md](../ARCHITECTURE.md)** — why `ComputeSource`, push-SSH lifecycle, slot back-pressure, known limitations.
- **[../CLAUDE.md](../CLAUDE.md)** — agent on-boarding: do-not-reintroduce list, gotchas-already-patched, test architecture.
