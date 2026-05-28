# Examples

Runnable references for HSM. Use these to verify your setup, and as
starting points for your own integration.

## Files

| File | What it is | When to use |
|---|---|---|
| [`test_train.py`](test_train.py) | Canonical training-script contract. Parses Hydra-style `key=value` args, honors `output.dir`, writes a sentinel, optional `_should_fail=true` for testing the FAILED path. Stdlib-only. | Copy as a starting point for your own train script; or use directly as a smoke target. |
| [`test_sweep.yaml`](test_sweep.yaml) | Minimal sweep config — small grid over `lr` and `batch_size`. | Quick local test of `hsm sweep run --config test_sweep.yaml --mode local`. |
| [`smoke_cli.sh`](smoke_cli.sh) | End-to-end smoke test for `--mode array` on a real Slurm cluster. Self-bootstrapping: writes a throwaway project and submits a 4-task array. | Validate HSM works on your HPC after install. |
| [`smoke_ssh_cli.sh`](smoke_ssh_cli.sh) | End-to-end smoke test for `--remote <alias>` against a real SSH box. Self-bootstrapping. **Requires `REMOTE=<your-alias>` env var.** | Validate the push-SSH path against your lab box / shared GPU server. |
| [`smoke_slurm.py`](smoke_slurm.py) | Direct-Python entry point — drives `SlurmComputeSource` with a fully typed `ResourceSpec`. | Use when the CLI `--resources` string can't express what you need (`gpu_type`, `modules`, `pre_script`, `account`) — see [HPC_EXECUTION.md](../docs/user_guide/HPC_EXECUTION.md#known-limitation--what---resources-cant-express). |

## Quick local test

```bash
cd <a fresh project dir containing your train.py>
hsm setup init
hsm sweep run --mode local --config sweeps/example_sweep.yaml --max-runs 4 --show-output
```

## Quick SSH smoke

```bash
REMOTE=my-box CONDA_ENV=my-env bash examples/smoke_ssh_cli.sh --dry-only   # safe; no submission
REMOTE=my-box CONDA_ENV=my-env bash examples/smoke_ssh_cli.sh              # real 2-task push
```

Replace `my-box` with your `~/.ssh/config` alias.

## Quick Slurm smoke

On a Slurm login node:

```bash
bash examples/smoke_cli.sh --dry-only         # dry-runs only
bash examples/smoke_cli.sh                    # real 4-task array submission
```

## See also

- [Getting started](../docs/user_guide/getting_started.md)
- [SSH execution recipe](../docs/user_guide/SSH_EXECUTION.md)
- [HPC execution recipe](../docs/user_guide/HPC_EXECUTION.md)
- [Compute sources API](../docs/api_reference/compute_sources.md)
- [Architecture](../ARCHITECTURE.md)
