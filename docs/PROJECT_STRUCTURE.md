# Project Structure - HPC Sweep Manager

## Overview

HSM uses a unified `.hsm/` folder for all configuration and state management, keeping your project organized and clean.

## Directory Layout

```
your-project/
├── .hsm/                      # HSM configuration
│   └── config.yaml            # Main HSM configuration
│
├── sweeps/                    # Sweep definitions and outputs
│   ├── outputs/               # Sweep results (generated)
│   │   └── sweep_YYYYMMDD_HHMMSS/
│   ├── logs/                  # HPC job logs (generated)
│   └── my_sweep.yaml          # Sweep configurations (user-defined)
│
├── wandb/                     # Wandb run directories (generated)
│   └── run-YYYYMMDD_HHMMSS-*/
│
└── ... (your project code)
```

## File Descriptions

### `.hsm/config.yaml`

Main HSM configuration file. Contains:
- Project metadata (name, root path)
- Python interpreter and script paths
- HPC system settings (PBS/SLURM)
- Default resource allocations
- W&B integration settings

**Example:**
```yaml
project:
  name: my-project
  root: /path/to/project

paths:
  python_interpreter: python
  train_script: scripts/train.py
  config_dir: configs
  output_dir: outputs

wandb:
  project: my-project
  entity: my-team
```

Resource defaults live in the typed `local:` / `slurm:` /
`distributed.remotes.<alias>.spec:` blocks — see
[HPC_EXECUTION.md](user_guide/HPC_EXECUTION.md#mode-scoped-config-blocks--no-cross-mode-bleed).
There is no `hpc:` block (it used to hold opaque-string `default_walltime`/
`default_resources` defaults that silently overrode the typed blocks; both
were deleted).

The bottom of the file holds two optional commented-out scaffolds —
`slurm:` (reach fields like `gpu_type`, `modules`, `qos`, `account`) and
`distributed:` (SSH remotes, populated by `hsm remote add`). Uncomment
either block to opt in.

## Migration from Old Structure

If you have an existing project with `sweeps/hsm_config.yaml`, simply run:

```bash
hsm setup init
```

This will:
1. Detect your existing configuration
2. Create the new `.hsm/` structure
3. Migrate your settings to `.hsm/config.yaml`
4. Optionally remove the old config file

Your existing sweeps and data are **not affected** - only configuration is migrated.

## Git Recommendations

### `.gitignore`

```gitignore
# Sweep outputs and wandb runs
sweeps/outputs/
sweeps/logs/
wandb/

# But keep configuration
!.hsm/config.yaml
```

### Version Control

**Do commit:**
- `.hsm/config.yaml` - Share project configuration with team

**Don't commit:**
- `sweeps/outputs/` - Large result files
- `wandb/` - Wandb run directories

## Backward Compatibility

HSM maintains backward compatibility with the old structure:

- Old location: `sweeps/hsm_config.yaml` ✅ Still works
- New location: `.hsm/config.yaml` ✅ Preferred

The config loader checks both locations, preferring the new `.hsm/` location.

## FAQ

### Why `.hsm/` instead of `sweeps/`?

1. **Clearer separation**: Configuration vs. outputs
2. **Standard practice**: Hidden folders for tools (like `.git/`, `.vscode/`)
3. **Cleaner `sweeps/`**: Now only contains user-facing sweep configs and outputs
4. **Extensibility**: Easy to add new HSM features without cluttering `sweeps/`

### Can I customize the structure?

The `.hsm/` folder structure is fixed, but you can customize:
- Output directories (in `config.yaml`)
- The typed `slurm:` block (in `config.yaml`) for Slurm reach fields
  (`gpu_type`, `modules`, `qos`, `account`, `pre_script`, ...)
- The `distributed:` block (in `config.yaml`, populated by
  `hsm remote add`) for SSH remotes
- Sweep configs location (anywhere, reference with `--config`)

### What if I delete `.hsm/`?

You can regenerate it by running:

```bash
hsm setup init
```

Your sweep outputs and wandb runs won't be affected.

### Do I need `.hsm/` on my remote box?

No. In the push-model SSH execution path the remote needs only `bash`,
`rsync`, and optionally `nvidia-smi`. HSM rsyncs the project up,
runs the tasks there, and rsyncs the results back. See
[SSH_EXECUTION.md](user_guide/SSH_EXECUTION.md).

## See Also

- [Main README](../README.md) - Overall HSM documentation
- [Getting started](user_guide/getting_started.md)
- [SSH execution](user_guide/SSH_EXECUTION.md)
- [HPC execution](user_guide/HPC_EXECUTION.md)

