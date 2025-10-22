# Project Structure - HPC Sweep Manager

## Overview

HSM uses a unified `.hsm/` folder for all configuration and state management, keeping your project organized and clean.

## Directory Layout

```
your-project/
├── .hsm/                      # HSM configuration and state
│   ├── config.yaml            # Main HSM configuration
│   ├── sync_config.yaml       # Sync targets configuration
│   ├── cache/                 # Cached metadata
│   │   ├── synced_runs.db     # Track synced runs (future)
│   │   └── sweep_metadata/    # Cached sweep configs
│   └── logs/                  # HSM operation logs
│       ├── sync_20251022.log
│       └── init_20251022.log
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

hpc:
  system: pbs
  default_walltime: "04:00:00"
  default_resources: "select=1:ncpus=4:mem=16gb"
  max_array_size: 10000

wandb:
  project: my-project
  entity: my-team
```

### `.hsm/sync_config.yaml`

Sync targets configuration. Defines remote machines for results syncing.

**Example:**
```yaml
default_target: desktop

sync_targets:
  desktop:
    host: my-desktop.example.com
    user: username
    paths:
      sweeps: /home/username/projects/my-project/sweeps
      wandb: /home/username/projects/my-project/wandb
    options:
      sync_sweep_metadata: true
      sync_wandb_runs: true
      latest_only: true
      parallel_transfers: 4
```

See [Sync Guide](SYNC_GUIDE.md) for detailed configuration options.

### `.hsm/cache/`

Stores cached metadata for faster operations:
- Sweep metadata
- Run tracking databases
- Completion status
- Error summaries

This directory can be safely deleted if needed - it will be regenerated.

### `.hsm/logs/`

HSM operation logs:
- Sync operations
- Initialization logs
- Error diagnostics

Useful for debugging issues.

## Migration from Old Structure

If you have an existing project with `sweeps/hsm_config.yaml`, simply run:

```bash
hsm init
```

This will:
1. Detect your existing configuration
2. Create the new `.hsm/` structure
3. Migrate your settings to `.hsm/config.yaml`
4. Create `.hsm/sync_config.yaml` template
5. Optionally remove the old config file

Your existing sweeps and data are **not affected** - only configuration is migrated.

## Git Recommendations

### `.gitignore`

```gitignore
# HSM cache and logs
.hsm/cache/
.hsm/logs/

# Sweep outputs and wandb runs
sweeps/outputs/
sweeps/logs/
wandb/

# But keep configuration
!.hsm/config.yaml
!.hsm/sync_config.yaml
```

### Version Control

**Do commit:**
- `.hsm/config.yaml` - Share project configuration with team
- `.hsm/sync_config.yaml` - Can be committed with placeholders, or kept local

**Don't commit:**
- `.hsm/cache/` - Generated metadata
- `.hsm/logs/` - Operation logs
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
- Output directories (in config.yaml)
- Sync destinations (in sync_config.yaml)
- Sweep configs location (anywhere, reference with `--config`)

### What if I delete `.hsm/`?

You can regenerate it by running:

```bash
hsm init
```

Your sweep outputs and wandb runs won't be affected.

### Do I need `.hsm/` on my local machine?

Not necessarily! The `.hsm/` folder is mainly for the **HPC side** where you run sweeps.

On your local analysis machine, you just need:
- `sweeps/outputs/` - Synced sweep metadata
- `wandb/` - Synced wandb runs
- Analysis scripts

However, if you want to sync **from** your local machine to other targets, then yes, create `.hsm/sync_config.yaml`.

## See Also

- [Sync Guide](SYNC_GUIDE.md) - Syncing results between machines
- [Main README](../README.md) - Overall HSM documentation

