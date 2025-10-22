# Sync Guide - HPC Sweep Manager

Guide for syncing sweep results and wandb runs from HPC to local/remote machines.

## Quick Start

### 1. Initialize Sync Configuration

On your HPC machine, in your project directory:

```bash
hsm sync init
```

This creates `.hsm/sync_config.yaml` with a template. Edit it to add your target machines.

### 2. Configure Sync Targets

Edit `.hsm/sync_config.yaml`:

```yaml
default_target: desktop

sync_targets:
  desktop:
    host: ee-lw1115.dept.ic.ac.uk
    user: gb21
    paths:
      sweeps: /home/gb21/code/Encoded-CVPR/sweeps
      wandb: /home/gb21/code/Encoded-CVPR/wandb
      project_root: /home/gb21/code/Encoded-CVPR
    options:
      sync_sweep_metadata: true
      sync_wandb_runs: true
      latest_only: true
      max_depth: 1
      parallel_transfers: 4
      check_remote_first: true
```

### 3. Sync Your Sweep

After your sweep completes:

```bash
# Sync everything (sweep metadata + wandb runs)
hsm sync run sweep_20251020_212836 --target desktop

# Or use the short form
hsm sync to sweep_20251020_212836 --target desktop
```

## Detailed Usage

### List Configured Targets

```bash
hsm sync list
```

Shows all configured sync targets and their settings.

### Sync Options

#### Basic Sync
```bash
# Sync to default target (specified in config)
hsm sync to sweep_20251020_212836

# Sync to specific target
hsm sync to sweep_20251020_212836 --target desktop

# Sync to multiple targets
hsm sync to sweep_20251020_212836 --target desktop --target laptop
```

#### Dry Run
```bash
# See what would be synced without actually transferring
hsm sync to sweep_20251020_212836 --target desktop --dry-run
```

#### Selective Sync
```bash
# Only sync sweep metadata (config, logs), not wandb runs
hsm sync to sweep_20251020_212836 --target desktop --sweep-only

# Only sync wandb runs, not sweep metadata
hsm sync to sweep_20251020_212836 --target desktop --wandb-only

# Sync all artifact versions (not just latest)
hsm sync to sweep_20251020_212836 --target desktop --all-versions
```

## Configuration Reference

### Sync Target Configuration

```yaml
target_name:
  host: hostname.example.com          # Remote hostname
  user: username                       # SSH username
  paths:
    sweeps: /path/to/sweeps           # Required: sweep outputs destination
    wandb: /path/to/wandb             # Required: wandb runs destination
    project_root: /path/to/project    # Optional: project root
  options:
    sync_sweep_metadata: true          # Sync sweep config and logs
    sync_wandb_runs: true              # Sync wandb run directories
    latest_only: true                  # Only sync latest artifact versions
    max_depth: 1                       # Directory depth for sweep metadata
    parallel_transfers: 4              # Number of parallel rsync transfers
    check_remote_first: true           # Check what exists before syncing
```

### Option Details

- **sync_sweep_metadata**: Controls whether sweep configuration and metadata are synced
- **sync_wandb_runs**: Controls whether wandb run directories are synced
- **latest_only**: When true, skips old versions of artifacts (e.g., `model:v1`, `model:v2` → only syncs `model:v3`)
- **max_depth**: Limits directory traversal depth when syncing sweep metadata
- **parallel_transfers**: Number of concurrent rsync operations for wandb runs (speeds up syncing many runs)
- **check_remote_first**: SSH to remote and check which runs already exist before syncing (avoids re-syncing)

## Workflow Examples

### Complete Workflow from HPC to Local

On HPC:
```bash
# 1. Run your sweep
hsm sweep run --config sweeps/my_sweep.yaml --mode array

# 2. Monitor progress
hsm monitor sweep_20251020_212836

# 3. Once complete, sync results
hsm sync to sweep_20251020_212836 --target desktop
```

On Local Machine (e.g., ee-lw1115):
```bash
# 4. Process results
python scripts/process_results.py --sweep-id sweep_20251020_212836 --num-workers 8

# 5. Analyze
python scripts/analyze_results.py --sweep-id sweep_20251020_212836
```

### Syncing Large Sweeps Efficiently

For sweeps with many runs, use parallel transfers and incremental syncing:

```yaml
options:
  parallel_transfers: 8              # Sync 8 runs simultaneously
  check_remote_first: true           # Skip already-synced runs
  latest_only: true                  # Skip old artifact versions
```

```bash
# First sync (may take time)
hsm sync to sweep_20251020_212836 --target desktop

# Later, if you re-run some jobs and need to sync updates
hsm sync to sweep_20251020_212836 --target desktop
# Only new/updated runs will be transferred!
```

### Syncing to Multiple Locations

```yaml
default_target: desktop

sync_targets:
  desktop:
    host: ee-lw1115.dept.ic.ac.uk
    user: gb21
    paths:
      sweeps: /home/gb21/code/Encoded-CVPR/sweeps
      wandb: /home/gb21/code/Encoded-CVPR/wandb
    options:
      sync_sweep_metadata: true
      sync_wandb_runs: true
      latest_only: true

  backup:
    host: backup.example.com
    user: backup_user
    paths:
      sweeps: /mnt/backup/sweeps
      wandb: /mnt/backup/wandb
    options:
      sync_sweep_metadata: true
      sync_wandb_runs: true
      latest_only: false               # Keep all versions for backup
```

```bash
# Sync to both desktop and backup
hsm sync to sweep_20251020_212836 --target desktop --target backup
```

## Migration from Old Workflow

### Old Workflow (Manual)
```bash
# On HPC
rsync_to_desktop sweeps/outputs/sweep_20251020_212836 -d 1 -p /home/gb21/code/Encoded-CVPR/

python scripts/sync_wandb_runs.py \
  --sweep-id sweep_20251020_212836 \
  --local-host gb21@ee-lw1115.dept.ic.ac.uk \
  --local-path /home/gb21/code/Encoded-CVPR/wandb \
  --latest-only \
  --ignore-date
```

### New Workflow (HSM)
```bash
# On HPC (one command!)
hsm sync to sweep_20251020_212836 --target desktop
```

### Benefits
- ✅ **Single command** instead of two
- ✅ **Configuration-based** - no remembering CLI flags
- ✅ **Incremental sync** - checks what's already on remote
- ✅ **Parallel transfers** - faster for many runs
- ✅ **Multi-target** - sync to multiple machines at once
- ✅ **Integrated** - part of HSM workflow

## SSH Setup

For passwordless syncing, set up SSH keys:

```bash
# On HPC, generate key if you don't have one
ssh-keygen -t ed25519

# Copy to your target machine
ssh-copy-id gb21@ee-lw1115.dept.ic.ac.uk

# Test connection
ssh gb21@ee-lw1115.dept.ic.ac.uk "echo 'Connection successful'"
```

## Troubleshooting

### "No sync configuration found"

```bash
# Create configuration
hsm sync init

# Edit .hsm/sync_config.yaml with your targets
```

### "Unknown sync target 'desktop'"

Check your target name matches what's in `.hsm/sync_config.yaml`:

```bash
hsm sync list  # Shows configured targets
```

### Slow syncing with many runs

Increase parallel transfers in `.hsm/sync_config.yaml`:

```yaml
options:
  parallel_transfers: 8  # Increase from default 4
```

### Re-syncing already transferred runs

Enable remote checking:

```yaml
options:
  check_remote_first: true  # Only sync new/changed runs
```

### Permission denied errors

Check SSH access:

```bash
ssh user@hostname "ls -la /path/to/destination"
```

## Advanced Usage

### Custom Sync Paths per Sweep

You can override sync paths on a per-sweep basis by modifying the sync configuration temporarily or using environment variables (feature to be added).

### Watching for New Runs (Future Feature)

```bash
# Automatically sync new runs as they complete
hsm sync to sweep_20251020_212836 --target desktop --continuous
```

This feature will be added in a future release.

## See Also

- [Main README](../README.md) - Overall HSM documentation
- [Quick Start Guide](QUICKSTART.md) - Getting started with HSM
- Project Structure - Understanding `.hsm/` folder organization

