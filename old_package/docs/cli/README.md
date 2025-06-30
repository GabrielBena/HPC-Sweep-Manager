# CLI Commands Reference

HSM provides a comprehensive command-line interface for managing parameter sweeps across different execution environments.

## Command Overview

### Core Commands
- [`hsm sweep`](sweep.md) - Execute parameter sweeps
- [`hsm monitor`](monitor.md) - Monitor job progress and status
- [`hsm init`](init.md) - Initialize project configuration

### Management Commands  
- [`hsm remote`](remote.md) - Manage remote machines
- [`hsm collect-results`](collect.md) - Collect and aggregate results
- [`hsm configure`](configure.md) - Manage HSM configuration

### Execution Mode Commands
- [`hsm local`](local.md) - Local execution management
- [`hsm hpc`](hpc.md) - HPC system interaction

## Quick Reference

### Common Workflows

#### Development & Testing
```bash
# Initialize project
hsm init

# Small local test
hsm sweep --mode local --parallel-jobs 2 --max-runs 5 --show-output

# Monitor progress
hsm monitor --watch
```

#### Production HPC Runs
```bash
# Large array job
hsm sweep --mode array --walltime "08:00:00" --resources "select=4:ncpus=8:mem=32gb"

# Monitor HPC queue
hsm monitor --mode hpc --watch

# Check queue status
hsm queue --watch
```

#### Remote Execution
```bash
# Setup remote machine
hsm remote add gpu-server --host "gpu.university.edu"

# Test connection
hsm remote test gpu-server

# Execute sweep
hsm sweep --mode remote --remote gpu-server --max-runs 20
```

## Global Options

All HSM commands support these global options:

- `--config-dir PATH` - Specify custom config directory
- `--verbose` - Enable verbose output
- `--quiet` - Suppress non-essential output
- `--help` - Show command help

## Configuration Precedence

HSM resolves configuration from multiple sources in this order:

1. **Command-line flags** (highest priority)
2. **Environment variables** 
3. **HSM config file** (`hsm_config.yaml`)
4. **Default values** (lowest priority)

## Environment Variables

Set these to override default behavior:

```bash
export HSM_CONFIG_DIR="./configs"
export HSM_VERBOSE=true
export HSM_DEFAULT_MODE="local"
export HSM_MAX_PARALLEL_JOBS=4
```

## Exit Codes

HSM commands return standardized exit codes:

- `0` - Success
- `1` - General error
- `2` - Configuration error
- `3` - Connection/network error  
- `4` - Permission/authentication error
- `5` - Resource not found
- `130` - Interrupted by user (Ctrl+C)

## Error Handling

HSM provides clear error messages and suggestions:

```bash
$ hsm sweep --mode invalid
Error: Invalid execution mode 'invalid'
Available modes: local, array, individual, remote

Suggestion: Use 'hsm sweep --help' to see available options
```

## Logging

HSM logs are written to:
- **Console**: Real-time status and errors
- **Log files**: Detailed execution logs in sweep directories
- **HSM log**: Global HSM operations in `~/.hsm/hsm.log`

Control logging verbosity:
```bash
hsm sweep --verbose      # Detailed output
hsm sweep --quiet        # Minimal output  
hsm sweep --debug        # Debug-level logging
```

## Auto-completion

Enable bash completion for HSM commands:

```bash
# Add to ~/.bashrc
eval "$(_HSM_COMPLETE=bash_source hsm)"

# Or for zsh, add to ~/.zshrc  
eval "$(_HSM_COMPLETE=zsh_source hsm)"
```

## Configuration Files

HSM uses YAML configuration files:

### Sweep Configuration (`sweeps/sweep.yaml`)
```yaml
sweep:
  grid:
    lr: [0.001, 0.01, 0.1]
    batch_size: [16, 32, 64]
```

### HSM Configuration (`sweeps/hsm_config.yaml`)
```yaml
paths:
  python_interpreter: /opt/conda/bin/python
  train_script: ./train.py
  
hpc:
  default_walltime: "04:00:00"
  default_resources: "select=1:ncpus=4:mem=16gb"
```

## Debugging Tips

### Dry Run Mode
Test commands without execution:
```bash
hsm sweep --dry-run    # Show what would be executed
```

### Verbose Output
Get detailed information:
```bash
hsm sweep --verbose    # Detailed execution info
hsm remote test --verbose  # Connection diagnostics
```

### Log Files
Check logs for detailed error information:
```bash
# Global HSM log
cat ~/.hsm/hsm.log

# Sweep-specific logs
cat sweeps/outputs/sweep_*/logs/*.log
```

### Configuration Validation
Verify your configuration:
```bash
hsm configure validate    # Check config syntax
hsm init --dry-run       # Validate auto-detected paths
``` 