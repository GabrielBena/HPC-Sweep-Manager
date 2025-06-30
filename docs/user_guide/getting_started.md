# Getting Started with HPC Sweep Manager v2

This guide walks you through the complete workflow of using HPC Sweep Manager v2 from project setup to sweep completion and monitoring.

## Overview

HPC Sweep Manager v2 provides a unified interface for running hyperparameter sweeps across different compute environments:
- **Local execution** - Run sweeps on your local machine
- **SSH remote execution** - Run sweeps on remote machines via SSH
- **HPC cluster execution** - Run sweeps on PBS/Torque or Slurm clusters
- **Cross-mode completion** - Start in one mode, complete in another

## Prerequisites

- Python 3.8+ with conda/mamba environment management
- For SSH execution: SSH access to remote machines
- For HPC execution: Access to PBS/Torque or Slurm clusters

## Installation

```bash
# Clone the repository
git clone https://github.com/your-org/HPC-Sweep-Manager.git
cd HPC-Sweep-Manager

# Create and activate conda environment
conda create -n hsm python=3.9
conda activate hsm

# Install HSM v2
pip install -e src/
```

## Quick Start

### 1. Project Initialization

Navigate to your project directory and initialize HSM:

```bash
cd /path/to/your/project
hsm config init --interactive
```

This will:
- **Auto-detect** your project structure (training scripts, Python interpreter, conda environment)
- **Detect HPC system** if available (PBS/Torque, Slurm, or local)
- **Create configuration** through interactive prompts
- **Set up sweep infrastructure** with example files

**Example output:**
```
╭───────────────────────────────────────────────╮
│ HPC Sweep Manager v2 - Project Initialization │
╰───────────────────────────────────────────────╯
Initializing project at: /path/to/your/project

Scanning project structure...
                    Detected Project Information                    
┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Component          ┃ Status       ┃ Path/Value                     ┃
┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Project Name       │ ✅ Detected  │ my_project                     │
│ Training Script    │ ✅ Found     │ /path/to/train.py              │
│ Python Interpreter │ ✅ Found     │ /path/to/conda/envs/myenv/bin/python │
│ Conda Environment  │ ✅ Active    │ myenv                          │
│ HPC System         │ ✅ Detected  │ SLURM                          │
└────────────────────┴──────────────┴────────────────────────────────┘
```

**Files created:**
- `hsm_config.yaml` - HSM system configuration
- `sweeps/sweep_config.yaml` - Example sweep configuration
- `sweeps/outputs/` - Directory for sweep results
- `sweeps/README.md` - Usage instructions

### 2. Configure Your Sweep

Edit the generated `sweeps/sweep_config.yaml` to define your hyperparameter search:

```yaml
# sweeps/sweep_config.yaml
grid:
  learning_rate: [0.001, 0.01, 0.1]
  batch_size: [32, 64, 128]
  hidden_dim: [64, 128, 256]

metadata:
  description: "Hyperparameter search for my model"
  tags: ["baseline", "grid_search"]
```

This creates a grid search with 3 × 3 × 3 = 27 parameter combinations.

### 3. Test Your Configuration

Before running the full sweep, test your configuration:

```bash
# Count total combinations
hsm run --config sweeps/sweep_config.yaml --count-only

# Preview what would be executed
hsm run --config sweeps/sweep_config.yaml --dry-run
```

**Example output:**
```
Total parameter combinations: 27

Dry Run Information
   Parameter Combinations   
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━┓
┃ Parameter     ┃ Values   ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━┩
│ learning_rate │ 3 values │
│ batch_size    │ 3 values │
│ hidden_dim    │ 3 values │
└───────────────┴──────────┘

Execution Plan:
  Total combinations: 27
  Tasks to execute: 27
  Tasks per source: ~27
```

### 4. Run Your Sweep

Choose your execution method:

#### Local Execution
```bash
# Run on local machine (4 parallel jobs by default)
hsm run --config sweeps/sweep_config.yaml --sources local

# Limit parallel jobs
hsm run --config sweeps/sweep_config.yaml --sources local --parallel-limit 2
```

#### HPC Cluster Execution
```bash
# Run on detected HPC system (auto-detects PBS/Slurm)
hsm run --config sweeps/sweep_config.yaml --sources hpc

# Specify resource requirements
hsm run --config sweeps/sweep_config.yaml --sources hpc:cluster_name
```

#### SSH Remote Execution
```bash
# Run on remote machine via SSH
hsm run --config sweeps/sweep_config.yaml --sources ssh:hostname

# Multiple sources (distributed execution)
hsm run --config sweeps/sweep_config.yaml --sources local,ssh:remote1,hpc:cluster
```

### 5. Monitor Progress

While your sweep is running, monitor progress in real-time:

```bash
# Watch specific sweep
hsm monitor watch <sweep_id>

# Watch all active sweeps
hsm monitor watch

# Show recent sweeps
hsm monitor recent

# Show sweep errors
hsm monitor errors <sweep_id>
```

**Example monitoring output:**
```
Sweep Status: sweep_20231201_143022
┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Metric    ┃ Value                      ┃
┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Status    │ Running                    │
│ Progress  │ 15/27 (55.6%)              │
│ Completed │ 12                         │
│ Failed    │ 0                          │
│ Running   │ 3                          │
│ Pending   │ 12                         │
│ Duration  │ 2h 15m                     │
│ ETA       │ 1h 30m                     │
└───────────┴────────────────────────────┘
```

### 6. Complete Partial Sweeps

If a sweep was interrupted or you want to complete it with different resources:

```bash
# Complete missing tasks using local execution
hsm complete <sweep_id> --sources local

# Complete using different compute source
hsm complete <sweep_id> --sources hpc

# Retry failed tasks as well
hsm complete <sweep_id> --sources local --retry-failed
```

### 7. Analyze Results

Results are organized in a standardized directory structure:

```
sweeps/outputs/<sweep_id>/
├── sweep_config.yaml          # Original sweep configuration
├── sweep_metadata.yaml        # Sweep metadata and timing
├── task_mapping.yaml          # Task assignments and status
├── tasks/                     # Individual task results
│   ├── task_001/
│   │   ├── params.yaml        # Task parameters
│   │   ├── stdout.log         # Standard output
│   │   ├── stderr.log         # Error output
│   │   └── results/           # Task output files
│   └── task_002/
└── logs/                      # System logs
    ├── sweep.log              # Main sweep log
    └── sources/               # Per-source logs
```

## Configuration Management

### View Current Configuration
```bash
# Display HSM configuration
hsm config show

# Validate configuration files
hsm config validate

# Validate specific files
hsm config validate --config hsm_config.yaml --sweep-config sweeps/sweep_config.yaml
```

### Update Configuration
```bash
# Re-run initialization to update settings
hsm config init --interactive --force

# Edit configuration files directly
vim hsm_config.yaml
vim sweeps/sweep_config.yaml
```

## Advanced Features

### Multiple Compute Sources
Distribute work across multiple compute sources:

```bash
hsm run --config sweeps/sweep_config.yaml --sources local,ssh:remote1,ssh:remote2,hpc:cluster
```

### Parameter Strategies

#### Grid Search
```yaml
grid:
  param1: [1, 2, 3]
  param2: [a, b, c]
# Creates all combinations: 3 × 3 = 9 total
```

#### Paired Parameters
```yaml
paired:
  - group1:
      param1: [1, 2, 3]
      param2: [a, b, c]
# Creates paired combinations: (1,a), (2,b), (3,c) = 3 total
```

#### Mixed Strategy
```yaml
grid:
  learning_rate: [0.001, 0.01]
  
paired:
  - model_config:
      hidden_dim: [64, 128, 256]
      num_layers: [2, 3, 4]
      
# Grid: 2 learning rates
# Paired: 3 model configurations  
# Total: 2 × 3 = 6 combinations
```

### Resource Limits
```bash
# Limit total tasks
hsm run --config sweeps/sweep_config.yaml --max-tasks 10

# Limit parallel execution per source
hsm run --config sweeps/sweep_config.yaml --parallel-limit 4

# Custom timeout (default: no timeout)
# Edit hsm_config.yaml: default_timeout: 3600  # seconds
```

## Troubleshooting

### Common Issues

1. **"No training script found"**
   ```bash
   # Manually specify training script in hsm_config.yaml
   paths:
     training_script: /path/to/your/train.py
   ```

2. **"No HPC system detected"**
   - Install PBS/Torque (`qsub`, `qstat`) or Slurm (`sbatch`, `squeue`) tools
   - Use `--sources local` for local-only execution

3. **"SSH connection failed"**
   ```bash
   # Test SSH connection manually
   ssh username@hostname
   
   # Check SSH key authentication
   ssh-add ~/.ssh/id_rsa
   ```

4. **Tasks failing with import errors**
   ```bash
   # Check conda environment is properly specified
   hsm config show
   
   # Ensure training script has correct imports
   # Check stderr logs in task directories
   hsm monitor errors <sweep_id>
   ```

### Validation Commands
```bash
# Check system environment
hsm config validate

# Test sweep configuration
hsm run --config sweeps/sweep_config.yaml --dry-run

# Debug specific task
ls sweeps/outputs/<sweep_id>/tasks/<task_id>/
cat sweeps/outputs/<sweep_id>/tasks/<task_id>/stderr.log
```

## Next Steps

- [Sweep Configuration Reference](sweep_configuration.md)
- [Compute Source Configuration](compute_sources.md)
- [Monitoring and Analysis](monitoring.md)
- [API Reference](../api/index.md)
- [Migration from v1](migration_guide.md) 