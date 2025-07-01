# Getting Started with HPC Sweep Manager v2

This guide walks you through the complete workflow of using HPC Sweep Manager v2 from project setup to sweep completion and monitoring.

## Overview

HPC Sweep Manager v2 provides a unified interface for running hyperparameter sweeps across different compute environments:
- **Local execution** - Run sweeps on your local machine
- **SSH remote execution** - Run sweeps on remote machines via SSH
- **HPC cluster execution** - Run sweeps on PBS/Torque or Slurm clusters
- **Distributed execution** - Run across multiple sources simultaneously
- **Cross-mode completion** - Start in one mode, complete in another
- **Unified result collection** - Automatic synchronization from all sources
- **Centralized logging** - All logs aggregated for easy analysis

## Key Features

### 🔧 Unified Architecture
- **Single SweepEngine** orchestrates all execution modes
- **Pluggable compute sources** with consistent interfaces
- **Real-time health monitoring** and automatic failover
- **Intelligent task distribution** with load balancing

### 📊 Comprehensive Monitoring
- **Live progress tracking** across all compute sources
- **Health monitoring** with automatic source disabling
- **Error analysis** with immediate failure collection
- **Resource utilization** tracking and optimization

### 📁 Unified File System
- **Consistent directory structure** across all execution modes
- **Automatic result synchronization** from remote sources
- **Centralized error collection** for debugging
- **Project sync verification** before distributed execution

### 🔄 Cross-Mode Flexibility
- **Complete partial sweeps** using different compute sources
- **Automatic recovery** from failed tasks
- **Resume interrupted sweeps** seamlessly
- **Mix local and remote** execution in the same sweep

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

#### Distributed Execution (New in v2)
```bash
# Run across multiple sources simultaneously with intelligent load balancing
hsm run --config sweeps/sweep_config.yaml --sources "local:4,ssh:server1:8,hpc:cluster:16"

# With automatic failover and health monitoring
hsm run --config sweeps/sweep_config.yaml --sources "local,ssh:backup1,ssh:backup2"
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
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ 🔄 Live Progress Monitor - Updates every 5s                     ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
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

Source Utilization:
┏━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┓
┃ Source     ┃ Active     ┃ Max Tasks    ┃ Health Status   ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━┩
│ local      │ 2/4        │ 4            │ ✅ Healthy      │
│ server1    │ 8/8        │ 8            │ ✅ Healthy      │
│ cluster    │ 3/16       │ 16           │ ⚠️ Degraded     │
└────────────┴────────────┴──────────────┴─────────────────┘
```

### 6. Complete Partial Sweeps

If a sweep was interrupted or you want to complete it with different resources:

```bash
# Complete missing tasks using local execution
hsm complete <sweep_id> --sources local

# Complete using different sources than original
hsm complete <sweep_id> --sources "hpc:cluster,ssh:server2"

# Retry failed tasks as well as missing ones
hsm complete <sweep_id> --sources local --retry-failed

# Dry run to see what would be completed
hsm complete <sweep_id> --sources local --dry-run
```

### 7. Analyze Results

HSM v2 provides comprehensive result analysis:

```bash
# Show detailed sweep status
hsm status <sweep_id>

# Show all sweeps
hsm status --all

# Analyze errors in detail
hsm monitor errors <sweep_id>

# Export results for analysis
hsm export <sweep_id> --format csv
```

## Unified Directory Structure

HSM v2 creates a consistent directory structure regardless of execution mode:

```
sweeps/outputs/{sweep_id}/
├── sweep_config.yaml              # Original sweep configuration
├── sweep_metadata.yaml            # Sweep metadata and settings
├── task_mapping.yaml              # Task-to-source assignments and status
├── tasks/                         # Individual task results
│   ├── task_001/                  # Task directory
│   │   ├── params.yaml            # Task parameters
│   │   ├── status.yaml            # Task status and timing
│   │   ├── stdout.log             # Standard output
│   │   ├── stderr.log             # Standard error
│   │   ├── results/               # Task output files
│   │   └── metrics.json           # Extracted metrics (if available)
│   └── task_002/
├── logs/                          # Centralized logs
│   ├── sweep.log                  # Main sweep log
│   ├── sweep_aggregated.log       # Aggregated log from all sources
│   ├── task_assignments.log       # Task assignment tracking
│   ├── source_utilization.log     # Source utilization over time
│   ├── health_status.log          # Health monitoring log
│   ├── sources/                   # Per-source logs
│   │   ├── local.log
│   │   ├── server1.log
│   │   └── cluster.log
│   └── errors/                    # Error summaries
│       ├── failed_tasks.yaml     # Failed task summary
│       └── error_patterns.yaml   # Common error analysis
├── scripts/                       # Generated scripts (if applicable)
│   ├── job_scripts/              # HPC job scripts
│   └── setup_scripts/            # Environment setup scripts
└── reports/                       # Generated reports
    ├── completion_report.html     # Completion status report
    ├── performance_analysis.yaml  # Performance metrics
    └── error_analysis.html        # Error analysis report
```

## Project Synchronization

HSM v2 automatically verifies that local and remote projects are synchronized:

**Git-based synchronization:**
- Checks if commit hashes match
- Verifies no uncommitted changes
- Warns about branch differences

**File-based synchronization:**
- Compares file checksums
- Identifies missing or modified files
- Provides sync recommendations

**Automatic sync verification:**
```bash
# Verify sync status manually
hsm config validate --check-sync

# Run with sync verification (default)
hsm run --config sweeps/sweep_config.yaml --sources ssh:server1

# Skip sync verification (not recommended)
hsm run --config sweeps/sweep_config.yaml --sources ssh:server1 --no-verify-sync
```

## Advanced Features

### Health Monitoring and Failover

HSM v2 continuously monitors compute source health:

```bash
# View health status
hsm monitor health

# Configure health check intervals
hsm config set compute.health_check_interval 300  # 5 minutes
```

**Automatic actions:**
- **Disable unhealthy sources** after consecutive failures
- **Redistribute tasks** from failed sources
- **Resume on recovery** when sources become healthy again

### Load Balancing

Choose from multiple task distribution strategies:

```bash
# Round-robin distribution (default)
hsm run --config sweep.yaml --sources "local,ssh:s1,ssh:s2" --strategy round_robin

# Least-loaded distribution
hsm run --config sweep.yaml --sources "local,ssh:s1,ssh:s2" --strategy least_loaded

# Random distribution
hsm run --config sweep.yaml --sources "local,ssh:s1,ssh:s2" --strategy random
```

### Error Analysis

HSM v2 provides comprehensive error analysis:

- **Immediate failure collection** from remote sources
- **Error pattern detection** across tasks
- **Categorized error summaries** for debugging
- **Resource exhaustion detection** (disk space, memory)

## Troubleshooting

### Common Issues

**Project sync failures:**
```bash
# Check sync status
hsm config validate --check-sync

# Force sync (use with caution)
hsm sync --force --target ssh:server1
```

**Compute source connection issues:**
```bash
# Test SSH connectivity
hsm test ssh:server1

# Validate HPC configuration
hsm test hpc:cluster
```

**Task failures:**
```bash
# Analyze recent failures
hsm monitor errors <sweep_id>

# Retry failed tasks
hsm complete <sweep_id> --retry-failed
```

### Performance Optimization

**Resource limits:**
```bash
# Optimize concurrent tasks per source
hsm run --config sweep.yaml --sources "local:2,ssh:server1:8,hpc:cluster:32"

# Set global concurrency limit
hsm run --config sweep.yaml --sources "local,ssh:s1,ssh:s2" --parallel-limit 16
```

**Network optimization:**
```bash
# Adjust result collection interval
hsm config set compute.result_collection_interval 120  # 2 minutes

# Configure SSH compression
hsm config set ssh.compression true
```

## Next Steps

1. **Configure multiple compute sources** in your `hsm_config.yaml`
2. **Set up experiment tracking** (W&B, MLflow) integration
3. **Create custom monitoring dashboards** using the HSM API
4. **Automate sweep workflows** with CI/CD integration

For more detailed information, see:
- [Configuration Guide](configuration.md)
- [Compute Sources Guide](compute_sources.md)
- [API Reference](../api/reference.md)
- [Advanced Usage](advanced_usage.md) 