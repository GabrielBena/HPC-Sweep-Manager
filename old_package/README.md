# HPC Sweep Manager (HSM)

A Python package for automated hyperparameter sweeps on High Performance Computing (HPC) systems using Hydra configs and W&B tracking, with **remote execution** capabilities via SSH.

## 🎯 Overview

HSM simplifies running large-scale hyperparameter sweeps across different compute environments:

- **Local execution** with parallel job management
- **HPC cluster submission** (PBS/Torque, Slurm) with array jobs
- **Remote machine execution** via SSH with auto-discovery
- **Unified configuration** and monitoring across all modes
- **Automatic path detection** and environment validation
- **Built-in result collection** and sweep management

## 🚀 Quick Start

### 1. Installation

```bash
pip install hpc-sweep-manager
```

### 2. Initialize Your Project

```bash
cd /path/to/your/ml/project
hsm init  # Auto-detects paths and creates config templates
```

### 3. Configure Parameter Sweep

Edit `sweeps/sweep.yaml`:
```yaml
sweep:
  grid:
    seed: [1, 2, 3]
    model.hidden_size: [128, 256, 512]
    optimizer.lr: [0.001, 0.01]
```

### 4. Run Your Sweep

```bash
# Local execution (parallel jobs)
hsm sweep --mode local --parallel-jobs 4

# HPC cluster (array job)
hsm sweep --mode array

# Remote machine execution
hsm sweep --mode remote --remote machine_name
```

## 🛠️ Execution Modes

### **Local Mode** - Single Machine Parallel Execution
```bash
hsm sweep --mode local --parallel-jobs 4 --show-output
```
- Runs jobs in parallel on your local machine
- Real-time output display option
- Automatic resource management and cleanup

### **Array Mode** - HPC Cluster Array Jobs  
```bash
hsm sweep --mode array --walltime "04:00:00"
```
- Submits efficient array jobs to PBS/Torque or Slurm
- Auto-detects HPC system and generates appropriate scripts
- Organized task outputs in `sweep_dir/tasks/task_N/`

### **Remote Mode** - Single Remote Machine via SSH
```bash
hsm sweep --mode remote --remote blossom
```
- Executes jobs on a remote machine via SSH
- Auto-discovery of remote environments and paths
- Automatic project synchronization and result collection

## 🌐 Remote Machine Setup

### Configure Remote Access

```bash
# Add a remote machine
hsm remote add blossom --host "blossom.example.com" --key "~/.ssh/id_ed25519"

# Test connection and auto-discovery  
hsm remote test blossom

# Check remote health
hsm remote health blossom
```

### Remote Requirements

The remote machine needs:
- SSH access with key-based authentication
- Python environment with your project dependencies
- `hsm_config.yaml` in the project directory for path discovery

## 📁 Project Structure

HSM creates organized sweep outputs:

```
your-project/
├── sweeps/
│   ├── sweep.yaml           # Parameter configuration
│   ├── hsm_config.yaml      # HSM project settings  
│   └── outputs/
│       └── sweep_20240315_143022/
│           ├── tasks/       # Individual task outputs
│           │   ├── task_1/  # Each job gets its own folder
│           │   │   ├── task_info.txt
│           │   │   ├── command.txt
│           │   │   └── [your outputs]/
│           │   └── task_N/
│           ├── logs/        # Job execution logs
│           ├── scripts/     # Generated job scripts
│           └── submission_summary.txt
```

**Benefits:**
- **Clean organization** - No directory pollution
- **Easy debugging** - Each task self-contained  
- **Scalable** - Works for 1 to 10,000+ jobs
- **Cross-platform** - Same structure everywhere

## 🎛️ CLI Commands

### Core Workflow
```bash
hsm init                          # Initialize project
hsm sweep                         # Run parameter sweep
hsm monitor SWEEP_ID              # Monitor progress
hsm collect-results SWEEP_ID      # Collect remote results
```

### Remote Management
```bash
hsm remote add NAME --host HOST   # Add remote machine
hsm remote list                   # List configured remotes
hsm remote test [NAME]            # Test connections
hsm remote health [NAME]          # Check remote status
```

### Monitoring & Management  
```bash
hsm monitor --watch               # Real-time monitoring
hsm recent --days 7               # Recent sweeps
hsm queue --watch                 # HPC queue status
hsm cancel SWEEP_ID               # Cancel running sweep
hsm cleanup --days 30             # Clean old jobs
```

## ⚙️ Configuration

### Sweep Configuration (`sweeps/sweep.yaml`)

```yaml
sweep:
  grid:  # All combinations
    seed: [1, 2, 3, 4, 5]
    model.hidden_size: [128, 256, 512]
    optimizer.lr: [0.001, 0.01, 0.1]
  
  paired:  # Paired parameters
    - model_and_data:
        model.name: [cnn, transformer, mlp]
        data.augmentation: [basic, advanced, none]
```

### HSM Configuration (`sweeps/hsm_config.yaml`)

Auto-generated with your project settings:
```yaml
hpc:
  default_walltime: "04:00:00"
  default_resources: "select=1:ncpus=4:mem=16gb"
  system: pbs

paths:
  python_interpreter: /path/to/python
  train_script: /path/to/train.py
  config_dir: /path/to/configs

# Remote machines (optional)
distributed:
  remotes:
    blossom:
      host: "blossom.example.com"
      ssh_key: ~/.ssh/id_ed25519
      max_parallel_jobs: 4
```

## 📊 Examples

### Basic Grid Search
```bash
# Edit sweeps/sweep.yaml with your parameters
hsm sweep --dry-run              # Preview jobs
hsm sweep --mode array           # Submit to cluster
hsm monitor --watch             # Monitor progress
```

### Remote Execution
```bash
hsm remote add gpu-server --host "gpu.university.edu"
hsm remote test gpu-server
hsm sweep --mode remote --remote gpu-server --max-runs 10
```

### Local Development
```bash
hsm sweep --mode local --parallel-jobs 2 --show-output --max-runs 5
```

## 🔧 Advanced Features

### Job Management
- **Smart job submission** - Automatic PBS/Slurm detection
- **Robust execution** - Error handling and recovery
- **Resource optimization** - Efficient parallel execution
- **Cross-platform** - Works on any Unix-like system

### Remote Capabilities  
- **Auto-discovery** - Finds Python environments and scripts
- **Environment validation** - Checks dependencies before execution
- **Secure transfer** - SSH-based file synchronization
- **Result aggregation** - Centralizes outputs for analysis

### Monitoring & Debugging
- **Real-time status** - Live job progress tracking
- **Detailed logging** - Comprehensive execution records
- **Task isolation** - Each job in separate directory
- **Easy debugging** - Clear error messages and logs

## 🏗️ Architecture

```
┌─────────────────┐    SSH/rsync    ┌─────────────────┐
│   Local Machine │◄──────────────►│  Remote Machine │
│   (Controller)  │                 │    (Worker)     │
│                 │                 │                 │
│ • HSM Config    │                 │ • HSM Config    │
│ • Sweep Config  │                 │ • Auto-detected │
│ • Job Scheduler │                 │ • Environment   │
│ • Result Collection              │ • Execution     │
└─────────────────┘                 └─────────────────┘
```

**Key Principles:**
- **Unified interface** across all execution modes
- **Auto-discovery** minimizes manual configuration  
- **Organized outputs** for easy analysis
- **Extensible design** for future enhancements

## 🤝 Contributing

HSM is designed to be extensible. Key areas for contribution:
- Additional HPC schedulers (GridEngine, etc.)
- Enhanced monitoring and visualization
- Distributed load balancing strategies
- Integration with other ML frameworks

## 📄 License

MIT License - see LICENSE file for details. 