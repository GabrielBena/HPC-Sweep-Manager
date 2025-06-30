# Getting Started with HPC Sweep Manager

This guide will help you get up and running with HPC Sweep Manager (HSM) across all execution modes.

## 🔧 Installation

### Prerequisites
- Python 3.8 or higher
- SSH access to remote machines (for remote/distributed modes)
- PBS/Torque or Slurm (for HPC mode)

### Install from PyPI
```bash
pip install hpc-sweep-manager
```

### Development Installation
```bash
git clone https://github.com/yourusername/hpc-sweep-manager.git
cd hpc-sweep-manager
pip install -e .
```

## 🎯 Quick Start: Choose Your Mode

HSM supports four execution modes. Choose based on your needs:

| Mode | Use Case | Setup Complexity | Scale |
|------|----------|------------------|-------|
| **Local** | Development, testing | ⭐ Simple | 1-10 jobs |
| **Remote** | Utilize remote GPU/server | ⭐⭐ Medium | 10-100 jobs |
| **Distributed** | Multi-machine experiments | ⭐⭐⭐ Advanced | 100-1000 jobs |
| **HPC** | Cluster computing | ⭐⭐ Medium | 1000+ jobs |

## 🚀 Local Mode - Quick Start

Perfect for development and small experiments on your local machine.

### 1. Initialize Project
```bash
cd /path/to/your/ml/project
hsm init
```

HSM will auto-detect your Python environment and training script:
```
✓ Detected Python: /opt/conda/bin/python
✓ Found training script: train.py
✓ Created configuration: sweeps/hsm_config.yaml
```

### 2. Configure Parameters
Edit `sweeps/sweep.yaml`:
```yaml
sweep:
  grid:
    seed: [1, 2, 3]
    model.hidden_size: [128, 256]
    optimizer.lr: [0.001, 0.01]
```

### 3. Run Local Sweep
```bash
# Small test run
hsm sweep --mode local --parallel-jobs 2 --max-runs 6

# With real-time output
hsm sweep --mode local --parallel-jobs 2 --show-output
```

## 🌐 Remote Mode - Quick Start

Execute jobs on a remote machine via SSH.

### 1. Setup SSH Access
Ensure passwordless SSH to your remote machine:
```bash
ssh-copy-id user@remote-server.edu
ssh user@remote-server.edu  # Should not prompt for password
```

### 2. Add Remote Machine
```bash
hsm remote add gpu-server remote-server.edu --key ~/.ssh/id_rsa
```

### 3. Test Connection
```bash
hsm remote test gpu-server
```

Expected output:
```
✓ gpu-server: Configuration discovered successfully
  Host: remote-server.edu
  Python: /home/user/miniconda3/bin/python
  Project Root: /home/user/my-project
  Train Script: train.py
  Max Jobs: 4
```

### 4. Run Remote Sweep
```bash
hsm sweep --mode remote --remote gpu-server --max-runs 12
```

## 🌟 Distributed Mode - Quick Start

Orchestrate jobs across multiple machines simultaneously.

### 1. Initialize Distributed Computing
```bash
hsm distributed init
```

### 2. Add Multiple Compute Sources
```bash
# Add remote machines
hsm distributed add gpu-server1 gpu1.university.edu --max-jobs 2
hsm distributed add gpu-server2 gpu2.university.edu --max-jobs 4
hsm distributed add workstation ws.lab.edu --max-jobs 1

# Local machine is automatically included
```

### 3. Test All Sources
```bash
hsm distributed test --all
```

### 4. Run Distributed Sweep
```bash
hsm sweep --mode distributed
```

HSM will automatically balance jobs across all available sources:
```
✓ 3 compute sources ready for distributed execution
Starting distributed execution across 3 sources...
[1/24] Job task_001 submitted to local
[2/24] Job task_002 submitted to gpu-server1  
[3/24] Job task_003 submitted to gpu-server2
...
Progress: 18/24 (75.0%) - ✓ 15 completed, ✗ 0 failed
```

## 🏛️ HPC Mode - Quick Start

Submit array jobs to PBS/Torque or Slurm clusters.

### 1. Verify HPC System
```bash
# PBS/Torque
qstat --version

# Slurm  
squeue --version
```

HSM auto-detects your scheduler.

### 2. Configure HPC Settings
Edit `sweeps/hsm_config.yaml`:
```yaml
hpc:
  default_walltime: "04:00:00"
  default_resources: "select=1:ncpus=4:mem=16gb"  # PBS
  # default_resources: "--nodes=1 --ntasks=4 --mem=16G"  # Slurm
  system: "pbs"  # or "slurm"
```

### 3. Submit Array Job
```bash
hsm sweep --mode array --walltime "02:00:00"
```

### 4. Monitor Progress
```bash
hsm monitor --watch
```

## 📊 Monitoring & Results

### Real-time Monitoring
```bash
# Monitor specific sweep
hsm monitor sweep_20241215_143022 --watch

# Monitor recent sweeps  
hsm recent --watch

# Monitor HPC queue
hsm queue --watch
```

### Collect Results
```bash
# Collect remote results
hsm collect-results sweep_20241215_143022

# Export results to CSV
hsm results sweep_20241215_143022 --format csv
```

## 🔧 Common Configuration

### HSM Configuration (`sweeps/hsm_config.yaml`)
```yaml
# Project paths
paths:
  python_interpreter: /opt/conda/bin/python
  train_script: train.py
  project_root: /path/to/project

# HPC settings
hpc:
  default_walltime: "04:00:00"  
  default_resources: "select=1:ncpus=4:mem=16gb"
  system: "pbs"

# W&B integration
wandb:
  project: my-experiment
  entity: my-team

# Distributed computing
distributed:
  enabled: true
  strategy: "round_robin"
  remotes:
    gpu-server:
      host: gpu.university.edu
      max_parallel_jobs: 2
```

### Sweep Configuration (`sweeps/sweep.yaml`)
```yaml
sweep:
  grid:
    seed: [1, 2, 3, 4, 5]
    model.hidden_size: [128, 256, 512]
    optimizer.lr: [0.001, 0.01, 0.1]
  
  paired:
    - model_and_data:
        model.name: [cnn, transformer]
        data.augmentation: [basic, advanced]

metadata:
  description: "Hyperparameter sweep for model comparison"
  tags: ["baseline", "comparison"]
```

## 📈 Scaling Guidelines

| Jobs | Recommended Mode | Example Command |
|------|------------------|-----------------|
| 1-10 | Local | `hsm sweep --mode local --max-runs 10` |
| 10-50 | Remote | `hsm sweep --mode remote --remote gpu-server` |
| 50-200 | Distributed | `hsm sweep --mode distributed` |
| 200+ | HPC Array | `hsm sweep --mode array` |

## 🆘 Troubleshooting

### SSH Connection Issues
```bash
# Test SSH manually
ssh -vvv user@remote-server.edu

# Check SSH key permissions
chmod 600 ~/.ssh/id_rsa
chmod 644 ~/.ssh/id_rsa.pub
```

### HPC Submission Failures
```bash
# Check queue status
qstat  # PBS
squeue # Slurm

# Verify resource requests
hsm sweep --dry-run --mode array
```

### Import Errors
```bash
# Check HSM installation
hsm --version

# Verify dependencies
pip install -r requirements.txt
```

## ➡️ Next Steps

- **[Configuration Guide](configuration.md)** - Detailed configuration options
- **[Execution Modes](execution_modes.md)** - Deep dive into each mode  
- **[Advanced Features](advanced_features.md)** - Power user features
- **[Examples](../examples/)** - Real-world use cases

## 💡 Tips for Success

1. **Start small**: Begin with local mode to validate your setup
2. **Test connections**: Always run `hsm remote test` or `hsm distributed test` 
3. **Use dry runs**: Preview jobs with `--dry-run` before submission
4. **Monitor progress**: Use `--watch` flags for real-time updates
5. **Organize outputs**: HSM creates clean directory structures automatically 