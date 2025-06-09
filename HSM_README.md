# HPC Sweep Manager

A Python package for automated hyperparameter sweeps on High Performance Computing (HPC) systems using Hydra configs and W&B tracking.

## 🎯 Vision & Motivation

HPC Sweep Manager (`hsm`) solves the common problem of managing large-scale hyperparameter sweeps across HPC environments. Instead of writing custom sweep scripts for each project, this package provides:

- **Reusable sweep infrastructure** across projects
- **Interactive configuration building** from existing Hydra configs
- **Unified job submission** for both individual and array jobs
- **Automatic path detection** and environment setup
- **Built-in monitoring** and result tracking
- **Multi-HPC system support** (PBS/Torque, Slurm, etc.)

## 📁 Project Structure

```
hpc-sweep-manager/
├── pyproject.toml                 # Modern Python packaging
├── README.md
├── LICENSE
├── src/hpc_sweep_manager/
│   ├── __init__.py
│   ├── cli/                       # Command-line interface
│   │   ├── __init__.py
│   │   ├── main.py               # Entry point with subcommands
│   │   ├── init.py               # Project initialization
│   │   ├── configure.py          # Interactive config builder
│   │   ├── sweep.py              # Sweep execution
│   │   └── monitor.py            # Job monitoring
│   ├── core/                     # Shared business logic
│   │   ├── __init__.py
│   │   ├── config_parser.py      # YAML parsing, validation
│   │   ├── param_generator.py    # Grid/paired combinations
│   │   ├── job_manager.py        # PBS/Slurm abstraction
│   │   ├── path_detector.py      # Auto-detect project paths
│   │   └── utils.py              # Common utilities
│   └── templates/                # Jinja2 templates for scripts
│       ├── sweep.yaml.j2         # Sweep config template
│       ├── sweep_single.sh.j2    # Individual job template
│       └── sweep_array.sh.j2     # Array job template
├── tests/
│   ├── test_config_parser.py
│   ├── test_param_generator.py
│   ├── test_job_manager.py
│   └── fixtures/
└── examples/
    ├── basic_sweep/
    ├── neural_network/
    └── multi_objective/
```

## 🚀 Installation & Setup

### Install the Package

```bash
pip install hpc-sweep-manager
```

### Initialize a New Project

```bash
# Navigate to your project directory
cd /path/to/your/ml/project

# Initialize sweep infrastructure
hsm init

# This will:
# 1. Auto-detect your project structure
# 2. Find Hydra configs, training scripts, Python environments
# 3. Create a 'sweeps/' directory with templates
# 4. Generate initial configuration files
```

### Manual Setup (if auto-detection fails)

```bash
hsm init --interactive
```

This will prompt you for:
- Python interpreter path
- Training script location  
- Hydra config directory
- HPC system type (PBS/Slurm)
- Default resource requirements

## 🛠️ CLI Interface

### Core Commands

```bash
hsm --help                        # Show all commands

# Project initialization
hsm init [--interactive]          # Initialize project sweep infrastructure

# Configuration building  
hsm configure                     # Interactive sweep config builder
hsm configure --from-file config.yaml  # Build from existing Hydra config

# Sweep execution
hsm sweep [OPTIONS]               # Run parameter sweep
hsm sweep --dry-run              # Preview without submitting
hsm sweep --array               # Submit as array job
hsm sweep --individual          # Submit individual jobs
hsm sweep --count               # Count total combinations
hsm sweep --max-runs N          # Limit number of runs

# Monitoring & management
hsm monitor [SWEEP_ID]           # Monitor active sweeps
hsm status                       # Show all active sweeps
hsm cancel SWEEP_ID             # Cancel running sweep
hsm results SWEEP_ID            # Collect and summarize results
```

### Sweep Command Options

```bash
hsm sweep [OPTIONS]

Options:
  --config PATH          Sweep config file [default: sweeps/sweep.yaml]
  --mode {individual,array}  Submission mode [default: individual]
  --dry-run             Preview jobs without submitting
  --count               Count combinations and exit
  --max-runs INTEGER    Maximum number of runs to submit
  --walltime TEXT       Job walltime [default: 04:00:00]
  --resources TEXT      HPC resources [default: select=1:ncpus=4:mem=16gb]
  --group TEXT          W&B group name [default: auto-generated]
  --priority INTEGER    Job priority
  --help                Show this message and exit.
```

## 📝 Configuration Files

### Sweep Configuration (`sweeps/sweep.yaml`)

```yaml
# Define parameter sweeps
defaults:
  - override hydra/launcher: basic

sweep:
  grid:  # Parameters to combine in all possible ways
    seed: [1, 2, 3, 4, 5]
    model.hidden_size: [128, 256, 512]
    optimizer.lr: [0.001, 0.01, 0.1]
    data.batch_size: [32, 64]

  paired:  # Parameters that vary together
    - model_and_data:
        model:
          name: [cnn, transformer, mlp]
          dropout: [0.1, 0.2, 0.3]
        data:
          augmentation: [basic, advanced, none]
          # Must have same length as model parameters

metadata:
  description: "Hyperparameter sweep for model comparison"
  wandb_project: "my-ml-project"
  tags: ["baseline", "comparison"]
```

### Project Configuration (`sweeps/hsm_config.yaml`)

```yaml
# HPC Sweep Manager Configuration
project:
  name: "my-ml-project"
  root: "/path/to/project"
  
paths:
  python_interpreter: "/home/user/miniconda3/envs/ml/bin/python"
  train_script: "scripts/train.py"
  config_dir: "configs"
  output_dir: "outputs"
  
hpc:
  system: "pbs"  # or "slurm"
  default_walltime: "04:00:00"
  default_resources: "select=1:ncpus=4:mem=16gb"
  max_array_size: 10000
  
wandb:
  project: "my-ml-project"
  entity: "my-team"
  
notifications:
  email: "user@example.com"
  slack_webhook: null
```

## 🎮 Usage Examples

### Basic Workflow

```bash
# 1. Initialize project
cd /path/to/ml/project
hsm init

# 2. Build sweep configuration interactively
hsm configure
# This scans your Hydra configs and lets you select parameters

# 3. Preview the sweep
hsm sweep --dry-run --count
# Output: Would generate 60 jobs (5 seeds × 3 models × 4 learning rates)

# 4. Submit as array job
hsm sweep --array --max-runs 50
# Output: Submitted array job 12345.pbs with 50 tasks

# 5. Monitor progress
hsm monitor 12345.pbs
# Shows job status, completion rate, failed jobs, etc.

# 6. Collect results
hsm results 12345.pbs
# Aggregates results, generates summary plots
```

### Advanced Usage

```bash
# Custom sweep config
hsm sweep --config experiments/ablation_study.yaml --array

# Limited resource sweep
hsm sweep --individual --max-runs 10 --walltime 02:00:00

# High priority jobs
hsm sweep --array --priority 100 --resources "select=1:ncpus=8:mem=32gb"

# Interactive configuration from specific Hydra config
hsm configure --from-file configs/experiment/neural_network.yaml
```

## 🔧 Interactive Configuration Builder

The `hsm configure` command provides an interactive TUI for building sweep configurations:

```
┌─── HPC Sweep Manager - Configuration Builder ───┐
│                                                   │
│ Detected Hydra configs:                          │
│ ✓ configs/model/cnn.yaml                         │
│ ✓ configs/optimizer/adam.yaml                    │
│ ✓ configs/data/cifar10.yaml                      │
│                                                   │
│ Available parameters:                             │
│ [x] model.hidden_size: [128, 256, 512]          │
│ [x] optimizer.lr: [0.001, 0.01]                 │
│ [ ] data.batch_size: [32, 64, 128]              │
│ [ ] training.epochs: [10, 20, 50]               │
│                                                   │
│ Parameter grouping:                               │
│ ○ Grid (all combinations)                        │
│ ○ Paired (vary together)                         │
│                                                   │
│ [Generate Config] [Preview] [Cancel]              │
└───────────────────────────────────────────────────┘
```

## 🏗️ Core Architecture

### Parameter Generation Engine

```python
from hpc_sweep_manager.core import ParameterGenerator, SweepConfig

# Load sweep config
config = SweepConfig.from_yaml("sweeps/sweep.yaml")

# Generate parameter combinations
generator = ParameterGenerator(config)
combinations = generator.generate_combinations(max_runs=100)

# Each combination is a flat dict like:
# {"model.hidden_size": 256, "optimizer.lr": 0.01, "seed": 42}
```

### Job Management Abstraction

```python
from hpc_sweep_manager.core import JobManager

# Auto-detect HPC system
job_manager = JobManager.auto_detect()

# Submit jobs
if args.array:
    job_id = job_manager.submit_array_job(combinations, "my_sweep")
else:
    job_ids = job_manager.submit_individual_jobs(combinations)
```

### Path Detection

```python
from hpc_sweep_manager.core import PathDetector

detector = PathDetector(project_root="/path/to/project")

# Auto-detect project structure
python_path = detector.detect_python_path()
train_script = detector.detect_train_script()  
config_dir = detector.detect_config_dir()
```

## 🎯 Key Features

### ✅ **Unified Parameter Generation**
- Single codebase for grid and paired parameter combinations
- Support for nested Hydra configurations
- Validation and conflict detection

### ✅ **Multi-HPC System Support**
- Abstract job manager interface
- PBS/Torque and Slurm implementations
- Easy to extend for other systems

### ✅ **Smart Path Detection**
- Auto-detect Python environments (conda, venv, system)
- Find training scripts and Hydra configs
- Detect HPC-specific paths and storage

### ✅ **Interactive Configuration**
- TUI for building sweep configs from Hydra files
- Parameter selection and grouping
- Real-time combination counting

### ✅ **Quality of Life Features**
- Dry run mode for testing
- Job monitoring and status tracking
- Automatic result collection
- Email/Slack notifications (optional)

### ✅ **Template System**
- Jinja2 templates for job scripts
- Customizable for different HPC environments
- Version control friendly

## 🔮 Future Extensions

### Planned Features
- **Slurm support** - Add Slurm job manager implementation
- **Advanced monitoring** - Real-time job progress dashboards  
- **Result analysis** - Automated hyperparameter importance analysis
- **Jupyter integration** - Notebook widgets for sweep management
- **Multi-objective optimization** - Integration with Optuna/Ray Tune
- **Fault tolerance** - Automatic job restart on failure
- **Resource optimization** - Dynamic resource allocation based on job requirements

### Plugin System
```python
# Custom job managers
from hpc_sweep_manager.plugins import BaseJobManager

class SGEJobManager(BaseJobManager):
    def submit_job(self, ...):
        # SGE-specific implementation
        pass

# Custom result collectors  
from hpc_sweep_manager.plugins import BaseResultCollector

class TensorboardCollector(BaseResultCollector):
    def collect_results(self, sweep_id):
        # Parse tensorboard logs
        pass
```

## 🙏 Acknowledgments

- Built on top of [Hydra](https://hydra.cc/) for configuration management
- Inspired by [W&B Sweeps](https://wandb.ai/site/sweeps) for experiment tracking
- Designed for HPC environments like Imperial College's CX3 and PBS systems