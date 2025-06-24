# HPC Sweep Manager

A Python package for automated hyperparameter sweeps on High Performance Computing (HPC) systems using Hydra configs and W&B tracking.

## 🎯 Vision & Motivation

HPC Sweep Manager (`hsm`) solves the common problem of managing large-scale hyperparameter sweeps across HPC environments. Instead of writing custom sweep scripts for each project, this package provides:

- **Reusable sweep infrastructure** across projects
- **Interactive configuration building** from existing Hydra configs
- **Unified job submission** for both individual and array jobs
- **Automatic path detection** and environment setup
- **Comprehensive monitoring** and job management
- **Built-in cleanup** and maintenance tools
- **Multi-HPC system support** (PBS/Torque, Slurm, etc.)

## 📁 Project Structure

### Package Structure
```
hpc-sweep-manager/
├── pyproject.toml                 # Modern Python packaging
├── README.md
├── LICENSE
├── requirements.txt
├── src/hpc_sweep_manager/
│   ├── __init__.py
│   ├── cli/                       # Command-line interface
│   │   ├── __init__.py
│   │   ├── main.py               # Entry point with all subcommands
│   │   ├── init.py               # Project initialization
│   │   ├── configure.py          # Interactive config builder
│   │   ├── sweep.py              # Sweep execution
│   │   └── monitor.py            # Comprehensive job monitoring
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
│       ├── sweep_array.sh.j2     # Array job template (PBS)
│       └── slurm_array.sh.j2     # Array job template (Slurm)
├── tests/                        # Comprehensive test suite
│   ├── test_core.py              # Core functionality tests
│   ├── test_full_functionality.py # Integration tests
│   ├── test_count.py             # Parameter counting tests
│   ├── test_sweep_direct.py      # Direct sweep tests
│   └── test_sweep.yaml           # Test configuration
└── examples/                     # Example configurations
```

### Sweep Directory Structure

When you run sweeps, HSM creates an organized directory structure to keep everything clean:

#### Individual Job Mode
```
your-project/
├── sweeps/
│   ├── sweep.yaml                # Your sweep configuration
│   ├── hsm_config.yaml          # HSM project configuration
│   └── outputs/
│       └── sweep_20240315_143022/
│           ├── pbs_files/        # Job scripts (pbs_files/ or slurm_files/)
│           │   ├── sweep_20240315_143022_job_001.pbs
│           │   ├── sweep_20240315_143022_job_002.pbs
│           │   └── ...
│           ├── logs/             # Job output/error logs
│           │   ├── job_001.out
│           │   ├── job_001.err
│           │   └── ...
│           ├── parameter_combinations.json
│           └── submission_summary.txt
```

#### Array Job Mode (Recommended for Large Sweeps)
```
your-project/
├── sweeps/
│   ├── sweep.yaml                # Your sweep configuration
│   ├── hsm_config.yaml          # HSM project configuration
│   └── outputs/
│       └── sweep_20240315_143022/
│           ├── tasks/            # ✨ Organized task outputs
│           │   ├── task_1/       # Each array subjob gets its own folder
│           │   │   ├── task_info.txt      # Task metadata and status
│           │   │   ├── command.txt        # Executed command
│           │   │   └── [your_outputs]/    # Model checkpoints, logs, etc.
│           │   ├── task_2/
│           │   ├── task_3/
│           │   └── task_N/
│           ├── pbs_files/        # Job scripts (pbs_files/ or slurm_files/)
│           │   └── sweep_20240315_143022_array.pbs
│           ├── logs/             # HPC system logs
│           │   ├── array_job.out
│           │   └── array_job.err
│           ├── parameter_combinations.json
│           └── submission_summary.txt
```

**Key Benefits of This Structure:**
- **Clean Organization**: Task outputs are contained in individual `task_X/` folders
- **No Directory Pollution**: Main sweep directory stays organized
- **Easy Debugging**: Each task's info, command, and outputs are co-located
- **Scalable**: Works efficiently for any number of array subjobs (1 to 10,000+)
- **HPC System Agnostic**: Same structure for PBS, Slurm, or other systems

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

# Monitoring & status
hsm monitor [SWEEP_ID]           # Monitor specific sweep with real-time updates
hsm monitor --watch             # Continuous monitoring mode
hsm monitor --detailed          # Show array job subjob breakdown
hsm status                      # Show all active sweeps
hsm recent --days 7             # Show recent sweeps from last N days
hsm queue                       # Show current PBS/Slurm queue status
hsm queue --watch               # Real-time queue monitoring

# Job management
hsm cancel SWEEP_ID             # Cancel running sweep
hsm delete-jobs SWEEP_ID        # Delete specific jobs with filters
hsm delete-jobs SWEEP_ID --state Q  # Delete only queued jobs
hsm delete-jobs SWEEP_ID --pattern "*_001"  # Delete jobs matching pattern
hsm cleanup --days 7           # Clean up old completed jobs

# Results & analysis
hsm results SWEEP_ID            # Collect and summarize results from tasks/ folder
```

### Advanced Monitoring Features

The package includes comprehensive monitoring capabilities with full array job support:

```bash
# Real-time sweep monitoring with detailed job status
hsm monitor sweep_20240101_143022 --watch --refresh 10

# Show all sweeps with array job subjob breakdown
hsm monitor --detailed                              # Shows individual task status

# Queue status with automatic refresh
hsm queue --watch --refresh 30

# Recent sweeps with filtering
hsm recent --days 14 --watch

# Targeted job cleanup with array job support
hsm delete-jobs sweep_20240101_143022 --state F --force    # Delete failed subjobs
hsm delete-jobs sweep_20240101_143022 --pattern "task_*"   # Delete specific tasks
hsm cleanup --days 30 --states C,F --dry-run              # Preview old job cleanup
```

**Array Job Monitoring Features:**
- **Individual Task Tracking**: Monitor each `task_X/` folder independently
- **Smart Deletion**: Delete entire arrays or specific subjobs
- **Task Status Files**: Each task folder contains execution info and status
- **Organized Output Collection**: Results grouped by task for easy analysis

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

# 4. Submit as array job (HPC)
hsm sweep --array --max-runs 50
# Output: Submitted array job 12345.pbs with 50 tasks
# Creates organized tasks/ folder with task_1/, task_2/, etc.

# 4b. OR run locally for development/testing
hsm sweep --mode local --parallel-jobs 4 --max-runs 10
# Output: Running 10 jobs locally with up to 4 parallel processes
# Same organized folder structure: tasks/task_1/, tasks/task_2/, etc.

# 5. Monitor progress with real-time updates
hsm monitor 12345.pbs --watch
# Shows job status, completion rate, failed jobs, and task organization

# 6. Check queue status and individual task progress
hsm queue --watch
# Real-time view of all your jobs in the queue
# Each task folder contains task_info.txt with status updates

# 7. Clean up completed jobs
hsm cleanup --days 7 --states C
# Remove completed jobs older than 7 days
# Preserves tasks/ folder structure for result analysis
```

### Advanced Monitoring Workflow

```bash
# Monitor all recent sweeps
hsm recent --days 14 --watch

# Detailed monitoring of specific sweep
hsm monitor sweep_20240315_143022 --watch --refresh 15

# Cancel problematic jobs from a sweep
hsm delete-jobs sweep_20240315_143022 --state H --force  # Delete held jobs

# Clean up old sweeps comprehensively
hsm cleanup --days 30 --dry-run  # Preview what would be cleaned
hsm cleanup --days 30 --force    # Actually clean up
```

### Array Job Management

```bash
# Submit large parameter sweep as array job
hsm sweep --array --max-runs 1000 --walltime 12:00:00

# Monitor array job progress with sub-job details
hsm monitor sweep_20240315_143022 --watch

# Manage problematic array sub-jobs
hsm delete-jobs sweep_20240315_143022 --state F  # Remove failed sub-jobs
hsm queue --watch  # Monitor remaining jobs
```

### Local Mode for Development & Testing

The local mode provides seamless development and testing capabilities while maintaining the same organized structure as HPC runs:

```bash
# Run sweeps locally with controlled parallelism
hsm sweep --mode local --parallel-jobs 2 --max-runs 10

# Sequential execution (safest for debugging)
hsm sweep --mode local --parallel-jobs 1

# Use all CPU cores for maximum local throughput  
hsm sweep --mode local --parallel-jobs 8

# Same monitoring works for local jobs
hsm monitor local_sweep_20240315_143022 --watch

# Same organized output structure:
# sweeps/outputs/sweep_20240315_143022/
# ├── tasks/task_1/    # Individual job outputs
# ├── tasks/task_2/    # Same W&B logging, checkpoints, etc.
# ├── local_scripts/   # Generated shell scripts
# └── logs/            # Execution logs
```

**Local Mode Benefits:**
- **Same folder structure** as HPC jobs for seamless workflow transition
- **Controlled parallelism** with `--parallel-jobs` option
- **Individual task folders** with status tracking and command history
- **W&B integration** works identically to HPC runs  
- **Real-time monitoring** with `hsm monitor` and `hsm queue`
- **Easy debugging** with direct access to logs and outputs
- **No HPC queue waiting** for rapid development iteration

## 🔧 Interactive Configuration Builder

The `hsm configure` command provides an interactive CLI for building sweep configurations by scanning your Hydra configs:

```
HPC Sweep Manager - Interactive Configuration Builder

Project: PVR
Config directory: /rds/general/user/gb21/home/PhD/INI/PVR/configs

Scanning for parameters...
Found 30 config files
Found 138 potential parameters:

┌─────────────────────────┬───────┬────────────┬─────────────────────────────────┐
│ Parameter               │ Type  │ Default    │ Source                          │
├─────────────────────────┼───────┼────────────┼─────────────────────────────────┤
│ seed                    │ str   │ ${seed}    │ data/encoded/base_encoder.yaml  │
│ device                  │ str   │ cpu        │ config.yaml                     │
│ wandb_enabled           │ bool  │ True       │ jupyter_config.yaml             │
│ mode                    │ str   │ online     │ wandb/default.yaml              │
│ factor                  │ float │ 0.1        │ scheduler/reduce_lr_on_plateau  │
│ patience                │ int   │ 10         │ scheduler/reduce_lr_on_plateau  │
│ verbose                 │ bool  │ False      │ scheduler/step_lr.yaml          │
│ threshold               │ float │ 0.0001     │ scheduler/reduce_lr_on_plateau  │
│ threshold_mode          │ str   │ rel        │ scheduler/reduce_lr_on_plateau  │
│ cooldown                │ int   │ 0          │ scheduler/reduce_lr_on_plateau  │
│ min_lr                  │ int   │ 0          │ scheduler/reduce_lr_on_plateau  │
│ eps                     │ str   │ 1e-8       │ optimizer/rmsprop.yaml          │
│ T_max                   │ str   │ ${training │ scheduler/cosine_annealing_lr   │
│ eta_min                 │ str   │ 1e-5       │ scheduler/cosine_annealing_lr   │
│ step_size               │ int   │ 10         │ scheduler/step_lr.yaml          │
│ gamma                   │ float │ 0.1        │ scheduler/step_lr.yaml          │
│ l1                      │ str   │ 1e-4       │ deepr/adam.yaml                 │
│ connectivity_lr_scale   │ float │ 1.0        │ deepr/adam.yaml                 │
│ ...                     │ ...   │ ...        │ and 118 more                    │
└─────────────────────────┴───────┴────────────┴─────────────────────────────────┘

Select parameters for sweep:

Parameter selection menu:
1. Add grid parameter (all combinations)
2. Add paired parameters (vary together)
3. Review current selection
4. Finish configuration
Choose option [1/2/3/4] (1): 
```

**Features:**
- **Auto-detection**: Scans your Hydra config directory and finds all parameters
- **Smart suggestions**: Provides intelligent default values for common parameters (lr, batch_size, etc.)
- **Grid vs Paired**: Choose between grid search (all combinations) or paired parameters (vary together)
- **Type inference**: Automatically detects parameter types (int, float, bool, str)
- **Interactive selection**: Menu-driven interface for easy parameter selection
- **Real-time validation**: Shows total combinations and validates paired parameter lengths

## 🎯 Key Features

### ✅ **Smart Configuration**
- **Auto-discovery**: Scans Hydra configs and detects 100+ parameters automatically
- **Interactive builder**: Menu-driven parameter selection with intelligent suggestions
- **Grid & paired sweeps**: Support for both exhaustive and coordinated parameter variations

### ✅ **Flexible Job Submission**
- **Multi-system support**: PBS/Torque, Slurm, and local execution with auto-detection
- **Multiple execution modes**: Array jobs, individual jobs, or local execution with controlled parallelism
- **Resource management**: Configurable walltime, CPU, memory, and priority settings
- **Seamless local development**: Same folder structure and monitoring for local vs. HPC runs

### ✅ **Comprehensive Monitoring**
- **Real-time tracking**: Live updates on job status, progress, and failures
- **Array job insight**: Detailed breakdown of subjob states (R, Q, C, F) for array jobs
- **Smart job completion**: Automatically treats jobs not in PBS queue as finished
- **Queue monitoring**: Watch your jobs in the HPC queue with auto-refresh
- **Smart cleanup**: Filter and delete jobs by state, age, or pattern matching

### ✅ **Production Ready**
- **Error handling**: Graceful failures with detailed logging and recovery suggestions
- **Template system**: Customizable job scripts for different HPC environments

## 📦 Dependencies

### Core Dependencies
- **click**: Command-line interface framework
- **rich**: Rich text and beautiful formatting
- **textual**: Terminal user interface framework
- **pyyaml**: YAML configuration parsing
- **jinja2**: Template engine for job scripts
- **hydra-core**: Hydra configuration system integration
- **wandb**: Weights & Biases integration
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computing

### Development Dependencies
- **pytest**: Testing framework with coverage
- **black**: Code formatting
- **isort**: Import sorting
- **flake8**: Linting
- **mypy**: Static type checking
- **pre-commit**: Git hooks for code quality

## 🙏 Acknowledgments

- Built on top of [Hydra](https://hydra.cc/) for configuration management
- Inspired by [W&B Sweeps](https://wandb.ai/site/sweeps) for experiment tracking
- Designed for HPC environments like Imperial College's CX3 and other PBS/Slurm systems
- Rich terminal UI powered by [Rich](https://rich.readthedocs.io/) and [Textual](https://textual.textualize.io/)