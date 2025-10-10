# HPC Execution Module

The HPC execution module provides interfaces for submitting jobs to High Performance Computing clusters using schedulers like PBS/Torque and Slurm.

## Overview

HPC execution supports:
- **PBS/Torque** clusters with `qsub`/`qstat` commands
- **Slurm** clusters with `sbatch`/`squeue` commands
- **Array jobs** for efficient large-scale execution
- **Individual jobs** for maximum flexibility
- **Auto-detection** of available HPC systems

## Components

### `HPCJobManager` (Base Class)

Abstract base class defining the HPC interface.

```python
class HPCJobManager(ABC):
    def __init__(
        self,
        walltime: str = "04:00:00",
        resources: str = "",
        python_path: str = "python",
        script_path: str = "",
        project_dir: str = "."
    )
```

### `PBSJobManager`

Implementation for PBS/Torque clusters.

```python
class PBSJobManager(HPCJobManager):
    @property
    def scheduler_name(self) -> str:
        return "pbs"
    
    @property
    def submit_command(self) -> str:
        return "qsub"
    
    @property
    def status_command(self) -> str:
        return "qstat"
```

### `SlurmJobManager`

Implementation for Slurm clusters.

```python
class SlurmJobManager(HPCJobManager):
    @property
    def scheduler_name(self) -> str:
        return "slurm"
    
    @property
    def submit_command(self) -> str:
        return "sbatch"
    
    @property
    def status_command(self) -> str:
        return "squeue"
```

## Auto-Detection

HSM automatically detects the available HPC system:

```python
from hpc_sweep_manager.core.hpc import HPCJobManager

# Auto-detect and create appropriate manager
hpc_manager = HPCJobManager.auto_detect(
    walltime="04:00:00",
    resources="select=1:ncpus=4:mem=16gb"
)

# Returns PBSJobManager or SlurmJobManager based on system
```

## Usage Examples

### Basic HPC Submission

```python
from hpc_sweep_manager.core.hpc import HPCJobManager

# Auto-detect HPC system
hpc_manager = HPCJobManager.auto_detect()

# Submit individual jobs
job_ids = []
for i, params in enumerate(param_combinations):
    job_id = hpc_manager.submit_single_job(
        params=params,
        job_name=f"sweep_task_{i}",
        sweep_dir=Path("outputs/hpc_sweep"),
        sweep_id="hpc_sweep"
    )
    job_ids.append(job_id)
```

### Array Job Submission

```python
# Submit as array job (more efficient)
array_job_id = hpc_manager.submit_array_job(
    param_combinations=param_combinations,
    sweep_id="hpc_array_sweep",
    sweep_dir=Path("outputs/hpc_array_sweep")
)
```

### Custom Resources

```python
# PBS/Torque resources
pbs_manager = HPCJobManager.auto_detect(
    walltime="08:00:00",
    resources="select=2:ncpus=8:mem=32gb"
)

# Slurm resources  
slurm_manager = HPCJobManager.auto_detect(
    walltime="08:00:00",
    resources="--nodes=2 --ntasks-per-node=8 --mem=32G"
)
```

## Job Submission Modes

### Individual Jobs (`mode="individual"`)

Each parameter combination becomes a separate job:
- **Pros**: Maximum flexibility, easy debugging, independent execution
- **Cons**: More scheduler overhead, slower submission
- **Best for**: Small sweeps, debugging, heterogeneous parameters

```python
job_ids = hpc_manager.submit_sweep(
    param_combinations=params,
    mode="individual",
    sweep_dir=Path("outputs/individual"),
    sweep_id="individual_sweep"
)
```

### Array Jobs (`mode="array"`)

All parameter combinations in a single array job:
- **Pros**: Efficient submission, reduced scheduler load, faster startup
- **Cons**: All tasks share same resources, harder to debug individual tasks
- **Best for**: Large sweeps, homogeneous parameters, production runs

```python
job_ids = hpc_manager.submit_sweep(
    param_combinations=params,
    mode="array",
    sweep_dir=Path("outputs/array"),
    sweep_id="array_sweep"
)
```

## Resource Specification

### PBS/Torque Format

```python
# Basic resources
resources = "select=1:ncpus=4:mem=16gb"

# Advanced resources
resources = "select=2:ncpus=8:mem=32gb:ngpus=1"

# Multiple node types
resources = "select=1:ncpus=4:mem=16gb+1:ncpus=8:mem=32gb"
```

### Slurm Format

```python
# Basic resources
resources = "--nodes=1 --ntasks-per-node=4 --mem=16G"

# GPU resources
resources = "--nodes=1 --ntasks-per-node=4 --mem=16G --gres=gpu:1"

# Partition specification
resources = "--partition=gpu --nodes=1 --gres=gpu:v100:2"
```

## Job Monitoring

### Status Checking

```python
# Check individual job status
status = hpc_manager.get_job_status("12345")
print(f"Job status: {status}")  # "queued", "running", "completed", "failed"

# Monitor multiple jobs
statuses = {}
for job_id in job_ids:
    statuses[job_id] = hpc_manager.get_job_status(job_id)
```

### Job States

Common job states across schedulers:
- **`queued`**: Job waiting in queue
- **`running`**: Job currently executing
- **`completed`**: Job finished successfully
- **`failed`**: Job terminated with error
- **`cancelled`**: Job was cancelled by user
- **`unknown`**: Status could not be determined

## Generated Job Scripts

HSM automatically generates appropriate job scripts:

### PBS Script Example

```bash
#!/bin/bash
#PBS -N sweep_task_001
#PBS -l walltime=04:00:00
#PBS -l select=1:ncpus=4:mem=16gb
#PBS -j oe
#PBS -o logs/task_001.log

cd $PBS_O_WORKDIR
python train.py "lr=0.001" "batch_size=32"
```

### Slurm Script Example

```bash
#!/bin/bash
#SBATCH --job-name=sweep_task_001
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --mem=16G
#SBATCH --output=logs/task_001.log

python train.py "lr=0.001" "batch_size=32"
```

## Configuration

### HSM Config File

```yaml
hpc:
  default_walltime: "04:00:00"
  default_resources: "select=1:ncpus=4:mem=16gb"
  python_interpreter: "python"
  system: "auto"  # or "pbs", "slurm"
  
  # PBS-specific settings
  pbs:
    queue: "normal"
    account: "my_project"
    
  # Slurm-specific settings  
  slurm:
    partition: "compute"
    account: "my_account"
```

### Environment Variables

```bash
export HSM_HPC_WALLTIME="08:00:00"
export HSM_HPC_RESOURCES="select=2:ncpus=8:mem=32gb"
export HSM_HPC_SYSTEM="pbs"
```

## Error Handling

Common HPC errors and solutions:

### Submission Errors
- **Queue not available**: Check queue name and permissions
- **Resource limits**: Reduce requested resources
- **Script errors**: Validate generated job scripts

### Runtime Errors
- **Module loading**: Ensure required modules are available
- **Path issues**: Use absolute paths or proper working directory
- **Permission errors**: Check file and directory permissions

### Monitoring Errors
- **Connection timeouts**: Increase polling intervals
- **Authentication**: Ensure proper HPC credentials
- **Command failures**: Verify scheduler commands are available

## Best Practices

### Resource Planning
1. **Start conservative**: Begin with minimal resources
2. **Profile first**: Run single jobs to estimate requirements
3. **Consider queue limits**: Check cluster policies and limits
4. **Plan for failures**: Account for node failures and preemption

### Job Organization
1. **Use array jobs** for large homogeneous sweeps
2. **Use individual jobs** for heterogeneous or debugging scenarios
3. **Organize outputs** with clear directory structures
4. **Enable logging** for all job types

### Monitoring Strategy
1. **Set reasonable timeouts** to avoid hanging jobs
2. **Monitor regularly** but don't overwhelm the scheduler
3. **Use job dependencies** for complex workflows
4. **Plan result collection** before submission

## Integration with CLI

HPC execution integrates with HSM CLI commands:

```bash
# Array job submission
hsm sweep run --mode array --walltime "08:00:00" --resources "select=2:ncpus=8:mem=32gb"

# Individual job submission
hsm sweep run --mode individual --walltime "04:00:00"

# HPC-specific monitoring
hsm monitor --mode hpc --sweep-id hpc_sweep

# Queue status
hsm queue --watch
``` 