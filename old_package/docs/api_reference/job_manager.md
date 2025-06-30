# Job Manager

The `JobManager` is the central orchestration component of HSM that coordinates job execution across different compute sources.

## Overview

The JobManager provides a unified interface for submitting, monitoring, and managing parameter sweep jobs regardless of the underlying execution backend (local, HPC, remote, or distributed).

## Class: `JobManager`

### Constructor

```python
def __init__(
    self,
    config: Optional[Dict] = None,
    project_dir: Optional[Path] = None,
    verbose: bool = False
)
```

**Parameters:**
- `config` (Dict, optional): HSM configuration dictionary
- `project_dir` (Path, optional): Project root directory path
- `verbose` (bool): Enable verbose logging

### Key Methods

#### `submit_sweep()`

```python
def submit_sweep(
    self,
    param_combinations: List[Dict[str, Any]],
    compute_source: ComputeSource,
    sweep_id: str,
    sweep_dir: Path,
    **kwargs
) -> List[str]
```

Submit a complete parameter sweep to the specified compute source.

**Parameters:**
- `param_combinations`: List of parameter dictionaries for each job
- `compute_source`: Compute backend to execute jobs
- `sweep_id`: Unique identifier for the sweep
- `sweep_dir`: Directory for sweep outputs

**Returns:** List of job IDs

#### `monitor_jobs()`

```python
def monitor_jobs(
    self,
    job_ids: List[str],
    compute_source: ComputeSource,
    interval: int = 30
) -> Dict[str, str]
```

Monitor job progress and return status updates.

**Parameters:**
- `job_ids`: List of job IDs to monitor
- `compute_source`: Compute source where jobs are running
- `interval`: Polling interval in seconds

**Returns:** Dictionary mapping job ID to status

#### `collect_results()`

```python
def collect_results(
    self,
    sweep_dir: Path,
    compute_source: Optional[ComputeSource] = None
) -> Dict[str, Any]
```

Collect and aggregate results from completed jobs.

**Parameters:**
- `sweep_dir`: Sweep directory containing job outputs
- `compute_source`: Optional compute source for remote collection

**Returns:** Aggregated results dictionary

## Usage Examples

### Basic Sweep Submission

```python
from hpc_sweep_manager.core.job_manager import JobManager
from hpc_sweep_manager.core.local import LocalComputeSource

# Initialize manager
manager = JobManager(verbose=True)

# Create parameter combinations
params = [
    {"lr": 0.001, "batch_size": 32},
    {"lr": 0.01, "batch_size": 64},
]

# Submit to local compute source
local_source = LocalComputeSource(max_parallel_jobs=2)
job_ids = manager.submit_sweep(
    param_combinations=params,
    compute_source=local_source,
    sweep_id="test_sweep",
    sweep_dir=Path("outputs/test_sweep")
)
```

### With HPC Backend

```python
from hpc_sweep_manager.core.hpc import HPCJobManager

# Auto-detect HPC system
hpc_source = HPCJobManager.auto_detect(
    walltime="04:00:00",
    resources="select=1:ncpus=4:mem=16gb"
)

job_ids = manager.submit_sweep(
    param_combinations=params,
    compute_source=hpc_source,
    sweep_id="hpc_sweep",
    sweep_dir=Path("outputs/hpc_sweep")
)
```

### Monitoring and Collection

```python
# Monitor job progress
while True:
    statuses = manager.monitor_jobs(job_ids, local_source)
    if all(status in ["completed", "failed"] for status in statuses.values()):
        break
    time.sleep(30)

# Collect results
results = manager.collect_results(Path("outputs/test_sweep"))
```

## Error Handling

The JobManager provides comprehensive error handling:

- **Configuration Errors**: Invalid or missing configuration
- **Path Errors**: Inaccessible directories or files
- **Submission Errors**: Failed job submissions
- **Monitoring Errors**: Communication failures with compute sources
- **Collection Errors**: Missing or corrupted result files

## Integration Points

The JobManager integrates with:

1. **CLI Commands**: All CLI commands use JobManager for orchestration
2. **Compute Sources**: Supports all HSM compute backends
3. **Configuration System**: Automatically loads HSM config
4. **Path Detection**: Uses path detector for automatic setup
5. **Parameter Generation**: Works with parameter sweep generators

## Thread Safety

The JobManager is designed to be thread-safe for concurrent operations:
- Job submission and monitoring can run in parallel
- Result collection is atomic
- Status updates are synchronized 