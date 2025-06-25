# Local Execution Module

The Local execution module enables parallel job execution on a single machine using Python's ThreadPoolExecutor.

## Overview

Local execution is ideal for:
- Development and testing
- Small to medium parameter sweeps
- Multi-core workstations
- Situations where HPC access is not available

## Components

### `LocalComputeSource`

The main compute source class for local execution.

```python
class LocalComputeSource(ComputeSource):
    def __init__(
        self,
        max_parallel_jobs: int = 4,
        timeout: Optional[int] = None,
        show_output: bool = False
    )
```

**Parameters:**
- `max_parallel_jobs`: Maximum number of concurrent jobs
- `timeout`: Job timeout in seconds (None for no timeout)
- `show_output`: Whether to display job output in real-time

### `LocalManager`

Higher-level manager for local job execution with additional features.

```python
class LocalManager:
    def __init__(
        self,
        max_parallel_jobs: int = 4,
        timeout: Optional[int] = None,
        show_output: bool = False,
        cleanup_on_exit: bool = True
    )
```

**Additional Features:**
- Automatic cleanup of temporary files
- Enhanced error handling and recovery
- Job progress tracking
- Resource usage monitoring

## Usage Examples

### Basic Local Execution

```python
from hpc_sweep_manager.core.local import LocalComputeSource

# Create local compute source
local_source = LocalComputeSource(
    max_parallel_jobs=4,
    show_output=True
)

# Submit jobs
job_ids = local_source.submit_jobs(
    param_combinations=[
        {"lr": 0.001, "epochs": 10},
        {"lr": 0.01, "epochs": 20},
    ],
    sweep_id="local_test",
    sweep_dir=Path("outputs/local_test")
)

# Monitor progress
for job_id in job_ids:
    status = local_source.get_job_status(job_id)
    print(f"Job {job_id}: {status}")
```

### With JobManager Integration

```python
from hpc_sweep_manager.core.job_manager import JobManager
from hpc_sweep_manager.core.local import LocalComputeSource

manager = JobManager()
local_source = LocalComputeSource(max_parallel_jobs=2)

# Full sweep workflow
job_ids = manager.submit_sweep(
    param_combinations=params,
    compute_source=local_source,
    sweep_id="local_sweep",
    sweep_dir=Path("outputs/local_sweep")
)

# Monitor and collect
statuses = manager.monitor_jobs(job_ids, local_source)
results = manager.collect_results(Path("outputs/local_sweep"))
```

### Advanced Configuration

```python
# High-performance local execution
local_source = LocalComputeSource(
    max_parallel_jobs=8,  # Use all cores
    timeout=3600,         # 1 hour timeout
    show_output=False     # Silent execution
)

# Resource-constrained execution
local_source = LocalComputeSource(
    max_parallel_jobs=2,  # Limit parallelism
    timeout=1800,         # 30 minute timeout
    show_output=True      # Monitor progress
)
```

## Features

### Parallel Execution
- Uses ThreadPoolExecutor for efficient parallelism
- Automatic load balancing across available cores
- Configurable concurrency limits

### Real-time Monitoring
- Live job status updates
- Optional real-time output display
- Progress tracking and ETA estimation

### Error Handling
- Graceful handling of job failures
- Automatic retry mechanisms
- Detailed error logging

### Resource Management
- Automatic cleanup of temporary files
- Memory usage monitoring
- Process isolation

## Configuration

Local execution can be configured via:

1. **Direct parameters** in constructor
2. **HSM config file** under `local` section
3. **Environment variables** for system-wide defaults

### HSM Config Example

```yaml
local:
  max_parallel_jobs: 4
  timeout: 3600
  show_output: false
  cleanup_on_exit: true
```

### Environment Variables

```bash
export HSM_LOCAL_MAX_JOBS=8
export HSM_LOCAL_TIMEOUT=7200
export HSM_LOCAL_SHOW_OUTPUT=true
```

## Performance Considerations

### Optimal Parallelism
- **CPU-bound tasks**: Set to number of CPU cores
- **I/O-bound tasks**: Can exceed core count (2-4x)
- **Memory-intensive**: Reduce based on available RAM

### Best Practices
1. **Start small**: Begin with 2-4 parallel jobs
2. **Monitor resources**: Watch CPU, memory, and disk usage
3. **Use timeouts**: Prevent runaway jobs
4. **Enable cleanup**: Avoid disk space issues

## Limitations

- **Single machine only**: Cannot distribute across multiple nodes
- **Shared resources**: All jobs compete for same CPU/memory
- **No fault tolerance**: Machine failure affects all jobs
- **Limited scalability**: Bounded by single machine resources

## Integration with CLI

The local module integrates seamlessly with HSM CLI commands:

```bash
# Local mode execution
hsm sweep --mode local --parallel-jobs 4 --show-output

# Local monitoring
hsm monitor --mode local --sweep-id local_test

# Local result collection
hsm collect-results --sweep-id local_test
``` 