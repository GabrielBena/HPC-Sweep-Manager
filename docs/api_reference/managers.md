# Job Managers API Reference

This document provides a detailed reference for the job manager classes in the HPC Sweep Manager. Job managers are responsible for submitting, monitoring, and managing jobs on different compute resources.

## `BaseJobManager`

The `BaseJobManager` is an abstract base class that defines the common interface for all job managers. It provides a standardized set of methods and attributes for job submission, status tracking, and cancellation.

**Location:** `src/hpc_sweep_manager/core/common/base_manager.py`

### Class Definition

```python
class BaseJobManager(ABC):
    """Abstract base class for job managers."""

    def __init__(self, max_parallel_jobs: int, show_progress: bool):
        ...

    @abstractmethod
    def submit_single_job(self, params: Dict[str, Any], job_name: str, sweep_id: str, **kwargs) -> str:
        ...

    @abstractmethod
    def get_job_status(self, job_id: str) -> str:
        ...

    @abstractmethod
    def cancel_job(self, job_id: str, timeout: int = 10) -> bool:
        ...

    @abstractmethod
    def cancel_all_jobs(self, timeout: int = 10) -> dict:
        ...

    @abstractmethod
    def wait_for_all_jobs(self, use_progress_bar: bool = False):
        ...

    def _params_to_string(self, params: Dict[str, Any]) -> str:
        ...
```

### Key Methods

-   `submit_single_job`: Submits a single job with the given parameters.
-   `get_job_status`: Retrieves the status of a specific job.
-   `cancel_job`: Cancels a running job.
-   `cancel_all_jobs`: Cancels all running jobs managed by the manager.
-   `wait_for_all_jobs`: Blocks until all submitted jobs have completed.
-   `_params_to_string`: A helper method to convert a dictionary of parameters into a command-line string.

---

## `LocalJobManager`

The `LocalJobManager` is a concrete implementation of `BaseJobManager` that runs jobs on the local machine. It is ideal for smaller sweeps or for testing scripts before deploying them to a remote or HPC environment.

**Location:** `src/hpc_sweep_manager/core/local/local_manager.py`

### Key Features

-   Runs jobs as local subprocesses.
-   Manages parallel execution with a configurable `max_parallel_jobs`.
-   Supports real-time output streaming for debugging.
-   Provides graceful shutdown and cleanup of child processes on interruption.

---

## `RemoteJobManager`

The `RemoteJobManager` manages jobs on remote machines via SSH. It is designed to work with remote servers or compute nodes where you have SSH access.

**Location:** `src/hpc_sweep_manager/core/remote/remote_manager.py`

### Key Features

-   Connects to remote machines using `asyncssh`.
-   Verifies project synchronization between the local and remote machines to ensure code consistency.
-   Submits jobs as background processes on the remote machine.
-   Collects results and logs back to the local machine.
-   Handles graceful cancellation of remote processes.

---

## `DistributedJobManager`

The `DistributedJobManager` is a higher-level manager that distributes jobs across multiple compute sources, which can be a mix of local, remote, or HPC resources. It orchestrates the entire distributed sweep.

**Location:** `src/hpc_sweep_manager/core/distributed/ditributed_manager.py`

### Key Features

-   Manages a collection of `ComputeSource` objects.
-   Distributes jobs based on a selected strategy (e.g., round-robin, least loaded).
-   Monitors the health of compute sources and can disable unhealthy ones.
-   Aggregates results from all sources into a unified sweep directory.

This manager does not inherit from `BaseJobManager` because it operates at a higher level of abstraction, coordinating other managers rather than directly managing jobs. 