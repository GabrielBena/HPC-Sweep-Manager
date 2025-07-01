# Compute Sources API Reference

This document provides a detailed reference for the compute source classes in the HPC Sweep Manager. Compute sources are abstractions that represent different environments where jobs can be executed, such as a local machine or a remote server.

## `ComputeSource`

The `ComputeSource` is an abstract base class that defines the common interface for all compute sources. It provides a standardized way for the `DistributedJobManager` to interact with different execution environments.

**Location:** `src/hpc_sweep_manager/core/common/compute_source.py`

### Class Definition

```python
class ComputeSource(ABC):
    """Abstract base class for compute sources."""

    def __init__(self, name: str, source_type: str, max_parallel_jobs: int):
        ...

    @abstractmethod
    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        ...

    @abstractmethod
    async def submit_job(self, params: Dict[str, Any], job_name: str, sweep_id: str, wandb_group: Optional[str] = None) -> str:
        ...

    @abstractmethod
    async def get_job_status(self, job_id: str) -> str:
        ...

    @abstractmethod
    async def cancel_job(self, job_id: str) -> bool:
        ...

    @abstractmethod
    async def collect_results(self, job_ids: Optional[List[str]] = None) -> bool:
        ...

    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    async def cleanup(self):
        ...
```

### Key Methods

-   `setup`: Prepares the compute source for job execution.
-   `submit_job`: Submits a single job to the compute source.
-   `get_job_status`: Retrieves the status of a specific job.
-   `cancel_job`: Cancels a running job.
-   `collect_results`: Collects results from completed jobs.
-   `health_check`: Performs a health check on the compute source to ensure it is operational.
-   `cleanup`: Cleans up any resources used by the compute source.

---

## `LocalComputeSource`

The `LocalComputeSource` is a concrete implementation of `ComputeSource` that wraps the `LocalJobManager`. It allows the `DistributedJobManager` to run jobs on the local machine.

**Location:** `src/hpc_sweep_manager/core/local/local_compute_source.py`

### Key Features

-   Executes jobs locally.
-   Integrates with the `DistributedJobManager` to run a portion of a distributed sweep on the local machine.
-   Useful for utilizing local resources in a larger, distributed sweep.

---

## `SSHComputeSource`

The `SSHComputeSource` is a concrete implementation of `ComputeSource` that wraps the `RemoteJobManager`. It enables the `DistributedJobManager` to run jobs on remote machines via SSH.

**Location:** `src/hpc_sweep_manager/core/remote/ssh_compute_source.py`

### Key Features

-   Manages job execution on a remote server.
-   Handles the setup and synchronization of the project directory on the remote machine.
-   Collects job results and logs from the remote server back to the local sweep directory.
-   Performs health checks to monitor the status of the remote machine. 