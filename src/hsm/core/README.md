# HSM Core Components

This directory contains the core orchestration components for HPC Sweep Manager, including the main sweep engine and new capabilities for project synchronization and result collection.

## Components

### SweepEngine (`engine.py`)
The central orchestrator that manages sweep execution across multiple compute sources, providing a unified interface regardless of where tasks are actually executed.

### Project Synchronization (`sync_manager.py`) 
Handles project synchronization between local and remote environments to ensure consistency before running distributed sweeps.

Key classes:
- `ProjectStateChecker` - Verifies that local and remote projects are in sync
- `ProjectSyncManager` - Manages syncing of files between local and remote
- `ProjectSyncError` - Exception for sync-related errors

### Result Collection (`result_collector.py`)
Manages collecting results, logs, and error information from remote compute sources after task execution.

Key classes:
- `RemoteResultCollector` - Collects results from remote compute sources via rsync/SSH
- `LocalResultCollector` - Collects results from local compute sources  
- `ResultCollectionManager` - Manages collection from multiple sources
- `ResultCollectionError` - Exception for collection-related errors

## Usage Examples

### Basic Project Sync Verification

```python
from hsm.core import ProjectStateChecker, ProjectSyncManager

# Check if local and remote projects are in sync
checker = ProjectStateChecker(
    local_project_root="/path/to/local/project",
    remote_host="compute-node.example.com", 
    remote_project_root="/path/to/remote/project",
    ssh_key="/path/to/ssh/key"
)

# Verify synchronization
sync_result = await checker.verify_project_sync()

if not sync_result["in_sync"]:
    print("Projects are not in sync!")
    
    # Optionally sync files if safe to do so
    sync_manager = ProjectSyncManager("/path/to/local/project")
    
    if sync_manager.can_offer_sync(sync_result):
        # Sync local changes to remote
        async with await checker._create_ssh_connection() as conn:
            success = await sync_manager.sync_files_to_remote(
                conn, sync_result, "/path/to/remote/project"
            )
            if success:
                print("✓ Successfully synced projects")
```

### Result Collection After Sweep

```python
from hsm.core import ResultCollectionManager

# Create result collection manager
collector_manager = ResultCollectionManager(
    local_sweep_dir=Path("./outputs/sweep_20241225_120000")
)

# Configure sources for collection
source_configs = {
    "remote_gpu_cluster": {
        "type": "remote",
        "host": "gpu-cluster.example.com",
        "ssh_key": "/path/to/ssh/key",
        "remote_sweep_dir": "/tmp/sweep_artifacts",
        "cleanup_after_sync": True
    },
    "local_machine": {
        "type": "local", 
        "task_dirs": [Path("./tasks/task_001"), Path("./tasks/task_002")]
    }
}

# Collect results from all sources
results = await collector_manager.collect_all_results(source_configs)

for source_name, success in results.items():
    if success:
        print(f"✓ Successfully collected results from {source_name}")
    else:
        print(f"✗ Failed to collect results from {source_name}")
```

### Immediate Error Collection for Failed Tasks

```python
from hsm.core import RemoteResultCollector

# Create remote collector for a specific host
collector = RemoteResultCollector(
    local_sweep_dir=Path("./outputs/sweep_20241225_120000"),
    remote_host="compute-node.example.com",
    ssh_key="/path/to/ssh/key"
)

# When a task fails, immediately collect error information
async with await collector._create_ssh_connection() as conn:
    # Collect comprehensive error information
    error_info = await collector.collect_and_analyze_task_error(
        conn, 
        remote_task_dir="/tmp/sweep/tasks/task_001",
        job_name="task_001"
    )
    
    # Save error summary locally for debugging
    await collector.save_error_summary_locally("task_001", error_info)
    
    # Immediately sync the failed task directory for investigation
    await collector.immediately_collect_failed_task(
        conn,
        remote_task_dir="/tmp/sweep/tasks/task_001", 
        job_name="task_001"
    )
```

### Integration with SweepEngine

```python
from hsm.core import SweepEngine, ProjectStateChecker, ResultCollectionManager
from hsm.compute.base import SweepContext

# Before starting a sweep, verify project sync
checker = ProjectStateChecker(
    local_project_root=Path.cwd(),
    remote_host="compute-cluster.example.com",
    remote_project_root="/home/user/project"
)

sync_result = await checker.verify_project_sync()
if not sync_result["in_sync"]:
    # Handle sync issues before proceeding
    print("Projects not in sync - please resolve before running sweep")
    return

# Set up sweep context and sources
sweep_context = SweepContext(
    sweep_id="experiment_001",
    sweep_dir=Path("./outputs/experiment_001"),
    # ... other context parameters
)

# Create and run sweep
engine = SweepEngine(sweep_context, sources)
await engine.setup_sources()

result = await engine.run_sweep(tasks)

# After sweep completion, collect all results
collector_manager = ResultCollectionManager(sweep_context.sweep_dir)

# Configure collection for all remote sources used in the sweep
source_configs = {}
for source in sources:
    if hasattr(source, 'remote_host'):
        source_configs[source.name] = {
            "type": "remote",
            "host": source.remote_host,
            "ssh_key": source.ssh_key,
            "remote_sweep_dir": source.remote_sweep_dir,
            "cleanup_after_sync": True
        }

# Collect all results
collection_results = await collector_manager.collect_all_results(source_configs)
```

## Key Features

### Project Synchronization
- **Git-based verification** - Uses git status and commit hashes to verify sync
- **Checksum-based fallback** - Falls back to file checksum verification for non-git projects
- **Safe syncing** - Analyzes changes to determine if automatic sync is safe
- **Bidirectional sync** - Can sync from local to remote or remote to local
- **Config file focus** - Special handling for configuration files

### Result Collection  
- **Rsync-based collection** - Uses rsync for efficient transfer of large result sets
- **Immediate error collection** - Collects failed task information immediately for debugging
- **Comprehensive error analysis** - Analyzes stderr, logs, and system info for failures
- **Progress reporting** - Shows collection progress and summaries
- **Multi-source support** - Can collect from multiple remote and local sources
- **Automatic cleanup** - Optionally cleans up remote directories after collection

### Error Handling and Debugging
- **Detailed error summaries** - Creates comprehensive error reports for failed tasks
- **Pattern recognition** - Recognizes common error patterns (JAX/CUDA issues, missing dependencies, etc.)
- **Immediate sync of failures** - Failed tasks are immediately synced for investigation
- **System information capture** - Captures disk space, memory, and process information at failure

## Dependencies

- `asyncssh` - Required for remote operations (SSH connections)
- `pathlib` - For path handling
- `subprocess` - For rsync and git operations
- Standard library modules: `asyncio`, `hashlib`, `logging`, etc.

Note: The modules gracefully handle missing `asyncssh` by disabling remote functionality and providing appropriate warnings. 