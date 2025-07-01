# Distributed Sweep Execution with HSM v2

This guide covers the advanced distributed sweep execution capabilities of HPC Sweep Manager v2, including multi-source coordination, health monitoring, and automatic failover.

## Overview

HSM v2's distributed execution allows you to run parameter sweeps across multiple compute sources simultaneously, with intelligent task distribution, real-time health monitoring, and unified result collection.

### Key Benefits

🚀 **Faster Execution**: Distribute tasks across multiple machines/clusters
🔧 **Automatic Failover**: Continue execution if sources become unavailable  
📊 **Load Balancing**: Intelligent task distribution based on source capacity
🔍 **Unified Monitoring**: Single view of progress across all sources
📁 **Centralized Results**: Automatic collection from all compute sources
🔄 **Cross-Mode Completion**: Resume with different source configurations

## Architecture

### Unified SweepEngine

The SweepEngine orchestrates all distributed execution:

```python
# Conceptual architecture
SweepEngine
├── Task Distribution (round-robin, least-loaded, etc.)
├── Health Monitoring (automatic source disabling)
├── Result Collection (continuous sync from remote sources)
├── Progress Tracking (real-time status across sources)
└── Error Management (immediate failure analysis)
```

### Compute Source Types

1. **Local**: Execute on the current machine
2. **SSH**: Execute on remote machines via SSH
3. **HPC**: Execute on HPC clusters (PBS/Slurm)
4. **Distributed**: Coordinate across multiple source types

## Configuration

### Multiple Compute Sources

Configure multiple sources in your `hsm_config.yaml`:

```yaml
# hsm_config.yaml
compute_sources:
  local:
    type: local
    max_parallel_tasks: 4
    
  server1:
    type: ssh
    host: compute-server-1.example.com
    max_parallel_tasks: 8
    ssh_key: ~/.ssh/id_rsa
    project_dir: /home/user/project
    conda_env: myenv
    
  server2:
    type: ssh  
    host: compute-server-2.example.com
    max_parallel_tasks: 16
    ssh_key: ~/.ssh/id_rsa
    project_dir: /home/user/project
    conda_env: myenv
    
  cluster:
    type: hpc
    system: slurm
    max_parallel_tasks: 32
    default_resources: "nodes=1:ntasks=4:mem=32GB"
    default_walltime: "12:00:00"
    queue: gpu
    module_commands:
      - "module load python/3.9"
      - "module load cuda/11.8"

# Health monitoring settings
compute:
  health_check_interval: 300  # 5 minutes
  result_collection_interval: 60  # 1 minute
  source_failure_threshold: 0.4  # Disable if 40% of tasks fail
  auto_disable_unhealthy_sources: true
```

## Execution Strategies

### Basic Distributed Execution

```bash
# Run across all configured sources
hsm run --config sweep.yaml --sources all

# Specify sources explicitly
hsm run --config sweep.yaml --sources "local,server1,server2,cluster"

# With custom task limits per source
hsm run --config sweep.yaml --sources "local:4,server1:8,server2:16,cluster:32"
```

### Distribution Strategies

#### Round-Robin (Default)
Tasks are distributed evenly across sources in rotation:

```bash
hsm run --config sweep.yaml --sources "local,server1,server2" --strategy round_robin
```

Task assignment pattern: local → server1 → server2 → local → server1 → ...

#### Least-Loaded
Tasks go to the source with the lowest current utilization:

```bash
hsm run --config sweep.yaml --sources "local,server1,server2" --strategy least_loaded
```

Automatically balances load based on source capacity and current usage.

#### Random
Random task assignment for load balancing:

```bash
hsm run --config sweep.yaml --sources "local,server1,server2" --strategy random
```

Useful when sources have varying performance characteristics.

### Resource-Aware Distribution

```bash
# Prioritize high-capacity sources
hsm run --config sweep.yaml --sources "local:2,server1:8,cluster:64"

# Mix different source types optimally
hsm run --config sweep.yaml --sources "local:4,ssh:gpu-server:16,hpc:cpu-cluster:32"
```

## Health Monitoring and Failover

### Automatic Health Checks

HSM v2 continuously monitors source health:

```bash
# View current health status
hsm monitor health

# Health check output
Source Health Status:
┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Source     ┃ Status        ┃ Details                    ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ local      │ ✅ Healthy    │ 2/4 tasks, CPU: 45%       │
│ server1    │ ⚠️ Degraded   │ 8/8 tasks, high load      │
│ server2    │ ❌ Unhealthy  │ Connection timeout         │
│ cluster    │ ✅ Healthy    │ 16/32 tasks, GPU: 60%     │
└────────────┴───────────────┴────────────────────────────┘
```

### Failure Detection and Response

**Health Check Failures:**
- SSH connection timeouts
- High system load or resource exhaustion
- Repeated task failures

**Automatic Actions:**
```bash
# Sources automatically disabled after consecutive failures
[2024-01-15 14:30:22] WARNING - Health check failed for server2: SSH timeout (consecutive failures: 2)
[2024-01-15 14:35:23] ERROR - Disabling source 'server2' due to 3 consecutive health check failures

# Tasks redistributed to healthy sources
[2024-01-15 14:35:24] INFO - Redistributing 4 pending tasks from disabled source 'server2'
[2024-01-15 14:35:25] INFO - Tasks reassigned: local(2), server1(2)
```

### Manual Source Management

```bash
# Disable a problematic source
hsm sources disable server2

# Re-enable after fixing issues  
hsm sources enable server2

# Check source status
hsm sources status

# Test connectivity before re-enabling
hsm sources test server2
```

## Live Monitoring

### Real-Time Progress Tracking

```bash
# Watch sweep progress across all sources
hsm monitor watch <sweep_id>

# Live output
╭─────────────────────────────────────────────────────────────────╮
│ 🔄 Distributed Sweep Monitor - sweep_20240115_143022           │
│ 📊 Live updates every 5s - Press Ctrl+C to exit               │
╰─────────────────────────────────────────────────────────────────╯

Overall Progress:
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Metric        ┃ Value                                          ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Status        │ 🏃 Running                                    │
│ Progress      │ ████████████░░░░░░░░░░░░ 48/100 (48%)        │
│ Completed     │ 45 tasks                                       │
│ Failed        │ 2 tasks                                        │
│ Active        │ 3 tasks                                        │
│ Pending       │ 50 tasks                                       │
│ Success Rate  │ 95.7%                                          │
│ ETA           │ ~2h 15m remaining                              │
└───────────────┴────────────────────────────────────────────────┘

Source Utilization:
┏━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┓
┃ Source     ┃ Active     ┃ Completed  ┃ Failed         ┃ Health Status   ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━┩
│ local      │ 2/4        │ 12         │ 0              │ ✅ Healthy      │
│ server1    │ 1/8        │ 18         │ 1              │ ⚠️ Degraded     │
│ server2    │ 0/16       │ 0          │ 0              │ ❌ Disabled     │
│ cluster    │ 0/32       │ 15         │ 1              │ ✅ Healthy      │
└────────────┴────────────┴────────────┴────────────────┴─────────────────┘

Recent Activity:
[14:45:23] ✅ Task task_045 completed on cluster (3.2s)
[14:45:24] 🚀 Task task_048 started on local  
[14:45:25] ❌ Task task_047 failed on server1 (timeout)
[14:45:26] 🔄 Task task_047 reassigned to cluster
```

### Detailed Source Monitoring

```bash
# Monitor specific source performance
hsm monitor source server1

# Source-specific metrics
Server1 Performance:
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Metric                     ┃ Value                         ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Tasks Submitted            │ 24                            │
│ Tasks Completed            │ 22                            │
│ Tasks Failed               │ 2                             │
│ Success Rate               │ 91.7%                         │
│ Average Task Duration      │ 4.8 minutes                   │
│ Current Utilization        │ 6/8 slots (75%)               │
│ CPU Load                   │ 3.2 (high)                    │
│ Memory Usage               │ 28.4GB/32GB (89%)             │
│ Network Latency            │ 12ms                          │
│ Last Health Check          │ 2 minutes ago (healthy)       │
└────────────────────────────┴───────────────────────────────┘
```

## Result Collection

### Unified Result Aggregation

HSM v2 automatically collects results from all sources:

```bash
# Results collected continuously during execution
[14:30:15] 📁 Collecting results from server1 (5 completed tasks)
[14:30:17] 📁 Collecting results from cluster (3 completed tasks)
[14:30:20] ✅ Results synchronized: 8 tasks collected

# Final comprehensive collection after sweep completion
[16:45:30] 🔄 Performing final result collection...
[16:45:32] 📊 Comprehensive collection from 3 sources
[16:45:35] ✅ Result collection completed: 3/3 sources successful
[16:45:36] 📋 Aggregated sweep logs written to: logs/sweep_aggregated.log
```

### Result Directory Structure

```
sweeps/outputs/sweep_20240115_143022/
├── tasks/
│   ├── task_001/    # From local
│   ├── task_002/    # From server1  
│   ├── task_003/    # From cluster
│   └── ...
├── logs/
│   ├── sweep_aggregated.log      # All logs combined
│   ├── task_assignments.log      # Task→source mapping
│   ├── source_utilization.log    # Resource usage over time
│   ├── health_status.log         # Health monitoring events
│   └── sources/
│       ├── local.log
│       ├── server1.log
│       └── cluster.log
└── reports/
    ├── performance_analysis.yaml
    └── source_utilization.html
```

## Cross-Mode Completion

### Resume with Different Sources

```bash
# Original sweep using multiple sources
hsm run --config sweep.yaml --sources "server1,server2,cluster"

# Server2 fails, complete with different sources
hsm complete sweep_20240115_143022 --sources "local,server1,backup-cluster"

# Retry failed tasks specifically
hsm complete sweep_20240115_143022 --sources "server1,cluster" --retry-failed
```

### Completion Strategies

**Missing Tasks Only (default):**
```bash
hsm complete <sweep_id> --sources "local,server1"
```

**Failed Tasks + Missing:**
```bash  
hsm complete <sweep_id> --sources "local,server1" --retry-failed
```

**Selective Completion:**
```bash
# Complete only specific task ranges
hsm complete <sweep_id> --sources local --tasks "task_050:task_100"

# Complete with higher resource allocation
hsm complete <sweep_id> --sources "hpc:large-cluster:64"
```

## Error Analysis and Debugging

### Immediate Error Collection

Failed tasks are immediately analyzed and collected:

```bash
# Automatic error analysis
[14:25:30] ❌ Task task_023 failed on server1 (exit code: 2)
[14:25:31] 🔍 Collecting error details for task_023...
[14:25:33] 📄 Error summary saved: errors/task_023_error.txt
[14:25:34] 📁 Failed task synced to: failed_tasks/task_023/

# Error categorization
[14:25:35] 🔍 Error pattern detected: "CUDA out of memory" (3 tasks)
[14:25:36] 💡 Suggestion: Reduce batch size or use CPU-only execution
```

### Error Analysis Commands

```bash
# Analyze all errors in a sweep
hsm monitor errors <sweep_id>

# Error analysis output
Error Analysis for sweep_20240115_143022:
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ 🔍 Error Pattern Analysis                                           ┃ 
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

Error Categories:
┏━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Error Type               ┃ Count      ┃ Affected Sources             ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ CUDA out of memory       │ 3 tasks    │ server1, cluster             │
│ Connection timeout       │ 2 tasks    │ server2                      │
│ Import error             │ 1 task     │ local                        │
└──────────────────────────┴────────────┴──────────────────────────────┘

Failed Tasks by Source:
┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Source     ┃ Failed Tasks                                                 ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ server1    │ task_023, task_045, task_067 (GPU memory issues)           │
│ server2    │ task_034, task_056 (connection timeouts)                   │
│ local      │ task_012 (missing dependency)                               │
└────────────┴──────────────────────────────────────────────────────────────┘

Recommendations:
• Reduce batch_size parameter for GPU memory errors
• Check server2 connectivity and SSH configuration  
• Install missing dependencies on local environment
• Consider using CPU-only execution for memory-intensive tasks
```

### Source-Specific Error Analysis

```bash
# Analyze errors for specific source
hsm monitor errors <sweep_id> --source server1

# Show error details for specific tasks
hsm monitor errors <sweep_id> --tasks "task_023,task_045"

# Export error analysis
hsm monitor errors <sweep_id> --export errors_analysis.yaml
```

## Performance Optimization

### Resource Allocation Strategies

**Compute-Intensive Tasks:**
```bash
# Prefer HPC clusters for CPU-intensive work
hsm run --config sweep.yaml --sources "hpc:cpu-cluster:64,local:2"
```

**Memory-Intensive Tasks:**
```bash  
# Use high-memory sources
hsm run --config sweep.yaml --sources "ssh:highmem-server:8,local:2"
```

**GPU-Accelerated Tasks:**
```bash
# Target GPU-enabled sources
hsm run --config sweep.yaml --sources "hpc:gpu-cluster:16,ssh:gpu-server:4"
```

### Network Optimization

```yaml
# hsm_config.yaml - Optimize for network efficiency
compute:
  result_collection_interval: 300  # Collect less frequently for slow networks
  ssh_compression: true            # Enable SSH compression
  rsync_compression: true          # Enable rsync compression
  
# Batch result collection
result_collection:
  batch_size: 10                   # Collect multiple tasks at once
  parallel_transfers: 4            # Concurrent transfer streams
```

### Source Priority and Weighting

```bash
# Weighted task distribution based on source performance
hsm run --config sweep.yaml --sources "local:2,server1:4,cluster:8" --strategy weighted

# Priority-based distribution (prefer faster sources)
hsm run --config sweep.yaml --sources "cluster:high,server1:medium,local:low"
```

## Best Practices

### 1. Source Configuration

**Diversify Source Types:**
```yaml
# Mix different source types for redundancy
compute_sources:
  local:     { type: local, max_parallel_tasks: 4 }
  remote:    { type: ssh, max_parallel_tasks: 8 }
  cluster:   { type: hpc, max_parallel_tasks: 32 }
  backup:    { type: ssh, max_parallel_tasks: 4 }  # Backup source
```

**Resource Right-Sizing:**
```yaml
# Match resource allocation to task requirements
server1:
  max_parallel_tasks: 8   # Don't over-allocate
  resources: "mem=16GB"   # Appropriate memory
  
cluster:
  max_parallel_tasks: 64  # Leverage cluster capacity  
  resources: "nodes=4:ntasks=16"
```

### 2. Health Monitoring

**Conservative Health Settings:**
```yaml
compute:
  health_check_interval: 180        # Check frequently (3 min)
  source_failure_threshold: 0.3     # Disable at 30% failure rate
  health_check_failure_threshold: 2  # Disable after 2 consecutive failures
  auto_disable_unhealthy_sources: true
```

**Monitor Resource Usage:**
```bash
# Regular health checks during long sweeps
hsm monitor health --watch --interval 300

# Set up alerts for source failures
hsm monitor health --alert-on-failure --email admin@example.com
```

### 3. Error Recovery

**Automatic Retry Logic:**
```yaml
# Task-level retry configuration
compute:
  retry_attempts: 3          # Retry failed tasks
  retry_delay: 60           # Wait between retries
  exponential_backoff: true # Increase delay exponentially
```

**Graceful Degradation:**
```bash
# Start with all sources, allow automatic fallback
hsm run --config sweep.yaml --sources "cluster,server1,server2,local" --allow-degraded
```

### 4. Result Management

**Immediate Collection:**
```yaml
result_collection:
  immediate_error_collection: true    # Collect failures immediately  
  continuous_sync: true              # Sync results during execution
  cleanup_remote_on_success: true    # Clean up remote files after sync
```

**Comprehensive Logging:**
```yaml
logging:
  level: INFO                    # Detailed logging
  file_logging: true            # Log to files
  source_specific_logs: true    # Per-source log files
  aggregated_logs: true         # Combined log file
```

## Troubleshooting

### Common Issues

**Source Connection Problems:**
```bash
# Test connectivity to all sources
hsm sources test --all

# Diagnose specific source issues
hsm sources diagnose server1

# Check SSH configuration
hsm sources validate ssh:server1 --verbose
```

**Task Distribution Issues:**
```bash
# Check task assignment balance
hsm monitor distribution <sweep_id>

# Force rebalancing
hsm sources rebalance <sweep_id>

# Manual task reassignment
hsm tasks reassign task_023 --from server2 --to server1
```

**Performance Problems:**
```bash
# Analyze source performance
hsm monitor performance <sweep_id>

# Identify bottlenecks
hsm analyze bottlenecks <sweep_id>

# Optimize resource allocation
hsm sources optimize <sweep_id>
```

### Recovery Procedures

**Complete Source Failure:**
```bash
# 1. Identify failed source
hsm monitor health

# 2. Disable permanently failed source  
hsm sources disable server2

# 3. Complete sweep with remaining sources
hsm complete <sweep_id> --sources "local,server1,cluster"
```

**Partial Network Outage:**
```bash
# 1. Check which sources are affected
hsm sources ping --all

# 2. Temporarily disable affected sources
hsm sources disable server1,server2 --temporary

# 3. Continue with available sources
hsm sources rebalance <sweep_id>

# 4. Re-enable when connectivity restored
hsm sources enable server1,server2
```

## Advanced Features

### Custom Health Checks

```python
# custom_health_check.py
async def custom_gpu_health_check(source):
    """Custom health check for GPU availability."""
    result = await source.run_command("nvidia-smi --query-gpu=memory.free --format=csv,noheader,nounits")
    free_memory = int(result.stdout.strip())
    
    if free_memory < 1000:  # Less than 1GB free
        return {"status": "unhealthy", "message": f"Low GPU memory: {free_memory}MB"}
    elif free_memory < 2000:  # Less than 2GB free  
        return {"status": "degraded", "message": f"Moderate GPU memory: {free_memory}MB"}
    else:
        return {"status": "healthy", "message": f"GPU memory OK: {free_memory}MB"}
```

### Custom Distribution Strategies

```python
# custom_strategy.py
class GPUPreferredStrategy:
    """Prefer GPU-enabled sources for GPU tasks."""
    
    def select_source(self, task, available_sources):
        gpu_sources = [s for s in available_sources if s.has_gpu]
        cpu_sources = [s for s in available_sources if not s.has_gpu]
        
        if task.requires_gpu and gpu_sources:
            return min(gpu_sources, key=lambda s: s.current_load)
        elif cpu_sources:
            return min(cpu_sources, key=lambda s: s.current_load)
        else:
            return min(available_sources, key=lambda s: s.current_load)
```

### Integration with External Systems

**Workflow Orchestration:**
```bash
# Integration with workflow managers
hsm run --config sweep.yaml --sources all --notify-on-completion webhook://workflow-manager

# Slurm job dependencies
hsm run --config sweep.yaml --sources hpc --dependency afterok:12345
```

**Monitoring Integration:**
```bash
# Send metrics to monitoring systems
hsm run --config sweep.yaml --sources all --metrics-endpoint prometheus://metrics:9090

# Real-time dashboards
hsm monitor dashboard --sources all --live
```

This comprehensive guide covers all aspects of distributed sweep execution with HSM v2. The unified architecture, intelligent failover, and comprehensive monitoring make it easy to scale your hyperparameter searches across multiple compute resources reliably and efficiently. 