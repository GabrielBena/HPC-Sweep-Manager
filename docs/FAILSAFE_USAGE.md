# HPC Sweep Manager - Failsafe and Disk Space Monitoring

## New Features

### 1. Enhanced Disk Space Prevention

The remote job scripts now include comprehensive disk space monitoring:

- **Pre-execution checks**: Jobs check available disk space before starting (requires 2GB project + 1GB task)
- **Periodic monitoring**: Every 5 minutes during execution
- **Automatic termination**: Jobs are terminated if disk space drops below 200MB
- **Controlled temp directories**: All temporary files use task-specific directories (no more `/tmp/` issues)

### 2. Source Failure Detection and Failsafe

Automatic detection and disabling of problematic compute sources:

- **Failure rate monitoring**: Sources with >40% job failure rate are automatically disabled
- **Health check monitoring**: Sources failing 3+ consecutive health checks are disabled
- **Disk space monitoring**: Sources with critical disk space are flagged and potentially disabled

## Configuration

The failsafe behavior can be configured in your `hsm_config.yaml`:

```yaml
distributed:
  enabled: true
  strategy: "round_robin"
  # Failsafe configuration
  enable_source_failsafe: true              # Enable failure rate monitoring
  source_failure_threshold: 0.4             # Disable at 40% failure rate
  min_jobs_for_failsafe: 5                  # Need 5+ jobs before checking
  auto_disable_unhealthy_sources: true      # Auto-disable on health failures
  health_check_failure_threshold: 3         # Disable after 3 health failures
  health_check_interval: 60                 # Check every 60 seconds
```

## Usage

### Health Monitoring

Check the health of your compute sources (including new disk space monitoring):

```bash
# Check all sources with detailed disk/system info
hsm distributed health --all

# Watch mode with continuous monitoring
hsm distributed health --all --watch

# Check specific sources
hsm distributed health source1 source2
```

The enhanced health command now shows:
- **Disk Space**: Available space and status (critical/warning/healthy)
- **System**: CPU load and memory usage
- **Failure Rate**: Recent job failure rate due to disk/other issues
- **Status**: Overall health (healthy/degraded/unhealthy)

### Running Distributed Sweeps

The failsafe mechanisms are automatic when running distributed sweeps:

```bash
# Normal distributed sweep - failsafe runs in background
hsm run sweep.yaml --distributed
```

During execution, you'll see logs like:
- `WARNING: Low disk space detected: 0.4GB remaining` - when space is getting low
- `Disabling source 'blossom' due to high failure rate: 3/5 (60%) >= 40%` - when sources fail too much
- `CRITICAL: blossom has critical disk space shortage: Critical: 0.8GB (3%) free` - critical space issues

### Failure Scenarios Handled

1. **Disk Space Exhaustion**:
   - Jobs check space before starting
   - Periodic monitoring during execution
   - Automatic termination before complete exhaustion
   - Source marked as degraded/unhealthy if consistently low

2. **Remote Machine Issues**:
   - SSH connection failures
   - System overload (high CPU/memory)
   - Consecutive health check failures
   - Sources automatically disabled and jobs redistributed

3. **Job Failure Patterns**:
   - High failure rates trigger source investigation
   - Failed sources are disabled automatically
   - Remaining healthy sources continue the sweep

## Monitoring and Logs

### Job-Level Monitoring

Each remote job now includes enhanced logging in `task_info.txt`:
```
Disk space check for /project at Mon Jan 1 12:00:00 2024:
  Available: 5.23 GB (15.2% free)
Disk space check PASSED
Environment variables:
  TMPDIR=/path/to/task/tmp
  CUDA_CACHE_PATH=/path/to/task/cuda_cache
```

### Health Check Logs

Health monitoring provides detailed information:
```
2024-01-01 12:00:00 INFO Health check passed for blossom: Healthy: 5.2GB (15%) free
2024-01-01 12:01:00 WARNING Source blossom is degraded: High disk failure rate with low space
2024-01-01 12:02:00 ERROR Disabling source 'blossom' due to 3 consecutive health check failures
```

### Source Status Tracking

The system tracks:
- Total jobs submitted per source
- Job success/failure rates per source
- Consecutive health check failures
- Disk space trends over time

## Troubleshooting

### Source Disabled Due to Failures

If a source gets disabled:

1. Check the health status: `hsm distributed health source_name`
2. Look at recent logs for failure patterns
3. Check disk space on the remote machine: `df -h`
4. Verify SSH connectivity and system load
5. Re-enable manually if issue is resolved (remove from disabled set)

### Disk Space Issues

If encountering disk space problems:

1. **Immediate**: Clean up old sweep directories and temporary files
2. **Prevention**: Increase monitoring frequency or raise thresholds
3. **Configuration**: Adjust `source_failure_threshold` if needed
4. **Infrastructure**: Add more disk space to remote machines

### False Positives

If sources are being disabled unnecessarily:

1. **Increase thresholds**: Raise `source_failure_threshold` above 0.4
2. **Increase minimum jobs**: Raise `min_jobs_for_failsafe` above 5
3. **Disable failsafe**: Set `enable_source_failsafe: false` temporarily
4. **Check networking**: Ensure stable SSH connections

## Best Practices

1. **Monitor health regularly**: Use `--watch` mode during long sweeps
2. **Set conservative thresholds**: Start with stricter settings and relax if needed
3. **Maintain disk space**: Keep >10GB free on remote machines when possible
4. **Test connectivity**: Verify SSH and system health before starting large sweeps
5. **Log analysis**: Review failure patterns to identify infrastructure issues

## Example Session

```bash
# Start distributed sweep with monitoring
hsm distributed health --all
hsm run my_sweep.yaml --distributed

# In another terminal, monitor health
hsm distributed health --all --watch --refresh 30

# If issues detected, check specific source
hsm distributed health problematic_source
```

This ensures robust, fault-tolerant distributed computing with automatic handling of common failure scenarios. 