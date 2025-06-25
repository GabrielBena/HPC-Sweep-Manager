# API Reference

This section provides detailed documentation for all HSM modules and classes.

## Core Modules

### Job Management
- [Job Manager](job_manager.md) - Central job management interface
- [Compute Sources](compute_sources.md) - Abstract compute resource interfaces

### Execution Modes
- [Local Execution](local/README.md) - Single machine parallel execution
- [HPC Execution](hpc/README.md) - HPC cluster job submission (PBS/Slurm)
- [Remote Execution](remote/README.md) - SSH-based remote execution
- [Distributed Execution](distributed/README.md) - Multi-machine coordination

### Common Utilities
- [Configuration](common/config.md) - Configuration management
- [Parameter Generation](common/param_generator.md) - Parameter sweep generation
- [Path Detection](common/path_detector.md) - Automatic path discovery
- [Utilities](common/utils.md) - Common utility functions

## CLI Commands

### Core Commands
- [sweep](../cli/sweep.md) - Parameter sweep execution
- [monitor](../cli/monitor.md) - Job monitoring and status
- [init](../cli/init.md) - Project initialization

### Management Commands
- [remote](../cli/remote.md) - Remote machine management
- [collect](../cli/collect.md) - Result collection
- [configure](../cli/configure.md) - Configuration management

### HPC-Specific Commands
- [hpc](../cli/hpc.md) - HPC system interaction
- [local](../cli/local.md) - Local execution management

## Architecture Overview

HSM follows a modular architecture with clear separation of concerns:

```
HSM Architecture
├── CLI Layer (Click-based commands)
├── Job Manager (Central orchestration)
├── Compute Sources (Execution backends)
│   ├── Local (ThreadPoolExecutor)
│   ├── HPC (PBS/Slurm)
│   ├── Remote (SSH)
│   └── Distributed (Multi-remote)
└── Common Utilities (Config, paths, params)
```

## Key Design Principles

1. **Unified Interface** - Same API across all execution modes
2. **Auto-Discovery** - Automatic detection of environments and paths
3. **Organized Outputs** - Structured job organization and logging
4. **Extensibility** - Easy to add new compute sources and schedulers
5. **Error Handling** - Robust error recovery and user feedback 