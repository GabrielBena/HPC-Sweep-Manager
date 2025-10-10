# API Reference

This section provides detailed documentation for HSM's core modules and classes for developers who want to extend or integrate HSM.

## Core Modules

### Job Management
- [Job Manager](job_manager.md) - Central job management interface
- [Compute Sources](compute_sources.md) - Abstract compute resource interfaces

### Execution Modes
- [Local Execution](local/README.md) - Single machine parallel execution
- [HPC Execution](hpc/README.md) - HPC cluster job submission (PBS/Slurm)

For complete CLI documentation, see the [CLI Reference](../cli/README.md).

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