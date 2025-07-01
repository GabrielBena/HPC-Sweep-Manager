# HPC Sweep Manager v2 Architecture Roadmap

## Overview

This document outlines the complete redesign of HPC Sweep Manager (HSM) to create a unified, clean, and maintainable sweep execution system. The v2 architecture focuses on separating the sweep orchestration logic from compute source implementations, enabling seamless mode switching and consistent behavior across all execution modes.

## Latest Updates

**Current Session Progress**: Completed SSH compute source implementation, bringing Phase 2 to near completion. The v2 architecture now supports both local and remote SSH execution with a unified interface, comprehensive health monitoring, and automatic result synchronization. Main remaining tasks are HPC compute source implementation and CLI migration.

## Current Progress Status

### ✅ Completed Components
- **Package Structure**: Complete `new_src/hsm/` package structure with all module directories and `__init__.py` files
- **Enhanced ComputeSource Base Class**: Comprehensive abstract base class with:
  - Standardized enums: `TaskStatus`, `HealthStatus`
  - Rich dataclasses: `Task`, `TaskResult`, `CollectionResult`, `HealthReport`, `SweepContext`, `ComputeSourceStats`
  - Complete abstract interface with all required methods
  - Built-in task registration and health tracking
- **SweepEngine**: Central orchestrator implementation with:
  - Async execution management
  - Multi-source task distribution 
  - Background monitoring and health checking
  - Result collection coordination
  - Progress tracking and status reporting
  - Proper cleanup and error handling
- **SweepTracker**: Enhanced task tracking with:
  - Cross-mode completion support
  - Async state management with thread safety
  - Comprehensive task metadata tracking
  - Disk synchronization capabilities
  - Legacy format compatibility
- **Configuration System**: Complete configuration management with:
  - **SweepConfig**: Comprehensive sweep parameter configuration
  - **HSMConfig**: System-wide HSM settings and preferences
  - **ValidationResult**: Detailed validation with errors, warnings, and suggestions
  - Support for grid and paired parameters
  - Hydra integration support
  - YAML serialization/deserialization
- **Validation Framework**: Robust validation system with:
  - **SweepConfigValidator**: Parameter validation with conflict detection
  - **HSMConfigValidator**: System configuration validation
  - **SystemValidator**: Python environment and system resource checks
  - Detailed error reporting with suggestions
- **Utility Modules**: Core utilities including:
  - **ParameterGenerator**: Advanced parameter combination generation
  - Support for complex nested parameters
  - Runtime estimation capabilities
  - Command-line argument generation
- **LocalComputeSource**: Complete local execution implementation with:
  - Async subprocess management
  - Concurrent task execution with semaphore control
  - Comprehensive health monitoring
  - Resource usage tracking
  - Conda environment support
  - Task timeout handling
  - Error recovery and cleanup

### ✅ Recently Completed (Current Session)
- **SSHComputeSource**: Complete SSH-based remote execution implementation with:
  - Async SSH connection management with asyncssh
  - Remote task submission and monitoring
  - Background result synchronization
  - Health monitoring and connection recovery
  - Remote environment setup and validation
  - Comprehensive error handling and cleanup
- **HPCComputeSource**: Complete HPC cluster execution implementation with:
  - Support for both PBS/Torque and Slurm schedulers
  - Auto-detection of available scheduler
  - Job script generation and submission
  - Background job monitoring and status tracking
  - Resource specification and queue management
  - Module loading and conda environment support
- **CLI Infrastructure**: Complete CLI migration to v2 architecture with:
  - Unified command structure using Click and Rich
  - Main CLI entry point with environment validation
  - Sweep commands (run, complete, status, cancel)
  - Monitoring commands (watch, recent, errors)
  - Configuration commands (init, validate, show)
  - Compute source parsing and creation utilities
  - Comprehensive error handling and user feedback

### ✅ Recently Completed (Current Session - Phase 2)
- **Complete CLI Testing**: All CLI commands tested and working:
  - Main CLI entry point with help system and version
  - Configuration management (init, show, validate) 
  - Environment validation and error handling
  - Rich console output and syntax highlighting
  - Fixed HSMConfig loading and validation issues
  - Proper error reporting and suggestions display

### 🚧 In Progress
- Integration testing of all compute sources
- End-to-end workflow testing

### 📋 Next Priorities
1. Test full sweep execution workflow with all compute sources
2. Implement result collection and monitoring utilities  
3. Add comprehensive testing suite
4. Create migration documentation from v1 to v2

## Current Issues (v1)

### Architecture Problems
1. **Fragmented execution logic**: Different managers (LocalJobManager, RemoteJobManager, DistributedJobManager) implement similar functionality differently
2. **Inconsistent interfaces**: Mix of sync/async APIs, different parameter formats, varying result structures  
3. **Mode-specific wrappers**: RemoteJobManagerWrapper and similar classes add unnecessary complexity
4. **Monolithic CLI functions**: The main `run_sweep()` function handles all modes in one massive function
5. **Scattered result handling**: Each mode collects and stores results differently

### User Experience Issues
1. **Inconsistent output structure**: Different directory layouts and file naming across modes
2. **Mode switching limitations**: Cannot easily start in one mode and complete in another
3. **Poor error reporting**: Error information scattered across different locations
4. **Missing progress tracking**: Inconsistent progress reporting across modes

### Development Issues
1. **Code duplication**: Similar functionality reimplemented in each manager
2. **Testing gaps**: Insufficient test coverage, especially for cross-mode scenarios
3. **Documentation holes**: Missing API documentation and user guides
4. **Maintenance burden**: Changes require updates across multiple managers

## V2 Architecture Goals

### Core Principles
1. **Separation of Concerns**: Clear separation between sweep orchestration and compute execution
2. **Compute Source Agnostic**: The sweep engine doesn't know or care where jobs run
3. **Unified Interface**: All execution modes use the same API and produce identical outputs
4. **Mode Flexibility**: Start in any mode, complete in any other mode seamlessly
5. **Centralized Management**: All sweep state, logs, and results managed centrally

### Key Features
1. **Single Sweep Engine**: One engine coordinates all sweep execution regardless of compute sources
2. **Unified ComputeSource Interface**: All execution modes implement the same abstract interface
3. **Consistent Output Structure**: Identical directory layout, logs, and metadata across all modes
4. **Cross-Mode Completion**: Complete partial sweeps using different execution modes
5. **Centralized Result Collection**: All results automatically synced to local sweep directory
6. **Comprehensive Error Tracking**: Unified error collection, categorization, and reporting
7. **Real-time Progress Monitoring**: Consistent progress tracking across all modes
8. **Robust Testing**: Full test coverage including integration and cross-mode scenarios

## Architecture Design

### Core Components

```
new_src/
├── hsm/                           # Main package
│   ├── __init__.py
│   ├── core/                      # Core sweep orchestration
│   │   ├── __init__.py
│   │   ├── engine.py              # ✅ SweepEngine - main orchestrator
│   │   ├── tracker.py             # ✅ Enhanced SweepTaskTracker
│   │   ├── collector.py           # 📋 Result collection coordinator
│   │   ├── monitor.py             # 📋 Progress monitoring
│   │   └── completion.py          # 📋 Cross-mode completion logic
│   ├── compute/                   # Compute source implementations
│   │   ├── __init__.py
│   │   ├── base.py               # ✅ ComputeSource ABC (enhanced)
│   │   ├── local.py              # ✅ Local execution compute source
│   │   ├── ssh.py                # ✅ SSH remote compute source
│   │   ├── distributed.py        # 📋 Multi-source coordinator
│   │   └── hpc.py                # 🚧 HPC cluster compute source
│   ├── config/                    # Configuration management
│   │   ├── __init__.py
│   │   ├── sweep.py              # ✅ Sweep configuration
│   │   ├── hsm.py                # ✅ HSM configuration
│   │   └── validation.py         # ✅ Configuration validation
│   ├── utils/                     # Shared utilities
│   │   ├── __init__.py
│   │   ├── params.py             # ✅ Parameter generation
│   │   ├── paths.py              # 📋 Path detection and management
│   │   ├── logging.py            # 📋 Logging utilities
│   │   └── io.py                 # 📋 File I/O utilities
│   └── cli/                       # Command line interface
│       ├── __init__.py
│       ├── main.py               # 🚧 Main CLI entry point
│       ├── sweep.py              # 🚧 Sweep commands
│       ├── monitor.py            # 📋 Monitoring commands
│       ├── config.py             # 📋 Configuration commands
│       └── utils.py              # 📋 CLI utilities
├── tests/                         # Comprehensive test suite
│   ├── __init__.py
│   ├── unit/                     # 📋 Unit tests
│   ├── integration/              # 📋 Integration tests
│   ├── e2e/                      # 📋 End-to-end tests
│   └── fixtures/                 # 📋 Test fixtures and mocks
└── docs/                         # Documentation
    ├── api/                      # 📋 API reference
    ├── user_guide/              # 📋 User guides
    ├── dev_guide/               # 📋 Developer documentation
    └── examples/                # 📋 Example configurations
```

### Key Classes

#### SweepEngine (core/engine.py) ✅
The central orchestrator that manages sweep execution across all compute sources.

```python
class SweepEngine:
    """Central sweep execution engine."""
    
    async def run_sweep(self, config: SweepConfig, sources: List[ComputeSource]) -> SweepResult
    async def complete_sweep(self, sweep_id: str, sources: List[ComputeSource]) -> SweepResult
    async def monitor_sweep(self, sweep_id: str) -> SweepStatus
    async def cancel_sweep(self, sweep_id: str) -> bool
```

#### Enhanced ComputeSource (compute/base.py) ✅
Improved abstract base class with standardized interface for all compute sources.

```python
class ComputeSource(ABC):
    """Abstract base class for all compute sources."""
    
    async def setup(self, sweep_context: SweepContext) -> bool
    async def submit_task(self, task: Task) -> TaskResult  
    async def get_task_status(self, task_id: str) -> TaskStatus
    async def collect_results(self, task_ids: List[str]) -> CollectionResult
    async def cancel_task(self, task_id: str) -> bool
    async def health_check(self) -> HealthStatus
    async def cleanup(self) -> bool
```

#### SweepTracker (core/tracker.py) ✅
Enhanced task tracking with better cross-mode support.

```python
class SweepTracker:
    """Enhanced sweep task tracking."""
    
    def register_task(self, task: Task, source: str) -> None
    def update_task_status(self, task_id: str, status: TaskStatus) -> None
    def get_completion_status(self) -> CompletionStatus
    def get_missing_tasks(self) -> List[Task]
    def sync_from_disk(self) -> None
```

### Execution Flow

1. **Configuration Loading**: Load and validate sweep configuration and HSM settings
2. **Compute Source Setup**: Initialize and validate selected compute sources  
3. **Task Generation**: Generate parameter combinations and create task definitions
4. **Sweep Initialization**: Create sweep directory structure and initialize tracking
5. **Task Distribution**: Distribute tasks across available compute sources using strategy
6. **Execution Monitoring**: Monitor task execution and collect real-time status
7. **Result Collection**: Continuously sync results from remote sources to local directory
8. **Completion Detection**: Detect when sweep is complete or needs intervention
9. **Cleanup**: Clean up remote resources while preserving results

### Directory Structure (Standardized)

```
sweeps/outputs/{sweep_id}/
├── sweep_config.yaml              # Original sweep configuration
├── sweep_metadata.yaml            # Sweep metadata and settings
├── task_mapping.yaml              # Task-to-source assignments and status
├── tasks/                         # Individual task results
│   ├── task_001/                  # Task directory
│   │   ├── params.yaml            # Task parameters
│   │   ├── status.yaml            # Task status and timing
│   │   ├── stdout.log             # Standard output
│   │   ├── stderr.log             # Standard error  
│   │   ├── results/               # Task output files
│   │   └── metrics.json           # Extracted metrics (if available)
│   └── task_002/
├── logs/                          # Centralized logs
│   ├── sweep.log                  # Main sweep log
│   ├── engine.log                 # Engine-specific logs
│   ├── sources/                   # Per-source logs
│   │   ├── local.log
│   │   └── remote1.log
│   └── errors/                    # Error summaries
│       ├── failed_tasks.yaml     # Failed task summary
│       └── error_patterns.yaml   # Common error analysis
├── scripts/                       # Generated scripts (if applicable)
│   ├── job_scripts/              # HPC job scripts
│   └── setup_scripts/            # Environment setup scripts
└── reports/                       # Generated reports
    ├── completion_report.html     # Completion status report
    ├── performance_analysis.yaml  # Performance metrics
    └── error_analysis.html        # Error analysis report
```

---

This roadmap provides a comprehensive plan for creating a robust, maintainable, and user-friendly v2 architecture that addresses all current limitations while providing a solid foundation for future enhancements. 