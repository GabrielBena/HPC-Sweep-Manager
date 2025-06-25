# HPC Sweep Manager Refactoring Progress

## 🎯 Refactoring Goals

The goal is to reorganize the HSM codebase into a clean, modular structure organized by execution mode for better maintainability and extensibility.

## ✅ Completed Tasks

### 1. Directory Structure Creation
- ✅ Created `core/common/` for shared components
- ✅ Created `core/local/` for local execution
- ✅ Created `core/remote/` for remote execution  
- ✅ Created `core/distributed/` for distributed execution
- ✅ Created `core/hpc/` for HPC cluster execution
- ✅ Created `docs/` structure with subdirectories

### 2. File Organization
- ✅ Moved `compute_source.py` → `core/common/`
- ✅ Moved `hsm_config.py` → `core/common/config.py`
- ✅ Moved `config_parser.py` → `core/common/config.py` (merged)
- ✅ Moved `param_generator.py` → `core/common/`
- ✅ Moved `path_detector.py` → `core/common/`
- ✅ Moved `utils.py` → `core/common/`
- ✅ Moved `local_compute_source.py` → `core/local/`
- ✅ Moved `remote_job_manager.py` → `core/remote/manager.py`
- ✅ Moved `ssh_compute_source.py` → `core/remote/`
- ✅ Moved `remote_discovery.py` → `core/remote/discovery.py`
- ✅ Moved `distributed_job_manager.py` → `core/distributed/manager.py`
- ✅ Moved `distributed_sweep_wrapper.py` → `core/distributed/wrapper.py`

### 3. Documentation Created
- ✅ Created comprehensive `docs/README.md`
- ✅ Created detailed `docs/user_guide/getting_started.md`
- ✅ Created package architecture documentation

### 4. Base Classes
- ✅ Created `core/hpc/hpc_base.py` with abstract HPC manager
- ✅ Updated `core/common/config.py` to merge configuration classes

## 🔄 In Progress Tasks

### 1. HPC Module Extraction
- ⏳ Need to extract PBS and Slurm managers from `job_manager.py`
- ⏳ Create `core/hpc/pbs_manager.py`
- ⏳ Create `core/hpc/slurm_manager.py`
- ⏳ Extract LocalJobManager to `core/local/local_manager.py`

### 2. CLI Reorganization
- ⏳ Create `cli/local.py` for local-specific commands
- ⏳ Create `cli/hpc.py` for HPC-specific commands
- ⏳ Create `cli/common.py` for shared CLI utilities
- ⏳ Update imports in existing CLI files

## 📋 Remaining Tasks

### 1. Code Refactoring
- [ ] **Extract HPC Managers**: Split `job_manager.py` into separate PBS and Slurm managers
- [ ] **Create Local Manager**: Extract local execution logic to dedicated module
- [ ] **Update Imports**: Fix all import statements throughout the codebase
- [ ] **Update __init__.py files**: Ensure proper exports from all modules

### 2. CLI Enhancement
- [ ] **Local CLI**: Add local-specific commands like `hsm local run`, `hsm local status`
- [ ] **HPC CLI**: Add HPC-specific commands like `hsm hpc submit`, `hsm hpc queue`
- [ ] **Unified Interface**: Ensure consistent command structure across modes

### 3. Documentation Completion
- [ ] **Configuration Guide**: Detailed configuration documentation
- [ ] **Execution Modes Guide**: Deep dive into each mode
- [ ] **API Reference**: Complete API documentation for all modules
- [ ] **Examples**: Real-world usage examples
- [ ] **Migration Guide**: Help users migrate from old structure

### 4. Testing Updates
- [ ] **Test Organization**: Organize tests by execution mode
- [ ] **Integration Tests**: Test cross-mode functionality
- [ ] **CLI Tests**: Test all CLI commands and options

### 5. Compatibility & Polish
- [ ] **Import Compatibility**: Ensure backward compatibility for imports
- [ ] **Error Handling**: Improve error messages with new structure
- [ ] **Logging**: Consistent logging across all modules
- [ ] **Type Hints**: Add comprehensive type hints

## 🗂️ New Package Structure (Target)

```
hpc_sweep_manager/
├── core/
│   ├── common/                 ✅ DONE
│   │   ├── __init__.py         ✅ DONE
│   │   ├── compute_source.py   ✅ DONE
│   │   ├── config.py          ✅ DONE (merged)
│   │   ├── param_generator.py ✅ DONE
│   │   ├── path_detector.py   ✅ DONE
│   │   └── utils.py           ✅ DONE
│   ├── local/                 ⏳ PARTIAL
│   │   ├── __init__.py        ✅ DONE
│   │   ├── local_compute_source.py ✅ DONE
│   │   └── local_manager.py   ❌ TODO (extract from job_manager.py)
│   ├── remote/                ✅ DONE
│   │   ├── __init__.py        ✅ DONE  
│   │   ├── manager.py         ✅ DONE
│   │   ├── ssh_compute_source.py ✅ DONE
│   │   └── discovery.py       ✅ DONE
│   ├── distributed/           ✅ DONE
│   │   ├── __init__.py        ✅ DONE
│   │   ├── manager.py         ✅ DONE
│   │   └── wrapper.py         ✅ DONE
│   └── hpc/                   ⏳ PARTIAL
│       ├── __init__.py        ✅ DONE
│       ├── hpc_base.py        ✅ DONE
│       ├── pbs_manager.py     ❌ TODO (extract from job_manager.py)
│       └── slurm_manager.py   ❌ TODO (extract from job_manager.py)
├── cli/
│   ├── main.py               ✅ EXISTS (needs updates)
│   ├── local.py              ❌ TODO (new file)
│   ├── remote.py             ✅ EXISTS 
│   ├── distributed.py        ✅ EXISTS
│   ├── hpc.py                ❌ TODO (new file)
│   ├── monitor.py            ✅ EXISTS
│   └── common.py             ❌ TODO (shared utilities)
└── docs/                     ⏳ PARTIAL
    ├── README.md             ✅ DONE
    ├── user_guide/           ⏳ PARTIAL
    │   └── getting_started.md ✅ DONE
    ├── api_reference/        ❌ TODO
    ├── examples/             ❌ TODO
    └── deployment/           ❌ TODO
```

## 🎯 Next Priority Tasks

1. **Extract HPC Managers** (High Priority)
   - Split `job_manager.py` into PBS/Slurm specific files
   - Create `core/hpc/pbs_manager.py` and `core/hpc/slurm_manager.py`

2. **Create Local Manager** (High Priority)
   - Extract `LocalJobManager` to `core/local/local_manager.py`

3. **Fix Imports** (Critical)
   - Update all import statements throughout codebase
   - Test that package still works after refactoring

4. **CLI Enhancements** (Medium Priority)
   - Create mode-specific CLI commands
   - Improve user experience with specialized commands

5. **Documentation** (Medium Priority)
   - Complete user guides and API documentation
   - Add comprehensive examples

## 🧪 Testing Strategy

- Test each mode independently after refactoring
- Ensure backward compatibility
- Add integration tests for cross-mode scenarios
- Test CLI commands with new structure

## 📝 Notes

- The refactoring maintains the same functionality while improving organization
- All existing CLI commands should continue to work
- New structure enables easier testing and maintenance
- Documentation provides clear migration path for users 