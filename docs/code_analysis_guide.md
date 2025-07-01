# Code Analysis and Usage Tracking Guide

This guide explains how to systematically analyze your HPC Sweep Manager codebase to identify useful code, bloated sections, and areas needing refactoring.

## 🎯 Overview

The analysis system provides several approaches to understand your codebase:

1. **Runtime Usage Tracking** - Monitor which functions are actually called during CLI usage
2. **Dead Code Detection** - Identify potentially unused functions and classes  
3. **Code Complexity Analysis** - Find functions that may need refactoring
4. **Test Coverage Analysis** - Identify areas needing more testing
5. **Dependency Analysis** - Understand module relationships and detect circular imports

## 🚀 Quick Start

### 1. Install and Set Up

```bash
# Install the package in development mode
make install-dev

# Or manually:
pip install -e ".[dev]"
```

### 2. Enable Usage Tracking

```bash
# Enable tracking for your session
export HSM_TRACK_USAGE=true

# Or run individual commands with tracking
HSM_TRACK_USAGE=true hsm init
HSM_TRACK_USAGE=true hsm sweep run --config sweeps/example.yaml --dry-run
HSM_TRACK_USAGE=true hsm monitor status
```

### 3. Run Analysis

```bash
# Quick comprehensive analysis
make analyze-all

# Individual analyses
make analyze-dead-code
make analyze-complexity
make analyze-usage

# Generate usage report
hsm analyze report --output analysis_report.md
```

## 📊 Analysis Types

### 1. Runtime Usage Tracking

**Purpose**: Track which parts of your code are actually used when running CLI commands.

**How it works**: 
- Decorators track function calls and timing
- Data is saved to `.hsm_usage_tracking/` directory
- Aggregate analysis across multiple sessions

**Usage**:
```bash
# Enable tracking
hsm analyze enable-tracking

# Use your CLI normally
hsm init
hsm sweep run --config sweeps/test.yaml --dry-run
hsm monitor recent

# Generate report
hsm analyze report
hsm analyze report --format json --output usage.json
```

**What it tells you**:
- Most frequently called functions
- Least used functions (potential cleanup candidates)
- CLI command usage patterns
- Function execution times

### 2. Dead Code Detection

**Purpose**: Find functions and classes that are defined but never called.

**Usage**:
```bash
hsm analyze dead-code
hsm analyze dead-code --exclude tests/ --exclude scripts/
hsm analyze dead-code --output dead_code.json
```

**Limitations**:
- May miss dynamic imports (`getattr`, `importlib`)
- May miss external API usage
- May miss usage in templates/configs

### 3. Code Complexity Analysis

**Purpose**: Identify functions with high cyclomatic complexity that may need refactoring.

**Requirements**: Installs `radon` automatically if not available.

**Usage**:
```bash
hsm analyze complexity
hsm analyze complexity --threshold 15
```

**What it tells you**:
- Functions with complexity above threshold
- Files with the most complex code
- Refactoring candidates

### 4. Test Coverage Analysis

**Purpose**: Find areas with low test coverage.

**Usage**:
```bash
# First run tests with coverage
make test-coverage

# Then analyze gaps
hsm analyze coverage-gaps
hsm analyze coverage-gaps --threshold 90.0
```

### 5. Dependency Analysis

**Purpose**: Understand module relationships and detect issues.

**Usage**:
```bash
hsm analyze dependencies
hsm analyze dependencies --show-external
```

**What it tells you**:
- Module import relationships
- Circular import detection
- External dependency usage

## 🔧 Integration with Your Workflow

### Systematic Analysis Workflow

1. **Baseline Analysis**:
   ```bash
   # Get current state
   make test-coverage
   make analyze-dead-code
   make analyze-complexity
   ```

2. **Enable Usage Tracking**:
   ```bash
   # Track real usage
   export HSM_TRACK_USAGE=true
   
   # Use your CLI for typical workflows
   hsm init
   hsm sweep run --config your_config.yaml
   hsm monitor status
   # ... your normal usage patterns
   ```

3. **Generate Comprehensive Report**:
   ```bash
   hsm analyze report --output analysis_$(date +%Y%m%d).md
   ```

4. **Make Decisions**:
   - Functions called only once → investigate if needed
   - High complexity functions → refactor
   - Unused functions + no usage tracking → potentially removable
   - Low coverage areas → add tests

### Continuous Integration

Add to your CI pipeline:

```yaml
# .github/workflows/analysis.yml
- name: Code Analysis
  run: |
    make install-dev
    make analyze-dead-code
    make analyze-complexity
    make test-coverage
    hsm analyze coverage-gaps --threshold 80
```

## 📈 Current Analysis Results (Real Data)

### Current Codebase Statistics

**Overall Metrics** (as of latest analysis):
- **385 function definitions** across 39 files
- **462 function calls** detected
- **107 potentially unused** functions/classes (27.8% of codebase!)
- Analysis covers entire `src/` directory excluding tests

### Critical Complexity Issues Found

**🚨 URGENT REFACTORING NEEDED:**

| Function | File | Complexity | Priority |
|----------|------|------------|----------|
| `run_sweep` | `cli/sweep.py` | **101** | **CRITICAL** |
| `delete_sweep_jobs` | `cli/monitor.py` | **71** | **HIGH** |
| `_execute_combinations` | `core/common/com...` | **44** | **HIGH** |
| `_gather_job_error_details` | `core/remote/rem...` | **32** | **MEDIUM** |
| `_verify_git_sync` | `core/remote/rem...` | **32** | **MEDIUM** |

**Total high-complexity functions (>10):** 65 functions need attention

### Dead Code Analysis Results

**Top Potentially Unused Functions** (immediate review candidates):

**Core Infrastructure** (may be used dynamically):
- `HPCJobManager`, `scheduler_name`, `status_command`, `submit_command` - **HPC abstractions**
- `ComputeSource`, `is_available`, `current_job_count` - **distributed computing**
- `ProgressTracker`, `format_walltime`, `parse_walltime` - **utilities**

**CLI Commands** (redundancy detected):
- Multiple `*_compat` functions in `main.py` - **backward compatibility wrappers**
- `test`, `remove`, `health` functions - **repeated across modules**  
- `visit_*` functions in `analyze.py` - **AST visitors (expected unused)**

**Configuration System**:
- `HydraConfigParser`, `SweepConfig`, `HSMConfig` - **multiple config classes**
- `suggest_sweep_ranges`, `discover_configs` - **wizard functionality**

### Immediate Action Plan

**Phase 1: Critical Refactoring (Week 1-2)**

1. **🔥 CRITICAL: Refactor `run_sweep` function (complexity 101)** ⚡ **IN PROGRESS**
   ```bash
   # Focus on this file first
   hsm analyze complexity --output complexity_analysis.json
   # Review: src/hpc_sweep_manager/cli/sweep.py:187-744 (558 lines!)
   ```
   
   **✅ ANALYSIS COMPLETE** - Identified 9 logical sections for extraction:
   1. Initial setup and configuration display  
   2. Configuration loading and validation ⬅️ **CURRENT FOCUS**
   3. Parameter generation and display
   4. Path detection and setup
   5. Job manager creation based on mode
   6. Dry run handling
   7. Sweep execution setup
   8. Job submission
   9. Result collection and reporting
   
   **✅ MAJOR SUCCESS!** All core sections extracted!
   - Config validation ✅, Path detection ✅, Job manager creation ✅, Dry-run handling ✅
   - Sweep directory setup ✅, Job submission ✅
   
   **🎉 INCREDIBLE PROGRESS ACHIEVED**: 
   - ✅ **85% complexity reduction!** **101 (CRITICAL)** → **15 (C rating)**
   - ✅ ~450 lines extracted into **8 helper functions**
   - ✅ Helper function complexity scores:
     - `_submit_and_track_jobs` - **A (4)** ⭐ Excellent
     - `_setup_sweep_directories_and_managers` - **B (9)** ⭐ Good  
     - `_create_job_manager` - **C (19)** ✅ Acceptable
     - `_handle_dry_run` - **C (19)** ✅ Acceptable
     - `_detect_project_paths` - **A** ⭐ Excellent
     - `_load_and_validate_config` - **A** ⭐ Excellent  
     - `_generate_parameter_combinations` - **A** ⭐ Excellent
     - `_handle_results_collection` - **C (18)** ✅ Acceptable
   - ✅ **Refactoring complete** - function is now maintainable!
   
2. **✅ MASSIVE SUCCESS!** `delete_sweep_jobs` refactoring **COMPLETE!** 
   ```bash
   # Original: src/hpc_sweep_manager/cli/monitor.py:1052-1314 (262 lines!)
   ```
   
   **🎉 INCREDIBLE ACHIEVEMENT**: 
   - ✅ **92% complexity reduction!** **71 (HIGH)** → **B (6)**  
   - ✅ ~200 lines extracted into **5 helper functions**
   - ✅ Helper function complexity scores:
     - `_filter_and_collect_jobs` - **C (17)** ✅ Acceptable
     - `_display_jobs_to_delete` - **B (6)** ⭐ Good  
     - `_get_user_confirmation` - **A** ⭐ Excellent
     - `_execute_job_deletions` - **D (28)** (complex but isolated)
     - `_report_deletion_results` - **C (20)** ✅ Acceptable
   - ✅ **Refactoring complete** - function is now maintainable!

**Phase 2: Dead Code Review (Week 3)**

3. **Review CLI compatibility wrappers**:
   ```bash
   hsm analyze dead-code --exclude tests/ --output dead_code_review.json
   ```
   
   **Action items**:
   - Consolidate `*_compat` functions in `main.py`
   - Remove duplicate `test`/`health`/`remove` implementations
   - Verify backward compatibility requirements

4. **Configuration system cleanup**:
   - Merge or clearly separate `HydraConfigParser` vs `SweepConfig`
   - Document which config classes are for internal vs external use
   - Consider removing unused discovery/suggestion features

**Phase 3: Usage Tracking (Week 4)**

5. **Implement comprehensive usage tracking**:
   ```bash
   export HSM_TRACK_USAGE=true
   # Run your typical workflow for a week
   hsm init test_project
   hsm sweep run --config typical_config.yaml
   hsm monitor status
   # Generate usage report
   hsm analyze report --output usage_week1.md
   ```

### Specific Code Actions

**Immediate TODO List:**

```python
# 1. CRITICAL: Break down run_sweep function
# File: src/hpc_sweep_manager/cli/sweep.py:187
# Current: 101 complexity -> Target: <15 per function

def run_sweep(...):
    # EXTRACT THESE INTO SEPARATE FUNCTIONS:
    # - validate_cli_options()
    # - determine_execution_mode()  
    # - setup_job_manager()
    # - execute_sweep_by_mode()
    # - handle_sweep_errors()
    pass

# 2. HIGH: Simplify delete_sweep_jobs  
# File: src/hpc_sweep_manager/cli/monitor.py:1052
# Current: 71 complexity -> Target: <20

# 3. Review potentially unused functions:
# These need manual verification before removal:
unused_candidates = [
    "force_cleanup_all",      # local_manager.py
    "stream_output",          # local_manager.py  
    "discover_all_remotes",   # discovery.py
    "safe_filename",          # utils.py
    "count_jobs_in_queue",    # utils.py
]
```

### Monitoring Progress

**Weekly Analysis Routine:**
```bash
# Run every Friday
make analyze-complexity --output weekly_complexity_$(date +%Y%m%d).json
make analyze-dead-code --output weekly_deadcode_$(date +%Y%m%d).json

# Track improvements
echo "Complexity trend:" 
grep -o '"complexity": [0-9]*' weekly_complexity_*.json | awk -F': ' '{sum+=$2; count++} END {print "Average:", sum/count}'
```

### Success Metrics

**Target Goals (Next 4 weeks):**
- ✅ Reduce `run_sweep` complexity from 101 → <15
- ✅ Reduce functions with >20 complexity from 14 → <5  
- ✅ Remove confirmed dead code: target 20-30 functions
- ✅ Establish usage tracking baseline
- ✅ Document remaining public API functions

**Risk Mitigation:**
- Always run full test suite after refactoring: `make test`
- Use git branches for major refactoring work
- Enable usage tracking before making removal decisions
- Document any breaking changes in public APIs

## 🛠️ Advanced Usage

### Custom Analysis Scripts

Create custom analysis scripts using the provided analyzers:

```python
from hpc_sweep_manager.core.common.usage_tracker import analyze_usage_patterns
from hpc_sweep_manager.cli.analyze import DeadCodeAnalyzer

# Custom analysis
usage_data = analyze_usage_patterns()
print(f"Most used: {usage_data['most_used_functions'][:5]}")

analyzer = DeadCodeAnalyzer(Path("src"), exclude_patterns=("tests/",))
results = analyzer.analyze()
print(f"Potentially unused: {len(results['potentially_unused'])}")
```

### Automated Cleanup

```bash
#!/bin/bash
# cleanup_script.sh

# Generate current analysis
hsm analyze dead-code --output dead_code.json

# Extract unused functions (manual review needed)
python -c "
import json
with open('dead_code.json') as f:
    data = json.load(f)
for item in data['potentially_unused']:
    print(f'Review: {item["name"]} in {item["locations"]}')
"
```

## 🔍 Troubleshooting

### "No tracking data found"
- Enable tracking: `export HSM_TRACK_USAGE=true`
- Use CLI commands to generate data
- Check `.hsm_usage_tracking/` directory exists

### "Module not found errors"
- Install package: `make install-dev`
- Ensure you're in the project root

### "Coverage file not found"
- Run tests with coverage first: `make test-coverage`
- Check `coverage.xml` exists

### High false positives in dead code
- Exclude test directories: `--exclude tests/`
- Consider dynamic imports and external usage
- Cross-reference with usage tracking data

## 📚 Best Practices

1. **Regular Analysis**: Run analysis monthly or before major releases
2. **Combine Methods**: Use both static analysis and runtime tracking
3. **Review Before Deleting**: Always manually verify unused code
4. **Document Public APIs**: Mark externally-used functions clearly
5. **Track Changes**: Keep analysis reports to track improvements
6. **Team Review**: Have team members review analysis results

## 🔗 Related Tools

- **pytest-cov**: Test coverage measurement
- **radon**: Complexity analysis
- **vulture**: Dead code detection (alternative)
- **bandit**: Security analysis
- **mypy**: Type checking

## 📝 Real Analysis Session (Current State)

### What We Found in Your Codebase

```bash
# 1. Current analysis results
$ hsm analyze dead-code --exclude tests/ --exclude scripts/
# Results: 107 potentially unused functions out of 385 total (27.8%)

$ hsm analyze complexity  
# Results: 65 functions with complexity >10, including:
#   - run_sweep: complexity 101 (CRITICAL!)
#   - delete_sweep_jobs: complexity 71  
#   - _execute_combinations: complexity 44

# 2. Set up tracking for real usage data
$ export HSM_TRACK_USAGE=true

# 3. Use CLI for typical workflows
$ hsm init test_project
$ hsm sweep run --config sweeps/experiment.yaml --dry-run
$ hsm monitor status
$ hsm analyze report  # Generate usage patterns

# 4. Systematic cleanup approach
$ make analyze-all  # Combined analysis workflow
```

### Immediate Actions You Can Take

**Start Here (This Week):**

1. **Focus on the `run_sweep` function first** - it's doing too much (complexity 101):
   ```bash
   # Examine the function
   code src/hpc_sweep_manager/cli/sweep.py +187
   
   # Look for these patterns to extract:
   # - CLI validation logic  
   # - Mode selection (local/remote/distributed)
   # - Job manager setup
   # - Error handling
   ```

2. **Quick wins - Remove obvious dead code**:
   ```bash
   # Review these CLI visitors (safe to remove):
   grep -n "visit_" src/hpc_sweep_manager/cli/analyze.py
   
   # Check these utility functions:
   grep -n "safe_filename\|count_jobs_in_queue" src/hpc_sweep_manager/core/common/utils.py
   ```

3. **Enable tracking for next week**:
   ```bash
   echo "export HSM_TRACK_USAGE=true" >> ~/.bashrc
   # Use your CLI normally for a week, then analyze patterns
   ```

### Tracking Your Progress

**Before you start** (baseline):
- Functions with complexity >20: **14 functions**
- Potentially unused functions: **107 functions**  
- Most complex function: **`run_sweep` (complexity 101)**

**Target after 1 month:**
- Functions with complexity >20: **<5 functions**
- Potentially unused functions: **<80 functions**
- Most complex function: **<15 complexity**

### Next Steps Workflow

```bash
# Week 1: Critical refactoring
git checkout -b refactor-run-sweep
# Refactor run_sweep function
make test  # Ensure nothing breaks
git commit -m "Refactor run_sweep: reduce complexity 101→15"

# Week 2: Dead code cleanup  
git checkout -b cleanup-dead-code
hsm analyze dead-code --output dead_code.json
# Review and remove confirmed unused functions
make test
git commit -m "Remove confirmed dead code: 20 functions"

# Week 3: Usage tracking analysis
export HSM_TRACK_USAGE=true
# Normal usage for a week
hsm analyze report --output week3_usage.md
# Make removal decisions based on real usage

# Week 4: Complexity improvements
hsm analyze complexity --threshold 15 --output week4_complexity.json
# Address remaining high-complexity functions
```

This systematic, data-driven approach helps you maintain a clean, efficient codebase while ensuring you don't accidentally remove important functionality. The real analysis data shows you have significant opportunities for improvement! 