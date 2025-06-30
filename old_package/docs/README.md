# HSM Documentation

Welcome to the comprehensive documentation for HPC Sweep Manager (HSM). This documentation covers all aspects of HSM, from basic usage to advanced development.

## 📖 Documentation Structure

### User Documentation
- **[User Guide](user_guide/)** - Getting started and common workflows
- **[CLI Reference](cli/)** - Complete command-line interface documentation
- **[Examples](examples/)** - Practical examples and tutorials
- **[Deployment Guide](deployment/)** - Production deployment strategies

### Developer Documentation
- **[API Reference](api_reference/)** - Complete API documentation
- **[Architecture Overview](api_reference/README.md#architecture-overview)** - System design and components
- **[Testing Guide](#testing)** - Running and writing tests
- **[Contributing Guidelines](#contributing)** - Development workflow

## 🚀 Quick Start

### For Users
1. **Installation**: `pip install hpc-sweep-manager`
2. **Initialize Project**: `hsm init`
3. **Run Your First Sweep**: `hsm sweep --mode local --max-runs 5`
4. **Monitor Progress**: `hsm monitor --watch`

See the [User Guide](user_guide/) for detailed instructions.

### For Developers
1. **Clone Repository**: `git clone <repo-url>`
2. **Setup Environment**: `make dev-setup`
3. **Run Tests**: `make test`
4. **View Documentation**: Open this directory in your browser

## 📚 Documentation Overview

### Core Concepts

**Execution Modes**
- **Local**: Parallel execution on single machine
- **HPC**: Cluster submission (PBS/Slurm)
- **Remote**: SSH-based execution
- **Distributed**: Multi-machine coordination

**Key Components**
- **Job Manager**: Central orchestration
- **Compute Sources**: Execution backends
- **Configuration System**: YAML-based setup
- **Parameter Generation**: Sweep definition

### Architecture

```
HSM Architecture
├── CLI Layer (User Interface)
├── Job Manager (Orchestration)
├── Compute Sources (Execution)
│   ├── Local (ThreadPool)
│   ├── HPC (PBS/Slurm)
│   ├── Remote (SSH)
│   └── Distributed (Multi-remote)
├── Common Utilities
│   ├── Configuration
│   ├── Path Detection
│   └── Parameter Generation
└── Templates & Scripts
```

## 🧪 Testing

HSM includes comprehensive test coverage with multiple test categories:

### Test Categories
- **Unit Tests**: Fast, isolated component tests
- **Integration Tests**: Multi-component workflow tests
- **CLI Tests**: Command-line interface verification
- **Remote Tests**: SSH and network functionality (requires setup)
- **HPC Tests**: Cluster integration (requires HPC access)

### Running Tests

```bash
# Run all tests
make test

# Run specific test categories
make test-unit          # Unit tests only
make test-integration   # Integration tests only
make test-cli          # CLI tests only
make test-fast         # Exclude slow/remote tests

# Generate coverage report
make test-coverage
```

### Test Markers

Tests are organized using pytest markers:

```bash
# By category
pytest -m unit           # Unit tests
pytest -m integration    # Integration tests
pytest -m cli           # CLI tests

# By characteristics  
pytest -m "not slow"     # Exclude slow tests
pytest -m "not remote"   # Exclude remote tests
pytest -m "unit and not slow"  # Fast unit tests only
```

### Coverage Requirements

- **Minimum Coverage**: 80%
- **Critical Modules**: 90%+ coverage required
- **Coverage Reports**: Generated in `htmlcov/`

## 🛠️ Development Workflow

### Setup Development Environment

```bash
# Clone and setup
git clone <repo-url>
cd HPC-Sweep-Manager
make dev-setup

# Verify installation
make verify-install
```

### Code Quality Standards

```bash
# Format code
make format

# Run linting
make lint

# Run all checks
make check
```

### Testing During Development

```bash
# Quick test during development
make quick-test

# Test specific module
pytest tests/unit/test_job_manager.py -v

# Debug failing test
pytest tests/unit/test_job_manager.py::TestJobManager::test_submit_sweep -v -s
```

## 📋 Documentation Guidelines

### Writing Documentation

1. **API Documentation**: Use docstrings with type hints
2. **User Documentation**: Focus on practical examples
3. **Code Examples**: Always test example code
4. **CLI Documentation**: Include all options and examples

### Documentation Standards

- **Clear Examples**: Every feature needs a working example
- **Error Handling**: Document common errors and solutions
- **Cross-References**: Link related concepts
- **Version Compatibility**: Note version requirements

## 🤝 Contributing

### Areas for Contribution

1. **New Compute Sources**: Additional HPC schedulers
2. **Enhanced Monitoring**: Better progress tracking
3. **Documentation**: Examples and tutorials
4. **Testing**: Additional test coverage
5. **Performance**: Optimization and profiling

### Contribution Process

1. **Fork Repository**: Create your feature branch
2. **Write Tests**: Add tests for new functionality
3. **Update Documentation**: Document your changes
4. **Run Checks**: `make check` must pass
5. **Submit PR**: Include detailed description

### Code Standards

- **Python Style**: Follow Black formatting
- **Type Hints**: Use type annotations
- **Docstrings**: Document all public methods
- **Error Handling**: Comprehensive error management
- **Testing**: Maintain 80%+ test coverage

## 📊 Monitoring and Debugging

### Log Files

HSM generates detailed logs for debugging:

```
~/.hsm/hsm.log                    # Global HSM log
sweeps/outputs/sweep_*/logs/      # Sweep-specific logs
sweeps/outputs/sweep_*/tasks/     # Individual task outputs
```

### Debug Mode

Enable debug logging for troubleshooting:

```bash
# Enable debug logging
export HSM_LOG_LEVEL=DEBUG
hsm sweep --verbose

# Check configuration
hsm configure validate

# Test connections
hsm remote test --verbose
```

### Common Issues

1. **Permission Errors**: Check SSH keys and file permissions
2. **Path Issues**: Verify auto-detected paths in config
3. **Resource Limits**: Check HPC queue limits and node availability
4. **Network Issues**: Test connectivity with `hsm remote test`

## 🔧 Configuration Reference

### HSM Configuration (`hsm_config.yaml`)

```yaml
# Essential paths
paths:
  python_interpreter: /path/to/python
  train_script: ./train.py
  config_dir: ./configs

# Local execution settings
local:
  max_parallel_jobs: 4
  timeout: 3600

# HPC settings
hpc:
  default_walltime: "04:00:00"
  default_resources: "select=1:ncpus=4:mem=16gb"
  system: "auto"  # or "pbs", "slurm"

# Remote machines
distributed:
  remotes:
    gpu_server:
      host: "gpu.university.edu"
      ssh_key: "~/.ssh/id_ed25519"
      max_parallel_jobs: 8
```

### Sweep Configuration (`sweep.yaml`)

```yaml
sweep:
  # Grid search (all combinations)
  grid:
    lr: [0.001, 0.01, 0.1]
    batch_size: [16, 32, 64]
    
  # Paired parameters (same-index combinations)
  paired:
    - model_and_optimizer:
        model.type: [cnn, transformer]
        optimizer.type: [adam, sgd]
```

## 📈 Performance Considerations

### Optimization Tips

1. **Local Mode**: Match parallel jobs to CPU cores
2. **HPC Mode**: Use array jobs for large sweeps
3. **Remote Mode**: Batch operations to reduce SSH overhead
4. **Parameter Generation**: Use efficient sweep strategies

### Scaling Guidelines

- **Small Sweeps (< 50 jobs)**: Local or individual HPC jobs
- **Medium Sweeps (50-500 jobs)**: HPC array jobs
- **Large Sweeps (500+ jobs)**: Distributed or HPC with dependencies

## 📋 Version Compatibility

### Python Versions
- **Minimum**: Python 3.8+
- **Recommended**: Python 3.10+
- **Tested**: Python 3.8, 3.9, 3.10, 3.11

### Dependencies
- **Click**: 8.0+
- **PyYAML**: 6.0+
- **Paramiko**: 2.8+ (for remote functionality)
- **Pytest**: 6.0+ (for development)

### HPC Compatibility
- **PBS/Torque**: All versions with `qsub`/`qstat`
- **Slurm**: All versions with `sbatch`/`squeue`
- **Other**: Extensible architecture for additional schedulers

---

## 📞 Getting Help

- **Documentation**: Start with [User Guide](user_guide/)
- **Examples**: Check [Examples](examples/) directory
- **Issues**: Report bugs and feature requests on GitHub
- **Discussions**: Community discussions and Q&A

---

*This documentation is continuously updated. Last updated: $(date)* 