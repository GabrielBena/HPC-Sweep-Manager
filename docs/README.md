# HSM Documentation

Welcome to the documentation for HPC Sweep Manager (HSM). This guide will help you get started and master HSM for your hyperparameter sweeps.

## 📖 Documentation Structure

### For Users
- **[Getting Started Guide](user_guide/getting_started.md)** - Installation, setup, and first sweep
- **[CLI Reference](cli/README.md)** - Complete command-line interface documentation

### For Developers
- **[API Reference](api_reference/README.md)** - Complete API documentation for extending HSM
- **[Testing](#testing)** - Running and writing tests

## 🚀 Quick Start

```bash
# Install HSM
pip install hpc-sweep-manager

# Initialize your project
cd /path/to/your/ml/project
hsm init

# Run your first sweep
hsm sweep run --mode local --max-runs 5

# Monitor progress
hsm monitor --watch
```

See the **[Getting Started Guide](user_guide/getting_started.md)** for detailed instructions.

## 🧪 Testing (For Developers)

HSM includes comprehensive test coverage. To run tests:

```bash
# Run all tests
make test

# Run fast tests only (exclude slow/remote tests)
make test-fast

# Run with coverage
make test-coverage
```

For more testing options, see `make help`.

## 🛠️ Development Setup (For Developers)

```bash
# Clone and setup
git clone https://github.com/GabrielBena/HPC-Sweep-Manager.git
cd HPC-Sweep-Manager

# Install in development mode
make dev-setup

# Run linting and formatting
make lint
make format

# Verify installation
make verify-install
```