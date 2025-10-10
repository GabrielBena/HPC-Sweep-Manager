# Examples

This directory contains example files to help you get started with HSM.

## Quick Test Example

This is a minimal example to test HSM functionality:

- **`test_train.py`** - A simple training script that accepts hyperparameters
- **`test_sweep.yaml`** - A basic sweep configuration
- **`hsm_config.yaml`** - Sample HSM configuration

### Running the Example

```bash
cd examples/

# Initialize HSM (or use existing config)
hsm init

# Run a local sweep
hsm sweep run --mode local --max-runs 4 --config test_sweep.yaml

# Monitor progress
hsm monitor --watch
```

This example sweep will run 4 jobs (2 learning rates × 2 batch sizes).

## Creating Your Own Sweeps

1. **Training Script**: Your script should accept command-line arguments for hyperparameters
2. **Sweep Config**: Define parameter grids in YAML format
3. **HSM Config**: Configure paths and execution settings

See the [User Guide](../docs/user_guide/getting_started.md) for detailed instructions.

