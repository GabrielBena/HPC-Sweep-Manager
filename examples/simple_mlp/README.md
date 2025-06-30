# Simple MLP Example for HSM

This is a minimal example demonstrating how to use the HPC Sweep Manager (HSM) for hyperparameter sweeps. It implements a simple single-layer MLP training script with configurable parameters.

## Overview

The example includes:
- **`main.py`**: A minimal MLP training script with Hydra configuration and optional W&B logging
- **`configs/config.yaml`**: Hydra configuration with key hyperparameters
- **`hsm_config.yaml`**: HSM configuration for managing sweeps
- **`sweep_config.yaml`**: Example sweep configurations (grid, random, bayesian)

## Features

- **Minimal dependencies**: Works with just Python standard library + Hydra/OmegaConf
- **Optional ML libraries**: Gracefully handles missing PyTorch, NumPy, W&B
- **Mock training**: Runs without ML libraries for testing HSM functionality
- **Fast execution**: Configurable simulation time for quick testing
- **Comprehensive logging**: Console and W&B logging support

## Quick Start

### 1. Basic Run

```bash
cd examples/simple_mlp
python main.py
```

### 2. Override Parameters

```bash
python main.py training.lr=0.01 model.hidden_dim=128 seed=123
```

### 3. Run with HSM

First, ensure HSM is installed:
```bash
pip install -e ../../  # Install HSM from parent directory
```

Then run a sweep:
```bash
hsm create sweep_config.yaml:test_sweep
hsm submit
hsm monitor
```

## Configuration

### Model Parameters

- `model.input_dim`: Input dimension (default: 10)
- `model.hidden_dim`: Hidden layer size (default: 64)
- `model.output_dim`: Output dimension (default: 1)

### Training Parameters

- `training.epochs`: Number of training epochs (default: 100)
- `training.lr`: Learning rate (default: 0.001)
- `training.gamma`: Decay factor for mock loss (default: 0.99)
- `training.simulate_time`: Sleep time per epoch in seconds (default: 0.01)

### Data Parameters

- `data.n_samples`: Number of synthetic data samples (default: 1000)
- `seed`: Random seed for reproducibility (default: 42)

### Output Parameters

- `output.dir`: Output directory (default: "./outputs")
- `output.save_results`: Whether to save results to file (default: true)

### Logging Parameters

- `logging.log_interval`: Console logging frequency (default: 10)
- `logging.wandb.enabled`: Enable W&B logging (default: true)
- `logging.wandb.project`: W&B project name (default: "hsm-simple-mlp")

## Sweep Examples

### Grid Sweep (test_sweep)
Tests 8 combinations (2×2×2):
```yaml
parameters:
  seed: [42, 123]
  training.lr: [0.001, 0.01]
  model.hidden_dim: [32, 64]
```

### Random Sweep
Tests 20 random combinations with distributions:
```yaml
parameters:
  seed: int_uniform(1, 1000)
  training.lr: log_uniform(0.0001, 0.1)
  model.hidden_dim: categorical([16, 32, 64, 128, 256])
```

### Bayesian Optimization
Uses W&B's Bayesian optimization to minimize `final_loss`.

## Dependency Management

The example is designed to work with minimal dependencies:

### Required (Core HSM functionality)
- `hydra-core`
- `omegaconf`

### Optional (Enhanced functionality)
- `torch`: For real MLP training (otherwise uses mock training)
- `numpy`: For better synthetic data generation
- `wandb`: For experiment tracking

### Installation Options

```bash
# Minimal (core functionality only)
pip install hydra-core omegaconf

# Full ML setup
pip install torch numpy wandb hydra-core omegaconf

# From requirements
pip install -r ../../requirements.txt
```

## Expected Output

### Successful Run
```
INFO:__main__:Starting simple MLP training example
INFO:__main__:PyTorch available: True
INFO:__main__:W&B available: True
INFO:__main__:Training completed!
INFO:__main__:Final loss: 0.123456
INFO:__main__:Best loss: 0.098765
INFO:__main__:Example completed successfully!
```

### Mock Mode (no PyTorch)
```
WARNING:__main__:PyTorch not available - using mock training
INFO:__main__:Using mock model (PyTorch not available)
INFO:__main__:Training completed!
INFO:__main__:Final loss: 0.456789
```

## Testing HSM Features

This example is perfect for testing HSM features because:

1. **Fast execution**: Each run takes ~1-2 seconds
2. **Deterministic**: Seeded for reproducible results
3. **Scalable**: Easy to adjust sweep size
4. **Observable**: Clear logging and metrics
5. **Portable**: Minimal dependencies

## Next Steps

1. Modify sweep configurations in `sweep_config.yaml`
2. Test different HSM features (distributed execution, monitoring, etc.)
3. Customize the training script for your use case
4. Use this as a template for more complex projects

## Troubleshooting

### Common Issues

1. **Import errors**: Install missing dependencies or run in mock mode
2. **Path issues**: Ensure you're in the `examples/simple_mlp` directory
3. **W&B login**: Run `wandb login` if using W&B logging
4. **HSM not found**: Install HSM with `pip install -e ../../`

### Debug Mode

Run with debug logging:
```bash
python main.py hydra.verbose=true
```

Add more simulation time for testing:
```bash
python main.py training.simulate_time=0.1
``` 