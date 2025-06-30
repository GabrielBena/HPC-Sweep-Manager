# HSM Examples

This directory contains examples demonstrating how to use the HPC Sweep Manager (HSM) for different types of projects and use cases.

## Available Examples

### 🚀 Simple MLP (`simple_mlp/`)

A minimal machine learning example that demonstrates the core HSM functionality:

- **Purpose**: Train a single-layer MLP with configurable hyperparameters
- **Dependencies**: Minimal (works with just Hydra + OmegaConf, optional PyTorch/NumPy/W&B)
- **Runtime**: Fast (~1-2 seconds per run)
- **Features**: Hydra configuration, W&B logging, mock training mode
- **Best for**: Learning HSM basics, testing, development

**Quick start:**
```bash
cd simple_mlp
python main.py
```

## Using Examples for Testing

These examples are designed to be perfect for:

1. **Learning HSM**: Start with `simple_mlp` to understand the basic workflow
2. **Testing features**: Fast execution makes it ideal for testing HSM functionality
3. **Development**: Use as templates for your own projects
4. **Documentation**: Examples used in tutorials and documentation

## Example Structure

Each example follows a consistent structure:

```
example_name/
├── main.py              # Main training script
├── configs/
│   └── config.yaml      # Hydra configuration
├── hsm_config.yaml      # HSM configuration
├── sweep_config.yaml    # Example sweep configurations  
├── requirements.txt     # Python dependencies
├── test_example.py      # Validation script
└── README.md           # Example-specific documentation
```

## Dependencies

Examples are designed with minimal dependencies:

- **Core**: Only Hydra and OmegaConf are required
- **Optional**: Additional ML libraries for enhanced functionality
- **Graceful degradation**: Examples work even with missing optional dependencies

Install dependencies per example:
```bash
# Minimal setup
pip install hydra-core omegaconf

# Full setup for ML examples  
pip install -r simple_mlp/requirements.txt

# Or use optional dependencies
pip install -e .[examples]
```

## Running Examples

### Direct Execution
```bash
cd simple_mlp
python main.py training.lr=0.01 model.hidden_dim=128
```

### With HSM
```bash
cd simple_mlp
hsm create sweep_config.yaml:test_sweep
hsm submit
hsm monitor
```

### Testing
```bash
python simple_mlp/test_example.py
```

## Contributing Examples

When adding new examples:

1. Follow the standard structure shown above
2. Include comprehensive documentation
3. Make dependencies optional where possible
4. Add validation tests
5. Ensure fast execution for testing
6. Update this README

## Troubleshooting

### Common Issues

1. **Import errors**: Install missing dependencies or run in mock mode
2. **Path issues**: Ensure you're in the correct example directory
3. **HSM not found**: Install HSM with `pip install -e .` from the root directory

### Getting Help

- Check example-specific README files
- Run with `--help` to see available options
- Use `--cfg job` to see current configuration
- Enable debug mode: `hydra.verbose=true`

## Next Steps

1. Start with the `simple_mlp` example
2. Modify configurations to understand parameter handling
3. Try different sweep types (grid, random, bayesian)
4. Test distributed execution features
5. Use examples as templates for your own projects 