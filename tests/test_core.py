#!/usr/bin/env python3
"""Quick test script to verify HSM core components."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from hpc_sweep_manager.core.config_parser import SweepConfig
from hpc_sweep_manager.core.param_generator import ParameterGenerator
from hpc_sweep_manager.core.utils import create_sweep_id, setup_logging


def test_basic_functionality():
    """Test basic functionality of core components."""

    print("🚀 Testing HPC Sweep Manager Core Components")
    print("=" * 50)

    # Test logger
    logger = setup_logging("INFO")
    logger.info("Logger initialized successfully")

    # Test sweep ID generation
    sweep_id = create_sweep_id("test")
    print(f"✅ Generated sweep ID: {sweep_id}")

    # Test sweep config
    test_config = {
        "sweep": {"grid": {"seed": [1, 2, 3], "model.hidden_size": [128, 256], "optimizer.lr": [0.001, 0.01]}},
        "metadata": {"description": "Test sweep configuration"},
    }

    config = SweepConfig.from_dict(test_config)
    print(f"✅ Created sweep config with {len(config.grid)} grid parameters")

    # Test parameter generation
    generator = ParameterGenerator(config)
    combinations = generator.generate_combinations()
    print(f"✅ Generated {len(combinations)} parameter combinations")

    # Test validation
    errors = config.validate()
    if not errors:
        print("✅ Configuration validation passed")
    else:
        print(f"❌ Configuration validation failed: {errors}")

    # Show first few combinations
    print("\n📋 First 3 parameter combinations:")
    for i, combo in enumerate(combinations[:3], 1):
        print(f"  {i}. {combo}")

    # Test parameter info
    info = generator.get_parameter_info()
    print(f"\n📊 Parameter info:")
    print(f"  - Total combinations: {info['total_combinations']}")
    print(f"  - Grid combinations: {info['grid_combinations']}")
    print(f"  - Grid parameters: {list(info['grid_parameters'].keys())}")

    print("\n🎉 All core components working correctly!")
    return True


if __name__ == "__main__":
    try:
        test_basic_functionality()
        print("\n✅ All tests passed!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
