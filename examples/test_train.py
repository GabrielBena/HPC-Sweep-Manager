#!/usr/bin/env python3
"""Simple test training script for sweep completion testing."""

import sys
import time


def main():
    # Parse Hydra-style arguments (key=value format)
    params = {}
    for arg in sys.argv[1:]:
        if "=" in arg:
            key, value = arg.split("=", 1)
            params[key] = value

    # Extract parameters with defaults
    learning_rate = float(params.get("learning_rate", "0.001"))
    batch_size = int(params.get("batch_size", "16"))
    output_dir = params.get("output.dir", ".")
    wandb_group = params.get("wandb.group", "test")

    print(f"Training with lr={learning_rate}, batch_size={batch_size}")
    print(f"Output dir: {output_dir}")
    print(f"W&B group: {wandb_group}")

    # Simulate some training time
    time.sleep(1)

    print("Training completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
