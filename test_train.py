#!/usr/bin/env python3
"""Simple test training script for sweep completion testing."""

import argparse
import time
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--learning_rate', type=float, default=0.001)
    parser.add_argument('--batch_size', type=int, default=16)
    parser.add_argument('--output.dir', type=str, default='.')
    parser.add_argument('--wandb.group', type=str, default='test')
    
    args = parser.parse_args()
    
    print(f"Training with lr={args.learning_rate}, batch_size={args.batch_size}")
    print(f"Output dir: {getattr(args, 'output.dir')}")
    
    # Simulate some training time
    time.sleep(2)
    
    print("Training completed successfully!")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 