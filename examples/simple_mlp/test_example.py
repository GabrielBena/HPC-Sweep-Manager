#!/usr/bin/env python3
"""
Test script for the Simple MLP example.

This script validates that the example runs correctly in different configurations.
"""

import os
import sys
import subprocess
import tempfile
import shutil
from pathlib import Path


def run_command(cmd, cwd=None, timeout=60):
    """Run a command and return the result."""
    try:
        result = subprocess.run(
            cmd, shell=True, cwd=cwd, capture_output=True, text=True, timeout=timeout
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Command timed out"


def test_basic_run():
    """Test basic run with minimal parameters."""
    print("Testing basic run...")

    # Run with minimal epochs and disabled wandb
    cmd = (
        "python main.py "
        "training.epochs=5 "
        "training.simulate_time=0.001 "
        "logging.wandb.enabled=false "
        "output.dir=./test_outputs"
    )

    success, stdout, stderr = run_command(cmd, cwd=".")

    if success:
        print("✓ Basic run successful")
        return True
    else:
        print(f"✗ Basic run failed")
        print(f"STDOUT: {stdout}")
        print(f"STDERR: {stderr}")
        return False


def test_parameter_overrides():
    """Test parameter overrides."""
    print("Testing parameter overrides...")

    cmd = (
        "python main.py "
        "seed=123 "
        "model.hidden_dim=32 "
        "training.lr=0.01 "
        "training.epochs=3 "
        "training.simulate_time=0.001 "
        "logging.wandb.enabled=false "
        "output.dir=./test_outputs_override"
    )

    success, stdout, stderr = run_command(cmd, cwd=".")

    if success:
        print("✓ Parameter overrides successful")
        return True
    else:
        print(f"✗ Parameter overrides failed")
        print(f"STDOUT: {stdout}")
        print(f"STDERR: {stderr}")
        return False


def test_help():
    """Test help output."""
    print("Testing help output...")

    cmd = "python main.py --help"
    success, stdout, stderr = run_command(cmd, cwd=".")

    if success and "Hydra" in stdout:
        print("✓ Help output successful")
        return True
    else:
        print(f"✗ Help output failed")
        return False


def test_config_generation():
    """Test Hydra config generation."""
    print("Testing config generation...")

    cmd = "python main.py --cfg job"
    success, stdout, stderr = run_command(cmd, cwd=".")

    if success and "seed:" in stdout:
        print("✓ Config generation successful")
        return True
    else:
        print(f"✗ Config generation failed")
        print(f"STDOUT: {stdout}")
        print(f"STDERR: {stderr}")
        return False


def cleanup_test_outputs():
    """Clean up test output directories."""
    test_dirs = ["test_outputs", "test_outputs_override"]
    for dir_name in test_dirs:
        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)
            print(f"Cleaned up {dir_name}")


def main():
    """Run all tests."""
    print("=" * 50)
    print("Testing Simple MLP Example")
    print("=" * 50)

    # Change to the example directory
    os.chdir("examples/simple_mlp")

    # Clean up any existing test outputs
    cleanup_test_outputs()

    tests = [
        test_help,
        test_config_generation,
        test_basic_run,
        test_parameter_overrides,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        try:
            if test():
                passed += 1
            else:
                print("Test failed!")
        except Exception as e:
            print(f"Test error: {e}")
        print()

    # Clean up
    cleanup_test_outputs()

    print("=" * 50)
    print(f"Test Results: {passed}/{total} passed")
    print("=" * 50)

    if passed == total:
        print("🎉 All tests passed!")
        return True
    else:
        print("❌ Some tests failed!")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
