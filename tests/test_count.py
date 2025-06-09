#!/usr/bin/env python3
"""Test the count functionality."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hpc_sweep_manager.cli.sweep import run_sweep
from rich.console import Console
import logging


def test_count():
    console = Console()
    logger = logging.getLogger()

    run_sweep(
        config_path=Path("tests/test_sweep.yaml"),
        mode="individual",
        dry_run=False,
        count_only=True,  # This should just count and exit
        max_runs=None,
        walltime="04:00:00",
        resources="select=1:ncpus=4:mem=16gb",
        group=None,
        priority=None,
        console=console,
        logger=logger,
    )


if __name__ == "__main__":
    test_count()
