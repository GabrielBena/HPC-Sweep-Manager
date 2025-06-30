#!/usr/bin/env python3
"""
Minimal example: Single layer MLP training with Hydra config and W&B tracking.

This is a simple example for testing and demonstrating the HPC Sweep Manager (HSM).
It trains a basic MLP on synthetic data with configurable hyperparameters.
"""

import os
import sys
import logging
import random
import time
from pathlib import Path

import hydra
from omegaconf import DictConfig, OmegaConf

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Try to import optional ML dependencies
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    import torch.nn.functional as F

    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    log.warning("PyTorch not available - using mock training")

try:
    import wandb

    WANDB_AVAILABLE = True
except ImportError:
    WANDB_AVAILABLE = False
    log.warning("W&B not available - skipping logging")

try:
    import numpy as np

    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    log.warning("NumPy not available - using basic Python")


class SimpleMLP:
    """Simple MLP implementation (with optional PyTorch backend)."""

    def __init__(self, input_dim, hidden_dim, output_dim):
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.output_dim = output_dim

        if TORCH_AVAILABLE:
            self.model = nn.Sequential(
                nn.Linear(input_dim, hidden_dim), nn.ReLU(), nn.Linear(hidden_dim, output_dim)
            )
        else:
            # Mock model for when PyTorch is not available
            self.model = None
            log.info("Using mock model (PyTorch not available)")

    def forward(self, x):
        if TORCH_AVAILABLE and self.model is not None:
            return self.model(x)
        else:
            # Mock forward pass
            if NUMPY_AVAILABLE:
                return np.random.randn(x.shape[0], self.output_dim)
            else:
                return [[random.random() for _ in range(self.output_dim)] for _ in range(len(x))]

    def parameters(self):
        if TORCH_AVAILABLE and self.model is not None:
            return self.model.parameters()
        else:
            return []


def generate_synthetic_data(n_samples, input_dim, output_dim, seed=42):
    """Generate synthetic regression data."""
    if NUMPY_AVAILABLE:
        np.random.seed(seed)
        X = np.random.randn(n_samples, input_dim)
        # Simple linear relationship with noise
        W_true = np.random.randn(input_dim, output_dim)
        y = X @ W_true + 0.1 * np.random.randn(n_samples, output_dim)

        if TORCH_AVAILABLE:
            return torch.FloatTensor(X), torch.FloatTensor(y)
        else:
            return X.tolist(), y.tolist()
    else:
        # Basic Python fallback
        random.seed(seed)
        X = [[random.random() for _ in range(input_dim)] for _ in range(n_samples)]
        y = [[random.random() for _ in range(output_dim)] for _ in range(n_samples)]
        return X, y


def train_model(cfg: DictConfig):
    """Train the MLP model."""
    log.info(f"Training with config: {OmegaConf.to_yaml(cfg)}")

    # Set random seeds
    random.seed(cfg.seed)
    if NUMPY_AVAILABLE:
        np.random.seed(cfg.seed)
    if TORCH_AVAILABLE:
        torch.manual_seed(cfg.seed)

    # Generate data
    X_train, y_train = generate_synthetic_data(
        cfg.data.n_samples, cfg.model.input_dim, cfg.model.output_dim, cfg.seed
    )

    # Create model
    model = SimpleMLP(cfg.model.input_dim, cfg.model.hidden_dim, cfg.model.output_dim)

    # Setup optimizer (if PyTorch available)
    if TORCH_AVAILABLE and model.model is not None:
        optimizer = optim.Adam(model.parameters(), lr=cfg.training.lr)
        criterion = nn.MSELoss()
    else:
        optimizer = None
        criterion = None

    # Training loop
    losses = []
    for epoch in range(cfg.training.epochs):
        if TORCH_AVAILABLE and model.model is not None:
            # Real training
            optimizer.zero_grad()
            outputs = model.forward(X_train)
            loss = criterion(outputs, y_train)
            loss.backward()
            optimizer.step()
            loss_value = loss.item()
        else:
            # Mock training
            loss_value = 1.0 * (cfg.training.gamma**epoch) + 0.1 * random.random()

        losses.append(loss_value)

        # Log to wandb if available
        if WANDB_AVAILABLE and cfg.logging.wandb.enabled:
            wandb.log(
                {
                    "epoch": epoch,
                    "loss": loss_value,
                    "lr": cfg.training.lr,
                }
            )

        # Console logging
        if epoch % cfg.logging.log_interval == 0:
            log.info(f"Epoch {epoch:4d}/{cfg.training.epochs}: Loss = {loss_value:.6f}")

        # Simulate some compute time
        time.sleep(cfg.training.simulate_time)

    # Final metrics
    final_loss = losses[-1]
    best_loss = min(losses)

    log.info(f"Training completed!")
    log.info(f"Final loss: {final_loss:.6f}")
    log.info(f"Best loss: {best_loss:.6f}")

    # Save results
    results = {
        "final_loss": final_loss,
        "best_loss": best_loss,
        "losses": losses,
        "config": OmegaConf.to_container(cfg, resolve=True),
    }

    if cfg.output.save_results:
        output_dir = Path(cfg.output.dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save as simple text file (no dependencies required)
        with open(output_dir / "results.txt", "w") as f:
            f.write(f"Final Loss: {final_loss:.6f}\n")
            f.write(f"Best Loss: {best_loss:.6f}\n")
            f.write(f"Config: {OmegaConf.to_yaml(cfg)}\n")

    return results


@hydra.main(version_base=None, config_path="configs", config_name="config")
def main(cfg: DictConfig) -> None:
    """Main training function."""
    log.info(f"Starting simple MLP training example")
    log.info(f"Working directory: {os.getcwd()}")
    log.info(f"PyTorch available: {TORCH_AVAILABLE}")
    log.info(f"W&B available: {WANDB_AVAILABLE}")
    log.info(f"NumPy available: {NUMPY_AVAILABLE}")

    # Initialize wandb if enabled and available
    if WANDB_AVAILABLE and cfg.logging.wandb.enabled:
        wandb.init(
            project=cfg.logging.wandb.project,
            entity=cfg.logging.wandb.entity,
            name=cfg.logging.wandb.run_name,
            config=OmegaConf.to_container(cfg, resolve=True),
            tags=cfg.logging.wandb.tags,
        )

    # Train model
    results = train_model(cfg)

    # Cleanup
    if WANDB_AVAILABLE and cfg.logging.wandb.enabled:
        wandb.finish()

    log.info("Example completed successfully!")
    return results


if __name__ == "__main__":
    main()
