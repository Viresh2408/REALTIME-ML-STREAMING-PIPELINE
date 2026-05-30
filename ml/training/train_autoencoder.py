"""
Autoencoder Training Script — PyTorch 2.3 + MLflow 2.13
Complements IsolationForest for high-dimensional feature spaces.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import mlflow
import mlflow.pytorch
import numpy as np
import structlog
import torch
from torch.utils.data import DataLoader, TensorDataset

from ml.models.autoencoder import Autoencoder, AutoencoderTrainer

logger = structlog.get_logger(__name__)


def train_autoencoder(
    data: np.ndarray,
    n_epochs: int = 50,
    batch_size: int = 256,
    lr: float = 1e-3,
    latent_dim: int = 8,
    hidden_dims: list[int] | None = None,
    dropout: float = 0.1,
    artifact_path: str = "/app/artifacts",
    model_version: str = "ae-v1",
) -> dict[str, float]:
    """
    Train the autoencoder on normalised feature data, track with MLflow,
    and save the PyTorch checkpoint to disk.

    Args:
        data: numpy array (n_samples, n_features)
        n_epochs: Training epochs
        batch_size: Mini-batch size
        lr: Adam learning rate
        latent_dim: Bottleneck dimension
        hidden_dims: Encoder hidden layer sizes (decoder is mirrored)
        dropout: Dropout probability in encoder/decoder
        artifact_path: Local path to save checkpoint
        model_version: Tag for artifact naming

    Returns:
        Final training metrics dict
    """
    if hidden_dims is None:
        hidden_dims = [64, 32, 16]

    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment(os.environ.get("MLFLOW_EXPERIMENT_NAME", "anomaly-detection"))

    # Normalise data
    mean = data.mean(axis=0)
    std = data.std(axis=0) + 1e-8
    data_norm = (data - mean) / std

    tensor = torch.tensor(data_norm, dtype=torch.float32)
    dataset = TensorDataset(tensor)
    loader = DataLoader(
        dataset, batch_size=batch_size, shuffle=True, num_workers=0, pin_memory=False
    )

    n_features = data.shape[1]
    model = Autoencoder(
        n_features=n_features, latent_dim=latent_dim, hidden_dims=hidden_dims, dropout=dropout
    )
    trainer = AutoencoderTrainer(model=model, lr=lr)

    with mlflow.start_run(run_name=f"autoencoder_{model_version}") as run:
        mlflow.log_params(
            {
                "model_type": "Autoencoder",
                "n_features": n_features,
                "n_samples": len(data),
                "latent_dim": latent_dim,
                "hidden_dims": str(hidden_dims),
                "n_epochs": n_epochs,
                "batch_size": batch_size,
                "lr": lr,
                "dropout": dropout,
            }
        )

        best_loss = float("inf")
        for epoch in range(1, n_epochs + 1):
            # DataLoader yields (x,) tuples from TensorDataset
            class _UnpackLoader:
                def __iter__(self_):
                    for (x,) in loader:
                        yield x

                def __len__(self_):
                    return len(loader)

            loss = trainer.train_epoch(_UnpackLoader())
            mlflow.log_metric("train_loss", loss, step=epoch)

            if loss < best_loss:
                best_loss = loss

            if epoch % 10 == 0:
                logger.info("Autoencoder training", epoch=epoch, loss=round(loss, 6))

        # Compute reconstruction errors on training set for threshold calibration
        model.eval()
        with torch.no_grad():
            errors = model.reconstruction_error(tensor).numpy()

        p95_threshold = float(np.percentile(errors, 95))
        mean_error = float(errors.mean())

        metrics = {
            "best_train_loss": best_loss,
            "mean_reconstruction_error": mean_error,
            "p95_reconstruction_threshold": p95_threshold,
        }
        mlflow.log_metrics(metrics)

        # Log model to MLflow
        mlflow.pytorch.log_model(
            model,
            artifact_path="autoencoder",
            registered_model_name="anomaly-autoencoder",
        )

        # Save checkpoint locally
        out_dir = Path(artifact_path)
        out_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_path = str(out_dir / f"autoencoder_{model_version}.pt")
        trainer.save(checkpoint_path)

        # Save normalisation stats
        import json

        norm_path = out_dir / "autoencoder_normalisation.json"
        norm_path.write_text(json.dumps({"mean": mean.tolist(), "std": std.tolist()}))

        logger.info(
            "Autoencoder training complete",
            run_id=run.info.run_id,
            version=model_version,
            **metrics,
        )

    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Autoencoder anomaly detector")
    parser.add_argument("--n-features", type=int, default=10)
    parser.add_argument("--n-samples", type=int, default=10000)
    parser.add_argument("--n-epochs", type=int, default=50)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--latent-dim", type=int, default=8)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--version", type=str, default="ae-v1")
    parser.add_argument("--artifact-path", type=str, default="/app/artifacts")
    args = parser.parse_args()

    rng = np.random.default_rng(42)
    normal = rng.standard_normal((args.n_samples, args.n_features))
    anomalies = rng.standard_normal((int(args.n_samples * 0.05), args.n_features)) * 4
    data = np.vstack([normal, anomalies])

    metrics = train_autoencoder(
        data=data,
        n_epochs=args.n_epochs,
        batch_size=args.batch_size,
        lr=args.lr,
        latent_dim=args.latent_dim,
        artifact_path=args.artifact_path,
        model_version=args.version,
    )
    print(f"Training complete: {metrics}")
