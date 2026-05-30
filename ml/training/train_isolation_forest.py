"""
IsolationForest Model Training Script
Uses scikit-learn 1.5 + MLflow 2.13 for experiment tracking
Retraining is scheduled daily via APScheduler (Model Retraining Agent)
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

import structlog

logger = structlog.get_logger(__name__)


def train_isolation_forest(
    data: np.ndarray,
    contamination: float = 0.1,
    n_estimators: int = 100,
    max_samples: str | int = "auto",
    random_state: int = 42,
    artifact_path: str = "/app/artifacts",
    model_version: str = "v1",
) -> dict[str, float]:
    """
    Train an IsolationForest anomaly detector, log to MLflow, and save artifact.

    Args:
        data: Feature matrix (n_samples, n_features)
        contamination: Expected proportion of anomalies
        n_estimators: Number of base estimators
        max_samples: Samples per tree
        random_state: Reproducibility seed
        artifact_path: Local path to save joblib artifact
        model_version: Tag for artifact versioning

    Returns:
        Dict of computed metrics
    """
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment(os.environ.get("MLFLOW_EXPERIMENT_NAME", "anomaly-detection"))

    # Normalise features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(data)

    with mlflow.start_run(run_name=f"isolation_forest_{model_version}") as run:
        # Log hyperparameters
        mlflow.log_params({
            "model_type": "IsolationForest",
            "contamination": contamination,
            "n_estimators": n_estimators,
            "max_samples": max_samples,
            "random_state": random_state,
            "n_features": data.shape[1],
            "n_samples": data.shape[0],
        })

        # Train
        model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            max_samples=max_samples,
            random_state=random_state,
            n_jobs=-1,
        )
        model.fit(X_scaled)

        # Compute metrics on training data
        scores = model.decision_function(X_scaled)
        predictions = model.predict(X_scaled)
        anomaly_rate = float((predictions == -1).mean())
        mean_score = float(scores.mean())
        std_score = float(scores.std())

        metrics = {
            "anomaly_rate": anomaly_rate,
            "mean_decision_score": mean_score,
            "std_decision_score": std_score,
        }
        mlflow.log_metrics(metrics)

        # Log model to MLflow
        mlflow.sklearn.log_model(
            model,
            artifact_path="isolation_forest",
            registered_model_name="anomaly-isolation-forest",
        )

        # Save artifact locally for hot-reload
        out_dir = Path(artifact_path)
        out_dir.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, out_dir / "isolation_forest.joblib")
        joblib.dump(scaler, out_dir / "scaler.joblib")
        (out_dir / "model_version.txt").write_text(model_version)

        logger.info(
            "IsolationForest trained and saved",
            run_id=run.info.run_id,
            version=model_version,
            **metrics,
        )

    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train IsolationForest anomaly detector")
    parser.add_argument("--n-features", type=int, default=10)
    parser.add_argument("--n-samples", type=int, default=10000)
    parser.add_argument("--contamination", type=float, default=0.1)
    parser.add_argument("--n-estimators", type=int, default=100)
    parser.add_argument("--version", type=str, default="v1")
    parser.add_argument("--artifact-path", type=str, default="/app/artifacts")
    args = parser.parse_args()

    # Generate synthetic training data for initial model
    rng = np.random.default_rng(42)
    normal_data = rng.standard_normal((args.n_samples, args.n_features))
    anomaly_data = rng.standard_normal((int(args.n_samples * 0.05), args.n_features)) * 3
    data = np.vstack([normal_data, anomaly_data])

    metrics = train_isolation_forest(
        data=data,
        contamination=args.contamination,
        n_estimators=args.n_estimators,
        artifact_path=args.artifact_path,
        model_version=args.version,
    )
    print(f"Training complete: {metrics}")
