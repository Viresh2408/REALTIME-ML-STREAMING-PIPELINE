import os
from io import BytesIO
from typing import Union

import boto3
import joblib
import numpy as np
import sklearn
from sklearn.ensemble import IsolationForest
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score

sklearn.set_config(assume_finite=True)


class AnomalyDetector:
    def __init__(self) -> None:
        self.model = IsolationForest(
            n_estimators=200, contamination=0.05, random_state=42, n_jobs=-1
        )
        self.is_trained: bool = False

    def train(self, X: np.ndarray) -> None:
        """Trains the Isolation Forest model."""
        self.model.fit(X)
        self.is_trained = True

    def predict(self, X: np.ndarray) -> dict[str, Union[float, bool, "list[float]", "list[bool]"]]:
        """
        Predicts anomaly scores and binary flags.
        Returns a dict: {score: float/list, is_anomaly: bool/list}
        """
        if not self.is_trained:
            raise ValueError("Model is not trained.")

        raw_scores = self.model.decision_function(X)
        # Normalise to [0, 1] where higher is more anomalous
        scores = 1.0 - (raw_scores - (-0.5)) / (0.5 - (-0.5))
        scores = np.clip(scores, 0.0, 1.0)

        # In scikit-learn's IsolationForest, predict(X) is defined exactly as decision_function(X) < 0
        # By doing this directly, we avoid executing all 200 trees twice!
        is_anomaly = raw_scores < 0.0

        return {
            "score": float(scores[0]) if len(scores) == 1 else scores.tolist(),
            "is_anomaly": bool(is_anomaly[0]) if len(is_anomaly) == 1 else is_anomaly.tolist(),
        }

    def evaluate(self, X_val: np.ndarray, y_val: np.ndarray) -> dict[str, float]:
        """Evaluates the model against ground truth labels."""
        if not self.is_trained:
            raise ValueError("Model is not trained.")

        preds = self.model.predict(X_val)
        is_anomaly = (preds == -1).astype(int)

        scores = -self.model.decision_function(X_val)

        return {
            "precision": float(precision_score(y_val, is_anomaly, zero_division=0)),
            "recall": float(recall_score(y_val, is_anomaly, zero_division=0)),
            "f1": float(f1_score(y_val, is_anomaly, zero_division=0)),
            "roc_auc": float(roc_auc_score(y_val, scores)) if len(np.unique(y_val)) > 1 else 0.0,
        }

    def save_to_minio(self, version: str) -> None:
        """Saves the trained model to MinIO."""
        if not self.is_trained:
            raise ValueError("Model is not trained.")

        bucket_name = os.getenv("MINIO_BUCKET_MODELS", "ml-models")
        object_name = f"isolation_forest/model_{version}.joblib"

        buffer = BytesIO()
        joblib.dump(self.model, buffer)
        buffer.seek(0)

        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "miniopassword123"),
        )

        try:
            s3.head_bucket(Bucket=bucket_name)
        except Exception:
            s3.create_bucket(Bucket=bucket_name)

        s3.upload_fileobj(buffer, bucket_name, object_name)
        print(f"Model saved to s3://{bucket_name}/{object_name}")

    def load_from_minio(self, version: str) -> None:
        """Loads a model version from MinIO."""
        bucket_name = os.getenv("MINIO_BUCKET_MODELS", "ml-models")
        object_name = f"isolation_forest/model_{version}.joblib"

        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "miniopassword123"),
        )

        buffer = BytesIO()
        s3.download_fileobj(bucket_name, object_name, buffer)
        buffer.seek(0)

        self.model = joblib.load(buffer)
        self.model.n_jobs = 1  # Force sequential execution to avoid joblib thread pool overhead on single-sample inference
        self.is_trained = True
        print(f"Model loaded from s3://{bucket_name}/{object_name}")
