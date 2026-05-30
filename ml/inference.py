import os
import time
from typing import Any

import pandas as pd
from features.pipeline import FeaturePipeline
from models.isolation_forest import AnomalyDetector


class InferenceEngine:
    def __init__(self, version: str = "latest") -> None:
        self.pipeline = FeaturePipeline()
        self.model = AnomalyDetector()

        # Resolve 'latest' dynamically by listing objects in MinIO
        if version == "latest":
            try:
                import boto3

                s3 = boto3.client(
                    "s3",
                    endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
                    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "miniopassword123"),
                )
                bucket_name = os.getenv("MINIO_BUCKET_MODELS", "ml-models")
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix="pipeline/pipeline_")
                if "Contents" in response and len(response["Contents"]) > 0:
                    # Sort by Key descending to get the lexicographically latest timestamped key
                    objects = sorted(response["Contents"], key=lambda x: x["Key"], reverse=True)
                    latest_key = objects[0]["Key"]
                    # Key structure: pipeline/pipeline_20260527_130637.joblib -> extract 20260527_130637
                    version = latest_key.split("pipeline_")[1].split(".joblib")[0]
                    print(f"Resolved 'latest' version dynamically from MinIO: {version}")
                else:
                    version = "default"
            except Exception as e:
                print(f"Error resolving latest model version from MinIO: {e}")
                version = "default"

        self.reload_model(version)

    def reload_model(self, version: str) -> None:
        """Hot-reload model and pipeline without restarting the service."""
        print(f"Reloading model version: {version}...")
        try:
            self.pipeline.load(
                bucket_name="ml-models", object_name=f"pipeline/pipeline_{version}.joblib"
            )
            self.model.load_from_minio(version=version)
            print("Successfully reloaded model and pipeline.")
        except Exception as e:
            print(f"Error loading model/pipeline (version {version}): {e}")
            # Fallback for initial boot when no models exist yet
            pass

    def infer(self, event_dict: dict[str, Any]) -> dict[str, Any]:
        """
        Runs inference on an incoming event dictionary within < 5ms.
        """
        start_time = time.time()

        raw_features = event_dict.get("features", {})

        # Create a single-row DataFrame
        df = pd.DataFrame([raw_features])

        # Transform
        try:
            X = self.pipeline.transform(df)

            # Predict
            pred_result = self.model.predict(X)
            score = pred_result["score"]
            is_anomaly = pred_result["is_anomaly"]

        except Exception as e:
            print(f"Inference error: {e}")
            score = 0.0
            is_anomaly = False

        # Build ScoredEvent dict matching Avro schema
        scored_event = {
            "event_id": event_dict.get("event_id", ""),
            "source_id": event_dict.get("source_id", ""),
            "event_type": event_dict.get("event_type", ""),
            "features": event_dict.get("features", {}),
            "event_time": event_dict.get("event_time", 0),
            "anomaly_score": score,
            "is_anomaly": is_anomaly,
            "model_version": "isolation_forest_v1",
            "processed_at": int(time.time() * 1000),
        }

        latency_ms = (time.time() - start_time) * 1000
        if latency_ms > 5.0:
            print(f"Warning: Inference took {latency_ms:.2f}ms (target < 5ms)")

        return scored_event
