import os
from io import BytesIO

import boto3
import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler


class FeaturePipeline:
    def __init__(self) -> None:
        # Implementing ALL features from dataset_and_model.docx Section 5 for Network (CICIDS)
        self.features: list[str] = [
            "packet_length",
            "flow_duration",
            "fwd_packets/s",
            "bwd_packets/s",
            "flag_counts"
        ]

        # Features requiring log1p transformation for skewness
        self.skewed_features: list[str] = [
            "packet_length",
            "flow_duration"
        ]

        self.scaler = StandardScaler()
        self.is_fitted: bool = False

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handles missing/infinite values and applies log1p to skewed features."""
        # Ensure all required features exist, fill with 0 if missing from incoming data
        for feature in self.features:
            if feature not in df.columns:
                df[feature] = 0.0

        df_copy = df[self.features].copy()

        # Handle infinite and missing values
        df_copy.replace([np.inf, -np.inf], np.nan, inplace=True)
        df_copy.fillna(0, inplace=True)

        # Apply log1p to skewed features
        for col in self.skewed_features:
            if col in df_copy.columns:
                # Ensure no negative values before log1p
                df_copy[col] = np.log1p(np.maximum(df_copy[col], 0))

        return df_copy

    def fit(self, df: pd.DataFrame) -> None:
        """Fit the scaler on the processed DataFrame."""
        processed_df = self._preprocess(df)
        self.scaler.fit(processed_df)
        self.is_fitted = True

    def transform(self, df: pd.DataFrame) -> np.ndarray:
        """Transform the DataFrame using the fitted scaler."""
        if not self.is_fitted:
            raise ValueError("FeaturePipeline must be fitted before transform.")
        processed_df = self._preprocess(df)
        return self.scaler.transform(processed_df)

    def save(self, bucket_name: str, object_name: str) -> None:
        """Saves the pipeline state (scaler + features list) to MinIO using joblib."""
        if not self.is_fitted:
            raise ValueError("Cannot save unfitted pipeline.")

        buffer = BytesIO()
        joblib.dump({"scaler": self.scaler, "features": self.features}, buffer)
        buffer.seek(0)

        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "miniopassword123")
        )

        # Ensure bucket exists
        try:
            s3.head_bucket(Bucket=bucket_name)
        except Exception:
            s3.create_bucket(Bucket=bucket_name)

        s3.upload_fileobj(buffer, bucket_name, object_name)
        print(f"Pipeline saved to s3://{bucket_name}/{object_name}")

    def load(self, bucket_name: str, object_name: str) -> None:
        """Loads the pipeline state from MinIO."""
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "miniopassword123")
        )
        buffer = BytesIO()
        s3.download_fileobj(bucket_name, object_name, buffer)
        buffer.seek(0)

        data = joblib.load(buffer)
        self.scaler = data["scaler"]
        self.features = data["features"]
        self.is_fitted = True
        print(f"Pipeline loaded from s3://{bucket_name}/{object_name}")
