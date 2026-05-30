"""
ml/worker/preprocessor.py
──────────────────────────
EventPreprocessor — production feature preprocessing for the ML inference worker.

Responsibilities (agents.docx § 3 / dataset_and_model.docx § 5):
  • Load FeaturePipeline (scaler + feature list + skew config) from MinIO on startup
  • preprocess(raw_event_dict) → numpy array in < 1 ms
  • Impute missing / NaN / Inf features with column mean (i.e. zero in scaled space)
  • Graceful fallback to dummy-fitted pipeline when MinIO is unreachable at boot

Feature categories (dataset_and_model.docx § 5, Table 4):
  Network (CICIDS): packet_length, flow_duration, fwd_packets/s, bwd_packets/s, flag_counts
  Financial:        price, volume, bid_ask_spread, price_return_1m, volume_z_score
  Server metrics:   cpu_pct, mem_pct, disk_io_bytes, net_rx_bytes, error_rate
  Temporal:         hour_of_day, day_of_week, is_market_hours
  Derived:          rolling_mean_5m, rolling_std_5m, deviation_from_mean

The scaler & skewed_features list are serialised inside the pipeline joblib artifact so
this class does NOT need to hard-code them — it reads them from the loaded pipeline.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import boto3
import structlog

try:
    from ml.features.pipeline import FeaturePipeline
except ImportError:
    from features.pipeline import FeaturePipeline  # type: ignore[no-redef]

logger = structlog.get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Fallback feature set — used ONLY when MinIO returns no pipeline artifact.
# Matches Section 5, Table 4 (all 5 categories concatenated).
# ──────────────────────────────────────────────────────────────────────────────
_FALLBACK_FEATURES: List[str] = [
    # Network (CICIDS)
    "packet_length", "flow_duration", "fwd_packets/s", "bwd_packets/s", "flag_counts",
    # Financial
    "price", "volume", "bid_ask_spread", "price_return_1m", "volume_z_score",
    # Server metrics
    "cpu_pct", "mem_pct", "disk_io_bytes", "net_rx_bytes", "error_rate",
    # Temporal
    "hour_of_day", "day_of_week", "is_market_hours",
    # Derived
    "rolling_mean_5m", "rolling_std_5m", "deviation_from_mean",
]

_FALLBACK_SKEWED: List[str] = [
    "packet_length", "flow_duration",
    "disk_io_bytes", "net_rx_bytes",
    "volume",
]

_BOOTSTRAP_MOCK_ROW: Dict[str, float] = {f: 0.0 for f in _FALLBACK_FEATURES}


class EventPreprocessor:
    """
    Loads a FeaturePipeline artifact from MinIO and applies it to raw event
    dictionaries in under 1 ms per event (vectorised NumPy, no DataFrame).

    Thread / coroutine safety:
      preprocess() is pure NumPy — no GIL-releasing I/O, safe to call concurrently.
      _load_pipeline() is only called once at construction time.
    """

    def __init__(self, version: str = "latest") -> None:
        self.pipeline: FeaturePipeline = FeaturePipeline()
        self.version: str = version
        self._load_pipeline(version)

    # ── MinIO bootstrap ───────────────────────────────────────────────────────

    def _load_pipeline(self, version: str) -> None:
        """
        Resolve the pipeline version from MinIO and load it.
        Falls back to a dummy-fitted pipeline on any error so the worker
        can start immediately and process events (with degraded accuracy).
        """
        bucket  = os.getenv("MINIO_BUCKET_MODELS", "ml-models")
        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        ak       = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        sk       = os.getenv("MINIO_SECRET_KEY", "miniopassword123")

        # ── Step 1: resolve "latest" tag to a real version string ─────────────
        resolved_version = version
        if version == "latest":
            resolved_version = self._resolve_latest_version(bucket, endpoint, ak, sk)

        self.version = resolved_version

        # ── Step 2: download and load the joblib artifact ─────────────────────
        object_name = f"pipeline/pipeline_{resolved_version}.joblib"
        t0 = time.perf_counter()
        try:
            logger.info(
                "preprocessor.loading_pipeline",
                bucket=bucket,
                object=object_name,
                version=resolved_version,
            )
            self.pipeline.load(bucket_name=bucket, object_name=object_name)
            elapsed_ms = (time.perf_counter() - t0) * 1_000
            logger.info(
                "preprocessor.pipeline_loaded",
                version=resolved_version,
                features=len(self.pipeline.features),
                elapsed_ms=round(elapsed_ms, 1),
            )
        except Exception as load_err:
            elapsed_ms = (time.perf_counter() - t0) * 1_000
            logger.error(
                "preprocessor.pipeline_load_failed",
                version=resolved_version,
                object=object_name,
                error=str(load_err),
                elapsed_ms=round(elapsed_ms, 1),
            )
            self._apply_fallback_pipeline()

    def _resolve_latest_version(
        self,
        bucket: str,
        endpoint: str,
        ak: str,
        sk: str,
    ) -> str:
        """
        List pipeline/ prefix in MinIO and return the lexicographically last
        version string (timestamps sort correctly as strings: YYYYMMDD_HHMMSS).
        Returns "default" if no artifact exists yet.
        """
        try:
            s3 = boto3.client(
                "s3",
                endpoint_url=f"http://{endpoint}",
                aws_access_key_id=ak,
                aws_secret_access_key=sk,
                region_name="us-east-1",
            )
            resp = s3.list_objects_v2(Bucket=bucket, Prefix="pipeline/pipeline_")
            contents = resp.get("Contents", [])
            if not contents:
                logger.warning("preprocessor.no_pipeline_in_minio", bucket=bucket)
                return "default"

            # Sort descending by key name (timestamp embedded in filename)
            latest_key: str = sorted(
                (obj["Key"] for obj in contents), reverse=True
            )[0]
            # Extract version: pipeline/pipeline_<version>.joblib
            ver = latest_key.split("pipeline_", 1)[1].removesuffix(".joblib")
            logger.info("preprocessor.resolved_latest", version=ver)
            return ver

        except Exception as list_err:
            logger.error(
                "preprocessor.resolve_latest_failed",
                error=str(list_err),
                fallback="default",
            )
            return "default"

    def _apply_fallback_pipeline(self) -> None:
        """
        Fit a dummy pipeline on a zero-row so is_fitted=True and the worker
        can start processing events without crashing.
        All missing values will impute to 0 in scaled space (column mean).
        """
        try:
            mock_df = pd.DataFrame([_BOOTSTRAP_MOCK_ROW])
            # Override pipeline feature list with full spec list before fit
            self.pipeline.features = list(_FALLBACK_FEATURES)
            self.pipeline.skewed_features = list(_FALLBACK_SKEWED)
            self.pipeline.fit(mock_df)
            self.version = "dummy-v0"
            logger.warning(
                "preprocessor.using_dummy_pipeline",
                reason="MinIO unavailable at boot",
                features=len(self.pipeline.features),
            )
        except Exception as fit_err:
            # Absolute last resort: mark as not fitted — preprocess() handles gracefully
            self.pipeline.is_fitted = False
            logger.critical(
                "preprocessor.dummy_fit_failed",
                error=str(fit_err),
            )

    # ── Core preprocessing (hot path) ─────────────────────────────────────────

    def preprocess(self, raw_event_dict: Dict[str, Any]) -> np.ndarray:
        """
        Transform a raw event dictionary to a (1, n_features) float64 array.

        Algorithm (< 1 ms per event):
          1. Extract features sub-dict from raw_event_dict["features"]
          2. For each expected feature column:
             a. Missing / None / NaN / ±Inf → impute as 0.0 in scaled space
                (equivalent to column mean because (mean - mean) / scale = 0)
             b. Otherwise cast to float64, apply log1p if skewed, then scale
          3. Return shape (1, n_features) array

        Spec: dataset_and_model.docx § 5 Feature Engineering, Table 4.
        """
        if not self._pipeline_is_usable():
            # Return zero vector matching expected feature dimensionality
            n = len(self.pipeline.features) if self.pipeline.features else len(_FALLBACK_FEATURES)
            return np.zeros((1, n), dtype=np.float64)

        features_dict: Dict[str, Any] = raw_event_dict.get("features", {})
        feature_names: List[str] = self.pipeline.features
        skewed: List[str] = getattr(self.pipeline, "skewed_features", [])
        mean: np.ndarray = self.pipeline.scaler.mean_
        scale: np.ndarray = self.pipeline.scaler.scale_

        n = len(feature_names)
        x = np.empty(n, dtype=np.float64)

        for i, col in enumerate(feature_names):
            raw_val = features_dict.get(col)
            x[i] = self._coerce_feature(raw_val, col, i, mean, scale, skewed)

        return x.reshape(1, -1)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _pipeline_is_usable(self) -> bool:
        """Return True only if the scaler has been properly fitted."""
        scaler = self.pipeline.scaler
        return (
            self.pipeline.is_fitted
            and hasattr(scaler, "mean_")
            and hasattr(scaler, "scale_")
            and scaler.mean_ is not None
            and scaler.scale_ is not None
        )

    @staticmethod
    def _coerce_feature(
        raw_val: Any,
        col: str,
        idx: int,
        mean: np.ndarray,
        scale: np.ndarray,
        skewed: List[str],
    ) -> float:
        """
        Coerce a single raw feature value to a scaled float64.
        Returns 0.0 (= scaled mean) for any invalid / missing input.
        """
        # Fast-path: missing or None
        if raw_val is None:
            return 0.0

        try:
            v = float(raw_val)
        except (ValueError, TypeError):
            return 0.0

        # Reject non-finite values
        if not np.isfinite(v):
            return 0.0

        # Apply log1p to positively skewed features (prevents inf distortion)
        if col in skewed:
            v = np.log1p(max(v, 0.0))

        # Standard scaling: z = (x - mean) / std
        std = scale[idx]
        if std == 0.0:
            return 0.0  # constant feature — safe to zero
        return (v - mean[idx]) / std
