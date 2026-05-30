import asyncio
import os
import sys
import time

# Ensure project root is in sys.path
project_root = "/app"
if project_root not in sys.path:
    sys.path.append(project_root)
ml_path = "/app/ml"
if ml_path not in sys.path:
    sys.path.append(ml_path)

# Set mock env variables for boto3/S3 to avoid connectivity issues during dry-run
os.environ["MINIO_ENDPOINT"] = "minio:9000"
os.environ["MINIO_ACCESS_KEY"] = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
os.environ["MINIO_SECRET_KEY"] = os.getenv("MINIO_SECRET_KEY", "miniopassword123")

from worker.hot_reload import ReadWriteLock
from worker.inference_worker import ProductionInferenceEngine
from worker.preprocessor import EventPreprocessor


async def run_tests():
    print("=== STARTING ML PREPROCESSOR & INFERENCE ENGINE TESTS ===")

    # 1. Test EventPreprocessor loading and fallback
    print("\n[1] Initializing EventPreprocessor...")
    preprocessor = EventPreprocessor("latest")
    print(f"Preprocessor Pipeline Features: {preprocessor.pipeline.features}")
    print(f"Is pipeline fitted? {preprocessor.pipeline.is_fitted}")
    assert preprocessor.pipeline.is_fitted, "Pipeline should be fitted (even via dummy fallback)!"

    # 2. Test Mean Imputation with missing features
    # Let's pass an event with some features missing (e.g. fwd_packets/s, flag_counts)
    raw_event = {
        "event_id": "evt_test_12345",
        "source_id": "test_agent",
        "event_type": "BENIGN",
        "features": {
            "packet_length": 1500.0,
            "flow_duration": 45000.0,
            "bwd_packets/s": 42.0,
            # missing: fwd_packets/s, flag_counts
        },
        "event_time": int(time.time() * 1000),
    }

    print("\n[2] Testing sub-millisecond preprocessing and imputation...")
    start_time = time.perf_counter()
    X = preprocessor.preprocess(raw_event)
    end_time = time.perf_counter()

    latency_ms = (end_time - start_time) * 1000
    print(f"Preprocessed Feature Array Shape: {X.shape}")
    print(f"Preprocessed Feature Array Value: {X}")
    print(f"Preprocessing Latency: {latency_ms:.4f} ms")

    assert X.shape == (1, 5), "Preprocessed array must be 1x5!"
    assert latency_ms < 1.0, "Preprocessing must execute in under 1ms!"
    print("✅ Preprocessing and imputation success!")

    # 3. Test ProductionInferenceEngine
    print("\n[3] Initializing ProductionInferenceEngine with ReadWriteLock...")
    rw_lock = ReadWriteLock()
    engine = ProductionInferenceEngine(rw_lock)

    print("\n[4] Running inference...")
    scored_event = await engine.infer(raw_event)

    print("Scored Event Output:")
    for k, v in scored_event.items():
        print(f"- {k}: {v}")

    assert scored_event["event_id"] == raw_event["event_id"], "Event ID must match!"
    assert "anomaly_score" in scored_event, "Score must be present!"
    assert "is_anomaly" in scored_event, "Anomaly flag must be present!"
    assert scored_event["model_version"] == engine.version, "Model version must match!"
    print("✅ Inference complete and output schema validated!")
    print("\n=== ALL ML INFRASTRUCTURE TESTS COMPLETED SUCCESSFULLY ===")


if __name__ == "__main__":
    asyncio.run(run_tests())
