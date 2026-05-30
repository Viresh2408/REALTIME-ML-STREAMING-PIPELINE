import os
import sys
from pathlib import Path


def load_env():
    # Locate the .env file in the project directory
    current_dir = Path(__file__).resolve().parent
    env_file = None
    for parent in [current_dir, current_dir.parent, current_dir.parent.parent]:
        env_path = parent / ".env"
        if env_path.exists():
            env_file = env_path
            break

    if env_file:
        print(f"Loading environment from {env_file}")
        with open(env_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip("'\"")
                    if key not in os.environ:
                        os.environ[key] = val

    # If running on Windows host (not inside a container), adjust hosts to localhost
    if sys.platform == "win32":
        # Adjust KAFKA_BOOTSTRAP_SERVERS
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        bootstrap = bootstrap.replace("kafka:29092", "localhost:9092")
        bootstrap = bootstrap.replace("kafka:9092", "localhost:9092")
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap
        print(f"Windows host detected. Set KAFKA_BOOTSTRAP_SERVERS to: {os.environ['KAFKA_BOOTSTRAP_SERVERS']}")

        # Adjust DATABASE_URL/url params
        db_url = os.environ.get("DATABASE_URL")
        if db_url:
            db_url = db_url.replace("timescaledb:5432", "localhost:5432")
            db_url = db_url.replace("anomaly_worker:WorkerPass123!", "worker_rw:WorkerRw_SecurePass2!")
            db_url = db_url.replace("anomaly_admin:StrongPass123!", "worker_rw:WorkerRw_SecurePass2!")
            os.environ["DATABASE_URL"] = db_url
            print("Windows host detected. Adjusted DATABASE_URL for worker_rw")
        else:
            # Create DATABASE_URL if it doesn't exist
            os.environ["DATABASE_URL"] = "postgresql://worker_rw:WorkerRw_SecurePass2!@localhost:5432/anomaly_db"
            print("DATABASE_URL was empty. Created default local connection URL for worker_rw.")

        # Adjust MINIO_ENDPOINT
        minio = os.environ.get("MINIO_ENDPOINT")
        if minio:
            minio = minio.replace("minio:9000", "localhost:9000")
            os.environ["MINIO_ENDPOINT"] = minio

        # Adjust MLFLOW_TRACKING_URI
        mlflow = os.environ.get("MLFLOW_TRACKING_URI")
        if mlflow:
            mlflow = mlflow.replace("mlflow:5000", "localhost:5000")
            os.environ["MLFLOW_TRACKING_URI"] = mlflow

        # Adjust REDIS_URL
        redis = os.environ.get("REDIS_URL")
        if redis:
            redis = redis.replace("redis:6379", "localhost:6379")
            os.environ["REDIS_URL"] = redis
