import os
import pandas as pd
import numpy as np
import mlflow
from datetime import datetime

# Import components from local modules
from features.pipeline import FeaturePipeline
from models.isolation_forest import AnomalyDetector

def main() -> None:
    # 1. Load data
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(base_dir, "data", "raw", "cicids2017.csv")
    
    if not os.path.exists(data_path):
        print(f"Data not found at {data_path}. Please run python ml/data/download_cicids.py first.")
        return
        
    print("Loading CICIDS2017 dataset...")
    df = pd.read_csv(data_path)
    
    # Preprocessing labels to binary
    # The CICIDS dataset usually has a column ' Label' (with a leading space)
    label_col = next((col for col in df.columns if 'label' in col.lower()), None)
    
    if not label_col:
        print("Warning: Label column not found. Generating mock labels for evaluation.")
        df['Label'] = np.random.choice([0, 1], size=len(df), p=[0.95, 0.05])
    else:
        # Convert string labels to binary (assuming 'BENIGN' is 0, else 1)
        df['Label'] = (df[label_col].astype(str).str.strip().str.upper() != 'BENIGN').astype(int)

    # 2. Run feature pipeline
    print("Running FeaturePipeline...")
    pipeline = FeaturePipeline()
    pipeline.fit(df)
    X_train = pipeline.transform(df)
    y_train = df['Label'].values

    # 3. Train Isolation Forest
    print("Training IsolationForest model...")
    detector = AnomalyDetector()
    detector.train(X_train)

    # 4. Evaluate
    print("Evaluating model...")
    metrics = detector.evaluate(X_train, y_train)
    
    print("\n--- Evaluation Report ---")
    for k, v in metrics.items():
        print(f"{k.capitalize()}: {v:.4f}")
    print("-------------------------\n")

    # 5. Log to MLflow
    mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(mlflow_uri)
    experiment_name = os.getenv("MLFLOW_EXPERIMENT_NAME", "anomaly-detection")
    mlflow.set_experiment(experiment_name)
    
    with mlflow.start_run():
        mlflow.log_param("model_type", "IsolationForest")
        mlflow.log_param("n_estimators", 200)
        mlflow.log_param("contamination", 0.05)
        
        mlflow.log_metrics(metrics)
        
        # 6. Save model and pipeline to MinIO
        version = datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"Saving model version {version} to MinIO...")
        
        # Save pipeline and model
        pipeline.save(bucket_name="ml-models", object_name=f"pipeline/pipeline_{version}.joblib")
        detector.save_to_minio(version=version)
        
        mlflow.log_param("model_version", version)
        print("Training and upload complete.")

if __name__ == "__main__":
    main()
