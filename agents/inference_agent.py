import asyncio
import os
import json
import time
from prometheus_client import start_http_server, Counter, Histogram
from confluent_kafka import Consumer, Producer

import sys
import os
import importlib.util

# Ensure project root and ml subdirectory are in sys.path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)
ml_path = os.path.join(project_root, "ml")
if ml_path not in sys.path:
    sys.path.append(ml_path)

# Load env variables and adjust hosts for local Windows execution
from agents.env_loader import load_env
load_env()

# Dynamically load InferenceEngine from ml/inference.py to avoid shadowing conflict
spec = importlib.util.spec_from_file_location("ml_inference_module", os.path.join(project_root, "ml", "inference.py"))
if spec is None or spec.loader is None:
    raise ImportError("Failed to load ml/inference.py spec or loader")
ml_inference_module = importlib.util.module_from_spec(spec)
sys.modules["ml_inference_module"] = ml_inference_module
spec.loader.exec_module(ml_inference_module)
InferenceEngine = ml_inference_module.InferenceEngine

# Prometheus Metrics
INFERENCE_LATENCY = Histogram('inference_latency_seconds', 'Latency of ML inference')
EVENTS_PROCESSED = Counter('events_processed_total', 'Total events processed')
ANOMALIES_DETECTED = Counter('anomalies_detected_total', 'Total anomalies detected')

class MLInferenceAgent:
    def __init__(self) -> None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.raw_topic = os.getenv("KAFKA_RAW_EVENTS_TOPIC", "raw-events")
        self.scored_topic = os.getenv("KAFKA_SCORED_EVENTS_TOPIC", "scored-events")
        self.update_topic = os.getenv("KAFKA_MODEL_UPDATES_TOPIC", "model-updates")
        self.threshold = float(os.getenv("ANOMALY_THRESHOLD", "0.7"))
        
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': os.getenv("KAFKA_CONSUMER_GROUP_ID", "ml-inference-group"),
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        
        self.update_consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'ml-inference-update-group',
            'auto.offset.reset': 'latest'
        })
        
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.engine = InferenceEngine()
        self.running = False

    async def _process_updates(self) -> None:
        """Listens on model-updates topic for hot-reload signal without restarting consumer."""
        self.update_consumer.subscribe([self.update_topic])
        while self.running:
            msg = self.update_consumer.poll(1.0)
            if msg is None or msg.error():
                await asyncio.sleep(1)
                continue
            val = msg.value()
            if val is None:
                await asyncio.sleep(1)
                continue
            
            try:
                update_event = json.loads(val.decode('utf-8'))
                version = update_event.get("version", "latest")
                print(f"Received model update signal for version: {version}")
                self.engine.reload_model(version)
            except Exception as e:
                print(f"Failed to process update: {e}")

    async def run(self) -> None:
        self.running = True
        self.consumer.subscribe([self.raw_topic])
        
        # Start Prometheus metrics server
        start_http_server(8001)
        
        # Start hot-reload listener in background
        asyncio.create_task(self._process_updates())
        
        print("MLInferenceAgent started polling...")
        try:
            while self.running:
                # Poll messages in loop
                msg = self.consumer.poll(1.0)
                if msg is None:
                    await asyncio.sleep(0.01)
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    start_time = time.time()
                    
                    val = msg.value()
                    if val is None:
                        continue
                    
                    event_dict = json.loads(val.decode('utf-8'))
                    scored_event = self.engine.infer(event_dict)
                    
                    # Apply explicit thresholding if required
                    if scored_event["anomaly_score"] >= self.threshold:
                        scored_event["is_anomaly"] = True
                        ANOMALIES_DETECTED.inc()
                    else:
                        scored_event["is_anomaly"] = False
                    
                    # Produce to scored-events
                    self.producer.produce(
                        topic=self.scored_topic,
                        key=scored_event["event_id"],
                        value=json.dumps(scored_event)
                    )
                    self.producer.poll(0)
                    
                    # Commit offset only on success
                    self.consumer.commit(message=msg)
                    
                    # Update metrics
                    INFERENCE_LATENCY.observe(time.time() - start_time)
                    EVENTS_PROCESSED.inc()
                    
                except Exception as e:
                    print(f"Error processing event: {e}")
                    
        finally:
            self.running = False
            self.consumer.close()
            self.update_consumer.close()
            self.producer.flush()

    def stop(self) -> None:
        self.running = False

if __name__ == "__main__":
    agent = MLInferenceAgent()
    asyncio.run(agent.run())
