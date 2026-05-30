import sys
import os
import json
import asyncio
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncpg
from confluent_kafka import Producer

# Ensure project root is in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.env_loader import load_env
load_env()

class RetrainingAgent:
    def __init__(self) -> None:
        self.db_url = os.getenv("DATABASE_URL", "postgresql://worker_rw:WorkerRw_SecurePass2!@localhost:5432/anomaly_db")
        if self.db_url.startswith("postgresql+asyncpg://"):
            self.db_url = self.db_url.replace("postgresql+asyncpg://", "postgresql://")
            
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.update_topic = os.getenv("KAFKA_MODEL_UPDATES_TOPIC", "model-updates")
        
        self.scheduler = AsyncIOScheduler()
        self.scheduler.add_job(self.run_retraining_job, 'cron', hour=3, minute=0, timezone='UTC')

    async def extract_data(self) -> str:
        """Extracts last 30 days of labelled data to a CSV for retraining."""
        print("Extracting data for retraining from TimescaleDB...")
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                # Mocking extraction logic to file
                query = "SELECT COUNT(*) FROM anomaly.anomaly_events WHERE event_time > NOW() - INTERVAL '30 days'"
                count = await conn.fetchval(query)
                print(f"Found {count} events in the last 30 days for retraining.")
        except Exception as e:
            print(f"DB Error: {e}")
        finally:
            await pool.close()
            
        return "latest_retrain"

    async def run_retraining_job(self) -> None:
        print(f"[{datetime.utcnow()}] Starting scheduled model retraining...")
        
        try:
            # 1. Extract data from DB
            await self.extract_data()
            
            # 2. Trigger the ML pipeline
            print("Triggering ML training pipeline...")
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            train_script = os.path.join(base_dir, "ml", "train.py")
            
            # Subprocess execution for encapsulated execution
            proc = await asyncio.create_subprocess_exec('python', train_script)
            await proc.wait()
            
            if proc.returncode != 0:
                print("Training pipeline failed.")
                return

            version = datetime.now().strftime("%Y%m%d_%H%M%S")
            print(f"Model retrained successfully. Version assigned: {version}")
            
            # 3. Produce model-update message to trigger hot-reload
            update_msg = {
                "action": "reload",
                "version": version,
                "timestamp": int(datetime.utcnow().timestamp() * 1000)
            }
            
            self.producer.produce(
                topic=self.update_topic,
                key="latest",
                value=json.dumps(update_msg)
            )
            self.producer.flush()
            print("Successfully published model-update message.")
            
        except Exception as e:
            print(f"Retraining job failed: {e}")

    def start(self) -> None:
        print("RetrainingAgent scheduler started. Next run at 03:00 UTC.")
        self.scheduler.start()
        
    def stop(self) -> None:
        self.scheduler.shutdown()

async def main():
    agent = RetrainingAgent()
    agent.start()
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        agent.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
