import sys
import os
import asyncio
import asyncpg
from confluent_kafka.admin import AdminClient

# Ensure project root is in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.env_loader import load_env
load_env()

class HealthAgent:
    def __init__(self) -> None:
        self.running = False
        self.db_url = os.getenv("DATABASE_URL", "postgresql://worker_rw:WorkerRw_SecurePass2!@localhost:5432/anomaly_db")
        if self.db_url.startswith("postgresql+asyncpg://"):
            self.db_url = self.db_url.replace("postgresql+asyncpg://", "postgresql://")
            
        self.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.check_interval = 30  # seconds

    async def check_kafka(self) -> bool:
        """Ping Kafka Admin Client to ensure cluster is reachable."""
        try:
            admin = AdminClient({'bootstrap.servers': self.kafka_servers})
            cluster_metadata = admin.list_topics(timeout=5)
            return len(cluster_metadata.topics) > 0
        except Exception as e:
            print(f"Kafka health check failed: {e}")
            return False

    async def check_postgres(self) -> bool:
        """Ping TimescaleDB to ensure query execution is working."""
        try:
            conn = await asyncpg.connect(self.db_url, timeout=5)
            await conn.execute("SELECT 1")
            await conn.close()
            return True
        except Exception as e:
            print(f"TimescaleDB health check failed: {e}")
            return False

    async def alert_orchestrator(self, service: str) -> None:
        """Alerts LangGraph Orchestrator or logging mechanism about failures."""
        print(f"[CRITICAL] Service '{service}' is down! Triggering Orchestrator recovery mechanisms.")
        
    async def run(self) -> None:
        self.running = True
        print("HealthAgent started monitoring every 30s...")
        
        while self.running:
            kafka_ok = await self.check_kafka()
            if not kafka_ok:
                await self.alert_orchestrator("Kafka")
                
            pg_ok = await self.check_postgres()
            if not pg_ok:
                await self.alert_orchestrator("TimescaleDB")
                
            if kafka_ok and pg_ok:
                print("Health check OK. All core services are running.")
                
            await asyncio.sleep(self.check_interval)

    def stop(self) -> None:
        self.running = False

if __name__ == "__main__":
    agent = HealthAgent()
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        agent.stop()
