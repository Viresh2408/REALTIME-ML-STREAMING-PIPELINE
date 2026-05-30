"""
Application Configuration
Pydantic v2 Settings — loaded from environment / .env file
"""
from __future__ import annotations

from pydantic import AnyHttpUrl, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # ── Application ────────────────────────────────────────────
    APP_ENV: str = Field(default="development")
    LOG_LEVEL: str = Field(default="INFO")
    API_V1_PREFIX: str = Field(default="/api/v1")
    WEBSOCKET_PATH: str = Field(default="/ws/events")
    CORS_ORIGINS: list[str] = Field(default=["http://localhost:3000"])
    ANOMALY_SCORE_THRESHOLD: float = Field(default=0.7, ge=0.0, le=1.0)

    # ── Database (TimescaleDB via asyncpg) ─────────────────────
    DATABASE_URL: str = Field(...)
    TIMESCALE_HOST: str = Field(default="timescaledb")
    TIMESCALE_PORT: int = Field(default=5432)
    TIMESCALE_DB: str = Field(default="anomaly_db")
    TIMESCALE_USER: str = Field(default="anomaly_admin")
    TIMESCALE_PASSWORD: str = Field(...)

    # ── JWT Authentication ──────────────────────────────────────
    JWT_SECRET_KEY: str = Field(...)
    JWT_ALGORITHM: str = Field(default="HS256")
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=30)

    # ── Kafka ──────────────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="kafka:29092")
    KAFKA_SCHEMA_REGISTRY_URL: str = Field(default="http://schema-registry:8081")
    KAFKA_RAW_EVENTS_TOPIC: str = Field(default="raw-events")
    KAFKA_SCORED_EVENTS_TOPIC: str = Field(default="scored-events")
    KAFKA_ALERTS_TOPIC: str = Field(default="alerts")
    KAFKA_MODEL_UPDATES_TOPIC: str = Field(default="model-updates")
    KAFKA_CONSUMER_GROUP_ID: str = Field(default="ml-inference-group")
    KAFKA_AUTO_OFFSET_RESET: str = Field(default="earliest")

    # ── Redis ──────────────────────────────────────────────────
    REDIS_URL: str = Field(default="redis://redis:6379/0")
    REDIS_TTL_SECONDS: int = Field(default=300)
    REDIS_MAX_CONNECTIONS: int = Field(default=50)

    # ── MLflow ─────────────────────────────────────────────────
    MLFLOW_TRACKING_URI: str = Field(default="http://mlflow:5000")
    MODEL_ARTIFACT_PATH: str = Field(default="/app/artifacts")

    # ── LLM / Anthropic (Claude claude-sonnet-4-20250514) ──────────────────────
    ANTHROPIC_API_KEY: str = Field(...)
    ANTHROPIC_MODEL: str = Field(default="claude-sonnet-4-20250514")
    ANTHROPIC_MAX_TOKENS: int = Field(default=4096)

    # ── APScheduler ────────────────────────────────────────────
    RETRAINING_CRON_HOUR: int = Field(default=2)
    RETRAINING_CRON_MINUTE: int = Field(default=0)

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v


settings = Settings()  # type: ignore[call-arg]
