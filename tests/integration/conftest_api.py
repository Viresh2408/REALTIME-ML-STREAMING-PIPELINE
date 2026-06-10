"""
tests/integration/conftest_api.py
──────────────────────────────────────────────────────────────────────────────
Shared helpers and fixtures used by the new API integration test modules:
  - test_alerts_api.py
  - test_anomalies_api.py
  - test_auth_api.py
  - test_events_api.py

All tests run WITHOUT a live database, Kafka broker, or Redis instance:
  • SQLAlchemy async sessions are replaced with MagicMock / AsyncMock
  • Kafka producer is replaced with a MagicMock
  • Redis is replaced with fakeredis.aioredis.FakeRedis
"""
