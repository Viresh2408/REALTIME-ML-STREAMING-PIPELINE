-- ============================================================
-- 03_create_users.sql
-- Real-Time Anomaly Detection System — Role-Based Access Control
--
-- Depends on: 01_create_hypertables.sql, 02_create_aggregates.sql
-- Source: architecture.docx §8, backend_requirements.docx §3
--
-- Roles created:
--   grafana_ro   — SELECT on anomaly_events + hourly_anomaly_stats
--   worker_rw    — INSERT on anomaly_events, SELECT on all tables
--   api_rw       — full CRUD (SELECT/INSERT/UPDATE/DELETE) on all tables
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- Role: grafana_ro
-- Dashboard read-only access — architecture.docx §8
-- Permissions: SELECT on anomaly_events and hourly_anomaly_stats only
-- ─────────────────────────────────────────────────────────────
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_ro') THEN
        CREATE ROLE grafana_ro
            WITH LOGIN
                 NOSUPERUSER
                 NOCREATEDB
                 NOCREATEROLE
                 NOINHERIT
                 NOREPLICATION
                 CONNECTION LIMIT 10
                 PASSWORD 'GrafanaRo_SecurePass1!';
    END IF;
END
$$;

COMMENT ON ROLE grafana_ro IS
    'Read-only Grafana dashboard role — SELECT on anomaly_events and hourly_anomaly_stats';

-- Grant schema visibility
GRANT USAGE ON SCHEMA anomaly TO grafana_ro;

-- Grant SELECT on hypertable and continuous aggregate
GRANT SELECT ON anomaly.anomaly_events        TO grafana_ro;
GRANT SELECT ON anomaly.hourly_anomaly_stats  TO grafana_ro;

-- Future-proof: ensure new tables/views in schema inherit SELECT
ALTER DEFAULT PRIVILEGES IN SCHEMA anomaly
    GRANT SELECT ON TABLES TO grafana_ro;


-- ─────────────────────────────────────────────────────────────
-- Role: worker_rw
-- ML Inference Consumer / TimescaleDB Writer Agent
-- Permissions: INSERT on anomaly_events, SELECT on all tables
-- backend_requirements.docx §3: write-only worker user
-- ─────────────────────────────────────────────────────────────
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'worker_rw') THEN
        CREATE ROLE worker_rw
            WITH LOGIN
                 NOSUPERUSER
                 NOCREATEDB
                 NOCREATEROLE
                 NOINHERIT
                 NOREPLICATION
                 CONNECTION LIMIT 20
                 PASSWORD 'WorkerRw_SecurePass2!';
    END IF;
END
$$;

COMMENT ON ROLE worker_rw IS
    'ML inference worker role — INSERT on anomaly_events, SELECT on all tables';

-- Grant schema visibility
GRANT USAGE ON SCHEMA anomaly TO worker_rw;

-- Primary write permission: INSERT rows produced by ML inference pipeline
GRANT INSERT ON anomaly.anomaly_events TO worker_rw;

-- Read access on all tables (needed to validate model_version, read aggregates)
GRANT SELECT ON ALL TABLES IN SCHEMA anomaly TO worker_rw;

-- Future-proof: new tables inherit SELECT
ALTER DEFAULT PRIVILEGES IN SCHEMA anomaly
    GRANT SELECT ON TABLES TO worker_rw;


-- ─────────────────────────────────────────────────────────────
-- Role: api_rw
-- FastAPI backend — full CRUD access
-- Permissions: SELECT, INSERT, UPDATE, DELETE on all tables
-- ─────────────────────────────────────────────────────────────
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'api_rw') THEN
        CREATE ROLE api_rw
            WITH LOGIN
                 NOSUPERUSER
                 NOCREATEDB
                 NOCREATEROLE
                 NOINHERIT
                 NOREPLICATION
                 CONNECTION LIMIT 50
                 PASSWORD 'ApiRw_SecurePass3!';
    END IF;
END
$$;

COMMENT ON ROLE api_rw IS
    'FastAPI backend role — full CRUD (SELECT/INSERT/UPDATE/DELETE) on all tables in anomaly schema';

-- Grant schema visibility
GRANT USAGE ON SCHEMA anomaly TO api_rw;

-- Full CRUD on all existing tables in the anomaly schema
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA anomaly TO api_rw;

-- Sequence access (needed for any SERIAL/gen_random_uuid fallback columns)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA anomaly TO api_rw;

-- Future-proof: new tables and sequences inherit full access
ALTER DEFAULT PRIVILEGES IN SCHEMA anomaly
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES    TO api_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA anomaly
    GRANT USAGE, SELECT                  ON SEQUENCES TO api_rw;
