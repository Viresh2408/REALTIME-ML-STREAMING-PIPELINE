-- ============================================================
-- 01_create_hypertables.sql
-- Real-Time Anomaly Detection System — TimescaleDB Setup
--
-- Source specifications:
--   architecture.docx         Section 6 — Database Schema
--   backend_requirements.docx Section 3 — Database Requirements
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- Prerequisites
-- ─────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Schema namespace — keeps anomaly tables isolated
CREATE SCHEMA IF NOT EXISTS anomaly;

-- ─────────────────────────────────────────────────────────────
-- Table: anomaly.anomaly_events
-- Hypertable — partitioned by event_time
--
-- Columns (architecture.docx Section 6):
--   event_id        UUID PK          Unique event identifier
--   event_time      TIMESTAMPTZ      Partition key — event occurrence time
--   source_id       TEXT             Origin system or user ID
--   feature_vector  JSONB            Raw numeric features passed to ML model
--   anomaly_score   FLOAT8           Model output (0–1, higher = more anomalous)
--   is_anomaly      BOOLEAN          Threshold-applied label
--   model_version   TEXT             Model artifact version used for inference
--   processed_at    TIMESTAMPTZ      When the consumer processed this event
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS anomaly.anomaly_events (
    event_id        UUID            NOT NULL DEFAULT gen_random_uuid(),
    event_time      TIMESTAMPTZ     NOT NULL,
    source_id       TEXT            NOT NULL,
    feature_vector  JSONB           NOT NULL,
    anomaly_score   FLOAT8          NOT NULL
                        CHECK (anomaly_score >= 0.0 AND anomaly_score <= 1.0),
    is_anomaly      BOOLEAN         NOT NULL DEFAULT false,
    model_version   TEXT            NOT NULL,
    processed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Composite PK required by TimescaleDB hypertable (partition col must be in PK)
    PRIMARY KEY (event_id, event_time)
);

COMMENT ON TABLE  anomaly.anomaly_events                    IS 'TimescaleDB hypertable — one row per ML-scored event (architecture.docx §6)';
COMMENT ON COLUMN anomaly.anomaly_events.event_id           IS 'UUID PK — unique event identifier';
COMMENT ON COLUMN anomaly.anomaly_events.event_time         IS 'TIMESTAMPTZ — partition key, event occurrence time';
COMMENT ON COLUMN anomaly.anomaly_events.source_id          IS 'TEXT — origin system or user ID';
COMMENT ON COLUMN anomaly.anomaly_events.feature_vector     IS 'JSONB — raw numeric features passed to ML model';
COMMENT ON COLUMN anomaly.anomaly_events.anomaly_score      IS 'FLOAT8 — model output (0–1, higher = more anomalous)';
COMMENT ON COLUMN anomaly.anomaly_events.is_anomaly         IS 'BOOLEAN — threshold-applied label';
COMMENT ON COLUMN anomaly.anomaly_events.model_version      IS 'TEXT — model artifact version used for inference';
COMMENT ON COLUMN anomaly.anomaly_events.processed_at       IS 'TIMESTAMPTZ — when the consumer processed this event';

-- ─────────────────────────────────────────────────────────────
-- Convert to TimescaleDB hypertable
-- backend_requirements.docx §3: partitioned by event_time, 1-day chunk interval
-- ─────────────────────────────────────────────────────────────
SELECT create_hypertable(
    'anomaly.anomaly_events',
    'event_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- ─────────────────────────────────────────────────────────────
-- Compression policy
-- backend_requirements.docx §3: compress chunks older than 7 days
--   columnar storage → ~10× compression ratio
--   segment by source_id for efficient per-source range scans
-- Note: compress_after syntax varies by TimescaleDB version:
--   v2.6+  → add_compression_policy('table', INTERVAL '7 days')
--   older  → second positional arg is the interval directly
-- ─────────────────────────────────────────────────────────────
ALTER TABLE anomaly.anomaly_events
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'source_id, is_anomaly',
        timescaledb.compress_orderby   = 'event_time DESC'
    );

SELECT add_compression_policy(
    'anomaly.anomaly_events',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- ─────────────────────────────────────────────────────────────
-- Retention policy
-- backend_requirements.docx §3: drop chunks older than 90 days
-- ─────────────────────────────────────────────────────────────
SELECT add_retention_policy(
    'anomaly.anomaly_events',
    INTERVAL '90 days',
    if_not_exists => TRUE
);

-- ─────────────────────────────────────────────────────────────
-- Indexes
-- backend_requirements.docx §3 — all three required indexes:
--   1. (event_time DESC)
--   2. (source_id, event_time DESC)
--   3. (is_anomaly, event_time DESC)
-- ─────────────────────────────────────────────────────────────

-- Index 1: time-only descending — powers time-range dashboard queries
CREATE INDEX IF NOT EXISTS idx_anomaly_events_event_time
    ON anomaly.anomaly_events (event_time DESC);

-- Index 2: composite source + time — powers per-source Grafana panels
CREATE INDEX IF NOT EXISTS idx_anomaly_events_source_time
    ON anomaly.anomaly_events (source_id, event_time DESC);

-- Index 3: partial composite — anomaly-only alert feeds (30% of rows)
CREATE INDEX IF NOT EXISTS idx_anomaly_events_is_anomaly_time
    ON anomaly.anomaly_events (is_anomaly, event_time DESC)
    WHERE is_anomaly = true;
