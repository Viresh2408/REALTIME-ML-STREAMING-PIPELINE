-- ============================================================
-- 02_create_aggregates.sql
-- Real-Time Anomaly Detection System — Continuous Aggregates
--
-- Depends on: 01_create_hypertables.sql (anomaly.anomaly_events)
-- Source: backend_requirements.docx §3
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- Continuous Aggregate: anomaly.hourly_anomaly_stats
--
-- Materialises per-hour, per-source_id aggregates:
--   event_count  — total events in the bucket
--   avg_score    — mean anomaly score
--   max_score    — worst-case anomaly score
--
-- Backed by TimescaleDB's incremental materialisation engine —
-- only newly ingested data is re-aggregated on each policy run,
-- keeping refresh cost sub-linear as the dataset grows.
-- ─────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS anomaly.hourly_anomaly_stats
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', event_time)   AS bucket,
    source_id,
    COUNT(*)                            AS event_count,
    AVG(anomaly_score)                  AS avg_score,
    MAX(anomaly_score)                  AS max_score
FROM anomaly.anomaly_events
GROUP BY
    time_bucket('1 hour', event_time),
    source_id
WITH NO DATA;

COMMENT ON VIEW anomaly.hourly_anomaly_stats IS
    'Continuous aggregate — hourly event count, avg/max anomaly score per source_id (backend_requirements.docx §3)';

-- ─────────────────────────────────────────────────────────────
-- Refresh policy for hourly_anomaly_stats
--
-- start_offset  3 h  — look back 3 buckets to catch late-arriving events
-- end_offset    1 h  — leave the current in-progress bucket unmaterialised
-- schedule      1 h  — refresh every hour
-- ─────────────────────────────────────────────────────────────
SELECT add_continuous_aggregate_policy(
    'anomaly.hourly_anomaly_stats',
    start_offset      => INTERVAL '3 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE
);

-- (Real-time aggregation is enabled by default in TimescaleDB 2.x)
-- ─────────────────────────────────────────────────────────────
-- Index on the continuous aggregate's bucket + source_id
-- Supports Grafana ORDER BY bucket DESC queries efficiently
-- ─────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_hourly_anomaly_stats_bucket_source
    ON anomaly.hourly_anomaly_stats (source_id, bucket DESC);

CREATE INDEX IF NOT EXISTS idx_hourly_anomaly_stats_bucket
    ON anomaly.hourly_anomaly_stats (bucket DESC);
