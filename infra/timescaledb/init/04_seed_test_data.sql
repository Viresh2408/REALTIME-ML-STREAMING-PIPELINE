-- ============================================================
-- 04_seed_test_data.sql
-- Real-Time Anomaly Detection System — Test Data Seed
--
-- Depends on: 01_create_hypertables.sql
-- Inserts 1,000 realistic sample rows into anomaly.anomaly_events:
--   • Timestamps spread across the last 7 days
--   • anomaly_score in [0.0, 1.0] — random distribution
--   • ~30% rows flagged is_anomaly = true (score > 0.70 threshold)
--   • 5 distinct source_ids (src-001 … src-005)
--   • 3 model versions in rotation
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- Seed helper: generate_series drives the row count (1–1000).
-- All randomness uses PostgreSQL built-in functions so the seed
-- script works without any extensions beyond TimescaleDB.
-- ─────────────────────────────────────────────────────────────
INSERT INTO anomaly.anomaly_events (
    event_id,
    event_time,
    source_id,
    feature_vector,
    anomaly_score,
    is_anomaly,
    model_version,
    processed_at
)
SELECT
    -- Unique event UUID
    gen_random_uuid()                                                           AS event_id,

    -- event_time: random instant in the last 7 days
    -- NOW() - (random() * INTERVAL '7 days') distributes uniformly
    NOW() - (random() * INTERVAL '7 days')                                      AS event_time,

    -- 5 distinct source IDs, round-robin assigned to each row
    'src-' || LPAD(((s % 5) + 1)::TEXT, 3, '0')                                AS source_id,

    -- feature_vector: realistic 5-feature JSON object
    -- Features represent normalised sensor readings [0, 1]
    jsonb_build_object(
        'cpu_util',    ROUND((random() * 100)::numeric, 4),
        'mem_util',    ROUND((random() * 100)::numeric, 4),
        'net_rx_mbps', ROUND((random() * 1000)::numeric, 4),
        'net_tx_mbps', ROUND((random() * 1000)::numeric, 4),
        'latency_ms',  ROUND((random() * 500)::numeric, 4)
    )                                                                           AS feature_vector,

    -- anomaly_score: bimodal distribution
    --   70% of rows → low score  [0.00, 0.65]   (normal events)
    --   30% of rows → high score [0.70, 1.00]   (anomalous events)
    CASE
        WHEN (s % 10) < 7
            -- Normal range: 0.00–0.65
            THEN ROUND((random() * 0.65)::numeric, 6)::float8
        ELSE
            -- Anomalous range: 0.70–1.00
            ROUND((0.70 + random() * 0.30)::numeric, 6)::float8
    END                                                                         AS anomaly_score,

    -- is_anomaly: true when score is in the anomalous range (30% of rows)
    CASE WHEN (s % 10) >= 7 THEN true ELSE false END                            AS is_anomaly,

    -- model_version: 3 versions in rotation (v1.0.0, v1.1.0, v2.0.0)
    CASE (s % 3)
        WHEN 0 THEN 'v1.0.0'
        WHEN 1 THEN 'v1.1.0'
        ELSE        'v2.0.0'
    END                                                                         AS model_version,

    -- processed_at: 50–500 ms after event_time (realistic pipeline latency)
    NOW() - (random() * INTERVAL '7 days')
        + (0.05 + random() * 0.45) * INTERVAL '1 second'                       AS processed_at

FROM generate_series(0, 999) AS s;

-- ─────────────────────────────────────────────────────────────
-- Verification queries — run after insert to confirm seeding
-- ─────────────────────────────────────────────────────────────
DO $$
DECLARE
    v_total     bigint;
    v_anomalies bigint;
    v_sources   bigint;
BEGIN
    SELECT COUNT(*)                        INTO v_total     FROM anomaly.anomaly_events;
    SELECT COUNT(*) FILTER (WHERE is_anomaly) INTO v_anomalies FROM anomaly.anomaly_events;
    SELECT COUNT(DISTINCT source_id)       INTO v_sources   FROM anomaly.anomaly_events;

    RAISE NOTICE '── Seed verification ──────────────────────────────────';
    RAISE NOTICE 'Total rows inserted : %',  v_total;
    RAISE NOTICE 'Anomaly rows (≈30%%) : % (%.1f%%)',
        v_anomalies,
        (v_anomalies::numeric / NULLIF(v_total, 0) * 100);
    RAISE NOTICE 'Distinct source_ids : %',  v_sources;
    RAISE NOTICE '───────────────────────────────────────────────────────';
END
$$;
