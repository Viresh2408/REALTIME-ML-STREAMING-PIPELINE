r"""
verify_timescaledb.py
-------------------------------------------------------------------------
Connects to TimescaleDB and verifies:
  1. \dt equivalent  -- lists all tables in the anomaly schema
  2. COUNT(*) on anomaly.anomaly_events -- expects 1000
  3. Hypertable check -- confirms anomaly_events is a hypertable
  4. Continuous aggregate check -- confirms hourly_anomaly_stats exists
  5. Compression policy check
  6. Retention policy check
  7. Index check -- all 3 required indexes present
  8. Role check -- grafana_ro, worker_rw, api_rw exist
-------------------------------------------------------------------------
Run: python verify_timescaledb.py
"""

import asyncio
import os
import sys

# Allow running from project root or script directory
sys.path.insert(0, os.path.dirname(__file__))

try:
    import asyncpg
except ImportError:
    print("ERROR: asyncpg not installed. Run: pip install asyncpg")
    sys.exit(1)

# Force UTF-8 output on Windows consoles to avoid cp1252 errors
sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]

# ── Connection config (matches .env) ─────────────────────────────────────────
DSN = "postgresql://anomaly_admin:StrongPass123!@localhost:5432/anomaly_db"


def banner(title: str) -> None:
    width = 72
    print("\n" + "-" * width)
    print(f"  {title}")
    print("-" * width)


async def verify(conn: asyncpg.Connection) -> dict:
    results = {}

    # ── 1. List all tables in anomaly schema ─────────────────────────────────
    banner("1. Tables in anomaly schema  (\\dt anomaly.*)")
    tables = await conn.fetch("""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'anomaly'
        ORDER BY table_name;
    """)
    if tables:
        print(f"  {'table_name':<40} {'table_type'}")
        print(f"  {'-' * 40} {'-' * 20}")
        for r in tables:
            print(f"  {r['table_name']:<40} {r['table_type']}")
    else:
        print("  [!] No tables found in anomaly schema")
    results["tables"] = [r["table_name"] for r in tables]

    # ── 2. Row count ─────────────────────────────────────────────────────────
    banner("2. SELECT COUNT(*) FROM anomaly.anomaly_events")
    count = await conn.fetchval("SELECT COUNT(*) FROM anomaly.anomaly_events;")
    status = "✓ PASS" if count == 1000 else f"✗ FAIL — expected 1000, got {count}"
    print(f"  COUNT = {count}   {status}")
    results["row_count"] = count
    results["row_count_pass"] = count == 1000

    # ── 3. Hypertable confirmation ────────────────────────────────────────────
    banner("3. Hypertable check")
    ht = await conn.fetch("""
        SELECT hypertable_schema, hypertable_name, num_chunks
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema = 'anomaly';
    """)
    if ht:
        for r in ht:
            print(f"  ✓ {r['hypertable_schema']}.{r['hypertable_name']}  chunks={r['num_chunks']}")
    else:
        print("  ✗ No hypertables found!")
    results["hypertables"] = [r["hypertable_name"] for r in ht]

    # ── 4. Continuous aggregate ───────────────────────────────────────────────
    banner("4. Continuous aggregate check")
    ca = await conn.fetch("""
        SELECT view_schema, view_name, materialization_hypertable_schema
        FROM timescaledb_information.continuous_aggregates
        WHERE view_schema = 'anomaly';
    """)
    if ca:
        for r in ca:
            print(f"  ✓ {r['view_schema']}.{r['view_name']}")
    else:
        print("  ✗ No continuous aggregates found!")
    results["continuous_aggregates"] = [r["view_name"] for r in ca]

    # ── 5. Compression policy ─────────────────────────────────────────────────
    banner("5. Compression policy check")
    # TimescaleDB 2.15: use jobs table — compression_settings has no compress_after col
    cp = await conn.fetch("""
        SELECT hypertable_schema, hypertable_name, proc_name, schedule_interval,
               config ->> 'compress_after' AS compress_after
        FROM timescaledb_information.jobs
        WHERE proc_name = 'policy_compression'
          AND hypertable_schema = 'anomaly';
    """)
    if cp:
        for r in cp:
            print(
                f"  [PASS] Compression on {r['hypertable_schema']}.{r['hypertable_name']}  compress_after={r['compress_after']}"
            )
    else:
        print("  [FAIL] No compression policy found")
    results["compression"] = bool(cp)

    # ── 6. Retention policy ───────────────────────────────────────────────────
    banner("6. Retention policy check")
    rp = await conn.fetch("""
        SELECT hypertable_schema, hypertable_name, proc_name, schedule_interval,
               config ->> 'drop_after' AS drop_after
        FROM timescaledb_information.jobs
        WHERE proc_name = 'policy_retention'
          AND hypertable_schema = 'anomaly';
    """)
    if rp:
        for r in rp:
            print(
                f"  ✓ Retention on {r['hypertable_schema']}.{r['hypertable_name']}  drop_after={r['drop_after']}"
            )
    else:
        print("  ✗ No retention policy found")
    results["retention"] = bool(rp)

    # ── 7. Indexes ────────────────────────────────────────────────────────────
    banner("7. Index check (backend_requirements.docx §3 — all 3 required)")
    required = {
        "anomaly_events_event_time_idx",
        "idx_anomaly_events_source_time",
        "idx_anomaly_events_is_anomaly_time",
    }
    indexes = await conn.fetch("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'anomaly'
          AND tablename  = 'anomaly_events'
        ORDER BY indexname;
    """)
    found_names = {r["indexname"] for r in indexes}
    for r in indexes:
        tick = "✓" if r["indexname"] in required else " "
        print(f"  {tick} {r['indexname']}")
    missing = required - found_names
    if missing:
        print(f"\n  ✗ MISSING indexes: {missing}")
    else:
        print("\n  ✓ All 3 required indexes present")
    results["indexes_ok"] = not bool(missing)

    # ── 8. Roles ──────────────────────────────────────────────────────────────
    banner("8. Role check (grafana_ro, worker_rw, api_rw)")
    required_roles = {"grafana_ro", "worker_rw", "api_rw"}
    roles = await conn.fetch(
        """
        SELECT rolname FROM pg_roles
        WHERE rolname = ANY($1);
    """,
        list(required_roles),
    )
    found_roles = {r["rolname"] for r in roles}
    for role in sorted(required_roles):
        tick = "✓" if role in found_roles else "✗"
        print(f"  {tick} {role}")
    results["roles_ok"] = found_roles >= required_roles

    # ── 9. Sample data sanity check ───────────────────────────────────────────
    banner("9. Sample data distribution (source_ids, anomaly rate, score range)")
    sample = await conn.fetch("""
        SELECT
            source_id,
            COUNT(*)                                      AS total,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END)  AS anomalies,
            ROUND(AVG(anomaly_score)::numeric, 4)         AS avg_score,
            MIN(event_time)::date                         AS oldest,
            MAX(event_time)::date                         AS newest
        FROM anomaly.anomaly_events
        GROUP BY source_id
        ORDER BY source_id;
    """)
    if sample:
        print(
            f"  {'source_id':<12} {'total':>6} {'anomalies':>10} {'avg_score':>10} {'oldest':>12} {'newest':>12}"
        )
        print(f"  {'─' * 12} {'─' * 6} {'─' * 10} {'─' * 10} {'─' * 12} {'─' * 12}")
        for r in sample:
            pct = round(r["anomalies"] / r["total"] * 100, 1)
            print(
                f"  {r['source_id']:<12} {r['total']:>6} {r['anomalies']:>7} ({pct:>4}%) {r['avg_score']:>10} {r['oldest']!s:>12} {r['newest']!s:>12}"
            )

    return results


async def main() -> None:
    print("\n" + "=" * 72)
    print("  TimescaleDB Verification -- Real-Time Anomaly Detection System")
    print("=" * 72)
    print(f"  DSN: {DSN.replace('StrongPass123!', '***')}")

    try:
        conn = await asyncpg.connect(DSN)
    except Exception as e:
        print(f"\n  ✗ CONNECTION FAILED: {e}")
        print("\n  Make sure TimescaleDB is running:")
        print("  > docker compose up -d timescaledb")
        sys.exit(1)

    try:
        results = await verify(conn)
    finally:
        await conn.close()

    # ── Summary ───────────────────────────────────────────────────────────────
    banner("SUMMARY")
    checks = [
        ("anomaly_events table exists", "anomaly_events" in results.get("tables", [])),
        (
            "hourly_anomaly_stats exists",
            "hourly_anomaly_stats" in results.get("continuous_aggregates", [])
            or "hourly_anomaly_stats" in results.get("tables", []),
        ),
        ("hypertable registered", bool(results.get("hypertables"))),
        ("row count == 1000", results.get("row_count_pass", False)),
        ("all 3 indexes present", results.get("indexes_ok", False)),
        ("retention policy set", results.get("retention", False)),
        ("all 3 roles exist", results.get("roles_ok", False)),
    ]
    all_pass = True
    for label, passed in checks:
        tick = "✓ PASS" if passed else "✗ FAIL"
        if not passed:
            all_pass = False
        print(f"  {tick}  {label}")

    print("\n" + "=" * 72)
    if all_pass:
        print("  [PASS]  ALL CHECKS PASSED -- TimescaleDB is correctly configured")
    else:
        print("  [FAIL]  SOME CHECKS FAILED -- review output above")
    print("=" * 72 + "\n")
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    asyncio.run(main())
