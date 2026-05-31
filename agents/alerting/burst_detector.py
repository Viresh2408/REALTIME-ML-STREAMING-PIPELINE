"""
agents/alerting/burst_detector.py
===================================
Detects alert burst conditions using a Redis sorted-set per source_id with
a sliding 60-second window.

Design
------
Each HIGH (or above) alert is stored as a member in a per-source sorted set
where the score is the Unix timestamp (milliseconds).  On every call we:

  1. Remove stale members older than `window_seconds` seconds.
  2. Add the current alert with ``ZADD``.
  3. Return the cardinality (i.e. count of alerts in the window).

This gives an exact sliding-window count with O(log N) amortised complexity
per operation.

Redis key format: ``burst:<source_id>``
Key TTL is set to ``window_seconds + 10`` seconds so that idle sources
don't accumulate memory indefinitely.
"""

from __future__ import annotations

import time

import redis


class BurstDetector:
    """
    Track HIGH+ alert counts per source_id using a Redis sorted-set
    sliding window.

    Parameters
    ----------
    redis_client:
        A connected ``redis.Redis`` instance (sync driver).  Callers are
        responsible for connection lifecycle.
    window_seconds:
        Length of the sliding detection window in seconds.  Default is 60
        (workflow.docx §4: "burst >= 10 HIGH/min").
    key_prefix:
        Namespace prefix for Redis keys.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        window_seconds: int = 60,
        key_prefix: str = "burst",
    ) -> None:
        self._redis = redis_client
        self._window = window_seconds
        self._prefix = key_prefix

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _key(self, source_id: str) -> str:
        return f"{self._prefix}:{source_id}"

    def _now_ms(self) -> float:
        return time.time() * 1000

    # ── Public API ────────────────────────────────────────────────────────────

    def record_alert(self, source_id: str, alert_id: str) -> int:
        """
        Record a new alert for *source_id* and return the count of alerts
        in the current sliding window.

        Parameters
        ----------
        source_id:
            Identifier of the event source (e.g. ``"sensor-42"``).
        alert_id:
            Unique alert UUID — used as the sorted-set member so that
            duplicate calls for the same alert are idempotent.

        Returns
        -------
        int
            Number of HIGH+ alerts recorded for *source_id* in the last
            ``window_seconds`` seconds (including the one just added).
        """
        key = self._key(source_id)
        now_ms = self._now_ms()
        cutoff_ms = now_ms - (self._window * 1000)

        pipe = self._redis.pipeline()
        # Remove entries outside the sliding window
        pipe.zremrangebyscore(key, "-inf", cutoff_ms)
        # Add current alert (score = timestamp for range queries)
        pipe.zadd(key, {alert_id: now_ms})
        # Count remaining members
        pipe.zcard(key)
        # Reset TTL so idle keys expire automatically
        pipe.expire(key, self._window + 10)
        results = pipe.execute()

        # results[2] is ZCARD output
        return int(results[2])

    def get_count(self, source_id: str) -> int:
        """
        Return the current alert count for *source_id* without adding a new
        record.  Expired members are pruned before counting.

        Parameters
        ----------
        source_id:
            Source identifier to inspect.

        Returns
        -------
        int
            Alert count in the sliding window (0 if no records exist).
        """
        key = self._key(source_id)
        now_ms = self._now_ms()
        cutoff_ms = now_ms - (self._window * 1000)

        pipe = self._redis.pipeline()
        pipe.zremrangebyscore(key, "-inf", cutoff_ms)
        pipe.zcard(key)
        results = pipe.execute()

        return int(results[1])

    def clear(self, source_id: str) -> None:
        """
        Remove all burst tracking data for *source_id*.

        Useful after alert resolution to reset the burst counter.
        """
        self._redis.delete(self._key(source_id))
