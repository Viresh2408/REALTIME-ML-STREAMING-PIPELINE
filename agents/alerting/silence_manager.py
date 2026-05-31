"""
agents/alerting/silence_manager.py
====================================
Manages alert silencing rules backed by Redis so that all pipeline workers
share a consistent silencing state without a database round-trip on every
alert.

Data model
----------
A silence rule is stored in Redis as a key::

    silence:<source_id>  →  { "reason": "...", "expires_at": <epoch_ms> }

The key TTL is set to ``duration_minutes * 60`` seconds so that Redis
naturally expires the rule at the correct time — no background job needed.

A global wildcard silence (silence all sources) is stored under the key
``silence:*``.  :py:meth:`is_silenced` checks the wildcard first.
"""

from __future__ import annotations

import json
import time

import redis


class SilenceManager:
    """
    Add and query alert silencing rules stored in Redis.

    Parameters
    ----------
    redis_client:
        Connected ``redis.Redis`` instance (sync driver).
    key_prefix:
        Namespace prefix for Redis keys.  Default ``"silence"``.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        key_prefix: str = "silence",
    ) -> None:
        self._redis = redis_client
        self._prefix = key_prefix

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _key(self, source_id: str) -> str:
        return f"{self._prefix}:{source_id}"

    # ── Public API ────────────────────────────────────────────────────────────

    def add_silence(
        self,
        source_id: str,
        duration_minutes: int,
        reason: str,
    ) -> None:
        """
        Create or refresh a silence rule for *source_id*.

        Parameters
        ----------
        source_id:
            Source identifier to silence.  Pass ``"*"`` to silence all sources
            (global wildcard).
        duration_minutes:
            How long the silence should last.  Must be >= 1.
        reason:
            Human-readable justification (stored for audit trail).

        Raises
        ------
        ValueError
            If *duration_minutes* < 1.
        """
        if duration_minutes < 1:
            msg = "duration_minutes must be at least 1"
            raise ValueError(msg)

        expires_at_ms = (time.time() + duration_minutes * 60) * 1000
        payload = json.dumps(
            {
                "reason": reason,
                "expires_at": expires_at_ms,
                "source_id": source_id,
            }
        )
        ttl_seconds = duration_minutes * 60
        self._redis.setex(self._key(source_id), ttl_seconds, payload)

    def is_silenced(self, source_id: str) -> bool:
        """
        Return ``True`` if *source_id* is currently silenced.

        Checks both a source-specific silence key and the global wildcard
        ``"*"`` key.

        Parameters
        ----------
        source_id:
            Source identifier to check.

        Returns
        -------
        bool
            ``True`` if any matching silence rule is active.
        """
        # Check global wildcard first
        if self._redis.exists(self._key("*")):
            return True
        # Check source-specific rule
        return bool(self._redis.exists(self._key(source_id)))

    def remove_silence(self, source_id: str) -> bool:
        """
        Remove an existing silence rule for *source_id* immediately.

        Parameters
        ----------
        source_id:
            Source identifier whose silence rule should be deleted.

        Returns
        -------
        bool
            ``True`` if a rule existed and was deleted, ``False`` otherwise.
        """
        result = self._redis.delete(self._key(source_id))
        return result > 0

    def get_silence_info(self, source_id: str) -> dict | None:
        """
        Return the raw silence metadata for *source_id* if active.

        Returns
        -------
        dict | None
            Parsed payload dict with keys ``reason``, ``expires_at``,
            ``source_id``, or ``None`` if no active silence rule exists.
        """
        raw = self._redis.get(self._key(source_id))
        if raw is None:
            return None
        return json.loads(raw)

    def list_silenced_sources(self) -> list[str]:
        """
        Return all currently silenced source IDs (scans Redis keys).

        Note: uses ``SCAN`` to avoid blocking the server; suitable for
        low-cardinality silence sets typical of this workload.
        """
        pattern = f"{self._prefix}:*"
        silenced: list[str] = []
        cursor = 0
        while True:
            cursor, keys = self._redis.scan(cursor=cursor, match=pattern, count=100)
            for key in keys:
                if isinstance(key, bytes):
                    key = key.decode()
                source_id = key.removeprefix(f"{self._prefix}:")
                silenced.append(source_id)
            if cursor == 0:
                break
        return silenced
