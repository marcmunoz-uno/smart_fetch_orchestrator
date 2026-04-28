"""SQLite-backed TTL cache for API responses.

Generic key/value cache with per-entry expiration. Used by ATTOM enricher;
reusable for any rate-limited/billable API where response data changes slowly.

Storage: `~/.smart_fetch/cache.db` (single SQLite file, lives outside the repo).
Multi-process safe via SQLite's built-in locking.

Usage:
    from smart_fetch.utils.api_cache import get_cache
    cache = get_cache()

    # Try cache first
    cached = cache.get("attom", "/property/expandedprofile",
                       params={"address1": "...", "address2": "..."})
    if cached is not None:
        return cached

    # Fetch + store
    response = requests.get(...).json()
    cache.put("attom", "/property/expandedprofile",
              params={...}, value=response, ttl_seconds=86400 * 30)
"""

import hashlib
import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional

DEFAULT_CACHE_PATH = Path(os.environ.get("SMART_FETCH_CACHE_DIR",
                                          str(Path.home() / ".smart_fetch"))) / "cache.db"

_INSTANCE = None
_INSTANCE_LOCK = threading.Lock()


class APICache:
    """SQLite TTL cache. Thread-safe within a process; SQLite locks across processes."""

    def __init__(self, db_path: Path = DEFAULT_CACHE_PATH):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_schema()

    def _conn(self) -> sqlite3.Connection:
        """Per-thread connection — sqlite3 connections aren't thread-safe."""
        c = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(str(self.db_path), timeout=10.0, isolation_level=None)
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
            c.row_factory = sqlite3.Row
            self._local.conn = c
        return c

    def _init_schema(self):
        c = self._conn()
        c.executescript("""
            CREATE TABLE IF NOT EXISTS api_cache (
                cache_key   TEXT PRIMARY KEY,
                namespace   TEXT NOT NULL,
                endpoint    TEXT NOT NULL,
                params_hash TEXT NOT NULL,
                response    TEXT NOT NULL,
                created_at  INTEGER NOT NULL,
                expires_at  INTEGER NOT NULL,
                hits        INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_namespace_endpoint
                ON api_cache(namespace, endpoint);
            CREATE INDEX IF NOT EXISTS idx_expires_at
                ON api_cache(expires_at);
        """)

    @staticmethod
    def _make_key(namespace: str, endpoint: str, params: Optional[dict]) -> tuple:
        """Stable key from (namespace, endpoint, sorted params). Returns (cache_key, params_hash)."""
        params = params or {}
        # Stable JSON: sort keys, str-coerce values
        canonical = json.dumps(
            {k: (v if isinstance(v, (int, float, bool, type(None))) else str(v))
             for k, v in sorted(params.items())},
            sort_keys=True,
            separators=(",", ":"),
        )
        params_hash = hashlib.sha256(canonical.encode()).hexdigest()[:24]
        cache_key = f"{namespace}::{endpoint}::{params_hash}"
        return cache_key, params_hash

    def get(self, namespace: str, endpoint: str, params: Optional[dict] = None) -> Optional[Any]:
        """Return cached value or None. Increments hit counter on hit."""
        cache_key, _ = self._make_key(namespace, endpoint, params)
        now = int(time.time())
        c = self._conn()
        row = c.execute(
            "SELECT response, expires_at FROM api_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if not row:
            return None
        if row["expires_at"] <= now:
            # Expired — leave for purge
            return None
        c.execute("UPDATE api_cache SET hits = hits + 1 WHERE cache_key = ?", (cache_key,))
        try:
            return json.loads(row["response"])
        except json.JSONDecodeError:
            return None

    def put(self, namespace: str, endpoint: str,
            value: Any, *, params: Optional[dict] = None,
            ttl_seconds: int = 86400 * 30) -> None:
        """Store a value with TTL (default 30 days)."""
        cache_key, params_hash = self._make_key(namespace, endpoint, params)
        now = int(time.time())
        try:
            payload = json.dumps(value, default=str)
        except (TypeError, ValueError):
            return  # Don't cache non-serializable
        c = self._conn()
        c.execute("""
            INSERT INTO api_cache (cache_key, namespace, endpoint, params_hash,
                                   response, created_at, expires_at, hits)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(cache_key) DO UPDATE SET
                response = excluded.response,
                created_at = excluded.created_at,
                expires_at = excluded.expires_at
        """, (cache_key, namespace, endpoint, params_hash,
              payload, now, now + ttl_seconds))

    def invalidate(self, namespace: str, endpoint: Optional[str] = None,
                   params: Optional[dict] = None) -> int:
        """Delete entries. Specify just namespace to clear all of an API,
        or namespace+endpoint+params for a single key. Returns count deleted."""
        c = self._conn()
        if endpoint and params is not None:
            cache_key, _ = self._make_key(namespace, endpoint, params)
            cur = c.execute("DELETE FROM api_cache WHERE cache_key = ?", (cache_key,))
        elif endpoint:
            cur = c.execute(
                "DELETE FROM api_cache WHERE namespace = ? AND endpoint = ?",
                (namespace, endpoint),
            )
        else:
            cur = c.execute("DELETE FROM api_cache WHERE namespace = ?", (namespace,))
        return cur.rowcount

    def purge_expired(self) -> int:
        """Delete expired entries. Returns count."""
        now = int(time.time())
        c = self._conn()
        cur = c.execute("DELETE FROM api_cache WHERE expires_at <= ?", (now,))
        return cur.rowcount

    def stats(self, namespace: Optional[str] = None) -> dict:
        """Return cache stats: total entries, expired count, hit counts by endpoint."""
        c = self._conn()
        now = int(time.time())
        where = "WHERE namespace = ?" if namespace else ""
        params = (namespace,) if namespace else ()

        total = c.execute(f"SELECT COUNT(*) FROM api_cache {where}", params).fetchone()[0]
        expired = c.execute(
            f"SELECT COUNT(*) FROM api_cache {where} {'AND' if namespace else 'WHERE'} expires_at <= ?",
            params + (now,),
        ).fetchone()[0]

        per_endpoint = {}
        for row in c.execute(
            f"SELECT endpoint, COUNT(*) AS n, SUM(hits) AS hits, "
            f"       SUM(CASE WHEN expires_at > ? THEN 1 ELSE 0 END) AS fresh "
            f"FROM api_cache {where} GROUP BY endpoint ORDER BY hits DESC",
            (now,) + params,
        ):
            per_endpoint[row["endpoint"]] = {
                "entries": row["n"],
                "hits": row["hits"] or 0,
                "fresh": row["fresh"] or 0,
            }

        return {
            "namespace": namespace or "*",
            "total_entries": total,
            "expired": expired,
            "fresh": total - expired,
            "per_endpoint": per_endpoint,
            "db_path": str(self.db_path),
        }


def get_cache() -> APICache:
    """Process-wide singleton."""
    global _INSTANCE
    if _INSTANCE is None:
        with _INSTANCE_LOCK:
            if _INSTANCE is None:
                _INSTANCE = APICache()
    return _INSTANCE
