import os
import time
import asyncio
from typing import Optional, Tuple
from urllib.parse import quote as _quote

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover - optional dependency at runtime
    redis = None  # Fallback to in-memory implementation below


def _safequote(value: str) -> str:
    """URL-encode host components similarly to kombu.utils.url.safequote."""
    return _quote(value or '', safe='')


class _InMemoryAsyncKV:
    """A minimal async in-memory key-value store with TTL support.

    This mirrors the tiny subset of Redis we use: set/get/expire/delete.
    Values are stored as-is; callers are responsible for encoding/decoding.
    """

    def __init__(self) -> None:
        self._store: dict[str, Tuple[bytes | str, Optional[float]]] = {}
        self._lock = asyncio.Lock()

    async def set(self, key: str, value: bytes | str) -> None:
        async with self._lock:
            # Preserve value type; encode only when caller expects bytes
            current = self._store.get(key)
            ttl = None if current is None else current[1]
            self._store[key] = (value, ttl)

    async def get(self, key: str):
        async with self._lock:
            item = self._store.get(key)
            if item is None:
                return None
            value, expires_at = item
            if expires_at is not None and time.time() >= expires_at:
                # Expired; delete and behave like missing
                self._store.pop(key, None)
                return None
            # Emulate redis-py returning bytes for str inputs when stored as str
            if isinstance(value, str):
                return value.encode('utf-8')
            return value

    async def expire(self, key: str, seconds: int) -> None:
        async with self._lock:
            item = self._store.get(key)
            if item is None:
                return
            value, _ = item
            self._store[key] = (value, time.time() + float(seconds))

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._store.pop(key, None)


def _build_kv_client():
    """Return a ready-to-use KV client: Redis if available, else in-memory.

    We attempt a lightweight ping to Redis. If it fails (no server, bad host,
    etc.), we fall back to an in-memory client so that local dev and stateless
    deployments still function for demo/testing flows.
    """
    # Prefer in-memory by default unless REDIS_HOST is explicitly provided.
    redis_host_raw = os.environ.get('REDIS_HOST', '').strip()
    if not redis_host_raw or redis is None:
        return _InMemoryAsyncKV()

    redis_host = _safequote(redis_host_raw)
    client = redis.Redis(host=redis_host, port=6379, db=0)

    async def _try_ping() -> bool:
        try:
            await client.ping()
            return True
        except Exception:
            return False

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    ok = loop.run_until_complete(_try_ping()) if loop.is_running() is False else False
    return client if ok else _InMemoryAsyncKV()


# Global KV client used by the integration modules
_kv_client = _build_kv_client()


async def add_key_value_redis(key, value, expire=None):
    await _kv_client.set(key, value)
    if expire:
        await _kv_client.expire(key, int(expire))


async def get_value_redis(key):
    return await _kv_client.get(key)


async def delete_key_redis(key):
    await _kv_client.delete(key)
