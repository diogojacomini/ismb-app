"""
TTL cache - no external dependencies required.
"""
from __future__ import annotations
import time
from functools import wraps
from typing import Any, Callable, Dict, Optional, Tuple

DEFAULT_TTL: int = 300

# Internal store: key -> (value, expiry_monotonic)
_store: Dict[str, Tuple[Any, float]] = {}


def get(key: str) -> Optional[Any]:
    """Return cached value or *None* if absent / expired."""
    entry = _store.get(key)
    if entry is None:
        return None
    value, expiry = entry
    if time.monotonic() > expiry:
        del _store[key]
        return None
    return value


def put(key: str, value: Any, ttl: int = DEFAULT_TTL) -> None:
    """Store *value* under *key* for *ttl* seconds."""
    _store[key] = (value, time.monotonic() + ttl)


def invalidate(key: str) -> None:
    """Remove a single cache entry (no-op if missing)."""
    _store.pop(key, None)


def clear() -> None:
    """Flush the entire cache (useful on startup / testing)."""
    _store.clear()


def stats() -> dict:
    """Return a brief stats snapshot (live_keys, expired_keys)."""
    now = time.monotonic()
    live = sum(1 for _, (_, exp) in _store.items() if exp > now)
    return {
        "total_keys": len(_store),
        "live_keys": live,
        "expired_keys": len(_store) - live,
    }


def cached(ttl: int = DEFAULT_TTL) -> Callable:
    """
    Function-level caching decorator.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = (
                f"{func.__module__}.{func.__qualname__}|{args}|{sorted(kwargs.items())}"
            )
            result = get(key)
            if result is None:
                result = func(*args, **kwargs)
                put(key, result, ttl)
            return result

        return wrapper

    return decorator
