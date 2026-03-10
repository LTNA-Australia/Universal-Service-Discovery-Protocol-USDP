"""In-memory request throttling."""

from __future__ import annotations

from collections import defaultdict, deque
from threading import Lock
import time


class InMemoryRateLimiter:
    def __init__(self) -> None:
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def consume(self, key: str, *, limit: int, window_seconds: int = 60) -> bool:
        if limit <= 0:
            return True

        now = time.monotonic()
        with self._lock:
            bucket = self._events[key]
            cutoff = now - window_seconds
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()
            if len(bucket) >= limit:
                return False
            bucket.append(now)
            return True
