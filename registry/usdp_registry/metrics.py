"""Operational metrics for the registry runtime."""

from __future__ import annotations

from collections import Counter, defaultdict
from threading import Lock


class OperationalMetrics:
    def __init__(self) -> None:
        self._lock = Lock()
        self._counters = Counter()
        self._status_counts = Counter()
        self._route_counts = Counter()
        self._latency = defaultdict(lambda: {"count": 0, "total_ms": 0.0, "max_ms": 0.0})

    def record_request(self, *, method: str, route: str, status: int, duration_ms: float) -> None:
        key = f"{method} {route}"
        with self._lock:
            self._counters["requests_total"] += 1
            self._status_counts[str(status)] += 1
            self._route_counts[key] += 1
            bucket = self._latency[key]
            bucket["count"] += 1
            bucket["total_ms"] += duration_ms
            bucket["max_ms"] = max(bucket["max_ms"], duration_ms)
            if status >= 400:
                self._counters["errors_total"] += 1
            if route.endswith("/query"):
                self._counters["query_requests_total"] += 1

    def increment(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[key] += amount

    def snapshot(self) -> dict:
        with self._lock:
            latency = {}
            for key, bucket in self._latency.items():
                average = bucket["total_ms"] / bucket["count"] if bucket["count"] else 0.0
                latency[key] = {
                    "count": bucket["count"],
                    "avg_ms": round(average, 2),
                    "max_ms": round(bucket["max_ms"], 2),
                }
            return {
                "counters": dict(self._counters),
                "status_counts": dict(self._status_counts),
                "route_counts": dict(self._route_counts),
                "latency_ms": latency,
            }
