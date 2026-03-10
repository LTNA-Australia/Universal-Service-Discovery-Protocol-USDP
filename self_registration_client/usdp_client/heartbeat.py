"""Background heartbeat worker."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import threading
from typing import Callable

from .client import USDPRegistrationClient


@dataclass(slots=True)
class HeartbeatStatus:
    service_id: str
    interval_seconds: float
    running: bool
    last_attempt_at: datetime | None
    last_success_at: datetime | None
    consecutive_failures: int
    last_error_message: str | None

    def to_dict(self) -> dict:
        return {
            "service_id": self.service_id,
            "interval_seconds": self.interval_seconds,
            "running": self.running,
            "last_attempt_at": self._serialize_datetime(self.last_attempt_at),
            "last_success_at": self._serialize_datetime(self.last_success_at),
            "consecutive_failures": self.consecutive_failures,
            "last_error_message": self.last_error_message,
        }

    @staticmethod
    def _serialize_datetime(value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class HeartbeatWorker:
    def __init__(
        self,
        client: USDPRegistrationClient,
        service_id: str,
        interval_seconds: float,
        status: str | None = None,
        *,
        error_handler: Callable[[Exception], None] | None = None,
    ) -> None:
        self._client = client
        self._service_id = service_id
        self._interval_seconds = interval_seconds
        self._status = status
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._error_handler = error_handler
        self.last_error: Exception | None = None
        self.last_attempt_at: datetime | None = None
        self.last_success_at: datetime | None = None
        self.consecutive_failures = 0

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self._interval_seconds + 1.0))

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval_seconds):
            try:
                self.last_attempt_at = datetime.now(timezone.utc)
                self._client.heartbeat(self._service_id, status=self._status)
                self.last_success_at = datetime.now(timezone.utc)
                self.last_error = None
                self.consecutive_failures = 0
            except Exception as exc:  # noqa: BLE001
                self.last_error = exc
                self.consecutive_failures += 1
                if self._error_handler is not None:
                    self._error_handler(exc)

    def snapshot(self) -> HeartbeatStatus:
        return HeartbeatStatus(
            service_id=self._service_id,
            interval_seconds=self._interval_seconds,
            running=self._thread is not None and self._thread.is_alive(),
            last_attempt_at=self.last_attempt_at,
            last_success_at=self.last_success_at,
            consecutive_failures=self.consecutive_failures,
            last_error_message=str(self.last_error) if self.last_error is not None else None,
        )
