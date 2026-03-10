"""Registry publisher client used by the local agent."""

from __future__ import annotations

from dataclasses import dataclass
import json
import random
import time
from urllib import error, request


@dataclass(slots=True)
class RegistryClientConfig:
    registry_url: str
    publisher_token: str
    protocol_version: str = "2.0"
    timeout_seconds: float = 5.0
    retry_attempts: int = 3
    retry_delay_seconds: float = 0.5
    retry_backoff_factor: float = 2.0
    retry_jitter_seconds: float = 0.1
    max_retry_delay_seconds: float = 5.0


class RegistryRequestError(ValueError):
    pass


class RegistryClientHTTPError(Exception):
    def __init__(self, status: int, payload: dict) -> None:
        super().__init__(f"Registry request failed with HTTP {status}")
        self.status = status
        self.payload = payload


class RegistryPublisherClient:
    def __init__(self, config: RegistryClientConfig) -> None:
        self.config = config

    def register_service(self, service_record: dict, *, idempotency_key: str | None = None) -> dict:
        payload = {
            "protocol_version": self.config.protocol_version,
            "service": service_record,
        }
        return _request_json(
            self.config,
            "POST",
            f"{self._route_prefix()}/services",
            payload,
            extra_headers=self._request_headers(idempotency_key),
        )

    def update_service(self, service_id: str, changes: dict, *, idempotency_key: str | None = None) -> dict:
        if not changes:
            raise RegistryRequestError("changes must not be empty")
        payload = {
            "protocol_version": self.config.protocol_version,
            "service_id": service_id,
            "changes": changes,
        }
        return _request_json(
            self.config,
            "PATCH",
            f"{self._route_prefix()}/services/{service_id}",
            payload,
            extra_headers=self._request_headers(idempotency_key),
        )

    def heartbeat(self, service_id: str, status: str | None = None, *, idempotency_key: str | None = None) -> dict:
        payload = {
            "protocol_version": self.config.protocol_version,
            "service_id": service_id,
        }
        if status is not None:
            payload["status"] = status
        return _request_json(
            self.config,
            "POST",
            f"{self._route_prefix()}/services/{service_id}/heartbeat",
            payload,
            extra_headers=self._request_headers(idempotency_key),
        )

    def deregister_service(
        self,
        service_id: str,
        reason: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> dict:
        payload = {
            "protocol_version": self.config.protocol_version,
            "service_id": service_id,
        }
        if reason is not None:
            payload["reason"] = reason
        return _request_json(
            self.config,
            "POST",
            f"{self._route_prefix()}/services/{service_id}/deregister",
            payload,
            extra_headers=self._request_headers(idempotency_key),
        )

    def _route_prefix(self) -> str:
        major = self.config.protocol_version.split(".", 1)[0]
        return f"/v{major}"

    def _request_headers(self, idempotency_key: str | None) -> dict[str, str] | None:
        if not idempotency_key:
            return None
        return {"X-USDP-Idempotency-Key": idempotency_key}


def _request_json(
    config: RegistryClientConfig,
    method: str,
    path: str,
    payload: dict | None,
    *,
    extra_headers: dict[str, str] | None = None,
) -> dict:
    body = None
    headers = {
        "Authorization": f"Bearer {config.publisher_token}",
        "Accept": "application/json",
    }
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if extra_headers:
        headers.update(extra_headers)

    url = config.registry_url.rstrip("/") + path
    last_error: Exception | None = None

    for attempt in range(config.retry_attempts):
        req = request.Request(url, data=body, headers=headers, method=method)
        try:
            with request.urlopen(req, timeout=config.timeout_seconds) as response:
                response_body = response.read().decode("utf-8")
                return json.loads(response_body) if response_body else {}
        except error.HTTPError as exc:
            response_body = exc.read().decode("utf-8")
            exc.close()
            parsed = json.loads(response_body) if response_body else {}
            if 500 <= exc.code < 600 and attempt + 1 < config.retry_attempts:
                _sleep_before_retry(config, attempt)
                last_error = RegistryClientHTTPError(exc.code, parsed)
                continue
            raise RegistryClientHTTPError(exc.code, parsed) from exc
        except error.URLError as exc:
            last_error = exc
            if attempt + 1 < config.retry_attempts:
                _sleep_before_retry(config, attempt)
                continue
            raise

    if last_error is not None:
        raise last_error
    raise RuntimeError("_request_json exhausted retries unexpectedly")


def _sleep_before_retry(config: RegistryClientConfig, attempt: int) -> None:
    base_delay = config.retry_delay_seconds * (config.retry_backoff_factor ** attempt)
    jitter = random.uniform(0.0, max(0.0, config.retry_jitter_seconds))
    delay = min(config.max_retry_delay_seconds, base_delay + jitter)
    time.sleep(delay)
