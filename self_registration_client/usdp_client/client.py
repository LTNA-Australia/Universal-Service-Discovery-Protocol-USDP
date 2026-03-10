"""High-level registration client."""

from __future__ import annotations

from .builders import build_service_update_changes
from .config import ClientConfig
from .errors import ClientHTTPError, ClientRequestError
from .http import request_json


class USDPRegistrationClient:
    def __init__(self, config: ClientConfig) -> None:
        self.config = config

    def register_service(self, service_record: dict, *, idempotency_key: str | None = None) -> dict:
        payload = {
            "protocol_version": self.config.protocol_version,
            "service": service_record,
        }
        return request_json(
            self.config,
            "POST",
            f"{self._route_prefix()}/services",
            payload,
            extra_headers=self._request_headers(idempotency_key),
        )

    def register_or_update_service(self, service_record: dict, *, idempotency_key: str | None = None) -> dict:
        service_id = service_record.get("service_id")
        if not isinstance(service_id, str) or not service_id:
            raise ClientRequestError("service_record must include service_id")
        try:
            return self.register_service(service_record, idempotency_key=idempotency_key or f"register:{service_id}")
        except ClientHTTPError as exc:
            if exc.status != 409:
                raise
            return self.update_service(
                service_id,
                build_service_update_changes(service_record),
                idempotency_key=idempotency_key or f"update:{service_id}",
            )

    def update_service(self, service_id: str, changes: dict, *, idempotency_key: str | None = None) -> dict:
        if not changes:
            raise ClientRequestError("changes must not be empty")
        payload = {
            "protocol_version": self.config.protocol_version,
            "service_id": service_id,
            "changes": changes,
        }
        return request_json(
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
        return request_json(
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
        return request_json(
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
