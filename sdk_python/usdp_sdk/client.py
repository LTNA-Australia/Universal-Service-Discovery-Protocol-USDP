"""High-level Python SDK surface."""

from __future__ import annotations

from collections.abc import Iterator

from .config import SDKConfig
from .errors import SDKHTTPError, SDKRequestError
from .http import request_json
from .models import QueryCriterion, QueryRequest, QueryResultPage, ServiceRecord
from .validators import validate_query_payload, validate_service_payload


class USDPSDK:
    def __init__(self, config: SDKConfig) -> None:
        self.config = config

    def register_service(self, service: dict | ServiceRecord, *, idempotency_key: str | None = None) -> dict:
        normalized = validate_service_payload(
            service,
            allow_timestamps=False,
            protocol_version=self.config.protocol_version,
        )
        payload = {
            "protocol_version": self.config.protocol_version,
            "service": normalized,
        }
        return request_json(
            self.config,
            "POST",
            f"{self._route_prefix()}/services",
            payload,
            auth_required=True,
            extra_headers=self._request_headers(idempotency_key),
        )

    def register_or_update_service(self, service: dict | ServiceRecord, *, idempotency_key: str | None = None) -> dict:
        normalized = validate_service_payload(
            service,
            allow_timestamps=False,
            protocol_version=self.config.protocol_version,
        )
        service_id = normalized["service_id"]
        try:
            return self.register_service(normalized, idempotency_key=idempotency_key or f"register:{service_id}")
        except SDKHTTPError as exc:
            if exc.status != 409:
                raise
            changes = dict(normalized)
            changes.pop("service_id", None)
            changes.pop("service_type", None)
            return self.update_service(
                service_id,
                changes,
                idempotency_key=idempotency_key or f"update:{service_id}",
            )

    def update_service(self, service_id: str, changes: dict, *, idempotency_key: str | None = None) -> dict:
        if not changes:
            raise SDKRequestError("changes must not be empty")
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
            auth_required=True,
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
            auth_required=True,
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
            auth_required=True,
            extra_headers=self._request_headers(idempotency_key),
        )

    def query_services(
        self,
        *,
        filters: dict | None = None,
        criteria: QueryCriterion | dict | None = None,
        query: QueryRequest | dict | None = None,
        page: int = 1,
        page_size: int = 25,
        sort: list[dict] | None = None,
        include_inactive: bool = False,
    ) -> dict:
        normalized = validate_query_payload(
            self._build_query_payload(
                filters=filters,
                criteria=criteria,
                query=query,
                page=page,
                page_size=page_size,
                sort=sort,
                include_inactive=include_inactive,
            ),
            self.config.protocol_version,
        )
        return request_json(self.config, "POST", f"{self._route_prefix()}/query", normalized, auth_required=False)

    def query_service_records(self, **kwargs) -> QueryResultPage:
        return QueryResultPage.from_response(self.query_services(**kwargs))

    def iter_services(self, **kwargs) -> Iterator[ServiceRecord]:
        page = 1
        while True:
            result = self.query_service_records(page=page, **kwargs)
            for item in result.items:
                yield item
            if page * result.page_size >= result.total:
                return
            page += 1

    def get_service(self, service_id: str) -> dict:
        return request_json(self.config, "GET", f"{self._route_prefix()}/services/{service_id}", auth_required=False)

    def get_health(self) -> dict:
        return request_json(self.config, "GET", f"{self._route_prefix()}/health", auth_required=False)

    def get_metrics(self) -> dict:
        return self._admin_request("GET", f"{self._route_prefix()}/metrics")

    def get_audit_events(self, *, limit: int = 50) -> dict:
        return self._admin_request("GET", f"{self._route_prefix()}/admin/audit?limit={limit}")

    def get_retention(self) -> dict:
        return self._admin_request("GET", f"{self._route_prefix()}/admin/retention")

    def purge_due_records(self) -> dict:
        payload = {"protocol_version": self.config.protocol_version}
        return self._admin_request("POST", f"{self._route_prefix()}/admin/purge", payload=payload)

    def _admin_request(self, method: str, path: str, payload: dict | None = None) -> dict:
        if not self.config.admin_token:
            raise SDKRequestError("admin_token is required for admin operations")
        return request_json(
            self.config,
            method,
            path,
            payload=payload,
            auth_required=True,
            auth_token=self.config.admin_token,
        )

    def _build_query_payload(
        self,
        *,
        filters: dict | None,
        criteria: QueryCriterion | dict | None,
        query: QueryRequest | dict | None,
        page: int,
        page_size: int,
        sort: list[dict] | None,
        include_inactive: bool,
    ) -> dict:
        if query is not None:
            if isinstance(query, QueryRequest):
                payload = query.to_dict()
            else:
                payload = dict(query)
            payload.setdefault("protocol_version", self.config.protocol_version)
            return payload

        payload = {"protocol_version": self.config.protocol_version}
        if filters:
            payload["filters"] = dict(filters)
        if criteria is not None:
            payload["criteria"] = criteria.to_dict() if isinstance(criteria, QueryCriterion) else dict(criteria)
        if page != 1:
            payload["page"] = page
        if page_size != 25:
            payload["page_size"] = page_size
        if sort:
            payload["sort"] = list(sort)
        if include_inactive:
            payload["include_inactive"] = include_inactive
        return payload

    def _route_prefix(self) -> str:
        major = self.config.protocol_version.split(".", 1)[0]
        return f"/v{major}"

    def _request_headers(self, idempotency_key: str | None) -> dict[str, str] | None:
        if not idempotency_key:
            return None
        return {"X-USDP-Idempotency-Key": idempotency_key}
