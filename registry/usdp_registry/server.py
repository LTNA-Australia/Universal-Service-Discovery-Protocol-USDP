"""HTTP server for the USDP registry."""

from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import re
import threading
import time
from copy import deepcopy
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from .auth import AuthorizedPublisher, PublisherAuthorizer
from .config import RegistryConfig
from .errors import (
    ConflictError,
    ForbiddenError,
    InvalidRequestError,
    NotFoundError,
    PayloadTooLargeError,
    RateLimitedError,
    RegistryError,
    UnauthorizedError,
    ValidationError as RegistryValidationError,
)
from .federation import apply_federated_withdrawal, prepare_federated_record
from .lifecycle import apply_deregister, apply_heartbeat, apply_update, create_registered_record
from .metrics import OperationalMetrics
from .rate_limit import InMemoryRateLimiter
from .storage import RegistryStorage
from .utils import utc_now
from .validation import (
    validate_deregister_request,
    validate_heartbeat_request,
    validate_query_request,
    validate_register_request,
    validate_service_record,
    validate_update_request,
)

ROUTE_PROTOCOL_VERSIONS = {"1": "1.0", "2": "2.0"}
SERVICE_PATH_RE = re.compile(r"^/services/([0-9a-fA-F-]{36})$")
HEARTBEAT_PATH_RE = re.compile(r"^/services/([0-9a-fA-F-]{36})/heartbeat$")
DEREGISTER_PATH_RE = re.compile(r"^/services/([0-9a-fA-F-]{36})/deregister$")


class RegistryRequestHandler(BaseHTTPRequestHandler):
    server_version = "USDPRegistry/0.2"

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def do_PATCH(self) -> None:  # noqa: N802
        self._dispatch("PATCH")

    def log_message(self, format_string: str, *args: object) -> None:
        self.server.logger.info("%s - %s", self.address_string(), format_string % args)

    def _dispatch(self, method: str) -> None:
        request_id = str(uuid4())
        protocol_version = self.server.config.protocol_version
        request_target = urlparse(self.path)
        route_label = request_target.path
        start = time.perf_counter()
        try:
            current_time = utc_now()
            self.server.expire_stale_services(current_time)
            protocol_version, path = self._resolve_protocol_route(request_target.path)
            route_label = f"/v{protocol_version.split('.', 1)[0]}{path}"
            query_params = parse_qs(request_target.query)

            if method == "GET" and path == "/health":
                self._handle_health(request_id, current_time, protocol_version)
                return
            if protocol_version == "2.0" and method == "GET" and path == "/metrics":
                self._handle_metrics(request_id, current_time, protocol_version)
                return
            if protocol_version == "2.0" and method == "GET" and path == "/admin/audit":
                self._handle_audit(request_id, protocol_version, query_params)
                return
            if protocol_version == "2.0" and method == "GET" and path == "/admin/retention":
                self._handle_retention(request_id, current_time, protocol_version)
                return
            if protocol_version == "2.0" and method == "POST" and path == "/admin/purge":
                self._handle_purge(request_id, current_time, protocol_version)
                return
            if protocol_version == "2.0" and method == "POST" and path == "/federation/import":
                self._handle_federation_import(request_id, current_time, protocol_version)
                return
            if protocol_version == "2.0" and method == "POST" and path == "/federation/withdrawals":
                self._handle_federation_withdrawals(request_id, current_time, protocol_version)
                return

            service_match = SERVICE_PATH_RE.fullmatch(path)
            if method == "GET" and service_match:
                self._handle_get_service(request_id, service_match.group(1), protocol_version)
                return

            if method == "POST" and path == "/services":
                self._handle_register(request_id, current_time, protocol_version)
                return

            heartbeat_match = HEARTBEAT_PATH_RE.fullmatch(path)
            if method == "POST" and heartbeat_match:
                self._handle_heartbeat(request_id, heartbeat_match.group(1), current_time, protocol_version)
                return

            deregister_match = DEREGISTER_PATH_RE.fullmatch(path)
            if method == "POST" and deregister_match:
                self._handle_deregister(request_id, deregister_match.group(1), current_time, protocol_version)
                return

            if method == "POST" and path == "/query":
                self._handle_query(request_id, current_time, protocol_version)
                return

            if method == "PATCH" and service_match:
                self._handle_update(request_id, service_match.group(1), current_time, protocol_version)
                return

            raise NotFoundError("Endpoint not found.")
        except RegistryError as exc:
            self._send_error(request_id, exc, protocol_version)
        except json.JSONDecodeError as exc:
            self._send_error(request_id, InvalidRequestError("Request body must be valid JSON.", str(exc)), protocol_version)
        except Exception as exc:  # noqa: BLE001
            self.server.logger.exception("Unhandled server error")
            self._send_error(
                request_id,
                RegistryError(500, "INTERNAL_ERROR", "Internal server error.", str(exc)),
                protocol_version,
            )
        finally:
            duration_ms = (time.perf_counter() - start) * 1000.0
            self.server.metrics.record_request(
                method=method,
                route=route_label,
                status=getattr(self, "_last_status", 500),
                duration_ms=duration_ms,
            )

    def _handle_health(self, request_id: str, current_time, protocol_version: str) -> None:
        payload = {
            "status": "ok",
            "protocol_version": protocol_version,
            "registry_id": self.server.config.registry_id,
            **self.server.storage.get_health_summary(current_time),
        }
        if protocol_version == "2.0":
            payload["supported_protocol_versions"] = list(ROUTE_PROTOCOL_VERSIONS.values())
        self._send_json(HTTPStatus.OK, request_id, True, payload, protocol_version)

    def _handle_metrics(self, request_id: str, current_time, protocol_version: str) -> None:
        actor = self._require_admin_auth()
        payload = {
            "registry_id": self.server.config.registry_id,
            "health": self.server.storage.get_health_summary(current_time),
            "runtime_metrics": self.server.metrics.snapshot(),
        }
        self._append_audit("metrics_view", actor, protocol_version, details={"path": "/metrics"})
        self._send_json(HTTPStatus.OK, request_id, True, payload, protocol_version)

    def _handle_audit(self, request_id: str, protocol_version: str, query_params: dict[str, list[str]]) -> None:
        actor = self._require_admin_auth()
        limit = 50
        if "limit" in query_params:
            try:
                limit = max(1, min(200, int(query_params["limit"][0])))
            except ValueError as exc:
                raise InvalidRequestError("limit must be an integer.") from exc
        events = self.server.storage.list_audit_events(limit=limit)
        self._append_audit("audit_view", actor, protocol_version, details={"limit": limit})
        self._send_json(HTTPStatus.OK, request_id, True, {"items": events, "count": len(events)}, protocol_version)

    def _handle_retention(self, request_id: str, current_time, protocol_version: str) -> None:
        actor = self._require_admin_auth()
        payload = {
            "registry_id": self.server.config.registry_id,
            "retention": {
                "stale_retention_seconds": self.server.config.stale_retention_seconds,
                "withdrawn_retention_seconds": self.server.config.withdrawn_retention_seconds,
            },
            "health": self.server.storage.get_health_summary(current_time),
        }
        self._append_audit("retention_view", actor, protocol_version)
        self._send_json(HTTPStatus.OK, request_id, True, payload, protocol_version)

    def _handle_purge(self, request_id: str, current_time, protocol_version: str) -> None:
        actor = self._require_admin_auth()
        body = self._read_json_body()
        self._require_protocol_version_in_body(body, protocol_version)
        purged = self.server.storage.purge_retired_services(current_time)
        for item in purged:
            self._append_audit("purge", actor, protocol_version, service_id=item["service_id"], details=item)
        self.server.metrics.increment("purged_services_total", len(purged))
        self._send_json(HTTPStatus.OK, request_id, True, {"purged": purged, "count": len(purged)}, protocol_version)

    def _handle_get_service(self, request_id: str, service_id: str, protocol_version: str) -> None:
        record = self.server.storage.get_service(service_id)
        if record is None:
            raise NotFoundError("Service not found.", {"service_id": service_id})
        self._send_json(
            HTTPStatus.OK,
            request_id,
            True,
            {"service": self._project_record_for_protocol(record, protocol_version)},
            protocol_version,
        )

    def _handle_register(self, request_id: str, current_time, protocol_version: str) -> None:
        actor = self._require_publisher_auth()
        body = self._read_json_body()
        normalized = validate_register_request(body, protocol_version)
        record = create_registered_record(normalized["service"], current_time, self.server.config.default_ttl_seconds)
        record["publisher"] = self._publisher_record(record.get("publisher"), actor)
        record["publisher_identity"] = self._publisher_identity_record(actor)
        if protocol_version == "2.0" and "provenance" not in record:
            record["provenance"] = {
                "source_kind": "agent" if record["publisher"]["publisher_type"] == "agent" else "publisher",
                "collected_by": actor.publisher_name,
                "observed_at": record["timestamps"]["registered_at"],
            }
        self.server.storage.create_service(record, current_time=current_time)
        self._append_audit("register", actor, protocol_version, service_id=record["service_id"])
        self._send_json(
            HTTPStatus.CREATED,
            request_id,
            True,
            {"service": self._project_record_for_protocol(record, protocol_version)},
            protocol_version,
        )

    def _handle_update(self, request_id: str, service_id: str, current_time, protocol_version: str) -> None:
        actor = self._require_publisher_auth()
        existing_record = self.server.storage.get_service(service_id)
        if existing_record is None:
            raise NotFoundError("Service not found.", {"service_id": service_id})
        existing_record = self._require_service_owner(existing_record, actor)

        body = self._read_json_body()
        normalized = validate_update_request(body, service_id, existing_record, protocol_version)
        record = apply_update(existing_record, normalized["changes"], current_time)
        self.server.storage.replace_service(record, current_time=current_time)
        self._append_audit("update", actor, protocol_version, service_id=service_id, details={"fields": sorted(normalized["changes"])})
        self._send_json(
            HTTPStatus.OK,
            request_id,
            True,
            {"service": self._project_record_for_protocol(record, protocol_version)},
            protocol_version,
        )

    def _handle_heartbeat(self, request_id: str, service_id: str, current_time, protocol_version: str) -> None:
        actor = self._require_publisher_auth()
        existing_record = self.server.storage.get_service(service_id)
        if existing_record is None:
            raise NotFoundError("Service not found.", {"service_id": service_id})
        existing_record = self._require_service_owner(existing_record, actor)

        body = self._read_json_body()
        normalized = validate_heartbeat_request(body, service_id, protocol_version)
        record = apply_heartbeat(existing_record, normalized["status"], current_time)
        self.server.storage.replace_service(record, current_time=current_time)
        self._append_audit("heartbeat", actor, protocol_version, service_id=service_id, details={"status": normalized["status"]})
        self._send_json(
            HTTPStatus.OK,
            request_id,
            True,
            {"service": self._project_record_for_protocol(record, protocol_version)},
            protocol_version,
        )

    def _handle_deregister(self, request_id: str, service_id: str, current_time, protocol_version: str) -> None:
        actor = self._require_publisher_auth()
        existing_record = self.server.storage.get_service(service_id)
        if existing_record is None:
            raise NotFoundError("Service not found.", {"service_id": service_id})
        existing_record = self._require_service_owner(existing_record, actor)

        body = self._read_json_body()
        normalized = validate_deregister_request(body, service_id, protocol_version)
        record = apply_deregister(existing_record, normalized["reason"], current_time)
        self.server.storage.replace_service(record, current_time=current_time)
        self._append_audit("deregister", actor, protocol_version, service_id=service_id, details={"reason": normalized["reason"]})
        self._send_json(
            HTTPStatus.OK,
            request_id,
            True,
            {"service": self._project_record_for_protocol(record, protocol_version)},
            protocol_version,
        )

    def _handle_query(self, request_id: str, current_time, protocol_version: str) -> None:
        self._enforce_rate_limit(f"query:{self.client_address[0]}", self.server.config.query_rate_limit_per_minute, category="query")
        body = self._read_json_body()
        normalized = validate_query_request(body, protocol_version)
        self._enforce_query_complexity(normalized)
        results = self.server.storage.query_services(normalized, current_time, protocol_version)
        projected_results = dict(results)
        projected_results["items"] = [
            self._project_record_for_protocol(item, protocol_version) for item in results["items"]
        ]
        self._send_json(HTTPStatus.OK, request_id, True, projected_results, protocol_version)

    def _handle_federation_import(self, request_id: str, current_time, protocol_version: str) -> None:
        actor = self._require_peer_auth()
        body = self._read_json_body()
        self._require_protocol_version_in_body(body, protocol_version)
        peer_registry = self._validate_string_field(body.get("peer_registry"), "peer_registry")
        if actor.role == "peer" and peer_registry != actor.publisher_name:
            raise ForbiddenError("peer_registry must match the authenticated peer identity.")
        records = body.get("records")
        if not isinstance(records, list) or not records:
            raise InvalidRequestError("records must be a non-empty array.")

        imported = []
        for index, item in enumerate(records):
            record = validate_service_record(
                item,
                allow_timestamps=True,
                protocol_version="2.0",
                allow_server_fields=True,
            )
            existing = self.server.storage.get_service(record["service_id"])
            if existing is not None and not self._is_record_owned_by_peer(existing, peer_registry):
                raise ConflictError(
                    "Federation import cannot overwrite a locally owned record.",
                    {"service_id": record["service_id"], "peer_registry": peer_registry, "index": index},
                )
            prepared = prepare_federated_record(
                record,
                peer_registry=peer_registry,
                current_time=current_time,
                default_ttl_seconds=self.server.config.default_ttl_seconds,
            )
            action = self.server.storage.upsert_federated_service(prepared, current_time=current_time)
            imported.append({"service_id": prepared["service_id"], "action": action})
            self._append_audit("federation_import", actor, protocol_version, service_id=prepared["service_id"], details={"peer_registry": peer_registry, "action": action})

        self.server.metrics.increment("federation_imported_records_total", len(imported))
        self._send_json(HTTPStatus.OK, request_id, True, {"peer_registry": peer_registry, "items": imported, "count": len(imported)}, protocol_version)

    def _handle_federation_withdrawals(self, request_id: str, current_time, protocol_version: str) -> None:
        actor = self._require_peer_auth()
        body = self._read_json_body()
        self._require_protocol_version_in_body(body, protocol_version)
        peer_registry = self._validate_string_field(body.get("peer_registry"), "peer_registry")
        if actor.role == "peer" and peer_registry != actor.publisher_name:
            raise ForbiddenError("peer_registry must match the authenticated peer identity.")
        service_ids = body.get("service_ids")
        if not isinstance(service_ids, list) or not service_ids:
            raise InvalidRequestError("service_ids must be a non-empty array.")
        reason = body.get("reason")
        if reason is not None:
            reason = self._validate_string_field(reason, "reason")

        withdrawn = []
        for raw_service_id in service_ids:
            service_id = self._validate_string_field(raw_service_id, "service_id")
            existing = self.server.storage.get_service(service_id)
            if existing is None:
                continue
            if not self._is_record_owned_by_peer(existing, peer_registry):
                raise ForbiddenError("Peer is not allowed to withdraw this service.", {"service_id": service_id})
            record = apply_federated_withdrawal(existing, reason=reason, current_time=current_time, peer_registry=peer_registry)
            self.server.storage.replace_service(record, current_time=current_time)
            withdrawn.append(service_id)
            self._append_audit("federation_withdrawal", actor, protocol_version, service_id=service_id, details={"peer_registry": peer_registry, "reason": reason})

        self.server.metrics.increment("federation_withdrawn_records_total", len(withdrawn))
        self._send_json(HTTPStatus.OK, request_id, True, {"peer_registry": peer_registry, "service_ids": withdrawn, "count": len(withdrawn)}, protocol_version)

    def _require_publisher_auth(self) -> AuthorizedPublisher:
        return self._authorize_with_failure_tracking(
            lambda header: self.server.authorizer.authorize_publisher(header),
            category="write",
            limit=self.server.config.write_rate_limit_per_minute,
        )

    def _require_admin_auth(self) -> AuthorizedPublisher:
        return self._authorize_with_failure_tracking(
            lambda header: self.server.authorizer.authorize_admin(header),
            category="admin",
            limit=self.server.config.admin_rate_limit_per_minute,
        )

    def _require_peer_auth(self) -> AuthorizedPublisher:
        return self._authorize_with_failure_tracking(
            lambda header: self.server.authorizer.authorize_peer(header),
            category="peer",
            limit=self.server.config.peer_rate_limit_per_minute,
        )

    def _authorize_with_failure_tracking(self, authorizer, *, category: str, limit: int) -> AuthorizedPublisher:
        auth_header = self.headers.get("Authorization")
        try:
            actor = authorizer(auth_header)
        except UnauthorizedError:
            self.server.metrics.increment("auth_failures_total")
            if not self.server.rate_limiter.consume(
                f"auth-failure:{self.client_address[0]}",
                limit=self.server.config.auth_failures_per_minute,
            ):
                self.server.metrics.increment("rate_limited_total")
                raise RateLimitedError("Too many failed authentication attempts.")
            raise
        self._enforce_rate_limit(f"{category}:{actor.role}:{actor.publisher_id}", limit, category=category)
        return actor

    def _enforce_rate_limit(self, key: str, limit: int, *, category: str) -> None:
        if not self.server.rate_limiter.consume(key, limit=limit):
            self.server.metrics.increment("rate_limited_total")
            raise RateLimitedError(details={"category": category})

    def _require_service_owner(self, record: dict, publisher: AuthorizedPublisher) -> dict:
        current = deepcopy(record)
        publisher_info = dict(current.get("publisher", {}))
        publisher_identity = dict(current.get("publisher_identity", {}))
        existing_publisher_id = publisher_identity.get("publisher_id") or publisher_info.get("publisher_id")
        if existing_publisher_id and existing_publisher_id != publisher.publisher_id:
            raise ForbiddenError(details={"service_id": record["service_id"]})

        current["publisher"] = self._publisher_record(publisher_info, publisher)
        current["publisher_identity"] = self._publisher_identity_record(publisher)
        return current

    def _publisher_record(self, raw_publisher: dict | None, publisher: AuthorizedPublisher) -> dict:
        publisher_info = dict(raw_publisher or {})
        publisher_info["publisher_id"] = publisher.publisher_id
        publisher_info["publisher_name"] = publisher_info.get("publisher_name") or publisher.publisher_name
        publisher_info["publisher_type"] = publisher_info.get("publisher_type") or "service"
        return publisher_info

    def _publisher_identity_record(self, publisher: AuthorizedPublisher) -> dict:
        return {
            "publisher_id": publisher.publisher_id,
            "publisher_name": publisher.publisher_name,
            "identity_type": publisher.identity_type,
            "authenticated": True,
            "asserted_by": "registry",
        }

    def _project_record_for_protocol(self, record: dict, protocol_version: str) -> dict:
        projected = deepcopy(record)
        if protocol_version == "1.0":
            projected.pop("publisher_identity", None)
            projected.pop("provenance", None)
            projected.pop("extensions", None)
        return projected

    def _resolve_protocol_route(self, request_path: str) -> tuple[str, str]:
        for route_version, protocol_version in ROUTE_PROTOCOL_VERSIONS.items():
            prefix = f"/v{route_version}"
            if request_path == prefix:
                return protocol_version, "/"
            if request_path.startswith(prefix + "/"):
                return protocol_version, request_path[len(prefix):]
        raise NotFoundError("Endpoint not found.")

    def _read_json_body(self) -> object:
        content_length_header = self.headers.get("Content-Length")
        if content_length_header is None:
            raise InvalidRequestError("Request body is required.")
        try:
            content_length = int(content_length_header)
        except ValueError as exc:
            raise InvalidRequestError("Invalid Content-Length header.") from exc
        if content_length <= 0:
            raise InvalidRequestError("Request body is required.")
        if content_length > self.server.config.max_request_bytes:
            raise PayloadTooLargeError(details={"max_request_bytes": self.server.config.max_request_bytes})
        raw_body = self.rfile.read(content_length)
        if not raw_body:
            raise InvalidRequestError("Request body is required.")
        return json.loads(raw_body.decode("utf-8"))

    def _enforce_query_complexity(self, query: dict) -> None:
        criteria = query.get("criteria")
        if criteria is None:
            return
        if self._count_criteria_nodes(criteria) > self.server.config.max_query_criteria_nodes:
            raise RegistryValidationError(
                "criteria exceeds the configured complexity limit.",
                {"max_query_criteria_nodes": self.server.config.max_query_criteria_nodes},
            )

    def _count_criteria_nodes(self, criteria: dict) -> int:
        if "all" in criteria:
            return 1 + sum(self._count_criteria_nodes(item) for item in criteria["all"])
        if "any" in criteria:
            return 1 + sum(self._count_criteria_nodes(item) for item in criteria["any"])
        if "not" in criteria:
            return 1 + self._count_criteria_nodes(criteria["not"])
        return 1

    def _append_audit(self, action: str, actor: AuthorizedPublisher | None, protocol_version: str, *, service_id: str | None = None, details: dict | None = None) -> None:
        self.server.storage.append_audit_event(
            action=action,
            actor_id=actor.publisher_id if actor else None,
            actor_name=actor.publisher_name if actor else None,
            actor_role=actor.role if actor else "system",
            protocol_version=protocol_version,
            service_id=service_id,
            details=details,
        )

    def _is_record_owned_by_peer(self, record: dict, peer_registry: str) -> bool:
        provenance = record.get("provenance", {})
        return provenance.get("source_kind") == "federated_registry" and provenance.get("source_registry") == peer_registry

    def _require_protocol_version_in_body(self, body: object, protocol_version: str) -> None:
        if not isinstance(body, dict):
            raise InvalidRequestError("Request body must be an object.")
        received = self._validate_string_field(body.get("protocol_version"), "protocol_version")
        if received != protocol_version:
            raise RegistryValidationError("Unsupported protocol_version.", {"expected": protocol_version, "received": received})

    def _validate_string_field(self, value: object, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise InvalidRequestError(f"{field_name} must be a non-empty string.")
        return value.strip()

    def _send_json(
        self,
        status: int,
        request_id: str,
        success: bool,
        data: object | None = None,
        protocol_version: str = "1.0",
        errors: list[dict] | None = None,
    ) -> None:
        self._last_status = status
        payload = {
            "protocol_version": protocol_version,
            "request_id": request_id,
            "success": success,
        }
        if success:
            payload["data"] = data
        else:
            payload["errors"] = errors or []

        encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_error(self, request_id: str, error: RegistryError, protocol_version: str) -> None:
        error_item = {"code": error.code, "message": error.message}
        if error.details is not None:
            error_item["details"] = error.details
        self._send_json(error.status, request_id, False, protocol_version=protocol_version, errors=[error_item])


class RegistryHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], config: RegistryConfig) -> None:
        super().__init__(server_address, RegistryRequestHandler)
        self.config = config
        self.storage = RegistryStorage(config)
        self.storage.initialize()
        self.authorizer = PublisherAuthorizer(config.publisher_tokens, config.admin_tokens, config.peer_tokens)
        self.metrics = OperationalMetrics()
        self.rate_limiter = InMemoryRateLimiter()
        self.logger = logging.getLogger("usdp_registry")
        self._expiry_stop = threading.Event()
        self._expiry_thread = threading.Thread(target=self._run_expiry_loop, daemon=True)
        if self.config.expiry_check_interval_seconds > 0:
            self._expiry_thread.start()

    def run_maintenance(self, current_time) -> None:
        self.expire_stale_services(current_time)
        self._purge_due_records(current_time)

    def expire_stale_services(self, current_time) -> None:
        expired = self.storage.expire_stale_services(current_time)
        if expired:
            self.metrics.increment("expired_services_total", expired)

    def _purge_due_records(self, current_time) -> None:
        purged = self.storage.purge_retired_services(current_time)
        if purged:
            self.metrics.increment("purged_services_total", len(purged))
            for item in purged:
                self.storage.append_audit_event(
                    action="purge",
                    actor_id=None,
                    actor_name="registry-maintenance",
                    actor_role="system",
                    protocol_version="2.0",
                    service_id=item["service_id"],
                    details=item,
                )

    def _run_expiry_loop(self) -> None:
        while not self._expiry_stop.wait(self.config.expiry_check_interval_seconds):
            try:
                self.run_maintenance(utc_now())
            except Exception:  # noqa: BLE001
                self.logger.exception("Registry maintenance loop failed")

    def server_close(self) -> None:
        self._expiry_stop.set()
        if self._expiry_thread.is_alive():
            self._expiry_thread.join(timeout=max(1.0, self.config.expiry_check_interval_seconds + 1.0))
        super().server_close()
