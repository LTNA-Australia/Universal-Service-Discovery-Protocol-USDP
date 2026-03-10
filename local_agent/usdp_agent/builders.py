"""Service record builders used by the local agent."""

from __future__ import annotations

from copy import deepcopy
from typing import Any
from urllib.parse import urlparse
from uuid import NAMESPACE_URL, uuid5


def stable_service_id(seed: str) -> str:
    return str(uuid5(NAMESPACE_URL, seed))


def build_api_service(
    *,
    service_id: str,
    name: str,
    base_url: str,
    auth_type: str,
    version: str,
    supported_protocols: list[str] | None = None,
    status: str = "online",
    tags: list[str] | None = None,
    publisher: dict | None = None,
    auth: dict | None = None,
    metadata: dict | None = None,
    health_endpoint: str | None = None,
    documentation_url: str | None = None,
    rate_limit_hint: str | None = None,
    capability_tags: list[str] | None = None,
    description: str | None = None,
    heartbeat_ttl_seconds: int | None = None,
    provenance: dict | None = None,
    extensions: dict | None = None,
) -> dict:
    parsed = urlparse(base_url)
    protocols = supported_protocols or ([parsed.scheme] if parsed.scheme else ["https"])
    record = {
        "service_id": service_id,
        "name": name,
        "service_type": "api",
        "status": status,
        "endpoints": [
            {
                "protocol": parsed.scheme or protocols[0],
                "url": base_url,
                "secure": (parsed.scheme == "https"),
            }
        ],
        "capabilities": {
            "base_url": base_url,
            "supported_protocols": protocols,
            "auth_type": auth_type,
            "version": version,
        },
    }
    if health_endpoint:
        record["capabilities"]["health_endpoint"] = health_endpoint
    if documentation_url:
        record["capabilities"]["documentation_url"] = documentation_url
    if rate_limit_hint:
        record["capabilities"]["rate_limit_hint"] = rate_limit_hint
    if capability_tags:
        record["capabilities"]["capability_tags"] = list(capability_tags)
    return _apply_common_fields(
        record,
        description=description,
        tags=tags,
        publisher=publisher,
        auth=auth,
        metadata=metadata,
        heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        provenance=provenance,
        extensions=extensions,
    )


def build_printer_service(
    *,
    service_id: str,
    name: str,
    endpoint_url: str,
    color: bool,
    duplex: bool,
    supported_paper_sizes: list[str],
    print_protocols: list[str],
    location: dict,
    status: str = "online",
    queue_name: str | None = None,
    manufacturer: str | None = None,
    model: str | None = None,
    max_resolution_dpi: int | None = None,
    tags: list[str] | None = None,
    publisher: dict | None = None,
    auth: dict | None = None,
    metadata: dict | None = None,
    description: str | None = None,
    heartbeat_ttl_seconds: int | None = None,
    provenance: dict | None = None,
    extensions: dict | None = None,
) -> dict:
    parsed = urlparse(endpoint_url)
    record = {
        "service_id": service_id,
        "name": name,
        "service_type": "printer",
        "status": status,
        "endpoints": [
            {
                "protocol": parsed.scheme or "ipp",
                "url": endpoint_url,
                "secure": (parsed.scheme in {"ipps", "https"}),
            }
        ],
        "capabilities": {
            "color": color,
            "duplex": duplex,
            "supported_paper_sizes": list(supported_paper_sizes),
            "print_protocols": list(print_protocols),
        },
    }
    if queue_name:
        record["capabilities"]["queue_name"] = queue_name
    if manufacturer:
        record["capabilities"]["manufacturer"] = manufacturer
    if model:
        record["capabilities"]["model"] = model
    if max_resolution_dpi is not None:
        record["capabilities"]["max_resolution_dpi"] = max_resolution_dpi
    return _apply_common_fields(
        record,
        description=description,
        tags=tags,
        publisher=publisher,
        auth=auth,
        metadata=metadata,
        location=location,
        heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        provenance=provenance,
        extensions=extensions,
    )


def build_camera_service(
    *,
    service_id: str,
    name: str,
    endpoint_url: str,
    stream_protocols: list[str],
    resolution: str,
    night_vision: bool,
    ptz: bool,
    location: dict,
    status: str = "online",
    thermal: bool | None = None,
    frame_rate: float | None = None,
    manufacturer: str | None = None,
    model: str | None = None,
    tags: list[str] | None = None,
    publisher: dict | None = None,
    auth: dict | None = None,
    metadata: dict | None = None,
    description: str | None = None,
    heartbeat_ttl_seconds: int | None = None,
    provenance: dict | None = None,
    extensions: dict | None = None,
) -> dict:
    parsed = urlparse(endpoint_url)
    record = {
        "service_id": service_id,
        "name": name,
        "service_type": "camera",
        "status": status,
        "endpoints": [
            {
                "protocol": parsed.scheme or stream_protocols[0],
                "url": endpoint_url,
                "secure": (parsed.scheme == "https"),
            }
        ],
        "capabilities": {
            "stream_protocols": list(stream_protocols),
            "resolution": resolution,
            "night_vision": night_vision,
            "ptz": ptz,
        },
    }
    if thermal is not None:
        record["capabilities"]["thermal"] = thermal
    if frame_rate is not None:
        record["capabilities"]["frame_rate"] = frame_rate
    if manufacturer:
        record["capabilities"]["manufacturer"] = manufacturer
    if model:
        record["capabilities"]["model"] = model
    return _apply_common_fields(
        record,
        description=description,
        tags=tags,
        publisher=publisher,
        auth=auth,
        metadata=metadata,
        location=location,
        heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        provenance=provenance,
        extensions=extensions,
    )


def build_database_service(
    *,
    service_id: str,
    name: str,
    endpoint_url: str,
    engine: str,
    version: str,
    role: str,
    supports_tls: bool,
    database_name: str,
    read_only: bool,
    status: str = "online",
    replication_mode: str | None = None,
    tags: list[str] | None = None,
    publisher: dict | None = None,
    auth: dict | None = None,
    metadata: dict | None = None,
    description: str | None = None,
    heartbeat_ttl_seconds: int | None = None,
    provenance: dict | None = None,
    extensions: dict | None = None,
) -> dict:
    parsed = urlparse(endpoint_url)
    record = {
        "service_id": service_id,
        "name": name,
        "service_type": "database",
        "status": status,
        "endpoints": [_endpoint_from_url(endpoint_url, default_protocol=parsed.scheme or "postgres")],
        "capabilities": {
            "engine": engine,
            "version": version,
            "role": role,
            "supports_tls": supports_tls,
            "database_name": database_name,
            "read_only": read_only,
        },
    }
    if replication_mode:
        record["capabilities"]["replication_mode"] = replication_mode
    return _apply_common_fields(
        record,
        description=description,
        tags=tags,
        publisher=publisher,
        auth=auth,
        metadata=metadata,
        heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        provenance=provenance,
        extensions=extensions,
    )


def build_ai_model_endpoint_service(
    *,
    service_id: str,
    name: str,
    endpoint_url: str,
    model_name: str,
    model_family: str,
    modalities: list[str],
    supports_streaming: bool,
    context_window: int,
    auth_type: str,
    provider_kind: str,
    status: str = "online",
    tags: list[str] | None = None,
    publisher: dict | None = None,
    auth: dict | None = None,
    metadata: dict | None = None,
    description: str | None = None,
    heartbeat_ttl_seconds: int | None = None,
    provenance: dict | None = None,
    extensions: dict | None = None,
) -> dict:
    parsed = urlparse(endpoint_url)
    record = {
        "service_id": service_id,
        "name": name,
        "service_type": "ai_model_endpoint",
        "status": status,
        "endpoints": [_endpoint_from_url(endpoint_url, default_protocol=parsed.scheme or "https")],
        "capabilities": {
            "model_name": model_name,
            "model_family": model_family,
            "modalities": list(modalities),
            "supports_streaming": supports_streaming,
            "context_window": context_window,
            "auth_type": auth_type,
            "provider_kind": provider_kind,
        },
    }
    return _apply_common_fields(
        record,
        description=description,
        tags=tags,
        publisher=publisher,
        auth=auth,
        metadata=metadata,
        heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        provenance=provenance,
        extensions=extensions,
    )


def build_storage_service(
    *,
    service_id: str,
    name: str,
    endpoint_url: str,
    storage_kind: str,
    protocols: list[str],
    supports_versioning: bool,
    supports_encryption: bool,
    region: str,
    status: str = "online",
    bucket_or_share: str | None = None,
    tags: list[str] | None = None,
    publisher: dict | None = None,
    auth: dict | None = None,
    metadata: dict | None = None,
    description: str | None = None,
    heartbeat_ttl_seconds: int | None = None,
    provenance: dict | None = None,
    extensions: dict | None = None,
) -> dict:
    parsed = urlparse(endpoint_url)
    record = {
        "service_id": service_id,
        "name": name,
        "service_type": "storage",
        "status": status,
        "endpoints": [_endpoint_from_url(endpoint_url, default_protocol=parsed.scheme or protocols[0])],
        "capabilities": {
            "storage_kind": storage_kind,
            "protocols": list(protocols),
            "supports_versioning": supports_versioning,
            "supports_encryption": supports_encryption,
            "region": region,
        },
    }
    if bucket_or_share:
        record["capabilities"]["bucket_or_share"] = bucket_or_share
    return _apply_common_fields(
        record,
        description=description,
        tags=tags,
        publisher=publisher,
        auth=auth,
        metadata=metadata,
        heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        provenance=provenance,
        extensions=extensions,
    )


def build_message_broker_service(
    *,
    service_id: str,
    name: str,
    endpoint_url: str,
    broker_kind: str,
    protocols: list[str],
    supports_persistence: bool,
    supports_tls: bool,
    tenant_scope: str,
    ordering_mode: str,
    status: str = "online",
    tags: list[str] | None = None,
    publisher: dict | None = None,
    auth: dict | None = None,
    metadata: dict | None = None,
    description: str | None = None,
    heartbeat_ttl_seconds: int | None = None,
    provenance: dict | None = None,
    extensions: dict | None = None,
) -> dict:
    parsed = urlparse(endpoint_url)
    record = {
        "service_id": service_id,
        "name": name,
        "service_type": "message_broker",
        "status": status,
        "endpoints": [_endpoint_from_url(endpoint_url, default_protocol=parsed.scheme or protocols[0])],
        "capabilities": {
            "broker_kind": broker_kind,
            "protocols": list(protocols),
            "supports_persistence": supports_persistence,
            "supports_tls": supports_tls,
            "tenant_scope": tenant_scope,
            "ordering_mode": ordering_mode,
        },
    }
    return _apply_common_fields(
        record,
        description=description,
        tags=tags,
        publisher=publisher,
        auth=auth,
        metadata=metadata,
        heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        provenance=provenance,
        extensions=extensions,
    )


def build_sensor_service(
    *,
    service_id: str,
    name: str,
    endpoint_url: str,
    sensor_kind: str,
    measurement_types: list[str],
    sampling_interval_ms: int,
    units: str,
    battery_powered: bool,
    location_scope: str,
    status: str = "online",
    tags: list[str] | None = None,
    publisher: dict | None = None,
    auth: dict | None = None,
    metadata: dict | None = None,
    description: str | None = None,
    location: dict | None = None,
    heartbeat_ttl_seconds: int | None = None,
    provenance: dict | None = None,
    extensions: dict | None = None,
) -> dict:
    parsed = urlparse(endpoint_url)
    record = {
        "service_id": service_id,
        "name": name,
        "service_type": "sensor",
        "status": status,
        "endpoints": [_endpoint_from_url(endpoint_url, default_protocol=parsed.scheme or "http")],
        "capabilities": {
            "sensor_kind": sensor_kind,
            "measurement_types": list(measurement_types),
            "sampling_interval_ms": sampling_interval_ms,
            "units": units,
            "battery_powered": battery_powered,
            "location_scope": location_scope,
        },
    }
    return _apply_common_fields(
        record,
        description=description,
        tags=tags,
        publisher=publisher,
        auth=auth,
        metadata=metadata,
        location=location,
        heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        provenance=provenance,
        extensions=extensions,
    )


def build_service_update_changes(service_record: dict[str, Any]) -> dict[str, Any]:
    changes = deepcopy(service_record)
    changes.pop("service_id", None)
    changes.pop("service_type", None)
    changes.pop("timestamps", None)
    changes.pop("publisher_identity", None)
    return changes


def _endpoint_from_url(url: str, *, default_protocol: str) -> dict:
    parsed = urlparse(url)
    protocol = parsed.scheme or default_protocol
    return {
        "protocol": protocol,
        "url": url,
        "secure": protocol in {"https", "ipps", "mqtts", "amqps", "postgresql+tls", "postgres+tls"},
    }


def _apply_common_fields(
    record: dict,
    *,
    description: str | None = None,
    tags: list[str] | None = None,
    publisher: dict | None = None,
    auth: dict | None = None,
    metadata: dict | None = None,
    location: dict | None = None,
    heartbeat_ttl_seconds: int | None = None,
    provenance: dict | None = None,
    extensions: dict | None = None,
) -> dict:
    if description is not None:
        record["description"] = description
    if tags:
        record["tags"] = list(tags)
    if publisher:
        record["publisher"] = deepcopy(publisher)
    if auth:
        record["auth"] = deepcopy(auth)
    if metadata:
        record["metadata"] = deepcopy(metadata)
    if location:
        record["location"] = deepcopy(location)
    if heartbeat_ttl_seconds is not None:
        record["heartbeat_ttl_seconds"] = heartbeat_ttl_seconds
    if provenance:
        record["provenance"] = deepcopy(provenance)
    if extensions:
        record["extensions"] = deepcopy(extensions)
    return record
