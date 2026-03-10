"""Request and record validation."""

from __future__ import annotations

from copy import deepcopy
from uuid import UUID

from .errors import ValidationError
from .utils import parse_datetime

V1_SERVICE_TYPES = {"printer", "camera", "api"}
V2_SERVICE_TYPES = V1_SERVICE_TYPES | {
    "database",
    "ai_model_endpoint",
    "storage",
    "message_broker",
    "sensor",
}
SERVICE_TYPES_BY_PROTOCOL = {
    "1.0": V1_SERVICE_TYPES,
    "2.0": V2_SERVICE_TYPES,
}
LOCATION_REQUIRED_SERVICE_TYPES = {"printer", "camera"}
STATUS_VALUES = {"online", "degraded", "offline", "unknown"}
AUTH_TYPES = {"none", "basic", "bearer", "apikey", "oauth2", "mtls", "other"}
IDENTITY_TYPES = {"bearer_token", "service_account", "mtls", "external"}
PROVENANCE_SOURCE_KINDS = {"publisher", "agent", "registry", "federated_registry", "external_import"}
BASE_SERVICE_FIELDS = {
    "service_id",
    "name",
    "description",
    "service_type",
    "status",
    "endpoints",
    "capabilities",
    "tags",
    "auth",
    "metadata",
    "publisher",
    "location",
    "heartbeat_ttl_seconds",
    "timestamps",
}
V2_SERVICE_FIELDS = {"provenance", "extensions"}
SERVER_MANAGED_FIELDS = {"publisher_identity"}
BASE_UPDATE_FIELDS = {
    "name",
    "description",
    "status",
    "endpoints",
    "capabilities",
    "tags",
    "auth",
    "metadata",
    "location",
    "heartbeat_ttl_seconds",
}
V2_UPDATE_FIELDS = {"provenance", "extensions"}
V1_QUERY_FIELDS = {"protocol_version", "filters", "page", "page_size", "sort", "include_inactive"}
V2_QUERY_FIELDS = V1_QUERY_FIELDS | {"criteria"}
FILTER_FIELDS = {"service_type", "status", "service_ids", "tags_all", "name_contains", "location", "capabilities"}
V1_SORT_FIELDS = {"name", "updated_at", "registered_at"}
V2_SORT_FIELDS = V1_SORT_FIELDS | {"service_type", "status", "last_heartbeat_at"}
CRITERIA_OPERATORS = {"eq", "neq", "in", "contains", "exists", "starts_with", "gte", "lte"}


def validate_register_request(body: object, protocol_version: str) -> dict:
    request = _require_mapping(body, "Request body")
    _require_protocol_version(request, protocol_version)
    service = validate_service_record(
        request.get("service"),
        allow_timestamps=False,
        protocol_version=protocol_version,
        allow_server_fields=False,
    )
    return {"protocol_version": protocol_version, "service": service}


def validate_update_request(body: object, path_service_id: str, existing_record: dict, protocol_version: str) -> dict:
    request = _require_mapping(body, "Request body")
    _require_protocol_version(request, protocol_version)
    body_service_id = _validate_uuid_string(request.get("service_id"), "service_id")
    if body_service_id != path_service_id:
        raise ValidationError("service_id in body must match the request path.")

    raw_changes = _require_mapping(request.get("changes"), "changes")
    if not raw_changes:
        raise ValidationError("changes must include at least one field.")

    unknown_fields = sorted(set(raw_changes) - _allowed_update_fields(protocol_version))
    if unknown_fields:
        raise ValidationError("changes includes unsupported fields.", {"fields": unknown_fields})

    candidate = deepcopy(existing_record)
    for key, value in raw_changes.items():
        candidate[key] = value

    validated_candidate = validate_service_record(
        candidate,
        allow_timestamps=True,
        protocol_version=protocol_version,
        allow_server_fields=True,
    )
    normalized_changes = {key: validated_candidate[key] for key in raw_changes}
    return {
        "protocol_version": protocol_version,
        "service_id": body_service_id,
        "changes": normalized_changes,
    }


def validate_heartbeat_request(body: object, path_service_id: str, protocol_version: str) -> dict:
    request = _require_mapping(body, "Request body")
    _require_protocol_version(request, protocol_version)
    service_id = _validate_uuid_string(request.get("service_id"), "service_id")
    if service_id != path_service_id:
        raise ValidationError("service_id in body must match the request path.")

    status = request.get("status")
    if status is not None:
        status = _validate_status(status)

    return {"protocol_version": protocol_version, "service_id": service_id, "status": status}


def validate_deregister_request(body: object, path_service_id: str, protocol_version: str) -> dict:
    request = _require_mapping(body, "Request body")
    _require_protocol_version(request, protocol_version)
    service_id = _validate_uuid_string(request.get("service_id"), "service_id")
    if service_id != path_service_id:
        raise ValidationError("service_id in body must match the request path.")

    reason = request.get("reason")
    if reason is not None:
        reason = _validate_optional_string(reason, "reason")

    return {"protocol_version": protocol_version, "service_id": service_id, "reason": reason}


def validate_query_request(body: object, protocol_version: str) -> dict:
    request = _require_mapping(body, "Request body")
    _require_protocol_version(request, protocol_version)

    allowed_query_fields = V2_QUERY_FIELDS if protocol_version == "2.0" else V1_QUERY_FIELDS
    unknown_query_fields = sorted(set(request) - allowed_query_fields)
    if unknown_query_fields:
        raise ValidationError("Query request includes unsupported fields.", {"fields": unknown_query_fields})

    result = {
        "protocol_version": protocol_version,
        "filters": {},
        "page": 1,
        "page_size": 25,
        "sort": [],
        "include_inactive": False,
    }

    if "filters" in request:
        result["filters"] = _validate_filters(request.get("filters"), protocol_version)
    if protocol_version == "2.0" and "criteria" in request:
        if result["filters"]:
            raise ValidationError("v2 query may include filters or criteria, but not both.")
        result["criteria"] = _validate_criteria(request.get("criteria"))

    if "page" in request:
        result["page"] = _validate_integer(request["page"], "page", minimum=1)
    if "page_size" in request:
        result["page_size"] = _validate_integer(request["page_size"], "page_size", minimum=1, maximum=100)
    if "sort" in request:
        result["sort"] = _validate_sort(request["sort"], protocol_version)
    if "include_inactive" in request:
        if not isinstance(request["include_inactive"], bool):
            raise ValidationError("include_inactive must be a boolean.")
        result["include_inactive"] = request["include_inactive"]

    return result


def validate_service_record(
    service: object,
    *,
    allow_timestamps: bool,
    protocol_version: str = "1.0",
    allow_server_fields: bool = False,
) -> dict:
    record = _require_mapping(service, "service")
    unknown_fields = sorted(set(record) - _allowed_service_fields(protocol_version, allow_server_fields))
    if unknown_fields:
        raise ValidationError("service includes unsupported fields.", {"fields": unknown_fields})

    service_type = _validate_service_type(record.get("service_type"), protocol_version)
    normalized = {
        "service_id": _validate_uuid_string(record.get("service_id"), "service_id"),
        "name": _validate_string(record.get("name"), "name"),
        "service_type": service_type,
        "status": _validate_status(record.get("status")),
        "endpoints": _validate_endpoints(record.get("endpoints")),
        "capabilities": _validate_capabilities(service_type, record.get("capabilities")),
    }

    if "description" in record:
        normalized["description"] = _validate_optional_string(record.get("description"), "description")
    if "tags" in record:
        normalized["tags"] = _validate_string_list(record.get("tags"), "tags", allow_empty=True)
    if "auth" in record:
        normalized["auth"] = _validate_auth(record.get("auth"))
    if "metadata" in record:
        normalized["metadata"] = deepcopy(_require_mapping(record.get("metadata"), "metadata"))
    if "publisher" in record:
        normalized["publisher"] = _validate_publisher(record.get("publisher"))
    if "location" in record:
        normalized["location"] = _validate_location(record.get("location"))
    if "heartbeat_ttl_seconds" in record:
        normalized["heartbeat_ttl_seconds"] = _validate_integer(
            record.get("heartbeat_ttl_seconds"),
            "heartbeat_ttl_seconds",
            minimum=30,
            maximum=300,
        )
    if allow_timestamps and "timestamps" in record:
        normalized["timestamps"] = _validate_timestamps(record.get("timestamps"))
    if protocol_version == "2.0":
        if "provenance" in record:
            normalized["provenance"] = _validate_provenance(record.get("provenance"))
        if "extensions" in record:
            normalized["extensions"] = deepcopy(_require_mapping(record.get("extensions"), "extensions"))
    if allow_server_fields and "publisher_identity" in record:
        normalized["publisher_identity"] = _validate_publisher_identity(record.get("publisher_identity"))

    if service_type in LOCATION_REQUIRED_SERVICE_TYPES and "location" not in normalized:
        raise ValidationError(f"{service_type} services must include location.")

    return normalized


def _allowed_service_fields(protocol_version: str, allow_server_fields: bool) -> set[str]:
    allowed = set(BASE_SERVICE_FIELDS)
    if protocol_version == "2.0":
        allowed |= V2_SERVICE_FIELDS
    if allow_server_fields:
        allowed |= SERVER_MANAGED_FIELDS
    return allowed


def _allowed_update_fields(protocol_version: str) -> set[str]:
    allowed = set(BASE_UPDATE_FIELDS)
    if protocol_version == "2.0":
        allowed |= V2_UPDATE_FIELDS
    return allowed


def _require_protocol_version(request: dict, protocol_version: str) -> None:
    version = _validate_string(request.get("protocol_version"), "protocol_version")
    if version != protocol_version:
        raise ValidationError("Unsupported protocol_version.", {"expected": protocol_version, "received": version})


def _validate_filters(value: object, protocol_version: str) -> dict:
    filters = _require_mapping(value if value is not None else {}, "filters")
    if not filters:
        return {}

    unknown_filter_fields = sorted(set(filters) - FILTER_FIELDS)
    if unknown_filter_fields:
        raise ValidationError("filters includes unsupported fields.", {"fields": unknown_filter_fields})

    normalized_filters: dict = {}
    if "service_type" in filters:
        normalized_filters["service_type"] = _validate_service_type(filters["service_type"], protocol_version)
    if "status" in filters:
        normalized_filters["status"] = _validate_status(filters["status"])
    if "service_ids" in filters:
        normalized_filters["service_ids"] = [
            _validate_uuid_string(item, "service_ids item")
            for item in _validate_string_list(filters["service_ids"], "service_ids", allow_empty=False)
        ]
    if "tags_all" in filters:
        normalized_filters["tags_all"] = _validate_string_list(filters["tags_all"], "tags_all", allow_empty=False)
    if "name_contains" in filters:
        normalized_filters["name_contains"] = _validate_string(filters["name_contains"], "name_contains")
    if "location" in filters:
        location_filter = _require_mapping(filters["location"], "filters.location")
        unknown_location_fields = sorted(set(location_filter) - {"site", "area"})
        if unknown_location_fields:
            raise ValidationError("filters.location includes unsupported fields.", {"fields": unknown_location_fields})
        normalized_filters["location"] = {
            key: _validate_string(item_value, f"filters.location.{key}")
            for key, item_value in location_filter.items()
        }
    if "capabilities" in filters:
        capability_filters = _require_mapping(filters["capabilities"], "filters.capabilities")
        normalized_capability_filters = {}
        for key, item_value in capability_filters.items():
            if not isinstance(item_value, (str, int, float, bool)):
                raise ValidationError("filters.capabilities values must be scalar.", {"field": key})
            normalized_capability_filters[key] = item_value
        normalized_filters["capabilities"] = normalized_capability_filters
    return normalized_filters


def _validate_criteria(value: object) -> dict:
    criteria = _require_mapping(value, "criteria")
    return _validate_criterion_node(criteria, "criteria")


def _validate_criterion_node(node: dict, field_name: str) -> dict:
    keys = set(node)
    composite_keys = keys & {"all", "any", "not"}
    if composite_keys:
        if len(keys) != 1:
            raise ValidationError(f"{field_name} composite nodes must contain exactly one key.")
        composite = next(iter(composite_keys))
        if composite in {"all", "any"}:
            if not isinstance(node[composite], list) or not node[composite]:
                raise ValidationError(f"{field_name}.{composite} must be a non-empty array.")
            return {
                composite: [
                    _validate_criterion_node(item, f"{field_name}.{composite}[{index}]")
                    for index, item in enumerate(node[composite])
                ]
            }
        return {"not": _validate_criterion_node(node["not"], f"{field_name}.not")}

    unknown_fields = sorted(keys - {"field", "op", "value"})
    if unknown_fields:
        raise ValidationError(f"{field_name} includes unsupported fields.", {"fields": unknown_fields})
    if keys != {"field", "op", "value"}:
        raise ValidationError(f"{field_name} predicate nodes must include field, op, and value.")

    field = _validate_string(node.get("field"), f"{field_name}.field")
    op = _validate_string(node.get("op"), f"{field_name}.op")
    if op not in CRITERIA_OPERATORS:
        raise ValidationError(f"{field_name}.op is unsupported.", {"op": op})

    raw_value = node.get("value")
    if op == "exists":
        if not isinstance(raw_value, bool):
            raise ValidationError(f"{field_name}.value must be a boolean for exists.")
        value = raw_value
    elif op == "in":
        if not isinstance(raw_value, list) or not raw_value:
            raise ValidationError(f"{field_name}.value must be a non-empty array for in.")
        value = [_validate_scalar(item, f"{field_name}.value item") for item in raw_value]
    else:
        value = _validate_scalar(raw_value, f"{field_name}.value")

    return {"field": field, "op": op, "value": value}


def _validate_service_type(value: object, protocol_version: str) -> str:
    service_type = _validate_string(value, "service_type")
    if service_type not in _service_types_for_protocol(protocol_version):
        raise ValidationError("Unsupported service_type.", {"service_type": service_type})
    return service_type


def _validate_status(value: object) -> str:
    status = _validate_string(value, "status")
    if status not in STATUS_VALUES:
        raise ValidationError("Unsupported status.", {"status": status})
    return status


def _validate_endpoints(value: object) -> list[dict]:
    if not isinstance(value, list) or not value:
        raise ValidationError("endpoints must be a non-empty array.")

    endpoints = []
    for index, item in enumerate(value):
        endpoint = _require_mapping(item, f"endpoints[{index}]")
        unknown_fields = sorted(set(endpoint) - {"protocol", "address", "port", "path", "url", "secure"})
        if unknown_fields:
            raise ValidationError("endpoint includes unsupported fields.", {"fields": unknown_fields})

        normalized = {"protocol": _validate_string(endpoint.get("protocol"), f"endpoints[{index}].protocol")}
        if "address" in endpoint:
            normalized["address"] = _validate_string(endpoint.get("address"), f"endpoints[{index}].address")
        if "url" in endpoint:
            normalized["url"] = _validate_string(endpoint.get("url"), f"endpoints[{index}].url")
        if "port" in endpoint:
            normalized["port"] = _validate_integer(endpoint.get("port"), f"endpoints[{index}].port", minimum=1, maximum=65535)
        if "path" in endpoint:
            normalized["path"] = _validate_optional_string(endpoint.get("path"), f"endpoints[{index}].path")
        if "secure" in endpoint:
            if not isinstance(endpoint["secure"], bool):
                raise ValidationError("endpoint secure must be a boolean.", {"index": index})
            normalized["secure"] = endpoint["secure"]
        if "address" not in normalized and "url" not in normalized:
            raise ValidationError("Each endpoint must include either address or url.", {"index": index})
        endpoints.append(normalized)
    return endpoints


def _validate_capabilities(service_type: str, value: object) -> dict:
    capabilities = _require_mapping(value, "capabilities")
    if service_type == "printer":
        return _validate_printer_capabilities(capabilities)
    if service_type == "camera":
        return _validate_camera_capabilities(capabilities)
    if service_type == "api":
        return _validate_api_capabilities(capabilities)
    if service_type == "database":
        return _validate_database_capabilities(capabilities)
    if service_type == "ai_model_endpoint":
        return _validate_ai_model_endpoint_capabilities(capabilities)
    if service_type == "storage":
        return _validate_storage_capabilities(capabilities)
    if service_type == "message_broker":
        return _validate_message_broker_capabilities(capabilities)
    if service_type == "sensor":
        return _validate_sensor_capabilities(capabilities)
    raise ValidationError("Unsupported service_type.", {"service_type": service_type})


def _validate_printer_capabilities(capabilities: dict) -> dict:
    unknown_fields = sorted(set(capabilities) - {"color", "duplex", "supported_paper_sizes", "print_protocols", "queue_name", "manufacturer", "model", "max_resolution_dpi"})
    if unknown_fields:
        raise ValidationError("printer capabilities include unsupported fields.", {"fields": unknown_fields})

    normalized = {
        "color": _validate_boolean(capabilities.get("color"), "capabilities.color"),
        "duplex": _validate_boolean(capabilities.get("duplex"), "capabilities.duplex"),
        "supported_paper_sizes": _validate_string_list(capabilities.get("supported_paper_sizes"), "capabilities.supported_paper_sizes", allow_empty=False),
        "print_protocols": _validate_string_list(capabilities.get("print_protocols"), "capabilities.print_protocols", allow_empty=False),
    }
    normalized.update(_optional_capability_strings(capabilities, {"queue_name", "manufacturer", "model"}))
    if "max_resolution_dpi" in capabilities:
        normalized["max_resolution_dpi"] = _validate_integer(capabilities.get("max_resolution_dpi"), "capabilities.max_resolution_dpi", minimum=1)
    return normalized


def _validate_camera_capabilities(capabilities: dict) -> dict:
    unknown_fields = sorted(set(capabilities) - {"stream_protocols", "resolution", "night_vision", "ptz", "thermal", "frame_rate", "manufacturer", "model"})
    if unknown_fields:
        raise ValidationError("camera capabilities include unsupported fields.", {"fields": unknown_fields})

    normalized = {
        "stream_protocols": _validate_string_list(capabilities.get("stream_protocols"), "capabilities.stream_protocols", allow_empty=False),
        "resolution": _validate_string(capabilities.get("resolution"), "capabilities.resolution"),
        "night_vision": _validate_boolean(capabilities.get("night_vision"), "capabilities.night_vision"),
        "ptz": _validate_boolean(capabilities.get("ptz"), "capabilities.ptz"),
    }
    normalized.update(_optional_capability_strings(capabilities, {"manufacturer", "model"}))
    if "thermal" in capabilities:
        normalized["thermal"] = _validate_boolean(capabilities.get("thermal"), "capabilities.thermal")
    if "frame_rate" in capabilities:
        normalized["frame_rate"] = _validate_number(capabilities.get("frame_rate"), "capabilities.frame_rate", minimum=0)
    return normalized


def _validate_api_capabilities(capabilities: dict) -> dict:
    unknown_fields = sorted(set(capabilities) - {"base_url", "supported_protocols", "auth_type", "version", "health_endpoint", "documentation_url", "rate_limit_hint", "capability_tags"})
    if unknown_fields:
        raise ValidationError("api capabilities include unsupported fields.", {"fields": unknown_fields})

    auth_type = _validate_string(capabilities.get("auth_type"), "capabilities.auth_type")
    if auth_type not in AUTH_TYPES:
        raise ValidationError("Unsupported api auth_type.", {"auth_type": auth_type})

    normalized = {
        "base_url": _validate_string(capabilities.get("base_url"), "capabilities.base_url"),
        "supported_protocols": _validate_string_list(capabilities.get("supported_protocols"), "capabilities.supported_protocols", allow_empty=False),
        "auth_type": auth_type,
        "version": _validate_string(capabilities.get("version"), "capabilities.version"),
    }
    for key in {"health_endpoint", "documentation_url", "rate_limit_hint"}:
        if key in capabilities:
            normalized[key] = _validate_optional_string(capabilities.get(key), f"capabilities.{key}")
    if "capability_tags" in capabilities:
        normalized["capability_tags"] = _validate_string_list(capabilities.get("capability_tags"), "capabilities.capability_tags", allow_empty=True)
    return normalized


def _validate_database_capabilities(capabilities: dict) -> dict:
    unknown_fields = sorted(
        set(capabilities)
        - {
            "engine",
            "version",
            "role",
            "supports_tls",
            "database_name",
            "read_only",
            "replication_mode",
        }
    )
    if unknown_fields:
        raise ValidationError("database capabilities include unsupported fields.", {"fields": unknown_fields})

    normalized = {
        "engine": _validate_string(capabilities.get("engine"), "capabilities.engine"),
        "version": _validate_string(capabilities.get("version"), "capabilities.version"),
        "role": _validate_string(capabilities.get("role"), "capabilities.role"),
        "supports_tls": _validate_boolean(capabilities.get("supports_tls"), "capabilities.supports_tls"),
        "database_name": _validate_string(capabilities.get("database_name"), "capabilities.database_name"),
        "read_only": _validate_boolean(capabilities.get("read_only"), "capabilities.read_only"),
    }
    if "replication_mode" in capabilities:
        normalized["replication_mode"] = _validate_optional_string(
            capabilities.get("replication_mode"),
            "capabilities.replication_mode",
        )
    return normalized


def _validate_ai_model_endpoint_capabilities(capabilities: dict) -> dict:
    unknown_fields = sorted(
        set(capabilities)
        - {
            "model_name",
            "model_family",
            "modalities",
            "supports_streaming",
            "context_window",
            "auth_type",
            "provider_kind",
        }
    )
    if unknown_fields:
        raise ValidationError("ai_model_endpoint capabilities include unsupported fields.", {"fields": unknown_fields})

    normalized = {
        "model_name": _validate_string(capabilities.get("model_name"), "capabilities.model_name"),
        "model_family": _validate_string(capabilities.get("model_family"), "capabilities.model_family"),
        "modalities": _validate_string_list(capabilities.get("modalities"), "capabilities.modalities", allow_empty=False),
        "supports_streaming": _validate_boolean(capabilities.get("supports_streaming"), "capabilities.supports_streaming"),
        "context_window": _validate_integer(capabilities.get("context_window"), "capabilities.context_window", minimum=1),
        "auth_type": _validate_supported_auth_type(capabilities.get("auth_type"), "capabilities.auth_type"),
        "provider_kind": _validate_string(capabilities.get("provider_kind"), "capabilities.provider_kind"),
    }
    return normalized


def _validate_storage_capabilities(capabilities: dict) -> dict:
    unknown_fields = sorted(
        set(capabilities)
        - {
            "storage_kind",
            "protocols",
            "supports_versioning",
            "supports_encryption",
            "region",
            "bucket_or_share",
        }
    )
    if unknown_fields:
        raise ValidationError("storage capabilities include unsupported fields.", {"fields": unknown_fields})

    normalized = {
        "storage_kind": _validate_string(capabilities.get("storage_kind"), "capabilities.storage_kind"),
        "protocols": _validate_string_list(capabilities.get("protocols"), "capabilities.protocols", allow_empty=False),
        "supports_versioning": _validate_boolean(capabilities.get("supports_versioning"), "capabilities.supports_versioning"),
        "supports_encryption": _validate_boolean(capabilities.get("supports_encryption"), "capabilities.supports_encryption"),
        "region": _validate_string(capabilities.get("region"), "capabilities.region"),
    }
    if "bucket_or_share" in capabilities:
        normalized["bucket_or_share"] = _validate_optional_string(
            capabilities.get("bucket_or_share"),
            "capabilities.bucket_or_share",
        )
    return normalized


def _validate_message_broker_capabilities(capabilities: dict) -> dict:
    unknown_fields = sorted(
        set(capabilities)
        - {
            "broker_kind",
            "protocols",
            "supports_persistence",
            "supports_tls",
            "tenant_scope",
            "ordering_mode",
        }
    )
    if unknown_fields:
        raise ValidationError("message_broker capabilities include unsupported fields.", {"fields": unknown_fields})

    normalized = {
        "broker_kind": _validate_string(capabilities.get("broker_kind"), "capabilities.broker_kind"),
        "protocols": _validate_string_list(capabilities.get("protocols"), "capabilities.protocols", allow_empty=False),
        "supports_persistence": _validate_boolean(capabilities.get("supports_persistence"), "capabilities.supports_persistence"),
        "supports_tls": _validate_boolean(capabilities.get("supports_tls"), "capabilities.supports_tls"),
        "tenant_scope": _validate_string(capabilities.get("tenant_scope"), "capabilities.tenant_scope"),
        "ordering_mode": _validate_string(capabilities.get("ordering_mode"), "capabilities.ordering_mode"),
    }
    return normalized


def _validate_sensor_capabilities(capabilities: dict) -> dict:
    unknown_fields = sorted(
        set(capabilities)
        - {
            "sensor_kind",
            "measurement_types",
            "sampling_interval_ms",
            "units",
            "battery_powered",
            "location_scope",
        }
    )
    if unknown_fields:
        raise ValidationError("sensor capabilities include unsupported fields.", {"fields": unknown_fields})

    normalized = {
        "sensor_kind": _validate_string(capabilities.get("sensor_kind"), "capabilities.sensor_kind"),
        "measurement_types": _validate_string_list(
            capabilities.get("measurement_types"),
            "capabilities.measurement_types",
            allow_empty=False,
        ),
        "sampling_interval_ms": _validate_integer(
            capabilities.get("sampling_interval_ms"),
            "capabilities.sampling_interval_ms",
            minimum=1,
        ),
        "units": _validate_string(capabilities.get("units"), "capabilities.units"),
        "battery_powered": _validate_boolean(capabilities.get("battery_powered"), "capabilities.battery_powered"),
        "location_scope": _validate_string(capabilities.get("location_scope"), "capabilities.location_scope"),
    }
    return normalized


def _validate_auth(value: object) -> dict:
    auth = _require_mapping(value, "auth")
    unknown_fields = sorted(set(auth) - {"required", "type", "details"})
    if unknown_fields:
        raise ValidationError("auth includes unsupported fields.", {"fields": unknown_fields})
    if not isinstance(auth.get("required"), bool):
        raise ValidationError("auth.required must be a boolean.")
    auth_type = _validate_supported_auth_type(auth.get("type"), "auth.type")
    normalized = {"required": auth["required"], "type": auth_type}
    if "details" in auth:
        normalized["details"] = deepcopy(_require_mapping(auth.get("details"), "auth.details"))
    return normalized


def _validate_publisher(value: object) -> dict:
    publisher = _require_mapping(value, "publisher")
    unknown_fields = sorted(set(publisher) - {"publisher_id", "publisher_type", "publisher_name"})
    if unknown_fields:
        raise ValidationError("publisher includes unsupported fields.", {"fields": unknown_fields})

    publisher_type = _validate_string(publisher.get("publisher_type"), "publisher.publisher_type")
    if publisher_type not in {"service", "agent"}:
        raise ValidationError("Unsupported publisher_type.", {"publisher_type": publisher_type})

    normalized = {
        "publisher_type": publisher_type,
        "publisher_name": _validate_string(publisher.get("publisher_name"), "publisher.publisher_name"),
    }
    if "publisher_id" in publisher:
        normalized["publisher_id"] = _validate_optional_string(publisher.get("publisher_id"), "publisher.publisher_id")
    return normalized


def _validate_supported_auth_type(value: object, field_name: str) -> str:
    auth_type = _validate_string(value, field_name)
    if auth_type not in AUTH_TYPES:
        raise ValidationError(f"Unsupported {field_name}.", {"auth_type": auth_type})
    return auth_type


def _validate_publisher_identity(value: object) -> dict:
    publisher_identity = _require_mapping(value, "publisher_identity")
    unknown_fields = sorted(
        set(publisher_identity) - {"publisher_id", "publisher_name", "identity_type", "authenticated", "asserted_by"}
    )
    if unknown_fields:
        raise ValidationError("publisher_identity includes unsupported fields.", {"fields": unknown_fields})

    identity_type = _validate_string(publisher_identity.get("identity_type"), "publisher_identity.identity_type")
    if identity_type not in IDENTITY_TYPES:
        raise ValidationError("Unsupported publisher_identity.identity_type.", {"identity_type": identity_type})
    if not isinstance(publisher_identity.get("authenticated"), bool):
        raise ValidationError("publisher_identity.authenticated must be a boolean.")

    return {
        "publisher_id": _validate_string(publisher_identity.get("publisher_id"), "publisher_identity.publisher_id"),
        "publisher_name": _validate_string(publisher_identity.get("publisher_name"), "publisher_identity.publisher_name"),
        "identity_type": identity_type,
        "authenticated": publisher_identity["authenticated"],
        "asserted_by": _validate_string(publisher_identity.get("asserted_by"), "publisher_identity.asserted_by"),
    }


def _validate_provenance(value: object) -> dict:
    provenance = _require_mapping(value, "provenance")
    unknown_fields = sorted(
        set(provenance) - {"source_kind", "observed_at", "collected_by", "source_registry", "source_service_id", "discovery_method", "hops"}
    )
    if unknown_fields:
        raise ValidationError("provenance includes unsupported fields.", {"fields": unknown_fields})

    source_kind = _validate_string(provenance.get("source_kind"), "provenance.source_kind")
    if source_kind not in PROVENANCE_SOURCE_KINDS:
        raise ValidationError("Unsupported provenance.source_kind.", {"source_kind": source_kind})

    normalized = {"source_kind": source_kind}
    if "observed_at" in provenance:
        observed_at = _validate_string(provenance.get("observed_at"), "provenance.observed_at")
        parse_datetime(observed_at)
        normalized["observed_at"] = observed_at
    if "collected_by" in provenance:
        normalized["collected_by"] = _validate_string(provenance.get("collected_by"), "provenance.collected_by")
    if "source_registry" in provenance:
        normalized["source_registry"] = _validate_string(provenance.get("source_registry"), "provenance.source_registry")
    if "source_service_id" in provenance:
        normalized["source_service_id"] = _validate_uuid_string(
            provenance.get("source_service_id"),
            "provenance.source_service_id",
        )
    if "discovery_method" in provenance:
        normalized["discovery_method"] = _validate_string(
            provenance.get("discovery_method"),
            "provenance.discovery_method",
        )
    if "hops" in provenance:
        normalized["hops"] = _validate_integer(provenance.get("hops"), "provenance.hops", minimum=0)
    return normalized


def _validate_location(value: object) -> dict:
    location = _require_mapping(value, "location")
    unknown_fields = sorted(set(location) - {"site", "area", "description", "coordinates"})
    if unknown_fields:
        raise ValidationError("location includes unsupported fields.", {"fields": unknown_fields})

    normalized = {}
    for key in {"site", "area", "description"}:
        if key in location:
            normalized[key] = _validate_optional_string(location.get(key), f"location.{key}")

    if "coordinates" in location:
        coordinates = _require_mapping(location.get("coordinates"), "location.coordinates")
        unknown_coordinate_fields = sorted(set(coordinates) - {"latitude", "longitude"})
        if unknown_coordinate_fields:
            raise ValidationError("location.coordinates includes unsupported fields.", {"fields": unknown_coordinate_fields})
        normalized["coordinates"] = {
            "latitude": _validate_number(coordinates.get("latitude"), "location.coordinates.latitude"),
            "longitude": _validate_number(coordinates.get("longitude"), "location.coordinates.longitude"),
        }
    return normalized


def _validate_timestamps(value: object) -> dict:
    timestamps = _require_mapping(value, "timestamps")
    unknown_fields = sorted(set(timestamps) - {"registered_at", "updated_at", "last_heartbeat_at", "expires_at"})
    if unknown_fields:
        raise ValidationError("timestamps includes unsupported fields.", {"fields": unknown_fields})

    normalized = {}
    for key in ("registered_at", "updated_at", "last_heartbeat_at", "expires_at"):
        timestamp_text = _validate_string(timestamps.get(key), f"timestamps.{key}")
        parse_datetime(timestamp_text)
        normalized[key] = timestamp_text
    return normalized


def _validate_sort(value: object, protocol_version: str) -> list[dict]:
    if not isinstance(value, list):
        raise ValidationError("sort must be an array.")
    allowed_sort_fields = V2_SORT_FIELDS if protocol_version == "2.0" else V1_SORT_FIELDS
    normalized = []
    for index, item in enumerate(value):
        rule = _require_mapping(item, f"sort[{index}]")
        field = _validate_string(rule.get("field"), f"sort[{index}].field")
        direction = _validate_string(rule.get("direction"), f"sort[{index}].direction")
        if field not in allowed_sort_fields:
            raise ValidationError("sort field is unsupported.", {"field": field})
        if direction not in {"asc", "desc"}:
            raise ValidationError("sort direction must be asc or desc.", {"direction": direction})
        normalized.append({"field": field, "direction": direction})
    return normalized


def _require_mapping(value: object, field_name: str) -> dict:
    if not isinstance(value, dict):
        raise ValidationError(f"{field_name} must be an object.")
    return value


def _validate_uuid_string(value: object, field_name: str) -> str:
    text = _validate_string(value, field_name)
    try:
        return str(UUID(text))
    except ValueError as exc:
        raise ValidationError(f"{field_name} must be a UUID string.") from exc


def _validate_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field_name} must be a non-empty string.")
    return value.strip()


def _validate_optional_string(value: object, field_name: str) -> str:
    if value is None or not isinstance(value, str):
        raise ValidationError(f"{field_name} must be a string.")
    return value


def _validate_string_list(value: object, field_name: str, *, allow_empty: bool) -> list[str]:
    if not isinstance(value, list):
        raise ValidationError(f"{field_name} must be an array.")
    if not value and not allow_empty:
        raise ValidationError(f"{field_name} must not be empty.")
    normalized = [_validate_string(item, f"{field_name} item") for item in value]
    if len(normalized) != len(set(normalized)):
        raise ValidationError(f"{field_name} must not contain duplicates.")
    return normalized


def _validate_boolean(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValidationError(f"{field_name} must be a boolean.")
    return value


def _validate_integer(value: object, field_name: str, *, minimum: int | None = None, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{field_name} must be an integer.")
    if minimum is not None and value < minimum:
        raise ValidationError(f"{field_name} must be >= {minimum}.")
    if maximum is not None and value > maximum:
        raise ValidationError(f"{field_name} must be <= {maximum}.")
    return value


def _validate_number(value: object, field_name: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{field_name} must be a number.")
    number = float(value)
    if minimum is not None and number < minimum:
        raise ValidationError(f"{field_name} must be >= {minimum}.")
    return number


def _validate_scalar(value: object, field_name: str):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ValidationError(f"{field_name} must be a scalar value.")


def _optional_capability_strings(capabilities: dict, fields: set[str]) -> dict:
    normalized = {}
    for field in fields:
        if field in capabilities:
            normalized[field] = _validate_optional_string(capabilities.get(field), f"capabilities.{field}")
    return normalized


def _service_types_for_protocol(protocol_version: str) -> set[str]:
    if protocol_version not in SERVICE_TYPES_BY_PROTOCOL:
        raise ValidationError("Unsupported protocol_version.", {"received": protocol_version})
    return SERVICE_TYPES_BY_PROTOCOL[protocol_version]
