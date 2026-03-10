"""Standalone validation for the Python SDK."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from uuid import UUID


V1_SERVICE_TYPES = {"printer", "camera", "api"}
V2_SERVICE_TYPES = V1_SERVICE_TYPES | {"database", "ai_model_endpoint", "storage", "message_broker", "sensor"}
SERVICE_TYPES_BY_PROTOCOL = {"1.0": V1_SERVICE_TYPES, "2.0": V2_SERVICE_TYPES}
LOCATION_REQUIRED_SERVICE_TYPES = {"printer", "camera"}
STATUS_VALUES = {"online", "degraded", "offline", "unknown"}
AUTH_TYPES = {"none", "basic", "bearer", "apikey", "oauth2", "mtls", "other"}
IDENTITY_TYPES = {"bearer_token", "service_account", "mtls", "external"}
PROVENANCE_SOURCE_KINDS = {"publisher", "agent", "registry", "federated_registry", "external_import"}
BASE_SERVICE_FIELDS = {
    "service_id", "name", "description", "service_type", "status", "endpoints", "capabilities", "tags", "auth",
    "metadata", "publisher", "location", "heartbeat_ttl_seconds", "timestamps",
}
V2_SERVICE_FIELDS = {"provenance", "extensions"}
SERVER_MANAGED_FIELDS = {"publisher_identity"}
V1_QUERY_FIELDS = {"protocol_version", "filters", "page", "page_size", "sort", "include_inactive"}
V2_QUERY_FIELDS = V1_QUERY_FIELDS | {"criteria"}
FILTER_FIELDS = {"service_type", "status", "service_ids", "tags_all", "name_contains", "location", "capabilities"}
V1_SORT_FIELDS = {"name", "updated_at", "registered_at"}
V2_SORT_FIELDS = V1_SORT_FIELDS | {"service_type", "status", "last_heartbeat_at"}
CRITERIA_OPERATORS = {"eq", "neq", "in", "contains", "exists", "starts_with", "gte", "lte"}
CAPABILITY_SPECS = {
    "printer": {
        "required": {
            "color": ("bool",),
            "duplex": ("bool",),
            "supported_paper_sizes": ("str_list", False),
            "print_protocols": ("str_list", False),
        },
        "optional": {
            "queue_name": ("opt_str",),
            "manufacturer": ("opt_str",),
            "model": ("opt_str",),
            "max_resolution_dpi": ("int", 1, None),
        },
    },
    "camera": {
        "required": {
            "stream_protocols": ("str_list", False),
            "resolution": ("str",),
            "night_vision": ("bool",),
            "ptz": ("bool",),
        },
        "optional": {
            "thermal": ("bool",),
            "frame_rate": ("number", 0),
            "manufacturer": ("opt_str",),
            "model": ("opt_str",),
        },
    },
    "api": {
        "required": {
            "base_url": ("str",),
            "supported_protocols": ("str_list", False),
            "auth_type": ("auth_type",),
            "version": ("str",),
        },
        "optional": {
            "health_endpoint": ("opt_str",),
            "documentation_url": ("opt_str",),
            "rate_limit_hint": ("opt_str",),
            "capability_tags": ("str_list", True),
        },
    },
    "database": {
        "required": {
            "engine": ("str",),
            "version": ("str",),
            "role": ("str",),
            "supports_tls": ("bool",),
            "database_name": ("str",),
            "read_only": ("bool",),
        },
        "optional": {"replication_mode": ("opt_str",)},
    },
    "ai_model_endpoint": {
        "required": {
            "model_name": ("str",),
            "model_family": ("str",),
            "modalities": ("str_list", False),
            "supports_streaming": ("bool",),
            "context_window": ("int", 1, None),
            "auth_type": ("auth_type",),
            "provider_kind": ("str",),
        },
        "optional": {},
    },
    "storage": {
        "required": {
            "storage_kind": ("str",),
            "protocols": ("str_list", False),
            "supports_versioning": ("bool",),
            "supports_encryption": ("bool",),
            "region": ("str",),
        },
        "optional": {"bucket_or_share": ("opt_str",)},
    },
    "message_broker": {
        "required": {
            "broker_kind": ("str",),
            "protocols": ("str_list", False),
            "supports_persistence": ("bool",),
            "supports_tls": ("bool",),
            "tenant_scope": ("str",),
            "ordering_mode": ("str",),
        },
        "optional": {},
    },
    "sensor": {
        "required": {
            "sensor_kind": ("str",),
            "measurement_types": ("str_list", False),
            "sampling_interval_ms": ("int", 1, None),
            "units": ("str",),
            "battery_powered": ("bool",),
            "location_scope": ("str",),
        },
        "optional": {},
    },
}


class ValidationError(ValueError):
    def __init__(self, message: str, details: object | None = None) -> None:
        super().__init__(message)
        self.details = details


def validate_query_request(body: object, protocol_version: str) -> dict:
    request = _require_mapping(body, "Request body")
    _require_protocol_version(request, protocol_version)
    allowed_fields = V2_QUERY_FIELDS if protocol_version == "2.0" else V1_QUERY_FIELDS
    unknown_fields = sorted(set(request) - allowed_fields)
    if unknown_fields:
        raise ValidationError("Query request includes unsupported fields.", {"fields": unknown_fields})

    result = {"protocol_version": protocol_version, "filters": {}, "page": 1, "page_size": 25, "sort": [], "include_inactive": False}
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
        result["include_inactive"] = _validate_boolean(request["include_inactive"], "include_inactive")
    return result


def validate_service_record(service: object, *, allow_timestamps: bool, protocol_version: str = "1.0", allow_server_fields: bool = False) -> dict:
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
    for field in ("description",):
        if field in record:
            normalized[field] = _validate_optional_string(record.get(field), field)
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
        normalized["heartbeat_ttl_seconds"] = _validate_integer(record.get("heartbeat_ttl_seconds"), "heartbeat_ttl_seconds", minimum=30, maximum=300)
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


def _validate_filters(value: object, protocol_version: str) -> dict:
    filters = _require_mapping(value if value is not None else {}, "filters")
    unknown_fields = sorted(set(filters) - FILTER_FIELDS)
    if unknown_fields:
        raise ValidationError("filters includes unsupported fields.", {"fields": unknown_fields})
    normalized = {}
    if "service_type" in filters:
        normalized["service_type"] = _validate_service_type(filters["service_type"], protocol_version)
    if "status" in filters:
        normalized["status"] = _validate_status(filters["status"])
    if "service_ids" in filters:
        normalized["service_ids"] = [_validate_uuid_string(item, "service_ids item") for item in _validate_string_list(filters["service_ids"], "service_ids", allow_empty=False)]
    if "tags_all" in filters:
        normalized["tags_all"] = _validate_string_list(filters["tags_all"], "tags_all", allow_empty=False)
    if "name_contains" in filters:
        normalized["name_contains"] = _validate_string(filters["name_contains"], "name_contains")
    if "location" in filters:
        location = _require_mapping(filters["location"], "filters.location")
        bad_location = sorted(set(location) - {"site", "area"})
        if bad_location:
            raise ValidationError("filters.location includes unsupported fields.", {"fields": bad_location})
        normalized["location"] = {key: _validate_string(item_value, f"filters.location.{key}") for key, item_value in location.items()}
    if "capabilities" in filters:
        capabilities = _require_mapping(filters["capabilities"], "filters.capabilities")
        for key, item_value in capabilities.items():
            if not isinstance(item_value, (str, int, float, bool)):
                raise ValidationError("filters.capabilities values must be scalar.", {"field": key})
        normalized["capabilities"] = dict(capabilities)
    return normalized


def _validate_criteria(value: object) -> dict:
    criteria = _require_mapping(value, "criteria")
    keys = set(criteria)
    composite = keys & {"all", "any", "not"}
    if composite:
        if len(keys) != 1:
            raise ValidationError("criteria composite nodes must contain exactly one key.")
        key = next(iter(composite))
        if key in {"all", "any"}:
            nodes = criteria[key]
            if not isinstance(nodes, list) or not nodes:
                raise ValidationError(f"criteria.{key} must be a non-empty array.")
            return {key: [_validate_criteria(item) for item in nodes]}
        return {"not": _validate_criteria(criteria["not"])}
    if keys != {"field", "op", "value"}:
        raise ValidationError("criteria predicate nodes must include field, op, and value.")
    field = _validate_string(criteria.get("field"), "criteria.field")
    op = _validate_string(criteria.get("op"), "criteria.op")
    if op not in CRITERIA_OPERATORS:
        raise ValidationError("criteria.op is unsupported.", {"op": op})
    raw_value = criteria.get("value")
    if op == "exists":
        value = _validate_boolean(raw_value, "criteria.value")
    elif op == "in":
        if not isinstance(raw_value, list) or not raw_value:
            raise ValidationError("criteria.value must be a non-empty array for in.")
        value = [_validate_scalar(item, "criteria.value item") for item in raw_value]
    else:
        value = _validate_scalar(raw_value, "criteria.value")
    return {"field": field, "op": op, "value": value}


def _validate_service_type(value: object, protocol_version: str) -> str:
    service_type = _validate_string(value, "service_type")
    if service_type not in SERVICE_TYPES_BY_PROTOCOL.get(protocol_version, set()):
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
            normalized["secure"] = _validate_boolean(endpoint.get("secure"), f"endpoints[{index}].secure")
        if "address" not in normalized and "url" not in normalized:
            raise ValidationError("Each endpoint must include either address or url.", {"index": index})
        endpoints.append(normalized)
    return endpoints


def _validate_capabilities(service_type: str, value: object) -> dict:
    capabilities = _require_mapping(value, "capabilities")
    spec = CAPABILITY_SPECS[service_type]
    unknown_fields = sorted(set(capabilities) - set(spec["required"]) - set(spec["optional"]))
    if unknown_fields:
        raise ValidationError(f"{service_type} capabilities include unsupported fields.", {"fields": unknown_fields})
    normalized = {field: _validate_typed_value(capabilities.get(field), f"capabilities.{field}", *rule) for field, rule in spec["required"].items()}
    for field, rule in spec["optional"].items():
        if field in capabilities:
            normalized[field] = _validate_typed_value(capabilities.get(field), f"capabilities.{field}", *rule)
    return normalized


def _validate_typed_value(value: object, field_name: str, rule: str, *args):
    if rule == "bool":
        return _validate_boolean(value, field_name)
    if rule == "str":
        return _validate_string(value, field_name)
    if rule == "opt_str":
        return _validate_optional_string(value, field_name)
    if rule == "auth_type":
        auth_type = _validate_string(value, field_name)
        if auth_type not in AUTH_TYPES:
            raise ValidationError(f"Unsupported {field_name}.", {"auth_type": auth_type})
        return auth_type
    if rule == "str_list":
        return _validate_string_list(value, field_name, allow_empty=args[0])
    if rule == "int":
        return _validate_integer(value, field_name, minimum=args[0], maximum=args[1])
    if rule == "number":
        return _validate_number(value, field_name, minimum=args[0])
    raise RuntimeError(f"Unsupported rule {rule}")


def _validate_auth(value: object) -> dict:
    auth = _require_mapping(value, "auth")
    unknown_fields = sorted(set(auth) - {"required", "type", "details"})
    if unknown_fields:
        raise ValidationError("auth includes unsupported fields.", {"fields": unknown_fields})
    normalized = {"required": _validate_boolean(auth.get("required"), "auth.required"), "type": _validate_typed_value(auth.get("type"), "auth.type", "auth_type")}
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
    normalized = {"publisher_type": publisher_type, "publisher_name": _validate_string(publisher.get("publisher_name"), "publisher.publisher_name")}
    if "publisher_id" in publisher:
        normalized["publisher_id"] = _validate_optional_string(publisher.get("publisher_id"), "publisher.publisher_id")
    return normalized


def _validate_publisher_identity(value: object) -> dict:
    identity = _require_mapping(value, "publisher_identity")
    unknown_fields = sorted(set(identity) - {"publisher_id", "publisher_name", "identity_type", "authenticated", "asserted_by"})
    if unknown_fields:
        raise ValidationError("publisher_identity includes unsupported fields.", {"fields": unknown_fields})
    identity_type = _validate_string(identity.get("identity_type"), "publisher_identity.identity_type")
    if identity_type not in IDENTITY_TYPES:
        raise ValidationError("Unsupported publisher_identity.identity_type.", {"identity_type": identity_type})
    return {
        "publisher_id": _validate_string(identity.get("publisher_id"), "publisher_identity.publisher_id"),
        "publisher_name": _validate_string(identity.get("publisher_name"), "publisher_identity.publisher_name"),
        "identity_type": identity_type,
        "authenticated": _validate_boolean(identity.get("authenticated"), "publisher_identity.authenticated"),
        "asserted_by": _validate_string(identity.get("asserted_by"), "publisher_identity.asserted_by"),
    }


def _validate_provenance(value: object) -> dict:
    provenance = _require_mapping(value, "provenance")
    unknown_fields = sorted(set(provenance) - {"source_kind", "observed_at", "collected_by", "source_registry", "source_service_id", "discovery_method", "hops"})
    if unknown_fields:
        raise ValidationError("provenance includes unsupported fields.", {"fields": unknown_fields})
    source_kind = _validate_string(provenance.get("source_kind"), "provenance.source_kind")
    if source_kind not in PROVENANCE_SOURCE_KINDS:
        raise ValidationError("Unsupported provenance.source_kind.", {"source_kind": source_kind})
    normalized = {"source_kind": source_kind}
    if "observed_at" in provenance:
        normalized["observed_at"] = _validate_datetime_string(provenance.get("observed_at"), "provenance.observed_at")
    if "collected_by" in provenance:
        normalized["collected_by"] = _validate_string(provenance.get("collected_by"), "provenance.collected_by")
    if "source_registry" in provenance:
        normalized["source_registry"] = _validate_string(provenance.get("source_registry"), "provenance.source_registry")
    if "source_service_id" in provenance:
        normalized["source_service_id"] = _validate_uuid_string(provenance.get("source_service_id"), "provenance.source_service_id")
    if "discovery_method" in provenance:
        normalized["discovery_method"] = _validate_string(provenance.get("discovery_method"), "provenance.discovery_method")
    if "hops" in provenance:
        normalized["hops"] = _validate_integer(provenance.get("hops"), "provenance.hops", minimum=0)
    return normalized


def _validate_location(value: object) -> dict:
    location = _require_mapping(value, "location")
    unknown_fields = sorted(set(location) - {"site", "area", "description", "coordinates"})
    if unknown_fields:
        raise ValidationError("location includes unsupported fields.", {"fields": unknown_fields})
    normalized = {}
    for key in ("site", "area", "description"):
        if key in location:
            normalized[key] = _validate_optional_string(location.get(key), f"location.{key}")
    if "coordinates" in location:
        coordinates = _require_mapping(location.get("coordinates"), "location.coordinates")
        unknown_coordinate_fields = sorted(set(coordinates) - {"latitude", "longitude"})
        if unknown_coordinate_fields:
            raise ValidationError("location.coordinates includes unsupported fields.", {"fields": unknown_coordinate_fields})
        normalized["coordinates"] = {"latitude": _validate_number(coordinates.get("latitude"), "location.coordinates.latitude"), "longitude": _validate_number(coordinates.get("longitude"), "location.coordinates.longitude")}
    return normalized


def _validate_timestamps(value: object) -> dict:
    timestamps = _require_mapping(value, "timestamps")
    unknown_fields = sorted(set(timestamps) - {"registered_at", "updated_at", "last_heartbeat_at", "expires_at"})
    if unknown_fields:
        raise ValidationError("timestamps includes unsupported fields.", {"fields": unknown_fields})
    return {key: _validate_datetime_string(timestamps.get(key), f"timestamps.{key}") for key in ("registered_at", "updated_at", "last_heartbeat_at", "expires_at")}


def _validate_sort(value: object, protocol_version: str) -> list[dict]:
    if not isinstance(value, list):
        raise ValidationError("sort must be an array.")
    allowed_fields = V2_SORT_FIELDS if protocol_version == "2.0" else V1_SORT_FIELDS
    normalized = []
    for index, item in enumerate(value):
        rule = _require_mapping(item, f"sort[{index}]")
        field = _validate_string(rule.get("field"), f"sort[{index}].field")
        direction = _validate_string(rule.get("direction"), f"sort[{index}].direction")
        if field not in allowed_fields:
            raise ValidationError("sort field is unsupported.", {"field": field})
        if direction not in {"asc", "desc"}:
            raise ValidationError("sort direction must be asc or desc.", {"direction": direction})
        normalized.append({"field": field, "direction": direction})
    return normalized


def _require_mapping(value: object, field_name: str) -> dict:
    if not isinstance(value, dict):
        raise ValidationError(f"{field_name} must be an object.")
    return value


def _require_protocol_version(request: dict, protocol_version: str) -> None:
    version = _validate_string(request.get("protocol_version"), "protocol_version")
    if version != protocol_version:
        raise ValidationError("Unsupported protocol_version.", {"expected": protocol_version, "received": version})


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


def _validate_datetime_string(value: object, field_name: str) -> str:
    text = _validate_string(value, field_name)
    _parse_datetime(text)
    return text


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
    if isinstance(value, bool) or isinstance(value, (int, float)):
        return value
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ValidationError(f"{field_name} must be a scalar value.")


def _parse_datetime(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)
