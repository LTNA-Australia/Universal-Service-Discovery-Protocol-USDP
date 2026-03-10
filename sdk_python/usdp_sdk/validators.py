"""SDK-side validation wrappers."""

from __future__ import annotations

from .models import ServiceRecord
from .validation import validate_query_request, validate_service_record


def validate_service_payload(
    service: dict | ServiceRecord,
    *,
    protocol_version: str = "1.0",
    allow_timestamps: bool = False,
) -> dict:
    if isinstance(service, ServiceRecord):
        service = service.to_dict()
    return validate_service_record(service, allow_timestamps=allow_timestamps, protocol_version=protocol_version)


def validate_query_payload(query: dict, protocol_version: str = "1.0") -> dict:
    return validate_query_request(query, protocol_version)
