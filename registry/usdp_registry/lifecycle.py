"""Record lifecycle transitions."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime

from .utils import expiry_for, isoformat_z, parse_datetime


def create_registered_record(service: dict, current_time: datetime, default_ttl_seconds: int) -> dict:
    record = deepcopy(service)
    ttl_seconds = int(record.get("heartbeat_ttl_seconds", default_ttl_seconds))
    record["heartbeat_ttl_seconds"] = ttl_seconds
    expires_at = expiry_for(current_time, ttl_seconds)
    record["timestamps"] = {
        "registered_at": isoformat_z(current_time),
        "updated_at": isoformat_z(current_time),
        "last_heartbeat_at": isoformat_z(current_time),
        "expires_at": isoformat_z(expires_at),
    }
    return record


def apply_update(existing_record: dict, changes: dict, current_time: datetime) -> dict:
    record = deepcopy(existing_record)
    for key, value in changes.items():
        record[key] = value

    ttl_seconds = int(record.get("heartbeat_ttl_seconds", 90))
    last_heartbeat = parse_datetime(record["timestamps"]["last_heartbeat_at"])
    record["timestamps"]["updated_at"] = isoformat_z(current_time)
    record["timestamps"]["expires_at"] = isoformat_z(expiry_for(last_heartbeat, ttl_seconds))
    return record


def apply_heartbeat(existing_record: dict, status: str | None, current_time: datetime) -> dict:
    record = deepcopy(existing_record)
    if status is not None:
        record["status"] = status

    ttl_seconds = int(record.get("heartbeat_ttl_seconds", 90))
    record["timestamps"]["last_heartbeat_at"] = isoformat_z(current_time)
    record["timestamps"]["updated_at"] = isoformat_z(current_time)
    record["timestamps"]["expires_at"] = isoformat_z(expiry_for(current_time, ttl_seconds))
    return record


def apply_deregister(existing_record: dict, reason: str | None, current_time: datetime) -> dict:
    record = deepcopy(existing_record)
    record["status"] = "offline"
    record["timestamps"]["updated_at"] = isoformat_z(current_time)
    record["timestamps"]["expires_at"] = isoformat_z(current_time)

    if reason:
        metadata = dict(record.get("metadata", {}))
        metadata["deregister_reason"] = reason
        record["metadata"] = metadata

    return record
