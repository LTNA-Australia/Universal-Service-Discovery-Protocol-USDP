"""Federation helpers for v2 registry imports."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime

from .lifecycle import apply_deregister, create_registered_record
from .utils import isoformat_z


def prepare_federated_record(service: dict, *, peer_registry: str, current_time: datetime, default_ttl_seconds: int) -> dict:
    record = deepcopy(service)
    if "timestamps" not in record:
        record = create_registered_record(record, current_time, default_ttl_seconds)

    provenance = dict(record.get("provenance", {}))
    provenance["source_kind"] = "federated_registry"
    provenance["source_registry"] = peer_registry
    provenance["observed_at"] = provenance.get("observed_at") or isoformat_z(current_time)
    provenance["hops"] = int(provenance.get("hops", 0)) + 1
    record["provenance"] = provenance
    return record


def apply_federated_withdrawal(existing_record: dict, *, reason: str | None, current_time: datetime, peer_registry: str) -> dict:
    record = apply_deregister(existing_record, reason, current_time)
    provenance = dict(record.get("provenance", {}))
    provenance["source_kind"] = "federated_registry"
    provenance["source_registry"] = peer_registry
    provenance["observed_at"] = isoformat_z(current_time)
    provenance["hops"] = int(provenance.get("hops", 0))
    record["provenance"] = provenance
    return record
