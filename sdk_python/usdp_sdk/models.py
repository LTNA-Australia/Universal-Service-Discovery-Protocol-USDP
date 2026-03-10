"""Typed models for the Python SDK."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _deep_copy_dict(value: dict[str, Any] | None) -> dict[str, Any] | None:
    return dict(value) if value is not None else None


@dataclass(slots=True)
class Endpoint:
    protocol: str
    url: str | None = None
    address: str | None = None
    port: int | None = None
    path: str | None = None
    secure: bool | None = None

    def to_dict(self) -> dict:
        data = {"protocol": self.protocol}
        if self.url is not None:
            data["url"] = self.url
        if self.address is not None:
            data["address"] = self.address
        if self.port is not None:
            data["port"] = self.port
        if self.path is not None:
            data["path"] = self.path
        if self.secure is not None:
            data["secure"] = self.secure
        return data


@dataclass(slots=True)
class AuthRequirement:
    required: bool
    type: str
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict:
        data = {"required": self.required, "type": self.type}
        if self.details is not None:
            data["details"] = dict(self.details)
        return data


@dataclass(slots=True)
class PublisherInfo:
    publisher_type: str
    publisher_name: str
    publisher_id: str | None = None

    def to_dict(self) -> dict:
        data = {
            "publisher_type": self.publisher_type,
            "publisher_name": self.publisher_name,
        }
        if self.publisher_id is not None:
            data["publisher_id"] = self.publisher_id
        return data


@dataclass(slots=True)
class PublisherIdentity:
    publisher_id: str
    publisher_name: str
    identity_type: str
    authenticated: bool
    asserted_by: str

    def to_dict(self) -> dict:
        return {
            "publisher_id": self.publisher_id,
            "publisher_name": self.publisher_name,
            "identity_type": self.identity_type,
            "authenticated": self.authenticated,
            "asserted_by": self.asserted_by,
        }


@dataclass(slots=True)
class Provenance:
    source_kind: str
    observed_at: str | None = None
    collected_by: str | None = None
    source_registry: str | None = None
    source_service_id: str | None = None
    discovery_method: str | None = None
    hops: int | None = None

    def to_dict(self) -> dict:
        data: dict[str, Any] = {"source_kind": self.source_kind}
        if self.observed_at is not None:
            data["observed_at"] = self.observed_at
        if self.collected_by is not None:
            data["collected_by"] = self.collected_by
        if self.source_registry is not None:
            data["source_registry"] = self.source_registry
        if self.source_service_id is not None:
            data["source_service_id"] = self.source_service_id
        if self.discovery_method is not None:
            data["discovery_method"] = self.discovery_method
        if self.hops is not None:
            data["hops"] = self.hops
        return data


@dataclass(slots=True)
class Location:
    site: str | None = None
    area: str | None = None
    description: str | None = None
    coordinates: dict[str, float] | None = None

    def to_dict(self) -> dict:
        data: dict[str, Any] = {}
        if self.site is not None:
            data["site"] = self.site
        if self.area is not None:
            data["area"] = self.area
        if self.description is not None:
            data["description"] = self.description
        if self.coordinates is not None:
            data["coordinates"] = dict(self.coordinates)
        return data


@dataclass(slots=True)
class QuerySort:
    field: str
    direction: str

    def to_dict(self) -> dict:
        return {"field": self.field, "direction": self.direction}


@dataclass(slots=True)
class QueryCriterion:
    node: dict[str, Any]

    def to_dict(self) -> dict:
        return dict(self.node)

    @classmethod
    def predicate(cls, field: str, op: str, value: Any) -> "QueryCriterion":
        return cls({"field": field, "op": op, "value": value})

    @classmethod
    def all_of(cls, *criteria: "QueryCriterion | dict[str, Any]") -> "QueryCriterion":
        return cls({"all": [_criterion_to_dict(item) for item in criteria]})

    @classmethod
    def any_of(cls, *criteria: "QueryCriterion | dict[str, Any]") -> "QueryCriterion":
        return cls({"any": [_criterion_to_dict(item) for item in criteria]})

    @classmethod
    def not_(cls, criterion: "QueryCriterion | dict[str, Any]") -> "QueryCriterion":
        return cls({"not": _criterion_to_dict(criterion)})


@dataclass(slots=True)
class QueryRequest:
    protocol_version: str = "2.0"
    filters: dict[str, Any] | None = None
    criteria: QueryCriterion | dict[str, Any] | None = None
    page: int = 1
    page_size: int = 25
    sort: list[QuerySort | dict[str, Any]] = field(default_factory=list)
    include_inactive: bool = False

    def to_dict(self) -> dict:
        payload: dict[str, Any] = {"protocol_version": self.protocol_version}
        if self.filters:
            payload["filters"] = dict(self.filters)
        if self.criteria is not None:
            payload["criteria"] = _criterion_to_dict(self.criteria)
        if self.page != 1:
            payload["page"] = self.page
        if self.page_size != 25:
            payload["page_size"] = self.page_size
        if self.sort:
            payload["sort"] = [item.to_dict() if isinstance(item, QuerySort) else dict(item) for item in self.sort]
        if self.include_inactive:
            payload["include_inactive"] = self.include_inactive
        return payload


@dataclass(slots=True)
class ServiceRecord:
    service_id: str
    name: str
    service_type: str
    status: str
    endpoints: list[Endpoint]
    capabilities: dict[str, Any]
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    auth: AuthRequirement | None = None
    metadata: dict[str, Any] | None = None
    publisher: PublisherInfo | None = None
    publisher_identity: PublisherIdentity | None = None
    provenance: Provenance | None = None
    extensions: dict[str, Any] | None = None
    location: Location | None = None
    heartbeat_ttl_seconds: int | None = None
    timestamps: dict[str, Any] | None = None

    def to_dict(self) -> dict:
        data: dict[str, Any] = {
            "service_id": self.service_id,
            "name": self.name,
            "service_type": self.service_type,
            "status": self.status,
            "endpoints": [endpoint.to_dict() for endpoint in self.endpoints],
            "capabilities": dict(self.capabilities),
        }
        if self.description is not None:
            data["description"] = self.description
        if self.tags:
            data["tags"] = list(self.tags)
        if self.auth is not None:
            data["auth"] = self.auth.to_dict()
        if self.metadata is not None:
            data["metadata"] = dict(self.metadata)
        if self.publisher is not None:
            data["publisher"] = self.publisher.to_dict()
        if self.publisher_identity is not None:
            data["publisher_identity"] = self.publisher_identity.to_dict()
        if self.provenance is not None:
            data["provenance"] = self.provenance.to_dict()
        if self.extensions is not None:
            data["extensions"] = dict(self.extensions)
        if self.location is not None:
            data["location"] = self.location.to_dict()
        if self.heartbeat_ttl_seconds is not None:
            data["heartbeat_ttl_seconds"] = self.heartbeat_ttl_seconds
        if self.timestamps is not None:
            data["timestamps"] = dict(self.timestamps)
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "ServiceRecord":
        return cls(
            service_id=data["service_id"],
            name=data["name"],
            service_type=data["service_type"],
            status=data["status"],
            endpoints=[Endpoint(**endpoint) for endpoint in data["endpoints"]],
            capabilities=dict(data["capabilities"]),
            description=data.get("description"),
            tags=list(data.get("tags", [])),
            auth=AuthRequirement(**data["auth"]) if data.get("auth") else None,
            metadata=_deep_copy_dict(data.get("metadata")),
            publisher=PublisherInfo(**data["publisher"]) if data.get("publisher") else None,
            publisher_identity=PublisherIdentity(**data["publisher_identity"]) if data.get("publisher_identity") else None,
            provenance=Provenance(**data["provenance"]) if data.get("provenance") else None,
            extensions=_deep_copy_dict(data.get("extensions")),
            location=Location(**data["location"]) if data.get("location") else None,
            heartbeat_ttl_seconds=data.get("heartbeat_ttl_seconds"),
            timestamps=_deep_copy_dict(data.get("timestamps")),
        )


@dataclass(slots=True)
class QueryResultPage:
    items: list[ServiceRecord]
    count: int
    page: int
    page_size: int
    total: int

    @classmethod
    def from_response(cls, payload: dict) -> "QueryResultPage":
        data = payload.get("data", payload)
        items = [ServiceRecord.from_dict(item) for item in data.get("items", [])]
        return cls(
            items=items,
            count=int(data.get("count", len(items))),
            page=int(data.get("page", 1)),
            page_size=int(data.get("page_size", len(items) or 25)),
            total=int(data.get("total", len(items))),
        )


def _criterion_to_dict(criterion: QueryCriterion | dict[str, Any]) -> dict[str, Any]:
    if isinstance(criterion, QueryCriterion):
        return criterion.to_dict()
    return dict(criterion)
