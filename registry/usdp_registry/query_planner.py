"""SQLite-backed query planning for supported registry queries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .utils import isoformat_z

ARRAY_SUFFIXES = {
    "tags",
    "capabilities.supported_protocols",
    "capabilities.capability_tags",
    "capabilities.stream_protocols",
    "capabilities.print_protocols",
    "capabilities.supported_paper_sizes",
    "capabilities.modalities",
    "capabilities.protocols",
    "capabilities.measurement_types",
}
DIRECT_COLUMNS = {
    "service_id": "service_id",
    "name": "name",
    "service_type": "service_type",
    "status": "status",
    "heartbeat_ttl_seconds": "heartbeat_ttl_seconds",
}
JSON_FIELD_COLUMNS = {
    "timestamps": "timestamps_json",
    "publisher": "publisher_json",
    "publisher_identity": "publisher_identity_json",
    "provenance": "provenance_json",
    "location": "location_json",
    "capabilities": "capabilities_json",
    "metadata": "metadata_json",
    "extensions": "extensions_json",
    "auth": "auth_json",
}
SORT_FIELDS = {
    "name": "name",
    "service_type": "service_type",
    "status": "status",
    "updated_at": "json_extract(timestamps_json, '$.updated_at')",
    "registered_at": "json_extract(timestamps_json, '$.registered_at')",
    "last_heartbeat_at": "json_extract(timestamps_json, '$.last_heartbeat_at')",
}


@dataclass(slots=True)
class QueryPlan:
    where_sql: str
    parameters: list[Any]
    order_sql: str
    limit: int
    offset: int
    requires_python_fallback: bool = False
    fallback_reason: str | None = None


def plan_query(query: dict, *, current_time) -> QueryPlan:
    where_parts: list[str] = []
    parameters: list[Any] = []

    if not query.get("include_inactive", False):
        where_parts.append("status != 'offline'")
        where_parts.append("json_extract(timestamps_json, '$.expires_at') > ?")
        parameters.append(isoformat_z(current_time))

    filters = query.get("filters", {})
    filter_sql, filter_params = _compile_filters(filters)
    where_parts.extend(filter_sql)
    parameters.extend(filter_params)

    requires_python_fallback = False
    fallback_reason = None
    criteria = query.get("criteria")
    if criteria:
        compiled = _compile_criteria(criteria)
        if compiled is None:
            requires_python_fallback = True
            fallback_reason = "criteria contains unsupported field or operator"
        else:
            criteria_sql, criteria_params = compiled
            where_parts.append(criteria_sql)
            parameters.extend(criteria_params)

    order_sql = _compile_order(query.get("sort", []))
    return QueryPlan(
        where_sql=" AND ".join(f"({part})" for part in where_parts) if where_parts else "1=1",
        parameters=parameters,
        order_sql=order_sql,
        limit=query.get("page_size", 25),
        offset=(query.get("page", 1) - 1) * query.get("page_size", 25),
        requires_python_fallback=requires_python_fallback,
        fallback_reason=fallback_reason,
    )


def _compile_filters(filters: dict) -> tuple[list[str], list[Any]]:
    where_parts: list[str] = []
    parameters: list[Any] = []
    if not filters:
        return where_parts, parameters

    if "service_type" in filters:
        where_parts.append("service_type = ?")
        parameters.append(filters["service_type"])
    if "status" in filters:
        where_parts.append("status = ?")
        parameters.append(filters["status"])
    if "service_ids" in filters:
        placeholders = ", ".join("?" for _ in filters["service_ids"])
        where_parts.append(f"service_id IN ({placeholders})")
        parameters.extend(filters["service_ids"])
    if "tags_all" in filters:
        for tag in filters["tags_all"]:
            where_parts.append("EXISTS (SELECT 1 FROM json_each(tags_json) WHERE value = ?)")
            parameters.append(tag)
    if "name_contains" in filters:
        where_parts.append("LOWER(name) LIKE ?")
        parameters.append(f"%{filters['name_contains'].casefold()}%")
    if "location" in filters:
        for key, value in filters["location"].items():
            where_parts.append(f"json_extract(location_json, '$.{key}') = ?")
            parameters.append(value)
    if "capabilities" in filters:
        for key, value in filters["capabilities"].items():
            where_parts.append(f"json_extract(capabilities_json, '$.{key}') = ?")
            parameters.append(value)
    return where_parts, parameters


def _compile_criteria(criteria: dict) -> tuple[str, list[Any]] | None:
    if "all" in criteria:
        compiled_parts = [_compile_criteria(item) for item in criteria["all"]]
        if any(item is None for item in compiled_parts):
            return None
        sql_parts = [item[0] for item in compiled_parts if item]
        params = [param for item in compiled_parts if item for param in item[1]]
        return " AND ".join(f"({part})" for part in sql_parts), params
    if "any" in criteria:
        compiled_parts = [_compile_criteria(item) for item in criteria["any"]]
        if any(item is None for item in compiled_parts):
            return None
        sql_parts = [item[0] for item in compiled_parts if item]
        params = [param for item in compiled_parts if item for param in item[1]]
        return " OR ".join(f"({part})" for part in sql_parts), params
    if "not" in criteria:
        compiled = _compile_criteria(criteria["not"])
        if compiled is None:
            return None
        return f"NOT ({compiled[0]})", compiled[1]
    return _compile_predicate(criteria)


def _compile_predicate(predicate: dict) -> tuple[str, list[Any]] | None:
    field = predicate["field"]
    op = predicate["op"]
    value = predicate.get("value")

    if field == "tags":
        if op in {"contains", "eq"}:
            return "EXISTS (SELECT 1 FROM json_each(tags_json) WHERE value = ?)", [value]
        if op == "exists":
            return "(tags_json IS NOT NULL) = ?", [1 if value else 0]
        return None

    resolved = _resolve_field(field)
    if resolved is None:
        return None
    expression, is_array = resolved

    if op == "exists":
        if expression["path"] == "$":
            return f"{expression['sql']} IS {'NOT ' if value else ''}NULL", []
        return f"json_type({expression['column']}, '{expression['path']}') IS {'NOT ' if value else ''}NULL", []

    if is_array:
        if op in {"contains", "eq"}:
            return (
                f"EXISTS (SELECT 1 FROM json_each({expression['column']}, '{expression['path']}') WHERE value = ?)",
                [value],
            )
        if op == "in":
            placeholders = ", ".join("?" for _ in value)
            return (
                f"EXISTS (SELECT 1 FROM json_each({expression['column']}, '{expression['path']}') WHERE value IN ({placeholders}))",
                list(value),
            )
        return None

    sql_expr = expression["sql"]
    if op == "eq":
        return f"{sql_expr} = ?", [value]
    if op == "neq":
        return f"{sql_expr} != ?", [value]
    if op == "in":
        placeholders = ", ".join("?" for _ in value)
        return f"{sql_expr} IN ({placeholders})", list(value)
    if op == "contains":
        return f"LOWER(CAST({sql_expr} AS TEXT)) LIKE ?", [f"%{str(value).casefold()}%"]
    if op == "starts_with":
        return f"LOWER(CAST({sql_expr} AS TEXT)) LIKE ?", [f"{str(value).casefold()}%"]
    if op == "gte":
        return f"{sql_expr} >= ?", [value]
    if op == "lte":
        return f"{sql_expr} <= ?", [value]
    return None


def _resolve_field(field_path: str) -> tuple[dict[str, str], bool] | None:
    if field_path in DIRECT_COLUMNS:
        return {"sql": DIRECT_COLUMNS[field_path], "column": DIRECT_COLUMNS[field_path], "path": "$"}, False

    if field_path in ARRAY_SUFFIXES:
        if field_path == "tags":
            return {"sql": "tags_json", "column": "tags_json", "path": "$"}, True
        prefix, suffix = field_path.split(".", 1)
        column = JSON_FIELD_COLUMNS.get(prefix)
        if not column:
            return None
        return {"sql": f"json_extract({column}, '$.{suffix}')", "column": column, "path": f"$.{suffix}"}, True

    if "." not in field_path:
        return None

    prefix, suffix = field_path.split(".", 1)
    column = JSON_FIELD_COLUMNS.get(prefix)
    if not column:
        return None
    return {"sql": f"json_extract({column}, '$.{suffix}')", "column": column, "path": f"$.{suffix}"}, False


def _compile_order(sort_rules: list[dict]) -> str:
    if not sort_rules:
        return "ORDER BY name COLLATE NOCASE ASC, service_id ASC"

    clauses = []
    for rule in sort_rules:
        expression = SORT_FIELDS.get(rule["field"])
        if expression is None:
            continue
        direction = "DESC" if rule["direction"] == "desc" else "ASC"
        clauses.append(f"{expression} {direction}")
    clauses.append("service_id ASC")
    return "ORDER BY " + ", ".join(clauses)
