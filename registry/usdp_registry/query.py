"""Query filtering and paging."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .utils import parse_datetime


def is_record_active(record: dict, current_time: datetime) -> bool:
    if record["status"] == "offline":
        return False
    return parse_datetime(record["timestamps"]["expires_at"]) > current_time


def query_records(records: list[dict], query: dict, current_time: datetime) -> dict:
    include_inactive = query.get("include_inactive", False)
    filters = query.get("filters", {})
    criteria = query.get("criteria")
    page = query.get("page", 1)
    page_size = query.get("page_size", 25)
    sort_rules = query.get("sort", [])

    filtered = []
    for record in records:
        if not include_inactive and not is_record_active(record, current_time):
            continue
        if not _matches_filters(record, filters):
            continue
        if criteria and not _matches_criteria(record, criteria):
            continue
        filtered.append(record)

    filtered = _sort_records(filtered, sort_rules)
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    items = filtered[start:end]

    return {
        "items": items,
        "count": len(items),
        "page": page,
        "page_size": page_size,
        "total": total,
    }


def _matches_filters(record: dict, filters: dict) -> bool:
    if not filters:
        return True

    if "service_type" in filters and record["service_type"] != filters["service_type"]:
        return False

    if "status" in filters and record["status"] != filters["status"]:
        return False

    if "service_ids" in filters and record["service_id"] not in set(filters["service_ids"]):
        return False

    if "tags_all" in filters:
        tag_set = set(record.get("tags", []))
        if not set(filters["tags_all"]).issubset(tag_set):
            return False

    if "name_contains" in filters:
        if filters["name_contains"].casefold() not in record["name"].casefold():
            return False

    if "location" in filters:
        location = record.get("location", {})
        for key, value in filters["location"].items():
            if location.get(key) != value:
                return False

    if "capabilities" in filters:
        capabilities = record.get("capabilities", {})
        for key, value in filters["capabilities"].items():
            if capabilities.get(key) != value:
                return False

    return True


def _sort_records(records: list[dict], sort_rules: list[dict]) -> list[dict]:
    if not sort_rules:
        return sorted(records, key=lambda item: item["name"].casefold())

    sorted_records = list(records)
    for rule in reversed(sort_rules):
        field = rule["field"]
        reverse = rule["direction"] == "desc"
        if field == "name":
            sorted_records.sort(key=lambda item: item["name"].casefold(), reverse=reverse)
        elif field == "service_type":
            sorted_records.sort(key=lambda item: item["service_type"], reverse=reverse)
        elif field == "status":
            sorted_records.sort(key=lambda item: item["status"], reverse=reverse)
        elif field == "updated_at":
            sorted_records.sort(key=lambda item: item["timestamps"]["updated_at"], reverse=reverse)
        elif field == "registered_at":
            sorted_records.sort(key=lambda item: item["timestamps"]["registered_at"], reverse=reverse)
        elif field == "last_heartbeat_at":
            sorted_records.sort(key=lambda item: item["timestamps"]["last_heartbeat_at"], reverse=reverse)
    return sorted_records


def _matches_criteria(record: dict, criteria: dict) -> bool:
    if "all" in criteria:
        return all(_matches_criteria(record, item) for item in criteria["all"])
    if "any" in criteria:
        return any(_matches_criteria(record, item) for item in criteria["any"])
    if "not" in criteria:
        return not _matches_criteria(record, criteria["not"])
    return _matches_predicate(record, criteria)


def _matches_predicate(record: dict, predicate: dict) -> bool:
    exists, actual = _resolve_field(record, predicate["field"])
    op = predicate["op"]
    value = predicate.get("value")

    if op == "exists":
        return exists is bool(value)
    if not exists:
        return False

    if op == "eq":
        return _matches_equality(actual, value)
    if op == "neq":
        return not _matches_equality(actual, value)
    if op == "in":
        return _matches_membership(actual, value)
    if op == "contains":
        return _matches_contains(actual, value)
    if op == "starts_with":
        return _matches_starts_with(actual, value)
    if op == "gte":
        return _matches_comparison(actual, value, operator="gte")
    if op == "lte":
        return _matches_comparison(actual, value, operator="lte")
    return False


def _resolve_field(record: dict, field_path: str) -> tuple[bool, Any]:
    current: Any = record
    for part in field_path.split("."):
        if isinstance(current, dict):
            if part not in current:
                return False, None
            current = current[part]
            continue
        if isinstance(current, list):
            next_values = []
            for item in current:
                if isinstance(item, dict) and part in item:
                    next_values.append(item[part])
            if not next_values:
                return False, None
            current = next_values
            continue
        return False, None
    return True, current


def _matches_equality(actual: Any, expected: Any) -> bool:
    if isinstance(actual, list):
        return any(item == expected for item in actual)
    return actual == expected


def _matches_membership(actual: Any, expected_values: list[Any]) -> bool:
    if isinstance(actual, list):
        return any(item in expected_values for item in actual)
    return actual in expected_values


def _matches_contains(actual: Any, expected: Any) -> bool:
    if isinstance(actual, str) and isinstance(expected, str):
        return expected.casefold() in actual.casefold()
    if isinstance(actual, list):
        return any(item == expected for item in actual)
    return False


def _matches_starts_with(actual: Any, expected: Any) -> bool:
    if isinstance(actual, str) and isinstance(expected, str):
        return actual.casefold().startswith(expected.casefold())
    if isinstance(actual, list):
        return any(isinstance(item, str) and item.casefold().startswith(str(expected).casefold()) for item in actual)
    return False


def _matches_comparison(actual: Any, expected: Any, *, operator: str) -> bool:
    if isinstance(actual, list):
        return any(_compare_values(item, expected, operator=operator) for item in actual)
    return _compare_values(actual, expected, operator=operator)


def _compare_values(actual: Any, expected: Any, *, operator: str) -> bool:
    actual_value = _coerce_comparable(actual)
    expected_value = _coerce_comparable(expected)
    if actual_value is None or expected_value is None:
        return False
    if operator == "gte":
        return actual_value >= expected_value
    return actual_value <= expected_value


def _coerce_comparable(value: Any) -> Any:
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        try:
            return parse_datetime(value)
        except Exception:  # noqa: BLE001
            return value.casefold()
    return None
