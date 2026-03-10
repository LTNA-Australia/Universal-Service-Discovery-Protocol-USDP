# USDP Python SDK

This folder contains the main application-developer SDK for the USDP v2 release.

## What It Implements

- publish lifecycle operations
- v1 and v2 discovery operations
- typed models for services, provenance, publisher identity, criteria, and paged query results
- iterator and page helpers for discovery workflows
- builders for all shipped v2 service types
- optional admin helpers for metrics, audit, retention, and purge

## Main Package

- `usdp_sdk/`

## Example

```python
from usdp_sdk import SDKConfig, USDPSDK, QueryCriterion

sdk = USDPSDK(SDKConfig(
    registry_url="http://127.0.0.1:8000",
    protocol_version="2.0",
))

result = sdk.query_service_records(
    criteria=QueryCriterion.predicate("service_type", "eq", "database"),
)
```
