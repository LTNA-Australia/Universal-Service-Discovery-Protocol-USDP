# USDP Registry

This folder contains the backend reference registry for the Universal Service Discovery Protocol.

## What It Implements

- registry HTTP server
- publisher auth for write operations
- service record validation
- SQLite persistence
- storage-backed query planning with Python fallback where needed
- heartbeat refresh and stale-record expiry
- richer registry health reporting
- request size limits, role-aware auth, and publisher ownership binding
- retention and purge handling
- audit events and runtime metrics
- bounded v2 federation import and withdrawal paths

## Protocol Dependency

The registry implements the public protocol packages in:

- `../protocol_spec/USDP_1.0_Spec.md`
- `../protocol_spec/USDP_2.0_Spec.md`

## Run

From this folder:

`usdp-registry`

or:

```powershell
python -m usdp_registry.main
```

Default server address:

- host: `127.0.0.1`
- port: `8000`

## Environment Variables

- `USDP_REGISTRY_HOST`
- `USDP_REGISTRY_PORT`
- `USDP_REGISTRY_DB`
- `USDP_PUBLISHER_TOKENS`
- `USDP_ADMIN_TOKENS`
- `USDP_PEER_TOKENS`
- `USDP_REGISTRY_ID`
- `USDP_REGISTRY_MAX_REQUEST_BYTES`
- `USDP_REGISTRY_EXPIRY_CHECK_INTERVAL`
- `USDP_REGISTRY_STALE_RETENTION_SECONDS`
- `USDP_REGISTRY_WITHDRAWN_RETENTION_SECONDS`
- `USDP_REGISTRY_WRITE_RATE_LIMIT_PER_MINUTE`
- `USDP_REGISTRY_QUERY_RATE_LIMIT_PER_MINUTE`
- `USDP_REGISTRY_ADMIN_RATE_LIMIT_PER_MINUTE`
- `USDP_REGISTRY_PEER_RATE_LIMIT_PER_MINUTE`
- `USDP_REGISTRY_AUTH_FAILURES_PER_MINUTE`
- `USDP_REGISTRY_MAX_QUERY_CRITERIA_NODES`

Default publisher token for development:

- `dev-token` (publisher alias `development`)

## Main Endpoints

- `POST /v1/services`
- `PATCH /v1/services/{service_id}`
- `POST /v1/services/{service_id}/heartbeat`
- `POST /v1/services/{service_id}/deregister`
- `POST /v1/query`
- `GET /v1/services/{service_id}`
- `GET /v1/health`
- `GET /v2/metrics`
- `GET /v2/admin/audit`
- `GET /v2/admin/retention`
- `POST /v2/admin/purge`
- `POST /v2/federation/import`
- `POST /v2/federation/withdrawals`

## Folder Structure

- `usdp_registry/`: implementation code
- `data/`: default SQLite database location at runtime

## Deployment

Container deployment assets now live in `../deployment/`.
