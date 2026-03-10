# USDP Self-Registration Client

This folder contains the native publisher client for the USDP v2 release.

## What It Implements

- v2-by-default publisher HTTP client
- builders for all shipped v2 service types
- register-or-update helper behavior
- retry support and client-side idempotency headers
- background heartbeat worker with structured status snapshots
- CLI entrypoint for register, register-or-update, update, heartbeat, run-heartbeat, and deregister

## Main Package

- `usdp_client/`

## Environment Variables

- `USDP_REGISTRY_URL`
- `USDP_PUBLISHER_TOKEN`
- `USDP_PROTOCOL_VERSION`
- `USDP_CLIENT_TIMEOUT`
- `USDP_CLIENT_RETRIES`
- `USDP_CLIENT_RETRY_DELAY`
