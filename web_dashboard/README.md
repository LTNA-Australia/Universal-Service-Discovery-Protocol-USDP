# USDP Web Dashboard

This folder contains the operator console for the USDP v2 release.

## What It Implements

- v2-by-default registry browsing
- criteria-driven discovery queries
- type-aware service detail rendering
- same-origin proxying to registry read and admin endpoints
- admin metrics, audit, retention, and purge views when `USDP_ADMIN_TOKEN` is configured

## Environment Variables

- `USDP_DASHBOARD_HOST`
- `USDP_DASHBOARD_PORT`
- `USDP_REGISTRY_URL`
- `USDP_PROTOCOL_VERSION`
- `USDP_ADMIN_TOKEN`

## Deployment

Container deployment assets live in `../deployment/`.
