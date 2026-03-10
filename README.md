# Universal Service Discovery Protocol (USDP)

This repository contains the public Python reference components for the Universal Service Discovery Protocol (USDP), including the registry, publisher clients, local agent, SDK, and operator dashboard.

For protocol semantics, payload structure, operational guidance, and implementation detail, start with [USDP_Guide.pdf](USDP_Guide.pdf). This README is intended to help GitHub users understand the repository layout and get the main components running quickly.

## What Is In This Repository

| Component | Path | Purpose |
| --- | --- | --- |
| Registry | [`registry/`](registry/) | Core USDP registry server with persistence, validation, lifecycle handling, admin endpoints, and federation support. |
| Local Agent | [`local_agent/`](local_agent/) | Publisher path for non-native devices and services such as printers, cameras, sensors, and proxied APIs. |
| Self-Registration Client | [`self_registration_client/`](self_registration_client/) | Native publisher CLI and client library for register, update, heartbeat, and deregister flows. |
| Python SDK | [`sdk_python/`](sdk_python/) | Application-facing SDK for discovery, publishing, typed models, and service builders. |
| Web Dashboard | [`web_dashboard/`](web_dashboard/) | Browser-based operator console for browsing registry data, queries, metrics, audit data, and retention actions. |

Each component is packaged separately and can be installed independently.

## Repository Structure

```text
.
|-- USDP_Guide.pdf
|-- registry/
|-- local_agent/
|-- self_registration_client/
|-- sdk_python/
`-- web_dashboard/
```

## Requirements

- Python 3.11 or newer
- `pip` for installing individual packages

## Quick Start

### 1. Start the Registry

The registry is the core service that stores and serves USDP records.

```powershell
cd registry
python -m pip install -e .
usdp-registry
```

Default development settings:

- Registry URL: `http://127.0.0.1:8000`
- Default publisher token: `dev-token`

For registry configuration, endpoints, retention behavior, auth details, and federation support, see [`registry/README.md`](registry/README.md) and [USDP_Guide.pdf](USDP_Guide.pdf).

### 2. Start the Web Dashboard

The dashboard provides a read/admin UI over the registry.

```powershell
cd web_dashboard
python -m pip install -e .
$env:USDP_REGISTRY_URL="http://127.0.0.1:8000"
usdp-dashboard
```

Default dashboard address:

- Dashboard URL: `http://127.0.0.1:8080`

For dashboard capabilities and admin-token behavior, see [`web_dashboard/README.md`](web_dashboard/README.md) and [USDP_Guide.pdf](USDP_Guide.pdf).

### 3. Register Native Services

Use the self-registration client when the service can publish its own USDP record directly.

```powershell
cd self_registration_client
python -m pip install -e .
$env:USDP_REGISTRY_URL="http://127.0.0.1:8000"
$env:USDP_PUBLISHER_TOKEN="dev-token"
usdp-client register path\to\service.json
```

Other supported CLI operations include:

- `register-or-update`
- `update`
- `heartbeat`
- `run-heartbeat`
- `deregister`

For service payload examples and lifecycle expectations, see [`self_registration_client/README.md`](self_registration_client/README.md) and [USDP_Guide.pdf](USDP_Guide.pdf).

### 4. Publish Non-Native Services and Devices

Use the local agent when a device or service cannot speak USDP directly.

```powershell
cd local_agent
python -m pip install -e .
usdp-agent run-once examples/agent_config.example.json
```

The sample config includes built-in plugin examples for:

- APIs
- Printers
- Cameras
- Sensors

For agent configuration, plugin behavior, and operating guidance, see [`local_agent/README.md`](local_agent/README.md), [`local_agent/examples/agent_config.example.json`](local_agent/examples/agent_config.example.json), and [USDP_Guide.pdf](USDP_Guide.pdf).

### 5. Integrate from Python

Use the SDK when you want to query or publish from application code.

```powershell
cd sdk_python
python -m pip install -e .
```

```python
from usdp_sdk import SDKConfig, USDPSDK, QueryCriterion

sdk = USDPSDK(
    SDKConfig(
        registry_url="http://127.0.0.1:8000",
        protocol_version="2.0",
    )
)

result = sdk.query_service_records(
    criteria=QueryCriterion.predicate("service_type", "eq", "database"),
)
```

For the full SDK surface, builders, and typed models, see [`sdk_python/README.md`](sdk_python/README.md) and [USDP_Guide.pdf](USDP_Guide.pdf).

## Documentation

- Primary documentation: [USDP_Guide.pdf](USDP_Guide.pdf)
- Registry notes: [`registry/README.md`](registry/README.md)
- Local agent notes: [`local_agent/README.md`](local_agent/README.md)
- Self-registration client notes: [`self_registration_client/README.md`](self_registration_client/README.md)
- Python SDK notes: [`sdk_python/README.md`](sdk_python/README.md)
- Web dashboard notes: [`web_dashboard/README.md`](web_dashboard/README.md)

If you need protocol rules, field definitions, lifecycle semantics, example payloads, or deployment guidance, refer to [USDP_Guide.pdf](USDP_Guide.pdf).

## License

This repository is licensed under the terms in [`LICENSE`](LICENSE).
