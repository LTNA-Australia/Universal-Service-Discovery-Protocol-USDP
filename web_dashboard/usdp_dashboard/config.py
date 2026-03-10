"""Dashboard configuration."""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(slots=True)
class DashboardConfig:
    host: str
    port: int
    registry_url: str
    protocol_version: str
    admin_token: str | None = None


def load_config() -> DashboardConfig:
    return DashboardConfig(
        host=os.getenv("USDP_DASHBOARD_HOST", "127.0.0.1"),
        port=int(os.getenv("USDP_DASHBOARD_PORT", "8080")),
        registry_url=os.getenv("USDP_REGISTRY_URL", "http://127.0.0.1:8000"),
        protocol_version=os.getenv("USDP_PROTOCOL_VERSION", "2.0"),
        admin_token=os.getenv("USDP_ADMIN_TOKEN"),
    )
