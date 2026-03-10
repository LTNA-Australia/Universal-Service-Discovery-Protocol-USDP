"""API discovery plugin."""

from __future__ import annotations

from ..builders import build_api_service, stable_service_id
from ..net import endpoint_reachable
from .base import PluginBase


class ApiPlugin(PluginBase):
    plugin_name = "api"

    def discover(self) -> list[dict]:
        records = []
        for service in self.config.get("services", []):
            base_url = service["base_url"]
            health_target = base_url.rstrip("/") + service.get("health_endpoint", "")
            reachable = endpoint_reachable(health_target)
            status = "online" if reachable else "offline"
            service_id = service.get("service_id") or stable_service_id(f"api:{base_url}")
            record = build_api_service(
                service_id=service_id,
                name=service["name"],
                base_url=base_url,
                auth_type=service["auth_type"],
                version=service["version"],
                supported_protocols=service.get("supported_protocols"),
                status=status,
                tags=service.get("tags"),
                publisher={
                    "publisher_type": "agent",
                    "publisher_name": self.publisher_name,
                },
                auth=service.get("auth"),
                metadata=service.get("metadata"),
                health_endpoint=service.get("health_endpoint"),
                documentation_url=service.get("documentation_url"),
                rate_limit_hint=service.get("rate_limit_hint"),
                capability_tags=service.get("capability_tags"),
            )
            records.append(record)
        return records
