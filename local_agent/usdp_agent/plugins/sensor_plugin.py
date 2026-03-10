"""Sensor discovery plugin."""

from __future__ import annotations

from ..builders import build_sensor_service, stable_service_id
from ..net import endpoint_reachable
from .base import PluginBase


class SensorPlugin(PluginBase):
    plugin_name = "sensor"

    def discover(self) -> list[dict]:
        records = []
        for device in self.config.get("devices", []):
            endpoint_url = device["endpoint_url"]
            reachable = endpoint_reachable(endpoint_url)
            status = "online" if reachable else "offline"
            service_id = device.get("service_id") or stable_service_id(f"sensor:{endpoint_url}")
            record = build_sensor_service(
                service_id=service_id,
                name=device["name"],
                endpoint_url=endpoint_url,
                sensor_kind=device["sensor_kind"],
                measurement_types=device["measurement_types"],
                sampling_interval_ms=int(device["sampling_interval_ms"]),
                units=device["units"],
                battery_powered=bool(device["battery_powered"]),
                location_scope=device["location_scope"],
                status=status,
                tags=device.get("tags"),
                publisher={
                    "publisher_type": "agent",
                    "publisher_name": self.publisher_name,
                },
                auth=device.get("auth"),
                metadata=device.get("metadata"),
                description=device.get("description"),
                location=device.get("location"),
                heartbeat_ttl_seconds=device.get("heartbeat_ttl_seconds"),
                provenance=device.get("provenance"),
                extensions=device.get("extensions"),
            )
            records.append(record)
        return records
