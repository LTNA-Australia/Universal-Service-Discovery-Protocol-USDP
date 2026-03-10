"""Camera discovery plugin."""

from __future__ import annotations

from ..builders import build_camera_service, stable_service_id
from ..net import endpoint_reachable
from .base import PluginBase


class CameraPlugin(PluginBase):
    plugin_name = "camera"

    def discover(self) -> list[dict]:
        records = []
        for device in self.config.get("devices", []):
            endpoint_url = device["endpoint_url"]
            reachable = endpoint_reachable(endpoint_url)
            status = "online" if reachable else "offline"
            service_id = device.get("service_id") or stable_service_id(f"camera:{endpoint_url}")
            record = build_camera_service(
                service_id=service_id,
                name=device["name"],
                endpoint_url=endpoint_url,
                stream_protocols=device["stream_protocols"],
                resolution=device["resolution"],
                night_vision=bool(device["night_vision"]),
                ptz=bool(device["ptz"]),
                location=device["location"],
                status=status,
                thermal=device.get("thermal"),
                frame_rate=device.get("frame_rate"),
                manufacturer=device.get("manufacturer"),
                model=device.get("model"),
                tags=device.get("tags"),
                publisher={
                    "publisher_type": "agent",
                    "publisher_name": self.publisher_name,
                },
                auth=device.get("auth"),
                metadata=device.get("metadata"),
            )
            records.append(record)
        return records
