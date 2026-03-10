"""Printer discovery plugin."""

from __future__ import annotations

from ..builders import build_printer_service, stable_service_id
from ..net import endpoint_reachable
from .base import PluginBase


class PrinterPlugin(PluginBase):
    plugin_name = "printer"

    def discover(self) -> list[dict]:
        records = []
        for device in self.config.get("devices", []):
            endpoint_url = device["endpoint_url"]
            reachable = endpoint_reachable(endpoint_url)
            status = "online" if reachable else "offline"
            service_id = device.get("service_id") or stable_service_id(f"printer:{endpoint_url}")
            record = build_printer_service(
                service_id=service_id,
                name=device["name"],
                endpoint_url=endpoint_url,
                color=bool(device["color"]),
                duplex=bool(device["duplex"]),
                supported_paper_sizes=device["supported_paper_sizes"],
                print_protocols=device["print_protocols"],
                location=device["location"],
                status=status,
                queue_name=device.get("queue_name"),
                manufacturer=device.get("manufacturer"),
                model=device.get("model"),
                max_resolution_dpi=device.get("max_resolution_dpi"),
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
