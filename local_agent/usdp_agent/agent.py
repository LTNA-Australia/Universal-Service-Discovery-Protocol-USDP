"""Local agent orchestration."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

from .builders import build_service_update_changes
from .config import AgentConfig
from .registry_client import RegistryClientConfig, RegistryClientHTTPError, RegistryPublisherClient
from .state import load_state, save_state
from .plugins.api_plugin import ApiPlugin
from .plugins.camera_plugin import CameraPlugin
from .plugins.printer_plugin import PrinterPlugin
from .plugins.sensor_plugin import SensorPlugin


PLUGIN_TYPES = {
    "api": ApiPlugin,
    "printer": PrinterPlugin,
    "camera": CameraPlugin,
    "sensor": SensorPlugin,
}


class LocalAgent:
    def __init__(self, config: AgentConfig) -> None:
        self.config = config
        self.logger = logging.getLogger("usdp_agent")
        self.client = RegistryPublisherClient(
            RegistryClientConfig(
                registry_url=config.registry_url,
                publisher_token=config.publisher_token,
                protocol_version=config.protocol_version,
                timeout_seconds=config.timeout_seconds,
                retry_attempts=config.retry_attempts,
                retry_delay_seconds=config.retry_delay_seconds,
                retry_backoff_factor=config.retry_backoff_factor,
                retry_jitter_seconds=config.retry_jitter_seconds,
                max_retry_delay_seconds=config.max_retry_delay_seconds,
            )
        )
        self.plugins = self._build_plugins()

    def run_once(self) -> dict:
        previous_state = load_state(self.config.state_file)
        discovered_records: dict[str, dict] = {}
        failures: list[dict] = []

        for plugin in self.plugins:
            try:
                records = plugin.discover()
            except Exception as exc:  # noqa: BLE001
                self.logger.exception("Plugin discovery failed: %s", plugin.plugin_name)
                failures.append({"plugin": plugin.plugin_name, "phase": "discover", "message": str(exc)})
                continue
            for record in records:
                discovered_records[record["service_id"]] = {
                    "plugin": plugin.plugin_name,
                    "record": record,
                }

        result = {
            "protocol_version": self.config.protocol_version,
            "registered": 0,
            "updated": 0,
            "hearted": 0,
            "deregistered": 0,
            "discovered": len(discovered_records),
            "failures": failures,
        }
        next_state: dict[str, dict] = {}

        for service_id, item in discovered_records.items():
            record = item["record"]
            fingerprint = self._fingerprint(record)
            state_entry = previous_state.get(service_id)

            try:
                if state_entry is None:
                    self._register_or_update(record)
                    result["registered"] += 1
                elif state_entry["fingerprint"] != fingerprint:
                    changes = build_service_update_changes(record)
                    self.client.update_service(service_id, changes, idempotency_key=f"update:{service_id}")
                    result["updated"] += 1
                else:
                    self.client.heartbeat(service_id, status=record["status"], idempotency_key=f"heartbeat:{service_id}")
                    result["hearted"] += 1
            except Exception as exc:  # noqa: BLE001
                self.logger.exception("Failed to publish service %s", service_id)
                result["failures"].append(
                    {
                        "plugin": item["plugin"],
                        "service_id": service_id,
                        "phase": "publish",
                        "message": str(exc),
                    }
                )
                continue

            next_state[service_id] = {
                "plugin": item["plugin"],
                "fingerprint": fingerprint,
            }

        for service_id in sorted(set(previous_state) - set(discovered_records)):
            try:
                self.client.deregister_service(
                    service_id,
                    "service no longer discovered by local agent",
                    idempotency_key=f"deregister:{service_id}",
                )
                result["deregistered"] += 1
            except RegistryClientHTTPError as exc:
                if exc.status != 404:
                    result["failures"].append(
                        {
                            "plugin": previous_state[service_id].get("plugin"),
                            "service_id": service_id,
                            "phase": "deregister",
                            "message": str(exc),
                        }
                    )

        save_state(self.config.state_file, next_state)
        self._write_report(result)
        return result

    def run_loop(self) -> None:
        while True:
            summary = self.run_once()
            self.logger.info(
                "Cycle complete registered=%s updated=%s hearted=%s deregistered=%s",
                summary["registered"],
                summary["updated"],
                summary["hearted"],
                summary["deregistered"],
            )
            time.sleep(self.config.cycle_interval_seconds)

    def _build_plugins(self) -> list:
        plugins = []
        for plugin_name, plugin_config in self.config.plugins.items():
            plugin_type = PLUGIN_TYPES.get(plugin_name)
            if plugin_type is None:
                raise ValueError(f"Unsupported plugin type: {plugin_name}")
            plugins.append(plugin_type(plugin_config, self.config.publisher_name))
        return plugins

    def _register_or_update(self, record: dict) -> None:
        try:
            self.client.register_service(record, idempotency_key=f"register:{record['service_id']}")
        except RegistryClientHTTPError as exc:
            if exc.status != 409:
                raise
            changes = build_service_update_changes(record)
            self.client.update_service(record["service_id"], changes, idempotency_key=f"update:{record['service_id']}")

    def _fingerprint(self, record: dict) -> str:
        return json.dumps(record, sort_keys=True, separators=(",", ":"))

    def _write_report(self, summary: dict) -> None:
        if self.config.report_file is None:
            return
        report = dict(summary)
        report["generated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        report["publisher_name"] = self.config.publisher_name
        report["plugin_count"] = len(self.plugins)
        self.config.report_file.write_text(json.dumps(report, indent=2), encoding="utf-8")
