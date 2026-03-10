"""Local agent configuration."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path


@dataclass(slots=True)
class AgentConfig:
    registry_url: str
    publisher_token: str
    publisher_name: str
    protocol_version: str
    state_file: Path
    report_file: Path | None
    cycle_interval_seconds: float
    timeout_seconds: float
    retry_attempts: int
    retry_delay_seconds: float
    retry_backoff_factor: float
    retry_jitter_seconds: float
    max_retry_delay_seconds: float
    plugins: dict


def load_agent_config(path: Path) -> AgentConfig:
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)

    state_file = Path(raw.get("state_file", "state/agent_state.json"))
    if not state_file.is_absolute():
        state_file = path.parent / state_file
    state_file.parent.mkdir(parents=True, exist_ok=True)

    report_file = raw.get("report_file")
    if report_file:
        report_path = Path(report_file)
        if not report_path.is_absolute():
            report_path = path.parent / report_path
        report_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        report_path = None

    return AgentConfig(
        registry_url=raw["registry_url"],
        publisher_token=raw["publisher_token"],
        publisher_name=raw.get("publisher_name", "usdp-local-agent"),
        protocol_version=raw.get("protocol_version", "2.0"),
        state_file=state_file,
        report_file=report_path,
        cycle_interval_seconds=float(raw.get("cycle_interval_seconds", 30.0)),
        timeout_seconds=float(raw.get("timeout_seconds", 5.0)),
        retry_attempts=int(raw.get("retry_attempts", 3)),
        retry_delay_seconds=float(raw.get("retry_delay_seconds", 0.5)),
        retry_backoff_factor=float(raw.get("retry_backoff_factor", 2.0)),
        retry_jitter_seconds=float(raw.get("retry_jitter_seconds", 0.1)),
        max_retry_delay_seconds=float(raw.get("max_retry_delay_seconds", 5.0)),
        plugins=raw.get("plugins", {}),
    )
