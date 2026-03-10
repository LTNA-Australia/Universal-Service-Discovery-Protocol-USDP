"""Client runtime configuration."""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(slots=True)
class ClientConfig:
    registry_url: str
    publisher_token: str
    protocol_version: str = "2.0"
    timeout_seconds: float = 5.0
    retry_attempts: int = 3
    retry_delay_seconds: float = 0.5
    retry_backoff_factor: float = 2.0
    retry_jitter_seconds: float = 0.1
    max_retry_delay_seconds: float = 5.0


def load_config() -> ClientConfig:
    return ClientConfig(
        registry_url=os.getenv("USDP_REGISTRY_URL", "http://127.0.0.1:8000"),
        publisher_token=os.getenv("USDP_PUBLISHER_TOKEN", "dev-token"),
        protocol_version=os.getenv("USDP_PROTOCOL_VERSION", "2.0"),
        timeout_seconds=float(os.getenv("USDP_CLIENT_TIMEOUT", "5")),
        retry_attempts=int(os.getenv("USDP_CLIENT_RETRIES", "3")),
        retry_delay_seconds=float(os.getenv("USDP_CLIENT_RETRY_DELAY", "0.5")),
        retry_backoff_factor=float(os.getenv("USDP_CLIENT_RETRY_BACKOFF", "2")),
        retry_jitter_seconds=float(os.getenv("USDP_CLIENT_RETRY_JITTER", "0.1")),
        max_retry_delay_seconds=float(os.getenv("USDP_CLIENT_MAX_RETRY_DELAY", "5")),
    )
