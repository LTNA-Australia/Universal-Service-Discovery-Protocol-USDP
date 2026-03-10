"""SDK configuration helpers."""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(slots=True)
class SDKConfig:
    registry_url: str
    publisher_token: str | None = None
    admin_token: str | None = None
    protocol_version: str = "2.0"
    timeout_seconds: float = 5.0
    retry_attempts: int = 3
    retry_delay_seconds: float = 0.5
    retry_backoff_factor: float = 2.0
    retry_jitter_seconds: float = 0.1
    max_retry_delay_seconds: float = 5.0


def load_config() -> SDKConfig:
    token = os.getenv("USDP_PUBLISHER_TOKEN")
    return SDKConfig(
        registry_url=os.getenv("USDP_REGISTRY_URL", "http://127.0.0.1:8000"),
        publisher_token=token,
        admin_token=os.getenv("USDP_ADMIN_TOKEN"),
        protocol_version=os.getenv("USDP_PROTOCOL_VERSION", "2.0"),
        timeout_seconds=float(os.getenv("USDP_SDK_TIMEOUT", "5")),
        retry_attempts=int(os.getenv("USDP_SDK_RETRIES", "3")),
        retry_delay_seconds=float(os.getenv("USDP_SDK_RETRY_DELAY", "0.5")),
        retry_backoff_factor=float(os.getenv("USDP_SDK_RETRY_BACKOFF", "2")),
        retry_jitter_seconds=float(os.getenv("USDP_SDK_RETRY_JITTER", "0.1")),
        max_retry_delay_seconds=float(os.getenv("USDP_SDK_MAX_RETRY_DELAY", "5")),
    )
