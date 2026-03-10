"""Runtime configuration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

DEFAULT_PUBLISHER_TOKENS = ("development=dev-token",)
DEFAULT_ADMIN_TOKENS: tuple[str, ...] = ()
DEFAULT_PEER_TOKENS: tuple[str, ...] = ()


@dataclass(slots=True)
class RegistryConfig:
    host: str
    port: int
    database_path: Path
    publisher_tokens: tuple[str, ...]
    admin_tokens: tuple[str, ...] = DEFAULT_ADMIN_TOKENS
    peer_tokens: tuple[str, ...] = DEFAULT_PEER_TOKENS
    protocol_version: str = "1.0"
    registry_id: str = "local-registry"
    default_ttl_seconds: int = 90
    max_request_bytes: int = 1_048_576
    expiry_check_interval_seconds: float = 5.0
    stale_retention_seconds: int = 86_400
    withdrawn_retention_seconds: int = 86_400
    write_rate_limit_per_minute: int = 240
    query_rate_limit_per_minute: int = 480
    admin_rate_limit_per_minute: int = 240
    peer_rate_limit_per_minute: int = 240
    auth_failures_per_minute: int = 30
    max_query_criteria_nodes: int = 32


def load_config(base_dir: Path | None = None) -> RegistryConfig:
    root_dir = Path(base_dir or Path(__file__).resolve().parents[1])
    db_env = os.getenv("USDP_REGISTRY_DB")
    if db_env:
        database_path = Path(db_env)
        if not database_path.is_absolute():
            database_path = root_dir / database_path
    else:
        database_path = root_dir / "data" / "registry.sqlite3"

    database_path.parent.mkdir(parents=True, exist_ok=True)

    token_env = os.getenv("USDP_PUBLISHER_TOKENS", ",".join(DEFAULT_PUBLISHER_TOKENS))
    publisher_tokens = tuple(token.strip() for token in token_env.split(",") if token.strip())
    if not publisher_tokens:
        publisher_tokens = DEFAULT_PUBLISHER_TOKENS

    admin_env = os.getenv("USDP_ADMIN_TOKENS", ",".join(DEFAULT_ADMIN_TOKENS))
    admin_tokens = tuple(token.strip() for token in admin_env.split(",") if token.strip())

    peer_env = os.getenv("USDP_PEER_TOKENS", ",".join(DEFAULT_PEER_TOKENS))
    peer_tokens = tuple(token.strip() for token in peer_env.split(",") if token.strip())

    return RegistryConfig(
        host=os.getenv("USDP_REGISTRY_HOST", "127.0.0.1"),
        port=int(os.getenv("USDP_REGISTRY_PORT", "8000")),
        database_path=database_path,
        publisher_tokens=publisher_tokens,
        admin_tokens=admin_tokens,
        peer_tokens=peer_tokens,
        protocol_version="1.0",
        registry_id=os.getenv("USDP_REGISTRY_ID", "local-registry"),
        default_ttl_seconds=90,
        max_request_bytes=int(os.getenv("USDP_REGISTRY_MAX_REQUEST_BYTES", "1048576")),
        expiry_check_interval_seconds=float(os.getenv("USDP_REGISTRY_EXPIRY_CHECK_INTERVAL", "5")),
        stale_retention_seconds=int(os.getenv("USDP_REGISTRY_STALE_RETENTION_SECONDS", "86400")),
        withdrawn_retention_seconds=int(os.getenv("USDP_REGISTRY_WITHDRAWN_RETENTION_SECONDS", "86400")),
        write_rate_limit_per_minute=int(os.getenv("USDP_REGISTRY_WRITE_RATE_LIMIT_PER_MINUTE", "240")),
        query_rate_limit_per_minute=int(os.getenv("USDP_REGISTRY_QUERY_RATE_LIMIT_PER_MINUTE", "480")),
        admin_rate_limit_per_minute=int(os.getenv("USDP_REGISTRY_ADMIN_RATE_LIMIT_PER_MINUTE", "240")),
        peer_rate_limit_per_minute=int(os.getenv("USDP_REGISTRY_PEER_RATE_LIMIT_PER_MINUTE", "240")),
        auth_failures_per_minute=int(os.getenv("USDP_REGISTRY_AUTH_FAILURES_PER_MINUTE", "30")),
        max_query_criteria_nodes=int(os.getenv("USDP_REGISTRY_MAX_QUERY_CRITERIA_NODES", "32")),
    )
