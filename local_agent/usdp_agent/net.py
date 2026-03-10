"""Simple network probing helpers for v1 plugins."""

from __future__ import annotations

from http.client import HTTPConnection, HTTPSConnection
import socket
from urllib.parse import urlparse


def tcp_probe(host: str, port: int, timeout: float = 2.0) -> bool:
    with socket.create_connection((host, port), timeout=timeout):
        return True


def endpoint_reachable(url: str, timeout: float = 2.0) -> bool:
    parsed = urlparse(url)
    if parsed.scheme in {"http", "https"}:
        connection_type = HTTPSConnection if parsed.scheme == "https" else HTTPConnection
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        connection = connection_type(parsed.hostname, port, timeout=timeout)
        try:
            connection.request("GET", parsed.path or "/")
            response = connection.getresponse()
            response.read()
            return 200 <= response.status < 400 or response.status in {401, 403}
        finally:
            connection.close()

    if parsed.hostname and parsed.port:
        return tcp_probe(parsed.hostname, parsed.port, timeout=timeout)

    if parsed.hostname:
        default_port = 554 if parsed.scheme == "rtsp" else 80
        return tcp_probe(parsed.hostname, default_port, timeout=timeout)

    raise ValueError(f"Unable to probe endpoint URL: {url}")
