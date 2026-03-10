"""Static dashboard server with registry API proxying."""

from __future__ import annotations

from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from urllib import error, request
from urllib.parse import urlparse

from .config import DashboardConfig


class DashboardRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, directory: str | None = None, **kwargs) -> None:
        super().__init__(*args, directory=directory, **kwargs)

    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/api/"):
            self._proxy_request("GET")
            return
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        if self.path.startswith("/api/"):
            self._proxy_request("POST")
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def _proxy_request(self, method: str) -> None:
        if self.path == "/api/config":
            self._send_json(
                HTTPStatus.OK,
                {
                    "protocol_version": self.server.config.protocol_version,
                    "admin_enabled": bool(self.server.config.admin_token),
                    "registry_url": self.server.config.registry_url,
                },
            )
            return

        target_path = self._translate_api_path(self.path)
        if target_path is None:
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        body = None
        headers = {"Accept": "application/json"}
        if target_path.startswith("/v2/admin/") or target_path == "/v2/metrics":
            if not self.server.config.admin_token:
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "protocol_version": self.server.config.protocol_version,
                        "success": False,
                        "errors": [
                            {
                                "code": "ADMIN_DISABLED",
                                "message": "Dashboard admin views require USDP_ADMIN_TOKEN.",
                            }
                        ],
                    },
                )
                return
            headers["Authorization"] = f"Bearer {self.server.config.admin_token}"
        if method == "POST":
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length) if content_length else b""
            headers["Content-Type"] = self.headers.get("Content-Type", "application/json")

        req = request.Request(
            self.server.config.registry_url.rstrip("/") + target_path,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with request.urlopen(req, timeout=5) as response:
                payload = response.read()
                self.send_response(response.status)
                self.send_header("Content-Type", response.headers.get("Content-Type", "application/json"))
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
        except error.HTTPError as exc:
            payload = exc.read()
            self.send_response(exc.code)
            self.send_header("Content-Type", exc.headers.get("Content-Type", "application/json"))
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except error.URLError as exc:
            self._send_json(
                HTTPStatus.BAD_GATEWAY,
                {
                    "protocol_version": self.server.config.protocol_version,
                    "success": False,
                    "errors": [
                        {
                            "code": "REGISTRY_UNAVAILABLE",
                            "message": "Dashboard could not reach the configured registry.",
                            "details": {"reason": str(exc.reason)},
                        }
                    ],
                },
            )

    def _send_json(self, status: int, payload: dict) -> None:
        encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _translate_api_path(self, request_path: str) -> str | None:
        parsed = urlparse(request_path)
        path = parsed.path
        prefix = self._route_prefix()
        query = f"?{parsed.query}" if parsed.query else ""
        if path == "/api/health":
            return f"{prefix}/health{query}"
        if path == "/api/query":
            return f"{prefix}/query{query}"
        if path == "/api/metrics":
            return "/v2/metrics"
        if path == "/api/admin/audit":
            return f"/v2/admin/audit{query}"
        if path == "/api/admin/retention":
            return "/v2/admin/retention"
        if path == "/api/admin/purge":
            return "/v2/admin/purge"
        if path.startswith("/api/services/"):
            suffix = path.removeprefix("/api")
            return f"{prefix}{suffix}{query}"
        return None

    def _route_prefix(self) -> str:
        major = self.server.config.protocol_version.split(".", 1)[0]
        return f"/v{major}"


class DashboardHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], config: DashboardConfig) -> None:
        self.config = config
        self.static_dir = Path(__file__).resolve().parent / "static"
        handler = self._build_handler()
        super().__init__(server_address, handler)

    def _build_handler(self):
        directory = str(self.static_dir)

        class BoundHandler(DashboardRequestHandler):
            def __init__(self, *args, **kwargs) -> None:
                super().__init__(*args, directory=directory, **kwargs)

        return BoundHandler
