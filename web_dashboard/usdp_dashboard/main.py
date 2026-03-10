"""Dashboard entrypoint."""

from __future__ import annotations

import logging

from .config import load_config
from .server import DashboardHTTPServer


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = load_config()
    server = DashboardHTTPServer((config.host, config.port), config)
    logging.getLogger("usdp_dashboard").info("Dashboard listening on http://%s:%s", config.host, server.server_address[1])
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
