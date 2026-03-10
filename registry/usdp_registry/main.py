"""Registry server entrypoint."""

from __future__ import annotations

import logging

from .config import load_config
from .server import RegistryHTTPServer


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = load_config()
    server = RegistryHTTPServer((config.host, config.port), config)
    server.logger.info("USDP registry listening on http://%s:%s", config.host, server.server_address[1])
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.logger.info("Shutting down registry server")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
