"""Plugin base classes."""

from __future__ import annotations


class PluginBase:
    plugin_name = "base"

    def __init__(self, config: dict, publisher_name: str) -> None:
        self.config = config
        self.publisher_name = publisher_name

    def discover(self) -> list[dict]:
        raise NotImplementedError
