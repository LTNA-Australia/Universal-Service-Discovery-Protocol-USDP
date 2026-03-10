"""CLI for the USDP local agent."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .agent import LocalAgent
from .config import load_agent_config


def main() -> None:
    parser = argparse.ArgumentParser(description="USDP local agent")
    parser.add_argument("command", choices=["run-once", "run-loop"])
    parser.add_argument("config_file", type=Path)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = load_agent_config(args.config_file)
    agent = LocalAgent(config)

    if args.command == "run-once":
        summary = agent.run_once()
        logging.getLogger("usdp_agent").info("Run-once summary: %s", summary)
        return

    agent.run_loop()


if __name__ == "__main__":
    main()
