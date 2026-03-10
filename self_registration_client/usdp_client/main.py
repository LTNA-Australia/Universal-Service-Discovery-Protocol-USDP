"""CLI for the self-registration client."""

from __future__ import annotations

import argparse
import json
import signal
import time
from pathlib import Path

from .builders import build_service_update_changes
from .client import USDPRegistrationClient
from .config import load_config
from .heartbeat import HeartbeatWorker


def main() -> None:
    parser = argparse.ArgumentParser(description="USDP self-registration client")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command_name in ("register", "register-or-update", "update"):
        command = subparsers.add_parser(command_name)
        command.add_argument("service_file", type=Path)

    heartbeat = subparsers.add_parser("heartbeat")
    heartbeat.add_argument("service_id")
    heartbeat.add_argument("--status")

    deregister = subparsers.add_parser("deregister")
    deregister.add_argument("service_id")
    deregister.add_argument("--reason")

    run_heartbeat = subparsers.add_parser("run-heartbeat")
    run_heartbeat.add_argument("service_id")
    run_heartbeat.add_argument("--interval", type=float, default=30.0)
    run_heartbeat.add_argument("--status")

    args = parser.parse_args()
    client = USDPRegistrationClient(load_config())

    if args.command == "register":
        payload = _load_json(args.service_file)
        print(json.dumps(client.register_service(payload), indent=2))
        return

    if args.command == "register-or-update":
        payload = _load_json(args.service_file)
        print(json.dumps(client.register_or_update_service(payload), indent=2))
        return

    if args.command == "update":
        payload = _load_json(args.service_file)
        service_id = payload["service_id"]
        changes = build_service_update_changes(payload)
        print(json.dumps(client.update_service(service_id, changes), indent=2))
        return

    if args.command == "heartbeat":
        print(json.dumps(client.heartbeat(args.service_id, args.status), indent=2))
        return

    if args.command == "deregister":
        print(json.dumps(client.deregister_service(args.service_id, args.reason), indent=2))
        return

    worker = HeartbeatWorker(client, args.service_id, args.interval, args.status)
    worker.start()

    should_stop = False

    def handle_signal(signum, frame):  # noqa: ANN001, ARG001
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        while not should_stop:
            time.sleep(0.5)
    finally:
        worker.stop()
        print(json.dumps(worker.snapshot().to_dict(), indent=2))


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


if __name__ == "__main__":
    main()
