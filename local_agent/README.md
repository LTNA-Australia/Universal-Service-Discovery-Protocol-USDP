# USDP Local Agent

This folder contains the non-native publisher path for the USDP v2 release.

## What It Implements

- local agent framework for non-native services and devices
- built-in plugins for `api`, `printer`, `camera`, and `sensor`
- v2-aware registry publishing with idempotency headers
- state tracking for register, update, heartbeat, and deregister decisions
- optional per-cycle report file for operator visibility
- CLI for run-once and looped execution

## Config Model

Important fields:

- `registry_url`
- `publisher_token`
- `publisher_name`
- `protocol_version`
- `state_file`
- `report_file`
- `cycle_interval_seconds`
- `plugins`
