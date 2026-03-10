"""SQLite persistence for the registry."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3

from .config import RegistryConfig
from .errors import ConflictError
from .query import query_records
from .query_planner import plan_query
from .utils import isoformat_z, parse_datetime, utc_now


JSON_COLUMNS = {
    "endpoints": "endpoints_json",
    "capabilities": "capabilities_json",
    "tags": "tags_json",
    "auth": "auth_json",
    "metadata": "metadata_json",
    "publisher": "publisher_json",
    "publisher_identity": "publisher_identity_json",
    "provenance": "provenance_json",
    "extensions": "extensions_json",
    "location": "location_json",
    "timestamps": "timestamps_json",
}

SERVICE_TABLE_COLUMNS = {
    "publisher_identity_json": "ALTER TABLE services ADD COLUMN publisher_identity_json TEXT",
    "provenance_json": "ALTER TABLE services ADD COLUMN provenance_json TEXT",
    "extensions_json": "ALTER TABLE services ADD COLUMN extensions_json TEXT",
    "owner_publisher_id": "ALTER TABLE services ADD COLUMN owner_publisher_id TEXT",
    "record_state": "ALTER TABLE services ADD COLUMN record_state TEXT DEFAULT 'active'",
    "withdrawn_at": "ALTER TABLE services ADD COLUMN withdrawn_at TEXT",
    "purge_after": "ALTER TABLE services ADD COLUMN purge_after TEXT",
    "source_registry_id": "ALTER TABLE services ADD COLUMN source_registry_id TEXT",
    "is_federated": "ALTER TABLE services ADD COLUMN is_federated INTEGER DEFAULT 0",
}


class RegistryStorage:
    def __init__(self, config: RegistryConfig) -> None:
        self.config = config
        self.database_path = Path(config.database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS services (
                    service_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    service_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    endpoints_json TEXT NOT NULL,
                    capabilities_json TEXT NOT NULL,
                    tags_json TEXT,
                    auth_json TEXT,
                    metadata_json TEXT,
                    publisher_json TEXT,
                    publisher_identity_json TEXT,
                    provenance_json TEXT,
                    extensions_json TEXT,
                    location_json TEXT,
                    heartbeat_ttl_seconds INTEGER NOT NULL,
                    timestamps_json TEXT NOT NULL,
                    owner_publisher_id TEXT,
                    record_state TEXT NOT NULL DEFAULT 'active',
                    withdrawn_at TEXT,
                    purge_after TEXT,
                    source_registry_id TEXT,
                    is_federated INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at TEXT NOT NULL,
                    action TEXT NOT NULL,
                    actor_id TEXT,
                    actor_name TEXT,
                    actor_role TEXT,
                    protocol_version TEXT NOT NULL,
                    service_id TEXT,
                    details_json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_audit_events_occurred_at ON audit_events(occurred_at DESC);
                CREATE INDEX IF NOT EXISTS idx_audit_events_service_id ON audit_events(service_id);
                """
            )
            self._ensure_service_columns(connection)
            connection.executescript(
                """
                CREATE INDEX IF NOT EXISTS idx_services_type ON services(service_type);
                CREATE INDEX IF NOT EXISTS idx_services_status ON services(status);
                CREATE INDEX IF NOT EXISTS idx_services_owner ON services(owner_publisher_id);
                CREATE INDEX IF NOT EXISTS idx_services_state ON services(record_state);
                CREATE INDEX IF NOT EXISTS idx_services_purge_after ON services(purge_after);
                CREATE INDEX IF NOT EXISTS idx_services_source_registry ON services(source_registry_id);
                """
            )

    def create_service(self, record: dict, *, current_time: datetime | None = None) -> None:
        params = self._record_to_params(record, current_time=current_time)
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO services (
                        service_id, name, description, service_type, status,
                        endpoints_json, capabilities_json, tags_json, auth_json,
                        metadata_json, publisher_json, publisher_identity_json,
                        provenance_json, extensions_json, location_json,
                        heartbeat_ttl_seconds, timestamps_json, owner_publisher_id,
                        record_state, withdrawn_at, purge_after, source_registry_id, is_federated
                    ) VALUES (
                        :service_id, :name, :description, :service_type, :status,
                        :endpoints_json, :capabilities_json, :tags_json, :auth_json,
                        :metadata_json, :publisher_json, :publisher_identity_json,
                        :provenance_json, :extensions_json, :location_json,
                        :heartbeat_ttl_seconds, :timestamps_json, :owner_publisher_id,
                        :record_state, :withdrawn_at, :purge_after, :source_registry_id, :is_federated
                    )
                    """,
                    params,
                )
        except sqlite3.IntegrityError as exc:
            raise ConflictError("service_id already exists.") from exc

    def replace_service(self, record: dict, *, current_time: datetime | None = None) -> None:
        params = self._record_to_params(record, current_time=current_time)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE services
                SET
                    name = :name,
                    description = :description,
                    service_type = :service_type,
                    status = :status,
                    endpoints_json = :endpoints_json,
                    capabilities_json = :capabilities_json,
                    tags_json = :tags_json,
                    auth_json = :auth_json,
                    metadata_json = :metadata_json,
                    publisher_json = :publisher_json,
                    publisher_identity_json = :publisher_identity_json,
                    provenance_json = :provenance_json,
                    extensions_json = :extensions_json,
                    location_json = :location_json,
                    heartbeat_ttl_seconds = :heartbeat_ttl_seconds,
                    timestamps_json = :timestamps_json,
                    owner_publisher_id = :owner_publisher_id,
                    record_state = :record_state,
                    withdrawn_at = :withdrawn_at,
                    purge_after = :purge_after,
                    source_registry_id = :source_registry_id,
                    is_federated = :is_federated
                WHERE service_id = :service_id
                """,
                params,
            )

    def upsert_federated_service(self, record: dict, *, current_time: datetime | None = None) -> str:
        existing = self.get_service(record["service_id"])
        if existing is None:
            self.create_service(record, current_time=current_time)
            return "created"
        self.replace_service(record, current_time=current_time)
        return "updated"

    def get_service(self, service_id: str) -> dict | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM services WHERE service_id = ?", (service_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def list_services(self) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM services").fetchall()
        return [self._row_to_record(row) for row in rows]

    def query_services(self, query: dict, current_time: datetime, protocol_version: str) -> dict:
        plan = plan_query(query, current_time=current_time)
        with self._connect() as connection:
            if plan.requires_python_fallback:
                rows = connection.execute(
                    f"SELECT * FROM services WHERE {plan.where_sql} {plan.order_sql}",
                    plan.parameters,
                ).fetchall()
                records = [self._row_to_record(row) for row in rows]
                return query_records(records, query, current_time)

            count_row = connection.execute(
                f"SELECT COUNT(*) AS total FROM services WHERE {plan.where_sql}",
                plan.parameters,
            ).fetchone()
            rows = connection.execute(
                f"SELECT * FROM services WHERE {plan.where_sql} {plan.order_sql} LIMIT ? OFFSET ?",
                [*plan.parameters, plan.limit, plan.offset],
            ).fetchall()

        items = [self._row_to_record(row) for row in rows]
        return {
            "items": items,
            "count": len(items),
            "page": query.get("page", 1),
            "page_size": query.get("page_size", 25),
            "total": int(count_row["total"]) if count_row is not None else 0,
        }

    def expire_stale_services(self, current_time: datetime) -> int:
        now_text = isoformat_z(current_time)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT service_id, timestamps_json
                FROM services
                WHERE status NOT IN ('offline', 'unknown')
                  AND json_extract(timestamps_json, '$.expires_at') <= ?
                """,
                (now_text,),
            ).fetchall()
            expired = 0
            purge_after = isoformat_z(current_time + timedelta(seconds=self.config.stale_retention_seconds))
            for row in rows:
                timestamps = json.loads(row["timestamps_json"])
                timestamps["updated_at"] = now_text
                connection.execute(
                    """
                    UPDATE services
                    SET status = 'unknown',
                        record_state = 'stale',
                        purge_after = ?,
                        timestamps_json = ?
                    WHERE service_id = ?
                    """,
                    (purge_after, json.dumps(timestamps, sort_keys=True), row["service_id"]),
                )
                expired += 1
        return expired

    def purge_retired_services(self, current_time: datetime) -> list[dict]:
        now_text = isoformat_z(current_time)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT service_id, record_state
                FROM services
                WHERE purge_after IS NOT NULL AND purge_after <= ?
                """,
                (now_text,),
            ).fetchall()
            purged = [{"service_id": row["service_id"], "record_state": row["record_state"]} for row in rows]
            for row in purged:
                connection.execute("DELETE FROM services WHERE service_id = ?", (row["service_id"],))
        return purged

    def append_audit_event(
        self,
        *,
        action: str,
        actor_id: str | None,
        actor_name: str | None,
        actor_role: str | None,
        protocol_version: str,
        service_id: str | None = None,
        details: dict | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO audit_events (
                    occurred_at, action, actor_id, actor_name, actor_role,
                    protocol_version, service_id, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    isoformat_z(utc_now()),
                    action,
                    actor_id,
                    actor_name,
                    actor_role,
                    protocol_version,
                    service_id,
                    json.dumps(details, sort_keys=True) if details is not None else None,
                ),
            )

    def list_audit_events(self, *, limit: int = 50) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT event_id, occurred_at, action, actor_id, actor_name, actor_role,
                       protocol_version, service_id, details_json
                FROM audit_events
                ORDER BY occurred_at DESC, event_id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        events = []
        for row in rows:
            event = {
                "event_id": row["event_id"],
                "occurred_at": row["occurred_at"],
                "action": row["action"],
                "actor": {
                    "actor_id": row["actor_id"],
                    "actor_name": row["actor_name"],
                    "actor_role": row["actor_role"],
                },
                "protocol_version": row["protocol_version"],
            }
            if row["service_id"] is not None:
                event["service_id"] = row["service_id"]
            if row["details_json"] is not None:
                event["details"] = json.loads(row["details_json"])
            events.append(event)
        return events

    def get_health_summary(self, current_time: datetime) -> dict:
        now_text = isoformat_z(current_time)
        status_counts = {"online": 0, "degraded": 0, "offline": 0, "unknown": 0}
        state_counts = {"active": 0, "stale": 0, "withdrawn": 0}
        publishers: set[str] = set()
        federated_services = 0

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT status, record_state, timestamps_json, publisher_json,
                       publisher_identity_json, is_federated
                FROM services
                """
            ).fetchall()
            total_services = len(rows)
            audit_event_count = connection.execute("SELECT COUNT(*) AS total FROM audit_events").fetchone()["total"]

        active_services = 0
        inactive_services = 0
        stale_services = 0
        withdrawn_services = 0
        for row in rows:
            status = row["status"]
            if status in status_counts:
                status_counts[status] += 1
            state = row["record_state"] or "active"
            if state in state_counts:
                state_counts[state] += 1
            timestamps = json.loads(row["timestamps_json"])
            is_active = status != "offline" and timestamps["expires_at"] > now_text
            if is_active:
                active_services += 1
            else:
                inactive_services += 1
            if state == "stale":
                stale_services += 1
            if state == "withdrawn":
                withdrawn_services += 1
            if row["is_federated"]:
                federated_services += 1

            publisher_id = None
            if row["publisher_identity_json"] is not None:
                publisher_id = json.loads(row["publisher_identity_json"]).get("publisher_id")
            elif row["publisher_json"] is not None:
                publisher_id = json.loads(row["publisher_json"]).get("publisher_id")
            if publisher_id:
                publishers.add(publisher_id)

        return {
            "total_services": total_services,
            "active_services": active_services,
            "inactive_services": inactive_services,
            "stale_services": stale_services,
            "withdrawn_services": withdrawn_services,
            "federated_services": federated_services,
            "unique_publishers": len(publishers),
            "status_counts": status_counts,
            "state_counts": state_counts,
            "audit_event_count": int(audit_event_count),
        }

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _record_to_params(self, record: dict, *, current_time: datetime | None = None) -> dict:
        current_time = current_time or utc_now()
        state = self._derive_record_state(record, current_time)
        owner_publisher_id = None
        if "publisher_identity" in record:
            owner_publisher_id = record["publisher_identity"].get("publisher_id")
        elif "publisher" in record:
            owner_publisher_id = record["publisher"].get("publisher_id")

        withdrawn_at = None
        purge_after = None
        if state == "withdrawn":
            withdrawn_at = record["timestamps"]["updated_at"]
            purge_after = isoformat_z(current_time + timedelta(seconds=self.config.withdrawn_retention_seconds))
        elif state == "stale":
            purge_after = isoformat_z(current_time + timedelta(seconds=self.config.stale_retention_seconds))

        provenance = record.get("provenance")
        is_federated = 1 if provenance and provenance.get("source_kind") == "federated_registry" else 0
        source_registry_id = provenance.get("source_registry") if provenance else None

        return {
            "service_id": record["service_id"],
            "name": record["name"],
            "description": record.get("description"),
            "service_type": record["service_type"],
            "status": record["status"],
            "endpoints_json": json.dumps(record["endpoints"], sort_keys=True),
            "capabilities_json": json.dumps(record["capabilities"], sort_keys=True),
            "tags_json": json.dumps(record.get("tags")) if "tags" in record else None,
            "auth_json": json.dumps(record.get("auth"), sort_keys=True) if "auth" in record else None,
            "metadata_json": json.dumps(record.get("metadata"), sort_keys=True) if "metadata" in record else None,
            "publisher_json": json.dumps(record.get("publisher"), sort_keys=True) if "publisher" in record else None,
            "publisher_identity_json": json.dumps(record.get("publisher_identity"), sort_keys=True) if "publisher_identity" in record else None,
            "provenance_json": json.dumps(provenance, sort_keys=True) if provenance is not None else None,
            "extensions_json": json.dumps(record.get("extensions"), sort_keys=True) if "extensions" in record else None,
            "location_json": json.dumps(record.get("location"), sort_keys=True) if "location" in record else None,
            "heartbeat_ttl_seconds": int(record.get("heartbeat_ttl_seconds", 90)),
            "timestamps_json": json.dumps(record["timestamps"], sort_keys=True),
            "owner_publisher_id": owner_publisher_id,
            "record_state": state,
            "withdrawn_at": withdrawn_at,
            "purge_after": purge_after,
            "source_registry_id": source_registry_id,
            "is_federated": is_federated,
        }

    def _ensure_service_columns(self, connection: sqlite3.Connection) -> None:
        existing_columns = {row["name"] for row in connection.execute("PRAGMA table_info(services)").fetchall()}
        for column_name, statement in SERVICE_TABLE_COLUMNS.items():
            if column_name not in existing_columns:
                connection.execute(statement)

    def _row_to_record(self, row: sqlite3.Row) -> dict:
        record = {
            "service_id": row["service_id"],
            "name": row["name"],
            "service_type": row["service_type"],
            "status": row["status"],
            "endpoints": json.loads(row["endpoints_json"]),
            "capabilities": json.loads(row["capabilities_json"]),
            "heartbeat_ttl_seconds": row["heartbeat_ttl_seconds"],
            "timestamps": json.loads(row["timestamps_json"]),
        }
        if row["description"] is not None:
            record["description"] = row["description"]
        for field, column in JSON_COLUMNS.items():
            if field in {"endpoints", "capabilities", "timestamps"}:
                continue
            if row[column] is not None:
                record[field] = json.loads(row[column])
        return record

    def _derive_record_state(self, record: dict, current_time: datetime) -> str:
        if record["status"] == "offline":
            return "withdrawn"
        if record["status"] == "unknown":
            return "stale"
        if parse_datetime(record["timestamps"]["expires_at"]) <= current_time:
            return "stale"
        return "active"
