"""Validate a gateway message and save its original bytes in SQLite."""

import json
import re
import sqlite3
from datetime import datetime, timezone


MAX_MESSAGE_BYTES = 64 * 1024  # Initial guard; revisit after measuring real syncs.
LIVE_ID_PATTERN = re.compile(r"[0-9a-f]{32}\Z")
HISTORICAL_ID_PATTERN = re.compile(r"[0-9a-f]{16}\Z")
DATABASE_SCHEMA_VERSION = 2


class InvalidMessage(ValueError):
    """The MQTT payload does not match the current envelope."""


class RecordConflict(ValueError):
    """A record ID was reused for a different identity or live payload."""


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for name, value in pairs:
        if name in result:
            raise InvalidMessage("JSON contains a duplicate field")
        result[name] = value
    return result


def _utc_timestamp(value: object) -> bool:
    if not isinstance(value, str) or not value.endswith("Z"):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    offset = parsed.utcoffset()
    return offset is not None and offset.total_seconds() == 0


def _integer_in_range(value: object, minimum: int, maximum: int) -> bool:
    return type(value) is int and minimum <= value <= maximum


def _valid_clock_time(value: object) -> bool:
    if not isinstance(value, str) or re.fullmatch(r"\d{2}:\d{2}", value) is None:
        return False
    hour, minute = (int(part) for part in value.split(":"))
    return hour < 24 and minute < 60


def _valid_record_id(kind: object, value: object) -> bool:
    if not isinstance(value, str):
        return False
    pattern = LIVE_ID_PATTERN if kind in ("heartRate", "spo2") else HISTORICAL_ID_PATTERN
    return pattern.fullmatch(value) is not None


def _valid_data(kind: str, data: dict) -> bool:
    if kind == "heartRate":
        return set(data) == {"bpm"} and _integer_in_range(data["bpm"], 1, 255)
    if kind == "heartRateHistory":
        return (
            set(data) == {"utc_time", "range", "samples"}
            and _integer_in_range(data["utc_time"], 0, 0xFFFFFFFF)
            and _integer_in_range(data["range"], 0, 255)
            and isinstance(data["samples"], list)
            and bool(data["samples"])
            and all(_integer_in_range(sample, 0, 255) for sample in data["samples"])
        )
    if kind == "hrvHistory":
        if set(data) not in (
            {"metric", "interval_minutes", "samples"},
            {"metric", "interval_minutes", "probe_midnight_utc", "samples"},
        ):
            return False
        interval = data["interval_minutes"]
        return (
            data["metric"] == "hrv_composite_ms"
            and _integer_in_range(interval, 1, 255)
            and (
                "probe_midnight_utc" not in data
                or _integer_in_range(data["probe_midnight_utc"], 0, 0xFFFFFFFF)
            )
            and isinstance(data["samples"], list)
            and bool(data["samples"])
            and all(
                isinstance(sample, dict)
                and set(sample) == {"days_ago", "slot", "value_ms"}
                and _integer_in_range(sample["days_ago"], 0, 255)
                and _integer_in_range(sample["slot"], 0, 0xFFFF)
                and sample["slot"] < 1440 // interval
                and _integer_in_range(sample["value_ms"], 1, 254)
                for sample in data["samples"]
            )
        )
    if kind == "spo2":
        return set(data) == {"o2Perc"} and _integer_in_range(data["o2Perc"], 1, 100)
    if kind == "spo2History":
        return (
            set(data) == {"days_ago", "samples"}
            and _integer_in_range(data["days_ago"], 0, 255)
            and isinstance(data["samples"], list)
            and bool(data["samples"])
            and all(
                isinstance(sample, dict)
                and set(sample) == {"slot", "min", "max"}
                and _integer_in_range(sample["slot"], 0, 23)
                and _integer_in_range(sample["min"], 0, 255)
                and _integer_in_range(sample["max"], 0, 255)
                and sample["min"] <= sample["max"]
                for sample in data["samples"]
            )
        )
    if kind == "sleep":
        return (
            set(data) == {"nights"}
            and isinstance(data["nights"], list)
            and bool(data["nights"])
            and all(
                isinstance(night, dict)
                and set(night) == {"days_ago", "start_time", "end_time", "stages"}
                and _integer_in_range(night["days_ago"], 0, 255)
                and _valid_clock_time(night["start_time"])
                and _valid_clock_time(night["end_time"])
                and isinstance(night["stages"], list)
                and bool(night["stages"])
                and all(
                    isinstance(stage, dict)
                    and set(stage) == {"stage", "duration_min"}
                    and stage["stage"] in ("light", "deep", "rem", "awake")
                    and _integer_in_range(stage["duration_min"], 0, 255)
                    for stage in night["stages"]
                )
                for night in data["nights"]
            )
        )
    return False


def parse_message(payload: bytes) -> dict:
    """Validate the gateway envelope and its currently published reading kinds."""
    if len(payload) > MAX_MESSAGE_BYTES:
        raise InvalidMessage("message exceeds the 64 KiB limit")
    try:
        message = json.loads(payload.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidMessage("message is not UTF-8 JSON") from exc

    if not isinstance(message, dict):
        raise InvalidMessage("top-level JSON must be an object")
    if type(message.get("schemaVersion")) is not int or message["schemaVersion"] != 1:
        raise InvalidMessage("unsupported schemaVersion")
    kind = message.get("kind")
    if kind not in (
        "heartRate",
        "heartRateHistory",
        "hrvHistory",
        "spo2",
        "spo2History",
        "sleep",
    ):
        raise InvalidMessage("unsupported reading kind")
    if not _valid_record_id(kind, message.get("recordId")):
        raise InvalidMessage("invalid recordId for reading kind")
    for name in ("deviceId", "gatewayId", "userId"):
        value = message.get(name)
        if (
            not isinstance(value, str)
            or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value) is None
        ):
            raise InvalidMessage(f"invalid {name}")
    if not _utc_timestamp(message.get("observedAt")):
        raise InvalidMessage("observedAt must be a UTC ISO 8601 timestamp")
    data = message.get("data")
    if not isinstance(data, dict):
        raise InvalidMessage("data must be an object")
    if not _valid_data(kind, data):
        raise InvalidMessage(f"invalid {kind} data")
    return message


def open_database(path: str) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    try:
        schema_version = connection.execute("PRAGMA user_version").fetchone()[0]
        table_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'received_messages'"
        ).fetchone() is not None
        if schema_version not in (0, DATABASE_SCHEMA_VERSION) or (
            schema_version == 0 and table_exists
        ):
            raise RuntimeError(
                "Database schema is incompatible; stop the subscriber and reset "
                "the prototype database manually before restarting."
            )

        with connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS received_messages (
                    record_id TEXT PRIMARY KEY,
                    device_id TEXT NOT NULL,
                    gateway_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    raw_payload BLOB NOT NULL
                )"""
            )
            columns = connection.execute(
                "PRAGMA table_info(received_messages)"
            ).fetchall()
            column_names = {column[1] for column in columns}
            primary_key_columns = [
                column[1] for column in sorted(columns, key=lambda column: column[5])
                if column[5]
            ]
            if column_names != {
                "record_id",
                "device_id",
                "gateway_id",
                "user_id",
                "kind",
                "observed_at",
                "received_at",
                "raw_payload",
            } or primary_key_columns != ["record_id"]:
                raise RuntimeError(
                    "Database schema is incompatible; stop the subscriber and reset "
                    "the prototype database manually before restarting."
                )
            connection.execute(f"PRAGMA user_version = {DATABASE_SCHEMA_VERSION}")
        return connection
    except Exception:
        connection.close()
        raise


def store_message(connection: sqlite3.Connection, payload: bytes) -> str:
    """Return 'stored', 'updated', or 'duplicate'; reject identity conflicts."""
    message = parse_message(payload)
    record_id = message["recordId"]
    received_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    with connection:  # Commit on success; roll back if an operation fails.
        row = connection.execute(
            """SELECT device_id, gateway_id, user_id, kind, raw_payload
               FROM received_messages WHERE record_id = ?""",
            (record_id,),
        ).fetchone()
        if row is not None:
            if row[:4] != (
                message["deviceId"],
                message["gatewayId"],
                message["userId"],
                message["kind"],
            ):
                raise RecordConflict("record ID reused with different content")
            if row[4] == payload:
                return "duplicate"
            if message["kind"] not in ("heartRate", "spo2"):
                connection.execute(
                    """UPDATE received_messages
                       SET observed_at = ?, received_at = ?, raw_payload = ?
                       WHERE record_id = ?""",
                    (message["observedAt"], received_at, payload, record_id),
                )
                return "updated"
            raise RecordConflict("record ID reused with different content")
        connection.execute(
            """INSERT INTO received_messages
               (device_id, record_id, gateway_id, user_id, kind,
                observed_at, received_at, raw_payload)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                message["deviceId"],
                record_id,
                message["gatewayId"],
                message["userId"],
                message["kind"],
                message["observedAt"],
                received_at,
                payload,
            ),
        )
    return "stored"
