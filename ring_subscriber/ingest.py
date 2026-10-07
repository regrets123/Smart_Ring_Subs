"""Validate a gateway message and save its original bytes in SQLite."""

import json
import re
import sqlite3
from datetime import datetime, timezone


MAX_MESSAGE_BYTES = 64 * 1024  # Initial guard; revisit after measuring real syncs.
ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")


class InvalidMessage(ValueError):
    """The MQTT payload does not match the current envelope."""


class RecordConflict(ValueError):
    """One record identity was used for two different payloads."""


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
    if kind == "spo2":
        return set(data) == {"o2Perc"} and _integer_in_range(data["o2Perc"], 1, 100)
    if kind == "spo2History":
        return (
            set(data) == {"unknown", "days_ago", "samples"}
            and _integer_in_range(data["unknown"], 0, 255)
            and _integer_in_range(data["days_ago"], 0, 255)
            and isinstance(data["samples"], list)
            and bool(data["samples"])
            and all(
                isinstance(sample, dict)
                and set(sample) == {"min", "max"}
                and _integer_in_range(sample["min"], 0, 255)
                and _integer_in_range(sample["max"], 0, 255)
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
                and set(night) == {"days_ago", "start_min", "end_min", "stages"}
                and _integer_in_range(night["days_ago"], 0, 255)
                and _integer_in_range(night["start_min"], -32768, 32767)
                and _integer_in_range(night["end_min"], -32768, 32767)
                and isinstance(night["stages"], list)
                and bool(night["stages"])
                and all(
                    isinstance(stage, dict)
                    and set(stage) == {"stage", "duration_min"}
                    and stage["stage"] in ("light", "deep", "awake")
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
    for name in ("recordId", "deviceId", "gatewayId", "userId"):
        value = message.get(name)
        if not isinstance(value, str) or ID_PATTERN.fullmatch(value) is None:
            raise InvalidMessage(f"invalid {name}")
    kind = message.get("kind")
    if kind not in ("heartRate", "heartRateHistory", "spo2", "spo2History", "sleep"):
        raise InvalidMessage("unsupported reading kind")
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
    connection.execute(
        """CREATE TABLE IF NOT EXISTS received_messages (
            device_id TEXT NOT NULL,
            record_id TEXT NOT NULL,
            gateway_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            received_at TEXT NOT NULL,
            raw_payload BLOB NOT NULL,
            PRIMARY KEY (device_id, record_id)
        )"""
    )
    connection.commit()
    return connection


def store_message(connection: sqlite3.Connection, payload: bytes) -> str:
    """Return 'stored' or 'duplicate'; reject conflicting uses of an ID."""
    message = parse_message(payload)
    key = (message["deviceId"], message["recordId"])
    received_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    with connection:  # Commit on success; roll back if an operation fails.
        row = connection.execute(
            "SELECT raw_payload FROM received_messages WHERE device_id = ? AND record_id = ?",
            key,
        ).fetchone()
        if row is not None:
            if row[0] != payload:
                raise RecordConflict("record ID reused with different content")
            return "duplicate"
        connection.execute(
            """INSERT INTO received_messages
               (device_id, record_id, gateway_id, user_id, kind,
                observed_at, received_at, raw_payload)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (*key, message["gatewayId"], message["userId"], message["kind"],
             message["observedAt"], received_at, payload),
        )
    return "stored"
