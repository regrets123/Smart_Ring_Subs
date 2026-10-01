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


def parse_message(payload: bytes) -> dict:
    """Check the envelope only; ring measurement fields remain provisional."""
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
    if message.get("kind") != "heartRate":
        raise InvalidMessage("only the simulated heartRate kind is supported")
    if not _utc_timestamp(message.get("observedAt")):
        raise InvalidMessage("observedAt must be a UTC ISO 8601 timestamp")
    if not isinstance(message.get("data"), dict):
        raise InvalidMessage("data must be an object")
    if set(message["data"]) != {"bpm"} or type(message["data"]["bpm"]) is not int:
        raise InvalidMessage("simulated heartRate data must contain an integer bpm")
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
