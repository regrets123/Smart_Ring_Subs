import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ring_subscriber.ingest import (
    MAX_MESSAGE_BYTES,
    InvalidMessage,
    RecordConflict,
    open_database,
    store_message,
)


PAYLOAD = (
    b'{"schemaVersion":1,"recordId":"example-stable-record-id-001",'
    b'"deviceId":"ring-01","gatewayId":"gateway-01","userId":"user-01",'
    b'"kind":"heartRate","observedAt":"2026-10-01T12:00:00Z",'
    b'"data":{"bpm":72}}'
)

HRV_PAYLOAD = (
    b'{"schemaVersion":1,"recordId":"00000000000000000000000000000006",'
    b'"deviceId":"ring-01","gatewayId":"gateway-01","userId":"user-01",'
    b'"kind":"hrvHistory","observedAt":"2026-10-08T12:00:05Z",'
    b'"data":{"metric":"hrv_composite_ms","interval_minutes":30,'
    b'"probe_midnight_utc":1791417600,"samples":['
    b'{"days_ago":0,"slot":0,"value_ms":43},'
    b'{"days_ago":0,"slot":2,"value_ms":39},'
    b'{"days_ago":1,"slot":20,"value_ms":45}]}}'
)


class IngestTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "ring.sqlite3"
        self.connection = open_database(str(self.database))
        self.addCleanup(self.connection.close)

    def test_saves_exact_payload_and_receipt_time(self):
        self.assertEqual(store_message(self.connection, PAYLOAD), "stored")
        row = self.connection.execute(
            "SELECT raw_payload, received_at FROM received_messages"
        ).fetchone()
        self.assertEqual(row[0], PAYLOAD)
        self.assertTrue(row[1].endswith("+00:00"))

    def test_stores_gateway_hrv_history_payload(self):
        self.assertEqual(store_message(self.connection, HRV_PAYLOAD), "stored")
        row = self.connection.execute(
            "SELECT kind, raw_payload FROM received_messages"
        ).fetchone()
        self.assertEqual(row, ("hrvHistory", HRV_PAYLOAD))

    def test_stores_hrv_history_without_optional_date_anchor(self):
        message = json.loads(HRV_PAYLOAD)
        message["recordId"] = "00000000000000000000000000000007"
        del message["data"]["probe_midnight_utc"]
        self.assertEqual(
            store_message(self.connection, json.dumps(message).encode()),
            "stored",
        )

    def test_rejects_invalid_hrv_history_samples(self):
        original = json.loads(HRV_PAYLOAD)
        invalid_data = (
            {**original["data"], "metric": "rmssd"},
            {**original["data"], "interval_minutes": 0},
            {
                **original["data"],
                "samples": [{**original["data"]["samples"][0], "slot": 48}],
            },
            {
                **original["data"],
                "samples": [{**original["data"]["samples"][0], "value_ms": 255}],
            },
        )
        for data in invalid_data:
            with self.subTest(data=data):
                message = {**original, "data": data}
                with self.assertRaises(InvalidMessage):
                    store_message(self.connection, json.dumps(message).encode())

    def test_same_record_is_stored_once(self):
        self.assertEqual(store_message(self.connection, PAYLOAD), "stored")
        self.assertEqual(store_message(self.connection, PAYLOAD), "duplicate")
        count = self.connection.execute("SELECT COUNT(*) FROM received_messages").fetchone()[0]
        self.assertEqual(count, 1)

    def test_changed_payload_with_same_identity_is_rejected(self):
        store_message(self.connection, PAYLOAD)
        with self.assertRaises(RecordConflict):
            store_message(self.connection, PAYLOAD.replace(b'"bpm":72', b'"bpm":73'))

    def test_malformed_and_large_payloads_are_rejected(self):
        for payload in (
            b"not json",
            b"[]",
            b"x" * (MAX_MESSAGE_BYTES + 1),
            PAYLOAD.replace(b'"bpm":72', b'"bpm":"72"'),
            PAYLOAD.replace(b'"bpm":72', b'"bpm":72,"bpm":73'),
        ):
            with self.subTest(payload=payload[:20]):
                with self.assertRaises(InvalidMessage):
                    store_message(self.connection, payload)
        count = self.connection.execute("SELECT COUNT(*) FROM received_messages").fetchone()[0]
        self.assertEqual(count, 0)

    def test_database_survives_reopen(self):
        store_message(self.connection, PAYLOAD)
        self.connection.close()
        reopened = sqlite3.connect(self.database)
        try:
            row = reopened.execute("SELECT raw_payload FROM received_messages").fetchone()
        finally:
            reopened.close()
        self.assertEqual(row[0], PAYLOAD)


if __name__ == "__main__":
    unittest.main()
