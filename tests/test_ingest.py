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
