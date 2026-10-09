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
    b'{"schemaVersion":1,"recordId":"00000000000000000000000000000001",'
    b'"deviceId":"ring-01","gatewayId":"gateway-01","userId":"user-01",'
    b'"kind":"heartRate","observedAt":"2026-10-01T12:00:00Z",'
    b'"data":{"bpm":72}}'
)

HRV_PAYLOAD = (
    b'{"schemaVersion":1,"recordId":"0000000000000006",'
    b'"deviceId":"ring-01","gatewayId":"gateway-01","userId":"user-01",'
    b'"kind":"hrvHistory","observedAt":"2026-10-08T12:00:05Z",'
    b'"data":{"metric":"hrv_composite_ms","interval_minutes":30,'
    b'"probe_midnight_utc":1791417600,"samples":['
    b'{"days_ago":0,"slot":0,"value_ms":43},'
    b'{"days_ago":0,"slot":2,"value_ms":39},'
    b'{"days_ago":1,"slot":20,"value_ms":45}]}}'
)

SLEEP_PAYLOAD = (
    b'{"schemaVersion":1,"recordId":"41615c40972cc3df",'
    b'"deviceId":"ring-01","gatewayId":"gateway-01","userId":"user-01",'
    b'"kind":"sleep","observedAt":"2026-10-07T12:00:04Z",'
    b'"data":{"nights":[{"days_ago":1,"start_time":"23:00",'
    b'"end_time":"07:00","stages":['
    b'{"stage":"light","duration_min":180},'
    b'{"stage":"deep","duration_min":180},'
    b'{"stage":"rem","duration_min":60},'
    b'{"stage":"awake","duration_min":60}]}]}}'
)

SPO2_HISTORY_PAYLOAD = (
    b'{"schemaVersion":1,"recordId":"42e062e1a35a9bce",'
    b'"deviceId":"ring-01","gatewayId":"gateway-01","userId":"user-01",'
    b'"kind":"spo2History","observedAt":"2026-10-08T12:00:03Z",'
    b'"data":{"days_ago":2,"samples":['
    b'{"slot":0,"min":99,"max":99},'
    b'{"slot":12,"min":99,"max":99}]}}'
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

    def test_stores_updated_sleep_payload(self):
        self.assertEqual(store_message(self.connection, SLEEP_PAYLOAD), "stored")

    def test_stores_updated_spo2_history_payload(self):
        self.assertEqual(store_message(self.connection, SPO2_HISTORY_PAYLOAD), "stored")

    def test_rejects_invalid_sleep_times_and_spo2_history_samples(self):
        invalid_sleep = json.loads(SLEEP_PAYLOAD)
        invalid_sleep["data"]["nights"][0]["start_time"] = "24:00"
        invalid_spo2 = json.loads(SPO2_HISTORY_PAYLOAD)
        invalid_spo2["data"]["samples"][0]["slot"] = 24
        for message in (invalid_sleep, invalid_spo2):
            with self.subTest(kind=message["kind"]):
                with self.assertRaises(InvalidMessage):
                    store_message(self.connection, json.dumps(message).encode())

    def test_stores_hrv_history_without_optional_date_anchor(self):
        message = json.loads(HRV_PAYLOAD)
        message["recordId"] = "0000000000000007"
        del message["data"]["probe_midnight_utc"]
        self.assertEqual(
            store_message(self.connection, json.dumps(message).encode()),
            "stored",
        )

    def test_updates_existing_hrv_history_when_later_sync_adds_samples(self):
        self.assertEqual(store_message(self.connection, HRV_PAYLOAD), "stored")
        updated_message = json.loads(HRV_PAYLOAD)
        updated_message["observedAt"] = "2026-10-08T13:00:05Z"
        updated_message["data"]["samples"].append(
            {"days_ago": 0, "slot": 4, "value_ms": 41}
        )
        updated_payload = json.dumps(updated_message).encode()

        self.assertEqual(store_message(self.connection, updated_payload), "updated")
        row = self.connection.execute(
            """SELECT record_id, observed_at, raw_payload
               FROM received_messages"""
        ).fetchone()
        self.assertEqual(row, (
            "0000000000000006",
            "2026-10-08T13:00:05Z",
            updated_payload,
        ))
        count = self.connection.execute(
            "SELECT COUNT(*) FROM received_messages"
        ).fetchone()[0]
        self.assertEqual(count, 1)
        self.assertEqual(store_message(self.connection, updated_payload), "duplicate")

    def test_record_id_length_matches_live_or_historical_kind(self):
        invalid_live = PAYLOAD.replace(
            b'"recordId":"00000000000000000000000000000001"',
            b'"recordId":"0000000000000001"',
        )
        invalid_historical = HRV_PAYLOAD.replace(
            b'"recordId":"0000000000000006"',
            b'"recordId":"00000000000000000000000000000006"',
        )
        for payload in (invalid_live, invalid_historical):
            with self.subTest(payload=payload):
                with self.assertRaises(InvalidMessage):
                    store_message(self.connection, payload)

    def test_record_id_is_unique_across_devices(self):
        store_message(self.connection, HRV_PAYLOAD)
        other_device_payload = HRV_PAYLOAD.replace(b'"ring-01"', b'"ring-02"')
        with self.assertRaises(RecordConflict):
            store_message(self.connection, other_device_payload)

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

    def test_old_prototype_schema_is_preserved_and_requires_manual_reset(self):
        legacy_path = Path(self.directory.name) / "legacy.sqlite3"
        legacy = sqlite3.connect(legacy_path)
        legacy.execute(
            """CREATE TABLE received_messages (
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
        legacy.execute(
            """INSERT INTO received_messages VALUES
               ('ring-01', 'old-random-id', 'gateway-01', 'user-01',
                'heartRate', '2026-10-01T12:00:00Z',
                '2026-10-01T12:00:01+00:00', ?)""",
            (PAYLOAD,),
        )
        legacy.commit()
        legacy.close()

        with self.assertRaisesRegex(RuntimeError, "reset.*manually"):
            open_database(str(legacy_path))

        preserved = sqlite3.connect(legacy_path)
        try:
            self.assertEqual(
                preserved.execute(
                    "SELECT COUNT(*) FROM received_messages"
                ).fetchone()[0],
                1,
            )
        finally:
            preserved.close()


if __name__ == "__main__":
    unittest.main()
