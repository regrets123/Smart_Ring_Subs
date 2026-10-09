# MQTT message contract

This contract joins the [ESP32 gateway](https://github.com/regrets123/Smart_Ring_Gateway)
to the Raspberry Pi subscriber in this repository. The gateway reads the ring
over BLE, publishes one JSON record per MQTT message, and the subscriber
validates and stores it in SQLite. The HTTPS API returns the stored message
inside a `readings` entry with a separate `receivedAt` timestamp.

## Transport

- MQTT over TLS, QoS 1, no retained messages. The gateway waits for a broker
  acknowledgement; the subscriber acknowledges a valid message after its
  SQLite transaction completes.
- Both programs must use the **same topic**. Their source defaults currently
  differ: the gateway defaults to `gateway/readings`, while the subscriber
  defaults to `gateway/live/readings`.
- Broker acknowledgement confirms MQTT delivery, not SQLite storage. There is
  no gateway flash queue or database-level acknowledgement, so an outage can
  lose records already read from the ring.

## Envelope

```json
{
  "schemaVersion": 1,
  "recordId": "00000000000000000000000000000001",
  "deviceId": "ring-01",
  "gatewayId": "gateway-01",
  "userId": "user-01",
  "kind": "heartRate",
  "observedAt": "2026-10-09T11:40:50Z",
  "data": {"bpm": 72}
}
```

The values in this example are illustrative. `schemaVersion` must be `1`.
`deviceId`, `gatewayId`, and `userId` are
1–64 ASCII letters, digits, `_`, or `-`; they are claims in the payload, not
proof of authorization. `kind` selects one of the six data shapes below.
`observedAt` is a UTC ISO 8601 string and currently records the gateway's
**publish time**, including for history. It is not necessarily the ring's
measurement time. The Pi adds `receivedAt` when storing the message.

| `kind` | Required `data` | Notes |
| --- | --- | --- |
| `heartRate` | `bpm`: integer 1–255 | Live reading. |
| `spo2` | `o2Perc`: integer 1–100 | Live reading. |
| `heartRateHistory` | `utc_time`: unsigned 32-bit integer; `range`: byte; nonempty `samples`: byte array | Samples retain packet order, including trailing padding. |
| `hrvHistory` | `metric: "hrv_composite_ms"`; `interval_minutes`: 1–255; nonempty `samples` of `{days_ago, slot, value_ms}`; optional `probe_midnight_utc` | `days_ago` is a byte, `slot` is within the day at the given interval, and `value_ms` is 1–254. The metric is a firmware composite, not validated RMSSD. |
| `spo2History` | `days_ago`: byte; nonempty `samples` of `{slot, min, max}` | One day per message; hourly `slot` 0–23; `min` and `max` are bytes with `min <= max`. |
| `sleep` | Nonempty `nights` of `{days_ago, start_time, end_time, stages}` | Gateway sends one night per message. Times are `HH:MM`; each nonempty `stages` list contains `{stage, duration_min}`, with stage `light`, `deep`, `rem`, or `awake` and a byte duration. |

The subscriber rejects unsupported kinds, duplicate JSON fields, malformed
values, and payloads over 64 KiB. The exact range checks are in
[`parse_message`](../ring_subscriber/ingest.py); gateway examples are in
[`payloadExample.json`](https://github.com/regrets123/Smart_Ring_Gateway/blob/main/main/models/payloadExample.json).

## Record identity and storage

Live `heartRate` and `spo2` messages use 32 lowercase hexadecimal characters
for `recordId`. Historical messages use 16. The gateway derives historical
IDs from device, kind, and measurement day, so a later sync can update the
same day's record. Ring history often supplies day offsets or clock times
rather than an independently verified absolute timestamp; the gateway uses its
probe date to anchor these records.

SQLite makes `recordId` globally unique. An identical retry is a no-op. A
changed historical payload with the same device, gateway, user, and kind
replaces the stored payload; a changed live payload or a reused ID with a
different identity is rejected. Invalid or conflicting MQTT messages are
logged and acknowledged to avoid endless replay. On a database failure, the
subscriber disconnects without acknowledging the message.

## Current limits

The subscriber validates payload structure but does not yet check that a
`deviceId` is authorized for the claimed `userId` and `gatewayId`. Credentials
and certificates stay outside Git; broker account permissions should restrict
publishing and subscribing to the configured topic. The gateway retries failed
syncs, but without a durable queue it cannot guarantee delivery through a
Wi-Fi, broker, or Pi outage. Ring timestamp semantics, packet sizes, and
retention still need measurement against the physical device.
