# MQTT message contract: first draft

This is the proposed boundary between the ESP32 gateway and the Raspberry Pi
subscriber. It is a design draft, not yet implemented by either program. The
real COLMI R09 measurement fields and record identity must be checked against
the ring when it arrives.

## What exists today

The gateway publishes a temporary mock JSON object to `gateway/mock/readings`:

```json
{"deviceId":"ring-01","gatewayId":"gateway-01","userId":"user-01","reading":42}
```

It uses MQTT QoS 1 and does not retain the message. This payload has no stable
record ID, timestamp, or schema version. It can test connectivity and parsing,
but it cannot prove that repeated deliveries or repeated ring syncs will produce
only one stored measurement.

## Proposed first real envelope

Publish one *logical record* per MQTT message, even if a ring sync sends many
messages in a burst. A logical record may itself contain several values, such as
a sleep interval. This example uses a **provisional heart-rate record**; the
gateway does not publish this format yet:

```json
{
  "schemaVersion": 1,
  "recordId": "example-stable-record-id-001",
  "deviceId": "ring-01",
  "gatewayId": "gateway-01",
  "userId": "user-01",
  "kind": "heartRate",
  "observedAt": "2026-10-01T12:00:00Z",
  "data": {"bpm": 72}
}
```

`recordId` here is illustrative. Its real derivation must keep the same ID for
the same ring record across later syncs, even after a gateway restart.

| Field | Meaning |
| --- | --- |
| `schemaVersion` | Selects the contract the subscriber must validate. |
| `recordId` | Identifies the same logical ring record across MQTT redelivery, gateway retries, and later ring syncs. It must not change when that record is resent. |
| `deviceId` | Identifies the source ring. |
| `gatewayId` | Identifies the ESP32 that sent the record. |
| `userId` | Identifies the owner; the subscriber must verify that this device is allowed for this user. |
| `kind` | Selects the meaning and parser for `data`. Proposed values appear below; `mock` remains only for pipeline tests. |
| `observedAt` | When the measurement occurred, in UTC, if the ring can supply or support a trustworthy time. |
| `data` | Measurement fields for this `kind`; real shapes remain open. |

The subscriber adds its own `receivedAt` timestamp. It must not substitute
`receivedAt` for `observedAt`: a twice-daily sync can deliver old measurements.
Whether every real record has a trustworthy `observedAt` is still unverified.
If it does not, the field may be `null` in the eventual contract; the parser
must not invent a measurement time from the arrival time.

## Candidate records from the reference projects

These are **candidate `kind` values and fields**, not a claim that our R09
firmware exposes every one or that its bytes have already been decoded. The
gateway will parse BLE data and publish readable JSON; the Pi will validate
that JSON. We will settle exact names, types, units, timestamp rules, and
sample grouping from captures of our own ring.

| Candidate `kind` | Likely `data` | Evidence and remaining uncertainty |
| --- | --- | --- |
| `heartRate` | Heart rate in beats per minute; possibly a sequence of timed samples. | The [R02-family client HR parser](https://github.com/patmorli/colmi-r09-smart-ring/blob/main/colmi_r02_client/hr.py) handles logs and a configured interval; its example expands a day into 288 five-minute slots. The interval and treatment of missing slots need verification on our R09. |
| `spo2` | Blood oxygen percentage and its measurement time. | [Daybreak's R09 implementation](https://github.com/reuhenbhalod/DayBreak) describes a streaming SpO2 assembler and background readings. Exact record fields and cadence still need captures. |
| `activity` | Step count; possibly distance and calories over a time interval. | The [R02-family SQLite schema](https://github.com/tahnok/colmi_r02_client/blob/main/tests/database_schema.sql) stores steps, distance, calories, and timestamp together. Units and whether these are interval or cumulative values need verification. |
| `sleep` | A session or segment with start/end times and sleep stages. | [Daybreak](https://github.com/reuhenbhalod/DayBreak) describes multi-packet sleep reassembly and stages; its README names wake, light, deep, and REM. The structure, stage codes, and accuracy need verification. |
| `battery` | Battery level and capture time, as device status rather than a health measurement. | The [R02-family client](https://github.com/patmorli/colmi-r09-smart-ring) exposes ring battery information. We need to check what our R09 returns. |

The [R02-family client](https://github.com/patmorli/colmi-r09-smart-ring)
also lists a stress measurement, while [Daybreak's R09 capability notes](https://github.com/reuhenbhalod/DayBreak/blob/main/Daybreak_PRD.md)
say HRV depends on firmware and body temperature is not reliable. We will
keep stress and HRV exploratory and will not define temperature records now.
Scores such as recovery are application-derived values, not raw ring readings.

## Duplicate and failure rule

The database will enforce uniqueness on `(deviceId, recordId)`. The subscriber
will validate the message and commit the original payload plus its metadata in
one SQLite transaction. A repeated delivery of the same logical record will
leave one stored record. A collision where the same identity carries different
content is an error to investigate, not an update to apply silently.

An MQTT QoS 1 acknowledgement means delivery through MQTT, not a successful
SQLite commit. We must test the subscriber's acknowledgement and restart
behavior before relying on it for storage guarantees.

## Gateway offline storage target

Goal: `gateway-01` should retain **a few days of records it has already read
from the ring** during Wi-Fi, broker, or Pi failure. This does not protect
readings that remain only on the ring when BLE sync itself fails.

The ESP32 needs a persistent, bounded queue in flash. For each record, it
stores the exact MQTT payload and stable `recordId` before attempting delivery.
After a restart or reconnection it retries queued records in order. The Pi
subscriber should publish an application-level acknowledgement containing the
record identity only after its SQLite transaction commits; an identical
duplicate may be acknowledged again. The gateway removes a queued record only
after matching that acknowledgement. A lost acknowledgement causes a resend,
which SQLite's uniqueness rule makes safe. Broker QoS 1 acknowledgement alone
must not cause queue removal.

The current gateway does **not** implement this queue or a database
acknowledgement topic. Its ESP32-C3 board reports 4 MB of total flash, shared
with firmware and other partitions. We cannot promise a number of offline days
until we choose a flash partition, measure real payload sizes and daily record
counts, and test flash writes/reboots. The queue must report when it is near
capacity and define an explicit full-queue policy rather than silently erase
unsaved data. Flash wear and power loss during writes also need tests.

## Validation and security boundaries

- Accept only the configured topic and supported `schemaVersion` values.
- Set a message-size limit before JSON decoding; choose the limit after
  measuring real sync output.
- Validate required fields and types, then check the allowed
  `deviceId`/`gatewayId`/`userId` relationship. Claimed IDs in JSON are not proof
  of authorization.
- Keep broker credentials outside the repository. Give the subscriber account
  subscribe access only to the required data topic and publish access only to
  the gateway's acknowledgement topic. Restrict access to the SQLite file.
- Record rejected messages and the reason without writing health data or secrets
  into routine logs.

## Questions to resolve with hardware

1. Which ring fields provide a stable identity for a record seen in two syncs?
2. What timestamps does the ring provide, and what timezone or clock behavior
   do they have?
3. How large are the largest logical records and complete sync bursts?
4. Do any record types need multiple MQTT messages because of gateway memory or
   MQTT packet limits?
5. How many bytes and flash writes does a normal day of records require, and
   how many days fit in the gateway's available flash partition?

Cloud backup is a separate storage step. It needs a destination outside the Pi
and a tested restore procedure; the provider and schedule are still undecided.
