# Smart Ring Subscriber

Python service for the Raspberry Pi side of the smart-ring system. The
[ESP32 gateway](https://github.com/regrets123/Smart_Ring_Gateway) reads the ring
over BLE and publishes JSON through MQTT. This service validates each message,
stores it in SQLite, and exposes stored readings through an authenticated
HTTPS API. The [message contract](docs/message-contract.md) defines the six
supported reading kinds and their record identities.

```text
Ring --BLE--> ESP32 gateway --MQTT/TLS:8883--> broker
                                            |
                                            v
                            Pi subscriber --> SQLite --> HTTPS API:8080
```

## Install on the Pi

Use Python 3.10 or newer. From this repository:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
```

Place the broker's public CA at `certs/mqtt_ca.pem`. The API uses a separate
certificate and private key on the Pi (`certs/api-cert.pem` and
`certs/api-key.pem`) and a bearer token of at least 32 characters in
`certs/token.txt`. The `certs/` directory, local configuration, and SQLite
files are ignored by Git. Keep the API private key on the Pi; a client needs
only the API certificate and token.

## Run the subscriber

Set the subscriber's broker credentials outside Git, then start the MQTT
process:

```sh
export SMART_RING_MQTT_HOST=mqtt.saxedesign.se
export SMART_RING_MQTT_CA_CERT=certs/mqtt_ca.pem
export SMART_RING_MQTT_USER=your-subscriber-account
export SMART_RING_MQTT_PASSWORD=your-password
python -m ring_subscriber --topic gateway/readings
```

The broker port defaults to `8883`; the database defaults to
`data/ring.sqlite3`. The gateway and subscriber **must use the same topic**.
Default is set to `gateway/readings` in the gateway and the sub.
 Set `--topic` if you want to change it. 
The subscriber uses TLS and MQTT QoS 1, logs connection and storage
events, and acknowledges valid messages after a successful database transaction.
Use `--host`, `--port`, `--database`, or `--ca-cert` to change these settings.

## Run the API

In a second shell on the Pi, with the same virtual environment activated,
start the API against the same database:

```sh
SMART_RING_API_TOKEN="$(cat certs/token.txt)" python -m ring_subscriber.api \
  --host 0.0.0.0 --port 8080 --database data/ring.sqlite3 \
  --certfile certs/api-cert.pem --keyfile certs/api-key.pem
```

The API defaults to loopback; a network-facing bind requires a TLS certificate
and key. Its certificate must name the hostname or IP used by clients. Limit
network access to the port as appropriate for your deployment.

`GET /readings` requires `Authorization: Bearer <token>`. Optional `limit`
(1–500, default 100) and `offset` (nonnegative, default 0) query parameters
page through readings ordered by receipt time. A successful response contains
`readings`, `limit`, and `offset`; each entry contains `receivedAt` and the
original `message`. Invalid parameters return 400, a missing or wrong token
returns 401, an unknown path returns 404, and a database read failure returns
500.

## Verify the data flow

On another device with this repository, copy `token.txt` and the **public**
`api-cert.pem` into its ignored `certs/` directory. From PowerShell:

```powershell
python .\read_readings.py https://mqtt.saxedesign.se:8080/readings --ca-cert .\certs\api-cert.pem
```

The client verifies TLS and the server hostname, sends the bearer token from
`certs/token.txt`, and prints the JSON response. A wrong token returns 401.
For an end-to-end check, find a real ring record's `recordId` in the gateway
log and API response, and confirm the Pi subscriber logged its storage. The
gateway's probe summary reports per-query packet counts and lost notifications;
the Pi logs stored, updated, duplicate, and rejected messages.

## Limits and local checks

The subscriber preserves the original payload, rejects malformed or oversized
JSON, and treats identical `recordId` retries as duplicates. Historical
records with the same identity may update; changed live records are rejected.
The gateway has no durable offline queue or database-level acknowledgement,
so MQTT broker acknowledgement alone does not guarantee SQLite storage.
The subscriber does not yet authorize the claimed device/user relationship.

The repository contains ingestion unit tests, which can be run manually with:

```sh
python -m unittest discover -s tests -v
```
