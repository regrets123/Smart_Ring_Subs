# Smart Ring Subscriber

Python service that subscribes to the gateway's MQTT test topic and stores each
validated message's original bytes and receipt time in SQLite. It validates
heart-rate, HRV history, SpO2, and sleep record shapes without interpreting
the measurements. See the [message contract](docs/message-contract.md).

## Run locally or on the Pi

Use Python 3.10 or newer. Create a virtual environment, then install the one
MQTT dependency:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
```

The existing broker uses a private CA. Set the broker host, path to its public
CA certificate, and the subscriber's own read-only MQTT credentials outside
the repository. The default topic is `gateway/mock/readings`; the default
database is `data/ring.sqlite3`.

```sh
export SMART_RING_MQTT_HOST=mqtt.saxedesign.se
export SMART_RING_MQTT_CA_CERT=/path/to/mqtt_ca.pem
export SMART_RING_MQTT_USER=your-subscriber-account
export SMART_RING_MQTT_PASSWORD=your-password
python -m ring_subscriber
```

`--host`, `--port`, `--topic`, `--database`, and `--ca-cert` can override those
connection defaults. TLS certificate and hostname verification remain enabled.
Use a stable MQTT client ID so the broker can retain this subscriber's QoS 1
session while it is disconnected. The subscriber acknowledges a valid message
only after the SQLite transaction completes. Invalid or conflicting messages
are logged without payload contents and acknowledged so they do not replay
forever; we will add a more explicit rejection policy during recovery work.

The gateway currently republishes the **same simulated record ID and timestamp**
on every cycle. Seeing one SQLite row after many publishes is expected. A new
real reading needs a new stable ID, while a retry of that reading must reuse it.
No database acknowledgement is sent back to the gateway yet.

## Verify ingestion without MQTT

```sh
python -m unittest discover -s tests -v
```
