"""Run the MQTT subscriber with ``python -m ring_subscriber``."""

import argparse
import logging
import os
from pathlib import Path

from paho.mqtt import client as mqtt

from .ingest import InvalidMessage, RecordConflict, open_database, store_message


def main() -> None:
    parser = argparse.ArgumentParser(description="Save smart-ring MQTT records to SQLite")
    parser.add_argument("--host", default=os.getenv("SMART_RING_MQTT_HOST"))
    parser.add_argument("--port", type=int, default=8883)
    parser.add_argument("--topic", default="gateway/mock/readings")
    parser.add_argument("--database", default="data/ring.sqlite3")
    parser.add_argument("--ca-cert", default=os.getenv("SMART_RING_MQTT_CA_CERT"))
    args = parser.parse_args()
    if not args.host:
        parser.error("set --host or SMART_RING_MQTT_HOST")

    username = os.getenv("SMART_RING_MQTT_USER")
    password = os.getenv("SMART_RING_MQTT_PASSWORD")
    if password and not username:
        parser.error("SMART_RING_MQTT_PASSWORD requires SMART_RING_MQTT_USER")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    os.umask(0o077)  # New database files are private to this service account.
    database_path = Path(args.database)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = open_database(str(database_path))

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="smart-ring-subs-01",
        clean_session=False,
        manual_ack=True,
    )
    client.tls_set(ca_certs=args.ca_cert)
    if username:
        client.username_pw_set(username, password)

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            logging.error("MQTT connection rejected: %s", reason_code)
            return
        result, _ = client.subscribe(args.topic, qos=1)
        if result != mqtt.MQTT_ERR_SUCCESS:
            logging.error("MQTT subscription failed: %s", result)
        else:
            logging.info("Subscribed to %s", args.topic)

    def on_message(client, userdata, message):
        if message.topic != args.topic:
            logging.error("Received message on unexpected topic")
            return
        try:
            result = store_message(connection, message.payload)
        except (InvalidMessage, RecordConflict) as exc:
            logging.warning("Rejected message: %s", exc)
        except Exception:
            logging.exception("Database write failed; leaving MQTT message unacknowledged")
            client.disconnect()
            return
        else:
            logging.info("Message %s", result)
        if message.qos >= 1:
            ack_result = client.ack(message.mid, message.qos)
            if ack_result != mqtt.MQTT_ERR_SUCCESS:
                logging.error("MQTT acknowledgement failed: %s", ack_result)

    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(args.host, args.port)
        client.loop_forever()
    except KeyboardInterrupt:
        logging.info("Stopping subscriber")
    finally:
        client.disconnect()
        connection.close()


if __name__ == "__main__":
    main()
