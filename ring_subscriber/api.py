"""Read stored ring messages over a small authenticated HTTP API.

Run with ``python -m ring_subscriber.api``. The server listens on loopback by
default; a network-facing bind requires a TLS certificate and private key.
"""

import argparse
import ipaddress
import json
import os
import secrets
import sqlite3
import ssl
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


def make_handler(database_path: Path, token: str):
    class ReadingsHandler(BaseHTTPRequestHandler):
        def send_json(self, status: int, body: dict, authenticate: bool = False) -> None:
            payload = json.dumps(body, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            if authenticate:
                self.send_header("WWW-Authenticate", 'Bearer realm="readings"')
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:
            supplied = self.headers.get_all("Authorization", [])
            if (
                len(supplied) != 1
                or not supplied[0].startswith("Bearer ")
                or not secrets.compare_digest(supplied[0][7:], token)
            ):
                self.send_json(401, {"error": "unauthorized"}, authenticate=True)
                return

            url = urlsplit(self.path)
            if url.path != "/readings":
                self.send_json(404, {"error": "not found"})
                return

            try:
                params = parse_qs(url.query, keep_blank_values=True, strict_parsing=True)
                if set(params) - {"limit", "offset"}:
                    raise ValueError("unknown parameter")
                if any(len(values) != 1 for values in params.values()):
                    raise ValueError("repeated parameter")
                limit = int(params.get("limit", ["100"])[0])
                offset = int(params.get("offset", ["0"])[0])
                if not 1 <= limit <= 500 or offset < 0:
                    raise ValueError("parameter out of range")
            except ValueError:
                self.send_json(400, {"error": "limit must be 1-500 and offset must be nonnegative integers"})
                return

            try:
                with closing(sqlite3.connect(database_path.as_uri() + "?mode=ro", uri=True)) as db:
                    rows = db.execute(
                        """SELECT received_at, raw_payload FROM received_messages
                           ORDER BY received_at, device_id, record_id
                           LIMIT ? OFFSET ?""",
                        (limit, offset),
                    ).fetchall()
                readings = [
                    {"receivedAt": received_at, "message": json.loads(raw_payload)}
                    for received_at, raw_payload in rows
                ]
            except (sqlite3.Error, ValueError, UnicodeDecodeError):
                self.log_error("Could not read stored messages")
                self.send_json(500, {"error": "database unavailable"})
                return

            self.send_json(200, {"readings": readings, "limit": limit, "offset": offset})

    return ReadingsHandler


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve stored smart-ring readings")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--database", default="data/ring.sqlite3")
    parser.add_argument("--certfile", help="TLS certificate (PEM)")
    parser.add_argument("--keyfile", help="TLS private key (PEM)")
    args = parser.parse_args()

    token = os.getenv("SMART_RING_API_TOKEN")
    if not token or len(token) < 32:
        parser.error("set SMART_RING_API_TOKEN to a random token of at least 32 characters")
    if bool(args.certfile) != bool(args.keyfile):
        parser.error("--certfile and --keyfile must be supplied together")
    try:
        loopback = ipaddress.ip_address(args.host).is_loopback
    except ValueError:
        loopback = False
    if not loopback and not args.certfile:
        parser.error("a network-facing --host requires --certfile and --keyfile")

    database_path = Path(args.database).resolve()
    if not database_path.is_file():
        parser.error(f"database does not exist: {database_path}")

    server = ThreadingHTTPServer((args.host, args.port), make_handler(database_path, token))
    if args.certfile:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(args.certfile, args.keyfile)
        server.socket = context.wrap_socket(server.socket, server_side=True)
    scheme = "https" if args.certfile else "http"
    print(f"Serving {scheme}://{args.host}:{args.port}/readings", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
