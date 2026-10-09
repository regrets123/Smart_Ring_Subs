"""Fetch readings from the HTTPS API using the local CA and bearer token."""

import argparse
import json
import ssl
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener


CERTS = Path(__file__).resolve().parent / "certs"


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch stored smart-ring readings")
    parser.add_argument("url", help="full API URL, e.g. https://host:8080/readings")
    parser.add_argument("--ca-cert", type=Path, default=CERTS / "mqtt_ca.pem")
    parser.add_argument("--token-file", type=Path, default=CERTS / "token.txt")
    args = parser.parse_args()

    url = urlsplit(args.url)
    if url.scheme != "https" or not url.hostname or url.path != "/readings" or url.fragment:
        parser.error("url must be an HTTPS URL with the path /readings")

    try:
        token = args.token_file.read_text(encoding="utf-8").rstrip("\r\n")
        if not token:
            parser.error(f"token file is empty: {args.token_file}")
        context = ssl.create_default_context(cafile=str(args.ca_cert))
    except (OSError, UnicodeError, ssl.SSLError) as exc:
        parser.error(str(exc))

    request = Request(args.url, headers={"Authorization": f"Bearer {token}"}, method="GET")
    opener = build_opener(HTTPSHandler(context=context), NoRedirect)
    try:
        with opener.open(request, timeout=10) as response:
            readings = json.load(response)
    except HTTPError as exc:
        parser.exit(1, f"HTTP {exc.code}: {exc.read().decode('utf-8', errors='replace')}\n")
    except (URLError, TimeoutError) as exc:
        parser.exit(1, f"Request failed: {exc}\n")

    print(json.dumps(readings, indent=2))


if __name__ == "__main__":
    main()
