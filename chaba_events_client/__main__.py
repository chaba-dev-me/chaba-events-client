"""Command-line client: register to the Chaba event server and print
every device event to stdout, one JSON object per line.

    python -m chaba_events_client --url ws://10.0.4.1:5000 --api-key KEY
    CHABA_API_KEY=KEY python -m chaba_events_client            # env fallback
    python -m chaba_events_client --devices <uid> [--devices <uid2>]

Output contract (handy in shell pipelines):

    stdout   one JSON event per line (pipe into jq, a logger, ...)
    stderr   connection status / errors / reconnections

The CLI is also the smallest working example of the override pattern:
StdoutEventClient subclasses ChabaEventClient and overrides on_event.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from .client import AuthenticationError, ChabaEventClient

DEFAULT_URL = "ws://10.0.4.1:5000"


class StdoutEventClient(ChabaEventClient):
    """ChabaEventClient that prints each event as one JSON line.

    This tiny subclass IS the override pattern: on_event is the only
    method that needed changing. Everything else — auth, reconnect,
    pings — is inherited.
    """

    def on_event(self, event: dict) -> None:
        print(json.dumps(event, separators=(",", ":")), flush=True)

    def on_connected(self) -> None:
        print(f"[{self.name}] connecting to {self.url} ...", file=sys.stderr)

    def on_auth_ok(self, info: dict) -> None:
        print(
            f"[{self.name}] authenticated as {info.get('customer_uid', '?')}; "
            "waiting for events (Ctrl-C to stop)",
            file=sys.stderr,
        )

    def on_disconnected(self, code: int, reason: str) -> None:
        if self.reconnect:
            print(
                f"[{self.name}] disconnected (code={code} {reason}); "
                "retrying ...",
                file=sys.stderr,
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chaba-events-client",
        description="Stream Chaba device events to stdout, one JSON object per line.",
        epilog=(
            "The API key is your customer API key from the Chaba app "
            "(43 characters). The default URL is the Chaba event server "
            "on the WireGuard tunnel; run this from a machine that can "
            "reach it."
        ),
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("CHABA_WS_URL", DEFAULT_URL),
        help=f"event server WebSocket URL (default: %(default)s)",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("CHABA_API_KEY", ""),
        help="customer API key (or set CHABA_API_KEY in the environment)",
    )
    parser.add_argument(
        "--devices",
        nargs="*",
        default=None,
        metavar="DEVICE_UID",
        help="only these device UIDs (default: all devices on the account)",
    )
    parser.add_argument(
        "--no-reconnect",
        action="store_true",
        help="exit on connection loss instead of retrying",
    )
    parser.add_argument(
        "--name",
        default="chaba-events",
        help="label used in stderr status lines",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.api_key:
        print(
            "error: no API key — pass --api-key or set CHABA_API_KEY",
            file=sys.stderr,
        )
        return 2

    client = StdoutEventClient(
        url=args.url,
        api_key=args.api_key,
        devices=args.devices,
        reconnect=not args.no_reconnect,
        name=args.name,
    )
    try:
        client.run_forever()
    except AuthenticationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print(f"[{args.name}] stopped", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
