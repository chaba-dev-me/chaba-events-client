"""Send a WhatsApp-style alert when a relay switches on, using the
hook set: on_auth_ok to announce, on_event to decide, on_error to
stay quiet during routine disconnects.

This example shows the FULL override surface working together. It
also demonstrates handing slow work off a queue so the event loop
never blocks (see "Keep on_event fast" in the README).

    python examples/alert_on_relay_on.py ws://10.0.4.1:5000 KEY relay_0
"""

import queue
import sys
import threading
import time

from chaba_events_client import ChabaEventClient


def send_alert(device_uid: str, relay: str, state: str) -> None:
    """Pretend this calls a real API. Slow work belongs off the event
    thread."""
    print(f"ALERT {device_uid}: {relay} is {state} (at {time.strftime('%H:%M:%S')})")


class RelayAlerter(ChabaEventClient):
    """Alert when any watched relay turns on."""

    def __init__(self, watch=("relay_0",), **kwargs):
        super().__init__(**kwargs)
        self.watch = set(watch)
        # on_event only ENQUEUES; this worker thread does the slow work.
        self._alerts: queue.Queue[tuple[str, str, str]] = queue.Queue()
        self._worker = threading.Thread(target=self._drain_alerts, daemon=True)
        self._worker.start()

    # --- event thread: fast path only -----------------------------
    def on_event(self, event: dict) -> None:
        relays = event.get("relays_state") or {}
        for relay in self.watch & relays.keys():
            if relays[relay] == "on":
                self._alerts.put((event["device_uid"], relay, "on"))

    # --- worker thread: slow path ---------------------------------
    def _drain_alerts(self) -> None:
        while True:
            device_uid, relay, state = self._alerts.get()
            try:
                send_alert(device_uid, relay, state)
            except Exception as exc:  # never let the worker die
                self.on_error(exc)

    # --- connection hooks -----------------------------------------
    def on_auth_ok(self, info: dict) -> None:
        print(f"watching {sorted(self.watch)} on {info.get('customer_uid')}")

    def on_disconnected(self, code: int, reason: str) -> None:
        print(f"connection lost ({code}); will reconnect", file=sys.stderr)


if __name__ == "__main__":
    url, api_key = sys.argv[1], sys.argv[2]
    watch = sys.argv[3:] or ["relay_0"]
    client = RelayAlerter(
        watch=watch,
        url=url,
        api_key=api_key,
        name="relay-alerter",
    )
    client.run_forever()
