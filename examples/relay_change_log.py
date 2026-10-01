"""Log relay CHANGES only: turn the event stream into a change feed.

The server already forwards changes-only traffic (one snapshot per
device after connecting, then relay flips). This example still diffs
locally — it is the defensive pattern for bridging an OLDER server
without change detection, and shows how to keep per-device state in
a subclass.

    python examples/relay_change_log.py ws://10.0.4.1:5000 KEY
"""

import sys

from chaba_events_client import ChabaEventClient


class RelayChangeLog(ChabaEventClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # device_uid -> {"relay_0": "on", ...} as of the last event
        self._previous: dict[str, dict] = {}

    def on_event(self, event: dict) -> None:
        device = event.get("device_uid", "?")
        current = event.get("relays_state") or {}
        previous = self._previous.get(device)

        if previous is None:
            print(f"{device}: first sighting: {current}")
        else:
            for relay, state in sorted(current.items()):
                if previous.get(relay) != state:
                    print(f"{device}: {relay} -> {state}")

        self._previous[device] = current


if __name__ == "__main__":
    url, api_key = sys.argv[1], sys.argv[2]
    RelayChangeLog(url=url, api_key=api_key).run_forever()
