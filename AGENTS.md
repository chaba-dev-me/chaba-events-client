# Agent integration guide — chaba-events-client

This document is for AI agents (and their operators) that want to
react to Chaba device events in real time. It complements
[README.md](README.md), which documents the full class surface; here
we cover only what an autonomous integration needs to get right.

## When to use this client

| need | use |
|---|---|
| "Tell me the moment relay_0 changes" | **this client** (WebSocket stream, sub-second) |
| "What is the state right now?" | REST API (`GET /api/v1/me/devices`) — state is embedded in the device list |
| "Switch a relay" | REST API / the customer's Chaba WhatsApp agent — this client is **read-only** |

The stream is **live only**: events arrive from the moment of
connection onward, there is no replay. A robust agent treats the
stream as "from now on" and reads the REST API once at startup for
the current state (or just waits for the next heartbeat — every
device re-reports its full state periodically, at most ~60 s apart).

## Prerequisites

1. **Network reachability.** The default server address is
   `ws://10.0.4.1:5000` — the Chaba server's WireGuard address. Run
   the client on a machine inside the customer's tunnel (e.g.
   anything on the MikroTik's LAN) or wherever your operator exposes
   the event service. Plain `ws://`: the tunnel is the encryption
   layer.
2. **A customer API key** (43 characters). This is the same key that
   authenticates the REST API. The customer creates/rotates/revokes
   it with their Chaba WhatsApp agent ("give me an API key",
   "rotate my iPad key"). Events are filtered server-side to the
   key's farm — a key can never see another farm's devices.

Treat the key as a bearer secret: store it in the environment or a
credential store, never in code, and never log it.

## Install

```bash
pip install git+https://github.com/chaba-dev-me/chaba-events-client.git
# or from a checkout:
pip install .
# dependencies (requirements.txt): websockets>=12
```

Python 3.9+.

## Two integration shapes

### A. Zero-code: run the CLI, read stdout

The command-line client emits **one JSON object per line on stdout**
(newline-delimited JSON) and sends all human-readable status to
stderr. Spawn it, read stdout, parse each line:

```bash
export CHABA_API_KEY="<key>"          # or pass --api-key
python -m chaba_events_client --url ws://10.0.4.1:5000
```

Machine contract:

- **stdout**: one JSON event per line, flushed immediately. Example
  event:

  ```json
  {"customer_uid":"cust_…","device_uid":"550e8400-…","relays_state":{"relay_0":"on","relay_1":"off"},"rssi":-58,"uptime_s":3600,"received_at":"2026-10-01T12:00:00.123456+00:00"}
  ```

- **stderr**: connection status lines (`connecting…`,
  `authenticated as cust_…`, `disconnected (code=1006); retrying`).
  Human-oriented; never contains events. Parse it only for display.
- **Exit codes**: `0` clean stop (SIGINT/SIGTERM or `--no-reconnect`
  close), `1` authentication rejected (bad/revoked key — do not
  restart in a loop; surface the error to the customer),
  `2` missing API key (configuration error).
- **Signals**: SIGINT/SIGTERM stop cleanly; with the default
  reconnect behavior the process only exits when stopped or on a
  permanent auth failure.

This shape is usually the right one for an agent: the subprocess
boundary isolates the stream loop from your logic, and a crash in
your consumer never drops the connection.

### B. In-process: subclass `ChabaEventClient`

```python
from chaba_events_client import ChabaEventClient, AuthenticationError

class MyAgent(ChabaEventClient):
    def on_event(self, event: dict) -> None:
        relays = event.get("relays_state") or {}
        if relays.get("relay_0") == "on":
            ...  # act

    def on_auth_ok(self, info: dict) -> None:
        ...  # stream is live; info["customer_uid"] is bound

    def on_disconnected(self, code: int, reason: str) -> None:
        ...  # transient; the library reconnects (unless code 4002-4004)

try:
    MyAgent(url="ws://10.0.4.1:5000",
            api_key=os.environ["CHABA_API_KEY"]).run_forever()
except AuthenticationError as exc:
    ...  # permanent: the key is wrong or revoked — stop and ask the customer
```

All hooks are documented in
[README.md](README.md#the-chabaeventclient-class); examples live in
[examples/](examples/).

## Event semantics agents must know

1. **Reports, not edges.** A device publishes on every relay change
   AND as a periodic heartbeat repeating the unchanged state. If you
   need *changes only*, keep the previous
   `event["relays_state"]` per `device_uid` in your own state and
   diff (see `examples/relay_change_log.py`).
2. **`relays_state` covers every relay** (`{"relay_0": "on", ...}`),
   so one event is a full snapshot of that device's relays, not a
   delta.
3. **`received_at`** is the server's receive timestamp (ISO-8601,
   UTC). Use it, not your local clock, when ordering across devices.
4. **`rssi`/`uptime_s`** are WiFi signal (dBm) and uptime (seconds) —
   useful for "device went quiet / rebooted" heuristics: a falling
   `uptime_s` means the device rebooted.
5. **Close code 1013** means the consumer was too slow. The client
   reconnects automatically; events during the gap are lost —
   re-read the REST API state afterwards if exact continuity
   matters.
6. **Filtering**: pass `devices=[uid, ...]` (constructor or
   `subscribe()`) to receive only listed devices; `[]`/`None` = all.
   `on_subscribed` fires when the server confirmed the filter.

## Reliability rules

- Run under a supervisor (systemd unit, Docker, etc.). The client
  already retries transient failures with capped exponential backoff;
  the supervisor covers reboots and crashes of the process itself.
- `AuthenticationError` (exit code 1) is **terminal by design** —
  retrying a revoked key only spams the server. Stop and have the
  customer re-issue the key.
- Keep `on_event` fast: enqueue and hand off to a worker for anything
  slow (HTTP calls, messages). A blocked handler eventually overflows
  the server-side buffer (close 1013). See
  `examples/alert_on_relay_on.py`.
- Health: `on_auth_ok` fires on every successful (re)connection. An
  agent that hasn't seen one for a long time should assume the
  tunnel, not the client, is down.

## Security notes

- The API key grants **read access to that farm's telemetry**.
  Rotate it with the customer's Chaba agent if it leaks; a rotated
  key invalidates the old one immediately and this client raises
  `AuthenticationError` on its next (re)connect.
- The connection is authenticated but the events are tenant-private
  data — don't forward them anywhere the customer hasn't approved.
- The server is reachable only inside the tunnel network; don't
  defeat that by proxying it to the public internet.
