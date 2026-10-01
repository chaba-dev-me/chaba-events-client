# chaba-events-client

Receive [Chaba](https://chaba.me) (AgroIoT) device events in Python.
Connect with your customer API key and get every relay state change,
heartbeat and signal report from your devices as they happen — on your
server, in your scripts, in your own integrations.

**Building an AI agent on this?** Read [AGENTS.md](AGENTS.md) — the
short integration guide for autonomous consumers (CLI machine
contract, event semantics, reliability rules).

```
pip install git+https://github.com/chaba-dev-me/chaba-events-client.git
# or from a checkout of this directory:
pip install .
python -m chaba_events_client --url ws://10.0.4.1:5000 --api-key KEY
```

Every event your farm emits arrives as one JSON object, delivered to
**one method you override**:

```python
from chaba_events_client import ChabaEventClient

class PumpWatcher(ChabaEventClient):
    def on_event(self, event):
        if (event.get("relays_state") or {}).get("relay_0") == "on":
            print("the north pump just started")

PumpWatcher(
    url="ws://10.0.4.1:5000",     # Chaba event server (inside your tunnel)
    api_key="<customer API key>",
).run_forever()
```

The library handles the WebSocket handshake, authentication, protocol
keep-alive, device filtering and reconnection-with-backoff. You write
`on_event`. That's the deal.

---

## Requirements

- Python 3.9+
- [`websockets`](https://websockets.readthedocs.io/) ≥ 12 (installed
  automatically; see `requirements.txt`)
- Network reachability to the Chaba event server. The default address
  `ws://10.0.4.1:5000` is the server's WireGuard address: any machine
  on your farm's tunnel (e.g. the MikroTik's LAN) can reach it; the
  public internet cannot. The connection is plain `ws://` because the
  tunnel is the encryption layer.

## Getting an API key

Events are filtered **per customer, server-side**: a key only ever
receives the devices of the farm it belongs to. Use your existing
customer API key — the same 43-character key that authenticates the
[Chaba REST API](https://docs.chaba.me). If you don't have one yet,
message your Chaba agent ("give me an API key") or use the app's
*Add API key* action.

Treat the key like a password: anyone holding it can read your device
telemetry. Revoke and re-issue it if it leaks.

## The command-line client

The quickest way in — register to the server and print every event to
stdout, one JSON object per line (perfect for `jq`, log files, or
piping anywhere):

```console
$ python -m chaba_events_client --url ws://10.0.4.1:5000 --api-key KEY
[chaba-events] connecting to ws://10.0.4.1:5000 ...          (stderr)
[chaba-events] authenticated as cust_1a2b3c4d5e6f; ...       (stderr)
{"customer_uid":"cust_1a2b3c4d5e6f","device_uid":"550e8400-...","relays_state":{"relay_0":"on"},"rssi":-58,"uptime_s":3600,"received_at":"2026-10-01T12:00:00.123456+00:00"}   (stdout)
```

| flag | meaning |
|---|---|
| `--url` | Event server URL (default `ws://10.0.4.1:5000`, or `CHABA_WS_URL`) |
| `--api-key` | Customer API key (or `CHABA_API_KEY` env var) |
| `--devices A B` | Only these device UIDs (default: all your devices) |
| `--no-reconnect` | Exit on connection loss instead of retrying |
| `--name` | Label for stderr status lines |

stdout carries **events only** (one JSON object per line); all status
and error chatter goes to stderr, so `... | jq` just works.

## The `ChabaEventClient` class

Import it, subclass it, override what you need:

```python
from chaba_events_client import ChabaEventClient
```

### Constructor

```python
ChabaEventClient(
    url,                          # required: server URL
    api_key,                      # required: customer API key
    devices=None,                 # optional: list of device UIDs to receive
    reconnect=True,               # auto-reconnect on transient drops
    initial_backoff_s=1.0,        # first reconnect delay
    max_backoff_s=30.0,           # delay ceiling (doubles from initial)
    name=None,                    # label for messages (defaults to class name)
    open_timeout_s=10.0,          # handshake timeout
    ping_interval_s=20.0,         # WebSocket ping keep-alive
)
```

### The hooks — what to override

All hooks run on the `run_forever()` thread and are plain synchronous
methods. The base implementations are all no-ops except `on_event`
(which prints one JSON line to stdout).

#### `on_event(self, event: dict) -> None` — the one that matters

Called for every device event on your account. The `event` dict:

```python
{
    "customer_uid": "cust_1a2b3c4d5e6f",       # your account
    "device_uid":   "550e8400-e29b-41d4-a716-446655440000",
    "relays_state": {"relay_0": "on", "relay_1": "off"},
    "rssi":         -58,        # WiFi signal strength, dBm
    "uptime_s":     3600,       # device uptime in seconds
    "received_at":  "2026-10-01T12:00:00.123456+00:00",  # server clock
    # ...any extra fields the device firmware includes flow through
}
```

Two things worth knowing:

1. **Events fire on change AND on heartbeat.** A device re-publishes
   its state periodically even when nothing changed. If you want
   *changes only*, keep the previous state in your subclass — see
   [`examples/relay_change_log.py`](examples/relay_change_log.py).
2. **Keep `on_event` fast.** While it runs, later events queue up; if
   your handler is slow for long enough the server closes the
   connection (code 1013, "try again later") and the client
   reconnects — you'd lose events in between. For slow work (HTTP
   calls, emails, uploads), put the event on a queue and let a worker
   thread do the work —
   [`examples/alert_on_relay_on.py`](examples/alert_on_relay_on.py)
   shows the pattern.

#### `on_auth_ok(self, info: dict) -> None`

The server accepted your key; the live event stream starts now.
`info` has `customer_uid` and `actor_type`. Good spot for a "back
online" notification or an account log line.

#### `on_connected(self) -> None`

Handshake succeeded, authentication pending. Note the client may
still be rejected right after (bad key) — pair it with
`on_auth_ok` for "fully up".

#### `on_disconnected(self, code: int, reason: str) -> None`

The connection ended. `code` is the WebSocket close code:

| code | meaning |
|---|---|
| 1000 | clean close (`stop()` or server restart) |
| 1006 | abnormal loss (network, tunnel down) |
| 1013 | **your consumer was too slow** — the event queue overflowed |
| 4001 | no auth frame sent in time (protocol misuse) |
| 4002 | malformed auth frame |
| 4003 | invalid API key |
| 4004 | not a customer key (agent/installer credentials can't subscribe) |

With `reconnect=True` a reconnect follows automatically — except for
4002/4003/4004, which raise `AuthenticationError` and stop the
client: a wrong key will never become right by retrying.

#### `on_subscribed(self, devices: list) -> None`

The server acknowledged your device filter — `devices` is the list now
in effect (`[]` = all devices). Useful when your logic must know the
filter is live before trusting the stream; fires on every reconnect.

#### `on_error(self, exc: Exception) -> None`

A transient failure (connection refused, timeout, DNS). The client
will back off and retry; override to route into your own logging.
The default prints to stderr.

### Methods

| method | what it does |
|---|---|
| `run_forever()` | Blocking main loop: connect → auth → dispatch, reconnecting per the backoff settings until `stop()`. Raises `AuthenticationError` on a permanently bad key; with `reconnect=False` transient failures propagate to the caller too. Ctrl-C stops cleanly. |
| `stop()` | Thread-safe: ends `run_forever()` from another thread or a signal handler. |
| `subscribe(devices)` | Change the device filter on the fly (`None`/`[]` = all devices again). Applies immediately if connected. |
| `on_subscribed(devices)` | Hook: the server acknowledged the filter (`devices` = list in effect; `[]` = all). Fires on every reconnect. |

`ChabaEventClient` also works as a context manager (`with client: ...`)
for tidy shutdown.

### Full example with the complete hook set

See [`examples/alert_on_relay_on.py`](examples/alert_on_relay_on.py):

```python
class RelayAlerter(ChabaEventClient):
    def __init__(self, watch=("relay_0",), **kwargs):
        super().__init__(**kwargs)
        self.watch = set(watch)
        self._alerts = queue.Queue()
        threading.Thread(target=self._drain_alerts, daemon=True).start()

    def on_event(self, event):                      # fast path: enqueue
        relays = event.get("relays_state") or {}
        for relay in self.watch & relays.keys():
            if relays[relay] == "on":
                self._alerts.put((event["device_uid"], relay))

    def _drain_alerts(self):                        # slow path: worker
        while True:
            device_uid, relay = self._alerts.get()
            send_alert(device_uid, relay)           # e.g. an HTTP call

    def on_auth_ok(self, info):
        print(f"watching {sorted(self.watch)} on {info['customer_uid']}")

    def on_disconnected(self, code, reason):
        print(f"connection lost ({code}); will reconnect")
```

## Wire protocol (for other languages)

The protocol is deliberately tiny — a competent WebSocket client in
any language can talk to the server directly:

1. **Connect** to `ws://10.0.4.1:5000/` (any path works).
2. **Authenticate** — within 10 seconds send one text frame:

   ```json
   {"type": "auth", "api_key": "<customer API key>"}
   ```

   (Alternative for browser clients: an `Authorization: Bearer <key>`
   header on the upgrade request skips this step.)

   The server replies `{"type":"auth_ok","customer_uid":"...","actor_type":"customer"}`
   or `{"type":"auth_error","error":"..."}` and closes with 4002/4003/4004.

3. **Optionally filter devices** (any time after auth):

   ```json
   {"type": "subscribe", "devices": ["<device_uid>", "..."]}
   ```

   Empty list = all devices again. Acked with a `subscribed` frame.

4. **Receive events** — one text frame per device state observation:

   ```json
   {"type": "event", "event": { ...as in on_event above... }}
   ```

5. Application-level `{"type": "ping"}` → `{"type": "pong"}` is
   available; WebSocket protocol pings also run by default.

Events are **live only** — there is no replay of history. If you need
what happened while you were offline, read the REST API
(`GET /api/v1/me/devices` embeds the current state) and treat the
stream as "from now on".

## Running your client reliably

- Run it under systemd / Docker / supervisor with automatic restart —
  the client already retries transient failures, but the process
  itself should also come back after a reboot.
- Point health monitoring at the fact that `on_auth_ok` fires on every
  (re)connection: a client that hasn't reconnected for a long time
  usually means the tunnel is down, not the client.
- Expect duplicate-adjacent events: change events + heartbeats mean
  state is *reported*, not *edged*. Diff in your code when you need
  edges.

## Publishing status

Published as a standalone open-source repository (MIT):
**https://github.com/chaba-dev-me/chaba-events-client** — alongside
[`chaba-dev-me/firmware`](https://github.com/chaba-dev-me/firmware).
The copy in the Chaba monorepo (`chaba-events-client/`) is the
source of truth; releases are published from it.
