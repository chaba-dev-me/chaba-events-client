"""The ChabaEventClient: connect, authenticate, receive device events.

This module has one job: make "act on my farm's device events" a
single method override, and keep every other concern (WebSocket
handshake, authentication, protocol pings, reconnection backoff,
clean shutdown) inside the library.

The client is synchronous on purpose: a consumer of relay-toggle
events should not need to know what an event loop is. Everything runs
on the calling thread inside :meth:`ChabaEventClient.run_forever`;
your overrides are plain synchronous methods.
"""

from __future__ import annotations

import json
import threading
import time
import types
from typing import Iterable, List, Optional

try:
    from websockets.sync.client import ClientConnection, connect
except ImportError as _exc:  # pragma: no cover - dependency is declared
    raise ImportError(
        "chaba-events-client needs the 'websockets' package "
        "(pip install websockets>=12)."
    ) from _exc

__all__ = ["AuthenticationError", "ChabaEventClient", "ConnectionClosedClean"]

#: WebSocket close codes the server uses for authentication/authorisation
#: problems. A close with one of these codes is PERMANENT — wrong key,
#: not a customer key, or the account's subscription does not cover the
#: event stream (upgrade/renewal is the fix, retrying is not) — so
#: :meth:`ChabaEventClient.run_forever` stops instead of backing off.
AUTH_CLOSE_CODES = {4001, 4002, 4003, 4004, 4005}


class AuthenticationError(Exception):
    """The server rejected the API key (close codes 4002-4004).

    This is permanent: run_forever stops rather than retrying. Check
    the key — it is the same 43-character customer API key the REST
    API uses, created in the Chaba app or via the MCP
    enable_customer_api_key tool.
    """


class ConnectionClosedClean(Exception):
    """Internal: the connection ended without an error (server
    restart, stop() called, network closed). Drives the reconnect
    decision in run_forever; you should never need to catch it.
    """


class ChabaEventClient:
    """Receive Chaba device events and act on them.

    Override :meth:`on_event` (and optionally the other ``on_*``
    hooks) in a subclass, then call :meth:`run_forever`::

        class PumpWatcher(ChabaEventClient):
            def on_event(self, event):
                relays = event.get("relays_state") or {}
                if relays.get("relay_0") == "on":
                    print("north pump just started")

        watcher = PumpWatcher(
            url="ws://10.0.4.1:5000",       # Chaba server, inside your tunnel
            api_key="<customer API key>",
        )
        watcher.run_forever()               # blocks; Ctrl-C stops cleanly

    Parameters (constructor keyword arguments):
        url:
            WebSocket URL of the Chaba event server, e.g.
            ``ws://10.0.4.1:5000`` (the default server address,
            reachable from inside the farm's WireGuard tunnel) or an
            ``ws://events.example.com`` style address if your
            operator fronted it with a proxy. Plain TCP ``ws://`` —
            the tunnel provides the encryption.
        api_key:
            Your customer API key (43 characters, base64url). The
            same key authenticates the REST API; events are filtered
            to YOUR devices server-side — you can never see another
            farm's events.
        devices:
            Optional list of device UIDs to receive. ``None`` (the
            default) receives every device on the account. The filter
            can also be changed later with :meth:`subscribe`.
        reconnect:
            Reconnect automatically after transient drops (default
            True). Bad-key authentication failures always stop the
            client; there is no point retrying a wrong key. With
            False, a transient failure propagates out of run_forever
            instead of retrying.
        initial_backoff_s / max_backoff_s:
            Reconnect delay in seconds; doubles from ``initial`` up
            to ``max`` while the server is unreachable, resets on a
            successful connection.
        name:
            Optional label used in log/exception messages (defaults
            to the class name).
        open_timeout_s:
            Seconds to wait for the TCP + WebSocket handshake.
        ping_interval_s:
            WebSocket protocol ping interval — keeps idle tunnel and
            NAT paths alive. The server pings too; either side
            detecting silence drops the connection, and (with
            ``reconnect=True``) the client dials again.

    Thread-safety: :meth:`run_forever` blocks the calling thread.
    :meth:`stop` is thread-safe and may be called from a signal
    handler or another thread to end it. Everything else (the
    ``on_*`` hooks, :meth:`subscribe`) is invoked on the run_forever
    thread.
    """

    def __init__(
        self,
        url: str,
        api_key: str,
        *,
        devices: Optional[Iterable[str]] = None,
        reconnect: bool = True,
        initial_backoff_s: float = 1.0,
        max_backoff_s: float = 30.0,
        name: Optional[str] = None,
        open_timeout_s: float = 10.0,
        ping_interval_s: float = 20.0,
    ):
        self.url = url
        self.api_key = api_key
        self.devices: Optional[List[str]] = list(devices) if devices else None
        self.reconnect = reconnect
        self.initial_backoff_s = initial_backoff_s
        self.max_backoff_s = max_backoff_s
        self.name = name or type(self).__name__
        self.open_timeout_s = open_timeout_s
        self.ping_interval_s = ping_interval_s

        self._stop_requested = threading.Event()
        self._ws: Optional[ClientConnection] = None

    # ==============================================================
    # Override these — this is the whole point of the class.
    # ==============================================================

    def on_event(self, event: dict) -> None:
        """Called for every device event on your account.

        Override this method to act on events. It runs on the
        run_forever thread: keep it quick — while it runs, further
        events queue up behind it (the server buffers, then drops
        THIS connection with code 1013 if the queue overflows, and a
        healthy client reconnects). For slow work (uploads, emails),
        hand the event to a queue/worker thread and return.

        ``event`` is a dict, e.g.::

            {
                "customer_uid": "cust_1a2b3c4d5e6f",
                "device_uid":   "550e8400-e29b-41d4-a716-446655440000",
                "relays_state": {"relay_0": "on", "relay_1": "off"},
                "rssi":         -58,          # WiFi signal, dBm
                "uptime_s":     3600,         # seconds since boot
                "received_at":  "2026-10-01T12:00:00.123456+00:00",
                ...                           # any extra firmware fields
            }

        ``relays_state`` maps every relay to ``"on"``/``"off"``. The
        server sends changes-only traffic: the first message of a
        device is its current state (your snapshot), after that only
        actual relay flips — heartbeat repeats and rssi/uptime drift
        are suppressed server-side, so a quiet stream means "nothing
        changed".

        The default implementation prints the event as one JSON line
        to stdout, which is exactly what the command-line client
        does. Override it to do anything else.
        """
        print(json.dumps(event, separators=(",", ":")), flush=True)

    def on_connected(self) -> None:
        """TCP + WebSocket handshake succeeded; about to authenticate.

        Override for "we are dialing again" logging. The connection is
        NOT usable yet — authentication may still fail.
        """

    def on_auth_ok(self, info: dict) -> None:
        """Authentication accepted; the event stream starts now.

        ``info`` carries ``customer_uid`` and ``actor_type``. Override
        to log which account a connection is bound to, or to send a
        "back online" notification.
        """

    def on_disconnected(self, code: int, reason: str) -> None:
        """The connection ended.

        ``code`` is the WebSocket close code (1000 = normal, 1013 =
        you were too slow reading events, 4001-4004 = authentication
        problem). ``reason`` is a short human-readable string. Called
        before a reconnect attempt (when ``reconnect=True``) — use it
        for "connection lost" alerting.
        """

    def on_subscribed(self, devices: list) -> None:
        """The server acknowledged the device filter.

        ``devices`` is the list now in effect; ``[]`` means every
        device on the account. Override when your logic must wait for
        the filter to be live before trusting what it sees (or to log
        the effective subscription). Fires on every reconnect too.
        """

    def on_error(self, exc: Exception) -> None:
        """A connection attempt or the stream failed transiently.

        Not called for AuthenticationError (that stops the client and
        propagates). Default prints to stderr; override to route into
        your own logging/alerting.
        """
        import sys

        print(f"{self.name}: {exc}", file=sys.stderr, flush=True)

    # ==============================================================
    # Lifecycle
    # ==============================================================

    def run_forever(self) -> None:
        """Connect, authenticate and dispatch events until stopped.

        Blocks the calling thread. With ``reconnect=True`` (default)
        transient failures back off and retry forever; a bad API key
        raises :class:`AuthenticationError` (permanent — nothing to
        retry). With ``reconnect=False`` a transient failure
        propagates to the caller instead of retrying.
        :meth:`stop` ends the loop from another thread;
        KeyboardInterrupt (Ctrl-C) ends it cleanly too.

        Returns when the client has stopped for good.
        """
        backoff = self.initial_backoff_s
        while not self._stop_requested.is_set():
            try:
                self._run_once()
                # _run_once returning normally means stop() was the
                # reason (or the server closed politely and reconnect
                # is off).
                return
            except AuthenticationError:
                # Permanent: wrong key, or not a customer key.
                raise
            except ConnectionClosedClean:
                if self._stop_requested.is_set() or not self.reconnect:
                    return
                self._sleep_backoff(backoff)
                backoff = self._next_backoff(backoff)
            except KeyboardInterrupt:  # pragma: no cover - signal path
                self._stop_requested.set()
                return
            except Exception as exc:
                self.on_error(exc)
                if self._stop_requested.is_set() or not self.reconnect:
                    raise
                self._sleep_backoff(backoff)
                backoff = self._next_backoff(backoff)

    def stop(self) -> None:
        """Ask a running client to finish (thread-safe).

        The current connection is closed and run_forever returns.
        Safe to call from a signal handler or watchdog thread even if
        the client never connected.
        """
        self._stop_requested.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close(1000, "client stop")
            except Exception:
                pass

    def subscribe(self, devices: Optional[Iterable[str]]) -> None:
        """Change the device filter on the live connection.

        ``None`` or an empty list returns to "every device on the
        account". Takes effect immediately if connected, otherwise at
        the next (re)connect. The server's confirmation is delivered
        to :meth:`on_subscribed`, never to :meth:`on_event`.
        """
        self.devices = list(devices) if devices else None
        ws = self._ws
        if ws is not None:
            self._send_subscribe(ws)

    # ==============================================================
    # Internals — one full connection, auth handshake, receive loop.
    # ==============================================================

    def _run_once(self) -> None:
        self.on_connected()
        # Context-manager form: websockets' sync client warns (and in
        # future versions may refuse) on bare connect(); the with-block
        # guarantees the socket is closed on every exit path.
        with connect(
            self.url,
            open_timeout=self.open_timeout_s,
            ping_interval=self.ping_interval_s,
        ) as ws:
            self._ws = ws
            try:
                self._authenticate(ws)
                if self.devices is not None:
                    self._send_subscribe(ws)
                self._receive_loop(ws)
            finally:
                self._ws = None

    def _authenticate(self, ws: ClientConnection) -> None:
        """Send the auth frame and wait for the verdict.

        The server closes with a 4xxx code and an ``auth_error``
        frame on failure — permanent, so raise AuthenticationError.
        """
        try:
            ws.send(json.dumps({"type": "auth", "api_key": self.api_key}))
            while True:
                frame = self._recv_json(ws)
                if frame is None:
                    raise ConnectionClosedClean("closed during authentication")
                kind = frame.get("type")
                if kind == "auth_ok":
                    self.on_auth_ok(frame)
                    return
                if kind == "auth_error":
                    raise AuthenticationError(
                        f"server rejected the api key: {frame.get('error', '?')}"
                    )
                # Other frame types are ignored during the handshake.
        except AuthenticationError:
            raise
        except Exception as exc:
            raise ConnectionClosedClean(f"authentication failed: {exc}") from exc

    def _receive_loop(self, ws: ClientConnection) -> None:
        """Dispatch frames until the connection ends.

        Only ``event`` frames reach on_event; everything else
        (subscribed acks, pongs, future additions) is library
        business.
        """
        try:
            while True:
                frame = self._recv_json(ws)
                if frame is None:
                    raise ConnectionClosedClean("connection closed by server")
                kind = frame.get("type")
                if kind == "event":
                    event = frame.get("event")
                    if isinstance(event, dict):
                        self.on_event(event)
                elif kind == "subscribed":
                    self.on_subscribed(frame.get("devices") or [])
                elif kind == "auth_error":
                    # Key revoked mid-session.
                    raise AuthenticationError(
                        f"server revoked the session: {frame.get('error', '?')}"
                    )
                # pong / unknown: ignored by design
        except AuthenticationError:
            raise
        except ConnectionClosedClean:
            raise
        except Exception as exc:
            raise ConnectionClosedClean(f"receive loop failed: {exc}") from exc

    # ---- small helpers -------------------------------------------------

    def _send_subscribe(self, ws: ClientConnection) -> None:
        ws.send(
            json.dumps(
                {"type": "subscribe", "devices": list(self.devices or [])}
            )
        )

    @staticmethod
    def _recv_json(ws: ClientConnection) -> Optional[dict]:
        """Next JSON-object frame, or None if the socket closed.

        Raises on transport errors (websockets raises its own
        ConnectionClosed subclasses, which the callers translate).
        """
        raw = ws.recv()  # returns None-like "closure" via exception or str
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            frame = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
        return frame if isinstance(frame, dict) else None

    def _sleep_backoff(self, seconds: float) -> None:
        self.on_disconnected(1006, "connection lost; backing off")
        # Interruptible sleep: stop() ends the wait early.
        if self._stop_requested.wait(seconds):
            self._stop_requested.set()

    def _next_backoff(self, current: float) -> float:
        return min(current * 2, self.max_backoff_s)

    # Context-manager sugar: `with client: client.run_forever()`
    def __enter__(self) -> "ChabaEventClient":
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc: Optional[BaseException],
        tb: Optional[types.TracebackType],
    ) -> None:
        self.stop()
