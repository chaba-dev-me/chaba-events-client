"""chaba-events-client — receive Chaba device events over WebSocket.

The public surface is one class: :class:`ChabaEventClient`. Subclass it
and override :meth:`~ChabaEventClient.on_event` to act on device events
as they arrive; the connection, authentication, keep-alive and
reconnect logic are handled for you.

Quick start::

    from chaba_events_client import ChabaEventClient

    client = ChabaEventClient(
        url="ws://10.0.4.1:5000",
        api_key="<your customer API key>",
    )
    client.run_forever()   # prints events to stdout until Ctrl-C

See the README shipped with this package for the full documentation
and more subclass examples.
"""

from .client import AuthenticationError, ChabaEventClient, ConnectionClosedClean

__all__ = [
    "ChabaEventClient",
    "AuthenticationError",
    "ConnectionClosedClean",
]

__version__ = "0.2.0"
