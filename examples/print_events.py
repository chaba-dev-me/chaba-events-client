"""Print every device event, one JSON line per event.

The simplest possible consumer — no subclassing, the default
on_event already prints:

    python examples/print_events.py ws://10.0.4.1:5000 KEY
"""

import sys

from chaba_events_client import ChabaEventClient

url, api_key = sys.argv[1], sys.argv[2]

client = ChabaEventClient(url=url, api_key=api_key)
client.run_forever()
