"""Bounded passive feed display. Inspect cached attributes only; no method calls."""
from datetime import datetime, timezone
import math

def public_feed_observation(feed):
    seen = set()
    leaves = []
    def walk(node, depth=0):
        if depth > 5 or id(node) in seen or len(leaves) >= 64:
            return
        seen.add(id(node))
        data = vars(node) if hasattr(node, '__dict__') else {}
        children = data.get('children', data.get('feeds'))
        if isinstance(children, (tuple, list)) and children:
            for child in children[:64]: walk(child, depth+1)
            return
        connected = data.get('connected') if isinstance(data.get('connected'), bool) else None
        if 'socket' in data:
            connected = data['socket'] is not None
        epoch = data.get('last_wire_received_at')
        received = None
        if isinstance(epoch, (int, float)) and not isinstance(epoch, bool) and math.isfinite(epoch) and epoch > 0:
            try: received = datetime.fromtimestamp(epoch, timezone.utc).isoformat()
            except (OverflowError, ValueError, OSError): pass
        leaves.append({'socketPresent': connected, 'lastFrameAt': received})
    walk(feed)
    return {'schemaVersion': 'short-live-feed-display-v1', 'streams': leaves,
            'meaning': 'CACHED_SOCKET_AND_LAST_PROCESSED_WIRE_RECEIPT_ONLY'}
