"""In-memory ring buffer + pub/sub fan-out for live dashboard updates.

Single-process only — this portal is a local observability tool, not a
durable store. Restarting it drops history; that's fine for its purpose.
"""

from __future__ import annotations

import asyncio
import itertools
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class Event:
    id: int
    kind: str  # log_query|log_response|log_state|log_dr|log_subscribe|log_other|sip_subscribe|sip_notify
    timestamp: str
    summary: str
    detail: dict[str, Any] = field(default_factory=dict)
    source: str = "unknown"   # node name (from nodes.json) or raw elementId/agencyId
    role: str = "?"           # LVF|ECRF|MCS|GCS|MDS|... from nodes.json, "?" if unattributed


class EventStore:
    def __init__(self, maxlen: int = 2000) -> None:
        self._events: deque[Event] = deque(maxlen=maxlen)
        self._subscribers: set[asyncio.Queue[Event]] = set()
        self._counter = itertools.count(1)

    def add(
        self,
        kind: str,
        summary: str,
        detail: dict[str, Any] | None = None,
        source: str = "unknown",
        role: str = "?",
    ) -> Event:
        event = Event(
            id=next(self._counter),
            kind=kind,
            timestamp=datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            summary=summary,
            detail=detail or {},
            source=source,
            role=role,
        )
        self._events.append(event)
        for queue in list(self._subscribers):
            queue.put_nowait(event)
        return event

    def recent(self, limit: int = 200) -> list[Event]:
        if limit >= len(self._events):
            return list(self._events)
        return list(self._events)[-limit:]

    def subscribe(self) -> asyncio.Queue[Event]:
        queue: asyncio.Queue[Event] = asyncio.Queue()
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[Event]) -> None:
        self._subscribers.discard(queue)

    def clear(self) -> None:
        self._events.clear()


store = EventStore()
