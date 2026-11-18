"""Process-local live-event fan-out backed by durable JobStore history."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any
from uuid import uuid4

from pydantic import Field

from .models import StrictModel, utc_now

EventHandler = Callable[["JobEvent"], Awaitable[None] | None]


class JobEvent(StrictModel):
    specversion: str = "1.0"
    event_id: str = Field(default_factory=lambda: f"evt_{uuid4().hex}")
    event_type: str
    job_id: str
    job_version: int
    tenant_id: str
    occurred_at: datetime = Field(default_factory=utc_now)
    data: dict[str, Any] = Field(default_factory=dict)


class InProcessEventBus:
    """Fan events out to async callbacks and bounded per-subscriber queues."""

    def __init__(self) -> None:
        self._handlers: set[EventHandler] = set()
        self._streams: set[asyncio.Queue[JobEvent]] = set()
        self._history: list[JobEvent] = []
        self._lock = asyncio.Lock()

    @property
    def history(self) -> tuple[JobEvent, ...]:
        return tuple(event.model_copy(deep=True) for event in self._history)

    def subscribe(self, handler: EventHandler) -> Callable[[], None]:
        self._handlers.add(handler)

        def unsubscribe() -> None:
            self._handlers.discard(handler)

        return unsubscribe

    async def publish(self, event: JobEvent) -> None:
        async with self._lock:
            self._history.append(event.model_copy(deep=True))
            handlers = tuple(self._handlers)
            streams = tuple(self._streams)

        for queue in streams:
            try:
                queue.put_nowait(event.model_copy(deep=True))
            except asyncio.QueueFull:
                # A slow UI projection must recover from canonical job state.
                pass

        results = []
        for handler in handlers:
            try:
                result = handler(event.model_copy(deep=True))
            except Exception:
                # Publication records the event before notifying projections. A
                # broken observer cannot roll back canonical job state.
                continue
            if inspect.isawaitable(result):
                results.append(result)
        if results:
            await asyncio.gather(*results, return_exceptions=True)

    @asynccontextmanager
    async def stream(self, max_queue_size: int = 100) -> AsyncIterator[asyncio.Queue[JobEvent]]:
        queue: asyncio.Queue[JobEvent] = asyncio.Queue(maxsize=max_queue_size)
        self._streams.add(queue)
        try:
            yield queue
        finally:
            self._streams.discard(queue)
