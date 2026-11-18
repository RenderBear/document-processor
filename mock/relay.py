"""Frontend projection and SSE relay for processor webhook events."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from mock.store import SQLiteProjectionStore

FRONTEND_STATE_BY_PROCESSOR_STATE = {
    "accepted": "queued",
    "validating": "queued",
    "queued": "queued",
    "processing": "processing",
    "succeeded": "completed",
    "failed": "failed",
    "rejected": "failed",
    "cancelled": "failed",
}
TERMINAL_FRONTEND_STATES = frozenset({"completed", "failed"})
PERSISTENT_PRODUCER_KINDS = frozenset(
    {"rag_ingestion", "agent", "workflow"}
)


@dataclass(frozen=True)
class RelayEvent:
    sequence: int
    job_id: str
    filename: str
    producer_kind: str
    context_id: str
    state: str
    processor_state: str

    def as_dict(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "job_id": self.job_id,
            "filename": self.filename,
            "producer_kind": self.producer_kind,
            "context_id": self.context_id,
            "state": self.state,
            "processor_state": self.processor_state,
        }


@dataclass
class JobProjection:
    job_id: str
    filename: str
    producer_kind: str
    context_id: str
    processor_state: str
    processor_version: int = 0
    events: list[RelayEvent] = field(default_factory=list)
    subscribers: set[asyncio.Queue[RelayEvent]] = field(default_factory=set)

    @property
    def state(self) -> str:
        return FRONTEND_STATE_BY_PROCESSOR_STATE[self.processor_state]

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_FRONTEND_STATES

    @property
    def persistent(self) -> bool:
        return self.producer_kind in PERSISTENT_PRODUCER_KINDS

    def snapshot(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "filename": self.filename,
            "producer_kind": self.producer_kind,
            "context_id": self.context_id,
            "state": self.state,
            "processor_state": self.processor_state,
            "processor_version": self.processor_version,
            "version": self.events[-1].sequence,
            "terminal": self.terminal,
            "persistent": self.persistent,
        }


class JobRelay:
    """Own normalized UI state while the processor remains authoritative."""

    def __init__(self, store: SQLiteProjectionStore | None = None) -> None:
        self._jobs: dict[str, JobProjection] = {}
        self._lock = asyncio.Lock()
        self._store = store

    async def start(self) -> None:
        if self._store is None:
            return
        await self._store.start()
        for stored in await self._store.load():
            payload = stored.payload
            context_id = str(payload.get("context_id", "default"))
            projection = JobProjection(
                job_id=str(payload["job_id"]),
                filename=str(payload["filename"]),
                producer_kind=str(payload["producer_kind"]),
                context_id=context_id,
                processor_state=str(payload["processor_state"]),
                processor_version=int(payload.get("processor_version", 0)),
                events=[
                    RelayEvent(
                        sequence=int(event["sequence"]),
                        job_id=str(event["job_id"]),
                        filename=str(event["filename"]),
                        producer_kind=str(event["producer_kind"]),
                        context_id=str(event.get("context_id", context_id)),
                        state=str(event["state"]),
                        processor_state=str(event["processor_state"]),
                    )
                    for event in stored.events
                ],
            )
            if projection.events:
                self._jobs[projection.job_id] = projection

    async def create(
        self,
        *,
        job_id: str,
        filename: str,
        producer_kind: str,
        context_id: str,
        processor_state: str,
    ) -> JobProjection:
        normalized = FRONTEND_STATE_BY_PROCESSOR_STATE.get(processor_state)
        if normalized is None:
            raise ValueError(f"unsupported processor state: {processor_state}")

        projection = JobProjection(
            job_id=job_id,
            filename=filename,
            producer_kind=producer_kind,
            context_id=context_id,
            processor_state=processor_state,
        )
        projection.events.append(
            RelayEvent(
                sequence=1,
                job_id=job_id,
                filename=filename,
                producer_kind=producer_kind,
                context_id=context_id,
                state=normalized,
                processor_state=processor_state,
            )
        )
        async with self._lock:
            self._jobs[job_id] = projection
            await self._persist(projection)
        return projection

    async def get(self, job_id: str) -> JobProjection:
        async with self._lock:
            projection = self._jobs.get(job_id)
            if projection is None:
                raise KeyError(job_id)
            return projection

    async def list_persistent(
        self,
        *,
        producer_kind: str | None = None,
        context_id: str | None = None,
    ) -> tuple[JobProjection, ...]:
        async with self._lock:
            return tuple(
                job
                for job in self._jobs.values()
                if job.persistent
                and (producer_kind is None or job.producer_kind == producer_kind)
                and (context_id is None or job.context_id == context_id)
            )

    async def publish(
        self, job_id: str, processor_state: str, processor_version: int
    ) -> RelayEvent | None:
        normalized = FRONTEND_STATE_BY_PROCESSOR_STATE.get(processor_state)
        if normalized is None:
            return None

        async with self._lock:
            projection = self._jobs.get(job_id)
            if projection is None:
                raise KeyError(job_id)
            if processor_version <= projection.processor_version:
                return None
            projection.processor_version = processor_version
            if projection.state == normalized:
                projection.processor_state = processor_state
                await self._persist(projection)
                return None

            projection.processor_state = processor_state
            event = RelayEvent(
                sequence=projection.events[-1].sequence + 1,
                job_id=job_id,
                filename=projection.filename,
                producer_kind=projection.producer_kind,
                context_id=projection.context_id,
                state=normalized,
                processor_state=processor_state,
            )
            projection.events.append(event)
            await self._persist(projection)
            subscribers = tuple(projection.subscribers)

        for queue in subscribers:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # The browser reconnects with Last-Event-ID and receives history.
                pass
        return event

    @asynccontextmanager
    async def subscribe(
        self, job_id: str, *, after_sequence: int = 0
    ) -> AsyncIterator[tuple[tuple[RelayEvent, ...], asyncio.Queue[RelayEvent]]]:
        queue: asyncio.Queue[RelayEvent] = asyncio.Queue(maxsize=50)
        async with self._lock:
            projection = self._jobs.get(job_id)
            if projection is None:
                raise KeyError(job_id)
            backfill = tuple(
                event for event in projection.events if event.sequence > after_sequence
            )
            if not projection.terminal:
                projection.subscribers.add(queue)
        try:
            yield backfill, queue
        finally:
            async with self._lock:
                projection.subscribers.discard(queue)

    async def _persist(self, projection: JobProjection) -> None:
        if self._store is None or not projection.persistent:
            return
        await self._store.save(
            projection.snapshot(),
            tuple(event.as_dict() for event in projection.events),
        )


def encode_sse(event: RelayEvent) -> str:
    data = json.dumps(event.as_dict(), separators=(",", ":"))
    return f"id: {event.sequence}\nevent: state\ndata: {data}\n\n"
