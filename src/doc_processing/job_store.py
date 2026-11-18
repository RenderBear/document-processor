"""Durable job and lifecycle-event persistence contracts."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from .events import JobEvent
from .models import ArtifactRetention, DispatchCommand, Job, TERMINAL_JOB_STATES


class ConcurrentJobUpdate(RuntimeError):
    """Raised when a stale job snapshot would overwrite newer state."""


@dataclass(frozen=True)
class PendingWebhookDelivery:
    """A retained lifecycle event awaiting webhook delivery."""

    event: JobEvent
    url: str
    attempt_count: int


class JobStore(Protocol):
    async def start(self) -> None: ...

    async def close(self) -> None: ...

    async def create_job(
        self,
        job: Job,
        *,
        webhook_url: str | None = None,
        events: tuple[JobEvent, ...] = (),
        dispatch: DispatchCommand | None = None,
    ) -> None: ...

    async def save_job(
        self,
        job: Job,
        event: JobEvent | None = None,
        *,
        dispatch: DispatchCommand | None = None,
    ) -> None: ...

    async def get_job(self, job_id: str) -> Job: ...

    async def list_events(self, job_id: str, *, after_version: int = 0) -> list[JobEvent]: ...

    async def list_recoverable_jobs(self) -> list[Job]: ...

    async def list_session_jobs(self, tenant_id: str, session_id: str) -> list[Job]: ...

    async def ensure_dispatch(self, job_id: str) -> DispatchCommand: ...

    async def list_pending_dispatches(self, *, limit: int = 100) -> list[DispatchCommand]: ...

    async def record_dispatch_published(self, command_id: str) -> None: ...

    async def record_dispatch_failure(self, command_id: str, error: str) -> None: ...

    async def list_pending_webhooks(
        self, *, limit: int = 100
    ) -> list[PendingWebhookDelivery]: ...

    async def record_webhook_attempt(
        self,
        event_id: str,
        *,
        state: str,
        next_attempt_at: datetime | None,
        error: str | None,
    ) -> None: ...


def _expected_version(job: Job, event: JobEvent | None) -> int:
    if event is None:
        return job.version
    if event.job_id != job.job_id or event.job_version != job.version:
        raise ValueError("event identity and version must match the job snapshot")
    return job.version - 1


def _job_payload(job: Job) -> str:
    return json.dumps(
        job.model_dump(mode="json", exclude_computed_fields=True),
        separators=(",", ":"),
    )


class InMemoryJobStore:
    """Hermetic JobStore implementation used by tests and local-only runs."""

    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._events: dict[str, list[JobEvent]] = {}
        self._webhook_urls: dict[str, str] = {}
        self._webhook_attempts: dict[str, dict[str, object]] = {}
        self._dispatches: dict[str, dict[str, object]] = {}
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def create_job(
        self,
        job: Job,
        *,
        webhook_url: str | None = None,
        events: tuple[JobEvent, ...] = (),
        dispatch: DispatchCommand | None = None,
    ) -> None:
        async with self._lock:
            if job.job_id in self._jobs:
                raise ValueError(f"job already exists: {job.job_id}")
            versions = [event.job_version for event in events]
            if any(event.job_id != job.job_id for event in events):
                raise ValueError("event identity must match the job snapshot")
            if versions != list(range(1, len(events) + 1)) or (
                events and events[-1].job_version != job.version
            ):
                raise ValueError("initial events must be contiguous through the job version")
            if dispatch is not None and dispatch.job_id != job.job_id:
                raise ValueError("dispatch identity must match the job snapshot")
            self._jobs[job.job_id] = job.model_copy(deep=True)
            self._events[job.job_id] = [event.model_copy(deep=True) for event in events]
            if webhook_url is not None:
                self._webhook_urls[job.job_id] = webhook_url
            if dispatch is not None:
                self._dispatches[dispatch.command_id] = {
                    "command": dispatch.model_copy(deep=True),
                    "state": "pending",
                    "attempt_count": 0,
                    "last_error": None,
                }

    async def save_job(
        self,
        job: Job,
        event: JobEvent | None = None,
        *,
        dispatch: DispatchCommand | None = None,
    ) -> None:
        expected_version = _expected_version(job, event)
        if dispatch is not None and dispatch.job_id != job.job_id:
            raise ValueError("dispatch identity must match the job snapshot")
        async with self._lock:
            current = self._jobs.get(job.job_id)
            if current is None:
                raise KeyError(f"unknown job: {job.job_id}")
            if current.version != expected_version:
                raise ConcurrentJobUpdate(
                    f"job {job.job_id} is at version {current.version}, "
                    f"expected {expected_version}"
                )
            if event is not None and any(
                saved.job_version == event.job_version for saved in self._events[job.job_id]
            ):
                raise ConcurrentJobUpdate(
                    f"job {job.job_id} already has event version {event.job_version}"
                )
            self._jobs[job.job_id] = job.model_copy(deep=True)
            if event is not None:
                self._events[job.job_id].append(event.model_copy(deep=True))
            if dispatch is not None:
                self._dispatches.setdefault(
                    dispatch.command_id,
                    {
                        "command": dispatch.model_copy(deep=True),
                        "state": "pending",
                        "attempt_count": 0,
                        "last_error": None,
                    },
                )

    async def get_job(self, job_id: str) -> Job:
        async with self._lock:
            try:
                return self._jobs[job_id].model_copy(deep=True)
            except KeyError as exc:
                raise KeyError(f"unknown job: {job_id}") from exc

    async def list_events(self, job_id: str, *, after_version: int = 0) -> list[JobEvent]:
        async with self._lock:
            if job_id not in self._jobs:
                raise KeyError(f"unknown job: {job_id}")
            return [
                event.model_copy(deep=True)
                for event in self._events[job_id]
                if event.job_version > after_version
            ]

    async def list_recoverable_jobs(self) -> list[Job]:
        async with self._lock:
            jobs = [job for job in self._jobs.values() if job.state not in TERMINAL_JOB_STATES]
            jobs.sort(key=lambda job: (-int(job.priority), job.created_at, job.job_id))
            return [job.model_copy(deep=True) for job in jobs]

    async def list_session_jobs(self, tenant_id: str, session_id: str) -> list[Job]:
        async with self._lock:
            return [
                job.model_copy(deep=True)
                for job in self._jobs.values()
                if job.tenant_id == tenant_id
                and job.document.lifecycle.retention is ArtifactRetention.SESSION_BOUND
                and job.document.lifecycle.owner_id == session_id
            ]

    async def ensure_dispatch(self, job_id: str) -> DispatchCommand:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"unknown job: {job_id}")
            pending = next(
                (
                    row["command"]
                    for row in self._dispatches.values()
                    if row["state"] == "pending" and row["command"].job_id == job_id
                ),
                None,
            )
            if pending is not None:
                return pending.model_copy(deep=True)
            command = DispatchCommand(job_id=job_id)
            self._dispatches[command.command_id] = {
                "command": command.model_copy(deep=True),
                "state": "pending",
                "attempt_count": 0,
                "last_error": None,
            }
            return command

    async def list_pending_dispatches(self, *, limit: int = 100) -> list[DispatchCommand]:
        async with self._lock:
            rows = [
                row["command"].model_copy(deep=True)
                for row in self._dispatches.values()
                if row["state"] == "pending"
            ]
            rows.sort(key=lambda command: (command.created_at, command.command_id))
            return rows[:limit]

    async def record_dispatch_published(self, command_id: str) -> None:
        async with self._lock:
            try:
                row = self._dispatches[command_id]
            except KeyError as exc:
                raise KeyError(f"unknown dispatch command: {command_id}") from exc
            row["state"] = "published"
            row["attempt_count"] = int(row["attempt_count"]) + 1
            row["last_error"] = None

    async def record_dispatch_failure(self, command_id: str, error: str) -> None:
        async with self._lock:
            try:
                row = self._dispatches[command_id]
            except KeyError as exc:
                raise KeyError(f"unknown dispatch command: {command_id}") from exc
            row["attempt_count"] = int(row["attempt_count"]) + 1
            row["last_error"] = error[:2000]

    async def list_pending_webhooks(
        self, *, limit: int = 100
    ) -> list[PendingWebhookDelivery]:
        from .models import utc_now

        async with self._lock:
            now = utc_now()
            pending = []
            job_ids = sorted(
                self._webhook_urls,
                key=lambda job_id: (self._jobs[job_id].created_at, job_id),
            )
            for job_id in job_ids:
                for event in self._events[job_id]:
                    attempt = self._webhook_attempts.get(event.event_id)
                    if attempt is not None and attempt["state"] != "pending":
                        continue
                    next_attempt_at = attempt["next_attempt_at"] if attempt else None
                    if not isinstance(next_attempt_at, datetime) or next_attempt_at <= now:
                        pending.append(
                            PendingWebhookDelivery(
                                event=event.model_copy(deep=True),
                                url=self._webhook_urls[job_id],
                                attempt_count=int(attempt["attempt_count"]) if attempt else 0,
                            )
                        )
                    # Preserve delivery order per job, including while an earlier
                    # event is waiting for its retry window.
                    break
                if len(pending) >= limit:
                    break
            return pending

    async def record_webhook_attempt(
        self,
        event_id: str,
        *,
        state: str,
        next_attempt_at: datetime | None,
        error: str | None,
    ) -> None:
        if state not in {"pending", "delivered", "abandoned"}:
            raise ValueError(f"invalid webhook delivery state: {state}")
        async with self._lock:
            known = any(
                event.event_id == event_id
                for events in self._events.values()
                for event in events
            )
            if not known:
                raise KeyError(f"unknown event: {event_id}")
            previous = self._webhook_attempts.get(event_id)
            self._webhook_attempts[event_id] = {
                "state": state,
                "attempt_count": int(previous["attempt_count"]) + 1 if previous else 1,
                "next_attempt_at": next_attempt_at,
                "error": error,
            }


class PostgresJobStore:
    """PostgreSQL-backed canonical job snapshots and ordered event history."""

    def __init__(
        self,
        database_url: str,
        *,
        min_pool_size: int = 1,
        max_pool_size: int = 10,
        pool: Any | None = None,
    ) -> None:
        if not database_url:
            raise ValueError("database_url must not be empty")
        self.database_url = database_url
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size
        self._pool = pool
        self._owns_pool = pool is None

    async def start(self) -> None:
        if self._pool is None:
            import asyncpg

            self._pool = await asyncpg.create_pool(
                dsn=self.database_url,
                min_size=self.min_pool_size,
                max_size=self.max_pool_size,
            )
        missing_tables = []
        for table_name in (
            "document_job",
            "job_event",
            "job_webhook_subscription",
            "webhook_delivery",
            "dispatch_outbox",
        ):
            schema_exists = await self._pool.fetchval(
                "SELECT to_regclass($1)", f"public.{table_name}"
            )
            if schema_exists is None:
                missing_tables.append(table_name)
        if missing_tables:
            if self._owns_pool:
                await self._pool.close()
                self._pool = None
            raise RuntimeError(
                "PostgreSQL schema is incomplete "
                f"(missing {', '.join(missing_tables)}); apply migrations in order"
            )

    async def close(self) -> None:
        if self._pool is not None and self._owns_pool:
            await self._pool.close()
            self._pool = None

    def _require_pool(self) -> Any:
        if self._pool is None:
            raise RuntimeError("PostgresJobStore has not been started")
        return self._pool

    async def create_job(
        self,
        job: Job,
        *,
        webhook_url: str | None = None,
        events: tuple[JobEvent, ...] = (),
        dispatch: DispatchCommand | None = None,
    ) -> None:
        payload = _job_payload(job)
        pool = self._require_pool()
        try:
            async with pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute(
                        """
                        INSERT INTO document_job (
                            job_id, tenant_id, state, priority, state_version,
                            payload, created_at, updated_at
                        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
                        """,
                        job.job_id,
                        job.tenant_id,
                        job.state.value,
                        int(job.priority),
                        job.version,
                        payload,
                        job.created_at,
                        job.updated_at,
                    )
                    if webhook_url is not None:
                        await connection.execute(
                            """
                            INSERT INTO job_webhook_subscription (job_id, url, created_at)
                            VALUES ($1, $2, $3)
                            """,
                            job.job_id,
                            webhook_url,
                            job.created_at,
                        )
                    for event in events:
                        if event.job_id != job.job_id:
                            raise ValueError("event identity must match the job snapshot")
                        await self._insert_event(connection, event)
                    if dispatch is not None:
                        if dispatch.job_id != job.job_id:
                            raise ValueError("dispatch identity must match the job snapshot")
                        await self._insert_dispatch(connection, dispatch)
        except Exception as exc:
            if getattr(exc, "sqlstate", None) == "23505":
                raise ValueError(f"job already exists: {job.job_id}") from exc
            raise

    async def save_job(
        self,
        job: Job,
        event: JobEvent | None = None,
        *,
        dispatch: DispatchCommand | None = None,
    ) -> None:
        expected_version = _expected_version(job, event)
        payload = _job_payload(job)
        pool = self._require_pool()
        async with pool.acquire() as connection:
            async with connection.transaction():
                updated = await connection.fetchval(
                    """
                    UPDATE document_job
                    SET tenant_id = $2,
                        state = $3,
                        priority = $4,
                        state_version = $5,
                        payload = $6::jsonb,
                        updated_at = $7
                    WHERE job_id = $1 AND state_version = $8
                    RETURNING job_id
                    """,
                    job.job_id,
                    job.tenant_id,
                    job.state.value,
                    int(job.priority),
                    job.version,
                    payload,
                    job.updated_at,
                    expected_version,
                )
                if updated is None:
                    current = await connection.fetchval(
                        "SELECT state_version FROM document_job WHERE job_id = $1",
                        job.job_id,
                    )
                    if current is None:
                        raise KeyError(f"unknown job: {job.job_id}")
                    raise ConcurrentJobUpdate(
                        f"job {job.job_id} is at version {current}, "
                        f"expected {expected_version}"
                    )
                if event is not None:
                    try:
                        await self._insert_event(connection, event)
                    except Exception as exc:
                        if getattr(exc, "sqlstate", None) == "23505":
                            raise ConcurrentJobUpdate(
                                f"job {job.job_id} already has event version "
                                f"{event.job_version}"
                            ) from exc
                        raise
                if dispatch is not None:
                    if dispatch.job_id != job.job_id:
                        raise ValueError("dispatch identity must match the job snapshot")
                    await self._insert_dispatch(connection, dispatch)

    @staticmethod
    async def _insert_event(connection: Any, event: JobEvent) -> None:
        event_payload = json.dumps(event.model_dump(mode="json"), separators=(",", ":"))
        await connection.execute(
            """
            INSERT INTO job_event (
                event_id, job_id, job_version, event_type,
                tenant_id, occurred_at, payload
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            """,
            event.event_id,
            event.job_id,
            event.job_version,
            event.event_type,
            event.tenant_id,
            event.occurred_at,
            event_payload,
        )

    @staticmethod
    async def _insert_dispatch(connection: Any, command: DispatchCommand) -> None:
        await connection.execute(
            """
            INSERT INTO dispatch_outbox (
                command_id, job_id, subject, payload, state, created_at, updated_at
            ) VALUES ($1, $2, $3, $4::jsonb, 'pending', $5, $5)
            ON CONFLICT (command_id) DO NOTHING
            """,
            command.command_id,
            command.job_id,
            command.subject,
            command.model_dump_json(),
            command.created_at,
        )

    async def get_job(self, job_id: str) -> Job:
        payload = await self._require_pool().fetchval(
            "SELECT payload FROM document_job WHERE job_id = $1",
            job_id,
        )
        if payload is None:
            raise KeyError(f"unknown job: {job_id}")
        return Job.model_validate_json(payload)

    async def list_events(self, job_id: str, *, after_version: int = 0) -> list[JobEvent]:
        rows = await self._require_pool().fetch(
            """
            SELECT payload
            FROM job_event
            WHERE job_id = $1 AND job_version > $2
            ORDER BY job_version
            """,
            job_id,
            after_version,
        )
        return [JobEvent.model_validate_json(row["payload"]) for row in rows]

    async def list_recoverable_jobs(self) -> list[Job]:
        rows = await self._require_pool().fetch(
            """
            SELECT payload
            FROM document_job
            WHERE state <> ALL($1::text[])
            ORDER BY priority DESC, created_at, job_id
            """,
            [state.value for state in TERMINAL_JOB_STATES],
        )
        return [Job.model_validate_json(row["payload"]) for row in rows]

    async def list_session_jobs(self, tenant_id: str, session_id: str) -> list[Job]:
        rows = await self._require_pool().fetch(
            """
            SELECT payload
            FROM document_job
            WHERE tenant_id = $1
              AND payload #>> '{document,lifecycle,retention}' = 'session_bound'
              AND payload #>> '{document,lifecycle,owner_id}' = $2
            ORDER BY created_at, job_id
            """,
            tenant_id,
            session_id,
        )
        return [Job.model_validate_json(row["payload"]) for row in rows]

    async def ensure_dispatch(self, job_id: str) -> DispatchCommand:
        existing = await self._require_pool().fetchval(
            """
            SELECT payload FROM dispatch_outbox
            WHERE job_id = $1 AND state = 'pending'
            ORDER BY created_at DESC LIMIT 1
            """,
            job_id,
        )
        if existing is not None:
            return DispatchCommand.model_validate_json(existing)
        command = DispatchCommand(job_id=job_id)
        async with self._require_pool().acquire() as connection:
            exists = await connection.fetchval(
                "SELECT 1 FROM document_job WHERE job_id = $1", job_id
            )
            if exists is None:
                raise KeyError(f"unknown job: {job_id}")
            await self._insert_dispatch(connection, command)
        return command

    async def list_pending_dispatches(self, *, limit: int = 100) -> list[DispatchCommand]:
        rows = await self._require_pool().fetch(
            """
            SELECT payload FROM dispatch_outbox
            WHERE state = 'pending'
            ORDER BY created_at, command_id
            LIMIT $1
            """,
            limit,
        )
        return [DispatchCommand.model_validate_json(row["payload"]) for row in rows]

    async def record_dispatch_published(self, command_id: str) -> None:
        result = await self._require_pool().execute(
            """
            UPDATE dispatch_outbox
            SET state = 'published', attempt_count = attempt_count + 1,
                last_error = NULL, published_at = NOW(), updated_at = NOW()
            WHERE command_id = $1
            """,
            command_id,
        )
        if result == "UPDATE 0":
            raise KeyError(f"unknown dispatch command: {command_id}")

    async def record_dispatch_failure(self, command_id: str, error: str) -> None:
        result = await self._require_pool().execute(
            """
            UPDATE dispatch_outbox
            SET attempt_count = attempt_count + 1, last_error = $2, updated_at = NOW()
            WHERE command_id = $1
            """,
            command_id,
            error[:2000],
        )
        if result == "UPDATE 0":
            raise KeyError(f"unknown dispatch command: {command_id}")

    async def list_pending_webhooks(
        self, *, limit: int = 100
    ) -> list[PendingWebhookDelivery]:
        rows = await self._require_pool().fetch(
            """
            SELECT event.payload,
                   subscription.url,
                   COALESCE(delivery.attempt_count, 0) AS attempt_count
            FROM job_event AS event
            JOIN job_webhook_subscription AS subscription USING (job_id)
            LEFT JOIN webhook_delivery AS delivery USING (event_id)
            WHERE COALESCE(delivery.state, 'pending') = 'pending'
              AND (delivery.next_attempt_at IS NULL OR delivery.next_attempt_at <= NOW())
              AND NOT EXISTS (
                  SELECT 1
                  FROM job_event AS earlier
                  LEFT JOIN webhook_delivery AS earlier_delivery
                    ON earlier_delivery.event_id = earlier.event_id
                  WHERE earlier.job_id = event.job_id
                    AND earlier.job_version < event.job_version
                    AND COALESCE(earlier_delivery.state, 'pending') = 'pending'
              )
            ORDER BY event.occurred_at, event.job_id, event.job_version
            LIMIT $1
            """,
            limit,
        )
        deliveries = []
        for row in rows:
            payload = row["payload"]
            event = (
                JobEvent.model_validate_json(payload)
                if isinstance(payload, str)
                else JobEvent.model_validate(payload)
            )
            deliveries.append(
                PendingWebhookDelivery(
                    event=event,
                    url=row["url"],
                    attempt_count=row["attempt_count"],
                )
            )
        return deliveries

    async def record_webhook_attempt(
        self,
        event_id: str,
        *,
        state: str,
        next_attempt_at: datetime | None,
        error: str | None,
    ) -> None:
        if state not in {"pending", "delivered", "abandoned"}:
            raise ValueError(f"invalid webhook delivery state: {state}")
        await self._require_pool().execute(
            """
            INSERT INTO webhook_delivery (
                event_id, state, attempt_count, next_attempt_at,
                last_error, delivered_at, updated_at
            ) VALUES (
                $1, $2, 1, $3, $4,
                CASE WHEN $2 = 'delivered' THEN NOW() ELSE NULL END,
                NOW()
            )
            ON CONFLICT (event_id) DO UPDATE
            SET state = EXCLUDED.state,
                attempt_count = webhook_delivery.attempt_count + 1,
                next_attempt_at = EXCLUDED.next_attempt_at,
                last_error = EXCLUDED.last_error,
                delivered_at = CASE
                    WHEN EXCLUDED.state = 'delivered' THEN NOW()
                    ELSE webhook_delivery.delivered_at
                END,
                updated_at = NOW()
            """,
            event_id,
            state,
            next_attempt_at,
            error,
        )
