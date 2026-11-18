"""Transactional-outbox publication and local or JetStream work delivery."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Protocol

from .models import DispatchCommand

CommandHandler = Callable[[DispatchCommand], Awaitable[None]]


class DispatchStore(Protocol):
    async def list_pending_dispatches(self, *, limit: int = 100) -> list[DispatchCommand]: ...

    async def record_dispatch_published(self, command_id: str) -> None: ...

    async def record_dispatch_failure(self, command_id: str, error: str) -> None: ...


class DispatchBroker(Protocol):
    async def publish(self, command: DispatchCommand) -> None: ...

    async def consume(self, handler: CommandHandler, *, workers: int) -> None: ...

    async def close(self) -> None: ...


class InProcessDispatchBroker:
    """Hermetic broker used for local runs; it preserves claim-check semantics."""

    def __init__(self) -> None:
        self.subject = "document.jobs.process"
        self._queue: asyncio.Queue[DispatchCommand] = asyncio.Queue()

    async def publish(self, command: DispatchCommand) -> None:
        await self._queue.put(command.model_copy(deep=True))

    async def consume(self, handler: CommandHandler, *, workers: int) -> None:
        async def handle(command: DispatchCommand) -> None:
            try:
                await handler(command)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Match at-least-once broker delivery without spinning hot.
                await asyncio.sleep(0.05)
                await self._queue.put(command)
            finally:
                self._queue.task_done()

        # Local delivery is intentionally eager so all pending commands enter
        # the orchestrator's weighted fair queue before a worker selects work.
        async with asyncio.TaskGroup() as group:
            while True:
                command = await self._queue.get()
                group.create_task(handle(command))

    async def close(self) -> None:
        return None


class OutboxPublisher:
    """Retry pending database commands without coupling API readiness to the broker."""

    def __init__(
        self,
        store: DispatchStore,
        broker: DispatchBroker,
        *,
        poll_interval: float = 0.25,
    ) -> None:
        self.store = store
        self.broker = broker
        self.poll_interval = poll_interval
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="dispatch-outbox-publisher")

    def notify(self) -> None:
        self._wake.set()

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _run(self) -> None:
        while True:
            try:
                pending = await self.store.list_pending_dispatches()
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(self.poll_interval)
                continue
            for command in pending:
                try:
                    await self.broker.publish(command)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    try:
                        await self.store.record_dispatch_failure(command.command_id, str(exc))
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        await asyncio.sleep(self.poll_interval)
                    break
                else:
                    await self.store.record_dispatch_published(command.command_id)
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.poll_interval)
            except TimeoutError:
                pass


class JetStreamDispatchBroker:
    """Resilient NATS JetStream transport with explicit ack and quarantine."""

    def __init__(
        self,
        servers: list[str],
        *,
        stream: str = "DOCUMENT_PROCESSING",
        subject: str = "document.jobs.process",
        durable: str = "document-processors",
        replicas: int = 3,
        ack_wait_seconds: float = 120.0,
        max_deliver: int = 5,
        max_age_seconds: float = 604800,
        max_bytes: int = 10 * 1024 * 1024 * 1024,
        max_messages: int = 1_000_000,
    ) -> None:
        self.servers = servers
        self.stream = stream
        self.subject = subject
        self.durable = durable
        self.replicas = replicas
        self.ack_wait_seconds = ack_wait_seconds
        self.max_deliver = max_deliver
        self.max_age_seconds = max_age_seconds
        self.max_bytes = max_bytes
        self.max_messages = max_messages
        self._nc = None
        self._js = None
        self._connect_lock = asyncio.Lock()

    async def _jetstream(self):
        async with self._connect_lock:
            if self._nc is not None and self._nc.is_connected:
                return self._js
            import nats
            from nats.js.api import RetentionPolicy, StorageType, StreamConfig
            from nats.js.errors import NotFoundError

            self._nc = await nats.connect(
                servers=self.servers,
                connect_timeout=2,
                max_reconnect_attempts=-1,
            )
            self._js = self._nc.jetstream()
            try:
                await self._js.stream_info(self.stream)
            except NotFoundError:
                await self._js.add_stream(
                    config=StreamConfig(
                        name=self.stream,
                        subjects=[self.subject, f"{self.subject}.quarantine"],
                        retention=RetentionPolicy.WORK_QUEUE,
                        storage=StorageType.FILE,
                        num_replicas=self.replicas,
                        max_age=self.max_age_seconds,
                        max_bytes=self.max_bytes,
                        max_msgs=self.max_messages,
                    )
                )
            return self._js

    async def publish(self, command: DispatchCommand) -> None:
        js = await self._jetstream()
        await js.publish(
            command.subject,
            command.model_dump_json().encode(),
            headers={"Nats-Msg-Id": command.command_id},
        )

    async def consume(self, handler: CommandHandler, *, workers: int) -> None:
        from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

        while True:
            try:
                js = await self._jetstream()
                subscription = await js.pull_subscribe(
                    self.subject,
                    durable=self.durable,
                    stream=self.stream,
                    config=ConsumerConfig(
                        durable_name=self.durable,
                        ack_policy=AckPolicy.EXPLICIT,
                        deliver_policy=DeliverPolicy.ALL,
                        ack_wait=self.ack_wait_seconds,
                        max_deliver=self.max_deliver,
                    ),
                )

                async def worker() -> None:
                    while True:
                        try:
                            messages = await subscription.fetch(1, timeout=1)
                        except TimeoutError:
                            continue
                        for message in messages:
                            await self._handle_message(js, message, handler)

                async with asyncio.TaskGroup() as group:
                    for _ in range(workers):
                        group.create_task(worker())
            except asyncio.CancelledError:
                raise
            except Exception:
                await self._disconnect()
                await asyncio.sleep(1)

    async def _handle_message(self, js, message, handler: CommandHandler) -> None:
        delivered = getattr(getattr(message, "metadata", None), "num_delivered", 1)
        try:
            command = DispatchCommand.model_validate_json(message.data)
        except Exception:
            if delivered >= self.max_deliver:
                import hashlib

                digest = hashlib.sha256(message.data).hexdigest()
                await js.publish(
                    f"{self.subject}.quarantine",
                    message.data,
                    headers={"Nats-Msg-Id": f"quarantine-invalid-{digest}"},
                )
                await message.ack()
            else:
                await message.nak(delay=min(60, 2 ** max(0, delivered - 1)))
            return
        heartbeat = asyncio.create_task(self._extend_ack(message))
        try:
            await handler(command)
        except asyncio.CancelledError:
            raise
        except Exception:
            if delivered >= self.max_deliver:
                await js.publish(
                    f"{self.subject}.quarantine",
                    message.data,
                    headers={
                        "Nats-Msg-Id": f"quarantine-{command.command_id}",
                        "X-Original-Command-Id": command.command_id,
                    },
                )
                await message.ack()
            else:
                await message.nak(delay=min(60, 2 ** max(0, delivered - 1)))
        else:
            await message.ack()
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat

    async def _extend_ack(self, message) -> None:
        interval = max(1.0, self.ack_wait_seconds / 2)
        while True:
            await asyncio.sleep(interval)
            await message.in_progress()

    async def _disconnect(self) -> None:
        if self._nc is not None:
            with suppress(Exception):
                await self._nc.close()
        self._nc = None
        self._js = None

    async def close(self) -> None:
        await self._disconnect()
