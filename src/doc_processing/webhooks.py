"""Signed, retryable webhook delivery for retained lifecycle events."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from datetime import timedelta

import httpx

from .job_store import JobStore, PendingWebhookDelivery
from .models import utc_now

SIGNATURE_HEADER = "X-Document-Webhook-Signature"
TIMESTAMP_HEADER = "X-Document-Webhook-Timestamp"
DELIVERY_HEADER = "X-Document-Webhook-Id"


def webhook_signature(secret: str, timestamp: str, body: bytes) -> str:
    signed = timestamp.encode("ascii") + b"." + body
    digest = hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()
    return f"v1={digest}"


def verify_webhook_signature(
    secret: str,
    timestamp: str,
    body: bytes,
    signature: str,
    *,
    now: int | None = None,
    tolerance_seconds: int = 300,
) -> bool:
    try:
        sent_at = int(timestamp)
    except ValueError:
        return False
    current = int(time.time()) if now is None else now
    if abs(current - sent_at) > tolerance_seconds:
        return False
    expected = webhook_signature(secret, timestamp, body)
    return hmac.compare_digest(expected, signature)


class WebhookDispatcher:
    """Poll retained events and deliver each job's webhook in version order."""

    def __init__(
        self,
        job_store: JobStore,
        secret: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        poll_interval: float = 0.1,
        retry_base: float = 0.2,
        retry_max: float = 30.0,
        max_attempts: int = 12,
    ) -> None:
        if not secret:
            raise ValueError("webhook signing secret must not be empty")
        self.job_store = job_store
        self.secret = secret
        self.poll_interval = poll_interval
        self.retry_base = retry_base
        self.retry_max = retry_max
        self.max_attempts = max_attempts
        self._client = httpx.AsyncClient(transport=transport, timeout=10.0)
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="webhook-dispatcher")

    async def close(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._client.aclose()

    async def dispatch_available(self) -> int:
        deliveries = await self.job_store.list_pending_webhooks()
        for delivery in deliveries:
            await self._deliver(delivery)
        return len(deliveries)

    async def _run(self) -> None:
        while True:
            try:
                delivered = await self.dispatch_available()
            except asyncio.CancelledError:
                raise
            except Exception:
                delivered = 0
            await asyncio.sleep(0 if delivered else self.poll_interval)

    async def _deliver(self, delivery: PendingWebhookDelivery) -> None:
        body = json.dumps(
            delivery.event.model_dump(mode="json"), separators=(",", ":")
        ).encode("utf-8")
        timestamp = str(int(time.time()))
        headers = {
            "Content-Type": "application/json",
            DELIVERY_HEADER: delivery.event.event_id,
            TIMESTAMP_HEADER: timestamp,
            SIGNATURE_HEADER: webhook_signature(self.secret, timestamp, body),
        }
        try:
            response = await self._client.post(delivery.url, content=body, headers=headers)
        except httpx.RequestError as exc:
            await self._retry_or_abandon(delivery, str(exc))
            return

        if 200 <= response.status_code < 300:
            await self.job_store.record_webhook_attempt(
                delivery.event.event_id,
                state="delivered",
                next_attempt_at=None,
                error=None,
            )
            return

        message = f"HTTP {response.status_code}: {response.text[:300]}"
        if response.status_code in {408, 409, 425, 429} or response.status_code >= 500:
            await self._retry_or_abandon(delivery, message)
            return
        await self.job_store.record_webhook_attempt(
            delivery.event.event_id,
            state="abandoned",
            next_attempt_at=None,
            error=message,
        )

    async def _retry_or_abandon(
        self, delivery: PendingWebhookDelivery, error: str
    ) -> None:
        next_attempt_count = delivery.attempt_count + 1
        if next_attempt_count >= self.max_attempts:
            await self.job_store.record_webhook_attempt(
                delivery.event.event_id,
                state="abandoned",
                next_attempt_at=None,
                error=error[:500],
            )
            return
        delay = min(self.retry_base * (2 ** delivery.attempt_count), self.retry_max)
        await self.job_store.record_webhook_attempt(
            delivery.event.event_id,
            state="pending",
            next_attempt_at=utc_now() + timedelta(seconds=delay),
            error=error[:500],
        )
