"""Stable in-process scheduling mechanics driven by admitted policy parameters.

Two interchangeable ready-queue algorithms consume the same admitted per-job
parameters. The active algorithm is selected by policy (`scheduling_algorithm`):

- ``weighted_fair_v1`` — hierarchical weighted fair queuing across service class
  and tenant, using admitted page count as service cost.
- ``rank_v1`` — strict queue-priority order with FIFO tie-breaking.

Both promote a job past its class once it exceeds its starvation bound, so no
job can be indefinitely starved regardless of the selected algorithm.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from .models import Job, utc_now


@dataclass(frozen=True)
class _ReadyItem:
    job: Job
    sequence: int


class SchedulingQueue(Protocol):
    """Rebuildable ready-queue interface shared by all scheduling algorithms."""

    async def put(self, job: Job, sequence: int) -> None: ...

    async def get(self) -> str: ...

    async def task_done(self, job_id: str) -> None: ...


def _select_starved(items: list[_ReadyItem], now: datetime) -> _ReadyItem | None:
    """Return the most-overdue job past its starvation bound, if any.

    This aging safety net is shared by every algorithm: a job whose age has
    exceeded its admitted starvation bound is promoted ahead of ordinary
    ordering, and the most-overdue such job wins.
    """

    starved = [
        item
        for item in items
        if (now - item.job.created_at).total_seconds() >= item.job.starvation_bound_seconds
    ]
    if not starved:
        return None
    return max(
        starved,
        key=lambda item: (
            (now - item.job.created_at).total_seconds() / item.job.starvation_bound_seconds,
            item.job.queue_priority,
            -item.sequence,
        ),
    )


class WeightedFairQueue:
    """Weighted class and tenant fairness using admitted pages as service cost.

    This queue owns rebuildable scheduling state, not policy. Each job carries
    the validated class parameters selected at admission.
    """

    def __init__(self) -> None:
        self._items: dict[str, _ReadyItem] = {}
        self._class_usage: dict[str, float] = {}
        self._tenant_usage: dict[tuple[str, str], float] = {}
        self._charges: dict[str, tuple[str, str, float]] = {}
        self._condition = asyncio.Condition()

    async def put(self, job: Job, sequence: int) -> None:
        async with self._condition:
            charge = self._charges.pop(job.job_id, None)
            if charge is not None:
                service_class, tenant_id, cost = charge
                self._class_usage[service_class] = max(
                    0.0,
                    self._class_usage.get(service_class, 0.0) - cost,
                )
                tenant_key = (service_class, tenant_id)
                self._tenant_usage[tenant_key] = max(
                    0.0,
                    self._tenant_usage.get(tenant_key, 0.0) - cost,
                )
            self._items[job.job_id] = _ReadyItem(job.model_copy(deep=True), sequence)
            self._condition.notify()

    async def get(self) -> str:
        async with self._condition:
            await self._condition.wait_for(lambda: bool(self._items))
            item = self._select()
            del self._items[item.job.job_id]
            cost = float(max(1, item.job.document.page_count))
            service_class = item.job.service_class
            tenant_key = (service_class, item.job.tenant_id)
            self._class_usage[service_class] = self._class_usage.get(service_class, 0.0) + cost
            self._tenant_usage[tenant_key] = self._tenant_usage.get(tenant_key, 0.0) + cost
            self._charges[item.job.job_id] = (
                service_class,
                item.job.tenant_id,
                cost,
            )
            return item.job.job_id

    def _select(self) -> _ReadyItem:
        now = utc_now()
        starved = _select_starved(list(self._items.values()), now)
        if starved is not None:
            return starved

        by_class: dict[str, list[_ReadyItem]] = {}
        for item in self._items.values():
            by_class.setdefault(item.job.service_class, []).append(item)

        class_settings = {
            name: min(items, key=lambda item: item.sequence).job
            for name, items in by_class.items()
        }
        reserved_total = sum(job.reserved_minimum for job in class_settings.values())
        elastic_share = max(0.0, 1.0 - min(1.0, reserved_total))
        borrowing_weight = sum(
            job.share_weight for job in class_settings.values() if job.borrow_unused
        )

        def target_share(job: Job) -> float:
            borrowed = (
                elastic_share * job.share_weight / borrowing_weight
                if job.borrow_unused and borrowing_weight
                else 0.0
            )
            target = job.reserved_minimum + borrowed
            return target if target > 0 else job.share_weight / max(
                1, sum(item.share_weight for item in class_settings.values())
            )

        selected_class = min(
            by_class,
            key=lambda name: (
                self._class_usage.get(name, 0.0) / target_share(class_settings[name]),
                -max(item.job.queue_priority for item in by_class[name]),
                min(item.sequence for item in by_class[name]),
            ),
        )
        class_items = by_class[selected_class]
        by_tenant: dict[str, list[_ReadyItem]] = {}
        for item in class_items:
            by_tenant.setdefault(item.job.tenant_id, []).append(item)
        selected_tenant = min(
            by_tenant,
            key=lambda tenant_id: (
                self._tenant_usage.get((selected_class, tenant_id), 0.0),
                min(item.sequence for item in by_tenant[tenant_id]),
            ),
        )
        return min(
            by_tenant[selected_tenant],
            key=lambda item: (-item.job.queue_priority, item.sequence, item.job.job_id),
        )

    async def task_done(self, job_id: str) -> None:
        """Commit the provisional service charge for a completed queue turn."""

        async with self._condition:
            self._charges.pop(job_id, None)


class RankQueue:
    """Strict queue-priority ordering with FIFO tie-breaking and aging.

    This algorithm ignores class and tenant fair-share: the highest admitted
    ``queue_priority`` runs next, ties broken by arrival order. It keeps only the
    shared starvation safety net, so a low-priority job cannot be starved
    indefinitely by a sustained stream of higher-priority work.
    """

    def __init__(self) -> None:
        self._items: dict[str, _ReadyItem] = {}
        self._condition = asyncio.Condition()

    async def put(self, job: Job, sequence: int) -> None:
        async with self._condition:
            self._items[job.job_id] = _ReadyItem(job.model_copy(deep=True), sequence)
            self._condition.notify()

    async def get(self) -> str:
        async with self._condition:
            await self._condition.wait_for(lambda: bool(self._items))
            item = self._select()
            del self._items[item.job.job_id]
            return item.job.job_id

    def _select(self) -> _ReadyItem:
        now = utc_now()
        starved = _select_starved(list(self._items.values()), now)
        if starved is not None:
            return starved
        return min(
            self._items.values(),
            key=lambda item: (-item.job.queue_priority, item.sequence, item.job.job_id),
        )

    async def task_done(self, job_id: str) -> None:
        # Strict-rank scheduling keeps no per-turn service accounting.
        return None


_SCHEDULING_QUEUES: dict[str, type[SchedulingQueue]] = {
    "weighted_fair_v1": WeightedFairQueue,
    "rank_v1": RankQueue,
}


def build_scheduling_queue(algorithm: str) -> SchedulingQueue:
    """Construct the ready queue for an admitted scheduling algorithm."""

    try:
        return _SCHEDULING_QUEUES[algorithm]()
    except KeyError as exc:
        raise ValueError(f"unsupported scheduling algorithm: {algorithm}") from exc
