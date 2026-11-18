"""Throughput, latency, and scheduling-adherence-under-load tests.

These are wall-clock tests and are marked ``perf``; they are excluded from the
default suite (see ``addopts`` in pyproject) so timing never flakes the
deterministic contract tests. Run them explicitly::

    pytest -m perf

They drive the real in-process pipeline end to end:
``submit -> durable accept -> eager in-process dispatch -> ready queue ->
workers -> mock adapter``. The mock adapter's ``latency_seconds`` is the
controllable per-job service cost, so assertions compare measured behaviour
against the analytic model with generous tolerances rather than absolute,
machine-specific numbers. In-process dispatch is intentionally eager
(``InProcessDispatchBroker.consume``), so every submitted job reaches the ready
queue before a worker selects work — which is what makes end-to-end scheduling
order observable under contention.

Aging/starvation promotion is asserted deterministically at the queue level in
``test_scheduler.py`` (bounds default to 3600s, too long to trigger within a
wall-clock test), so it is not re-tested here.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from doc_processing.config import OCRServiceConfig
from doc_processing.models import PDF, DocumentReference, JobPriority, JobState
from doc_processing.orchestrator import OCRService
from doc_processing.storage import LocalArtifactStorage

pytestmark = pytest.mark.perf


def _config(
    *,
    workers: int = 1,
    max_concurrency: int = 64,
    latency: float = 0.02,
    request_timeout: float = 5.0,
    rpm: int | None = None,
    burst: int | None = None,
) -> OCRServiceConfig:
    scheduler: dict[str, object] = {"workers": workers}
    if rpm is not None:
        scheduler |= {
            "tenant_requests_per_minute": rpm,
            "tenant_burst": burst,
            "tenant_rate_retry_delay_seconds": 0.02,
        }
    return OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "scheduler": scheduler,
            "paths": [
                {
                    "path_id": "external",
                    "revision": 1,
                    "adapter": "mock_external",
                    "routing_priority": 10,
                    "capabilities": {"media_types": [PDF]},
                    "limits": {
                        "max_concurrency": max_concurrency,
                        "request_timeout_seconds": request_timeout,
                        "max_document_bytes": 10_000_000,
                        "max_pages": 1000,
                    },
                    "mock": {"latency_seconds": latency},
                }
            ],
        }
    )


def _doc(pages: int = 1) -> DocumentReference:
    # A synthetic reference (no storage_key) skips storage verification and heavy
    # preflight profiling, isolating scheduler + adapter behaviour.
    return DocumentReference(
        uri="s3://documents/x",
        filename="x.pdf",
        media_type=PDF,
        size_bytes=16,
        page_count=pages,
    )


def _max_run(seq: list[str]) -> int:
    """Longest run of identical consecutive values (fairness sanity check)."""

    best = run = 0
    prev: str | None = None
    for value in seq:
        run = run + 1 if value == prev else 1
        prev = value
        best = max(best, run)
    return best


# -- Throughput --------------------------------------------------------------


@pytest.mark.asyncio
async def test_throughput_scales_with_worker_count(tmp_path) -> None:
    """Draining a fixed backlog is materially faster with more workers, and the
    achieved rate stays within a generous factor of the analytic best case."""

    latency, jobs = 0.03, 24

    async def drain(workers: int) -> float:
        storage = LocalArtifactStorage(tmp_path)
        async with OCRService(_config(workers=workers, latency=latency), storage) as svc:
            start = time.perf_counter()
            submitted = [await svc.submit(_doc(), tenant_id="t") for _ in range(jobs)]
            results = await asyncio.gather(
                *(svc.wait_for_job(job.job_id, timeout=30) for job in submitted)
            )
            elapsed = time.perf_counter() - start
        assert all(result.state is JobState.SUCCEEDED for result in results)
        return elapsed

    single = await drain(1)
    quad = await drain(4)

    print(
        f"\n[perf] throughput: 1w={single:.3f}s  4w={quad:.3f}s  "
        f"speedup={single / quad:.2f}x  (4w≈{jobs / quad:.0f} jobs/s)"
    )
    # Four workers must be well under half the single-worker time.
    assert quad < single * 0.5, f"expected scaling: 1w={single:.3f}s 4w={quad:.3f}s"
    # Ideal drain ≈ ceil(jobs / workers) * latency; allow 4x for loop/dispatch overhead.
    ideal_quad = (jobs / 4) * latency
    assert quad < ideal_quad * 4, f"4-worker drain {quad:.3f}s exceeded budget"


# -- Latency -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_uncontended_latency_near_adapter_cost(tmp_path) -> None:
    """With capacity for every job, end-to-end latency stays close to the
    adapter's own service cost — never below it, and not far above it."""

    latency, jobs = 0.05, 16
    storage = LocalArtifactStorage(tmp_path)
    async with OCRService(
        _config(workers=jobs, max_concurrency=jobs, latency=latency), storage
    ) as svc:

        async def one() -> float:
            start = time.perf_counter()
            job = await svc.process_document(_doc(), tenant_id="t", timeout=30)
            assert job.state is JobState.SUCCEEDED
            return time.perf_counter() - start

        samples = sorted(await asyncio.gather(*(one() for _ in range(jobs))))

    p50 = samples[len(samples) // 2]
    p95 = samples[max(0, round(len(samples) * 0.95) - 1)]
    print(
        f"\n[perf] latency (adapter={latency}s): min={min(samples):.3f}s  "
        f"p50={p50:.3f}s  p95={p95:.3f}s  max={max(samples):.3f}s"
    )
    # Cannot be faster than the adapter's configured cost.
    assert min(samples) >= latency, f"min {min(samples):.3f}s < adapter cost {latency}s"
    # With full capacity, overhead above the adapter cost stays bounded.
    assert p95 <= latency + 0.2, f"p95 {p95:.3f}s exceeded latency budget"


# -- Scheduling adherence: rank ordering -------------------------------------


@pytest.mark.asyncio
async def test_rank_priority_ordering_under_contention(tmp_path) -> None:
    """Under a single-worker backlog, ``rank_v1`` drains strictly by priority:
    a low-priority job submitted amid high-priority work finishes last."""

    storage = LocalArtifactStorage(tmp_path)
    async with OCRService(
        _config(workers=1, latency=0.03), storage, scheduling_algorithm="rank_v1"
    ) as svc:
        completion_order: list[str] = []

        async def watch(job_id: str) -> None:
            await svc.wait_for_job(job_id, timeout=30)
            completion_order.append(job_id)

        # High-priority jobs first; the single low-priority job submitted LAST so
        # it is never the in-flight job the worker grabs before others queue.
        high = [
            await svc.submit(_doc(), tenant_id="t", priority=JobPriority.INTERACTIVE)
            for _ in range(6)
        ]
        low = await svc.submit(_doc(), tenant_id="t", priority=JobPriority.BULK)
        assert low.queue_priority < high[0].queue_priority

        await asyncio.gather(*(watch(job.job_id) for job in [*high, low]))

    trace = "->".join("LOW" if jid == low.job_id else "high" for jid in completion_order)
    print(f"\n[perf] rank order (LOW must be last): {trace}")
    assert completion_order[-1] == low.job_id, completion_order
    assert set(completion_order[:-1]) == {job.job_id for job in high}


# -- Scheduling adherence: weighted-fair tenant sharing ----------------------


@pytest.mark.asyncio
async def test_weighted_fair_interleaves_tenants(tmp_path) -> None:
    """``weighted_fair_v1`` shares a single worker across two equally-weighted
    tenants instead of draining the first-submitted tenant entirely first."""

    storage = LocalArtifactStorage(tmp_path)
    per_tenant = 6
    async with OCRService(
        _config(workers=1, latency=0.02), storage, scheduling_algorithm="weighted_fair_v1"
    ) as svc:
        order: list[str] = []

        async def watch(job_id: str, tenant: str) -> None:
            await svc.wait_for_job(job_id, timeout=30)
            order.append(tenant)

        watchers = []
        # Tenant A is fully submitted before tenant B; fair queuing must still
        # interleave them rather than serving all of A first.
        for tenant in ("A", "B"):
            for _ in range(per_tenant):
                job = await svc.submit(
                    _doc(), tenant_id=tenant, priority=JobPriority.STANDARD
                )
                watchers.append(watch(job.job_id, tenant))
        await asyncio.gather(*watchers)

    print(f"\n[perf] weighted-fair tenant order: {''.join(order)}  (max run={_max_run(order)})")
    assert order.count("A") == per_tenant and order.count("B") == per_tenant
    # B's first completion happens before A's last: B is not starved behind A.
    a_last = len(order) - 1 - order[::-1].index("A")
    assert order.index("B") < a_last, order
    # No long single-tenant runs once both tenants are backlogged.
    assert _max_run(order) <= 3, order


# -- Scheduling adherence: per-tenant rate isolation -------------------------


@pytest.mark.asyncio
async def test_tenant_rate_gate_isolates_a_noisy_tenant(tmp_path) -> None:
    """A tenant flooding past its rate gate is delayed (never dropped) and does
    not block a well-behaved tenant's job from completing promptly."""

    latency = 0.02
    storage = LocalArtifactStorage(tmp_path)
    async with OCRService(
        _config(workers=2, latency=latency, rpm=600, burst=1), storage
    ) as svc:
        # Noisy tenant: only the burst token admits immediately; the rest are
        # requeued and trickle through as the per-tenant bucket refills.
        noisy = [await svc.submit(_doc(), tenant_id="noisy") for _ in range(6)]

        # Well-behaved tenant: a single job under its own independent bucket.
        start = time.perf_counter()
        good = await svc.process_document(_doc(), tenant_id="good", timeout=30)
        good_latency = time.perf_counter() - start

        noisy_results = await asyncio.gather(
            *(svc.wait_for_job(job.job_id, timeout=30) for job in noisy)
        )

    print(
        f"\n[perf] rate isolation: good-tenant latency={good_latency:.3f}s "
        f"(adapter={latency}s); {len(noisy_results)} throttled jobs all succeeded"
    )
    assert good.state is JobState.SUCCEEDED
    # Delayed, never dropped: every throttled job still succeeds.
    assert all(result.state is JobState.SUCCEEDED for result in noisy_results)
    # The good tenant is not stuck behind the throttled backlog.
    assert good_latency <= latency + 0.3, f"good-tenant latency {good_latency:.3f}s"
