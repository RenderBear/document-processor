from __future__ import annotations

from datetime import timedelta

import pytest

from doc_processing.config import OCRServiceConfig
from doc_processing.models import PDF, DocumentReference, Job, JobState, utc_now
from doc_processing.orchestrator import OCRService
from doc_processing.policy import BuiltinPolicyEngine, default_policy
from doc_processing.scheduler import (
    RankQueue,
    WeightedFairQueue,
    build_scheduling_queue,
)
from doc_processing.storage import LocalArtifactStorage


def _job(
    priority: int,
    *,
    job_id: str,
    created_at=None,
    starvation: int = 3600,
    tenant: str = "tenant-1",
) -> Job:
    return Job(
        job_id=job_id,
        tenant_id=tenant,
        document=DocumentReference(
            uri="s3://documents/x",
            filename="x.pdf",
            media_type=PDF,
            size_bytes=16,
            page_count=1,
        ),
        queue_priority=priority,
        starvation_bound_seconds=starvation,
        created_at=created_at or utc_now(),
    )


# -- RankQueue ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_rank_queue_orders_by_priority() -> None:
    queue = RankQueue()
    await queue.put(_job(10, job_id="low"), 0)
    await queue.put(_job(40, job_id="high"), 1)
    await queue.put(_job(20, job_id="mid"), 2)
    assert [await queue.get() for _ in range(3)] == ["high", "mid", "low"]


@pytest.mark.asyncio
async def test_rank_queue_breaks_ties_by_arrival() -> None:
    queue = RankQueue()
    await queue.put(_job(20, job_id="first"), 0)
    await queue.put(_job(20, job_id="second"), 1)
    assert await queue.get() == "first"


@pytest.mark.asyncio
async def test_rank_queue_promotes_starved_low_priority_job() -> None:
    queue = RankQueue()
    starved = _job(
        10, job_id="starved", created_at=utc_now() - timedelta(seconds=30), starvation=1
    )
    fresh = _job(40, job_id="fresh")
    await queue.put(starved, 0)
    await queue.put(fresh, 1)
    assert await queue.get() == "starved"


# -- factory -----------------------------------------------------------------


def test_build_scheduling_queue_selects_algorithm() -> None:
    assert isinstance(build_scheduling_queue("weighted_fair_v1"), WeightedFairQueue)
    assert isinstance(build_scheduling_queue("rank_v1"), RankQueue)
    with pytest.raises(ValueError):
        build_scheduling_queue("nope")


# -- orchestrator wiring -----------------------------------------------------


def _config() -> OCRServiceConfig:
    return OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "paths": [
                {
                    "path_id": "external",
                    "revision": 1,
                    "adapter": "mock_external",
                    "routing_priority": 10,
                    "capabilities": {"media_types": [PDF]},
                    "limits": {
                        "max_concurrency": 1,
                        "request_timeout_seconds": 1.0,
                        "max_document_bytes": 1_000_000,
                        "max_pages": 10,
                    },
                }
            ],
        }
    )


def test_service_selects_queue_from_policy(tmp_path) -> None:
    storage = LocalArtifactStorage(tmp_path)
    default_engine = BuiltinPolicyEngine(default_policy())
    default_service = OCRService(_config(), storage, policy_engine=default_engine)
    assert isinstance(default_service._queue, WeightedFairQueue)

    rank_spec = default_policy().model_copy(update={"scheduling_algorithm": "rank_v1"})
    rank_service = OCRService(_config(), storage, policy_engine=BuiltinPolicyEngine(rank_spec))
    assert isinstance(rank_service._queue, RankQueue)


# -- per-tenant rate limit (reservation gate) -------------------------------


def _rate_limited_config(rpm: int, burst: int) -> OCRServiceConfig:
    data = _config().model_dump()
    data["scheduler"] = {
        "workers": 2,
        "capacity_poll_interval_seconds": 0.005,
        "tenant_requests_per_minute": rpm,
        "tenant_burst": burst,
        "tenant_rate_retry_delay_seconds": 0.02,
    }
    return OCRServiceConfig.model_validate(data)


def test_tenant_rate_config_requires_both_fields() -> None:
    from doc_processing.config import SchedulerConfig

    with pytest.raises(ValueError, match="must be declared together"):
        SchedulerConfig(tenant_requests_per_minute=60)


def test_tenant_allows_consumes_per_tenant_tokens(tmp_path) -> None:
    storage = LocalArtifactStorage(tmp_path)
    service = OCRService(_rate_limited_config(rpm=60, burst=1), storage)
    assert service._tenant_allows("tenant-a") is True   # burst token
    assert service._tenant_allows("tenant-a") is False  # exhausted immediately
    assert service._tenant_allows("tenant-b") is True   # independent per tenant


def _synthetic_document() -> DocumentReference:
    return DocumentReference(
        uri="s3://documents/x", filename="x.pdf", media_type=PDF, size_bytes=16, page_count=1
    )


@pytest.mark.asyncio
async def test_rate_limited_tenant_jobs_are_delayed_not_dropped(tmp_path) -> None:
    storage = LocalArtifactStorage(tmp_path)
    async with OCRService(_rate_limited_config(rpm=6000, burst=1), storage) as service:
        first = await service.submit(_synthetic_document(), tenant_id="tenant-a")
        second = await service.submit(_synthetic_document(), tenant_id="tenant-a")
        results = [
            await service.wait_for_job(first.job_id, timeout=3),
            await service.wait_for_job(second.job_id, timeout=3),
        ]
    # The gate requeues the throttled job; both still complete, never failed.
    assert [r.state for r in results] == [JobState.SUCCEEDED, JobState.SUCCEEDED]
