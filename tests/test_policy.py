from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from doc_processing.config import OCRServiceConfig
from doc_processing.models import (
    PDF,
    DocumentReference,
    JobPriority,
    JobState,
    PolicyReference,
    ProducerKind,
)
from doc_processing.orchestrator import OCRService
from doc_processing.ocr import AdapterStatus, OCRResult
from doc_processing.policy import (
    AdmissionDecision,
    AdmissionInput,
    BuiltinPolicyEngine,
    FailureDecision,
    FailureDisposition,
    FailureInput,
    PolicyEvaluationError,
    PolicySpec,
    RouteDecision,
    RouteInput,
    default_policy,
    load_policy,
)
from doc_processing.storage import LocalArtifactStorage

ROOT = Path(__file__).parents[1]
NOW = datetime(2026, 9, 3, tzinfo=UTC)
CUSTOM_POLICY = PolicyReference(
    policy_id="customer-engine",
    revision=7,
    digest_sha256="a" * 64,
)


def document(*, pages: int = 2) -> DocumentReference:
    return DocumentReference(
        uri="s3://documents/source.pdf",
        filename="source.pdf",
        media_type=PDF,
        size_bytes=1024,
        page_count=pages,
    )


def two_path_config(
    *,
    max_pages: int = 100,
    external_failure: str | None = None,
    internal_failure: str | None = None,
) -> OCRServiceConfig:
    return OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "scheduler": {
                "workers": 1,
                "capacity_poll_interval_seconds": 0.001,
                "policy_retry_delay_seconds": 0.001,
            },
            "paths": [
                {
                    "path_id": "external",
                    "revision": 1,
                    "adapter": "mock_external",
                    "routing_priority": 100,
                    "fallback_path_ids": ["internal"],
                    "capabilities": {"media_types": [PDF]},
                    "limits": {
                        "max_concurrency": 1,
                        "request_timeout_seconds": 1.0,
                        "max_document_bytes": 1_000_000,
                        "max_pages": max_pages,
                    },
                    "estimates": {
                        "quality_score": 0.95,
                        "fixed_seconds": 1.0,
                        "seconds_per_page": 1.0,
                    },
                    "mock": {
                        "latency_seconds": 0.0,
                        "fail_with": external_failure,
                    },
                },
                {
                    "path_id": "internal",
                    "revision": 1,
                    "adapter": "mock_internal",
                    "routing_priority": 50,
                    "capabilities": {"media_types": [PDF]},
                    "limits": {
                        "max_concurrency": 1,
                        "request_timeout_seconds": 1.0,
                        "max_document_bytes": 1_000_000,
                        "max_pages": max_pages,
                    },
                    "estimates": {
                        "quality_score": 0.80,
                        "fixed_seconds": 0.01,
                        "seconds_per_page": 0.01,
                    },
                    "mock": {
                        "latency_seconds": 0.0,
                        "fail_with": internal_failure,
                    },
                },
            ],
        }
    )


def admission_input(producer: ProducerKind) -> AdmissionInput:
    return AdmissionInput(
        tenant_id="tenant-1",
        producer_kind=producer,
        requested_priority=JobPriority.BULK,
        document=document(),
        observed_at=NOW,
    )


def test_policy_wire_contract_is_explicitly_versioned() -> None:
    decision_input = admission_input(ProducerKind.AGENT)

    assert decision_input.model_dump(mode="json")["contract_version"] == 1
    raw = decision_input.model_dump(mode="json")
    raw["contract_version"] = 2
    with pytest.raises(ValidationError):
        AdmissionInput.model_validate(raw)


def test_repository_policy_is_strict_versioned_and_content_addressed() -> None:
    first = load_policy(ROOT / "configs" / "policy.yaml")
    second = load_policy(ROOT / "configs" / "policy.yaml")

    assert first.schema_version == 1
    assert first.reference == second.reference
    assert first.reference.policy_id == "default-ocr"
    assert len(first.reference.digest_sha256) == 64

    raw = first.model_dump(mode="json")
    raw["unknown"] = True
    with pytest.raises(ValidationError):
        PolicySpec.model_validate(raw)

    raw = first.model_dump(mode="json")
    raw["service_classes"]["knowledge"]["borrow_unused"] = False
    with pytest.raises(ValidationError):
        PolicySpec.model_validate(raw)


@pytest.mark.asyncio
async def test_producer_policy_places_knowledge_above_chat_and_rag() -> None:
    engine = BuiltinPolicyEngine(load_policy(ROOT / "configs" / "policy.yaml"))

    knowledge = await engine.admit(admission_input(ProducerKind.AGENT))
    chat = await engine.admit(admission_input(ProducerKind.CHAT))
    rag = await engine.admit(admission_input(ProducerKind.RAG_INGESTION))

    assert knowledge.service_class == "knowledge"
    assert knowledge.queue_priority > chat.queue_priority > rag.queue_priority


@pytest.mark.asyncio
async def test_route_profiles_choose_quality_for_knowledge_and_speed_for_chat() -> None:
    engine = BuiltinPolicyEngine(default_policy())
    config = two_path_config()
    candidates = []
    for path in config.enabled_paths:
        duration = path.estimates.fixed_seconds + path.estimates.seconds_per_page * 2
        candidates.append(
            {
                "path_id": path.path_id,
                "path_revision": path.revision,
                "routing_priority": path.routing_priority,
                "quality_score": path.estimates.quality_score,
                "expected_duration_seconds": duration,
                "expected_cost": 0.0,
                "healthy": True,
                "in_flight": 0,
                "max_concurrency": 1,
            }
        )

    knowledge = await engine.route(
        RouteInput.model_validate(
            {
                "job_id": "job-1",
                "tenant_id": "tenant-1",
                "service_class": "knowledge",
                "document": document(),
                "candidates": candidates,
                "observed_at": NOW,
            }
        )
    )
    chat = await engine.route(
        RouteInput.model_validate(
            {
                "job_id": "job-2",
                "tenant_id": "tenant-1",
                "service_class": "chat",
                "document": document(),
                "candidates": candidates,
                "observed_at": NOW,
            }
        )
    )

    assert knowledge.ordered_path_ids[0] == "external"
    assert chat.ordered_path_ids[0] == "internal"


class CustomerPolicyEngine:
    async def admit(self, decision_input: AdmissionInput) -> AdmissionDecision:
        return AdmissionDecision(
            service_class="customer-priority",
            queue_priority=777,
            policy=CUSTOM_POLICY,
            reason_codes=("customer.classified",),
        )

    async def route(self, decision_input: RouteInput) -> RouteDecision:
        return RouteDecision(
            ordered_path_ids=tuple(
                candidate.path_id for candidate in reversed(decision_input.candidates)
            ),
            policy=CUSTOM_POLICY,
            reason_codes=("customer.route",),
        )

    async def on_failure(self, decision_input: FailureInput) -> FailureDecision:
        return FailureDecision(
            disposition=FailureDisposition.FAIL,
            policy=CUSTOM_POLICY,
            reason_codes=("customer.fail",),
        )


@pytest.mark.asyncio
async def test_customer_policy_engine_replaces_decisions_not_orchestration(tmp_path: Path) -> None:
    async with OCRService(
        two_path_config(),
        LocalArtifactStorage(tmp_path),
        policy_engine=CustomerPolicyEngine(),
    ) as service:
        submitted = await service.submit(
            document(),
            tenant_id="tenant-1",
            producer_kind=ProducerKind.AGENT,
        )
        result = await service.wait_for_job(submitted.job_id, timeout=1)

    assert result.state is JobState.SUCCEEDED
    assert result.service_class == "customer-priority"
    assert result.queue_priority == 777
    assert result.selected_path_id == "internal"
    assert result.admission_policy == CUSTOM_POLICY
    assert result.attempts[0].policy == CUSTOM_POLICY
    assert result.attempts[0].route_explanation["reason_codes"] == ["customer.route"]


class UnsafeCustomerPolicy(CustomerPolicyEngine):
    async def route(self, decision_input: RouteInput) -> RouteDecision:
        return RouteDecision(
            ordered_path_ids=("path-outside-hard-eligibility",),
            policy=CUSTOM_POLICY,
        )


@pytest.mark.asyncio
async def test_orchestrator_rejects_policy_that_bypasses_hard_eligibility(tmp_path: Path) -> None:
    async with OCRService(
        two_path_config(),
        LocalArtifactStorage(tmp_path),
        policy_engine=UnsafeCustomerPolicy(),
    ) as service:
        submitted = await service.submit(document(), tenant_id="tenant-1")
        result = await service.wait_for_job(submitted.job_id, timeout=1)

    assert result.state is JobState.FAILED
    assert result.error_code == "policy.invalid_decision"
    assert result.attempts == []


class UnavailableRoutePolicy(CustomerPolicyEngine):
    async def route(self, decision_input: RouteInput) -> RouteDecision:
        raise RuntimeError("customer policy service is down")


@pytest.mark.asyncio
async def test_policy_outage_keeps_an_accepted_job_queued(tmp_path: Path) -> None:
    service = OCRService(
        two_path_config(),
        LocalArtifactStorage(tmp_path),
        policy_engine=UnavailableRoutePolicy(),
    )
    submitted = await service.submit(document(), tenant_id="tenant-1")
    await service.start()
    await asyncio.sleep(0.02)
    queued = await service.get_job(submitted.job_id)
    await service.stop()

    assert queued.state is JobState.QUEUED
    assert queued.attempts == []


class SlowAdmissionPolicy(CustomerPolicyEngine):
    async def admit(self, decision_input: AdmissionInput) -> AdmissionDecision:
        await asyncio.sleep(1)
        return await super().admit(decision_input)


@pytest.mark.asyncio
async def test_policy_evaluation_has_an_orchestration_timeout(tmp_path: Path) -> None:
    raw = two_path_config().model_dump(mode="json")
    raw["scheduler"]["policy_evaluation_timeout_seconds"] = 0.001
    service = OCRService(
        OCRServiceConfig.model_validate(raw),
        LocalArtifactStorage(tmp_path),
        policy_engine=SlowAdmissionPolicy(),
    )

    with pytest.raises(PolicyEvaluationError, match="admission"):
        await service.submit(document(), tenant_id="tenant-1")

    await service.stop()


@pytest.mark.asyncio
async def test_builtin_failure_decision_records_unknown_provider_outcome() -> None:
    engine = BuiltinPolicyEngine(default_policy())
    decision = await engine.on_failure(
        FailureInput(
            job_id="job-1",
            tenant_id="tenant-1",
            service_class="knowledge",
            path_id="external",
            error_code="dependency.timeout",
            provider_outcome="unknown",
            attempts_on_path=1,
            attempts_on_job=1,
            fallback_path_ids=("internal",),
            observed_at=NOW,
        )
    )

    assert decision.disposition is FailureDisposition.FALLBACK
    assert decision.next_path_ids == ("internal",)
    assert "provider_outcome.unknown" in decision.reason_codes


class RecordingAdapter:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def status(self) -> AdapterStatus:
        return AdapterStatus()

    async def process(
        self, source: DocumentReference, *, job_id: str, attempt_id: str
    ) -> OCRResult:
        self.order.append(source.filename)
        return OCRResult(
            markdown=f"# {source.filename}",
            path_id="internal",
            engine="recording",
            source_uri=source.uri,
        )


@pytest.mark.asyncio
async def test_weighted_fair_scheduler_enforces_class_shares(tmp_path: Path) -> None:
    config = OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "scheduler": {"workers": 1, "capacity_poll_interval_seconds": 0.001},
            "paths": [
                {
                    "path_id": "internal",
                    "revision": 1,
                    "adapter": "mock_internal",
                    "capabilities": {"media_types": [PDF]},
                    "limits": {
                        "max_concurrency": 1,
                        "request_timeout_seconds": 1.0,
                        "max_document_bytes": 1_000_000,
                        "max_pages": 100,
                    },
                }
            ],
        }
    )
    order: list[str] = []
    service = OCRService(
        config,
        LocalArtifactStorage(tmp_path),
        adapters={"internal": RecordingAdapter(order)},
        policy_engine=BuiltinPolicyEngine(load_policy(ROOT / "configs" / "policy.yaml")),
    )
    submitted = []
    for index in range(10):
        submitted.append(
            await service.submit(
                document().model_copy(update={"filename": f"rag-{index}.pdf"}),
                tenant_id="tenant-1",
                producer_kind=ProducerKind.RAG_INGESTION,
            )
        )
    for index in range(10):
        submitted.append(
            await service.submit(
                document().model_copy(update={"filename": f"chat-{index}.pdf"}),
                tenant_id="tenant-1",
                producer_kind=ProducerKind.CHAT,
            )
        )
    for index in range(10):
        submitted.append(
            await service.submit(
                document().model_copy(update={"filename": f"knowledge-{index}.pdf"}),
                tenant_id="tenant-1",
                producer_kind=ProducerKind.AGENT,
            )
        )

    await service.start()
    await asyncio.gather(
        *(service.wait_for_job(job.job_id, timeout=2) for job in submitted)
    )
    await service.stop()

    first_ten = [name.split("-", 1)[0] for name in order[:10]]
    assert first_ten.count("knowledge") == 6
    assert first_ten.count("chat") == 3
    assert first_ten.count("rag") == 1


class RecoveringFailurePolicy:
    def __init__(self) -> None:
        self.builtin = BuiltinPolicyEngine(default_policy())
        self.failure_calls = 0

    async def admit(self, decision_input: AdmissionInput) -> AdmissionDecision:
        return await self.builtin.admit(decision_input)

    async def route(self, decision_input: RouteInput) -> RouteDecision:
        return await self.builtin.route(decision_input)

    async def on_failure(self, decision_input: FailureInput) -> FailureDecision:
        self.failure_calls += 1
        if self.failure_calls == 1:
            raise RuntimeError("temporary policy outage")
        return await self.builtin.on_failure(decision_input)


@pytest.mark.asyncio
async def test_failure_input_is_durable_across_policy_outage(tmp_path: Path) -> None:
    policy = RecoveringFailurePolicy()
    async with OCRService(
        two_path_config(external_failure="dependency.unavailable"),
        LocalArtifactStorage(tmp_path),
        policy_engine=policy,
    ) as service:
        submitted = await service.submit(
            document(),
            tenant_id="tenant-1",
            producer_kind=ProducerKind.AGENT,
        )
        result = await service.wait_for_job(submitted.job_id, timeout=2)

    assert result.state is JobState.SUCCEEDED
    assert [attempt.path_id for attempt in result.attempts] == ["external", "internal"]
    assert result.pending_failure is None
    assert policy.failure_calls == 2


@pytest.mark.asyncio
async def test_fallback_does_not_cycle_back_to_an_attempted_path(tmp_path: Path) -> None:
    async with OCRService(
        two_path_config(
            external_failure="dependency.unavailable",
            internal_failure="engine.crash",
        ),
        LocalArtifactStorage(tmp_path),
    ) as service:
        submitted = await service.submit(
            document(),
            tenant_id="tenant-1",
            producer_kind=ProducerKind.AGENT,
        )
        result = await service.wait_for_job(submitted.job_id, timeout=1)

    assert result.state is JobState.FAILED
    assert [attempt.path_id for attempt in result.attempts] == ["external", "internal"]


class RetryForeverPolicy(CustomerPolicyEngine):
    async def route(self, decision_input: RouteInput) -> RouteDecision:
        return RouteDecision(
            ordered_path_ids=("internal",),
            policy=CUSTOM_POLICY,
        )

    async def on_failure(self, decision_input: FailureInput) -> FailureDecision:
        return FailureDecision(
            disposition=FailureDisposition.RETRY,
            policy=CUSTOM_POLICY,
            next_path_ids=(decision_input.path_id,),
        )


@pytest.mark.asyncio
async def test_custom_policy_cannot_raise_attempt_safety_ceiling(tmp_path: Path) -> None:
    raw = two_path_config(internal_failure="engine.crash").model_dump(mode="json")
    raw["scheduler"]["max_attempts_per_job"] = 2
    async with OCRService(
        OCRServiceConfig.model_validate(raw),
        LocalArtifactStorage(tmp_path),
        policy_engine=RetryForeverPolicy(),
    ) as service:
        submitted = await service.submit(document(), tenant_id="tenant-1")
        result = await service.wait_for_job(submitted.job_id, timeout=1)

    assert result.state is JobState.FAILED
    assert result.error_code == "attempts.exhausted"
    assert len(result.attempts) == 2
