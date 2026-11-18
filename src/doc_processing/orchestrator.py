"""Async orchestration kernel with fair scheduling and bounded policy effects."""

from __future__ import annotations

import asyncio
import itertools
import time
from collections import deque
from dataclasses import dataclass, field

from .config import OCRPathConfig, OCRServiceConfig
from .dispatch import DispatchBroker, InProcessDispatchBroker, OutboxPublisher
from .events import InProcessEventBus, JobEvent
from .job_store import ConcurrentJobUpdate, InMemoryJobStore, JobStore
from .models import (
    Attempt,
    AttemptState,
    DataClassification,
    DocumentProfile,
    DocumentReference,
    DispatchCommand,
    document_kind_for,
    Job,
    JobPriority,
    JobState,
    PendingFailure,
    ProviderOutcome,
    ProducerKind,
    StoredArtifact,
    TERMINAL_JOB_STATES,
    utc_now,
)
from .ocr import OCRAdapter, OCRAdapterError, build_mock_adapter
from .preflight import DefaultDocumentPreflight, DocumentProfiler, PreflightRejection
from .policy import (
    AdmissionDecision,
    AdmissionInput,
    BuiltinPolicyEngine,
    FailureDecision,
    FailureDisposition,
    FailureInput,
    PathCandidate,
    PolicyContractError,
    PolicyEngine,
    PolicyEvaluationError,
    RouteDecision,
    RouteInput,
    default_policy,
)
from .scheduler import build_scheduling_queue
from .storage import ArtifactStorage


class _TokenBucket:
    def __init__(self, requests_per_minute: int, burst: int) -> None:
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.refill_per_second = requests_per_minute / 60.0
        self.updated_at = time.monotonic()

    def try_take(self) -> bool:
        now = time.monotonic()
        self.tokens = min(
            self.capacity,
            self.tokens + (now - self.updated_at) * self.refill_per_second,
        )
        self.updated_at = now
        if self.tokens < 1:
            return False
        self.tokens -= 1
        return True


@dataclass
class _PathRuntime:
    config: OCRPathConfig
    adapter: OCRAdapter
    in_flight: int = 0
    bucket: _TokenBucket | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def observe(self, document: DocumentReference) -> PathCandidate:
        try:
            status = await self.adapter.status()
            healthy = status.healthy
            observed_queue_depth = status.observed_queue_depth
        except Exception:
            healthy = False
            observed_queue_depth = None
        async with self.lock:
            in_flight = self.in_flight
        base_duration = (
            self.config.estimates.fixed_seconds
            + self.config.estimates.seconds_per_page * document.page_count
        )
        queued_work = (observed_queue_depth or 0) + in_flight
        expected_duration = base_duration * (1 + queued_work)
        return PathCandidate(
            path_id=self.config.path_id,
            path_revision=self.config.revision,
            routing_priority=self.config.routing_priority,
            quality_score=self.config.estimates.quality_score,
            expected_duration_seconds=expected_duration,
            expected_cost=self.config.estimates.cost_per_page * document.page_count,
            healthy=healthy,
            in_flight=in_flight,
            max_concurrency=self.config.limits.max_concurrency,
            observed_queue_depth=observed_queue_depth,
        )

    async def try_reserve(self, document: DocumentReference) -> bool:
        try:
            status = await self.adapter.status()
        except Exception:
            return False
        if not status.healthy:
            return False
        async with self.lock:
            if self.in_flight >= self.config.limits.max_concurrency:
                return False
            if self.bucket is not None and not self.bucket.try_take():
                return False
            self.in_flight += 1
            return True

    async def release(self) -> None:
        async with self.lock:
            self.in_flight -= 1
            if self.in_flight < 0:  # Defensive invariant, never expected.
                self.in_flight = 0


class OCRService:
    """Policy-driven orchestration core with pluggable durable state.

    Policy may classify, rank safe candidates, and choose bounded failure
    dispositions. Static hard limits, state effects, attempt ceilings, adapter
    health, and capacity remain under orchestration control. Queue and capacity
    accounting are process-local; jobs and lifecycle events can be canonical in
    PostgreSQL through the JobStore contract.
    """

    def __init__(
        self,
        config: OCRServiceConfig,
        storage: ArtifactStorage,
        *,
        event_bus: InProcessEventBus | None = None,
        job_store: JobStore | None = None,
        adapters: dict[str, OCRAdapter] | None = None,
        policy_engine: PolicyEngine | None = None,
        preflight: DocumentProfiler | None = None,
        scheduling_algorithm: str | None = None,
        dispatch_broker: DispatchBroker | None = None,
        publish_dispatch: bool = True,
        consume_dispatch: bool = True,
    ) -> None:
        self.config = config
        self.storage = storage
        self.events = event_bus or InProcessEventBus()
        self.job_store = job_store or InMemoryJobStore()
        self.policy_engine = policy_engine or BuiltinPolicyEngine(default_policy())
        self.preflight = preflight or DefaultDocumentPreflight(config.preflight)
        # The scheduling algorithm is an admitted policy parameter; fall back to
        # the built-in engine's snapshot, then the default, when not supplied.
        if scheduling_algorithm is None:
            spec = getattr(self.policy_engine, "spec", None)
            scheduling_algorithm = getattr(spec, "scheduling_algorithm", "weighted_fair_v1")
        self.scheduling_algorithm = scheduling_algorithm
        self.dispatch_broker = dispatch_broker or InProcessDispatchBroker()
        self.publish_dispatch = publish_dispatch
        self.consume_dispatch = consume_dispatch
        self.outbox_publisher = OutboxPublisher(self.job_store, self.dispatch_broker)
        adapters = adapters or {path.path_id: build_mock_adapter(path) for path in config.paths}
        missing = {path.path_id for path in config.enabled_paths} - adapters.keys()
        if missing:
            raise ValueError(f"missing adapters for: {', '.join(sorted(missing))}")

        self._paths: dict[str, _PathRuntime] = {}
        for path in config.enabled_paths:
            bucket = None
            if path.limits.requests_per_minute is not None:
                assert path.limits.burst is not None
                bucket = _TokenBucket(path.limits.requests_per_minute, path.limits.burst)
            self._paths[path.path_id] = _PathRuntime(path, adapters[path.path_id], bucket=bucket)

        self._completion: dict[str, asyncio.Future[Job]] = {}
        self._queue = build_scheduling_queue(self.scheduling_algorithm)
        self._sequence = itertools.count()
        self._tenant_buckets: dict[str, _TokenBucket] = {}
        self._requeue_tasks: set[asyncio.Task[None]] = set()
        self._workers: list[asyncio.Task[None]] = []
        self._dispatch_consumer: asyncio.Task[None] | None = None
        self._command_locks: dict[str, asyncio.Lock] = {}
        self._state_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._initialized = False
        self._capacity_changed = asyncio.Condition()

    async def __aenter__(self) -> "OCRService":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    async def start(self) -> None:
        await self._initialize()
        if self.publish_dispatch:
            await self.outbox_publisher.start()
            self.outbox_publisher.notify()
        if self.consume_dispatch and not self._workers:
            self._workers = [
                asyncio.create_task(self._worker(), name=f"ocr-worker-{number}")
                for number in range(self.config.scheduler.workers)
            ]
            self._dispatch_consumer = asyncio.create_task(
                self.dispatch_broker.consume(
                    self._accept_dispatch,
                    workers=self.config.scheduler.workers,
                ),
                name="dispatch-consumer",
            )

    async def stop(self) -> None:
        if self._dispatch_consumer is not None:
            self._dispatch_consumer.cancel()
            await asyncio.gather(self._dispatch_consumer, return_exceptions=True)
            self._dispatch_consumer = None
        await self.outbox_publisher.close()
        requeues, self._requeue_tasks = self._requeue_tasks, set()
        for task in requeues:
            task.cancel()
        await asyncio.gather(*requeues, return_exceptions=True)
        workers, self._workers = self._workers, []
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        await self.dispatch_broker.close()
        async with self._lifecycle_lock:
            if self._initialized:
                await self.job_store.close()
                self._initialized = False
                self._completion.clear()
                self._queue = build_scheduling_queue(self.scheduling_algorithm)

    async def _initialize(self) -> None:
        async with self._lifecycle_lock:
            if self._initialized:
                return
            await self.job_store.start()
            loop = asyncio.get_running_loop()
            try:
                recoverable = await self.job_store.list_recoverable_jobs()
                for job in recoverable:
                    self._completion[job.job_id] = loop.create_future()
                    # Accepted and validating jobs are re-dispatched so the
                    # consumer re-runs preflight from the durable source; a job
                    # crashed mid-validation resumes idempotently.
                    if self.consume_dispatch and job.state is JobState.PROCESSING:
                        for index, attempt in enumerate(job.attempts):
                            if attempt.state is AttemptState.RUNNING:
                                await self._end_attempt(
                                    job.job_id,
                                    index,
                                    AttemptState.FAILED,
                                    "process.restarted",
                                    "service stopped before the attempt completed",
                                )
                        await self._transition(
                            job.job_id,
                            JobState.QUEUED,
                            "document.job.retrying",
                            {"error_code": "process.restarted"},
                        )
                    if self.publish_dispatch or self.consume_dispatch:
                        await self.job_store.ensure_dispatch(job.job_id)
            except Exception:
                await self.job_store.close()
                raise
            self._initialized = True

    async def submit(
        self,
        document: DocumentReference,
        *,
        tenant_id: str,
        priority: JobPriority = JobPriority.STANDARD,
        producer_kind: ProducerKind | None = None,
        document_profile: DocumentProfile | None = None,
        data_classification: DataClassification | None = None,
        home_region: str | None = None,
        webhook_url: str | None = None,
    ) -> Job:
        await self._initialize()
        profile = document_profile or DocumentProfile()
        decision = await self._admit(
            AdmissionInput(
                tenant_id=tenant_id,
                producer_kind=producer_kind,
                requested_priority=priority,
                document=document,
                document_profile=profile,
                data_classification=data_classification,
                home_region=home_region,
                observed_at=utc_now(),
            )
        )
        job = Job(
            tenant_id=tenant_id,
            document=document.model_copy(deep=True),
            document_profile=profile.model_copy(deep=True),
            producer_kind=producer_kind,
            data_classification=data_classification,
            home_region=home_region,
            priority=priority,
            service_class=decision.service_class,
            queue_priority=decision.queue_priority,
            share_weight=decision.share_weight,
            reserved_minimum=decision.reserved_minimum,
            borrow_unused=decision.borrow_unused,
            starvation_bound_seconds=decision.starvation_bound_seconds,
            admission_policy=decision.policy,
        )
        accepted_event = JobEvent(
            event_type="document.job.accepted",
            job_id=job.job_id,
            job_version=1,
            tenant_id=job.tenant_id,
            data={
                "state": JobState.ACCEPTED.value,
                "service_class": decision.service_class,
                "policy_id": decision.policy.policy_id,
                "policy_revision": decision.policy.revision,
                "reason_codes": list(decision.reason_codes),
            },
        )
        events = [accepted_event]
        if not decision.admitted:
            job.state = JobState.REJECTED
            job.error_code = decision.error_code or "policy.denied"
            job.error_message = decision.message or "submission denied by policy"
            job.version = 2
            job.updated_at = utc_now()
            events.append(
                JobEvent(
                    event_type="document.job.rejected",
                    job_id=job.job_id,
                    job_version=2,
                    tenant_id=job.tenant_id,
                    data={
                        "state": JobState.REJECTED.value,
                        "error_code": job.error_code,
                        "message": job.error_message,
                    },
                )
            )
            dispatch = None
        else:
            # Acceptance is durable but not yet queued: preflight derives the
            # authoritative page count and document profile before the job enters
            # the scheduler. The claim-check command drives that transition.
            job.state = JobState.ACCEPTED
            job.version = 1
            job.updated_at = utc_now()
            dispatch = DispatchCommand(
                job_id=job.job_id,
                subject=getattr(self.dispatch_broker, "subject", "document.jobs.process"),
            )
        loop = asyncio.get_running_loop()
        async with self._state_lock:
            await self.job_store.create_job(
                job,
                webhook_url=webhook_url,
                events=tuple(events),
                dispatch=dispatch,
            )
            completion = loop.create_future()
            self._completion[job.job_id] = completion
            if job.terminal:
                completion.set_result(job.model_copy(deep=True))
        for event in events:
            await self.events.publish(event)
        if dispatch is not None and self.publish_dispatch:
            self.outbox_publisher.notify()
        return await self.get_job(job.job_id)

    async def _accept_dispatch(self, command: DispatchCommand) -> None:
        lock = self._command_locks.setdefault(command.job_id, asyncio.Lock())
        async with lock:
            job = await self.get_job(command.job_id)
            if job.terminal:
                return
            if job.state in (JobState.ACCEPTED, JobState.VALIDATING):
                await self._preflight_job(job.job_id)
                job = await self.get_job(job.job_id)
                if job.terminal:
                    return
            if job.state is JobState.PROCESSING:
                while not (await self.get_job(job.job_id)).terminal:
                    await asyncio.sleep(0.05)
                return
            await self._queue.put(job, next(self._sequence))
            while not (await self.get_job(job.job_id)).terminal:
                await asyncio.sleep(0.05)

    async def _preflight_job(self, job_id: str) -> None:
        """Derive authoritative page count and profile before scheduling.

        A job with a managed source object is profiled: it moves
        ``accepted -> validating -> queued`` and its client-supplied page count is
        replaced with the parsed truth. A synthetic reference without a stored
        source (library callers, tests) has nothing to validate and moves
        ``accepted -> queued`` directly, trusting the supplied facts. Profiling is
        idempotent across redelivery and restart; rejection or an unreadable
        source terminates the job before any OCR attempt.
        """

        job = await self.get_job(job_id)
        if job.state not in (JobState.ACCEPTED, JobState.VALIDATING):
            return

        if job.document.storage_key is None:
            await self._transition(
                job_id,
                JobState.QUEUED,
                "document.job.queued",
                {"profiler_id": "unprofiled"},
            )
            return

        if job.state is JobState.ACCEPTED:
            await self._transition(
                job_id,
                JobState.VALIDATING,
                "document.job.validating",
            )
            job = await self.get_job(job_id)

        try:
            content = await self.storage.get_bytes(job.document.storage_key)
        except Exception:
            await self._finish_failed(
                job_id,
                "artifact.unavailable",
                "source artifact could not be read for preflight",
            )
            return

        try:
            result = await self.preflight.run(job.document, content)
        except PreflightRejection as exc:
            await self._finish_rejected(job_id, exc.code, exc.message)
            return
        except Exception as exc:  # A profiler defect must not run ungoverned OCR.
            await self._finish_failed(job_id, "preflight.error", str(exc))
            return

        updated_document = job.document.model_copy(
            update={"page_count": result.page_count}
        )
        await self._transition(
            job_id,
            JobState.QUEUED,
            "document.job.queued",
            {
                "profiler_id": result.profile.profiler_id,
                "page_count": result.page_count,
                "text_layer": result.profile.text_layer.value,
            },
            {"document": updated_document, "document_profile": result.profile},
        )

    async def release_session_artifacts(self, tenant_id: str, session_id: str) -> int:
        """Cancel session work and idempotently remove only session-owned artifacts."""

        jobs = await self.job_store.list_session_jobs(tenant_id, session_id)
        for job in jobs:
            while not job.terminal:
                try:
                    await self._transition(
                        job.job_id,
                        JobState.CANCELLED,
                        "document.job.cancelled",
                        {"reason": "artifact_session.closed"},
                        {"error_code": "artifact_session.closed"},
                    )
                except ConcurrentJobUpdate:
                    job = await self.get_job(job.job_id)
                    continue
                await self._resolve(job.job_id)
                job = await self.get_job(job.job_id)
            await self.storage.delete(self._source_key(job))
            if job.result is not None:
                await self.storage.delete(job.result.key)
            job.result = None
            job.artifacts_released_at = utc_now()
            job.updated_at = utc_now()
            await self.job_store.save_job(job)
        return len(jobs)

    @staticmethod
    def _source_key(job: Job) -> str:
        if job.document.storage_key is not None:
            return job.document.storage_key
        uri = job.document.uri
        if "/sources/" in uri:
            return "sources/" + uri.split("/sources/", 1)[1]
        if uri.startswith("file:"):
            marker = "/sources/"
            if marker in uri:
                return "sources/" + uri.split(marker, 1)[1]
        raise ValueError("document URI does not identify a managed source key")

    async def process_document(
        self,
        document: DocumentReference,
        *,
        tenant_id: str,
        priority: JobPriority = JobPriority.STANDARD,
        producer_kind: ProducerKind | None = None,
        document_profile: DocumentProfile | None = None,
        timeout: float | None = None,
    ) -> Job:
        await self.start()
        job = await self.submit(
            document,
            tenant_id=tenant_id,
            priority=priority,
            producer_kind=producer_kind,
            document_profile=document_profile,
        )
        return await self.wait_for_job(job.job_id, timeout=timeout)

    async def get_job(self, job_id: str) -> Job:
        return await self.job_store.get_job(job_id)

    async def get_events(self, job_id: str, *, after_version: int = 0) -> list[JobEvent]:
        return await self.job_store.list_events(job_id, after_version=after_version)

    async def wait_for_job(self, job_id: str, timeout: float | None = None) -> Job:
        job = await self.get_job(job_id)
        if job.terminal:
            return job
        async with self._state_lock:
            future = self._completion.get(job_id)
            if future is None:
                future = asyncio.get_running_loop().create_future()
                self._completion[job_id] = future
        result = await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
        return result.model_copy(deep=True)

    async def _admit(self, decision_input: AdmissionInput) -> AdmissionDecision:
        try:
            async with asyncio.timeout(
                self.config.scheduler.policy_evaluation_timeout_seconds
            ):
                raw = await self.policy_engine.admit(decision_input.model_copy(deep=True))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise PolicyEvaluationError("admission policy evaluation failed") from exc
        try:
            return AdmissionDecision.model_validate(raw).model_copy(deep=True)
        except Exception as exc:
            raise PolicyContractError("invalid admission policy decision") from exc

    async def _route(self, job: Job, candidate_ids: list[str]) -> RouteDecision:
        candidates = tuple(
            await asyncio.gather(
                *(self._paths[path_id].observe(job.document) for path_id in candidate_ids)
            )
        )
        decision_input = RouteInput(
            job_id=job.job_id,
            tenant_id=job.tenant_id,
            service_class=job.service_class,
            admission_policy=job.admission_policy,
            document=job.document,
            document_profile=job.document_profile,
            candidates=candidates,
            observed_at=utc_now(),
        )
        try:
            async with asyncio.timeout(
                self.config.scheduler.policy_evaluation_timeout_seconds
            ):
                raw = await self.policy_engine.route(decision_input.model_copy(deep=True))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise PolicyEvaluationError("route policy evaluation failed") from exc
        try:
            decision = RouteDecision.model_validate(raw).model_copy(deep=True)
        except Exception as exc:
            raise PolicyContractError("invalid route policy decision") from exc

        known = set(candidate_ids)
        ordered = decision.ordered_path_ids
        if len(ordered) != len(set(ordered)):
            raise PolicyContractError("route decision contains duplicate path ids")
        unknown = set(ordered) - known
        if unknown:
            raise PolicyContractError(
                "route decision selected paths outside hard eligibility: "
                + ", ".join(sorted(unknown))
            )
        unknown_exclusions = set(decision.excluded) - known
        if unknown_exclusions:
            raise PolicyContractError(
                "route decision explained unknown paths: "
                + ", ".join(sorted(unknown_exclusions))
            )
        unknown_assessments = set(decision.assessments) - known
        if unknown_assessments:
            raise PolicyContractError(
                "route decision assessed unknown paths: "
                + ", ".join(sorted(unknown_assessments))
            )
        contradictory = set(ordered) & set(decision.excluded)
        if contradictory:
            raise PolicyContractError(
                "route decision both selected and excluded paths: "
                + ", ".join(sorted(contradictory))
            )
        return decision

    async def _failure(
        self,
        decision_input: FailureInput,
        allowed_fallbacks: tuple[str, ...],
    ) -> FailureDecision:
        try:
            async with asyncio.timeout(
                self.config.scheduler.policy_evaluation_timeout_seconds
            ):
                raw = await self.policy_engine.on_failure(
                    decision_input.model_copy(deep=True)
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise PolicyEvaluationError("failure policy evaluation failed") from exc
        try:
            decision = FailureDecision.model_validate(raw).model_copy(deep=True)
        except Exception as exc:
            raise PolicyContractError("invalid failure policy decision") from exc

        next_paths = decision.next_path_ids
        if len(next_paths) != len(set(next_paths)):
            raise PolicyContractError("failure decision contains duplicate path ids")
        if decision.disposition is FailureDisposition.FAIL:
            if next_paths:
                raise PolicyContractError("fail disposition cannot select another path")
        elif decision.disposition is FailureDisposition.RETRY:
            if next_paths != (decision_input.path_id,):
                raise PolicyContractError("retry disposition must select only the current path")
        elif decision.disposition is FailureDisposition.FALLBACK:
            if not next_paths:
                raise PolicyContractError("fallback disposition requires a next path")
            unknown = set(next_paths) - set(allowed_fallbacks)
            if unknown:
                raise PolicyContractError(
                    "failure decision selected paths outside the allowed fallback set: "
                    + ", ".join(sorted(unknown))
                )
        return decision

    async def _worker(self) -> None:
        while True:
            job_id = await self._queue.get()
            try:
                await self._process_job(job_id)
            except asyncio.CancelledError:
                raise
            except PolicyEvaluationError:
                # An accepted job remains durable and queued while a replaceable
                # policy engine is temporarily unavailable.
                await asyncio.sleep(self.config.scheduler.policy_retry_delay_seconds)
                job = await self.get_job(job_id)
                if not job.terminal:
                    await self._queue.put(job, next(self._sequence))
            except PolicyContractError as exc:
                await self._finish_failed(job_id, "policy.invalid_decision", str(exc))
            except ConcurrentJobUpdate:
                # At-least-once delivery may race another processor replica.
                # The successful compare-and-swap owns this job; the command
                # handler observes its durable terminal state before acking.
                pass
            except Exception as exc:  # Keep workers alive and jobs observable.
                await self._finish_failed(job_id, "unknown", str(exc))
            finally:
                await self._queue.task_done(job_id)

    async def _process_job(self, job_id: str) -> None:
        job = await self.get_job(job_id)
        if job.state in (JobState.ACCEPTED, JobState.VALIDATING):
            # Defensive: the dispatch path preflights before enqueue, so a job
            # normally arrives here already queued. If one slips through, run
            # preflight now rather than routing an unvalidated document.
            await self._preflight_job(job_id)
            job = await self.get_job(job_id)
            if job.terminal or job.state is not JobState.QUEUED:
                return
        if (
            job.document.storage_key is not None
            and job.document.checksum_sha256 is not None
            and not await self.storage.verify(
                job.document.storage_key,
                size_bytes=job.document.size_bytes,
                checksum_sha256=job.document.checksum_sha256,
            )
        ):
            await self._finish_failed(
                job_id,
                "artifact.unavailable",
                "source artifact is missing or failed integrity verification",
            )
            return
        if (
            job.pending_failure is None
            and not job.attempts
            and not self._tenant_allows(job.tenant_id)
        ):
            # Tenant rate gate: requeue after a short delay so the worker stays
            # free for other tenants. Rate limiting delays work, never drops it.
            self._schedule_requeue(job)
            return
        restricted_candidates: set[str] | None = None
        if job.pending_failure is not None:
            pending_failure = job.pending_failure
            failure_decision = await self._pending_failure_decision(job)
            await self._clear_pending_failure(job_id)
            if failure_decision.disposition is FailureDisposition.FAIL:
                await self._finish_failed(
                    job_id,
                    pending_failure.error_code,
                    pending_failure.error_message,
                )
                return
            restricted_candidates = set(failure_decision.next_path_ids)
            await self._transition(
                job_id,
                JobState.QUEUED,
                "document.job.retrying",
                {
                    "from_path_id": pending_failure.path_id,
                    "error_code": pending_failure.error_code,
                    "disposition": failure_decision.disposition.value,
                    "policy_id": failure_decision.policy.policy_id,
                    "policy_revision": failure_decision.policy.revision,
                    "reason_codes": list(failure_decision.reason_codes),
                },
            )
            if failure_decision.retry_delay_seconds:
                await asyncio.sleep(failure_decision.retry_delay_seconds)
            job = await self.get_job(job_id)

        candidates = self._eligible_path_ids(
            job.document, job.document_profile, job.data_classification, job.home_region
        )
        if restricted_candidates is not None:
            candidates = [
                path_id for path_id in candidates if path_id in restricted_candidates
            ]
        if not candidates:
            if restricted_candidates is not None:
                await self._finish_failed(
                    job_id, "input.unsupported", "no path accepts document limits"
                )
            elif self._eligible_path_ids(job.document, job.document_profile, None, None):
                # A capable path exists but none is cleared for this job's data
                # classification or residency: a policy denial, not a bad input.
                await self._finish_rejected(
                    job_id,
                    "policy.denied",
                    "no path satisfies the data classification or residency policy",
                )
            else:
                await self._finish_rejected(
                    job_id, "input.unsupported", "no path accepts document limits"
                )
            return

        route_decision = await self._route(job, candidates)
        pending = deque(route_decision.ordered_path_ids)
        if not pending:
            await self._finish_rejected(
                job_id,
                "policy.no_eligible_path",
                "policy excluded every path that accepts the document",
            )
            return
        last_error = ("dependency.unavailable", "no path was available")

        while pending:
            current = await self.get_job(job_id)
            if len(current.attempts) >= self.config.scheduler.max_attempts_per_job:
                last_error = (
                    "attempts.exhausted",
                    "orchestration safety ceiling for attempts was reached",
                )
                break

            path_id = await self._reserve_available_path(
                job.document,
                job.document_profile,
                job.data_classification,
                job.home_region,
                pending,
            )
            if path_id is None:
                break
            runtime = self._paths[path_id]
            provider_outcome = ProviderOutcome.NOT_STARTED
            try:
                await self._transition(
                    job_id,
                    JobState.PROCESSING,
                    "document.job.processing",
                    {
                        "path_id": path_id,
                        "policy_id": route_decision.policy.policy_id,
                        "policy_revision": route_decision.policy.revision,
                    },
                )
                attempt_index = await self._start_attempt(
                    job_id,
                    runtime.config,
                    route_decision,
                )
                attempt_id = (await self.get_job(job_id)).attempts[attempt_index].attempt_id
                try:
                    async with asyncio.timeout(runtime.config.limits.request_timeout_seconds):
                        result = await runtime.adapter.process(
                            job.document,
                            job_id=job_id,
                            attempt_id=attempt_id,
                        )
                except TimeoutError:
                    last_error = ("dependency.timeout", f"path {path_id} timed out")
                    provider_outcome = ProviderOutcome.UNKNOWN
                    await self._end_attempt(
                        job_id,
                        attempt_index,
                        AttemptState.TIMED_OUT,
                        *last_error,
                    )
                except OCRAdapterError as exc:
                    last_error = (exc.code, str(exc))
                    provider_outcome = ProviderOutcome.FAILED
                    await self._end_attempt(job_id, attempt_index, AttemptState.FAILED, *last_error)
                except Exception as exc:
                    last_error = ("unknown", str(exc))
                    provider_outcome = ProviderOutcome.UNKNOWN
                    await self._end_attempt(job_id, attempt_index, AttemptState.FAILED, *last_error)
                else:
                    artifact = await self.storage.put_bytes(
                        f"results/{job_id}/result.md",
                        result.markdown.encode("utf-8"),
                        "text/markdown",
                    )
                    await self._end_attempt(job_id, attempt_index, AttemptState.SUCCEEDED)
                    await self._finish_succeeded(job_id, path_id, artifact)
                    return
            finally:
                await runtime.release()
                async with self._capacity_changed:
                    self._capacity_changed.notify_all()

            remaining_pending = tuple(
                candidate for candidate in pending if candidate != path_id
            )
            configured_fallbacks = runtime.config.fallback_path_ids
            if configured_fallbacks:
                allowed_fallbacks = tuple(
                    candidate
                    for candidate in configured_fallbacks
                    if candidate in route_decision.ordered_path_ids
                    and candidate in self._paths
                )
            else:
                allowed_fallbacks = remaining_pending

            latest = await self.get_job(job_id)
            await self._set_pending_failure(
                job_id,
                PendingFailure(
                    path_id=path_id,
                    error_code=last_error[0],
                    error_message=last_error[1],
                    provider_outcome=provider_outcome,
                    fallback_path_ids=allowed_fallbacks,
                ),
            )
            latest = await self.get_job(job_id)
            failure_decision = await self._pending_failure_decision(latest)
            await self._clear_pending_failure(job_id)
            if failure_decision.disposition is FailureDisposition.FAIL:
                break

            pending = deque(failure_decision.next_path_ids)
            if pending:
                await self._transition(
                    job_id,
                    JobState.QUEUED,
                    "document.job.retrying",
                    {
                        "from_path_id": path_id,
                        "error_code": last_error[0],
                        "disposition": failure_decision.disposition.value,
                        "policy_id": failure_decision.policy.policy_id,
                        "policy_revision": failure_decision.policy.revision,
                        "reason_codes": list(failure_decision.reason_codes),
                    },
                )
                if failure_decision.retry_delay_seconds:
                    await asyncio.sleep(failure_decision.retry_delay_seconds)

        await self._finish_failed(job_id, *last_error)

    async def _pending_failure_decision(self, job: Job) -> FailureDecision:
        pending = job.pending_failure
        if pending is None:
            raise RuntimeError("job does not have a pending failure")
        return await self._failure(
            FailureInput(
                job_id=job.job_id,
                tenant_id=job.tenant_id,
                service_class=job.service_class,
                admission_policy=job.admission_policy,
                attempt_policy=(job.attempts[-1].policy if job.attempts else None),
                path_id=pending.path_id,
                error_code=pending.error_code,
                provider_outcome=pending.provider_outcome,
                attempts_on_path=sum(
                    1 for attempt in job.attempts if attempt.path_id == pending.path_id
                ),
                attempts_on_job=len(job.attempts),
                fallback_path_ids=pending.fallback_path_ids,
                observed_at=utc_now(),
            ),
            pending.fallback_path_ids,
        )

    def _tenant_allows(self, tenant_id: str) -> bool:
        """Consume one per-tenant dispatch token; True if the tenant may proceed.

        A reservation gate, not a rank: an exhausted tenant is made to wait, never
        deprioritized or failed. Unconfigured tenant limits admit everything.
        """

        requests_per_minute = self.config.scheduler.tenant_requests_per_minute
        if requests_per_minute is None:
            return True
        bucket = self._tenant_buckets.get(tenant_id)
        if bucket is None:
            assert self.config.scheduler.tenant_burst is not None
            bucket = _TokenBucket(requests_per_minute, self.config.scheduler.tenant_burst)
            self._tenant_buckets[tenant_id] = bucket
        return bucket.try_take()

    def _schedule_requeue(self, job: Job) -> None:
        task = asyncio.create_task(self._requeue_after_delay(job.job_id))
        self._requeue_tasks.add(task)
        task.add_done_callback(self._requeue_tasks.discard)

    async def _requeue_after_delay(self, job_id: str) -> None:
        try:
            await asyncio.sleep(self.config.scheduler.tenant_rate_retry_delay_seconds)
            job = await self.get_job(job_id)
            if not job.terminal:
                await self._queue.put(job, next(self._sequence))
        except asyncio.CancelledError:
            raise
        except Exception:
            # A failed requeue is recovered by reconciliation/redelivery, not here.
            pass

    def _eligible_path_ids(
        self,
        document: DocumentReference,
        profile: DocumentProfile,
        classification: DataClassification | None,
        region: str | None,
    ) -> list[str]:
        return [
            path.path_id
            for path in self.config.enabled_paths
            if path.path_id in self._paths
            and self._accepts(path, document, profile, classification, region)
        ]

    @staticmethod
    def _accepts(
        path: OCRPathConfig,
        document: DocumentReference,
        profile: DocumentProfile,
        classification: DataClassification | None = None,
        region: str | None = None,
    ) -> bool:
        if (
            document.media_type not in path.capabilities.media_types
            or document.size_bytes > path.limits.max_document_bytes
            or document.page_count > path.limits.max_pages
        ):
            return False
        # Profile-driven eligibility: once preflight knows the document kind, a
        # path must declare support for it. An unknown kind gates nothing.
        kind = document_kind_for(profile.text_layer)
        if kind is not None and kind.value not in path.capabilities.document_kinds:
            return False
        # Compliance gates (hard, never ranked): a classified document may only
        # run on a path cleared for its sensitivity, and a region-bound job may
        # only run on a path serving that region. Unset attributes gate nothing.
        if (
            classification is not None
            and classification.value not in path.compliance.allowed_classifications
        ):
            return False
        if region is not None and path.compliance.regions and region not in path.compliance.regions:
            return False
        return True

    async def _reserve_available_path(
        self,
        document: DocumentReference,
        profile: DocumentProfile,
        classification: DataClassification | None,
        region: str | None,
        candidates: deque[str],
    ) -> str | None:
        candidate_ids = [
            path_id
            for path_id in candidates
            if path_id in self._paths
            and self._accepts(
                self._paths[path_id].config, document, profile, classification, region
            )
        ]
        if not candidate_ids:
            return None

        while True:
            for path_id in candidate_ids:
                if await self._paths[path_id].try_reserve(document):
                    return path_id
            async with self._capacity_changed:
                try:
                    await asyncio.wait_for(
                        self._capacity_changed.wait(),
                        timeout=self.config.scheduler.capacity_poll_interval_seconds,
                    )
                except TimeoutError:
                    pass

    async def _start_attempt(
        self,
        job_id: str,
        path: OCRPathConfig,
        route_decision: RouteDecision,
    ) -> int:
        async with self._state_lock:
            job = await self.job_store.get_job(job_id)
            assessment = route_decision.assessments.get(path.path_id)
            job.attempts.append(
                Attempt(
                    path_id=path.path_id,
                    path_revision=path.revision,
                    policy=route_decision.policy,
                    route_explanation={
                        "reason_codes": list(route_decision.reason_codes),
                        "excluded": route_decision.excluded,
                        "assessment": (
                            assessment.model_dump(mode="json") if assessment is not None else {}
                        ),
                    },
                )
            )
            job.updated_at = utc_now()
            await self.job_store.save_job(job)
            return len(job.attempts) - 1

    async def _end_attempt(
        self,
        job_id: str,
        attempt_index: int,
        state: AttemptState,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        async with self._state_lock:
            job = await self.job_store.get_job(job_id)
            attempt = job.attempts[attempt_index]
            attempt.state = state
            attempt.finished_at = utc_now()
            attempt.error_code = error_code
            attempt.error_message = error_message
            job.updated_at = utc_now()
            await self.job_store.save_job(job)

    async def _set_pending_failure(
        self,
        job_id: str,
        pending_failure: PendingFailure,
    ) -> None:
        async with self._state_lock:
            job = await self.job_store.get_job(job_id)
            job.pending_failure = pending_failure
            job.updated_at = utc_now()
            await self.job_store.save_job(job)

    async def _clear_pending_failure(self, job_id: str) -> None:
        async with self._state_lock:
            job = await self.job_store.get_job(job_id)
            job.pending_failure = None
            job.updated_at = utc_now()
            await self.job_store.save_job(job)

    async def _transition(
        self,
        job_id: str,
        state: JobState,
        event_type: str,
        data: dict[str, object] | None = None,
        updates: dict[str, object] | None = None,
    ) -> None:
        async with self._state_lock:
            job = await self.job_store.get_job(job_id)
            if job.state in TERMINAL_JOB_STATES:
                return
            for field_name, value in (updates or {}).items():
                setattr(job, field_name, value)
            job.state = state
            job.version += 1
            job.updated_at = utc_now()
            event = JobEvent(
                event_type=event_type,
                job_id=job.job_id,
                job_version=job.version,
                tenant_id=job.tenant_id,
                data={"state": state.value, **(data or {})},
            )
            await self.job_store.save_job(job, event)
        await self.events.publish(event)

    async def _finish_succeeded(
        self,
        job_id: str,
        path_id: str,
        artifact: StoredArtifact,
    ) -> None:
        await self._transition(
            job_id,
            JobState.SUCCEEDED,
            "document.job.succeeded",
            {"path_id": path_id, "result_uri": artifact.uri},
            {"result": artifact, "selected_path_id": path_id},
        )
        if (await self.get_job(job_id)).state is not JobState.SUCCEEDED:
            await self.storage.delete(artifact.key)
        await self._resolve(job_id)

    async def _finish_failed(self, job_id: str, code: str, message: str) -> None:
        await self._transition(
            job_id,
            JobState.FAILED,
            "document.job.failed",
            {"error_code": code, "message": message},
            {"error_code": code, "error_message": message},
        )
        await self._resolve(job_id)

    async def _finish_rejected(self, job_id: str, code: str, message: str) -> None:
        await self._transition(
            job_id,
            JobState.REJECTED,
            "document.job.rejected",
            {"error_code": code, "message": message},
            {"error_code": code, "error_message": message},
        )
        await self._resolve(job_id)

    async def _resolve(self, job_id: str) -> None:
        async with self._state_lock:
            job = await self.job_store.get_job(job_id)
            future = self._completion.get(job_id)
            if future is not None and not future.done():
                future.set_result(job)
