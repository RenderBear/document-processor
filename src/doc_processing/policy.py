"""Stable policy decision contracts and the built-in declarative evaluator.

Policy engines choose among actions that the orchestration kernel already knows
how to execute safely. They never mutate jobs, reserve capacity, invoke OCR, or
publish lifecycle events.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated, Literal, Protocol

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import (
    REGION_PATTERN,
    DataClassification,
    DocumentProfile,
    DocumentReference,
    JobPriority,
    PolicyReference,
    ProducerKind,
    ProviderOutcome,
)

PositiveInt = Annotated[int, Field(strict=True, gt=0)]
NonNegativeFloat = Annotated[float, Field(strict=True, ge=0)]
ServiceClassName = Annotated[str, Field(pattern=r"^[a-z][a-z0-9-]{1,62}$")]
PathId = Annotated[str, Field(pattern=r"^[a-z][a-z0-9-]{1,62}$")]
ReasonCode = Annotated[
    str,
    Field(min_length=1, max_length=128, pattern=r"^[a-z0-9][a-z0-9._-]*$"),
]
ReasonCodes = Annotated[tuple[ReasonCode, ...], Field(max_length=32)]


class PolicyContractError(ValueError):
    """A policy engine returned a decision outside the stable contract."""


class PolicyEvaluationError(RuntimeError):
    """A policy engine could not produce a decision."""


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class _PolicyMessage(_PolicyModel):
    contract_version: Literal[1] = 1


class RouteObjective(str, Enum):
    AVAILABILITY = "availability"
    QUALITY = "quality"
    EXPECTED_DURATION = "expected_duration"
    EXPECTED_COST = "expected_cost"
    SATURATION = "saturation"
    ROUTING_PRIORITY = "routing_priority"


class FailureDisposition(str, Enum):
    RETRY = "retry"
    FALLBACK = "fallback"
    FAIL = "fail"


class RequestedPriorityClasses(_PolicyModel):
    bulk: ServiceClassName = "bulk"
    standard: ServiceClassName = "standard"
    workflow: ServiceClassName = "workflow"
    interactive: ServiceClassName = "interactive"


class ServiceClassPolicy(_PolicyModel):
    queue_priority: Annotated[int, Field(strict=True, ge=0, le=1_000_000)]
    share_weight: PositiveInt = 1
    reserved_minimum: Annotated[float, Field(strict=True, ge=0, le=1)] = 0.0
    borrow_unused: Literal[True] = True
    starvation_bound_seconds: PositiveInt = 3600
    routing_profile: ServiceClassName


class RoutingProfile(_PolicyModel):
    minimum_quality_score: Annotated[float, Field(strict=True, ge=0, le=1)] = 0.0
    objectives: tuple[RouteObjective, ...] = (
        RouteObjective.AVAILABILITY,
        RouteObjective.QUALITY,
        RouteObjective.EXPECTED_DURATION,
        RouteObjective.ROUTING_PRIORITY,
    )

    @model_validator(mode="after")
    def validate_objectives(self) -> "RoutingProfile":
        if not self.objectives:
            raise ValueError("routing profiles require at least one objective")
        if len(self.objectives) != len(set(self.objectives)):
            raise ValueError("routing objectives must not contain duplicates")
        return self


class FailureRule(_PolicyModel):
    disposition: FailureDisposition
    max_attempts_per_path: PositiveInt = 1
    max_attempts_per_job: PositiveInt = 3
    retry_delay_seconds: NonNegativeFloat = 0.0


class PolicySpec(_PolicyModel):
    schema_version: Literal[1]
    policy_id: Annotated[str, Field(pattern=r"^[a-z][a-z0-9-]{1,62}$")]
    revision: PositiveInt
    scheduling_algorithm: Literal["weighted_fair_v1", "rank_v1"] = "weighted_fair_v1"
    scheduling_cost: Literal["pages"] = "pages"
    producer_defaults: dict[ProducerKind, ServiceClassName]
    requested_priority_defaults: RequestedPriorityClasses = Field(
        default_factory=RequestedPriorityClasses
    )
    service_classes: dict[ServiceClassName, ServiceClassPolicy]
    routing_profiles: dict[ServiceClassName, RoutingProfile]
    failure_rules: dict[ReasonCode, FailureRule] = Field(default_factory=dict)
    default_failure_rule: FailureRule = Field(
        default_factory=lambda: FailureRule(disposition=FailureDisposition.FAIL)
    )

    @model_validator(mode="after")
    def validate_references(self) -> "PolicySpec":
        required_producers = set(ProducerKind)
        missing_producers = required_producers - self.producer_defaults.keys()
        if missing_producers:
            missing = ", ".join(sorted(item.value for item in missing_producers))
            raise ValueError(f"producer_defaults is missing: {missing}")

        requested_classes = set(self.requested_priority_defaults.model_dump().values())
        referenced_classes = set(self.producer_defaults.values()) | requested_classes
        missing_classes = referenced_classes - self.service_classes.keys()
        if missing_classes:
            raise ValueError(
                "policy references unknown service classes: "
                + ", ".join(sorted(missing_classes))
            )

        missing_profiles = {
            item.routing_profile for item in self.service_classes.values()
        } - self.routing_profiles.keys()
        if missing_profiles:
            raise ValueError(
                "service classes reference unknown routing profiles: "
                + ", ".join(sorted(missing_profiles))
            )

        reserved_total = sum(item.reserved_minimum for item in self.service_classes.values())
        if reserved_total > 1.0 + 1e-9:
            raise ValueError("service class reserved_minimum values must sum to at most 1")
        return self

    @property
    def reference(self) -> PolicyReference:
        canonical = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return PolicyReference(
            policy_id=self.policy_id,
            revision=self.revision,
            digest_sha256=hashlib.sha256(canonical).hexdigest(),
        )


class AdmissionInput(_PolicyMessage):
    tenant_id: str = Field(min_length=1)
    producer_kind: ProducerKind | None = None
    requested_priority: JobPriority
    document: DocumentReference
    document_profile: DocumentProfile = Field(default_factory=DocumentProfile)
    data_classification: DataClassification | None = None
    home_region: Annotated[str, Field(pattern=REGION_PATTERN)] | None = None
    observed_at: datetime


class AdmissionDecision(_PolicyMessage):
    admitted: bool = True
    service_class: ServiceClassName
    queue_priority: Annotated[int, Field(strict=True, ge=0, le=1_000_000)]
    share_weight: PositiveInt = 1
    reserved_minimum: Annotated[float, Field(strict=True, ge=0, le=1)] = 0.0
    borrow_unused: Literal[True] = True
    starvation_bound_seconds: PositiveInt = 3600
    policy: PolicyReference
    reason_codes: ReasonCodes = ()
    error_code: ReasonCode | None = None
    message: Annotated[str, Field(max_length=1024)] | None = None

    @model_validator(mode="after")
    def validate_rejection(self) -> "AdmissionDecision":
        if not self.admitted and not self.error_code:
            raise ValueError("a rejected admission decision requires error_code")
        return self


class PathCandidate(_PolicyModel):
    path_id: PathId
    path_revision: PositiveInt
    routing_priority: Annotated[int, Field(strict=True, ge=0)]
    quality_score: Annotated[float, Field(strict=True, ge=0, le=1)]
    expected_duration_seconds: NonNegativeFloat
    expected_cost: NonNegativeFloat
    healthy: bool
    in_flight: Annotated[int, Field(strict=True, ge=0)]
    max_concurrency: PositiveInt
    observed_queue_depth: Annotated[int, Field(strict=True, ge=0)] | None = None

    @property
    def saturation(self) -> float:
        return self.in_flight / self.max_concurrency


class RouteInput(_PolicyMessage):
    job_id: str
    tenant_id: str
    service_class: ServiceClassName
    admission_policy: PolicyReference | None = None
    document: DocumentReference
    document_profile: DocumentProfile = Field(default_factory=DocumentProfile)
    candidates: tuple[PathCandidate, ...]
    observed_at: datetime


class CandidateAssessment(_PolicyModel):
    quality_score: Annotated[float, Field(strict=True, ge=0, le=1)]
    expected_duration_seconds: NonNegativeFloat
    expected_cost: NonNegativeFloat
    saturation: NonNegativeFloat
    healthy: bool


class RouteDecision(_PolicyMessage):
    ordered_path_ids: Annotated[tuple[PathId, ...], Field(max_length=256)]
    policy: PolicyReference
    excluded: dict[PathId, ReasonCode] = Field(default_factory=dict)
    assessments: dict[PathId, CandidateAssessment] = Field(default_factory=dict)
    reason_codes: ReasonCodes = ()


class FailureInput(_PolicyMessage):
    job_id: str
    tenant_id: str
    service_class: ServiceClassName
    admission_policy: PolicyReference | None = None
    attempt_policy: PolicyReference | None = None
    path_id: PathId
    error_code: ReasonCode
    provider_outcome: ProviderOutcome
    attempts_on_path: PositiveInt
    attempts_on_job: PositiveInt
    fallback_path_ids: Annotated[tuple[PathId, ...], Field(max_length=256)]
    observed_at: datetime


class FailureDecision(_PolicyMessage):
    disposition: FailureDisposition
    policy: PolicyReference
    next_path_ids: Annotated[tuple[PathId, ...], Field(max_length=256)] = ()
    retry_delay_seconds: NonNegativeFloat = 0.0
    reason_codes: ReasonCodes = ()


class PolicyEngine(Protocol):
    """Replaceable decision port. Implementations may be local or remote."""

    async def admit(self, decision_input: AdmissionInput) -> AdmissionDecision: ...

    async def route(self, decision_input: RouteInput) -> RouteDecision: ...

    async def on_failure(self, decision_input: FailureInput) -> FailureDecision: ...


def _priority_name(priority: JobPriority) -> str:
    return priority.name.lower()


class BuiltinPolicyEngine:
    """Deterministic evaluator for the bounded YAML policy vocabulary."""

    def __init__(self, spec: PolicySpec) -> None:
        self.spec = spec

    async def admit(self, decision_input: AdmissionInput) -> AdmissionDecision:
        if decision_input.producer_kind is None:
            service_class = getattr(
                self.spec.requested_priority_defaults,
                _priority_name(decision_input.requested_priority),
            )
            reason = "legacy.requested_priority"
        else:
            service_class = self.spec.producer_defaults[decision_input.producer_kind]
            reason = f"producer.{decision_input.producer_kind.value}"
        class_policy = self.spec.service_classes[service_class]
        return AdmissionDecision(
            service_class=service_class,
            queue_priority=class_policy.queue_priority,
            share_weight=class_policy.share_weight,
            reserved_minimum=class_policy.reserved_minimum,
            borrow_unused=class_policy.borrow_unused,
            starvation_bound_seconds=class_policy.starvation_bound_seconds,
            policy=self.spec.reference,
            reason_codes=(reason,),
        )

    async def route(self, decision_input: RouteInput) -> RouteDecision:
        try:
            class_policy = self.spec.service_classes[decision_input.service_class]
        except KeyError as exc:
            raise PolicyEvaluationError(
                f"unknown service class: {decision_input.service_class}"
            ) from exc
        profile = self.spec.routing_profiles[class_policy.routing_profile]
        eligible = [
            candidate
            for candidate in decision_input.candidates
            if candidate.quality_score >= profile.minimum_quality_score
        ]
        excluded = {
            candidate.path_id: "quality_below_floor"
            for candidate in decision_input.candidates
            if candidate.quality_score < profile.minimum_quality_score
        }

        def objective_value(candidate: PathCandidate, objective: RouteObjective) -> float:
            if objective is RouteObjective.AVAILABILITY:
                return 0.0 if candidate.healthy else 1.0
            if objective is RouteObjective.QUALITY:
                return -candidate.quality_score
            if objective is RouteObjective.EXPECTED_DURATION:
                return candidate.expected_duration_seconds
            if objective is RouteObjective.EXPECTED_COST:
                return candidate.expected_cost
            if objective is RouteObjective.SATURATION:
                return candidate.saturation
            if objective is RouteObjective.ROUTING_PRIORITY:
                return -float(candidate.routing_priority)
            raise AssertionError(f"unsupported route objective: {objective}")

        ordered = sorted(
            eligible,
            key=lambda candidate: (
                *(objective_value(candidate, objective) for objective in profile.objectives),
                candidate.path_id,
            ),
        )
        assessments = {
            candidate.path_id: CandidateAssessment(
                quality_score=candidate.quality_score,
                expected_duration_seconds=candidate.expected_duration_seconds,
                expected_cost=candidate.expected_cost,
                saturation=candidate.saturation,
                healthy=candidate.healthy,
            )
            for candidate in decision_input.candidates
        }
        return RouteDecision(
            ordered_path_ids=tuple(candidate.path_id for candidate in ordered),
            policy=self.spec.reference,
            excluded=excluded,
            assessments=assessments,
            reason_codes=(f"routing_profile.{class_policy.routing_profile}",),
        )

    async def on_failure(self, decision_input: FailureInput) -> FailureDecision:
        rule = self.spec.failure_rules.get(
            decision_input.error_code,
            self.spec.default_failure_rule,
        )
        disposition = rule.disposition
        reasons = [f"failure_rule.{decision_input.error_code}"]

        if decision_input.attempts_on_job >= rule.max_attempts_per_job:
            disposition = FailureDisposition.FAIL
            reasons.append("attempt_budget.job_exhausted")
        elif (
            disposition is FailureDisposition.RETRY
            and decision_input.attempts_on_path >= rule.max_attempts_per_path
        ):
            disposition = (
                FailureDisposition.FALLBACK
                if decision_input.fallback_path_ids
                else FailureDisposition.FAIL
            )
            reasons.append("attempt_budget.path_exhausted")
        elif disposition is FailureDisposition.FALLBACK and not decision_input.fallback_path_ids:
            disposition = FailureDisposition.FAIL
            reasons.append("fallback.unavailable")

        if decision_input.provider_outcome is ProviderOutcome.UNKNOWN:
            reasons.append("provider_outcome.unknown")

        if disposition is FailureDisposition.RETRY:
            next_paths = (decision_input.path_id,)
        elif disposition is FailureDisposition.FALLBACK:
            next_paths = decision_input.fallback_path_ids
        else:
            next_paths = ()

        return FailureDecision(
            disposition=disposition,
            policy=self.spec.reference,
            next_path_ids=next_paths,
            retry_delay_seconds=(
                rule.retry_delay_seconds
                if disposition is FailureDisposition.RETRY
                else 0.0
            ),
            reason_codes=tuple(reasons),
        )


def default_policy() -> PolicySpec:
    """Return the compatibility policy used by direct library consumers."""

    return PolicySpec.model_validate(
        {
            "schema_version": 1,
            "policy_id": "builtin-default",
            "revision": 1,
            "scheduling_algorithm": "weighted_fair_v1",
            "scheduling_cost": "pages",
            "producer_defaults": {
                "agent": "knowledge",
                "chat": "chat",
                "rag_ingestion": "rag",
                "workflow": "workflow",
            },
            "service_classes": {
                "knowledge": {"queue_priority": 50, "routing_profile": "quality-first"},
                "interactive": {"queue_priority": 40, "routing_profile": "latency-first"},
                "chat": {"queue_priority": 30, "routing_profile": "latency-first"},
                "workflow": {"queue_priority": 30, "routing_profile": "balanced"},
                "standard": {"queue_priority": 20, "routing_profile": "balanced"},
                "rag": {"queue_priority": 10, "routing_profile": "throughput"},
                "bulk": {"queue_priority": 10, "routing_profile": "throughput"},
            },
            "routing_profiles": {
                "quality-first": {
                    "objectives": [
                        "availability",
                        "quality",
                        "expected_duration",
                        "routing_priority",
                    ]
                },
                "latency-first": {
                    "objectives": [
                        "availability",
                        "expected_duration",
                        "quality",
                        "routing_priority",
                    ]
                },
                "balanced": {
                    "objectives": [
                        "availability",
                        "quality",
                        "expected_duration",
                        "routing_priority",
                    ]
                },
                "throughput": {
                    "objectives": [
                        "availability",
                        "expected_cost",
                        "expected_duration",
                        "quality",
                        "routing_priority",
                    ]
                },
            },
            "failure_rules": {
                "dependency.timeout": {
                    "disposition": "fallback",
                    "max_attempts_per_job": 4,
                },
                "dependency.unavailable": {
                    "disposition": "fallback",
                    "max_attempts_per_job": 4,
                },
                "engine.crash": {
                    "disposition": "fallback",
                    "max_attempts_per_job": 3,
                },
                "capacity.busy": {
                    "disposition": "retry",
                    "max_attempts_per_path": 2,
                    "max_attempts_per_job": 4,
                    "retry_delay_seconds": 0.05,
                },
            },
            "default_failure_rule": {"disposition": "fail"},
        }
    )


def load_policy(path: str | Path) -> PolicySpec:
    """Load a policy snapshot, rejecting unknown fields and unsafe YAML tags."""

    policy_path = Path(path)
    try:
        raw = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid YAML in {policy_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"{policy_path} must contain a YAML mapping")
    return PolicySpec.model_validate(raw)
