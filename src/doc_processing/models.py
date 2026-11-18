"""Shared models for jobs, attempts, artifacts, and OCR output."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum, IntEnum
from typing import Annotated, Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator

PDF = "application/pdf"
DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
PPTX = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
SUPPORTED_MEDIA_TYPES = frozenset({PDF, DOCX, PPTX})


def utc_now() -> datetime:
    return datetime.now(UTC)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class JobPriority(IntEnum):
    BULK = 10
    STANDARD = 20
    WORKFLOW = 30
    INTERACTIVE = 40


class ProducerKind(str, Enum):
    """Trusted origin used by policy; it is not a caller-controlled priority."""

    CHAT = "chat"
    AGENT = "agent"
    RAG_INGESTION = "rag_ingestion"
    WORKFLOW = "workflow"


class ArtifactRetention(str, Enum):
    """Who controls how long source and result artifacts remain available."""

    SESSION_BOUND = "session_bound"
    BACKGROUND = "background"


class ArtifactLifecycle(StrictModel):
    """Backend-authorized retention facts carried with an artifact reference."""

    retention: ArtifactRetention = ArtifactRetention.BACKGROUND
    owner_id: str | None = Field(default=None, min_length=1, max_length=128)

    @model_validator(mode="after")
    def require_owner_for_session_bound(self) -> "ArtifactLifecycle":
        if self.retention is ArtifactRetention.SESSION_BOUND and self.owner_id is None:
            raise ValueError("session-bound artifacts require an owner_id")
        return self


class PolicyReference(StrictModel):
    """Immutable identity of the policy snapshot that produced a decision."""

    policy_id: str = Field(pattern=r"^[a-z][a-z0-9-]{1,62}$")
    revision: int = Field(strict=True, gt=0)
    digest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class JobState(str, Enum):
    ACCEPTED = "accepted"
    VALIDATING = "validating"
    QUEUED = "queued"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


TERMINAL_JOB_STATES = frozenset(
    {JobState.SUCCEEDED, JobState.FAILED, JobState.REJECTED, JobState.CANCELLED}
)


class AttemptState(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"


class ProviderOutcome(str, Enum):
    NOT_STARTED = "not_started"
    FAILED = "failed"
    UNKNOWN = "unknown"


class PendingFailure(StrictModel):
    """Durable input awaiting a policy disposition after an attempt failure."""

    path_id: str
    error_code: str
    error_message: str
    provider_outcome: ProviderOutcome
    fallback_path_ids: tuple[str, ...] = ()


class DocumentReference(StrictModel):
    """A document artifact reference; document bytes do not enter events or jobs."""

    uri: str = Field(min_length=1)
    storage_key: str | None = Field(default=None, min_length=1)
    filename: str = Field(min_length=1)
    media_type: str
    size_bytes: int = Field(strict=True, ge=0)
    page_count: int = Field(strict=True, gt=0)
    checksum_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    lifecycle: ArtifactLifecycle = Field(default_factory=ArtifactLifecycle)

    @field_validator("media_type")
    @classmethod
    def validate_media_type(cls, value: str) -> str:
        if value not in SUPPORTED_MEDIA_TYPES:
            raise ValueError(f"unsupported media type: {value}")
        return value


class TextLayer(str, Enum):
    UNKNOWN = "unknown"
    PRESENT = "present"
    ABSENT = "absent"


class DocumentKind(str, Enum):
    """Routing-relevant document shape derived from a known text layer."""

    DIGITAL = "digital"
    SCANNED = "scanned"


DOCUMENT_KINDS = frozenset(kind.value for kind in DocumentKind)


class DataClassification(str, Enum):
    """Sensitivity of a document's contents; a hard routing eligibility gate.

    Ordered least to most sensitive. Classification never ranks a path: a path
    either is permitted to process a given class or is excluded before scoring.
    """

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


DATA_CLASSIFICATIONS = frozenset(level.value for level in DataClassification)
REGION_PATTERN = r"^[a-z][a-z0-9-]{1,62}$"


def document_kind_for(text_layer: TextLayer) -> DocumentKind | None:
    """Map a text-layer fact to a routing kind, or None when unknown."""

    if text_layer is TextLayer.PRESENT:
        return DocumentKind.DIGITAL
    if text_layer is TextLayer.ABSENT:
        return DocumentKind.SCANNED
    return None


class LayoutClass(str, Enum):
    UNKNOWN = "unknown"
    PLAIN = "plain"
    TABLE_HEAVY = "table_heavy"
    MIXED = "mixed"


class FeaturePresence(str, Enum):
    UNKNOWN = "unknown"
    NONE = "none"
    PRESENT = "present"


class DocumentProfile(StrictModel):
    """Bounded preflight facts used by policy; unknown is always representable."""

    profiler_id: str = Field(default="unprofiled", pattern=r"^[a-z][a-z0-9-]{1,62}$")
    revision: int = Field(default=1, strict=True, gt=0)
    text_layer: TextLayer = TextLayer.UNKNOWN
    layout_class: LayoutClass = LayoutClass.UNKNOWN
    handwriting: FeaturePresence = FeaturePresence.UNKNOWN
    mathematical_notation: FeaturePresence = FeaturePresence.UNKNOWN
    languages: tuple[Annotated[str, Field(pattern=r"^[a-z]{2,3}(-[A-Z]{2})?$")], ...] = ()


class StoredArtifact(StrictModel):
    key: str
    uri: str
    content_type: str
    size_bytes: int = Field(ge=0)


class DispatchCommand(StrictModel):
    """Claim-check command: only job and artifact-reference identity cross the broker."""

    command_id: str = Field(default_factory=lambda: f"cmd_{uuid4().hex}")
    job_id: str
    subject: str = "document.jobs.process"
    created_at: datetime = Field(default_factory=utc_now)


class OCRResult(StrictModel):
    markdown: str
    path_id: str
    engine: str
    source_uri: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class Attempt(StrictModel):
    attempt_id: str = Field(default_factory=lambda: f"att_{uuid4().hex}")
    path_id: str
    path_revision: int
    state: AttemptState = AttemptState.RUNNING
    started_at: datetime = Field(default_factory=utc_now)
    finished_at: datetime | None = None
    error_code: str | None = None
    error_message: str | None = None
    policy: PolicyReference | None = None
    route_explanation: dict[str, Any] = Field(default_factory=dict)


class Job(StrictModel):
    job_id: str = Field(default_factory=lambda: f"job_{uuid4().hex}")
    tenant_id: str = Field(min_length=1)
    document: DocumentReference
    document_profile: DocumentProfile = Field(default_factory=DocumentProfile)
    producer_kind: ProducerKind | None = None
    data_classification: DataClassification | None = None
    home_region: Annotated[str, Field(pattern=REGION_PATTERN)] | None = None
    priority: JobPriority = JobPriority.STANDARD
    service_class: str = Field(default="standard", pattern=r"^[a-z][a-z0-9-]{1,62}$")
    queue_priority: int = Field(default=int(JobPriority.STANDARD), ge=0, le=1_000_000)
    share_weight: int = Field(default=1, gt=0)
    reserved_minimum: float = Field(default=0.0, ge=0, le=1)
    borrow_unused: bool = True
    starvation_bound_seconds: int = Field(default=3600, gt=0)
    admission_policy: PolicyReference | None = None
    state: JobState = JobState.ACCEPTED
    version: int = 0
    attempts: list[Attempt] = Field(default_factory=list)
    pending_failure: PendingFailure | None = None
    result: StoredArtifact | None = None
    artifacts_released_at: datetime | None = None
    selected_path_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="before")
    @classmethod
    def preserve_legacy_priority_defaults(cls, value: Any) -> Any:
        """Load v0.1 payloads without changing their prior queue behavior."""

        if not isinstance(value, dict):
            return value
        data = dict(value)
        priority = data.get("priority", JobPriority.STANDARD)
        try:
            normalized = JobPriority(priority)
        except (TypeError, ValueError):
            return data
        data.setdefault("service_class", normalized.name.lower())
        data.setdefault("queue_priority", int(normalized))
        return data

    @computed_field
    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_JOB_STATES
