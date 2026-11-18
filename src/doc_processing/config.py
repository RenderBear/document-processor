"""Strict, versioned configuration for an arbitrary number of OCR paths."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, StrictBool, model_validator

from .models import DATA_CLASSIFICATIONS, DOCUMENT_KINDS, REGION_PATTERN, SUPPORTED_MEDIA_TYPES

PositiveInt = Annotated[int, Field(strict=True, gt=0)]
PositiveFloat = Annotated[float, Field(strict=True, gt=0)]
NonNegativeFloat = Annotated[float, Field(strict=True, ge=0)]


class _ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SchedulerConfig(_ContractModel):
    workers: PositiveInt = 1
    capacity_poll_interval_seconds: PositiveFloat = 0.05
    policy_evaluation_timeout_seconds: PositiveFloat = 1.0
    policy_retry_delay_seconds: PositiveFloat = 1.0
    max_attempts_per_job: PositiveInt = 8
    # Per-tenant dispatch rate limit (a reservation gate): when set, a tenant that
    # has spent its allowance has new jobs requeued after a short delay, never failed.
    tenant_requests_per_minute: PositiveInt | None = None
    tenant_burst: PositiveInt | None = None
    tenant_rate_retry_delay_seconds: PositiveFloat = 0.5

    @model_validator(mode="after")
    def validate_tenant_rate(self) -> "SchedulerConfig":
        if (self.tenant_requests_per_minute is None) != (self.tenant_burst is None):
            raise ValueError(
                "tenant_requests_per_minute and tenant_burst must be declared together"
            )
        return self


class PreflightConfig(_ContractModel):
    """Bounds for the preflight profiling stage run before scheduling."""

    max_pages: PositiveInt = 5000
    text_sample_pages: PositiveInt = 5
    digital_text_threshold_chars: PositiveInt = 16


class PathCapabilities(_ContractModel):
    media_types: frozenset[str] = Field(min_length=1)
    output_formats: frozenset[str] = Field(default_factory=lambda: frozenset({"text/markdown"}), min_length=1)
    # Which document kinds (from preflight text-layer detection) this path serves.
    # Defaulting to both keeps paths that predate profile-driven routing eligible
    # for every document; a text-extraction path may declare {"digital"} only.
    document_kinds: frozenset[str] = Field(
        default_factory=lambda: frozenset(DOCUMENT_KINDS), min_length=1
    )

    @model_validator(mode="after")
    def validate_capabilities(self) -> "PathCapabilities":
        unsupported = self.media_types - SUPPORTED_MEDIA_TYPES
        if unsupported:
            raise ValueError(f"unsupported media types: {', '.join(sorted(unsupported))}")
        if "text/markdown" not in self.output_formats:
            raise ValueError("v0.1 paths must produce text/markdown")
        unknown_kinds = self.document_kinds - DOCUMENT_KINDS
        if unknown_kinds:
            raise ValueError(f"unsupported document kinds: {', '.join(sorted(unknown_kinds))}")
        return self


class PathCompliance(_ContractModel):
    """Hard data-governance gates: which sensitivities and regions a path serves.

    Both default to permissive for backward compatibility: all classifications,
    and no region constraint (empty regions means the path serves any region).
    Production deployments must narrow these to enforce real policy.
    """

    allowed_classifications: frozenset[str] = Field(
        default_factory=lambda: frozenset(DATA_CLASSIFICATIONS), min_length=1
    )
    regions: frozenset[Annotated[str, Field(pattern=REGION_PATTERN)]] = Field(
        default_factory=frozenset
    )

    @model_validator(mode="after")
    def validate_compliance(self) -> "PathCompliance":
        unknown = self.allowed_classifications - DATA_CLASSIFICATIONS
        if unknown:
            raise ValueError(
                f"unsupported data classifications: {', '.join(sorted(unknown))}"
            )
        return self


class PathLimits(_ContractModel):
    """Operator-declared hard limits; runtime health and free slots are discovered."""

    max_concurrency: PositiveInt
    request_timeout_seconds: PositiveFloat
    max_document_bytes: PositiveInt
    max_pages: PositiveInt
    requests_per_minute: PositiveInt | None = None
    burst: PositiveInt | None = None

    @model_validator(mode="after")
    def validate_rate_limit(self) -> "PathLimits":
        if (self.requests_per_minute is None) != (self.burst is None):
            raise ValueError("requests_per_minute and burst must be declared together")
        return self


class MockSettings(_ContractModel):
    latency_seconds: Annotated[float, Field(strict=True, ge=0)] = 0.1
    fail_with: Literal["dependency.unavailable", "engine.crash"] | None = None


class PathEstimates(_ContractModel):
    """Operator priors used until document-class telemetry supersedes them."""

    quality_score: Annotated[float, Field(strict=True, ge=0, le=1)] = 0.5
    fixed_seconds: NonNegativeFloat = 0.0
    seconds_per_page: NonNegativeFloat = 0.0
    cost_per_page: NonNegativeFloat = 0.0


class OCRPathConfig(_ContractModel):
    path_id: Annotated[str, Field(pattern=r"^[a-z][a-z0-9-]{1,62}$")]
    revision: PositiveInt
    adapter: Literal["mock_external", "mock_internal"]
    enabled: StrictBool = True
    routing_priority: Annotated[int, Field(strict=True, ge=0)] = 0
    fallback_path_ids: tuple[str, ...] = ()
    capabilities: PathCapabilities
    compliance: PathCompliance = Field(default_factory=PathCompliance)
    limits: PathLimits
    estimates: PathEstimates = Field(default_factory=PathEstimates)
    mock: MockSettings = Field(default_factory=MockSettings)


class OCRServiceConfig(_ContractModel):
    schema_version: Literal[1]
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    preflight: PreflightConfig = Field(default_factory=PreflightConfig)
    paths: tuple[OCRPathConfig, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_path_graph(self) -> "OCRServiceConfig":
        ids = [path.path_id for path in self.paths]
        if len(ids) != len(set(ids)):
            raise ValueError("path_id values must be unique")
        if not any(path.enabled for path in self.paths):
            raise ValueError("at least one OCR path must be enabled")

        known = set(ids)
        for path in self.paths:
            fallbacks = path.fallback_path_ids
            if len(fallbacks) != len(set(fallbacks)):
                raise ValueError(f"{path.path_id} contains duplicate fallback paths")
            missing = set(fallbacks) - known
            if missing:
                raise ValueError(
                    f"{path.path_id} references unknown fallback paths: "
                    f"{', '.join(sorted(missing))}"
                )
            if path.path_id in fallbacks:
                raise ValueError(f"{path.path_id} cannot fall back to itself")

        self._validate_acyclic_fallbacks()
        return self

    def _validate_acyclic_fallbacks(self) -> None:
        graph = {path.path_id: path.fallback_path_ids for path in self.paths}
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(path_id: str) -> None:
            if path_id in visiting:
                raise ValueError("fallback graph must not contain a cycle")
            if path_id in visited:
                return
            visiting.add(path_id)
            for fallback_id in graph[path_id]:
                visit(fallback_id)
            visiting.remove(path_id)
            visited.add(path_id)

        for path_id in graph:
            visit(path_id)

    @property
    def enabled_paths(self) -> tuple[OCRPathConfig, ...]:
        return tuple(
            sorted(
                (path for path in self.paths if path.enabled),
                key=lambda path: (-path.routing_priority, path.path_id),
            )
        )

    def path(self, path_id: str) -> OCRPathConfig:
        try:
            return next(path for path in self.paths if path.path_id == path_id)
        except StopIteration as exc:
            raise KeyError(path_id) from exc


def load_config(path: str | Path) -> OCRServiceConfig:
    """Load a YAML path contract, rejecting unknown fields and unsafe YAML tags."""

    config_path = Path(path)
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid YAML in {config_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"{config_path} must contain a YAML mapping")
    return OCRServiceConfig.model_validate(raw)
