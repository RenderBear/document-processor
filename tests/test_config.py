from pathlib import Path

import pytest
from pydantic import ValidationError

from doc_processing.config import OCRServiceConfig, load_config


ROOT = Path(__file__).parents[1]


def test_repository_path_contract_loads() -> None:
    config = load_config(ROOT / "configs" / "ocr-paths.yaml")

    assert config.schema_version == 1
    assert [path.path_id for path in config.enabled_paths] == [
        "external-mock",
        "internal-mock",
    ]
    assert config.path("external-mock").fallback_path_ids == ("internal-mock",)
    assert config.path("external-mock").limits.request_timeout_seconds == 10.0
    assert config.path("external-mock").mock.latency_seconds == 3.5
    assert config.path("internal-mock").limits.request_timeout_seconds == 12.0
    assert config.path("internal-mock").mock.latency_seconds == 5.0


def test_contract_rejects_unknown_fields_and_fallback_targets() -> None:
    raw = {
        "schema_version": 1,
        "unknown": True,
        "paths": [
            {
                "path_id": "external",
                "revision": 1,
                "adapter": "mock_external",
                "fallback_path_ids": ["missing"],
                "capabilities": {"media_types": ["application/pdf"]},
                "limits": {
                    "max_concurrency": 1,
                    "request_timeout_seconds": 1.0,
                    "max_document_bytes": 100,
                    "max_pages": 1,
                },
            }
        ],
    }

    with pytest.raises(ValidationError):
        OCRServiceConfig.model_validate(raw)
    raw.pop("unknown")
    with pytest.raises(ValidationError, match="unknown fallback paths"):
        OCRServiceConfig.model_validate(raw)


def test_contract_rejects_cyclic_fallbacks() -> None:
    base = {
        "revision": 1,
        "adapter": "mock_external",
        "capabilities": {"media_types": ["application/pdf"]},
        "limits": {
            "max_concurrency": 1,
            "request_timeout_seconds": 1.0,
            "max_document_bytes": 100,
            "max_pages": 1,
        },
    }
    raw = {
        "schema_version": 1,
        "paths": [
            {**base, "path_id": "path-a", "fallback_path_ids": ["path-b"]},
            {**base, "path_id": "path-b", "fallback_path_ids": ["path-a"]},
        ],
    }

    with pytest.raises(ValidationError, match="must not contain a cycle"):
        OCRServiceConfig.model_validate(raw)


def test_contract_requires_at_least_one_enabled_path() -> None:
    raw = {
        "schema_version": 1,
        "paths": [
            {
                "path_id": "disabled-path",
                "revision": 1,
                "adapter": "mock_internal",
                "enabled": False,
                "capabilities": {"media_types": ["application/pdf"]},
                "limits": {
                    "max_concurrency": 1,
                    "request_timeout_seconds": 1.0,
                    "max_document_bytes": 100,
                    "max_pages": 1,
                },
            }
        ],
    }

    with pytest.raises(ValidationError, match="at least one OCR path must be enabled"):
        OCRServiceConfig.model_validate(raw)
