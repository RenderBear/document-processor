# Policy decision contract

## Purpose

The document service separates stable orchestration effects from replaceable policy decisions.
The orchestration kernel owns durable state transitions, hard path eligibility, capacity
reservation, attempt ceilings, OCR invocation, artifact persistence, and terminal events. A
policy engine may classify admitted work, order already-safe paths, and choose a bounded failure
disposition. It cannot perform those effects itself.

The default engine evaluates `configs/policy.yaml`. An embedding application can instead provide
any object satisfying the asynchronous `PolicyEngine` protocol in `doc_processing.policy`.

## Stable decision surface

The protocol has three operations:

```text
admit(AdmissionInput) -> AdmissionDecision
route(RouteInput) -> RouteDecision
on_failure(FailureInput) -> FailureDecision
```

Inputs and outputs are strict Pydantic models and therefore have JSON-compatible wire shapes for
an adapter that calls a customer-hosted engine. The service validates every returned decision,
even when the engine is in-process. Each call is bounded by the orchestration-owned
`scheduler.policy_evaluation_timeout_seconds` ceiling; unavailable evaluations are retried no
faster than `scheduler.policy_retry_delay_seconds`.

Every top-level input and decision carries `contract_version: 1`. A future incompatible wire shape
requires a new contract version and an adapter or explicit migration; policy revision changes do
not silently redefine the wire contract.

### Admission

`AdmissionInput` supplies tenant, trusted producer kind when known, legacy requested priority,
the bounded document reference, a versioned `DocumentProfile`, and the observation time.
`AdmissionDecision` supplies:

- Whether the job is admitted.
- Its server-selected `service_class` and queue priority.
- Weighted-fair scheduling parameters: share weight, reserved minimum, borrowing permission, and
  starvation bound.
- The immutable policy identity and stable reason codes.

Requests without `producer_kind` retain the v0.1 priority mapping. When a producer is supplied,
the built-in policy ignores the requested priority for classification. Production ingress must
derive producer identity from authenticated caller context; the unauthenticated v0.1 form field is
only a runnable contract demonstration.

`scheduling_algorithm` selects how ready work is ordered from the same admitted per-job parameters.
The in-process `weighted_fair_v1` scheduler first protects starved jobs, then selects the class
with the lowest consumed page cost relative to its active target share, selects the least-served
tenant in that class, and finally uses queue priority and arrival order. Idle shares are borrowed
when allowed. `rank_v1` instead orders strictly by admitted `queue_priority` with FIFO
tie-breaking and no class/tenant fair-share. Both algorithms keep the shared starvation-bound aging
safety net, so no job is starved indefinitely. Accounting is rebuildable process-local state; the
admitted job stores the policy parameters required to requeue it after restart.

Schema version 1 requires borrowing to be enabled for `weighted_fair_v1`; a no-borrow class would
require windowed idle capacity accounting that this process-local scheduler does not claim to
implement.

Ready work is ordered by rank of entry (the admitted queue priority) first, then time of entry
(arrival order) as the tie-break; arrival-time aging is the only signal that lets waiting override
rank, promoting a job past its class once it exceeds its starvation bound so nothing waits
indefinitely. The system has no deadlines. Separately, a per-tenant dispatch rate limit is a
reservation gate: an over-quota tenant has its new jobs requeued after a short delay rather than
reordered or failed, so rate limiting delays work without dropping it. This state is process-local
and rebuildable.

### Routing

The kernel applies media type, bytes, page, document-kind, data-classification, and residency hard
limits before constructing `RouteInput`. A path declares the document kinds it serves (`digital`,
`scanned`); once preflight detects the text layer, only paths that support the document's kind are
eligible, and a kind that is still unknown gates nothing. A path also declares the data
classifications it may process and the regions it serves; a classified or region-bound job is
excluded from paths not cleared for it, and no compliant path yields `policy.denied` rather than a
scoring penalty. Classification and residency never rank a path — they only filter. Each `PathCandidate` contains an immutable path revision, operator
quality/performance priors, bounded cost, health, queue-depth, and saturation observations. The
default engine applies the service class's quality floor and lexicographic objective order.

`DocumentProfile` gives customer engines a bounded place for preflight facts: profiler identity and
revision, text-layer presence, layout class, handwriting, mathematical notation, and language
hints. All fields default to `unknown` or empty until a real preflight stage supplies them.

The current duration prior is intentionally simple:

```text
base = fixed_seconds + seconds_per_page * page_count
expected_duration = base * (1 + observed_queue_depth + in_flight)
```

A replacement engine can use the document reference and candidate metadata to query a richer
document-class model. It still returns only an ordered subset of the candidate path IDs. Unknown,
duplicate, or hard-ineligible IDs make the job fail with `policy.invalid_decision` before an OCR
attempt begins.

Every attempt records the route policy identity, stable reason codes, exclusions, and the selected
candidate assessment. Raw document content, provider payloads, and credentials are not policy
facts or decision-log fields.

### Failure disposition

`FailureInput` contains the normalized error code, provider-outcome certainty, attempt counts, and
the path IDs permitted by hard eligibility and the configured fallback graph. A policy may return:

- `retry`, selecting only the current path.
- `fallback`, selecting a nonempty ordered subset of the allowed fallbacks.
- `fail`, selecting no path.

The scheduler's `max_attempts_per_job` is an orchestration safety ceiling and cannot be raised by a
custom policy. A timed-out or unexpected provider call is recorded with `provider_outcome=unknown`;
policy may react to that uncertainty but cannot erase it.

The failed-attempt input is persisted as `pending_failure` before the engine is consulted. If the
engine is unavailable, the accepted job is requeued without launching another attempt. Evaluation
resumes from the same failure input when the engine recovers. A validated disposition clears the
pending input before the next attempt begins.

## Default declarative vocabulary

`configs/policy.yaml` is a strict schema-versioned snapshot containing:

- Producer-to-service-class defaults.
- Legacy requested-priority mappings.
- Service-class queue priority, fair-share, borrowing, and aging parameters.
- Routing quality floors and ordered objectives.
- Error-code failure rules and attempt budgets.

Unknown fields, missing producer mappings, dangling service-class or routing-profile references,
duplicate route objectives, and reserved minimums totaling more than one are rejected at startup.
The SHA-256 digest of canonical policy content, together with policy ID and revision, identifies
every decision snapshot.

The vocabulary is deliberately not a rule language: it has no expressions, functions, loops,
dynamic imports, or arbitrary state mutations. New behavior is introduced as a reviewed, named
algorithm version with tests and migration semantics.

## Supplying a customer engine

Library consumers inject an engine into `OCRService` or `create_app`:

```python
from doc_processing import OCRService

service = OCRService(
    ocr_config,
    artifact_storage,
    policy_engine=customer_policy_engine,
)
```

An external-service adapter should serialize the provided strict input model, call the customer's
decision point with a bounded timeout, validate the response into the corresponding decision
model, and return it. Keep that adapter outside the orchestration kernel so transport, credentials,
and customer-specific availability policy do not become workflow semantics.

Policy-engine behavior on failure is fixed:

- Admission evaluation failure returns service unavailable and no job is claimed as accepted.
- Routing evaluation failure leaves an accepted job queued.
- Failure evaluation failure retains `pending_failure` and does not launch another attempt.
- A malformed or unsafe decision terminates the job as `policy.invalid_decision`.

## Rollout

Policy files are loaded as immutable snapshots at service startup in v0.1. A production policy
administration process should validate and simulate a prospective bundle, store its content digest,
activate it explicitly, and retain the previous revision for rollback. Do not hot-edit an in-memory
snapshot or change an already-created attempt's path revision and timeout.

Fair-queue accounting and runtime capacity are process-local in v0.1. Redis-backed coordination
can later implement the same admitted scheduling parameters without changing the policy engine
contract. Kubernetes may host the service and path pools, but Kubernetes does not replace this
document scheduler.
