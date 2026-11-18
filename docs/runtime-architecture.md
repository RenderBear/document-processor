# Runnable v0.1 runtime architecture

Status: Accepted architecture for the runnable v0.1 service

This document preserves the runtime decisions that future changes must respect. `README.md`
defines the current runnable behavior. `docs/spec.md` remains target v1 architecture and does not
silently expand the guarantees of this process-local slice.

## Payload boundary

Source documents and OCR results live behind `ArtifactStorage`, the stable interface shared by the
backend control plane and processor data plane. The backend alone authorizes object keys and
lifecycle ownership. It accepts bounded proxy uploads and also issues short-lived signed upload
grants: the caller then PUTs bytes directly to object storage (or, in local development, to a
backend receiver) while the backend authorizes the key up front and verifies the object's presence
and size at job creation. Byte transfer may bypass the backend, but key authorization and
post-upload verification do not. Processors verify the referenced source before execution. Jobs, policy messages, lifecycle
events, and PostgreSQL rows carry artifact references, checksums, sizes, bounded metadata, and
decision facts; they do not carry source bytes or extracted OCR text. HTTP ingress may buffer an
upload while validating and storing it, but that transient transport handling does not make the
job store or event stream a payload store.

This boundary keeps persistence implementations replaceable, prevents retained events and policy
logs from becoming document stores, and limits accidental content exposure. A change that embeds
document or result content in job, event, policy, or database payloads changes the architecture and
requires an explicit compatibility and security decision.

## Artifact lifecycle boundary

Artifact lifetime follows backend-authorized purpose, never transport lifetime alone. `chat` is
session-bound and requires an artifact-session owner; an explicit, idempotent close cancels unfinished
work and deletes that session's source and result objects. RAG ingestion, agent, workflow,
and unclassified background submissions outlive browser or UI closure. A best-effort browser close
signal is not sufficient garbage collection, so production still requires authenticated ownership and
an expiry/deletion reconciler.

Lifecycle facts travel with the artifact reference so processors and cleanup code apply the same
decision. Changing a producer's retention class, allowing a client to choose its own class, or deleting
background work on UI disconnect changes this architecture.

## Durable dispatch boundary

For admitted work, the queued job snapshot, its initial ordered lifecycle events, and one claim-check
dispatch command commit in a single `JobStore` transaction. Only after object size/checksum verification
and that commit may the API return `202 Accepted`. JetStream availability is not an acceptance gate:
the outbox publisher retries pending commands and uses the command id as the broker deduplication id.

Commands contain job identity rather than document bytes or expiring object grants. File-backed
JetStream uses bounded age, bytes, and message count; durable pull consumers use explicit acknowledgement,
extend acknowledgement deadlines during OCR, and acknowledge only after terminal state is durable.
Delivery is at least once. Job/version compare-and-swap prevents a duplicate command from owning the
same transition, adapters receive job and attempt identities for downstream idempotency, and exhausted
deliveries are explicitly copied to a quarantine subject.

## Preflight boundary

Durable acceptance commits a job as `accepted`, not `queued`. An asynchronous preflight stage then
derives authoritative facts from the stored source bytes before the job enters the scheduler, moving
it `accepted -> validating -> queued` on success or `-> rejected` when the input is unusable. The
client-supplied page count is an untrusted hint; preflight replaces it with the parsed truth, which
is what fair-share cost and hard page limits are then evaluated against. Preflight owns the
`DocumentProfile` (text-layer presence, and future layout/feature facts) that routing consumes.

Preflight is an orchestration effect, not a policy decision: it reads bytes through
`ArtifactStorage` and emits only bounded decision facts — never extracted document text. It runs
before the job is charged fair-share cost, is idempotent across redelivery and restart, and rejects
encrypted, structurally invalid, or oversized documents with a stable reason code. A job whose
reference has no managed source object (a synthetic library submission) has nothing to validate and
moves `accepted -> queued` directly. Moving validation after scheduling, trusting a client page
count, or letting preflight publish source content changes this architecture.

## Policy effect boundary

Policy classifies admission, orders paths already admitted by hard eligibility, and chooses retry,
allowed fallback, or failure. Orchestration alone owns state transitions, capacity reservation,
attempt ceilings, OCR invocation, artifact completion, and terminal-event production.

Every built-in or customer-supplied policy response is validated at this boundary. Policy outages
must fail safely without launching ungoverned effects, and a policy implementation cannot widen
hard path limits or the orchestration safety ceiling.

## Compliance eligibility boundary

Data classification and residency are hard routing gates, never ranking inputs. A job may carry a
sensitivity classification (a fixed ordered enum) and a home region; each path declares the
classifications it is cleared to process and, optionally, the regions it serves. The orchestrator
excludes any path not cleared for the job's classification or region before route scoring runs, so a
cheaper or faster path can never win a document it is not permitted to process. When a capability-
eligible path exists but none is compliant, the job terminates as `policy.denied`, distinct from an
unsupported input.

These attributes are trusted, server-authoritative inputs in principle; the v0.1 submission form
accepts them like `producer_kind`, and production ingress must derive them from authenticated caller
and tenant policy rather than client claims. Defaults are permissive for compatibility — no
classification and no region gate nothing, and a path with no declared regions serves any region —
so enforcing real policy is a configuration act. Narrowing a producer's permitted classifications,
letting a client raise its own clearance, or turning a compliance gate into a score changes this
architecture.

## Runtime state ownership

The selected `JobStore` is authoritative for job snapshots, pending failure-policy input,
ordered lifecycle-event history, and dispatch-outbox state. With PostgreSQL, a transition and its event commit atomically,
stale versions fail, and a restarted service reconstructs unfinished work from durable state.
The repository launcher applies every ordered, idempotent migration to the local Compose database
before starting document services. It preserves existing volumes and fails closed if migration
does not complete; deployments outside that launcher remain responsible for schema rollout.

Fair-share usage, provisional scheduling charges, the ready queue, path and per-tenant rate-limit
tokens, runtime capacity, and live SSE fan-out are process-local rebuildable state. Ready work is
ordered by rank of entry then arrival order, with arrival-time aging as the only signal that lets
waiting override rank; the system has no deadlines. The per-tenant rate gate requeues an over-quota
tenant's new jobs after a short delay rather than reordering or failing them; none of this persists
across processes. Restart recovery deliberately resets scheduling accounting and re-enters
recoverable jobs using their persisted admission parameters and creation time. Persisting fair-share
counters in the job store would be a coordination redesign, not a routine durability improvement.

## Webhook delivery boundary

An optional webhook subscription is recorded atomically with an accepted job. Each lifecycle
event committed by `JobStore` is eligible for ordered at-least-once delivery, so PostgreSQL event
history is also the current durable notification outbox. Delivery attempts, retry time, terminal
delivery state, and failure diagnostics are stored independently from the job state. Notification
failure never changes a successful OCR job into a failed job.

Webhook bodies contain the existing bounded lifecycle-event representation and are signed with an
HMAC over the timestamp and exact request body. Callback URLs require an explicit hostname
allowlist and cannot contain URL credentials or fragments. Receivers authenticate the signature,
reject stale timestamps, and deduplicate by event identity and job version. Replacing the retained
event scan with JetStream later is a transport rollout; it must not change this webhook contract.

## Replica and scheduling boundary

The runnable deployment separates an API/outbox-publisher role from a JetStream processor role, so
processor outages do not reject otherwise durable uploads. PostgreSQL compare-and-swap and JetStream
redelivery make duplicate command delivery safe at the job-transition boundary, but fair-share usage,
path tokens, and capacity accounting remain local to a processor process. Run one processor replica per
declared path-capacity envelope until shared scheduling, leases, and fencing exist; additional replicas
would otherwise interpret a process-local limit as a global limit.

Live SSE fan-out and webhook scanning also remain process-local to the API role. Horizontally scaling
processor capacity or event delivery requires the shared coordination and notification mechanisms in
`docs/spec.md`; those target mechanisms are not current runtime guarantees.

## Versioned architecture lifecycle

Current v0.1 contracts and target v1 architecture evolve independently. A locally reasonable
implementation of a target-v1 feature must not be presented as a current guarantee until its API,
persistence, failure, migration, compatibility, rollout, and verification behavior is accepted.
Conversely, target design text does not retire a v0.1 promise without an explicit governance
change.
