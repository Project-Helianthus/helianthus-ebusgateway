# Runtime driver/provider and north-south contract v1

Status: proposed software 0.7 contract. This document specifies a subsequent
implementation; the current binary does not expose the lifecycle service or a
universal manager for every protocol family.

Contract ID: `helianthus.gateway.driver-runtime/v1`

Gateway baseline: `Project-Helianthus/helianthus-ebusgateway` commit
[`e31106c9c726fbb8df7546901763e19b93659e72`](https://github.com/Project-Helianthus/helianthus-ebusgateway/tree/e31106c9c726fbb8df7546901763e19b93659e72)

Program source: software 0.7 guide commit
[`ec6050fb31738f201c96203c52be0702671df343`](https://github.com/Project-Helianthus/helianthus-execution-plans/tree/ec6050fb31738f201c96203c52be0702671df343/software-stabilization-07-08.implementing)

## 1. Decision and boundary

The gateway owns one runtime control service and one composition boundary. It
adapts existing native runtimes; it does not replace their protocol state
machines, qualification, I/O, retry, reconnect, framing, authorization, raw
evidence, acknowledgement, readback, or close mechanisms.

The contract covers:

- deterministic `list` and `get` views;
- `start`, `stop`, and `restart` without restarting the gateway process;
- persistent desired state separate from observed runtime state;
- optimistic revision checks and persistent idempotency records;
- source-epoch and driver-generation fencing;
- capability activation, admission, withdrawal, drain, and close proof;
- native observation to qualified semantic publication and projection;
- semantic intent to one admitted native route and truthful outcome;
- bounded causal loop and echo suppression; and
- compatibility with current public surfaces during staged migration.

This contract adds no autonomous control, optimization, fallback policy, or
write authority. A semantic capability describes a qualified possibility. A
current admission, authority decision, precondition decision, exact route, and
deadline are still required for every operation.

## 2. Semantic dependency

Reusable protocol-neutral types belong to the public semantic owners, not this
repository. This contract depends on the accepted revision of
`helianthus.semantic.kernel/v1` published by
[`Project-Helianthus/helianthus-docs-semantic`](https://github.com/Project-Helianthus/helianthus-docs-semantic)
and implemented by
[`Project-Helianthus/helianthus-semreg`](https://github.com/Project-Helianthus/helianthus-semreg).

The exact active dependency candidate is docs-semantic
[#3](https://github.com/Project-Helianthus/helianthus-docs-semantic/issues/3),
PR [#5](https://github.com/Project-Helianthus/helianthus-docs-semantic/pull/5),
full HEAD `cd3f30fba21bf1517984fcfb693ded6ffa5ae060`, specifically
[`api/v1/kernel.md`](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/cd3f30fba21bf1517984fcfb693ded6ffa5ae060/api/v1/kernel.md),
[`serialization.md`](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/cd3f30fba21bf1517984fcfb693ded6ffa5ae060/api/v1/serialization.md),
[`acceptance.md`](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/cd3f30fba21bf1517984fcfb693ded6ffa5ae060/api/v1/acceptance.md),
and
[`acceptance-vectors.json`](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/cd3f30fba21bf1517984fcfb693ded6ffa5ae060/api/v1/acceptance-vectors.json).
That PR is a review candidate, not accepted current product. This gateway
contract depends on those exact records being independently accepted and merged,
or on a reviewed successor whose compatibility with every consumed name and
invariant below is recorded before this contract merges.

| Semantic package | Candidate types consumed here |
|---|---|
| `semreg/v1` | `ContractVersion`, `DefinitionRef`, `SourceID`, `SourceEpochID`, `NativeBindingID`, `ClockEpochID`, `RevisionVector`, `NativeBinding`, `ServiceInstance`, `CapabilityInstance`, `CapabilityInstanceID`, `EvidenceRef`, `Digest`, `GenerationFence`, `PublicationBatch`, `Snapshot` |
| `semreg/v1/operation` | `Intent`, `CapabilityRequirement`, `Precondition`, `Route`, `DispatchEvidence`, `Acknowledgement`, `Readback`, `ExecutionRecord`, `CausalContext` |
| `semreg/v1/projection` | versioned target manifest, projection result, and loss report |

The dependency must preserve these distinct identities:

- process/source epoch;
- native transport connection generation;
- driver generation;
- semantic snapshot and object revisions;
- lifecycle operation ID;
- intent ID and idempotency key; and
- causal correlation ID.

No counter substitutes for another. Presentation selection, projection choice,
public alias, and compatibility identity never choose a native route.

## 3. Ownership

| Owner | Required responsibility |
|---|---|
| Gateway runtime control | Driver catalog, desired-state persistence, lifecycle serialization, revision/idempotency decisions, per-driver isolation, admission coordination, composition, and public lifecycle projection |
| Native driver/adapter | Native endpoint identity, connection and protocol lifecycle, discovery, qualification, decode, raw evidence, request construction, native retry/reconnect, native authorization, ACK/readback interpretation, and resource close proof |
| Semantic kernel and packs | Protocol-neutral identities, facts, services, capabilities, operation intents/outcomes, provenance, validation, conflict, and projection-loss types |
| Native-to-semantic binding | Exact profile/version mapping from qualified native evidence to semantic facts/capabilities and from a semantic operation to one typed native operation |
| Public output/consumer | Target projection and compatibility behavior; never upstream qualification or native route selection |

Common lifecycle code must not switch on protocol, vendor, model, register,
feature, or function code. Native mappings register against an exact protocol,
profile, profile version, capability definition, and operation definition.

## 4. Control-plane types

The subsequent Go implementation is versioned under the gateway module. The
shapes below are normative Go-like definitions; names imported from semreg use
the accepted dependency without local copies. `semv1` denotes `semreg/v1` and
`operationv1` denotes `semreg/v1/operation` in the snippets.

```go
package driverv1

type ContractID string        // fixed: helianthus.gateway.driver-runtime/v1
type DriverID string          // stable configured runtime identity
type DriverRevision uint64    // revision of one persistent desired record
type CatalogRevision uint64   // revision-consistent list view
type DriverGeneration uint64  // starts at 1 inside one SourceEpochID
type LifecycleOperationID string
type IdempotencyKey string

type ReasonCode string
const (
    ReasonNone                  ReasonCode = "NONE"
    ReasonConfigDisabled        ReasonCode = "CONFIG_DISABLED"
    ReasonOperatorStopped       ReasonCode = "OPERATOR_STOPPED"
    ReasonStartRequested        ReasonCode = "START_REQUESTED"
    ReasonStopRequested         ReasonCode = "STOP_REQUESTED"
    ReasonConfigInvalid         ReasonCode = "CONFIG_INVALID"
    ReasonProviderUnavailable   ReasonCode = "PROVIDER_UNAVAILABLE"
    ReasonDependencyUnavailable ReasonCode = "DEPENDENCY_UNAVAILABLE"
    ReasonRuntimeNotReady       ReasonCode = "RUNTIME_NOT_READY"
    ReasonCapabilityDegraded    ReasonCode = "CAPABILITY_DEGRADED"
    ReasonRetryScheduled        ReasonCode = "RETRY_SCHEDULED"
    ReasonRetryExhausted        ReasonCode = "RETRY_EXHAUSTED"
    ReasonStopTimeout           ReasonCode = "STOP_TIMEOUT"
    ReasonCloseUnconfirmed      ReasonCode = "CLOSE_UNCONFIRMED"
    ReasonInternalError         ReasonCode = "INTERNAL_ERROR"
)

type Reason struct {
    Code      ReasonCode
    Retryable bool
}

type NativeContractRef struct {
    Owner    string // public native owner repository/module
    Contract string // owner-defined operation/profile contract
    Version  string // exact owner-defined version or revision
}

type GenerationKey struct {
    DriverID    DriverID
    SourceID    semv1.SourceID
    SourceEpoch semv1.SourceEpochID
    Generation  DriverGeneration
}

type LifecycleCorrelation struct {
    Generation  GenerationKey
    OperationID LifecycleOperationID
}

type ControlErrorCode string
const (
    ErrorInvalidArgument      ControlErrorCode = "INVALID_ARGUMENT"
    ErrorPermissionDenied     ControlErrorCode = "PERMISSION_DENIED"
    ErrorNotFound             ControlErrorCode = "NOT_FOUND"
    ErrorRevisionConflict     ControlErrorCode = "REVISION_CONFLICT"
    ErrorIdempotencyConflict  ControlErrorCode = "IDEMPOTENCY_CONFLICT"
    ErrorFailedPrecondition   ControlErrorCode = "FAILED_PRECONDITION"
    ErrorUnavailable          ControlErrorCode = "UNAVAILABLE"
    ErrorDeadlineExceeded     ControlErrorCode = "DEADLINE_EXCEEDED"
    ErrorCloseUnconfirmed     ControlErrorCode = "CLOSE_UNCONFIRMED"
)

type ControlError struct {
    Code        ControlErrorCode
    Message     string
    Retriable   bool
    Current     *DriverView
    OperationID *LifecycleOperationID
}

type DesiredState string
const (
    DesiredRunning DesiredState = "RUNNING"
    DesiredStopped DesiredState = "STOPPED"
)

type ObservedState string
const (
    ObservedDisabled ObservedState = "DISABLED"
    ObservedStopped  ObservedState = "STOPPED"
    ObservedStarting ObservedState = "STARTING"
    ObservedRunning  ObservedState = "RUNNING"
    ObservedDegraded ObservedState = "DEGRADED"
    ObservedBackoff  ObservedState = "BACKOFF"
    ObservedStopping ObservedState = "STOPPING"
    ObservedFailed   ObservedState = "FAILED"
)

type DesiredRecord struct {
    DriverID       DriverID
    State          DesiredState
    Revision       DriverRevision
    LastOperation  LifecycleOperationID
    UpdatedAtUTC   time.Time
}

type ObservedRecord struct {
    State                 ObservedState
    Reason                Reason
    SourceEpoch           semv1.SourceEpochID
    Generation            DriverGeneration
    NativeRevision        uint64
    EffectiveCapabilities []semv1.CapabilityInstance
    Quarantined           bool
    ActiveOperation       *LifecycleOperationID
    ChangedAtUTC          time.Time
}

type DriverView struct {
    Contract ContractID
    Desired  DesiredRecord
    Observed ObservedRecord
}

type ListDriversRequest struct{}
type ListDriversResult struct {
    CatalogRevision CatalogRevision
    Drivers         []DriverView
}

type GetDriverRequest struct { DriverID DriverID }
type GetDriverResult struct {
    CatalogRevision CatalogRevision
    Driver          DriverView
}

type StartDriverRequest struct {
    DriverID         DriverID
    ExpectedRevision DriverRevision
    IdempotencyKey   IdempotencyKey
}
type StopDriverRequest = StartDriverRequest
type RestartDriverRequest = StartDriverRequest

type LifecycleDisposition string
const (
    LifecycleAccepted LifecycleDisposition = "ACCEPTED"
    LifecycleNoOp     LifecycleDisposition = "NO_OP"
)

type LifecycleReceipt struct {
    OperationID LifecycleOperationID
    Disposition LifecycleDisposition
    Desired     DesiredRecord
}

type ControlService interface {
    ListDrivers(context.Context, ListDriversRequest) (ListDriversResult, error)
    GetDriver(context.Context, GetDriverRequest) (GetDriverResult, error)
    StartDriver(context.Context, StartDriverRequest) (LifecycleReceipt, error)
    StopDriver(context.Context, StopDriverRequest) (LifecycleReceipt, error)
    RestartDriver(context.Context, RestartDriverRequest) (LifecycleReceipt, error)
}
```

`DriverID` is stable across process restart and gateway rename. It is not a bus
address, semantic asset ID, native binding ID, or public compatibility alias.
IDs are canonical non-empty UTF-8 strings matching
`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`, with a maximum of 128 bytes.
Every `NativeContractRef` field is non-empty and records the exact public native
owner, owner-defined contract ID, and owner-defined version/revision. It contains
no endpoint, credential, raw payload, or handle.

`ListDrivers` returns all configured drivers sorted bytewise by `DriverID`. The
entries and `CatalogRevision` come from one read transaction. `GetDriver`
returns `NOT_FOUND` for an unknown ID; it never constructs a driver as a read
side effect.

## 5. Mutation, persistence, and reconciliation

Lifecycle calls acknowledge a persistent control decision. They do not claim
that native startup, shutdown, or replacement has completed.

For every mutation, the control service performs one atomic transaction:

1. Validate the caller's lifecycle authority, closed request shape, driver ID,
   non-zero expected revision, non-empty idempotency key, and caller deadline.
2. Canonically hash contract ID, operation name, driver ID, expected revision,
   and all request fields.
3. Look up `(authority principal, idempotency key)` in the persistent ledger.
   An identical hash returns the stored receipt. A different hash returns
   `IDEMPOTENCY_CONFLICT` without state change.
4. Compare `ExpectedRevision` with the current persistent driver revision. A
   mismatch returns `REVISION_CONFLICT` with the current view.
5. Resolve `ACCEPTED` or `NO_OP`. For `ACCEPTED`, commit the desired record,
   non-empty lifecycle operation ID, request hash, and receipt together, then
   wake the asynchronous reconciler. For `NO_OP`, persist only the request hash
   and receipt against the unchanged desired record; its operation ID is empty
   and no reconciler wakeup is required.

The idempotency ledger survives process restart and remains at least as long as
the desired-state schema's rollback window. Expiry cannot occur while the
associated lifecycle operation or migration rollback remains reachable.

Operation semantics are:

- `start`: change desired state to `RUNNING`. If it is already `RUNNING`, return
  `NO_OP` without resetting backoff, replacing a degraded generation, or bumping
  revision. Recovery that replaces a live/degraded generation uses `restart`.
- `stop`: change desired state to `STOPPED`. If already `STOPPED`, return
  `NO_OP`. A no-op does not assert that an unconfirmed native close succeeded;
  the observed view retains failure/quarantine.
- `restart`: require desired state `RUNNING`, increment its revision, record a
  new operation, fence the current generation, and reconcile a replacement. A
  stopped driver returns `FAILED_PRECONDITION`.

For accepted changes, the driver revision and catalog revision increment once.
An idempotent replay and a no-op do not increment either revision. Two accepted
operations for one driver are serialized in persistent operation order.

At process start the HTTP/API/health shell becomes available before drivers are
reconciled. The store loads every desired record independently. Corrupt or
unmigratable state for one driver produces that driver's `FAILED` observation;
it does not prevent other drivers or the control service from starting.

Every configured source instance creates a new `SourceEpochID` on process start.
No admission, callback, observation, capability, route, or close proof from an
earlier source epoch is current. The reconciler starts or leaves stopped each
driver from persistent desired state and records new observations. Persisted
observed state is historical diagnostic evidence, never startup authority.

## 6. Provider SPI

The public SPI adapts the existing eBUS `Runtime`/`Admit`, eeBUS runtime slot,
and Modbus adapter boundaries. Protocol-specific types remain below the SPI.

```go
type ProviderDescriptor struct {
    DriverID       DriverID
    ProviderKind   string
    Contract       ContractID
    NativeContract NativeContractRef
}

type StartContext struct {
    Correlation LifecycleCorrelation
    Deadline    time.Time
    Publish     PublicationSink
    Report      HealthReporter
}

type ActivationProof struct {
    Correlation LifecycleCorrelation
    Binding     semv1.NativeBinding
    ActivatedAt time.Time
    Evidence    []semv1.EvidenceRef
}

type FenceProof struct {
    Generation        GenerationKey
    Withdrawal        semv1.GenerationFence
    AdmissionClosedAt time.Time
    InFlightAtFence   uint64
}

type CloseStatus string
const (
    CloseProven      CloseStatus = "PROVEN"
    CloseUnconfirmed CloseStatus = "UNCONFIRMED"
)

type CloseProof struct {
    Generation  GenerationKey
    Status      CloseStatus
    DrainedAt   *time.Time
    ClosedAt    *time.Time
    Evidence    []semv1.EvidenceRef
}

type ProviderFactory interface {
    Descriptor() ProviderDescriptor
    Start(context.Context, StartContext) (Generation, error)
}

type ReplacementFactory interface {
    Replace(context.Context, StartContext, Generation) (Generation, error)
}

type Generation interface {
    Activation(context.Context) (ActivationProof, error)
    Admit(context.Context, AdmissionRequest) (ExecutionLease, error)
    FenceWithdrawal(GenerationKey) FenceProof
    Stop(context.Context, FenceProof) (CloseProof, error)
}

type NativeOperationBody interface {
    NativeOperationContract() NativeContractRef
    Validate() error
}

type AdmissionRequest struct {
    Generation         GenerationKey
    CapabilityInstance semv1.CapabilityInstanceID
    NativeBinding      semv1.NativeBindingID
    NativeOperation    NativeContractRef
    Deadline           time.Time
}

type NativeExecutionEvidence struct {
    Dispatch        operationv1.DispatchEvidence
    Acknowledgement *operationv1.Acknowledgement
    Readback        *operationv1.Readback
}

type ExecutionLease interface {
    Generation() GenerationKey
    Execute(context.Context, NativeOperationBody) (NativeExecutionEvidence, error)
    Release() error
}

type PublicationEnvelope struct {
    Generation GenerationKey
    Sequence uint64
    Digest   semv1.Digest
    Batch    semv1.PublicationBatch
}

type PublicationSink interface {
    Publish(context.Context, PublicationEnvelope) error
}

type HealthReporter interface {
    ReportDegraded(context.Context, LifecycleCorrelation, Reason, []semv1.CapabilityInstanceID) bool
    ReportFailure(context.Context, LifecycleCorrelation, Reason) bool
}
```

`ProviderKind` and `NativeContract` select a registered adapter. They do not
permit common code to inspect vendor payloads. `NativeOperationBody` must be one
concrete typed value owned by the selected native binding. A mismatched contract
or concrete type fails before dispatch.

`GenerationKey` is the gateway's live admission identity. It is not a semantic
withdrawal record. The accepted semantic `GenerationFence` is the irreversible
withdrawal record and supplies `SourceID`, `SourceEpochID`, driver generation,
reason, evidence, and revision. The accepted `PublicationBatch` repeats the
exact source/epoch/generation and carries its own batch ID, digest, sequence,
expected semantic revision, observed time, typed upserts/withdrawals, and
generation fences. The gateway must validate those fields instead of filling
them from ambient process state. All semantic wire `uint64` values use the
kernel's canonical decimal-string representation.

`LifecycleCorrelation` keeps the lifecycle operation ID distinct from its
generation key. Construction completion and every asynchronous health callback
must carry the correlation supplied by the manager. A callback for a superseded
operation is rejected even if its native code reports a plausible generation.

`ReplacementFactory` is optional. If absent, `restart` fences and proves close
of the old generation before calling `Start` for the new generation. If close is
unconfirmed, replacement is prohibited and the driver is quarantined for the
current process epoch.

The gateway manager may retain its bounded lifecycle construction retry policy.
It does not retry a native operation or duplicate an adapter's reconnect loop.
An adapter may report a retry classification and owner-authorized earliest time;
the manager consumes it within one configured lifecycle budget. Modbus reconnect
and request retry remain inside the current Modbus owner and its one total
operation context.

## 7. Lifecycle ordering

### Start and activation

1. Reserve the next non-zero `DriverGeneration` inside the current source epoch.
2. Construct the native generation outside the manager state lock.
3. Obtain and validate `ActivationProof`: exact source epoch, driver generation,
   native binding, profile/version, and close owner.
4. Install the admission gate closed, then activate the native generation.
5. In one externally consistent transition, open admission, publish observed
   `RUNNING`, and publish qualified available capability instances.
6. Bind asynchronous health and publication callbacks to the exact generation
   key.

A capability cannot be externally available before native activation and live
admission both succeed. Static catalog entries may remain `candidate`,
`unsupported`, or `unknown`; they are not effective device capabilities.

### Withdrawal, stop, and restart

1. Under the same serialization boundary used by admission, close admission for
   the exact generation.
2. Atomically publish capability/service withdrawals and observed `STOPPING` (or
   `BACKOFF`/`FAILED` for a failure path).
3. Cancel generation-owned asynchronous work and wait for admitted leases to
   release within the operation deadline.
4. Ask the native owner to close its resources and return `CloseProof`.
5. Publish `STOPPED` only for `CloseProven`. For timeout or unconfirmed close,
   publish `FAILED` with `CLOSE_UNCONFIRMED` and quarantine the driver for the
   source epoch.
6. A restart may activate the next generation only after close is proven, unless
   the native replacement seam itself proves non-overlap, fencing, drain, and
   ownership transfer.

Withdrawal hooks are bounded, non-blocking, and must not re-enter the manager.
Drain and I/O occur outside a manager lock. A generation admitted after its
start operation was superseded is fenced immediately and retired with an
independent bounded cleanup deadline; it never becomes effective.

## 8. Upward flow

The upward path is:

```text
native observation
  -> native-owner validation and qualification
  -> exact native binding and evidence references
  -> typed semantic facts/services/capabilities
  -> atomic publication batch
  -> revision-consistent semantic snapshot
  -> versioned target projection plus loss report
```

Every observation records its exact source epoch and driver generation, native
profile/version, native observation identity, available native/source/receipt
times, and raw evidence reference. Endpoint secrets and private payloads remain
behind the native owner's evidence access contract.

Qualification remains `candidate`, `qualified`, `unsupported`, or `unknown`.
Runtime availability remains `available`, `degraded`, or `withdrawn`. These axes
must not be collapsed. Only a qualified capability on the current admitted
generation may become available.

Publication rules:

- sequence starts at 1 for each generation and increases by exactly one;
- replay of the same `(generation key, sequence, digest)` is a no-op;
- reuse of `(generation key, sequence)` with another digest is a source conflict, rejects
  the batch, withdraws effective capability, and degrades the driver;
- a sequence gap rejects later deltas until the adapter supplies a complete
  resynchronization batch;
- batches from another source epoch, withdrawn generation, or future generation
  are rejected without mutating semantic state;
- publication is atomic: facts, services, capabilities, withdrawals, conflicts,
  and the resulting revision vector become visible together; and
- a partial read updates only evidenced fields. Retained fields keep their own
  provenance and age under their own freshness rules.

Identity conflict is public conflict. Similar addresses, values, model strings,
or topology positions cannot merge assets. Selection may choose a presentation
candidate; it never removes alternatives or grants operation authority.

Projection consumes one revision-consistent snapshot and a versioned target
manifest. Every requested fact, relationship, capability, and operation receives
the semantic contract's explicit `exact`, `transformed`, `withheld`,
`unrepresentable`, `unsupported`, or `unknown` disposition, including structured
loss where required. A projected or reflected value is still an observation and
cannot create a fresh command.

## 9. Downward flow and outcome truth

The downward path is:

```text
typed intent
  -> caller authority
  -> current qualified capability
  -> typed preconditions and deadline
  -> exactly one native binding, driver, endpoint, source epoch, and generation
  -> live admission
  -> one typed native operation
  -> dispatch evidence
  -> native acknowledgement
  -> readback/observation
  -> execution record and public state
```

Resolution returns exactly one semreg `Route`. Zero routes returns
`NO_ROUTE`; more than one returns `AMBIGUOUS_ROUTE`. There is no priority-based
silent selection. A cached route is a hint only; admission rechecks current
authority, capability, binding, epoch, generation, preconditions, and deadline.
The accepted route must name the capability instance, service instance, native
binding, source, source epoch, and driver generation. Gateway-only endpoint and
typed native-operation details remain behind that exact binding.

Before dispatch, cancellation, deadline, stale revision, withdrawal, missing
native operation interface, failed authority, or failed precondition returns a
terminal rejection with no native I/O. After dispatch, the gateway never falls
back to another route and never turns a timeout into permission to repeat a
mutation.

An execution record uses the semantic contract's exact outcomes:

| State | Meaning |
|---|---|
| `rejected` | No route or dispatch was admitted; a stable admission error is explicit |
| `failed_no_contact` | Dispatch evidence proves `not_sent` and no possible side effect |
| `acknowledged_unverified` | Native accepted/provisional ACK is proven; confirming readback is absent |
| `applied` | Current-generation readback/observation satisfies the typed success predicate |
| `no_effect` | Current-generation evidence proves that the requested effect did not occur |
| `conflict` | Post-dispatch evidence contradicts the requested effect or another current result |
| `indeterminate` | Dispatch may have occurred and evidence cannot prove applied or no effect |

ACK is not readback. Readback is not permission. Public state changes only from
qualified observation/readback, not from requested intent or ACK. A later
current-generation readback may append a linked reconciliation record proving
`applied`, `no_effect`, or `conflict`; it does not rewrite the original immutable
execution record or create a second native execution.

Native retry is allowed only when the exact native operation contract proves it
safe and keeps all attempts inside the original route, generation, authority,
idempotency identity, deadline, and retry quota. A mutation with indeterminate
dispatch cannot be retried automatically. Lifecycle construction retry remains
separate from operation retry.

## 10. Causal loop and echo suppression

Every intent and resulting observation/projection carries semreg
`CausalContext`. Each output binding preserves origin, correlation ID, parent
execution ID, and hop count.

The accepted semantic context names `Origin`, `CorrelationID`, optional
`ParentCorrelationID`, `HopCount`, `MaxHops`, `FirstSeenAt`, `ExpiresAt`, and
the traversed target `Path`. Output bindings append their stable target ID to
`Path`; they do not replace the original correlation.

The v1 bounds are:

- maximum hop count: 8;
- every operation definition supplies an `echo_window` greater than zero and no
  more than the semantic v1 causal limit of 300 seconds before it is eligible
  for output binding;
- correlation tombstones remain through the later of the operation deadline or
  dispatch time plus `echo_window`, plus the semantic contract's clock
  uncertainty; and
- a context older than its echo window or beyond 8 hops may still contribute a
  truthful observation, but it cannot produce a derived intent.

An output rejects an intent when its causal ancestry already includes that
output binding and operation definition inside the echo window, or when the
same correlation/operation was already admitted. Rejection is recorded as
`CAUSAL_ECHO_SUPPRESSED` with no I/O.

A separate, explicitly authorized intent with a new intent ID and causal
correlation remains eligible even when its value equals a recent output. Value
equality alone is not loop detection. Idempotency suppresses duplicate requests;
causal suppression prevents cross-output loops. Neither grants authority.

## 11. Current operation inventory at the pinned baseline

This inventory describes code reachable from the production composition at the
pinned gateway baseline. It is not a 0.7 capability promise. Profile/catalog
code that is not connected to that composition stays unavailable.

| Runtime/profile | Current reachable operations | Current boundary and INT-06 disposition |
|---|---|---|
| eBUS `ebus.primary` | Discovery, raw evidence, read, semantic projection, and write are declared on the one managed runtime. Stable MCP provides runtime/registry/semantic reads, guarded schedules/config writes, and registry-routed `ebus.v1.rpc.invoke`; B503 adds five read-only evidence/session views. | Reuse current `DriverManager`, generation admission, selected-source intersection, and registry method mutability. A declared `WRITE` is not universal semantic write authority; the exact current registry method and source admission still decide. |
| eeBUS SHIP/SPINE runtime | Public redacted runtime, service, session, topology, pairing, and snapshot reads. Owner-only raw feature and mutation-record reads exist. Raw feature set and rollback exist only through the owner boundary, exact write authorization, configured mutation-lab profile, and a runtime implementing the optional mutation interface. | Adapt the eeBUS runtime slot and command router. Missing mutation interface is `unsupported`, never success. No generic semantic eeBUS write is inferred. Exact normative use-case mappings remain a separate docs/registry dependency. |
| Modbus TCP and qualified SunSpec/Fronius path | Bounded FC03/FC04 raw read, retained profile observation, qualified canonical PV read, and the existing qualify/refresh worker. | Read-only. Reuse adapter-owned scheduling, one owner-gated reconnect/retry, endpoint sanitization, and full wire/logical/physical/generation provenance. No FC06/FC16 or vendor-private write is permitted by the current gateway provider. |
| Tesla HSC provider in production composition | One disabled-by-default status snapshot with compatibility `unknown` and registry-derived `outbound_allowed` (currently false). | No serial acquisition or transmission. FC100/101/102 records and current-limit evidence types do not authorize a live route. |
| Huawei SmartLogger/EMMA/S-Dongle, Growatt Protocol II and BMS RS485, OutBack AXS, Fronius-specific status, Tesla FC100/WC/current-limit optional tools | Typed read/injected-provider contracts exist in the repository, but the production `gatewayModbusMCPProvider` does not implement their optional provider interfaces. | `unavailable` or `uncomposed` at this baseline. Later INT-07 composition must use exact qualified profile/version evidence. No write is inferred from a decoder, fixture, or provisional ACK/readback record. |
| Gree CAN and Growatt CAN | No production provider is composed by this gateway baseline. | `unavailable` here. Receive-only registry readiness remains native evidence, not a gateway operation or transmit grant. |
| Matter output | No production output binding is composed. | No read, write, or conformance claim. INT-13 owns the later versioned target mapping. |

The exact currently registered profile-facing operation names relevant to this
contract are:

- eBUS registry and generic route: `ebus.v1.registry.devices.list`,
  `ebus.v1.registry.devices.get`, `ebus.v1.registry.planes.list`,
  `ebus.v1.registry.methods.list`, and `ebus.v1.rpc.invoke`. Invoke permits
  `READ_ONLY` only for a known read-only registry method. `MUTATE` requires the
  current registry route, dangerous-operation acknowledgement, idempotency key,
  deadline, and admitted source.
- eBUS named semantic mutation surfaces:
  `ebus.v1.semantic.schedules.set_zone_time_program`,
  `ebus.v1.semantic.schedules.set_dhw_time_program`,
  `ebus.v1.semantic.system.set_config`, and
  `ebus.v1.semantic.boiler_status.set_config`. Their presence is not proof that
  every configured device supplies the corresponding writer or native method.
- eBUS standard catalog reads: `ebus.v1.ebus_standard.services.list`,
  `ebus.v1.ebus_standard.commands.list`,
  `ebus.v1.ebus_standard.command.get`, and
  `ebus.v1.ebus_standard.decode`.
- Vaillant B503 reads: `ebus.v1.vaillant.errors.get`,
  `ebus.v1.vaillant.errors.history.get`,
  `ebus.v1.vaillant.service.current.get`,
  `ebus.v1.vaillant.service.history.get`, and
  `ebus.v1.vaillant.live_monitor.get`. No B503 install-write tool is present.
- eeBUS public reads: `eebus.v1.runtime.status.get`,
  `eebus.v1.services.list`, `eebus.v1.services.get`,
  `eebus.v1.sessions.list`, `eebus.v1.sessions.get`,
  `eebus.v1.topology.get`, `eebus.v1.snapshot.capture`,
  `eebus.v1.snapshot.drop`, and `eebus.v1.pairing.status.get`.
- eeBUS owner-only raw operations: `eebus.v1.features.get`,
  `eebus.v1.features.data.get`, `eebus.v1.features.data.set`,
  `eebus.v1.mutations.get`, and `eebus.v1.mutations.rollback`. Set and rollback
  remain unavailable without the exact write authorization, mutation-lab
  profile, and optional native mutation interface.
- Modbus production composition: `modbus.v1.raw.read`,
  `modbus.v1.profile.observation.get`, `modbus.v1.semantic.pv.get`, and
  `modbus.v1.tesla.hsc.status.get`. Only FC03/FC04 raw reads are admitted; the
  Tesla status is an inert disabled-profile report with outbound disabled.

The gateway exposes no public driver lifecycle `list/get/start/stop/restart`
operation at this baseline. The control service in section 4 is the contract
for that later implementation.

Exact public source anchors for these statements:

- eBUS manager configuration and declared capabilities:
  [`cmd/gateway/ebus_driver_runtime.go#L18-L94`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/ebus_driver_runtime.go#L18-L94)
- eBUS source/write admission and lifecycle callbacks:
  [`cmd/gateway/ebus_driver_runtime.go#L241-L282`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/ebus_driver_runtime.go#L241-L282) and
  [`#L345-L573`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/ebus_driver_runtime.go#L345-L573)
- eBUS public tool inventory, invoke guard, and B503 read-only surface:
  [`mcp/server_tools.go#L1-L369`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/mcp/server_tools.go#L1-L369),
  [`mcp/server.go#L1932-L1989`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/mcp/server.go#L1932-L1989), and
  [`mcp/vaillant_b503.go#L46-L152`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/mcp/vaillant_b503.go#L46-L152)
- current manager lifecycle, late admission retirement, generation callback
  fencing, and admission:
  [`internal/drivermanager/manager.go#L93-L189`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/internal/drivermanager/manager.go#L93-L189),
  [`#L325-L658`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/internal/drivermanager/manager.go#L325-L658), and
  [`#L764-L845`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/internal/drivermanager/manager.go#L764-L845)
- eeBUS slot replacement/retirement and provider adapter:
  [`cmd/gateway/eebus_runtime_adapter.go#L26-L156`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/eebus_runtime_adapter.go#L26-L156)
- eeBUS public read inventory and owner-only raw command inventory:
  [`mcp/eebus_v1.go#L19-L108`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/mcp/eebus_v1.go#L19-L108) and
  [`mcp/eebus_v1_commands.go#L18-L69`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/mcp/eebus_v1_commands.go#L18-L69)
- eeBUS native authorization and optional mutation runtime:
  [`internal/eebuscommand/router.go#L50-L127`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/internal/eebuscommand/router.go#L50-L127)
  and the closed mutation-lab profile loader
  [`cmd/gateway/eebus_mutation_lab_profile.go#L12-L84`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/eebus_mutation_lab_profile.go#L12-L84)
- Modbus public tool/provider surface and composition:
  [`mcp/modbus_v1.go#L15-L152`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/mcp/modbus_v1.go#L15-L152),
  [`cmd/gateway/modbus_mcp_provider.go#L32-L159`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/modbus_mcp_provider.go#L32-L159), and
  [`cmd/gateway/gateway_http_server.go#L68-L85`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/gateway_http_server.go#L68-L85)
- Modbus adapter ownership and reconnect boundary:
  [`internal/modbusadapter/adapter.go#L19-L75`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/internal/modbusadapter/adapter.go#L19-L75) and
  [`#L257-L299`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/internal/modbusadapter/adapter.go#L257-L299)
- independent driver startup and process availability behavior:
  [`cmd/gateway/gateway_run_lifecycle.go#L43-L140`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/gateway_run_lifecycle.go#L43-L140)

## 12. Compatibility and staged migration

1. Freeze exact current MCP schemas/goldens, GraphQL behavior, Portal views, HA
   identities, eBUS registry IDs, eeBUS pairing/trust state, PV snapshot
   accounting, and persistent runtime-state behavior at their owning revisions.
2. Add the runtime control service internally. Adapt only `ebus.primary` first,
   preserving current `DriverManager` behavior and stable public APIs.
3. Publish semreg facts/capabilities in shadow mode beside current eBUS and PV
   donors. Compare value, unit, dimensions, quality, freshness, provenance,
   identity, availability, and explicit loss over the same fixtures/replays.
4. Adapt eeBUS without weakening the runtime-slot drain/close boundary or pairing
   state. Adapt Modbus without adding a second scheduler/reconnect owner.
5. Connect each remaining native family only after its provider issue supplies an
   exact profile/version contract and qualification. An unavailable family stays
   visible as unavailable; it does not disappear or become a generic profile.
6. Expose lifecycle `list/get/start/stop/restart` through INT-08 public surfaces.
   Existing APIs remain unchanged until their own compatibility/parity acceptance
   passes. HA later consumes the GraphQL form under INT-15.
7. Activate one driver/consumer slice behind a reversible versioned selection.
   Persist the selected semantic contract and migration version. Rollback restores
   the previous compatible path and state schema, never a mixed semantic state.
8. Remove historical eBUS-owned universal semantic donors only after every known
   consumer and persisted identifier has migrated and the exact integrated BOM
   passes INT-17/release acceptance.

Compatibility aliases can locate a public semantic asset. They cannot populate
`DriverID`, `NativeBindingID`, endpoint, generation, or route. No existing
`ebus.v1.*`, `eebus.v1.*`, or `modbus.v1.*` contract is redefined by this
document.

## 13. Acceptance scenarios for implementation

Each scenario becomes an executable fixture or integration test in its later
owning issue. The expected result is normative.

| ID | Scenario | Expected result |
|---|---|---|
| CTL-01 | List with drivers in arbitrary registration order | Bytewise `DriverID` order and one catalog revision |
| CTL-02 | Get an unknown driver | `NOT_FOUND`; no factory call or state mutation |
| CTL-03 | Start with stale expected revision | `REVISION_CONFLICT` plus current view; no persistent/native change |
| CTL-04 | Replay identical start key and request after process restart | Original receipt; no revision bump or second native start |
| CTL-05 | Reuse an idempotency key with another request hash | `IDEMPOTENCY_CONFLICT`; no state change |
| CTL-06 | Start an already desired-running driver | `NO_OP`; no backoff reset, replacement, or revision bump |
| CTL-07 | Restart a desired-stopped driver | `FAILED_PRECONDITION`; no state change |
| LIFE-01 | Native start and activation succeed | Admission, `RUNNING`, and qualified capabilities become visible together for the new generation key |
| LIFE-02 | One driver startup fails | That driver becomes `BACKOFF`/`FAILED`; API/health and unrelated drivers remain available |
| LIFE-03 | Stop drains and native close is proven | Capabilities withdraw before further admission; final state `STOPPED` |
| LIFE-04 | Stop deadline expires without close proof | `FAILED/CLOSE_UNCONFIRMED`, source-epoch quarantine, no replacement |
| LIFE-05 | Restart succeeds | Old generation is fenced, withdrawn, and closed before a strictly newer generation publishes |
| LIFE-06 | Gateway restarts with desired running and historical observed running | New source epoch reconciles a new generation; historical observation grants no admission |
| LIFE-07 | Canceled start returns a provider late | Provider is never published, is fenced and retired on an independent deadline; unproven close quarantines |
| LIFE-08 | Callback arrives from prior epoch/generation or superseded lifecycle operation | Rejected with no semantic or observed-state mutation |
| UP-01 | Partial observation fails for one field | Only evidenced fields update; other candidates retain provenance and age normally |
| UP-02 | Same publication sequence and digest repeats | No-op with no semantic revision bump |
| UP-03 | Same sequence repeats with another digest | Reject, withdraw effective capability, and degrade source |
| UP-04 | Publication sequence has a gap | Reject deltas until complete resynchronization |
| UP-05 | Similar native identities compete for one asset | Public identity conflict; no silent merge or route |
| UP-06 | Catalog knows an operation but device evidence does not qualify it | Candidate/unknown/unsupported as justified; no available capability |
| DOWN-01 | Exactly one current route passes authority, capability, preconditions, and deadline | One live lease and one native dispatch path |
| DOWN-02 | Zero or two current routes match | `NO_ROUTE` or `AMBIGUOUS_ROUTE`; no I/O |
| DOWN-03 | Route was resolved before generation withdrawal | Admission recheck rejects the stale generation key; no I/O |
| DOWN-04 | Native optional mutation interface is absent | `rejected/unsupported_operation`; no synthesized success |
| DOWN-05 | Deadline expires before dispatch | `failed_no_contact` only when native owner proves `not_sent` and no possible side effect |
| DOWN-06 | Deadline expires after dispatch with no terminal proof | `indeterminate`; no fallback or automatic mutation retry |
| DOWN-07 | Native ACK arrives without matching readback | `acknowledged_unverified`; projected state unchanged |
| DOWN-08 | Current-generation readback satisfies success predicate | A linked reconciliation record proves `applied`; facts update from readback evidence and the original execution stays immutable |
| DOWN-09 | Native NACK plus current evidence proves no effect | `no_effect`; no success projection |
| LOOP-01 | Matter/eeBUS output returns through another output with the same ancestry | `CAUSAL_ECHO_SUPPRESSED`; no I/O |
| LOOP-02 | Hop count exceeds 8 or echo context expires | Observation may publish; derived intent is rejected |
| LOOP-03 | Independent authorized intent requests the same value | It remains eligible with its own correlation, subject to ordinary checks |
| MIG-01 | Old and semreg paths run on the same fixture | Comparator covers value, exact unit, dimensions, quality, time, provenance, identity, availability, and loss |
| MIG-02 | Gateway rolls back within the supported state window | Previous compatible APIs and persisted state load without mixed contract versions |

## 14. Evidence and completion boundary

Acceptance of this document proves only an implementable gateway-owned contract
against the pinned source baseline and accepted semantic dependency. It does not
prove that the lifecycle service, SPI, semantic publications, output bindings,
vendor profiles, conformance, Portal, HA, deployment, or physical hardware work.

Later implementation must add executable tests for every applicable scenario,
current-source comparators, profile-specific mapping acceptance, public API
parity, and full exact-BOM integration. Live writes, installations, credentials,
and physical control remain action-time operator decisions.
