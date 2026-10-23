// Package semanticoperations routes one generic semantic operation request
// through SemReg operation admission to the single executor injected by the
// qualified native owner of the admitted route. It implements the bounded
// dispatcher of the public semantic operation route contract
// (helianthus-docs-ebus api/semantic-operation-route.md). It owns no native
// handle, transport, opcode, receipt or authorization tier: admission belongs to
// the SemReg kernel, intent identity to the runtime intent owner, and native
// execution, ACK and readback evidence to the injected executor.
package semanticoperations

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/operation"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/infrastructure"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/storage"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
)

// API resource bounds of the route contract. They are not native control ranges.
const (
	MaxRequestBytes  = 65536
	MaxOpaqueIDBytes = 256
	MaxArguments     = 64
	MaxPreconditions = 64
	MinTimeout       = time.Millisecond
	MaxTimeout       = 5000 * time.Millisecond
)

// Code is the closed request-level error classification. Domain admission
// failures are never errors: they are projected as a failed Result.
type Code string

const (
	CodeInvalidArgument   Code = "INVALID_ARGUMENT"
	CodeResourceExhausted Code = "RESOURCE_EXHAUSTED"
	CodeUnavailable       Code = "UNAVAILABLE"
)

// Error is a request-level refusal. Every Error is returned before any
// executor invocation, so it always means no send.
type Error struct {
	Code   Code
	Detail string
}

func (e *Error) Error() string {
	if e == nil {
		return "semantic operation: <nil>"
	}
	return "semantic operation: " + string(e.Code) + ": " + e.Detail
}

func invalid(detail string) error   { return &Error{Code: CodeInvalidArgument, Detail: detail} }
func exhausted(detail string) error { return &Error{Code: CodeResourceExhausted, Detail: detail} }
func unavailable(detail string) error {
	return &Error{Code: CodeUnavailable, Detail: detail}
}

// Request is the closed, typed semantic operation input. Every identity is an
// existing opaque SemReg value compared byte-for-byte; BindingID is a match
// constraint on the admitted route, never a locator or route override.
type Request struct {
	AssetID                            semreg.AssetID
	BindingID                          semreg.NativeBindingID
	CapabilityInstanceID               semreg.CapabilityInstanceID
	Operation                          semreg.DefinitionRef
	Arguments                          []semreg.TypedField
	ExpectedSemanticRevision           semreg.Uint64
	ExpectedCapabilityRevision         semreg.Uint64
	ExpectedCapabilityInstanceRevision semreg.Uint64
	ExpectedSourceEpochID              semreg.SourceEpochID
	ExpectedDriverGeneration           semreg.Uint64
	Preconditions                      []operation.Precondition
	Timeout                            time.Duration
}

// Validate applies the route contract's request bounds and the existing SemReg
// typed validation of every request-owned member. Bound overflow is
// RESOURCE_EXHAUSTED; every other defect is INVALID_ARGUMENT.
func (r Request) Validate() error {
	if len(r.Arguments) > MaxArguments {
		return exhausted("arguments exceed the entry bound")
	}
	if len(r.Preconditions) > MaxPreconditions {
		return exhausted("preconditions exceed the entry bound")
	}
	if overlongIdentifier(reflect.ValueOf(r)) {
		return exhausted("opaque identifier exceeds the byte bound")
	}
	for _, check := range []error{
		r.AssetID.Validate(), r.BindingID.Validate(), r.CapabilityInstanceID.Validate(),
		r.Operation.Validate(), r.ExpectedSourceEpochID.Validate(),
	} {
		if check != nil {
			return invalid("identity")
		}
	}
	for _, revision := range []semreg.Uint64{
		r.ExpectedSemanticRevision, r.ExpectedCapabilityRevision,
		r.ExpectedCapabilityInstanceRevision, r.ExpectedDriverGeneration,
	} {
		if !positiveUint64(revision) {
			return invalid("revision")
		}
	}
	if r.Arguments == nil {
		return invalid("arguments missing")
	}
	for index, argument := range r.Arguments {
		if argument.Validate() != nil {
			return invalid("argument")
		}
		if index > 0 && strings.Compare(string(r.Arguments[index-1].ID), string(argument.ID)) >= 0 {
			return invalid("arguments must be strictly ordered by field id")
		}
	}
	if r.Preconditions == nil {
		return invalid("preconditions missing")
	}
	for index, precondition := range r.Preconditions {
		if precondition.Validate() != nil {
			return invalid("precondition")
		}
		if index > 0 {
			order, ok := comparePreconditions(r.Preconditions[index-1], precondition)
			if !ok || order >= 0 {
				return invalid("preconditions must be strictly ordered by fact, candidate and revision")
			}
		}
	}
	if r.Timeout < MinTimeout || r.Timeout > MaxTimeout {
		return invalid("timeout out of range")
	}
	return nil
}

// overlongIdentifier walks the request and reports any SemReg identifier value
// (a SemReg string type named *ID) longer than MaxOpaqueIDBytes.
func overlongIdentifier(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		return !value.IsNil() && overlongIdentifier(value.Elem())
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if overlongIdentifier(value.Field(index)) {
				return true
			}
		}
	case reflect.Slice:
		for index := 0; index < value.Len(); index++ {
			if overlongIdentifier(value.Index(index)) {
				return true
			}
		}
	case reflect.String:
		return IsIdentifierType(value.Type()) && len(value.String()) > MaxOpaqueIDBytes
	}
	return false
}

var semregPackagePath = reflect.TypeOf(semreg.AssetID("")).PkgPath()

// IsIdentifierType reports whether a SemReg string type is an opaque or
// definition identifier subject to MaxOpaqueIDBytes.
func IsIdentifierType(kind reflect.Type) bool {
	return kind.Kind() == reflect.String && kind.PkgPath() == semregPackagePath && strings.HasSuffix(kind.Name(), "ID")
}

func positiveUint64(value semreg.Uint64) bool {
	if value.Validate() != nil {
		return false
	}
	n, ok := new(big.Int).SetString(string(value), 10)
	return ok && n.Sign() > 0
}

// comparePreconditions mirrors the SemReg intent collection order:
// canonical fact key, candidate id, then numeric candidate revision.
func comparePreconditions(left, right operation.Precondition) (int, bool) {
	leftFact, leftErr := semreg.CanonicalJSON(left.Fact)
	rightFact, rightErr := semreg.CanonicalJSON(right.Fact)
	if leftErr != nil || rightErr != nil {
		return 0, false
	}
	if order := bytes.Compare(leftFact, rightFact); order != 0 {
		return order, true
	}
	if order := strings.Compare(string(left.CandidateID), string(right.CandidateID)); order != 0 {
		return order, true
	}
	a, aok := new(big.Int).SetString(string(left.CandidateRevision), 10)
	b, bok := new(big.Int).SetString(string(right.CandidateRevision), 10)
	if !aok || !bok {
		return 0, false
	}
	return a.Cmp(b), true
}

// Status is the API projection of the evidence, never a new SemReg outcome.
type Status string

const (
	// StatusAccepted reports admission and executor match only: never sent,
	// executed, protocol-acknowledged or readback-confirmed.
	StatusAccepted          Status = "accepted"
	StatusProtocolAck       Status = "protocol_ack"
	StatusReadbackConfirmed Status = "readback_confirmed"
	StatusFailed            Status = "failed"
	StatusUnknown           Status = "unknown"
)

// Result is the closed projection. A nil member was not evidenced.
type Result struct {
	Status           Status
	KernelOutcome    *operation.Outcome
	Delivery         *operation.DeliveryState
	Acknowledgement  *operation.AckState
	ReadbackRelation *operation.ReadbackRelation
	Reason           *string
}

// Router-owned sanitized reasons. Kernel rejections keep their SemReg error
// identifier; capture refusals keep the shared read contract's reason.
const (
	ReasonExecutorMissing          = "semantic.operation.reason.executor_missing"
	ReasonBindingMismatch          = "semantic.operation.reason.binding_mismatch"
	ReasonRouteMismatch            = "semantic.operation.reason.route_mismatch"
	ReasonAdmissionReplayed        = "semantic.operation.reason.admission_replayed"
	ReasonSnapshotUnavailable      = "semantic.operation.reason.snapshot_unavailable"
	ReasonEvidenceTimeout          = "semantic.operation.reason.evidence_timeout"
	ReasonExecutionEvidenceMissing = "semantic.operation.reason.execution_evidence_missing"
	ReasonExecutionEvidenceInvalid = "semantic.operation.reason.execution_evidence_invalid"
)

var captureReasons = map[string]bool{
	"semantic.read.reason.asset_unknown":        true,
	"semantic.read.reason.snapshot_unavailable": true,
	"semantic.read.reason.source_unavailable":   true,
	"semantic.read.reason.publication_withheld": true,
}

// SnapshotCapture is the shared coherent capture: it selects only the current
// admitted epoch and generation of each asset and applies public-exposure and
// lifecycle gates before returning. It performs no native acquisition.
type SnapshotCapture interface {
	Capture(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error)
}

// IntentOwner is the existing runtime/kernel integration that owns the intent
// members this request does not carry: intent identity, pack-owned expected
// effect, required-capability definition and version range, authority
// evidence, causal context, idempotency key and deadline. It must return the
// draft's request-owned members unchanged and must not fabricate members it
// cannot own. budget is the request's wait limit: the router never waits past
// it, and an earlier owner deadline shortens the wait; the request never
// extends the owner deadline. Without an owner the route is unwired and
// answers UNAVAILABLE with no send.
type IntentOwner interface {
	CompleteIntent(ctx context.Context, draft operation.Intent, budget time.Time) (operation.Intent, operation.AuthorityResolver, error)
}

// Dispatch is the immutable admitted work handed to the qualified executor.
type Dispatch struct {
	Intent           operation.Intent
	Route            operation.Route
	Guard            operation.GuardClaims
	AdmittedAt       semreg.TimePoint
	AdmittedRevision semreg.RevisionVector
}

// ExecutionRecord returns a terminal record skeleton bound to this admission.
// The executor supplies dispatch, acknowledgement, readback, outcome and
// outcome evidence; it never receives evidence invented by the router.
func (d Dispatch) ExecutionRecord() operation.ExecutionRecord {
	at, revision, route := d.AdmittedAt, d.AdmittedRevision, d.Route
	return operation.ExecutionRecord{
		Contract: operation.ContractOperationV1, Intent: cloneIntent(d.Intent),
		AdmittedAt: &at, AdmittedRevision: &revision, Route: &route,
	}
}

// Completion is the native owner's evidence. A nil Record means the owner
// accepted the dispatch without terminal evidence yet. ReadbackSnapshot is the
// exact later snapshot named by Record.Readback, when present. An Execute
// error discards the completion: it proves nothing about delivery.
type Completion struct {
	Record           *operation.ExecutionRecord
	ReadbackSnapshot *semreg.Snapshot
}

// Executor is injected by the qualified native driver owner for one admitted
// route and exact operation. Immediately before any native invocation it must
// recheck Dispatch.Guard under its own lifecycle guard; a retired or changed
// route returns a failed_no_contact record with not-sent dispatch evidence.
// It owns its ACK and readback evidence and must honor ctx.
type Executor interface {
	Execute(ctx context.Context, dispatch Dispatch) (Completion, error)
}

// ExecutorLookup selects the executor injected for exactly one admitted route
// and operation. It performs no transport factory, probe or discovery.
type ExecutorLookup interface {
	Lookup(route operation.Route, kind semreg.DefinitionRef) (Executor, bool)
}

type noExecutors struct{}

func (noExecutors) Lookup(operation.Route, semreg.DefinitionRef) (Executor, bool) { return nil, false }

// NoExecutors is the default lookup: it holds no send-capable executor.
func NoExecutors() ExecutorLookup { return noExecutors{} }

// NewBuiltinKernel returns the SemReg operation kernel over the accepted
// built-in pack validators, with the kernel's default bounded retention.
func NewBuiltinKernel() (*operation.Kernel, error) {
	return operation.NewKernel(thermal.New(), pv.New(), storage.New(), evse.New(), infrastructure.New())
}

// Config composes a Router. Snapshots, Kernel and Intents are required for a
// wired route; a nil Executors lookup holds no executor.
type Config struct {
	Snapshots SnapshotCapture
	Kernel    *operation.Kernel
	Intents   IntentOwner
	Executors ExecutorLookup
	Now       func() time.Time
}

// Router is the bounded dispatcher. It is safe for concurrent use. It keeps
// every admission it has handed to an executor, so an identical replay never
// dispatches that admission again, whether or not terminal evidence was
// recorded. The set is bounded by the kernel's own retention: the kernel never
// evicts an admission and refuses new ones at its bound.
type Router struct {
	snapshots SnapshotCapture
	kernel    *operation.Kernel
	intents   IntentOwner
	executors ExecutorLookup
	now       func() time.Time

	mu         sync.Mutex
	dispatched map[*operation.Admission]struct{}
}

// NewRouter composes a router. Typed-nil dependencies count as absent.
func NewRouter(config Config) *Router {
	router := &Router{
		kernel:     config.Kernel,
		now:        config.Now,
		dispatched: map[*operation.Admission]struct{}{},
	}
	if !isNil(config.Snapshots) {
		router.snapshots = config.Snapshots
	}
	if !isNil(config.Intents) {
		router.intents = config.Intents
	}
	router.executors = NoExecutors()
	if !isNil(config.Executors) {
		router.executors = config.Executors
	}
	if router.now == nil {
		router.now = time.Now
	}
	return router
}

// Route validates, admits and dispatches one request. Request-level refusals
// are *Error values; domain admission and execution evidence are a Result.
func (r *Router) Route(ctx context.Context, request Request) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := request.Validate(); err != nil {
		return Result{}, err
	}
	if r == nil || r.snapshots == nil || r.kernel == nil || r.intents == nil {
		return Result{}, unavailable("operation provider unwired")
	}
	deadline := r.now().Add(request.Timeout)
	if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	draft := request.draftIntent()
	intent, authority, err := r.intents.CompleteIntent(ctx, cloneIntent(draft), deadline)
	if err != nil || isNil(authority) {
		return Result{}, unavailable("intent owner cannot construct the intent")
	}
	if !sameRequestMembers(draft, intent) {
		return Result{}, unavailable("intent owner altered request-owned members")
	}
	if err := intent.Validate(); err != nil && !causalDomainError(err) {
		return Result{}, unavailable("intent owner produced an invalid intent")
	}
	ownerDeadline, ok := wallTime(intent.Deadline)
	if !ok {
		return Result{}, unavailable("intent owner produced an invalid deadline")
	}
	if ownerDeadline.Before(deadline) {
		var cancelOwner context.CancelFunc
		ctx, cancelOwner = context.WithDeadline(ctx, ownerDeadline)
		defer cancelOwner()
	}

	_, inputs, err := r.snapshots.Capture(ctx, []semreg.AssetID{request.AssetID})
	if err != nil {
		if ctx.Err() != nil {
			return refused(string(semreg.DeadlineExpired)), nil
		}
		return Result{}, unavailable("current snapshot capture unavailable")
	}
	if len(inputs) != 1 || inputs[0].AssetID != request.AssetID {
		return Result{}, unavailable("current snapshot capture returned another selection")
	}
	input := inputs[0]
	if input.UnavailableReason != "" {
		if !captureReasons[input.UnavailableReason] {
			return refused(ReasonSnapshotUnavailable), nil
		}
		return refused(input.UnavailableReason), nil
	}
	if input.Snapshot == nil || input.Evaluation == nil {
		return refused(ReasonSnapshotUnavailable), nil
	}

	admission, err := r.kernel.Admit(*input.Snapshot, input.Evaluation.Context, intent, authority)
	if err != nil {
		id := semreg.ErrorIdentifier(err)
		// The owning rejection contract records the stable rejection; a record
		// failure changes nothing about the no-send projection.
		if recorded, recordErr := r.kernel.RecordRejection(intent, id, []semreg.EvidenceRef{}); recordErr == nil {
			return project(recorded), nil
		}
		return refused(string(id)), nil
	}
	route, hasRoute := admission.Route()
	if hasRoute && route.BindingID != request.BindingID {
		return refused(ReasonBindingMismatch), nil
	}
	if recorded, ok := admission.Recorded(); ok {
		return project(recorded), nil
	}
	guard, hasGuard := admission.GuardClaims()
	admittedAt, admittedRevision, admitted := admission.AdmittedAt()
	if !hasRoute || !hasGuard || !admitted {
		return refused(ReasonRouteMismatch), nil
	}
	if admittedAt != input.Evaluation.Context.EvaluatedAt {
		// Identical intent bytes returned an earlier admission that was not
		// re-evaluated against this capture. Never dispatch it again.
		return r.replayed(admission), nil
	}
	if route.CapabilityInstance != request.CapabilityInstanceID || route.SourceEpochID != request.ExpectedSourceEpochID || route.DriverGeneration != request.ExpectedDriverGeneration {
		return refused(ReasonRouteMismatch), nil
	}
	executor, ok := r.executors.Lookup(route, intent.Kind)
	if !ok || isNil(executor) {
		return refused(ReasonExecutorMissing), nil
	}
	if ctx.Err() != nil {
		return refused(string(semreg.DeadlineExpired)), nil
	}
	if !r.claim(admission) {
		return r.replayed(admission), nil
	}
	dispatch := Dispatch{Intent: admission.Intent(), Route: route, Guard: guard, AdmittedAt: admittedAt, AdmittedRevision: admittedRevision}
	outcome := make(chan Result, 1)
	go func() { outcome <- r.execute(ctx, executor, admission, dispatch) }()
	select {
	case result := <-outcome:
		return result, nil
	case <-ctx.Done():
		// Evidence that completed together with the deadline still wins.
		select {
		case result := <-outcome:
			return result, nil
		default:
			return unknown(ReasonEvidenceTimeout), nil
		}
	}
}

// execute runs the executor and validates its terminal evidence through the
// kernel. It completes even after the bounded wait expired, so late evidence
// still becomes the admission's immutable record that a replay projects.
func (r *Router) execute(ctx context.Context, executor Executor, admission *operation.Admission, dispatch Dispatch) (result Result) {
	// A panicking executor may already have dispatched: like an executor
	// error, it proves nothing about delivery.
	defer func() {
		if recover() != nil {
			result = unknown(ReasonExecutionEvidenceMissing)
		}
	}()
	completion, err := executor.Execute(ctx, dispatch)
	switch {
	case err != nil:
		return unknown(ReasonExecutionEvidenceMissing)
	case completion.Record == nil:
		return Result{Status: StatusAccepted}
	}
	recorded, err := r.kernel.Record(admission, *completion.Record, completion.ReadbackSnapshot)
	if err != nil {
		return unknown(ReasonExecutionEvidenceInvalid)
	}
	return project(recorded)
}

// claim marks the admission dispatched; it reports false when it already was.
func (r *Router) claim(admission *operation.Admission) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dispatched := r.dispatched[admission]; dispatched {
		return false
	}
	r.dispatched[admission] = struct{}{}
	return true
}

// replayed answers an admission that must not be dispatched again: its
// recorded evidence, admission only while a dispatch has no terminal record,
// or a no-send refusal for an earlier admission that was never dispatched.
func (r *Router) replayed(admission *operation.Admission) Result {
	if recorded, ok := admission.Recorded(); ok {
		return project(recorded)
	}
	r.mu.Lock()
	_, dispatched := r.dispatched[admission]
	r.mu.Unlock()
	if dispatched {
		return Result{Status: StatusAccepted}
	}
	return refused(ReasonAdmissionReplayed)
}

// project maps one kernel-validated record to the API result. Validated
// terminal outcomes take precedence over ACK-only classification.
func project(record operation.ExecutionRecord) Result {
	outcome := record.Outcome
	result := Result{KernelOutcome: &outcome}
	if record.Dispatch != nil {
		delivery := record.Dispatch.Delivery
		result.Delivery = &delivery
	}
	if record.Acknowledgement != nil {
		state := record.Acknowledgement.State
		result.Acknowledgement = &state
	}
	if record.Readback != nil {
		relation := record.Readback.Relation
		result.ReadbackRelation = &relation
	}
	switch outcome {
	case operation.OutcomeApplied:
		result.Status = StatusReadbackConfirmed
	case operation.OutcomeAcknowledgedUnverified:
		result.Status = StatusProtocolAck
	case operation.OutcomeIndeterminate:
		result.Status = StatusUnknown
	case operation.OutcomeRejected:
		result.Status = StatusFailed
		notSent := operation.DeliveryNotSent
		result.Delivery = &notSent
		if record.ErrorID != nil {
			reason := string(*record.ErrorID)
			result.Reason = &reason
		}
	default:
		result.Status = StatusFailed
	}
	return result
}

// refused is an admission-level no-send statement: failed, rejected, not sent.
func refused(reason string) Result {
	outcome, delivery := operation.OutcomeRejected, operation.DeliveryNotSent
	return Result{Status: StatusFailed, KernelOutcome: &outcome, Delivery: &delivery, Reason: &reason}
}

// unknown follows a possible dispatch without validated evidence. Nothing
// about delivery, ACK or readback is evidenced, so those members stay null.
func unknown(reason string) Result {
	return Result{Status: StatusUnknown, Reason: &reason}
}

func (r Request) draftIntent() operation.Intent {
	instance := r.CapabilityInstanceID
	return operation.Intent{
		Contract:                           operation.ContractOperationV1,
		Kind:                               r.Operation,
		AssetID:                            r.AssetID,
		Arguments:                          append([]semreg.TypedField{}, r.Arguments...),
		RequiredCapability:                 operation.CapabilityRequirement{InstanceID: &instance},
		ExpectedSemanticRevision:           r.ExpectedSemanticRevision,
		ExpectedCapabilityRevision:         r.ExpectedCapabilityRevision,
		ExpectedCapabilityInstanceRevision: r.ExpectedCapabilityInstanceRevision,
		ExpectedSourceEpochID:              r.ExpectedSourceEpochID,
		ExpectedDriverGeneration:           r.ExpectedDriverGeneration,
		Preconditions:                      append([]operation.Precondition{}, r.Preconditions...),
	}
}

type requestMembers struct {
	Contract                           semreg.ContractVersion       `json:"contract"`
	Kind                               semreg.DefinitionRef         `json:"kind"`
	AssetID                            semreg.AssetID               `json:"asset_id"`
	Arguments                          []semreg.TypedField          `json:"arguments"`
	Instance                           *semreg.CapabilityInstanceID `json:"instance"`
	ExpectedSemanticRevision           semreg.Uint64                `json:"expected_semantic_revision"`
	ExpectedCapabilityRevision         semreg.Uint64                `json:"expected_capability_revision"`
	ExpectedCapabilityInstanceRevision semreg.Uint64                `json:"expected_capability_instance_revision"`
	ExpectedSourceEpochID              semreg.SourceEpochID         `json:"expected_source_epoch_id"`
	ExpectedDriverGeneration           semreg.Uint64                `json:"expected_driver_generation"`
	Preconditions                      []operation.Precondition     `json:"preconditions"`
}

func membersOf(intent operation.Intent) requestMembers {
	return requestMembers{
		Contract: intent.Contract, Kind: intent.Kind, AssetID: intent.AssetID, Arguments: intent.Arguments,
		Instance: intent.RequiredCapability.InstanceID, ExpectedSemanticRevision: intent.ExpectedSemanticRevision,
		ExpectedCapabilityRevision: intent.ExpectedCapabilityRevision, ExpectedCapabilityInstanceRevision: intent.ExpectedCapabilityInstanceRevision,
		ExpectedSourceEpochID: intent.ExpectedSourceEpochID, ExpectedDriverGeneration: intent.ExpectedDriverGeneration,
		Preconditions: intent.Preconditions,
	}
}

func sameRequestMembers(draft, completed operation.Intent) bool {
	want, wantErr := json.Marshal(membersOf(draft))
	got, gotErr := json.Marshal(membersOf(completed))
	return wantErr == nil && gotErr == nil && bytes.Equal(want, got)
}

// causalDomainError identifies the intent validation outcomes that the kernel
// reports as domain admission rejections rather than malformed intents.
func causalDomainError(err error) bool {
	switch semreg.ErrorIdentifier(err) {
	case semreg.CausalBudgetExceeded, semreg.EchoSuppressed:
		return true
	}
	return false
}

func wallTime(point semreg.TimePoint) (time.Time, bool) {
	if point.ClockID != "clock.utc" {
		return time.Time{}, false
	}
	nanoseconds, err := strconv.ParseInt(string(point.UnixNanoseconds), 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, nanoseconds), true
}

func cloneIntent(intent operation.Intent) operation.Intent { return clone(intent) }

func clone[T any](value T) T {
	raw, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var copied T
	if json.Unmarshal(raw, &copied) != nil {
		return value
	}
	return copied
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	}
	return false
}
