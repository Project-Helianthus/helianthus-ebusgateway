package semhub

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// ControlSettleTimeout bounds how long Activate waits for the reprojection of
// a settling control operation.
const ControlSettleTimeout = 5 * time.Second

var settlePollInterval = 10 * time.Millisecond

var (
	// ErrSemanticOutstanding reports a Resettle of an asset whose semantic
	// control outcome is not accepted or replayed. Nothing changes.
	ErrSemanticOutstanding = errors.New("semhub: semantic control outcome outstanding")
	// ErrControlRequest reports a control request refused before any asset
	// is processed. Nothing changes.
	ErrControlRequest = errors.New("semhub: invalid control request")
	// ErrControlAbandoned reports an activation behind an abandoned control
	// operation: the affected assets stay closed until a successful new
	// control operation.
	ErrControlAbandoned = errors.New("semhub: control operation abandoned")
	// ErrGenerationLive reports an activation of a source whose current
	// generation was never fenced.
	ErrGenerationLive = errors.New("semhub: current generation is not fenced")
	errNoPublication  = errors.New("semhub: no publication for the control key")
	// ErrReprojectionInFlight reports a Resettle while a reprojection attempt
	// of the same asset is still running. Nothing changes.
	ErrReprojectionInFlight = errors.New("semhub: reprojection attempt in flight")
	// ErrReprojectionQueued reports a Resettle whose attempt was queued
	// behind the running scheduler; it runs in HubSeq order.
	ErrReprojectionQueued = errors.New("semhub: reprojection attempt queued")
)

// ControlID identifies one control operation of one source.
type ControlID struct {
	Incarnation string          `json:"incarnation"`
	Source      semreg.SourceID `json:"source"`
	N           uint64          `json:"n"`
}

// ControlPendingError reports a mutating request during an unfinished
// control operation. It changes nothing and emits nothing.
type ControlPendingError struct{ ID ControlID }

func (e *ControlPendingError) Error() string {
	return fmt.Sprintf("semhub: control operation %d of the source is unfinished", e.ID.N)
}

// SettlePendingError reports a mutating request while a settling control
// operation still has an unsettled reprojection after one Resettle pass.
type SettlePendingError struct{ ID ControlID }

func (e *SettlePendingError) Error() string {
	return fmt.Sprintf("semhub: control operation %d of the source is settling", e.ID.N)
}

// SettleTimeoutError reports an activation that waited ControlSettleTimeout
// for reprojection to settle. Nothing changes and no ControlFailed is emitted.
type SettleTimeoutError struct{ ID ControlID }

func (e *SettleTimeoutError) Error() string {
	return fmt.Sprintf("semhub: control operation %d of the source did not settle", e.ID.N)
}

// SemanticState is the semantic outcome of one asset in a control operation.
type SemanticState string

const (
	SemanticUnbuilt            SemanticState = "unbuilt"
	SemanticPending            SemanticState = "pending"
	SemanticAccepted           SemanticState = "accepted"
	SemanticReplayed           SemanticState = "replayed"
	SemanticRejected           SemanticState = "rejected"
	SemanticUncertain          SemanticState = "uncertain"
	SemanticRevokedUnpublished SemanticState = "revoked_unpublished"
)

// ReprojectionState is the reprojection outcome of one asset.
type ReprojectionState string

const (
	ReprojectionUnsettled ReprojectionState = "unsettled"
	ReprojectionSettled   ReprojectionState = "settled"
)

// ControlKind names one control operation kind.
type ControlKind string

const (
	ControlFence  ControlKind = "fence"
	ControlRetire ControlKind = "retire"
)

// FenceRequest fences one generation on every asset of one source.
type FenceRequest struct {
	Source     semreg.SourceID
	Epoch      semreg.SourceEpochID
	Generation uint64
	Reason     semreg.DefinitionID
	Evidence   []semreg.EvidenceRef
	ObservedAt semreg.TimePoint
	Monotonic  semreg.MonotonicPoint
	// BatchID overrides the hub-derived batch identifier.
	BatchID semreg.BatchID
}

// RetireRequest retires one epoch on every asset of one source. The reason
// is event metadata only: SourceRetirements carries epochs on the wire.
type RetireRequest struct {
	Source           semreg.SourceID
	Epoch            semreg.SourceEpochID
	Reason           semreg.DefinitionID
	Evidence         []semreg.EvidenceRef
	ObservedAt       semreg.TimePoint
	Monotonic        semreg.MonotonicPoint
	BatchID          semreg.BatchID
	RetirementReason semreg.DefinitionID
}

// Attempt is one retained control batch: its exact bytes, sequence and digest.
type Attempt struct {
	Batch    []byte
	Sequence semreg.Uint64
	Digest   semreg.Digest
}

// AssetControlResult is the per-asset result of a control operation.
type AssetControlResult struct {
	Asset      semreg.AssetID
	Outcome    SemanticState
	SnapshotID semreg.SnapshotID
	HubSeq     uint64
	Err        error
}

// ControlResult lists every asset of a control operation in bytewise order.
type ControlResult struct {
	ID     ControlID
	Kind   ControlKind
	Assets []AssetControlResult
}

// ControlStatus is the control view of one asset.
type ControlStatus struct {
	ID           *ControlID        `json:"id,omitempty"`
	Semantic     SemanticState     `json:"semantic,omitempty"`
	Reprojection ReprojectionState `json:"reprojection,omitempty"`
	ErrorID      semreg.ErrorID    `json:"error_id,omitempty"`
	Attempts     int               `json:"attempts,omitempty"`
}

// InspectView is an ungated, read-only deep copy of one asset's current
// snapshot. It is an internal seam for reprojection, validation and tests
// and is never returned by a public surface.
type InspectView struct {
	Snapshot  semreg.Snapshot
	Canonical []byte
	Revisions semreg.RevisionVector
	HubSeq    uint64
	Control   ControlStatus
	Present   bool
}

// ControlEvent is passed to a declaration's OnControl callback.
type ControlEvent struct {
	Asset      semreg.AssetID
	SnapshotID semreg.SnapshotID
	HubSeq     uint64
	Kind       EventKind
	Control    ControlID
	View       InspectView
}

type controlOp struct {
	id         ControlID
	kind       ControlKind
	src        *source
	epoch      semreg.SourceEpochID
	generation uint64
	reason     semreg.DefinitionID
	metadata   semreg.DefinitionID
	evidence   []semreg.EvidenceRef
	observedAt semreg.TimePoint
	monotonic  semreg.MonotonicPoint
	batchID    semreg.BatchID
	assets     []*controlAsset
	abandoned  bool
}

type controlAsset struct {
	op           *controlOp
	asset        *asset
	semantic     SemanticState
	errID        semreg.ErrorID
	err          error
	attempts     []Attempt
	reprojection ReprojectionState
	reprojErr    error
	hubSeq       uint64
	snapshot     semreg.Snapshot
	canonical    []byte
	inFlight     chan struct{}
	entry        *reprojectionEntry
}

// reprojectionEntry is one scheduled reprojection attempt. done closes when
// the attempt has run (or was found unnecessary).
type reprojectionEntry struct {
	ca   *controlAsset
	done chan struct{}
}

// enqueueLocked schedules one attempt for ca, or returns the one already
// queued or running. h.mu must be held.
func (h *Hub) enqueueLocked(ca *controlAsset) *reprojectionEntry {
	if ca.entry != nil {
		return ca.entry
	}
	entry := &reprojectionEntry{ca: ca, done: make(chan struct{})}
	ca.entry = entry
	h.queue = append(h.queue, entry)
	return entry
}

func semanticSuccess(state SemanticState) bool {
	return state == SemanticAccepted || state == SemanticReplayed
}

type opState int

const (
	opFinished opState = iota
	opSettling
	opUnfinished
)

// stateLocked derives the aggregate state of one operation. Unresolved
// semantic outcomes take precedence over reprojection. h.mu must be held.
func (op *controlOp) stateLocked() opState {
	if op == nil {
		return opFinished
	}
	settling := false
	for _, ca := range op.assets {
		switch {
		case ca.semantic == SemanticRevokedUnpublished:
		case semanticSuccess(ca.semantic):
			if ca.reprojection != ReprojectionSettled {
				settling = true
			}
		default:
			if !op.abandoned {
				return opUnfinished
			}
		}
	}
	if settling {
		return opSettling
	}
	return opFinished
}

func (op *controlOp) abandonedOpenLocked() bool {
	if op == nil || !op.abandoned {
		return false
	}
	for _, ca := range op.assets {
		if ca.semantic != SemanticRevokedUnpublished && !semanticSuccess(ca.semantic) && ca.asset.gate == ca {
			return true
		}
	}
	return false
}

// ReadAdmitted reports whether public reads of the asset are admitted by the
// control gate: no control operation, or a terminal successful one.
func (h *Hub) ReadAdmitted(id semreg.AssetID) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	target := h.assets[id]
	if target == nil {
		return false
	}
	gate := target.gate
	return gate == nil || semanticSuccess(gate.semantic) && gate.reprojection == ReprojectionSettled
}

// Settled reports whether the asset's family cache acknowledged the latest
// control snapshot.
func (h *Hub) Settled(id semreg.AssetID) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	target := h.assets[id]
	if target == nil {
		return false
	}
	return target.gate == nil || target.gate.reprojection == ReprojectionSettled
}

// Readable is ReadAdmitted and Settled, the condition of every public read.
func (h *Hub) Readable(id semreg.AssetID) bool {
	return h.ReadAdmitted(id) && h.Settled(id)
}

// Inspect returns an ungated deep copy of the asset's current snapshot.
func (h *Hub) Inspect(id semreg.AssetID) (InspectView, error) {
	h.mu.RLock()
	target := h.assets[id]
	if target == nil {
		h.mu.RUnlock()
		return InspectView{}, ErrUnknownAsset
	}
	kernel, hubSeq, status := target.kernel, target.hubSeq, controlStatusLocked(target.gate)
	h.mu.RUnlock()
	snapshot, canonical, ok := kernel.Current()
	return InspectView{Snapshot: snapshot, Canonical: canonical, Revisions: snapshot.Revisions, HubSeq: hubSeq, Control: status, Present: ok}, nil
}

func controlStatusLocked(ca *controlAsset) ControlStatus {
	if ca == nil {
		return ControlStatus{}
	}
	id := ca.op.id
	return ControlStatus{ID: &id, Semantic: ca.semantic, Reprojection: ca.reprojection, ErrorID: ca.errID, Attempts: len(ca.attempts)}
}

func validateControlEvidence(reason semreg.DefinitionID, evidence []semreg.EvidenceRef, observed semreg.TimePoint, monotonic semreg.MonotonicPoint, batchID semreg.BatchID) error {
	if reason.Validate() != nil || len(evidence) < 1 || len(evidence) > 32 || observed.Validate() != nil || monotonic.Validate() != nil {
		return ErrControlRequest
	}
	for _, ref := range evidence {
		if ref.Validate() != nil {
			return ErrControlRequest
		}
	}
	if batchID != "" && batchID.Validate() != nil {
		return ErrControlRequest
	}
	return nil
}

// Fence fences one generation of one source on every asset that published
// under it. The whole request is validated against every asset before any
// asset commits; assets without a publication for the generation are revoked
// at the hub level without a SemReg batch.
func (h *Hub) Fence(request FenceRequest) (ControlResult, error) {
	if request.Generation == 0 || ValidateOpaque(string(request.Epoch)) != nil {
		return ControlResult{}, ErrControlRequest
	}
	if err := validateControlEvidence(request.Reason, request.Evidence, request.ObservedAt, request.Monotonic, request.BatchID); err != nil {
		return ControlResult{}, err
	}
	op := &controlOp{kind: ControlFence, epoch: request.Epoch, generation: request.Generation, reason: request.Reason, evidence: append([]semreg.EvidenceRef(nil), request.Evidence...), observedAt: request.ObservedAt, monotonic: request.Monotonic, batchID: request.BatchID}
	return h.control(request.Source, op)
}

// RetireEpoch retires one epoch of one source on every asset that holds it as
// current. RetirementReason is event and diagnostic metadata only.
func (h *Hub) RetireEpoch(request RetireRequest) (ControlResult, error) {
	if ValidateOpaque(string(request.Epoch)) != nil || (request.RetirementReason != "" && request.RetirementReason.Validate() != nil) {
		return ControlResult{}, ErrControlRequest
	}
	if err := validateControlEvidence(request.Reason, request.Evidence, request.ObservedAt, request.Monotonic, request.BatchID); err != nil {
		return ControlResult{}, err
	}
	op := &controlOp{kind: ControlRetire, epoch: request.Epoch, reason: request.Reason, metadata: request.RetirementReason, evidence: append([]semreg.EvidenceRef(nil), request.Evidence...), observedAt: request.ObservedAt, monotonic: request.Monotonic, batchID: request.BatchID}
	return h.control(request.Source, op)
}

func (h *Hub) control(id semreg.SourceID, op *controlOp) (ControlResult, error) {
	result, err := h.runControl(id, op)
	if op.id.N == 0 {
		return result, err
	}
	// Reprojection callbacks run after the source control mutex is released,
	// so a callback may itself request control on the same source.
	h.drain()
	return h.result(op), err
}

func (h *Hub) runControl(id semreg.SourceID, op *controlOp) (ControlResult, error) {
	h.mu.RLock()
	src := h.sources[id]
	h.mu.RUnlock()
	if src == nil {
		return ControlResult{}, ErrUnknownSource
	}
	src.control.Lock()
	defer src.control.Unlock()
	if err := h.awaitFinished(src, false); err != nil {
		return ControlResult{}, err
	}
	op.src = src
	h.mu.RLock()
	currentEpoch, currentGeneration, admitted, fenced, retired := src.epoch, src.generation, src.admitted, src.fenced, src.retired[op.epoch]
	assets := make([]*asset, 0, len(src.assets))
	for assetID := range src.assets {
		assets = append(assets, h.assets[assetID])
	}
	h.mu.RUnlock()
	sort.Slice(assets, func(i, j int) bool { return assets[i].id < assets[j].id })
	targetsCurrent := admitted && op.epoch == currentEpoch && (op.kind == ControlRetire || op.generation == currentGeneration)
	if op.kind == ControlFence && admitted && op.epoch == currentEpoch {
		if op.generation > currentGeneration {
			return ControlResult{}, &semreg.Error{ID: semreg.StaleDriverGeneration, Detail: "future generation fence"}
		}
	}
	// Whole-request validation: every published asset must accept its batch
	// on a detached fork before any asset is committed.
	// An asset on which this exact transition already holds (a fenced cursor
	// or a retired epoch) is left out; when every published asset is such an
	// asset, SemReg's own refusal is returned verbatim.
	published := make(map[semreg.AssetID]bool, len(assets))
	var alreadyErr error
	for _, target := range assets {
		attempt, already, err := h.trial(op, target)
		if errors.Is(err, errNoPublication) {
			continue
		}
		if already {
			if alreadyErr == nil {
				alreadyErr = err
			}
			continue
		}
		if err != nil {
			return ControlResult{}, err
		}
		if attempt.Batch != nil {
			published[target.id] = true
		}
	}
	if len(published) == 0 && alreadyErr != nil {
		return ControlResult{}, alreadyErr
	}
	if len(published) == 0 {
		if op.kind == ControlFence && targetsCurrent && fenced {
			return ControlResult{}, &semreg.Error{ID: semreg.StaleDriverGeneration, Detail: "generation fence"}
		}
		if op.kind == ControlRetire && (!targetsCurrent || retired) {
			return ControlResult{}, &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "source retirement"}
		}
	}
	skipped := make(map[semreg.AssetID]bool, len(assets))
	for _, target := range assets {
		if !published[target.id] {
			if _, already, err := h.trial(op, target); already && err != nil {
				skipped[target.id] = true
			}
		}
	}
	h.fault("control-revoke", "")
	h.mu.Lock()
	// Reconcile at the revocation serialization point. Commit validates the
	// handle under h.mu, so no publication can land between this
	// classification and the revocation below.
	assets = assets[:0]
	for assetID := range src.assets {
		assets = append(assets, h.assets[assetID])
	}
	sort.Slice(assets, func(i, j int) bool { return assets[i].id < assets[j].id })
	for _, target := range assets {
		if published[target.id] || skipped[target.id] {
			continue
		}
		if _, batch, err := h.build(op, target, target.kernel); err == nil {
			if snapshot, _, ok := target.kernel.Current(); ok {
				if cursor, found := cursorFor(snapshot, op.src.id, op.epoch, batch.DriverGeneration); found && !cursor.Fenced {
					published[target.id] = true
				}
			}
		}
	}
	src.counter++
	op.id = ControlID{Incarnation: h.incarnation, Source: src.id, N: src.counter}
	for _, target := range assets {
		if skipped[target.id] {
			continue
		}
		ca := &controlAsset{op: op, asset: target, semantic: SemanticUnbuilt, reprojection: ReprojectionSettled}
		if !published[target.id] {
			ca.semantic = SemanticRevokedUnpublished
		} else {
			target.gate = ca
		}
		op.assets = append(op.assets, ca)
	}
	if targetsCurrent {
		src.live = false
		src.token++
		if op.kind == ControlFence {
			src.fenced = true
		} else {
			src.retired[op.epoch] = true
		}
	}
	src.ops[op.id.N] = op
	src.last = op
	h.mu.Unlock()
	for _, ca := range op.assets {
		if ca.semantic == SemanticRevokedUnpublished {
			control := op.id
			h.bus.emit(Event{Kind: EventGenerationRevoked, Asset: ca.asset.id, Source: src.id, Epoch: op.epoch, Generation: op.generation, Control: &control, Reason: op.reason, NoPublication: true})
		}
	}
	if targetsCurrent {
		kind := EventGenerationFenced
		if op.kind == ControlRetire {
			kind = EventSourceRetired
		}
		if err := h.journal.Append(EpochRecord{Incarnation: h.incarnation, Source: src.id, Epoch: op.epoch, Generation: currentGeneration, Kind: kind}); err != nil {
			return h.result(op), err
		}
	}
	failure := h.process(op, false)
	return h.result(op), failure
}

// trial builds the asset's batch from the committed kernel and applies it to
// a detached fork. It takes no asset commit lock, so a staged publication
// cannot delay validation or the handle revocation that follows it. already
// reports that the transition already holds on this asset.
func (h *Hub) trial(op *controlOp, target *asset) (Attempt, bool, error) {
	h.mu.RLock()
	kernel := target.kernel
	h.mu.RUnlock()
	attempt, batch, err := h.build(op, target, kernel)
	if err != nil {
		return Attempt{}, false, err
	}
	already := false
	if snapshot, _, ok := kernel.Current(); ok {
		if op.kind == ControlFence {
			cursor, found := cursorFor(snapshot, op.src.id, op.epoch, batch.DriverGeneration)
			already = found && cursor.Fenced
		} else {
			for _, descriptor := range snapshot.Sources {
				if descriptor.SourceID == op.src.id && descriptor.SourceEpochID == op.epoch && descriptor.State == semreg.SourceRetired {
					already = true
				}
			}
		}
	}
	fork, err := kernel.Fork()
	if err != nil {
		return Attempt{}, false, err
	}
	if _, _, err := fork.Apply(batch, op.monotonic); err != nil {
		return Attempt{}, already, err
	}
	return attempt, false, nil
}

// build constructs one control batch from the current asset snapshot. Every
// object revision is current+1 and the sequence comes from the asset cursor.
func (h *Hub) build(op *controlOp, target *asset, kernel *semreg.PublicationKernel) (Attempt, semreg.PublicationBatch, error) {
	snapshot, _, ok := kernel.Current()
	if !ok {
		return Attempt{}, semreg.PublicationBatch{}, errNoPublication
	}
	generation := semreg.Uint64(strconv.FormatUint(op.generation, 10))
	var cursor semreg.PublicationCursor
	found := false
	if op.kind == ControlFence {
		cursor, found = cursorFor(snapshot, op.src.id, op.epoch, generation)
	} else {
		for _, candidate := range snapshot.Cursors {
			if candidate.SourceID == op.src.id && candidate.SourceEpochID == op.epoch && (!found || compareDecimal(candidate.DriverGeneration, cursor.DriverGeneration) > 0) {
				cursor, found = candidate, true
			}
		}
		generation = cursor.DriverGeneration
	}
	if !found {
		return Attempt{}, semreg.PublicationBatch{}, errNoPublication
	}
	sequence := incrementUint(cursor.LastSequence)
	batchID := op.batchID
	if batchID == "" {
		batchID = semreg.BatchID("batch:semhub:" + idHash("helianthus.semhub.batch.v1", string(target.id), string(op.src.id), string(op.epoch), string(generation), string(sequence)))
	}
	batch := semreg.PublicationBatch{
		Contract: semreg.ContractKernelV1, BatchID: batchID, AssetID: target.id, SourceID: op.src.id, SourceEpochID: op.epoch, DriverGeneration: generation,
		Sequence: sequence, ExpectedSemanticRevision: snapshot.Revisions.Semantic, ObservedAt: op.observedAt,
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: []semreg.SourceEpochID{}, BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{},
		FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{}, ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{},
		CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{},
	}
	if op.kind == ControlFence {
		revision := semreg.Uint64("1")
		for _, fence := range snapshot.Fences {
			if fence.SourceID == op.src.id && fence.SourceEpochID == op.epoch && fence.DriverGeneration == generation {
				revision = incrementUint(fence.Revision)
			}
		}
		batch.GenerationFences = []semreg.GenerationFence{{SourceID: op.src.id, SourceEpochID: op.epoch, DriverGeneration: generation, Reason: op.reason, Evidence: append([]semreg.EvidenceRef(nil), op.evidence...), Revision: revision}}
	} else {
		batch.SourceRetirements = []semreg.SourceEpochID{op.epoch}
	}
	digest, err := batch.ComputedDigest()
	if err != nil {
		return Attempt{}, semreg.PublicationBatch{}, err
	}
	batch.BatchDigest = digest
	encoded, err := json.Marshal(batch)
	if err != nil {
		return Attempt{}, semreg.PublicationBatch{}, err
	}
	return Attempt{Batch: encoded, Sequence: sequence, Digest: digest}, batch, nil
}

func compareDecimal(left, right semreg.Uint64) int {
	l, _ := strconv.ParseUint(string(left), 10, 64)
	r, _ := strconv.ParseUint(string(right), 10, 64)
	switch {
	case l < r:
		return -1
	case l > r:
		return 1
	}
	return 0
}

type submitMode int

const (
	submitBuild submitMode = iota
	submitResubmit
)

// process submits every unresolved asset in bytewise order and stops at the
// first failure; later assets stay unbuilt and closed.
func (h *Hub) process(op *controlOp, retry bool) error {
	for _, ca := range op.assets {
		h.mu.RLock()
		state, errID := ca.semantic, ca.errID
		h.mu.RUnlock()
		mode := submitBuild
		switch state {
		case SemanticAccepted, SemanticReplayed, SemanticRevokedUnpublished:
			continue
		case SemanticRejected:
			if !retry || errID != semreg.RevisionConflict {
				return ca.err
			}
		case SemanticUncertain:
			mode = submitResubmit
		}
		if err := h.submit(ca, mode); err != nil {
			return err
		}
	}
	return nil
}

// submit builds (once per attempt, under the asset lock) or resubmits the
// exact retained bytes, then lets SemReg decide.
func (h *Hub) submit(ca *controlAsset, mode submitMode) (failure error) {
	target, op := ca.asset, ca.op
	target.commit.Lock()
	defer target.commit.Unlock()
	defer func() {
		if recovered := recover(); recovered != nil {
			h.mu.Lock()
			ca.semantic, ca.errID, ca.err = SemanticUncertain, "", fmt.Errorf("semhub: control submission interrupted: %v", recovered)
			failure = ca.err
			h.mu.Unlock()
			control := op.id
			h.bus.emit(Event{Kind: EventControlFailed, Asset: target.id, Source: op.src.id, Epoch: op.epoch, Generation: op.generation, Control: &control})
		}
	}()
	h.mu.RLock()
	kernel := target.kernel
	h.mu.RUnlock()
	if mode == submitBuild {
		attempt, _, err := h.build(op, target, kernel)
		if err != nil {
			return h.reject(ca, err)
		}
		h.mu.Lock()
		ca.attempts = append(ca.attempts, attempt)
		ca.semantic = SemanticPending
		h.mu.Unlock()
	}
	h.mu.RLock()
	attempt := ca.attempts[len(ca.attempts)-1]
	h.mu.RUnlock()
	var batch semreg.PublicationBatch
	if err := json.Unmarshal(attempt.Batch, &batch); err != nil {
		return h.reject(ca, err)
	}
	h.fault("before-submit", target.id)
	h.mu.RLock()
	kernel = target.kernel
	h.mu.RUnlock()
	fork, err := kernel.Fork()
	if err != nil {
		return h.reject(ca, err)
	}
	prior := semreg.PublicationCursor{}
	priorKnown := false
	if snapshot, _, ok := fork.Current(); ok {
		prior, priorKnown = cursorFor(snapshot, batch.SourceID, batch.SourceEpochID, batch.DriverGeneration)
	}
	snapshot, canonical, err := fork.Apply(batch, op.monotonic)
	if err != nil {
		return h.reject(ca, err)
	}
	replayed := priorKnown && prior.LastSequence == batch.Sequence
	h.fault("before-swap", target.id)
	kind := EventGenerationFenced
	reason := op.reason
	if op.kind == ControlRetire {
		kind = EventSourceRetired
		if op.metadata != "" {
			reason = op.metadata
		}
	}
	control := op.id
	generation, _ := strconv.ParseUint(string(batch.DriverGeneration), 10, 64)
	// One serialized transition installs the kernel, the HubSeq, the control
	// status and the queued reprojection, so Inspect never pairs the new
	// snapshot with older metadata.
	func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if !replayed {
			target.kernel = fork
		}
		h.fault("after-swap", target.id)
		seq := h.bus.emit(Event{Kind: kind, Asset: target.id, Source: op.src.id, Epoch: op.epoch, Generation: generation, Sequence: batch.Sequence, SnapshotID: snapshot.SnapshotID, Revisions: &snapshot.Revisions, Digest: batch.BatchDigest, Control: &control, Reason: reason})
		target.hubSeq = seq
		ca.hubSeq, ca.snapshot, ca.canonical = seq, snapshot, canonical
		ca.semantic, ca.errID, ca.err = SemanticAccepted, "", nil
		if replayed {
			ca.semantic = SemanticReplayed
		}
		ca.reprojection, ca.reprojErr = ReprojectionUnsettled, nil
		h.enqueueLocked(ca)
	}()
	h.fault("after-commit", target.id)
	return nil
}

func (h *Hub) reject(ca *controlAsset, err error) error {
	var semErr *semreg.Error
	id := semreg.ErrorID("")
	if errors.As(err, &semErr) {
		id = semErr.ID
	}
	h.mu.Lock()
	ca.semantic, ca.errID, ca.err = SemanticRejected, id, err
	h.mu.Unlock()
	control := ca.op.id
	h.bus.emit(Event{Kind: EventControlFailed, Asset: ca.asset.id, Source: ca.op.src.id, Epoch: ca.op.epoch, Generation: ca.op.generation, Control: &control, ErrorID: id})
	return err
}

// drain is the one reprojection scheduler. It runs queued attempts one at a
// time, in queue order (control commits enqueue in HubSeq order), outside
// every hub and source lock. Only one goroutine drains at a time; a callback
// that triggers control merely enqueues and returns, and the active drainer
// runs the new entries afterwards. An entry whose asset still has an attempt
// in flight waits for it and then runs; it is never dropped.
func (h *Hub) drain() {
	h.mu.Lock()
	if h.draining {
		h.mu.Unlock()
		return
	}
	h.draining = true
	for len(h.queue) > 0 {
		entry := h.queue[0]
		h.queue = h.queue[1:]
		ca := entry.ca
		for ca.inFlight != nil {
			running := ca.inFlight
			h.mu.Unlock()
			<-running
			h.mu.Lock()
		}
		done, _ := h.startAttemptLocked(ca)
		h.mu.Unlock()
		if done != nil {
			<-done
		}
		h.mu.Lock()
		if ca.entry == entry {
			ca.entry = nil
		}
		close(entry.done)
	}
	h.draining = false
	h.mu.Unlock()
}

// startAttemptLocked starts one reprojection attempt for ca in its own
// goroutine and returns its completion channel. At most one attempt per
// control asset is in flight. A nil channel means nothing needs to run.
// h.mu must be held.
func (h *Hub) startAttemptLocked(ca *controlAsset) (chan struct{}, error) {
	if ca.inFlight != nil {
		return nil, ErrReprojectionInFlight
	}
	if !semanticSuccess(ca.semantic) {
		return nil, ErrSemanticOutstanding
	}
	if ca.reprojection == ReprojectionSettled {
		return nil, nil
	}
	target := ca.asset
	event := ControlEvent{Asset: target.id, SnapshotID: ca.snapshot.SnapshotID, HubSeq: ca.hubSeq, Kind: EventGenerationFenced, Control: ca.op.id,
		View: InspectView{Snapshot: cloneSnapshot(ca.snapshot), Canonical: append([]byte(nil), ca.canonical...), Revisions: ca.snapshot.Revisions, HubSeq: ca.hubSeq, Control: controlStatusLocked(ca), Present: true}}
	if ca.op.kind == ControlRetire {
		event.Kind = EventSourceRetired
	}
	callback := target.onControl
	done := make(chan struct{})
	ca.inFlight = done
	go func() {
		err := runCallback(callback, event)
		h.mu.Lock()
		if target.gate == ca && ca.hubSeq == event.HubSeq {
			if err != nil {
				ca.reprojection, ca.reprojErr = ReprojectionUnsettled, err
			} else {
				ca.reprojection, ca.reprojErr = ReprojectionSettled, nil
			}
		}
		ca.inFlight = nil
		h.mu.Unlock()
		close(done)
	}()
	return done, nil
}

func runCallback(callback func(ControlEvent) error, event ControlEvent) (err error) {
	if callback == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("semhub: reprojection callback panicked: %v", recovered)
		}
	}()
	return callback(event)
}

func cloneSnapshot(snapshot semreg.Snapshot) semreg.Snapshot {
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return semreg.Snapshot{}
	}
	var clone semreg.Snapshot
	if err := json.Unmarshal(raw, &clone); err != nil {
		return semreg.Snapshot{}
	}
	return clone
}

// Resettle retries only the reprojection of one asset through the one
// scheduler. It refuses with ErrSemanticOutstanding, touching no gate, when
// the asset's semantic control outcome is not accepted or replayed, with
// ErrReprojectionInFlight while an attempt for the asset is running, and with
// ErrReprojectionQueued when the scheduler is busy with earlier attempts (the
// queued attempt then runs in order). Otherwise it waits for its attempt.
func (h *Hub) Resettle(id semreg.AssetID) error {
	h.mu.Lock()
	target := h.assets[id]
	if target == nil {
		h.mu.Unlock()
		return ErrUnknownAsset
	}
	ca := target.gate
	switch {
	case ca == nil:
		h.mu.Unlock()
		return nil
	case !semanticSuccess(ca.semantic):
		h.mu.Unlock()
		return ErrSemanticOutstanding
	case ca.inFlight != nil:
		h.mu.Unlock()
		return ErrReprojectionInFlight
	case ca.reprojection == ReprojectionSettled:
		h.mu.Unlock()
		return nil
	}
	entry := h.enqueueLocked(ca)
	busy := h.draining
	h.mu.Unlock()
	if busy {
		return ErrReprojectionQueued
	}
	h.drain()
	<-entry.done
	h.mu.RLock()
	defer h.mu.RUnlock()
	if ca.reprojection != ReprojectionSettled {
		return ca.reprojErr
	}
	return nil
}

// awaitFinished applies the unfinished and settling rules for the latest
// control operation of src. The caller holds src.control; awaitFinished
// releases it while reprojection attempts run and re-acquires it before
// re-validating, so a callback may request control on the same source. A
// mutating request returns SettlePending while an attempt is in flight or
// the scheduler is busy, or when the attempts it scheduled leave the source
// unsettled; an activation refuses an abandoned operation and waits up to
// ControlSettleTimeout, returning SettleTimeout with the attempt still in
// flight.
func (h *Hub) awaitFinished(src *source, activation bool) error {
	deadline := time.Now().Add(ControlSettleTimeout)
	for {
		h.mu.Lock()
		op := src.last
		state := op.stateLocked()
		abandoned := op.abandonedOpenLocked()
		switch {
		case state == opUnfinished:
			h.mu.Unlock()
			return &ControlPendingError{ID: op.id}
		case activation && abandoned:
			h.mu.Unlock()
			return ErrControlAbandoned
		case state == opFinished:
			h.mu.Unlock()
			return nil
		}
		busy := h.draining
		for _, ca := range op.assets {
			if semanticSuccess(ca.semantic) && ca.reprojection != ReprojectionSettled && ca.inFlight != nil {
				busy = true
			}
		}
		if busy && !activation {
			h.mu.Unlock()
			return &SettlePendingError{ID: op.id}
		}
		var waits []chan struct{}
		for _, ca := range op.assets {
			if semanticSuccess(ca.semantic) && ca.reprojection != ReprojectionSettled {
				waits = append(waits, h.enqueueLocked(ca).done)
			}
		}
		h.mu.Unlock()
		src.control.Unlock()
		if activation {
			go h.drain()
		} else {
			h.drain()
		}
		expired := false
		for _, done := range waits {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				expired = true
				break
			}
			timer := time.NewTimer(remaining)
			select {
			case <-done:
			case <-timer.C:
				expired = true
			}
			timer.Stop()
			if expired {
				break
			}
		}
		src.control.Lock()
		h.mu.RLock()
		state = op.stateLocked()
		latest := src.last == op
		h.mu.RUnlock()
		switch {
		case !latest:
			continue
		case state == opFinished:
			return nil
		case !activation:
			return &SettlePendingError{ID: op.id}
		case expired || !time.Now().Before(deadline):
			return &SettleTimeoutError{ID: op.id}
		}
		time.Sleep(settlePollInterval)
	}
}

// RetryControl continues the latest unfinished control operation of a
// source. Accepted and replayed assets are skipped; unbuilt assets are built;
// a proven revision_conflict is rebuilt at the current revision as a new
// attempt; any other rejection is terminal; an uncertain submission is
// resubmitted with its exact retained bytes. A settling operation retries
// only its reprojection callbacks; while an attempt is in flight it returns
// SettlePending.
func (h *Hub) RetryControl(id ControlID) (ControlResult, error) {
	op, err := h.lookupControl(id)
	if err != nil {
		return ControlResult{}, err
	}
	failure := func() error {
		op.src.control.Lock()
		defer op.src.control.Unlock()
		h.mu.Lock()
		latest, abandoned, state := op.src.last == op, op.abandoned, op.stateLocked()
		if !latest || abandoned {
			h.mu.Unlock()
			return ErrControlRequest
		}
		if state == opSettling {
			defer h.mu.Unlock()
			for _, ca := range op.assets {
				if semanticSuccess(ca.semantic) && ca.inFlight != nil {
					return &SettlePendingError{ID: op.id}
				}
			}
			for _, ca := range op.assets {
				if semanticSuccess(ca.semantic) && ca.reprojection != ReprojectionSettled {
					h.enqueueLocked(ca)
				}
			}
			return nil
		}
		h.mu.Unlock()
		if state == opUnfinished {
			return h.process(op, true)
		}
		return nil
	}()
	h.drain()
	return h.result(op), failure
}

// AbandonControl marks an unfinished control operation abandoned. Its
// affected assets stay closed with handles revoked until a successful new
// control operation or a new hub incarnation.
func (h *Hub) AbandonControl(id ControlID) error {
	op, err := h.lookupControl(id)
	if err != nil {
		return err
	}
	op.src.control.Lock()
	defer op.src.control.Unlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	if op.src.last != op || op.abandoned || op.stateLocked() != opUnfinished {
		return ErrControlRequest
	}
	op.abandoned = true
	return nil
}

func (h *Hub) lookupControl(id ControlID) (*controlOp, error) {
	if id.Incarnation != h.incarnation {
		return nil, ErrControlRequest
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	src := h.sources[id.Source]
	if src == nil || src.ops[id.N] == nil {
		return nil, ErrControlRequest
	}
	return src.ops[id.N], nil
}

func (h *Hub) result(op *controlOp) ControlResult {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := ControlResult{ID: op.id, Kind: op.kind, Assets: make([]AssetControlResult, 0, len(op.assets))}
	for _, ca := range op.assets {
		out.Assets = append(out.Assets, AssetControlResult{Asset: ca.asset.id, Outcome: ca.semantic, SnapshotID: ca.snapshot.SnapshotID, HubSeq: ca.hubSeq, Err: ca.err})
	}
	return out
}

// Activate creates the handle of the next generation of the handle's source
// and epoch. An unfinished preceding control operation returns
// ControlPending at once; a settling one is waited for up to
// ControlSettleTimeout; the new generation is admitted only when finished.
func (h *Hub) Activate(previous *Handle, generation uint64) (*Handle, error) {
	if previous == nil || previous.hub != h {
		return nil, &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "hub incarnation"}
	}
	h.mu.RLock()
	src := h.sources[previous.source]
	h.mu.RUnlock()
	if src == nil {
		return nil, ErrUnknownSource
	}
	src.control.Lock()
	defer src.control.Unlock()
	if err := h.awaitFinished(src, true); err != nil {
		return nil, err
	}
	h.mu.Lock()
	if src.epoch != previous.epoch || src.retired[src.epoch] {
		h.mu.Unlock()
		return nil, &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "activation epoch"}
	}
	if src.live || !src.fenced {
		h.mu.Unlock()
		return nil, ErrGenerationLive
	}
	if generation <= src.generation {
		h.mu.Unlock()
		return nil, &semreg.Error{ID: semreg.StaleDriverGeneration, Detail: "activation generation"}
	}
	src.generation = generation
	src.token++
	src.live, src.fenced, src.first = true, false, false
	handle := &Handle{hub: h, source: src.id, epoch: src.epoch, generation: generation, token: src.token, legacy: src.legacy}
	h.mu.Unlock()
	h.bus.emit(Event{Kind: EventGenerationActivated, Source: src.id, Epoch: handle.epoch, Generation: generation})
	if err := h.journal.Append(EpochRecord{Incarnation: h.incarnation, Source: src.id, Epoch: handle.epoch, Generation: generation, Kind: EventGenerationActivated}); err != nil {
		return nil, err
	}
	return handle, nil
}
