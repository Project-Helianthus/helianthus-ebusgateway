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

// CoverError reports an accepted publication whose transition cover (the
// control operation that fences the replaced generation, or retires the
// replaced epoch, on the source's other assets) did not complete its initial
// submission. The publication is committed and its handle valid; the other
// assets stay behind the cover's control gate until RetryControl(ID)
// resolves them, exactly as for a failed Fence or RetireEpoch.
type CoverError struct {
	ID  ControlID
	Err error
}

func (e *CoverError) Error() string {
	return fmt.Sprintf("semhub: publication accepted; transition cover %d is unfinished: %v", e.ID.N, e.Err)
}

func (e *CoverError) Unwrap() error { return e.Err }

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
	exporting    bool
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
	// undelivered marks a committed acceptance whose Accepted callback was
	// interrupted; RetryControl delivers it again.
	undelivered bool
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
	if target == nil || target.restoreFailed {
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
	if target == nil || target.restoreFailed {
		return false
	}
	return target.gate == nil || target.gate.reprojection == ReprojectionSettled
}

// Admitted is the condition of every public read: the control gate admits
// reads and the reprojection acknowledged the latest control snapshot. Both
// predicates are evaluated over one gate value under one read lock.
func (h *Hub) Admitted(id semreg.AssetID) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	target := h.assets[id]
	if target == nil || target.restoreFailed {
		return false
	}
	gate := target.gate
	h.fault("admitted-read", id)
	return gate == nil || semanticSuccess(gate.semantic) && gate.reprojection == ReprojectionSettled
}

// Readable is Admitted.
func (h *Hub) Readable(id semreg.AssetID) bool { return h.Admitted(id) }

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
	h.lock()
	// Pending-control ownership is revalidated at this serialization point: a
	// control operation installed since the preflight (for example the cover
	// of a generation transition) keeps ownership and nothing changes.
	if last := src.last; last != nil {
		switch last.stateLocked() {
		case opUnfinished:
			h.unlock()
			return ControlResult{}, &ControlPendingError{ID: last.id}
		case opSettling:
			h.unlock()
			return ControlResult{}, &SettlePendingError{ID: last.id}
		}
	}
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
	// Revalidate what the trial saw. A publication transition may have
	// committed since (it commits under h.mu, as this section runs): an asset
	// whose control key is now fenced is left out like any asset on which the
	// transition already holds, and the source is revoked only while the
	// targeted generation is still its current one.
	targetsCurrent = src.admitted && op.epoch == src.epoch && !src.retired[src.epoch] && (op.kind == ControlRetire || op.generation == src.generation)
	// Event and journal metadata are taken here, under the revocation lock.
	currentGeneration = src.generation
	for _, target := range assets {
		if !published[target.id] {
			continue
		}
		if _, batch, err := h.build(op, target, target.kernel); err == nil {
			if snapshot, _, ok := target.kernel.Current(); ok {
				if cursor, found := cursorFor(snapshot, op.src.id, op.epoch, batch.DriverGeneration); found && cursor.Fenced {
					delete(published, target.id)
					skipped[target.id] = true
				}
			}
		}
	}
	if len(published) == 0 && !targetsCurrent {
		h.unlock()
		if op.kind == ControlRetire {
			return ControlResult{}, &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "source retirement"}
		}
		return ControlResult{}, &semreg.Error{ID: semreg.StaleDriverGeneration, Detail: "generation fence"}
	}
	// Write-ahead: every control transition, of the current key or of a
	// historical one, is journalled with the assets it covers before any
	// handle, gate or event changes.
	covered := []semreg.AssetID{}
	for _, target := range assets {
		if published[target.id] {
			covered = append(covered, target.id)
		}
	}
	// A retirement of a historical key carries the newest generation of the
	// retired epoch among the covered assets, like every other retirement.
	historicalGeneration := uint64(0)
	for _, target := range assets {
		if !published[target.id] {
			continue
		}
		if snapshot, _, ok := target.kernel.Current(); ok {
			if generation := latestGeneration(snapshot, src.id, op.epoch); generation > historicalGeneration {
				historicalGeneration = generation
			}
		}
	}
	if targetsCurrent || len(covered) != 0 {
		kind, generation := EventGenerationFenced, op.generation
		if op.kind == ControlRetire {
			kind, generation = EventSourceRetired, historicalGeneration
			if targetsCurrent {
				generation = src.generation
			}
		}
		if err := h.appendEpochLocked(EpochRecord{Incarnation: h.incarnation, Source: src.id, Epoch: op.epoch, Generation: generation, Kind: kind, Assets: covered}); err != nil {
			h.unlock()
			return ControlResult{}, fmt.Errorf("semhub: journal control transition: %w", err)
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
	// Hub-level revocations without a batch: a retirement emits
	// SourceRetired with the retired generation, a fence GenerationRevoked.
	// When the operation covers no asset at all, one source-level event
	// still records the transition of the current key. They are sequenced
	// inside the revocation serialization.
	kind, generation := EventGenerationRevoked, op.generation
	if op.kind == ControlRetire {
		kind, generation = EventSourceRetired, historicalGeneration
		if targetsCurrent {
			generation = currentGeneration
		}
	}
	control := op.id
	for _, ca := range op.assets {
		if ca.semantic == SemanticRevokedUnpublished {
			h.bus.emit(Event{Kind: kind, Asset: ca.asset.id, Source: src.id, Epoch: op.epoch, Generation: generation, Control: &control, Reason: op.reason, NoPublication: true})
		}
	}
	if len(op.assets) == 0 && targetsCurrent {
		h.bus.emit(Event{Kind: kind, Source: src.id, Epoch: op.epoch, Generation: generation, Control: &control, Reason: op.reason, NoPublication: true})
	}
	h.unlock()
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
	// A retirement carried by an epoch-transition cover may meet an asset
	// whose newest cursor for the replaced epoch is already fenced (an
	// earlier generation transition fenced it). SemReg refuses a batch under
	// a fenced header, so the retirement goes out under the replaced epoch's
	// later generation as a new key at sequence 1. A standalone RetireEpoch
	// (no generation) keeps SemReg's refusal after a fence.
	if op.kind == ControlRetire && cursor.Fenced && op.generation != 0 {
		later := semreg.Uint64(strconv.FormatUint(op.generation, 10))
		if compareDecimal(later, cursor.DriverGeneration) > 0 {
			generation, sequence = later, "1"
			if existing, ok := cursorFor(snapshot, op.src.id, op.epoch, later); ok {
				sequence = incrementUint(existing.LastSequence)
			}
		}
	}
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
	// Revalidate under the asset lock: an attempt that resolved meanwhile is
	// never submitted again.
	h.mu.RLock()
	resolved := semanticSuccess(ca.semantic) || ca.semantic == SemanticRevokedUnpublished
	exporting := ca.exporting
	h.mu.RUnlock()
	if exporting {
		return ErrControlPending
	}
	if resolved {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			h.lock()
			if semanticSuccess(ca.semantic) {
				// The batch committed before the interruption (the Accepted
				// callback): a callback cannot roll back an acceptance.
				ca.undelivered = true
				failure = fmt.Errorf("semhub: accepted control delivery interrupted: %v", recovered)
				h.unlock()
				return
			}
			ca.semantic, ca.errID, ca.err = SemanticUncertain, "", fmt.Errorf("semhub: control submission interrupted: %v", recovered)
			failure = ca.err
			control := op.id
			h.bus.emit(Event{Kind: EventControlFailed, Asset: target.id, Source: op.src.id, Epoch: op.epoch, Generation: op.generation, Control: &control})
			h.unlock()
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
		h.lock()
		ca.attempts = append(ca.attempts, attempt)
		ca.semantic = SemanticPending
		h.unlock()
	}
	h.mu.RLock()
	attempt := ca.attempts[len(ca.attempts)-1]
	h.mu.RUnlock()
	var batch semreg.PublicationBatch
	if err := json.Unmarshal(attempt.Batch, &batch); err != nil {
		return h.reject(ca, err)
	}
	if h.recovery != nil && h.recovery.BeforeControl != nil {
		staged := ControlAttempt{Control: op.id, Asset: target.id, Kind: op.kind, Attempt: copyAttempt(attempt), Monotonic: op.monotonic}
		var exportErr error
		h.outsideControlLocks(ca, func() { exportErr = h.recovery.BeforeControl(staged) })
		if exportErr != nil {
			return h.reject(ca, fmt.Errorf("%w: %w", &semreg.Error{ID: "semhub.control_export_refused", Detail: "BeforeControl refused submission"}, exportErr))
		}
		h.mu.RLock()
		superseded := op.abandoned || target.gate != ca
		h.mu.RUnlock()
		if superseded {
			return ErrControlAbandoned
		}
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
	var recovery AcceptedBatch
	if h.recovery != nil {
		recovery = acceptedBatch(attempt.Batch, op.monotonic, snapshot, canonical)
		recovery.Incarnation = h.incarnation
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
		h.lock()
		defer h.unlock()
		if !replayed {
			target.kernel = fork
			if h.recovery != nil {
				target.records = append(target.records, recovery)
			}
		}
		h.fault("after-swap", target.id)
		seq := h.bus.emit(Event{Kind: kind, Asset: target.id, Source: op.src.id, Epoch: op.epoch, Generation: generation, Sequence: batch.Sequence, SnapshotID: snapshot.SnapshotID, Revisions: &snapshot.Revisions, Digest: batch.BatchDigest, Control: &control, Reason: reason})
		target.hubSeq = seq
		recovery.HubSeq = seq
		if h.recovery != nil && len(target.records) > 0 {
			last := &target.records[len(target.records)-1]
			if !replayed || last.HubSeq == 0 && last.Cursor == recovery.Cursor {
				last.HubSeq = seq
			}
			if replayed && last.Cursor == recovery.Cursor {
				recovery = *last
			}
		}
		ca.hubSeq, ca.snapshot, ca.canonical = seq, snapshot, canonical
		ca.semantic, ca.errID, ca.err = SemanticAccepted, "", nil
		if replayed {
			ca.semantic = SemanticReplayed
		}
		ca.reprojection, ca.reprojErr = ReprojectionUnsettled, nil
		h.enqueueLocked(ca)
	}()
	if h.recovery != nil {
		h.outsideControlLocks(ca, func() { h.exportAccepted(recovery) })
	}
	h.fault("after-commit", target.id)
	return nil
}

func (h *Hub) reject(ca *controlAsset, err error) error {
	var semErr *semreg.Error
	id := semreg.ErrorID("")
	if errors.As(err, &semErr) {
		id = semErr.ID
	}
	h.lock()
	ca.semantic, ca.errID, ca.err = SemanticRejected, id, err
	control := ca.op.id
	h.bus.emit(Event{Kind: EventControlFailed, Asset: ca.asset.id, Source: ca.op.src.id, Epoch: ca.op.epoch, Generation: ca.op.generation, Control: &control, ErrorID: id})
	h.unlock()
	return err
}

// drain is the one reprojection scheduler. It runs queued attempts one at a
// time, in queue order (control commits enqueue in HubSeq order), outside
// every hub and source lock. Only one goroutine drains at a time; a callback
// that triggers control merely enqueues and returns, and the active drainer
// runs the new entries afterwards. An entry whose asset still has an attempt
// in flight waits for it and then runs; it is never dropped.
func (h *Hub) drain() {
	h.lock()
	if h.draining {
		h.unlock()
		return
	}
	h.draining = true
	for len(h.queue) > 0 {
		entry := h.queue[0]
		h.queue = h.queue[1:]
		ca := entry.ca
		for ca.inFlight != nil {
			running := ca.inFlight
			h.unlock()
			<-running
			h.lock()
		}
		done, _ := h.startAttemptLocked(ca)
		h.unlock()
		if done != nil {
			<-done
		}
		h.lock()
		if ca.entry == entry {
			ca.entry = nil
		}
		close(entry.done)
	}
	h.draining = false
	h.unlock()
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
		View: InspectView{Snapshot: cloneSnapshot(ca.snapshot), Canonical: append([]byte(nil), ca.canonical...), Revisions: ca.snapshot.Revisions, HubSeq: ca.hubSeq, Control: controlStatusLocked(ca), Present: ca.snapshot.SnapshotID != ""}}
	if ca.op.kind == ControlRetire {
		event.Kind = EventSourceRetired
	}
	callback := target.onControl
	if target.restored && callback == nil {
		return nil, ErrReprojectionPending
	}
	done := make(chan struct{})
	ca.inFlight = done
	go func() {
		err := runCallback(callback, event)
		h.lock()
		if target.gate == ca && ca.hubSeq == event.HubSeq {
			if err != nil {
				ca.reprojection, ca.reprojErr = ReprojectionUnsettled, err
			} else {
				ca.reprojection, ca.reprojErr = ReprojectionSettled, nil
			}
		}
		ca.inFlight = nil
		h.unlock()
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
	h.lock()
	target := h.assets[id]
	if target == nil {
		h.unlock()
		return ErrUnknownAsset
	}
	ca := target.gate
	switch {
	case target.restoreFailed:
		h.unlock()
		return ErrControlPending
	case target.restored && target.onControl == nil:
		h.unlock()
		return ErrReprojectionPending
	case ca == nil:
		h.unlock()
		return nil
	case !semanticSuccess(ca.semantic):
		h.unlock()
		return ErrSemanticOutstanding
	case ca.inFlight != nil:
		h.unlock()
		return ErrReprojectionInFlight
	case ca.reprojection == ReprojectionSettled:
		h.unlock()
		return nil
	}
	entry := h.enqueueLocked(ca)
	busy := h.draining
	h.unlock()
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
		h.lock()
		op := src.last
		state := op.stateLocked()
		abandoned := op.abandonedOpenLocked()
		switch {
		case state == opUnfinished:
			h.unlock()
			return &ControlPendingError{ID: op.id}
		case activation && abandoned:
			h.unlock()
			return ErrControlAbandoned
		case state == opFinished:
			h.unlock()
			return nil
		}
		busy := h.draining
		for _, ca := range op.assets {
			if semanticSuccess(ca.semantic) && ca.reprojection != ReprojectionSettled && ca.inFlight != nil {
				busy = true
			}
		}
		if busy && !activation {
			h.unlock()
			return &SettlePendingError{ID: op.id}
		}
		var waits []chan struct{}
		for _, ca := range op.assets {
			if semanticSuccess(ca.semantic) && ca.reprojection != ReprojectionSettled {
				waits = append(waits, h.enqueueLocked(ca).done)
			}
		}
		h.unlock()
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
	var redeliver []*controlAsset
	failure := func() error {
		h.lockSourceControl(op.src)
		defer op.src.control.Unlock()
		h.lock()
		// An interrupted acceptance stays owed to the service even after a
		// newer operation supersedes this one, so it is collected first.
		for _, ca := range op.assets {
			if ca.undelivered {
				redeliver = append(redeliver, ca)
			}
		}
		latest, abandoned, state := op.src.last == op, op.abandoned, op.stateLocked()
		if !latest || abandoned {
			h.unlock()
			return ErrControlRequest
		}
		if state == opSettling {
			defer h.unlock()
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
		h.unlock()
		if state == opUnfinished {
			return h.process(op, true)
		}
		return nil
	}()
	// Interrupted acceptances are delivered again outside every lock, with
	// their original HubSeq; the service deduplicates repeated deliveries.
	for _, ca := range redeliver {
		h.mu.RLock()
		record, found := acceptedRecordLocked(ca.asset, ca.hubSeq)
		h.mu.RUnlock()
		if !found {
			continue
		}
		h.exportAccepted(record)
		h.lock()
		ca.undelivered = false
		h.unlock()
	}
	h.drain()
	return h.result(op), failure
}

// acceptedRecordLocked returns the stored acceptance committed at seq.
// h.mu must be held.
func acceptedRecordLocked(target *asset, seq uint64) (AcceptedBatch, bool) {
	for i := len(target.records) - 1; i >= 0 && seq != 0; i-- {
		if target.records[i].HubSeq == seq {
			return target.records[i], true
		}
	}
	return AcceptedBatch{}, false
}

// lockSourceControl takes the source control mutex. When the mutex is held
// by another control operation it reports the contention to the fault hook
// before blocking, which lets tests order an interleaving deterministically.
func (h *Hub) lockSourceControl(src *source) {
	if src.control.TryLock() {
		return
	}
	h.fault("source-control-contended", "")
	src.control.Lock()
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
	h.lock()
	defer h.unlock()
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
	h.lock()
	// The admission identity survives fences but changes on readmission, so
	// a handle of a withdrawn admission cannot activate the readmitted one.
	if src.admission != previous.admission || src.epoch != previous.epoch || src.retired[src.epoch] {
		h.unlock()
		return nil, &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "activation epoch"}
	}
	if src.live || !src.fenced {
		h.unlock()
		return nil, ErrGenerationLive
	}
	// Only the handle of the generation being replaced may activate its
	// successor; an older handle is obsolete.
	if previous.generation != src.generation || generation <= src.generation {
		h.unlock()
		return nil, &semreg.Error{ID: semreg.StaleDriverGeneration, Detail: "activation generation"}
	}
	// Journal before exposure: a failed append changes nothing.
	if err := h.appendEpochLocked(EpochRecord{Incarnation: h.incarnation, Source: src.id, Epoch: src.epoch, Generation: generation, Kind: EventGenerationActivated}); err != nil {
		h.unlock()
		return nil, fmt.Errorf("semhub: journal generation activation: %w", err)
	}
	src.generation = generation
	src.token++
	src.live, src.fenced, src.first = true, false, false
	handle := &Handle{hub: h, source: src.id, epoch: src.epoch, generation: generation, token: src.token, admission: src.admission, legacy: src.legacy}
	h.bus.emit(Event{Kind: EventGenerationActivated, Source: src.id, Epoch: handle.epoch, Generation: generation})
	h.unlock()
	return handle, nil
}
