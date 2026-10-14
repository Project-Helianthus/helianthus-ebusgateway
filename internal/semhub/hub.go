package semhub

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

var (
	// ErrUnknownAsset reports an asset that was never declared.
	ErrUnknownAsset = errors.New("semhub: unknown asset")
	// ErrUnknownSource reports a source that was never declared or admitted.
	ErrUnknownSource = errors.New("semhub: unknown source")
	// ErrDeclarationConflict reports a second declaration of one asset with a
	// different source or different validators.
	ErrDeclarationConflict = errors.New("semhub: conflicting asset declaration")
	// ErrAdmission reports an admission the epoch rules refuse.
	ErrAdmission = errors.New("semhub: admission refused")
	// ErrTransaction reports misuse of a publication transaction.
	ErrTransaction = errors.New("semhub: invalid transaction")
)

// CollisionError reports one identifier claimed by two owners, or two tuples
// producing one identifier. It names driver labels only.
type CollisionError struct {
	Drivers [2]string
}

func (e *CollisionError) Error() string {
	return fmt.Sprintf("semhub: identity collision between drivers %q and %q", e.Drivers[0], e.Drivers[1])
}

// Options configures one hub incarnation.
type Options struct {
	// Journal records epoch lifecycle events. A nil journal uses an
	// in-memory journal; durable storage is not provided by this package.
	Journal EpochJournal
}

// Hub is one in-process hub incarnation.
type Hub struct {
	mu          sync.RWMutex
	incarnation string
	journal     EpochJournal
	bus         *eventBus
	ids         map[string]idOwner
	assets      map[semreg.AssetID]*asset
	sources     map[semreg.SourceID]*source
	faults      func(stage string, asset semreg.AssetID)
	// queue holds accepted control assets awaiting their first reprojection,
	// appended in HubSeq order under mu; draining marks the one drainer.
	queue    []*reprojectionEntry
	draining bool
}

type idOwner struct {
	driver string
	kind   Kind
}

type asset struct {
	commit    sync.Mutex
	id        semreg.AssetID
	source    semreg.SourceID
	driver    string
	packs     []semreg.PackRef
	kernel    *semreg.PublicationKernel
	onControl func(ControlEvent) error
	hubSeq    uint64
	gate      *controlAsset
}

type source struct {
	control    sync.Mutex
	id         semreg.SourceID
	driver     string
	legacy     LegacyFamily
	admitted   bool
	epoch      semreg.SourceEpochID
	generation uint64
	first      bool
	admissions int
	token      uint64
	live       bool
	fenced     bool
	epochs     map[semreg.SourceEpochID]bool
	retired    map[semreg.SourceEpochID]bool
	assets     map[semreg.AssetID]struct{}
	counter    uint64
	last       *controlOp
	ops        map[uint64]*controlOp
}

// New starts one hub incarnation with a random 128-bit incarnation identity.
func New(options Options) (*Hub, error) {
	return newHub(options, SubscriberQueueLimit)
}

func newHub(options Options, queueLimit int) (*Hub, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, fmt.Errorf("semhub: incarnation identity: %w", err)
	}
	journal := options.Journal
	if journal == nil {
		journal = NewMemoryJournal()
	}
	incarnation := hex.EncodeToString(raw[:])
	return &Hub{
		incarnation: incarnation, journal: journal, bus: newEventBus(incarnation, queueLimit),
		ids: make(map[string]idOwner), assets: make(map[semreg.AssetID]*asset), sources: make(map[semreg.SourceID]*source),
	}, nil
}

// Incarnation returns the random identity of this hub incarnation.
func (h *Hub) Incarnation() string { return h.incarnation }

// Subscribe returns a new bounded event subscription.
func (h *Hub) Subscribe() *Subscription { return h.bus.subscribe() }

// Cursor returns the latest stream position.
func (h *Hub) Cursor() Cursor { return h.bus.cursor() }

// Since returns the retained events after cursor or ErrResyncRequired.
func (h *Hub) Since(cursor Cursor) ([]Event, error) { return h.bus.since(cursor) }

func (h *Hub) fault(stage string, id semreg.AssetID) {
	if h.faults != nil {
		h.faults(stage, id)
	}
}

// claim registers id for driver under h.mu. A different owner, or the same
// owner under another kind, is a collision.
func (h *Hub) claimLocked(id string, kind Kind, driver string) error {
	if owner, ok := h.ids[id]; ok {
		if owner.driver != driver || owner.kind != kind {
			return &CollisionError{Drivers: [2]string{driver, owner.driver}}
		}
		return nil
	}
	h.ids[id] = idOwner{driver: driver, kind: kind}
	return nil
}

func (h *Hub) sourceLocked(id semreg.SourceID, driver string, legacy LegacyFamily) (*source, error) {
	if err := h.claimLocked(string(id), KindSource, driver); err != nil {
		return nil, err
	}
	src := h.sources[id]
	if src == nil {
		src = &source{id: id, driver: driver, legacy: legacy, epochs: make(map[semreg.SourceEpochID]bool), retired: make(map[semreg.SourceEpochID]bool), assets: make(map[semreg.AssetID]struct{}), ops: make(map[uint64]*controlOp)}
		h.sources[id] = src
	}
	if legacy != "" && src.legacy == "" {
		src.legacy = legacy
	}
	if src.legacy != legacy && legacy != "" {
		return nil, ErrAdmission
	}
	return src, nil
}

// AssetSpec declares one asset. The asset's kernel is constructed once with
// these validators; the registry is frozen and shared by every fork.
type AssetSpec struct {
	AssetID    semreg.AssetID
	SourceID   semreg.SourceID
	Driver     string
	Validators []semreg.PackValidator
	// OnControl reprojects the declaring family's cache after a control
	// commit. It runs outside every hub lock, in HubSeq order.
	OnControl func(ControlEvent) error
}

// Declaration is one declared asset.
type Declaration struct {
	hub   *Hub
	asset *asset
}

// Asset returns the declared asset identifier.
func (d *Declaration) Asset() semreg.AssetID { return d.asset.id }

// Source returns the declared source identifier.
func (d *Declaration) Source() semreg.SourceID { return d.asset.source }

// Hub returns the declaring hub.
func (d *Declaration) Hub() *Hub { return d.hub }

// SetReprojection binds the asset's reprojection callback. A family whose
// cache is constructed after the declaration (for example behind a startup
// preflight) binds it here; the latest binding wins.
func (d *Declaration) SetReprojection(onControl func(ControlEvent) error) {
	d.hub.mu.Lock()
	d.asset.onControl = onControl
	d.hub.mu.Unlock()
}

// Declare registers one asset with its single source. A repeated identical
// declaration returns the existing one.
func (h *Hub) Declare(spec AssetSpec) (*Declaration, error) {
	if spec.Driver == "" || len(spec.Validators) == 0 || ValidateOpaque(string(spec.AssetID)) != nil || ValidateOpaque(string(spec.SourceID)) != nil || string(spec.AssetID) == string(spec.SourceID) {
		return nil, ErrInvalidIdentity
	}
	packs := make([]semreg.PackRef, 0, len(spec.Validators))
	for _, validator := range spec.Validators {
		if validator == nil {
			return nil, ErrInvalidIdentity
		}
		packs = append(packs, validator.Pack())
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if existing := h.assets[spec.AssetID]; existing != nil {
		if existing.driver != spec.Driver {
			return nil, &CollisionError{Drivers: [2]string{spec.Driver, existing.driver}}
		}
		if existing.source != spec.SourceID || !samePacks(existing.packs, packs) {
			return nil, ErrDeclarationConflict
		}
		return &Declaration{hub: h, asset: existing}, nil
	}
	if owner, ok := h.ids[string(spec.SourceID)]; ok && owner.driver != spec.Driver {
		return nil, &CollisionError{Drivers: [2]string{spec.Driver, owner.driver}}
	}
	if owner, ok := h.ids[string(spec.AssetID)]; ok && (owner.driver != spec.Driver || owner.kind != KindAsset) {
		return nil, &CollisionError{Drivers: [2]string{spec.Driver, owner.driver}}
	}
	kernel, err := semreg.NewPublicationKernel(spec.AssetID, spec.Validators...)
	if err != nil {
		return nil, err
	}
	src, err := h.sourceLocked(spec.SourceID, spec.Driver, "")
	if err != nil {
		return nil, err
	}
	if err := h.claimLocked(string(spec.AssetID), KindAsset, spec.Driver); err != nil {
		return nil, err
	}
	declared := &asset{id: spec.AssetID, source: spec.SourceID, driver: spec.Driver, packs: packs, kernel: kernel, onControl: spec.OnControl}
	h.assets[spec.AssetID] = declared
	src.assets[spec.AssetID] = struct{}{}
	return &Declaration{hub: h, asset: declared}, nil
}

func samePacks(left, right []semreg.PackRef) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// ClassSpec declares a family of lazily declared assets sharing one driver,
// validator set and reprojection callback.
type ClassSpec struct {
	Name       string
	Driver     string
	Validators func() []semreg.PackValidator
	OnControl  func(ControlEvent) error
}

// Class lazily declares assets discovered at runtime.
type Class struct {
	hub  *Hub
	spec ClassSpec
}

// Hub returns the class's hub.
func (c *Class) Hub() *Hub { return c.hub }

// DeclareClass registers a lazily declared asset class.
func (h *Hub) DeclareClass(spec ClassSpec) (*Class, error) {
	if spec.Name == "" || spec.Driver == "" || spec.Validators == nil {
		return nil, ErrInvalidIdentity
	}
	return &Class{hub: h, spec: spec}, nil
}

// Declare declares one discovered asset. A refusal is per asset: it is
// returned and emitted as DeclareRefused naming driver labels only.
func (c *Class) Declare(id semreg.AssetID, src semreg.SourceID) (*Declaration, error) {
	declaration, err := c.hub.Declare(AssetSpec{AssetID: id, SourceID: src, Driver: c.spec.Driver, Validators: c.spec.Validators(), OnControl: c.spec.OnControl})
	if err != nil {
		drivers := []string{c.spec.Driver}
		var collision *CollisionError
		if errors.As(err, &collision) {
			drivers = []string{collision.Drivers[0], collision.Drivers[1]}
		}
		c.hub.bus.emit(Event{Kind: EventDeclareRefused, Drivers: drivers})
		return nil, err
	}
	return declaration, nil
}

// Admission requests one source epoch. Hub-native sources leave Epoch empty
// and Generation zero: the hub allocates the epoch and starts at generation 1.
// Legacy sources declare their epoch and a nonzero starting generation.
type Admission struct {
	Source     semreg.SourceID
	Driver     string
	Legacy     LegacyFamily
	Epoch      semreg.SourceEpochID
	Generation uint64
}

// SourceInfo is the read-only lifecycle view of one source.
type SourceInfo struct {
	Source     semreg.SourceID      `json:"source"`
	Driver     string               `json:"driver"`
	Legacy     LegacyFamily         `json:"legacy,omitempty"`
	Epoch      semreg.SourceEpochID `json:"epoch,omitempty"`
	Generation uint64               `json:"generation,omitempty"`
	Live       bool                 `json:"live"`
}

// AdmitSource admits one source epoch before its provider or endpoint opens.
func (h *Hub) AdmitSource(request Admission) (*Handle, error) {
	if request.Driver == "" || ValidateOpaque(string(request.Source)) != nil {
		return nil, ErrInvalidIdentity
	}
	if request.Legacy != "" && !request.Legacy.valid() {
		return nil, ErrInvalidIdentity
	}
	if request.Legacy == "" && (request.Epoch != "" || request.Generation != 0) {
		return nil, fmt.Errorf("%w: hub-native epochs are hub-allocated", ErrAdmission)
	}
	if request.Legacy != "" && (request.Generation == 0 || ValidateOpaque(string(request.Epoch)) != nil) {
		return nil, fmt.Errorf("%w: legacy epoch or generation", ErrAdmission)
	}
	h.mu.Lock()
	src, err := h.sourceLocked(request.Source, request.Driver, request.Legacy)
	if err == nil && src.legacy != request.Legacy {
		err = fmt.Errorf("%w: source belongs to another identity regime", ErrAdmission)
	}
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	src.control.Lock()
	defer src.control.Unlock()
	if src.admitted {
		if request.Legacy == "" {
			return nil, fmt.Errorf("%w: hub-native source already admitted in this incarnation", ErrAdmission)
		}
		if err := h.awaitFinished(src, true); err != nil {
			return nil, err
		}
		h.mu.RLock()
		live, fenced, retired, epoch, generation := src.live, src.fenced, src.retired[src.epoch], src.epoch, src.generation
		h.mu.RUnlock()
		if live || (!fenced && !retired) || request.Epoch == epoch {
			return nil, fmt.Errorf("%w: successor requires a fenced or retired generation and a new epoch", ErrAdmission)
		}
		// A successor continues the generation sequence (current+1). Only the
		// PV provisional source, retired with its runtime, restarts freely.
		if src.legacy != LegacyPVSunSpec && request.Generation != generation+1 {
			return nil, fmt.Errorf("%w: successor generation must be the fenced generation plus one", ErrAdmission)
		}
	}
	epoch, generation := request.Epoch, request.Generation
	if request.Legacy == "" {
		epoch, generation = semreg.SourceEpochID("source-epoch:semhub:"+idHash("helianthus.semhub.epoch.v1", h.incarnation, string(request.Source))), 1
	}
	h.mu.Lock()
	if src.retired[epoch] || src.epochs[epoch] {
		h.mu.Unlock()
		return nil, fmt.Errorf("%w: epoch repeats within this hub incarnation", ErrAdmission)
	}
	src.epochs[epoch] = true
	src.admitted, src.epoch, src.generation, src.first = true, epoch, generation, src.admissions == 0
	src.admissions++
	src.token++
	src.live, src.fenced = true, false
	handle := &Handle{hub: h, source: src.id, epoch: epoch, generation: generation, token: src.token, first: src.first, legacy: src.legacy}
	h.mu.Unlock()
	h.bus.emit(Event{Kind: EventEpochAdmitted, Source: src.id, Epoch: epoch, Generation: generation})
	if err := h.journal.Append(EpochRecord{Incarnation: h.incarnation, Source: src.id, Epoch: epoch, Generation: generation, Kind: EventEpochAdmitted}); err != nil {
		return nil, err
	}
	return handle, nil
}

// Source returns the lifecycle view of one source.
func (h *Hub) Source(id semreg.SourceID) (SourceInfo, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	src := h.sources[id]
	if src == nil {
		return SourceInfo{}, false
	}
	return sourceInfo(src), true
}

// Sources returns every source in bytewise identifier order.
func (h *Hub) Sources() []SourceInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]SourceInfo, 0, len(h.sources))
	for _, src := range h.sources {
		out = append(out, sourceInfo(src))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Source < out[j].Source })
	return out
}

func sourceInfo(src *source) SourceInfo {
	return SourceInfo{Source: src.id, Driver: src.driver, Legacy: src.legacy, Epoch: src.epoch, Generation: src.generation, Live: src.live}
}

// Handle is the only way to publish. It binds the hub incarnation, the
// source, the epoch and the generation.
type Handle struct {
	hub        *Hub
	source     semreg.SourceID
	epoch      semreg.SourceEpochID
	generation uint64
	token      uint64
	first      bool
	legacy     LegacyFamily
}

// Source returns the bound source.
func (handle *Handle) Source() semreg.SourceID { return handle.source }

// Epoch returns the bound epoch.
func (handle *Handle) Epoch() semreg.SourceEpochID { return handle.epoch }

// Generation returns the bound generation.
func (handle *Handle) Generation() uint64 { return handle.generation }

// GenerationID returns the bound generation in SemReg decimal form.
func (handle *Handle) GenerationID() semreg.Uint64 {
	return semreg.Uint64(strconv.FormatUint(handle.generation, 10))
}

// Incarnation returns the minter input of this handle for one asset.
func (handle *Handle) Incarnation(id semreg.AssetID) Incarnation {
	return Incarnation{Asset: id, Source: handle.source, Epoch: handle.epoch, Generation: handle.generation, First: handle.first}
}

// checkHandle returns SemReg's own stale identifier for a handle that no
// longer binds the current (incarnation, source, epoch, generation).
func (h *Hub) checkHandle(handle *Handle) error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.checkHandleLocked(handle)
}

// checkHandleLocked is checkHandle with h.mu already held.
func (h *Hub) checkHandleLocked(handle *Handle) error {
	if handle == nil || handle.hub != h {
		return &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "hub incarnation"}
	}
	src := h.sources[handle.source]
	if src == nil || !src.admitted || src.epoch != handle.epoch || src.retired[handle.epoch] {
		return &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "publication handle epoch"}
	}
	if !src.live || src.token != handle.token || src.generation != handle.generation {
		return &semreg.Error{ID: semreg.StaleDriverGeneration, Detail: "publication handle generation"}
	}
	return nil
}

// Outcome is the result of one committed publication.
type Outcome string

const (
	// Accepted means the batch committed and every side effect ran.
	Accepted Outcome = "accepted"
	// Replayed means SemReg recognised an already accepted batch: no
	// callback, no cache swap, no event and no HubSeq.
	Replayed Outcome = "replayed"
)

// Committed is passed to the onCommit callback of an accepted publication.
type Committed struct {
	Snapshot  semreg.Snapshot
	Canonical []byte
	HubSeq    uint64
}

// Tx is one staged publication on one asset. It holds the asset commit lock
// from Begin until Commit or Abort.
type Tx struct {
	hub        *Hub
	handle     *Handle
	asset      *asset
	fork       *semreg.PublicationKernel
	priorKnown bool
	priorSeq   semreg.Uint64
	applied    *semreg.PublicationBatch
	snapshot   semreg.Snapshot
	canonical  []byte
	replayed   bool
	done       bool
}

// Begin stages one publication of handle on asset.
func (h *Hub) Begin(handle *Handle, id semreg.AssetID) (*Tx, error) {
	h.mu.RLock()
	target := h.assets[id]
	h.mu.RUnlock()
	if target == nil {
		return nil, ErrUnknownAsset
	}
	if handle != nil && handle.hub == h && target.source != handle.source {
		return nil, fmt.Errorf("%w: asset is declared with another source", ErrTransaction)
	}
	target.commit.Lock()
	if err := h.checkHandle(handle); err != nil {
		target.commit.Unlock()
		return nil, err
	}
	h.mu.RLock()
	kernel := target.kernel
	h.mu.RUnlock()
	fork, err := kernel.Fork()
	if err != nil {
		target.commit.Unlock()
		return nil, err
	}
	return &Tx{hub: h, handle: handle, asset: target, fork: fork}, nil
}

// Current returns the staged snapshot.
func (tx *Tx) Current() (semreg.Snapshot, bool) {
	snapshot, _, ok := tx.fork.Current()
	return snapshot, ok
}

// Incarnation returns the minter input of the transaction's handle.
func (tx *Tx) Incarnation() Incarnation { return tx.handle.Incarnation(tx.asset.id) }

// NextSequence is the next publication sequence of the handle's key, taken
// from the asset kernel cursor: 1 for an unseen key.
func (tx *Tx) NextSequence() semreg.Uint64 {
	snapshot, ok := tx.Current()
	if !ok {
		return "1"
	}
	if cursor, found := cursorFor(snapshot, tx.handle.source, tx.handle.epoch, tx.handle.GenerationID()); found {
		return incrementUint(cursor.LastSequence)
	}
	return "1"
}

// SourceRetirements lists the epochs of the handle's source that are still
// current in the asset snapshot but are not the handle's epoch. The first
// batch of a new epoch carries them as SourceRetirements.
func (tx *Tx) SourceRetirements() []semreg.SourceEpochID {
	snapshot, ok := tx.Current()
	if !ok {
		return []semreg.SourceEpochID{}
	}
	out := []semreg.SourceEpochID{}
	for _, descriptor := range snapshot.Sources {
		if descriptor.SourceID == tx.handle.source && descriptor.State == semreg.SourceCurrent && descriptor.SourceEpochID != tx.handle.epoch {
			out = append(out, descriptor.SourceEpochID)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Apply submits batch to the staged kernel. Replay has no shortcut: the batch
// is always applied, and every SemReg error identifier passes through.
func (tx *Tx) Apply(batch semreg.PublicationBatch, monotonic semreg.MonotonicPoint) (semreg.Snapshot, []byte, error) {
	if tx == nil || tx.done || tx.applied != nil {
		return semreg.Snapshot{}, nil, ErrTransaction
	}
	if batch.AssetID != tx.asset.id || batch.SourceID != tx.handle.source || batch.SourceEpochID != tx.handle.epoch {
		return semreg.Snapshot{}, nil, &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "batch header does not match the publication handle"}
	}
	if batch.DriverGeneration != tx.handle.GenerationID() {
		return semreg.Snapshot{}, nil, &semreg.Error{ID: semreg.StaleDriverGeneration, Detail: "batch header does not match the publication handle"}
	}
	if snapshot, ok := tx.Current(); ok {
		if cursor, found := cursorFor(snapshot, batch.SourceID, batch.SourceEpochID, batch.DriverGeneration); found {
			tx.priorKnown, tx.priorSeq = true, cursor.LastSequence
		}
	}
	snapshot, canonical, err := tx.fork.Apply(batch, monotonic)
	if err != nil {
		return semreg.Snapshot{}, nil, err
	}
	applied := batch
	tx.applied, tx.snapshot, tx.canonical = &applied, snapshot, canonical
	tx.replayed = tx.priorKnown && tx.priorSeq == batch.Sequence
	return snapshot, append([]byte(nil), canonical...), nil
}

// Commit makes an applied batch current. The handle is checked again at this
// serialization point: a handle fenced after Begin aborts with SemReg's stale
// identifier, discarding the fork with no sequence and no event. onCommit
// runs under the asset commit lock so the family cache swaps atomically with
// the kernel.
func (tx *Tx) Commit(onCommit func(Committed)) (Outcome, error) {
	if tx == nil || tx.done {
		return "", ErrTransaction
	}
	if tx.applied == nil {
		tx.Abort()
		return "", ErrTransaction
	}
	h := tx.hub
	defer tx.finish()
	h.fault("commit", tx.asset.id)
	// The handle check and the kernel swap form one critical section: a fence
	// that revokes the handle either precedes it (stale) or follows it.
	h.mu.Lock()
	if err := h.checkHandleLocked(tx.handle); err != nil {
		h.mu.Unlock()
		return "", err
	}
	h.fault("commit-locked", tx.asset.id)
	if tx.replayed {
		h.mu.Unlock()
		return Replayed, nil
	}
	tx.asset.kernel = tx.fork
	seq := h.bus.emit(Event{Kind: EventPublished, Asset: tx.asset.id, Source: tx.applied.SourceID, Epoch: tx.applied.SourceEpochID, Generation: tx.handle.generation, Sequence: tx.applied.Sequence, SnapshotID: tx.snapshot.SnapshotID, Revisions: &tx.snapshot.Revisions, Digest: tx.applied.BatchDigest})
	tx.asset.hubSeq = seq
	h.mu.Unlock()
	if onCommit != nil {
		onCommit(Committed{Snapshot: tx.snapshot, Canonical: append([]byte(nil), tx.canonical...), HubSeq: seq})
	}
	return Accepted, nil
}

// Abort discards the staged fork. It is idempotent.
func (tx *Tx) Abort() {
	if tx == nil || tx.done {
		return
	}
	tx.finish()
}

func (tx *Tx) finish() {
	tx.done = true
	tx.fork = nil
	tx.asset.commit.Unlock()
}

func cursorFor(snapshot semreg.Snapshot, src semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64) (semreg.PublicationCursor, bool) {
	for _, cursor := range snapshot.Cursors {
		if cursor.SourceID == src && cursor.SourceEpochID == epoch && cursor.DriverGeneration == generation {
			return cursor, true
		}
	}
	return semreg.PublicationCursor{}, false
}

func incrementUint(value semreg.Uint64) semreg.Uint64 {
	n, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil || n == ^uint64(0) {
		return ""
	}
	return semreg.Uint64(strconv.FormatUint(n+1, 10))
}

// NextSequence returns the next publication sequence of (asset, source,
// epoch, generation) from the asset kernel cursor.
func (h *Hub) NextSequence(id semreg.AssetID, src semreg.SourceID, epoch semreg.SourceEpochID, generation uint64) (semreg.Uint64, error) {
	h.mu.RLock()
	target := h.assets[id]
	h.mu.RUnlock()
	if target == nil {
		return "", ErrUnknownAsset
	}
	h.mu.RLock()
	kernel := target.kernel
	h.mu.RUnlock()
	snapshot, _, ok := kernel.Current()
	if !ok {
		return "1", nil
	}
	if cursor, found := cursorFor(snapshot, src, epoch, semreg.Uint64(strconv.FormatUint(generation, 10))); found {
		return incrementUint(cursor.LastSequence), nil
	}
	return "1", nil
}
