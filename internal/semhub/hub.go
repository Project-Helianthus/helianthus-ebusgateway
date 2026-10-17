package semhub

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

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
	// ErrControlPending refuses a publication on an asset whose control
	// operation has not reached a successful semantic outcome. The caller
	// retries after the control operation resolves.
	ErrControlPending = errors.New("semhub: control operation pending for the asset")
	// ErrReprojectionPending refuses a publication on an asset whose family
	// reprojection has not acknowledged the latest control snapshot. The
	// caller retries after settlement.
	ErrReprojectionPending = errors.New("semhub: reprojection pending for the asset")
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
	// Recovery exports detached records; callbacks run outside the hub mutex.
	Recovery *RecoveryHooks
	// Faults, when set by a test in another package, is called at the hub's
	// named fault stages (for example "before-swap" of a control submission)
	// so the test can interrupt a submission. Production leaves it nil.
	Faults func(stage string, asset semreg.AssetID)
}

// Hub is one in-process hub incarnation.
type Hub struct {
	mu           sync.RWMutex
	incarnation  string
	journal      EpochJournal
	bus          *eventBus
	ids          map[string]idOwner
	assets       map[semreg.AssetID]*asset
	sources      map[semreg.SourceID]*source
	faults       func(stage string, asset semreg.AssetID)
	recovery     *RecoveryHooks
	epochHistory []EpochRecord
	// restoredEpochs reserves every epoch named in restored history, including
	// epochs of sources that have no record in this hub yet, so no admission can
	// reuse a historical epoch (D1: a restore always moves to a new epoch).
	restoredEpochs  map[semreg.SourceID]map[semreg.SourceEpochID]bool
	restoredRegimes map[semreg.SourceID]LegacyFamily
	// queue holds accepted control assets awaiting their first reprojection,
	// appended in HubSeq order under mu; draining marks the one drainer.
	queue    []*reprojectionEntry
	draining bool
	// trackOwner, set by a test before the hub is used, makes lock record
	// the goroutine holding the write lock in owner, so a test can assert
	// that an event is emitted inside the hub serialization.
	trackOwner bool
	owner      atomic.Uint64
}

// lock takes the hub write lock.
func (h *Hub) lock() {
	h.mu.Lock()
	if h.trackOwner {
		h.owner.Store(goroutineID())
	}
}

// unlock releases the hub write lock.
func (h *Hub) unlock() {
	if h.trackOwner {
		h.owner.Store(0)
	}
	h.mu.Unlock()
}

// goroutineID parses the current goroutine number from its stack header. It
// runs only when trackOwner is set.
func goroutineID() uint64 {
	var buf [64]byte
	header := strings.TrimPrefix(string(buf[:runtime.Stack(buf[:], false)]), "goroutine ")
	if index := strings.IndexByte(header, ' '); index > 0 {
		id, _ := strconv.ParseUint(header[:index], 10, 64)
		return id
	}
	return 0
}

type idOwner struct {
	driver string
	kind   Kind
}

type asset struct {
	commit        sync.Mutex
	id            semreg.AssetID
	source        semreg.SourceID
	driver        string
	packs         []semreg.PackRef
	kernel        *semreg.PublicationKernel
	onControl     func(ControlEvent) error
	hubSeq        uint64
	gate          *controlAsset
	records       []AcceptedBatch
	restoreFailed bool
	restored      bool
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
	// prior is the source state before the latest admission, restored when
	// that admission is withdrawn.
	prior priorAdmission
	// admission identifies the latest successful admission. It increases on
	// every admission, never decreases (a withdrawal does not undo it), and
	// survives fences and generation activations, so a handle of a withdrawn
	// admission stays revoked after an identical readmission.
	admission uint64
}

type priorAdmission struct {
	admitted   bool
	epoch      semreg.SourceEpochID
	generation uint64
	first      bool
	live       bool
	fenced     bool
	legacy     LegacyFamily
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
		faults: options.Faults, recovery: options.Recovery,
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

// Reserve claims one statically derivable binding, service or capability
// identifier for driver before any publication, under the same ownership
// rules as Declare and as a commit's identifier claims: another driver's
// identifier, or the same identifier under another kind, is a
// CollisionError naming both driver labels. A repeated reservation by the
// same driver and kind is idempotent.
func (h *Hub) Reserve(driver string, kind Kind, id string) error {
	switch kind {
	case KindBinding, KindService, KindCapability:
	default:
		return ErrInvalidIdentity
	}
	if driver == "" || ValidateOpaque(id) != nil {
		return ErrInvalidIdentity
	}
	h.lock()
	defer h.unlock()
	return h.claimLocked(id, kind, driver)
}

// sourceLocked returns the source record, creating it on first use. The
// identity regime (hub-native or one legacy family) is not set here: the first
// successful admission fixes it, and every later admission must match it.
func (h *Hub) sourceLocked(id semreg.SourceID, driver string) (*source, error) {
	if err := h.claimLocked(string(id), KindSource, driver); err != nil {
		return nil, err
	}
	src := h.sources[id]
	if src == nil {
		src = newSource(id, driver)
		h.sources[id] = src
	}
	return src, nil
}

func newSource(id semreg.SourceID, driver string) *source {
	return &source{id: id, driver: driver, epochs: make(map[semreg.SourceEpochID]bool), retired: make(map[semreg.SourceEpochID]bool), assets: make(map[semreg.AssetID]struct{}), ops: make(map[uint64]*controlOp)}
}

// claimAvailableLocked reports whether id can be claimed as kind for driver
// without claiming it. h.mu must be held.
func (h *Hub) claimAvailableLocked(id string, kind Kind, driver string) error {
	if owner, ok := h.ids[id]; ok && (owner.driver != driver || owner.kind != kind) {
		return &CollisionError{Drivers: [2]string{driver, owner.driver}}
	}
	return nil
}

// regimeLocked refuses an admission whose identity regime differs from the
// regime fixed by the source's first successful admission. h.mu must be held.
func (h *Hub) regimeLocked(src *source, legacy LegacyFamily) error {
	if family, ok := h.restoredRegimes[src.id]; ok && family != legacy {
		return fmt.Errorf("%w: restored source belongs to another identity regime", ErrAdmission)
	}
	if src.admissions > 0 && src.legacy != legacy {
		return fmt.Errorf("%w: source belongs to another identity regime", ErrAdmission)
	}
	return nil
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
	d.hub.lock()
	d.asset.onControl = onControl
	d.hub.unlock()
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
	h.lock()
	defer h.unlock()
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
	src, err := h.sourceLocked(spec.SourceID, spec.Driver)
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
	// A source neither declared nor admitted yet is admitted on an
	// unregistered record: it is registered, and its identifier claimed, only
	// together with the journalled admission, so a failed append leaves no
	// source and no claim behind.
	h.lock()
	src := h.sources[request.Source]
	err := h.claimAvailableLocked(string(request.Source), KindSource, request.Driver)
	if err == nil && src == nil {
		src = newSource(request.Source, request.Driver)
	}
	if err == nil {
		err = h.regimeLocked(src, request.Legacy)
	}
	h.unlock()
	if err != nil {
		return nil, err
	}
	src.control.Lock()
	defer src.control.Unlock()
	// Admissions serialize on the source control mutex, so the regime checked
	// here holds until this admission completes.
	h.mu.RLock()
	err = h.regimeLocked(src, request.Legacy)
	h.mu.RUnlock()
	if err != nil {
		return nil, err
	}
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
		if request.Legacy != LegacyPVSunSpec && request.Generation != generation+1 {
			return nil, fmt.Errorf("%w: successor generation must be the fenced generation plus one", ErrAdmission)
		}
	}
	h.fault("admit-commit", "")
	epoch, generation := request.Epoch, request.Generation
	if request.Legacy == "" {
		epoch, generation = semreg.SourceEpochID("source-epoch:semhub:"+idHash("helianthus.semhub.epoch.v1", h.incarnation, string(request.Source))), 1
	}
	h.lock()
	register := h.sources[src.id] != src
	if register {
		// The record is not registered yet: another declaration or admission
		// may have registered the identifier meanwhile, and the claim is
		// checked again at this serialization point.
		if h.sources[src.id] != nil {
			h.unlock()
			return nil, fmt.Errorf("%w: the source was registered concurrently; retry the admission", ErrAdmission)
		}
		if err := h.claimAvailableLocked(string(src.id), KindSource, src.driver); err != nil {
			h.unlock()
			return nil, err
		}
	}
	if src.retired[epoch] || src.epochs[epoch] || h.restoredEpochs[src.id][epoch] {
		h.unlock()
		return nil, fmt.Errorf("%w: epoch repeats within this hub incarnation", ErrAdmission)
	}
	// A restore may have installed this source's regime after the earlier
	// checks, so the regime is checked again where the admission commits.
	if err := h.regimeLocked(src, request.Legacy); err != nil {
		h.unlock()
		return nil, err
	}
	// The admission is journalled before it becomes visible: a failed append
	// leaves the source exactly as it was, so the admission can be retried.
	if err := h.appendEpochLocked(EpochRecord{Incarnation: h.incarnation, Source: src.id, Epoch: epoch, Generation: generation, Kind: EventEpochAdmitted}); err != nil {
		h.unlock()
		return nil, fmt.Errorf("semhub: journal epoch admission: %w", err)
	}
	if register {
		h.sources[src.id] = src
		h.ids[string(src.id)] = idOwner{driver: src.driver, kind: KindSource}
	}
	src.prior = priorAdmission{admitted: src.admitted, epoch: src.epoch, generation: src.generation, first: src.first, live: src.live, fenced: src.fenced, legacy: src.legacy}
	src.epochs[epoch] = true
	src.legacy = request.Legacy
	// Restored history means the source already had admissions in an earlier
	// incarnation: the next admission is a successor, never the historical first.
	src.admitted, src.epoch, src.generation, src.first = true, epoch, generation, src.admissions == 0 && len(h.restoredEpochs[src.id]) == 0
	src.admissions++
	src.admission++
	src.token++
	src.live, src.fenced = true, false
	handle := &Handle{hub: h, source: src.id, epoch: epoch, generation: generation, token: src.token, admission: src.admission, first: src.first, legacy: src.legacy}
	// The event is sequenced inside the hub serialization, so the stream
	// order matches the journal and state order.
	h.bus.emit(Event{Kind: EventEpochAdmitted, Source: src.id, Epoch: epoch, Generation: generation})
	h.unlock()
	return handle, nil
}

// Withdraw rolls back the handle's admission when its runtime failed to start
// before publishing anything: no asset holds a cursor of the admitted epoch
// and no control operation targeted it. The withdrawal is journalled first
// as a retirement of that epoch; a failed append changes nothing. The source
// then returns to its state before the admission, the handle is revoked, and
// the same admission may be retried, because nothing of the withdrawn epoch
// was ever published.
func (h *Hub) Withdraw(handle *Handle) error {
	if handle == nil || handle.hub != h {
		return fmt.Errorf("%w: withdrawal handle", ErrAdmission)
	}
	h.mu.RLock()
	src := h.sources[handle.source]
	h.mu.RUnlock()
	if src == nil {
		return ErrUnknownSource
	}
	src.control.Lock()
	defer src.control.Unlock()
	h.lock()
	defer h.unlock()
	if err := h.checkHandleLocked(handle); err != nil {
		return err
	}
	if src.last != nil && src.last.epoch == handle.epoch {
		return fmt.Errorf("%w: a control operation targeted the admitted epoch", ErrAdmission)
	}
	for id := range src.assets {
		if snapshot, _, ok := h.assets[id].kernel.Current(); ok {
			for _, cursor := range snapshot.Cursors {
				if cursor.SourceID == src.id && cursor.SourceEpochID == handle.epoch {
					return fmt.Errorf("%w: the admitted epoch has published", ErrAdmission)
				}
			}
		}
	}
	if err := h.appendEpochLocked(EpochRecord{Incarnation: h.incarnation, Source: src.id, Epoch: handle.epoch, Generation: handle.generation, Kind: EventSourceRetired, Assets: []semreg.AssetID{}}); err != nil {
		return fmt.Errorf("semhub: journal admission withdrawal: %w", err)
	}
	delete(src.epochs, handle.epoch)
	src.admissions--
	prior := src.prior
	src.admitted, src.epoch, src.generation, src.first, src.live, src.fenced, src.legacy = prior.admitted, prior.epoch, prior.generation, prior.first, prior.live, prior.fenced, prior.legacy
	src.prior = priorAdmission{}
	src.token++
	h.bus.emit(Event{Kind: EventSourceRetired, Source: src.id, Epoch: handle.epoch, Generation: handle.generation, NoPublication: true})
	return nil
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
	admission  uint64
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
	if src == nil || !src.admitted || src.admission != handle.admission || src.epoch != handle.epoch || src.retired[handle.epoch] {
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
	generation uint64
	transition bool
	// epoch is the epoch the transaction publishes under; an epoch
	// transition replaces the handle's epoch atomically with the commit.
	epoch           semreg.SourceEpochID
	epochTransition bool
	next            *Handle
	fork            *semreg.PublicationKernel
	monotonic       semreg.MonotonicPoint
	priorKnown      bool
	priorSeq        semreg.Uint64
	applied         *semreg.PublicationBatch
	snapshot        semreg.Snapshot
	canonical       []byte
	replayed        bool
	done            bool
}

// publicationGateLocked refuses a publication on an asset whose latest
// control operation is not semantically resolved (ErrControlPending) or whose
// reprojection has not acknowledged it (ErrReprojectionPending). h.mu must be
// held.
func publicationGateLocked(target *asset) error {
	gate := target.gate
	if target.restoreFailed {
		return ErrControlPending
	}
	switch {
	case gate == nil || gate.semantic == SemanticRevokedUnpublished:
		return nil
	case !semanticSuccess(gate.semantic):
		return ErrControlPending
	case gate.reprojection != ReprojectionSettled:
		return ErrReprojectionPending
	}
	return nil
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
	// Publication admission: an asset with outstanding control or
	// reprojection work refuses publications before taking its commit lock,
	// which a pending control submission may hold.
	h.mu.RLock()
	err := publicationGateLocked(target)
	h.mu.RUnlock()
	if err != nil {
		return nil, err
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
	return &Tx{hub: h, handle: handle, asset: target, generation: handle.generation, epoch: handle.epoch, fork: fork}, nil
}

// BeginEpoch stages the first publication of a new epoch of the handle's
// legacy source on asset. Its batch carries the retirement of the replaced
// epoch (see SourceRetirements). The replaced epoch is retired and the new
// one admitted atomically with the commit, so an aborted or rejected draft
// leaves the old epoch, its handle and the public view untouched. A PV
// source may start the new epoch at any nonzero generation; every other
// legacy source continues its generation sequence (current+1).
func (h *Hub) BeginEpoch(previous *Handle, id semreg.AssetID, epoch semreg.SourceEpochID, generation uint64) (*Tx, error) {
	if previous == nil || generation == 0 || ValidateOpaque(string(epoch)) != nil || epoch == previous.epoch {
		return nil, fmt.Errorf("%w: epoch transition", ErrAdmission)
	}
	if previous.legacy == "" || previous.legacy != LegacyPVSunSpec && generation != previous.generation+1 {
		return nil, fmt.Errorf("%w: epoch transition requires a legacy source and, outside PV, generation current+1", ErrAdmission)
	}
	tx, err := h.Begin(previous, id)
	if err != nil {
		return nil, err
	}
	h.mu.RLock()
	src := h.sources[previous.source]
	repeated := src.epochs[epoch] || src.retired[epoch] || h.restoredEpochs[previous.source][epoch]
	h.mu.RUnlock()
	if repeated {
		tx.Abort()
		return nil, fmt.Errorf("%w: epoch repeats within this hub incarnation", ErrAdmission)
	}
	tx.epoch, tx.generation, tx.epochTransition = epoch, generation, true
	return tx, nil
}

// BeginGeneration stages the first publication of a later generation of the
// handle's source and epoch on asset. The batch carries the fences of the
// older unfenced generations (see GenerationFences); the new generation is
// activated atomically with the commit, so an aborted or rejected transition
// leaves the old generation, its handle and the public view untouched. The
// commit and a control operation's revocation both run under the hub lock and
// each revalidates the other's effect, so they serialize without a source or
// asset lock inversion.
func (h *Hub) BeginGeneration(handle *Handle, id semreg.AssetID, generation uint64) (*Tx, error) {
	if handle == nil || generation <= handle.generation {
		return nil, &semreg.Error{ID: semreg.StaleDriverGeneration, Detail: "generation transition"}
	}
	tx, err := h.Begin(handle, id)
	if err != nil {
		return nil, err
	}
	tx.generation, tx.transition = generation, true
	return tx, nil
}

func (tx *Tx) generationID() semreg.Uint64 {
	return semreg.Uint64(strconv.FormatUint(tx.generation, 10))
}

// Handle returns the handle valid after the transaction: the new generation's
// handle after a committed generation transition, otherwise the original.
func (tx *Tx) Handle() *Handle {
	if tx.next != nil {
		return tx.next
	}
	return tx.handle
}

// transitionOthersLocked lists the other assets of the source that still hold
// the replaced generation, in bytewise order. h.mu must be held.
func (h *Hub) transitionOthersLocked(tx *Tx, src *source) []*asset {
	old := tx.handle.generation
	others := make([]*asset, 0, len(src.assets))
	for id := range src.assets {
		if id == tx.asset.id {
			continue
		}
		target := h.assets[id]
		if snapshot, _, ok := target.kernel.Current(); ok {
			if cursor, found := cursorFor(snapshot, src.id, tx.handle.epoch, semreg.Uint64(strconv.FormatUint(old, 10))); found && !cursor.Fenced {
				others = append(others, target)
			}
		}
	}
	sort.Slice(others, func(i, j int) bool { return others[i].id < others[j].id })
	return others
}

// epochOthersLocked lists the other assets of the source that still hold the
// replaced epoch as current, in bytewise order. h.mu must be held.
func (h *Hub) epochOthersLocked(tx *Tx, src *source, old semreg.SourceEpochID) []*asset {
	others := make([]*asset, 0, len(src.assets))
	for id := range src.assets {
		if id == tx.asset.id {
			continue
		}
		target := h.assets[id]
		if snapshot, _, ok := target.kernel.Current(); ok {
			for _, descriptor := range snapshot.Sources {
				if descriptor.SourceID == src.id && descriptor.SourceEpochID == old && descriptor.State == semreg.SourceCurrent {
					others = append(others, target)
					break
				}
			}
		}
	}
	sort.Slice(others, func(i, j int) bool { return others[i].id < others[j].id })
	return others
}

// coverEvidence is the evidence of a transition cover: the transaction's own
// fence evidence when it carries one, otherwise a hub digest of the batch.
func (tx *Tx) coverEvidence() []semreg.EvidenceRef {
	for _, fence := range tx.applied.GenerationFences {
		if len(fence.Evidence) != 0 {
			return append([]semreg.EvidenceRef(nil), fence.Evidence...)
		}
	}
	return []semreg.EvidenceRef{{Owner: "helianthus.semhub", Kind: "semhub.generation_transition", Digest: semreg.Digest("sha256:" + idHash("helianthus.semhub.transition.v1", string(tx.applied.BatchDigest)) + idHash("helianthus.semhub.transition.v1/asset", string(tx.asset.id))), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}}
}

// installCoverLocked builds the control operation that fences the replaced
// generation, or retires the replaced epoch, on others and closes their read
// gates. h.mu must be held.
func (h *Hub) installCoverLocked(src *source, kind ControlKind, epoch semreg.SourceEpochID, old uint64, others []*asset, evidence []semreg.EvidenceRef, observedAt semreg.TimePoint, monotonic semreg.MonotonicPoint) *controlOp {
	if len(others) == 0 {
		return nil
	}
	reason := semreg.DefinitionID("lifecycle.driver_generation_replaced")
	if kind == ControlRetire {
		reason = "lifecycle.source_epoch_replaced"
	}
	src.counter++
	op := &controlOp{id: ControlID{Incarnation: h.incarnation, Source: src.id, N: src.counter}, kind: kind, src: src, epoch: epoch, generation: old,
		reason: reason, evidence: evidence, observedAt: observedAt, monotonic: monotonic}
	for _, target := range others {
		ca := &controlAsset{op: op, asset: target, semantic: SemanticUnbuilt, reprojection: ReprojectionSettled}
		target.gate = ca
		op.assets = append(op.assets, ca)
	}
	src.ops[op.id.N] = op
	src.last = op
	return op
}

// GenerationFences lists the older unfenced generations of the handle's
// source and epoch in the staged snapshot. A batch of a later generation
// carries one GenerationFence for each, as SemReg requires.
func (tx *Tx) GenerationFences() []semreg.Uint64 {
	snapshot, ok := tx.Current()
	if !ok {
		return []semreg.Uint64{}
	}
	out := []semreg.Uint64{}
	for _, cursor := range snapshot.Cursors {
		generation, err := strconv.ParseUint(string(cursor.DriverGeneration), 10, 64)
		if err == nil && cursor.SourceID == tx.handle.source && cursor.SourceEpochID == tx.epoch && !cursor.Fenced && generation < tx.generation {
			out = append(out, cursor.DriverGeneration)
		}
	}
	sort.Slice(out, func(i, j int) bool { return compareDecimal(out[i], out[j]) < 0 })
	return out
}

// Current returns the staged snapshot.
func (tx *Tx) Current() (semreg.Snapshot, bool) {
	snapshot, _, ok := tx.fork.Current()
	return snapshot, ok
}

// Incarnation returns the minter input of the transaction's handle.
func (tx *Tx) Incarnation() Incarnation {
	inc := tx.handle.Incarnation(tx.asset.id)
	inc.Epoch, inc.Generation = tx.epoch, tx.generation
	if tx.transition || tx.epochTransition {
		inc.First = false
	}
	return inc
}

// NextSequence is the next publication sequence of the handle's key, taken
// from the asset kernel cursor: 1 for an unseen key.
func (tx *Tx) NextSequence() semreg.Uint64 {
	snapshot, ok := tx.Current()
	if !ok {
		return "1"
	}
	if cursor, found := cursorFor(snapshot, tx.handle.source, tx.epoch, tx.generationID()); found {
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
		if descriptor.SourceID == tx.handle.source && descriptor.State == semreg.SourceCurrent && descriptor.SourceEpochID != tx.epoch {
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
	// The transaction keeps an immutable deep copy of the batch: the kernel
	// validates that copy and Commit's journal, retirement and identifier
	// claims read only it, so a caller reusing its buffers after Apply
	// cannot make Commit describe a batch other than the one validated.
	batch, err := cloneBatch(batch)
	if err != nil {
		return semreg.Snapshot{}, nil, err
	}
	if batch.AssetID != tx.asset.id || batch.SourceID != tx.handle.source || batch.SourceEpochID != tx.epoch {
		return semreg.Snapshot{}, nil, &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "batch header does not match the publication handle"}
	}
	if batch.DriverGeneration != tx.generationID() {
		return semreg.Snapshot{}, nil, &semreg.Error{ID: semreg.StaleDriverGeneration, Detail: "batch header does not match the publication handle"}
	}
	if snapshot, ok := tx.Current(); ok {
		if cursor, found := cursorFor(snapshot, batch.SourceID, batch.SourceEpochID, batch.DriverGeneration); found {
			tx.priorKnown, tx.priorSeq = true, cursor.LastSequence
		}
	}
	tx.monotonic = monotonic
	snapshot, canonical, err := tx.fork.Apply(batch, monotonic)
	if err != nil {
		return semreg.Snapshot{}, nil, err
	}
	tx.applied, tx.snapshot, tx.canonical = &batch, snapshot, canonical
	tx.replayed = tx.priorKnown && tx.priorSeq == batch.Sequence
	return snapshot, append([]byte(nil), canonical...), nil
}

// Commit makes an applied batch current. The handle is checked again at this
// serialization point: a handle fenced after Begin aborts with SemReg's stale
// identifier, discarding the fork with no sequence and no event. onCommit
// runs under the asset commit lock so the family cache swaps atomically with
// the kernel.
//
// When the commit installs a transition cover over the source's other assets
// and the cover's initial submission fails (interrupted or rejected), Commit
// still returns Accepted (the publication is committed and Handle is valid)
// together with a *CoverError naming the cover's ControlID. The other assets
// stay behind the cover's control gate until RetryControl resolves them, as
// for a failed Fence.
func (tx *Tx) Commit(onCommit func(Committed)) (outcome Outcome, err error) {
	if tx == nil || tx.done {
		return "", ErrTransaction
	}
	if tx.applied == nil {
		tx.Abort()
		return "", ErrTransaction
	}
	h := tx.hub
	var recovery AcceptedBatch
	defer func() {
		if recovery.HubSeq != 0 {
			h.exportAccepted(recovery)
		}
	}()
	if h.recovery != nil {
		var exportErr error
		recovery, exportErr = tx.Export()
		if exportErr != nil {
			tx.Abort()
			return "", exportErr
		}
	}
	// A generation transition covers every asset the source owns: the other
	// assets still holding the replaced generation are fenced by a control
	// operation whose read gates close in this commit's critical section and
	// whose batches are submitted after the asset lock is released.
	var cover *controlOp
	defer func() {
		if cover != nil {
			// The initial cover submission serializes with RetryControl and
			// every other control operation of the source.
			cover.src.control.Lock()
			failure := h.process(cover, false)
			cover.src.control.Unlock()
			go h.drain()
			if failure != nil && err == nil {
				err = &CoverError{ID: cover.id, Err: failure}
			}
		}
	}()
	defer tx.finish()
	h.fault("commit", tx.asset.id)
	// The handle check and the kernel swap form one critical section: a fence
	// that revokes the handle either precedes it (stale) or follows it.
	h.lock()
	if err := h.checkHandleLocked(tx.handle); err != nil {
		h.unlock()
		return "", err
	}
	if err := publicationGateLocked(tx.asset); err != nil {
		h.unlock()
		return "", err
	}
	h.fault("commit-locked", tx.asset.id)
	if tx.replayed {
		// An exact replay repeats the stored acceptance with its original
		// HubSeq, so an interrupted Accepted delivery heals at least once.
		recovery = replayedAcceptanceLocked(tx.asset, recovery)
		h.unlock()
		return Replayed, nil
	}
	src := h.sources[tx.handle.source]
	// The batch's nested identifiers are validated against every claimed
	// identifier before anything is journalled or swapped, and claimed for
	// the asset's driver atomically with the swap below.
	claims, err := h.nestedClaimsLocked(tx)
	if err != nil {
		h.unlock()
		return "", err
	}
	// Epoch retirements the batch carries (a successor's first publication
	// retires the replaced epoch on this asset) are journalled write-ahead,
	// in the same append as any transition records.
	// Each carries the retired epoch's newest generation on this asset, read
	// from the snapshot before the swap.
	retiredGenerations := make(map[semreg.SourceEpochID]uint64, len(tx.applied.SourceRetirements))
	if snapshot, _, ok := tx.asset.kernel.Current(); ok {
		for _, retired := range tx.applied.SourceRetirements {
			retiredGenerations[retired] = latestGeneration(snapshot, src.id, retired)
		}
	}
	retirements := make([]semreg.SourceEpochID, 0, len(tx.applied.SourceRetirements))
	retirementRecords := make([]EpochRecord, 0, len(tx.applied.SourceRetirements))
	for _, retired := range tx.applied.SourceRetirements {
		if tx.epochTransition && retired == tx.handle.epoch {
			continue
		}
		retirements = append(retirements, retired)
		retirementRecords = append(retirementRecords, EpochRecord{Incarnation: h.incarnation, Source: src.id, Epoch: retired, Generation: retiredGenerations[retired], Kind: EventSourceRetired, Assets: []semreg.AssetID{tx.asset.id}})
	}
	if !tx.transition && !tx.epochTransition && len(retirementRecords) != 0 {
		if err := h.appendEpochLocked(retirementRecords...); err != nil {
			h.unlock()
			return "", fmt.Errorf("semhub: journal source retirement: %w", err)
		}
	}
	if tx.transition {
		// Write-ahead: the replaced generation's fence, naming every asset it
		// covers, and the activation are journalled as one operation before
		// either becomes visible.
		others := h.transitionOthersLocked(tx, src)
		fenced := []semreg.AssetID{tx.asset.id}
		for _, other := range others {
			fenced = append(fenced, other.id)
		}
		sort.Slice(fenced, func(i, j int) bool { return fenced[i] < fenced[j] })
		records := append([]EpochRecord{
			{Incarnation: h.incarnation, Source: src.id, Epoch: tx.handle.epoch, Generation: tx.handle.generation, Kind: EventGenerationFenced, Assets: fenced},
			{Incarnation: h.incarnation, Source: src.id, Epoch: tx.handle.epoch, Generation: tx.generation, Kind: EventGenerationActivated},
		}, retirementRecords...)
		if err := h.appendEpochLocked(records...); err != nil {
			h.unlock()
			return "", fmt.Errorf("semhub: journal generation transition: %w", err)
		}
		cover = h.installCoverLocked(src, ControlFence, tx.handle.epoch, tx.handle.generation, others, tx.coverEvidence(), tx.applied.ObservedAt, tx.monotonic)
		src.generation, src.first, src.fenced = tx.generation, false, false
		src.token++
		tx.next = &Handle{hub: h, source: src.id, epoch: src.epoch, generation: tx.generation, token: src.token, admission: src.admission, legacy: src.legacy}
		h.bus.emit(Event{Kind: EventGenerationActivated, Source: src.id, Epoch: src.epoch, Generation: tx.generation})
	}
	if tx.epochTransition {
		// Write-ahead: the replaced epoch's retirement, naming every asset it
		// covers, and the new epoch's admission are journalled as one
		// operation; the other assets still holding the replaced epoch are
		// retired by a cover control operation, as for a generation transition.
		if src.epochs[tx.epoch] || src.retired[tx.epoch] || h.restoredEpochs[src.id][tx.epoch] {
			h.unlock()
			return "", fmt.Errorf("%w: epoch repeats within this hub incarnation", ErrAdmission)
		}
		old := tx.handle.epoch
		others := h.epochOthersLocked(tx, src, old)
		retired := []semreg.AssetID{tx.asset.id}
		for _, other := range others {
			retired = append(retired, other.id)
		}
		sort.Slice(retired, func(i, j int) bool { return retired[i] < retired[j] })
		records := append([]EpochRecord{
			{Incarnation: h.incarnation, Source: src.id, Epoch: old, Generation: tx.handle.generation, Kind: EventSourceRetired, Assets: retired},
			{Incarnation: h.incarnation, Source: src.id, Epoch: tx.epoch, Generation: tx.generation, Kind: EventEpochAdmitted},
		}, retirementRecords...)
		if err := h.appendEpochLocked(records...); err != nil {
			h.unlock()
			return "", fmt.Errorf("semhub: journal epoch transition: %w", err)
		}
		cover = h.installCoverLocked(src, ControlRetire, old, tx.handle.generation, others, tx.coverEvidence(), tx.applied.ObservedAt, tx.monotonic)
		src.retired[old], src.epochs[tx.epoch] = true, true
		src.epoch, src.generation, src.first, src.fenced, src.live = tx.epoch, tx.generation, false, false, true
		src.token++
		tx.next = &Handle{hub: h, source: src.id, epoch: src.epoch, generation: tx.generation, token: src.token, admission: src.admission, legacy: src.legacy}
		h.bus.emit(Event{Kind: EventSourceRetired, Source: src.id, Epoch: old, Generation: tx.handle.generation})
		h.bus.emit(Event{Kind: EventEpochAdmitted, Source: src.id, Epoch: tx.epoch, Generation: tx.generation})
	}
	for _, retired := range retirements {
		src.retired[retired] = true
	}
	for _, claim := range claims {
		h.ids[claim.id] = idOwner{driver: tx.asset.driver, kind: claim.kind}
	}
	tx.asset.kernel = tx.fork
	if h.recovery != nil {
		tx.asset.records = append(tx.asset.records, recovery)
	}
	for _, retired := range tx.applied.SourceRetirements {
		h.bus.emit(Event{Kind: EventSourceRetired, Asset: tx.asset.id, Source: tx.applied.SourceID, Epoch: retired, Generation: retiredGenerations[retired], Sequence: tx.applied.Sequence, SnapshotID: tx.snapshot.SnapshotID, Revisions: &tx.snapshot.Revisions, Digest: tx.applied.BatchDigest})
	}
	for _, fence := range tx.applied.GenerationFences {
		fenced, _ := strconv.ParseUint(string(fence.DriverGeneration), 10, 64)
		h.bus.emit(Event{Kind: EventGenerationFenced, Asset: tx.asset.id, Source: fence.SourceID, Epoch: fence.SourceEpochID, Generation: fenced, Sequence: tx.applied.Sequence, SnapshotID: tx.snapshot.SnapshotID, Revisions: &tx.snapshot.Revisions, Digest: tx.applied.BatchDigest, Reason: fence.Reason})
	}
	seq := h.bus.emit(Event{Kind: EventPublished, Asset: tx.asset.id, Source: tx.applied.SourceID, Epoch: tx.applied.SourceEpochID, Generation: tx.generation, Sequence: tx.applied.Sequence, SnapshotID: tx.snapshot.SnapshotID, Revisions: &tx.snapshot.Revisions, Digest: tx.applied.BatchDigest})
	tx.asset.hubSeq = seq
	if h.recovery != nil {
		tx.asset.records[len(tx.asset.records)-1].HubSeq = seq
	}
	recovery.HubSeq = seq
	h.unlock()
	if onCommit != nil {
		onCommit(Committed{Snapshot: tx.snapshot, Canonical: append([]byte(nil), tx.canonical...), HubSeq: seq})
	}
	return Accepted, nil
}

// cloneBatch returns a structural deep copy of batch that shares no slice,
// map or pointer with it. It preserves nil versus empty collections and
// string bytes exactly, so SemReg validates the batch exactly as typed.
func cloneBatch(batch semreg.PublicationBatch) (semreg.PublicationBatch, error) {
	var out semreg.PublicationBatch
	if err := deepCopy(reflect.ValueOf(&out).Elem(), reflect.ValueOf(batch)); err != nil {
		return semreg.PublicationBatch{}, fmt.Errorf("%w: batch copy: %v", ErrTransaction, err)
	}
	return out, nil
}

// deepCopy copies src into the settable dst of the same type. Strings and
// scalars are copied by value; pointers, slices, maps, arrays, structs and
// interfaces are copied recursively, nil staying nil and empty staying empty.
// An unexported struct field is refused rather than silently shared.
func deepCopy(dst, src reflect.Value) error {
	switch src.Kind() {
	case reflect.Pointer:
		if src.IsNil() {
			return nil
		}
		target := reflect.New(src.Type().Elem())
		if err := deepCopy(target.Elem(), src.Elem()); err != nil {
			return err
		}
		dst.Set(target)
	case reflect.Slice:
		if src.IsNil() {
			return nil
		}
		target := reflect.MakeSlice(src.Type(), src.Len(), src.Len())
		for index := 0; index < src.Len(); index++ {
			if err := deepCopy(target.Index(index), src.Index(index)); err != nil {
				return err
			}
		}
		dst.Set(target)
	case reflect.Array:
		for index := 0; index < src.Len(); index++ {
			if err := deepCopy(dst.Index(index), src.Index(index)); err != nil {
				return err
			}
		}
	case reflect.Map:
		if src.IsNil() {
			return nil
		}
		target := reflect.MakeMapWithSize(src.Type(), src.Len())
		iter := src.MapRange()
		for iter.Next() {
			key := reflect.New(src.Type().Key()).Elem()
			if err := deepCopy(key, iter.Key()); err != nil {
				return err
			}
			value := reflect.New(src.Type().Elem()).Elem()
			if err := deepCopy(value, iter.Value()); err != nil {
				return err
			}
			target.SetMapIndex(key, value)
		}
		dst.Set(target)
	case reflect.Struct:
		for index := 0; index < src.NumField(); index++ {
			if !src.Type().Field(index).IsExported() {
				return fmt.Errorf("unexported field %s.%s", src.Type(), src.Type().Field(index).Name)
			}
			if err := deepCopy(dst.Field(index), src.Field(index)); err != nil {
				return err
			}
		}
	case reflect.Interface:
		if src.IsNil() {
			return nil
		}
		value := reflect.New(src.Elem().Type()).Elem()
		if err := deepCopy(value, src.Elem()); err != nil {
			return err
		}
		dst.Set(value)
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return fmt.Errorf("uncopyable %s", src.Type())
	default:
		dst.Set(src)
	}
	return nil
}

type nestedClaim struct {
	id   string
	kind Kind
}

// nestedClaimsLocked validates the binding, service and capability
// identifiers the applied batch upserts against every claimed identifier and
// returns the ones not yet claimed. An identifier owned by another driver, or
// by the same driver under another kind, is a collision. h.mu must be held.
func (h *Hub) nestedClaimsLocked(tx *Tx) ([]nestedClaim, error) {
	driver := tx.asset.driver
	seen := make(map[string]Kind)
	claims := []nestedClaim{}
	check := func(id string, kind Kind) error {
		if prior, ok := seen[id]; ok {
			if prior != kind {
				return &CollisionError{Drivers: [2]string{driver, driver}}
			}
			return nil
		}
		seen[id] = kind
		if owner, ok := h.ids[id]; ok {
			if owner.driver != driver || owner.kind != kind {
				return &CollisionError{Drivers: [2]string{driver, owner.driver}}
			}
			return nil
		}
		claims = append(claims, nestedClaim{id: id, kind: kind})
		return nil
	}
	for _, binding := range tx.applied.BindingUpserts {
		if err := check(string(binding.BindingID), KindBinding); err != nil {
			return nil, err
		}
	}
	for _, service := range tx.applied.ServiceUpserts {
		if err := check(string(service.InstanceID), KindService); err != nil {
			return nil, err
		}
	}
	for _, capability := range tx.applied.CapabilityUpserts {
		if err := check(string(capability.InstanceID), KindCapability); err != nil {
			return nil, err
		}
	}
	return claims, nil
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

// latestGeneration returns the newest driver generation of (src, epoch) among
// the snapshot's cursors, or 0 when the snapshot has none.
func latestGeneration(snapshot semreg.Snapshot, src semreg.SourceID, epoch semreg.SourceEpochID) uint64 {
	newest := uint64(0)
	for _, cursor := range snapshot.Cursors {
		if cursor.SourceID != src || cursor.SourceEpochID != epoch {
			continue
		}
		if generation, err := strconv.ParseUint(string(cursor.DriverGeneration), 10, 64); err == nil && generation > newest {
			newest = generation
		}
	}
	return newest
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
