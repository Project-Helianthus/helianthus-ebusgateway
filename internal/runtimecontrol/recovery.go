package runtimecontrol

// Service write-ahead recovery for service-routed managed writers (the S3.7
// to S3.9 recovery slice of the runtime driver lifecycle contract).
//
// A Recorder is the D5 RecoveryAdapter of the desired-state service. Every
// publication and control mutation of a registered managed source goes
// through a Writer and follows two durable steps around one hub call:
//
//  1. Intent: under the service-side source gate and every affected asset
//     gate, assign the source's WAL sequence, one intent ordinal per asset
//     and a monotonic point, and durably persist them with the exact staged
//     bytes (and the advanced clock high-water mark and elapsed-time anchor).
//  2. Outcome: after the hub call returns, durably persist the accepted
//     result (exact bytes, original point, opaque result, incarnation and
//     HubSeq) or the verbatim rejection identifier in a second transaction.
//
// Both transactions rewrite the one opaque D5 envelope through Store; no
// store write happens under a hub lock, a manager lock or the service's view
// or driver locks. Restoration runs before any source admission, replays the
// accepted stream on detached kernels, resolves each uncertain intent by
// exact cursor comparison (exact-byte resubmission or readback, never a
// rebuilt batch) in intent order, and only then installs the result into the
// hub. Compaction replaces the stream with a hub checkpoint only after that
// checkpoint is restored into a scratch hub and reproduces the live state
// exactly. Nothing here names a protocol, a family or the hub package: the
// hub is reached only through ManagedHub.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Managed recovery bounds. The stream bound leaves room in the 64 MiB control
// namespace for the mark, the driver records and their typed slots.
const (
	// RecoveryStreamFormat names the version-1 managed write-ahead stream
	// carried in the D5 payload.
	RecoveryStreamFormat = "helianthus.runtime-control.managed-wal/v1"
	// MaxStagedAssets bounds the assets of one staged operation.
	MaxStagedAssets = 128
	// MaxStagedBytes bounds the staged bytes of one operation.
	MaxStagedBytes = 8 << 20
	// MaxPendingOperations bounds the intents that have no durable outcome.
	MaxPendingOperations = 128
	// DefaultRecoveryStreamBytes is the default encoded stream bound.
	DefaultRecoveryStreamBytes = 48 << 20
	// DefaultCompactAfter is the entry count that triggers a compaction
	// attempt after an outcome commits.
	DefaultCompactAfter = 64
	// MaxManagedSources bounds the managed sources of one recorder.
	MaxManagedSources = 128
	// outcomeOverhead is the fixed per-asset outcome reservation in bytes.
	outcomeOverhead = 4096
)

const (
	entryPublication = "publication"
	entryControl     = "control"
)

// Managed recovery errors.
var (
	// ErrRecoveryNotRestored refuses a managed write before restoration.
	ErrRecoveryNotRestored = errors.New("runtimecontrol: managed recovery not restored")
	// ErrOutcomeNotDurable reports a hub result whose outcome transaction
	// failed; the intent stays pending until Flush succeeds or a restore
	// resolves it.
	ErrOutcomeNotDurable = errors.New("runtimecontrol: hub outcome not yet durable")
	// ErrOutcomeUncertain reports a hub call whose completion is unknown;
	// the source and its assets refuse further submissions in this process.
	ErrOutcomeUncertain = errors.New("runtimecontrol: hub outcome uncertain")
	// ErrCompaction reports a refused or unverified compaction; the previous
	// recovery stream is preserved unchanged.
	ErrCompaction = errors.New("runtimecontrol: compaction refused")
	// ErrInvalidStaging reports staged bytes that violate the staging rules.
	ErrInvalidStaging = errors.New("runtimecontrol: invalid staged batch")
)

// AssetID names one managed asset. It is opaque to the service.
type AssetID string

// StagedBatch is the exact staged envelope or control attempt of one asset.
// Key names the asset-scoped publication cursor (source, epoch and
// generation) in the hub's own encoding; Sequence and Digest are that
// cursor's next sequence and the batch digest. Meta is hub-owned material
// that restoration needs to reinstate a control gate; it is never
// interpreted here.
type StagedBatch struct {
	Asset    AssetID `json:"asset"`
	Key      string  `json:"key"`
	Sequence uint64  `json:"sequence"`
	Digest   string  `json:"digest"`
	Bytes    []byte  `json:"bytes"`
	Meta     []byte  `json:"meta,omitempty"`
}

// AcceptedStep is one accepted replay step: the exact accepted bytes, the
// original monotonic point, the hub's exact accepted result (snapshot
// identity, canonical bytes, revision vector and cursor, opaque here) and the
// hub commit position.
type AcceptedStep struct {
	Batch       StagedBatch `json:"batch"`
	Point       uint64      `json:"point"`
	Result      []byte      `json:"result"`
	Incarnation string      `json:"incarnation"`
	HubSeq      HubSeq      `json:"hub_seq"`
}

// AssetOutcome is the hub's per-asset result of one call. Exactly one of
// Accepted, a nonempty ErrorID or Uncertain is set, except for a control that
// produced no kernel batch on the asset (never attempted, or nothing to
// fence), which sets none of them. Detail is hub-owned material that
// reinstates a rejected or unattempted control gate; it is never interpreted
// here.
type AssetOutcome struct {
	Asset     AssetID
	Accepted  *AcceptedStep
	ErrorID   string
	Detail    []byte
	Uncertain bool
}

// ManagedHub is the publication hub as seen by the recorder. Every method is
// called outside the recorder's memory lock; the hub must never call back
// into a Writer.
type ManagedHub interface {
	// Publish submits the exact staged bytes of one asset at point.
	Publish(ctx context.Context, source SourceID, batch StagedBatch, point uint64) AssetOutcome
	// Control runs one control operation over every asset of source at
	// point. before receives each asset's exact staged attempt outside every
	// hub lock and before that asset is submitted; an error refuses that
	// asset's submission.
	Control(ctx context.Context, source SourceID, spec []byte, point uint64, before func(StagedBatch) error) []AssetOutcome
	// Frame exports the hub-only history (admissions, retirements and
	// ownership) of the managed sources as opaque bytes.
	Frame() ([]byte, error)
	// Checkpoint exports a complete restore point of every managed asset.
	Checkpoint() ([]byte, error)
	// Fingerprint returns exact comparable state of every managed asset.
	Fingerprint() (map[AssetID][]byte, error)
	// Replay opens a detached replay of base (a checkpoint, or nil) with the
	// history frame (or nil to use the checkpoint's). A scratch replay
	// installs into a fresh hub; otherwise it installs into the live hub.
	Replay(base, frame []byte, scratch bool) (Replay, error)
	// Reproject requests a fresh reprojection of a restored asset; nil means
	// the reprojection acknowledged settlement and reads reopened.
	Reproject(asset AssetID) error
}

// Replay is one detached restoration in progress.
type Replay interface {
	// Failed names the assets whose checkpoint replay failed verification.
	Failed() map[AssetID]error
	// Cursor returns the replayed cursor of key on asset: its last accepted
	// sequence (0 when unseen) and that batch's digest.
	Cursor(asset AssetID, key string) (uint64, string, error)
	// Apply replays one recorded accepted step and verifies its result.
	Apply(step AcceptedStep) error
	// Rejected reinstates a recorded rejection with its verbatim identifier,
	// or a control that produced no batch (empty identifier), with its hub
	// detail. It never applies the batch.
	Rejected(kind string, asset AssetID, staged *StagedBatch, point uint64, errorID string, detail []byte)
	// Resubmit applies uncertain exact bytes at their original point.
	Resubmit(kind string, batch StagedBatch, point uint64) AssetOutcome
	// Install restores every replayed asset except failed, returning the
	// fingerprints of installed assets and the assets the hub refused.
	Install(failed map[AssetID]bool) (map[AssetID][]byte, map[AssetID]error)
}

// RecoveryClock supplies process monotonic samples and restart-comparable
// elapsed-time anchors in nanoseconds. Elapsed must fail when continuity
// cannot be verified, including a backward or foreign anchor.
type RecoveryClock interface {
	Sample() (uint64, error)
	Anchor(sample uint64) ([]byte, error)
	Elapsed(anchor []byte, sample uint64) (uint64, error)
}

// EmptyRestorer is an optional RecoveryAdapter extension. Start calls
// RestoreEmpty when no D5 material is committed, so the adapter opens its
// managed writers only after the service's recovery step.
type EmptyRestorer interface {
	RestoreEmpty(ctx context.Context) error
}

// RestoreError names the drivers, sources and assets a restoration failed
// closed. Start withholds only the named drivers.
type RestoreError struct {
	Drivers []DriverID
	Sources []SourceID
	Assets  []AssetID
	Err     error
}

func (e *RestoreError) Error() string {
	return fmt.Sprintf("runtimecontrol: managed recovery failed closed for drivers %v (sources %v, assets %v): %v", e.Drivers, e.Sources, e.Assets, e.Err)
}

func (e *RestoreError) Unwrap() error { return e.Err }

// StepRef names one resolved step of a restoration.
type StepRef struct {
	Source  SourceID
	WAL     uint64
	Asset   AssetID
	Ordinal uint64
	ErrorID string
}

// RestoreReport is the in-memory result of the last restoration. It is
// diagnostic only and never persisted.
type RestoreReport struct {
	Restored     []AssetID
	Reopened     []AssetID
	Failed       []AssetID
	FailedSource []SourceID
	Resubmitted  []StepRef
	ReadBack     []StepRef
	Rejected     []StepRef
	Unsubmitted  []StepRef
}

// ManagedSource is one registered, service-routed managed source with its
// complete owned asset set. Each asset belongs to exactly one source.
type ManagedSource struct {
	Driver DriverID
	Source SourceID
	Assets []AssetID
}

// RecorderOptions configures a Recorder.
type RecorderOptions struct {
	Store   Store
	Hub     ManagedHub
	Clock   RecoveryClock
	Sources []ManagedSource
	// MaxStreamBytes bounds the encoded stream, including reservations;
	// zero selects DefaultRecoveryStreamBytes.
	MaxStreamBytes int
	// CompactAfter is the entry count that triggers a compaction attempt;
	// zero selects DefaultCompactAfter and a negative value disables it.
	CompactAfter int
	OnError      func(error)
}

// gate is a service-side serialization gate that honours cancellation.
type gate chan struct{}

func (g gate) lock(ctx context.Context) error {
	select {
	case g <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g gate) unlock() { <-g }

type managedSource struct {
	driver DriverID
	id     SourceID
	assets []AssetID
	gate   gate
}

type managedAsset struct {
	id     AssetID
	source SourceID
	gate   gate
}

// clockMap is one asset's durable clock mapping for this process.
type clockMap struct {
	origin uint64 // H + d
	m0     uint64
	last   uint64
	failed bool
}

type walStream struct {
	Format      string                `json:"format"`
	Base        []byte                `json:"base,omitempty"`
	BaseWAL     map[SourceID]uint64   `json:"base_wal"`
	BaseOrdinal map[AssetID]uint64    `json:"base_ordinal"`
	NextWAL     map[SourceID]uint64   `json:"next_wal"`
	NextOrdinal map[AssetID]uint64    `json:"next_ordinal"`
	Clocks      map[AssetID]clockMark `json:"clocks"`
	Frame       []byte                `json:"frame,omitempty"`
	Entries     []walEntry            `json:"entries"`
}

type clockMark struct {
	High   uint64 `json:"high"`
	Anchor []byte `json:"anchor"`
	Unit   string `json:"unit"`
}

type walEntry struct {
	Kind   string     `json:"kind"`
	Source SourceID   `json:"source"`
	WAL    uint64     `json:"wal"`
	Point  uint64     `json:"point"`
	Spec   []byte     `json:"spec,omitempty"`
	Assets []walAsset `json:"assets"`
}

type walAsset struct {
	Asset   AssetID      `json:"asset"`
	Ordinal uint64       `json:"ordinal"`
	Staged  *StagedBatch `json:"staged,omitempty"`
	Outcome *walOutcome  `json:"outcome,omitempty"`
}

type walOutcome struct {
	Accepted *AcceptedStep `json:"accepted,omitempty"`
	ErrorID  string        `json:"error_id,omitempty"`
	Detail   []byte        `json:"detail,omitempty"`
	NoBatch  bool          `json:"no_batch,omitempty"`
}

const clockUnit = "ns"

func newStream() walStream {
	return walStream{Format: RecoveryStreamFormat, BaseWAL: map[SourceID]uint64{}, BaseOrdinal: map[AssetID]uint64{},
		NextWAL: map[SourceID]uint64{}, NextOrdinal: map[AssetID]uint64{}, Clocks: map[AssetID]clockMark{}, Entries: []walEntry{}}
}

func (s walStream) encode() ([]byte, error) { return json.Marshal(s) }

func (s walStream) clone() walStream {
	raw, err := s.encode()
	if err != nil {
		return newStream()
	}
	out, err := decodeStream(raw)
	if err != nil {
		return newStream()
	}
	return out
}

func decodeStream(raw []byte) (walStream, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var s walStream
	if err := dec.Decode(&s); err != nil {
		return walStream{}, err
	}
	if dec.More() {
		return walStream{}, errors.New("trailing data")
	}
	if s.Format != RecoveryStreamFormat {
		return walStream{}, errors.New("unsupported stream format")
	}
	if s.BaseWAL == nil || s.BaseOrdinal == nil || s.NextWAL == nil || s.NextOrdinal == nil || s.Clocks == nil || s.Entries == nil {
		return walStream{}, errors.New("incomplete stream")
	}
	return s, nil
}

func (s *walStream) entry(source SourceID, wal uint64) *walEntry {
	for i := len(s.Entries) - 1; i >= 0; i-- {
		if s.Entries[i].Source == source && s.Entries[i].WAL == wal {
			return &s.Entries[i]
		}
	}
	return nil
}

func (s *walStream) pending() int {
	n := 0
	for _, e := range s.Entries {
		for _, a := range e.Assets {
			if a.Outcome == nil {
				n++
				break
			}
		}
	}
	return n
}

type queuedOutcome struct {
	source SourceID
	wal    uint64
	outs   []AssetOutcome
}

// Recorder is the managed write-ahead recorder and the service's D5
// RecoveryAdapter.
type Recorder struct {
	store        Store
	hub          ManagedHub
	clock        RecoveryClock
	onError      func(error)
	maxBytes     int
	compactAfter int
	sources      map[SourceID]*managedSource
	assets       map[AssetID]*managedAsset
	drivers      []DriverID

	// writeMu serializes durable stream writes. It is a service-side lock,
	// never a hub, manager, view or driver lock, and nothing acquires a
	// gate while holding it.
	writeMu sync.Mutex
	// flushMu serializes Flush end to end. It is taken before writeMu and
	// mu and never while holding either.
	flushMu sync.Mutex
	// mu guards the fields below and is never held across a store or hub
	// call.
	mu            sync.Mutex
	stream        walStream
	queued        []queuedOutcome
	restoring     bool
	restored      bool
	resolved      int
	failedSources map[SourceID]bool
	failedAssets  map[AssetID]bool
	blocked       map[SourceID]bool
	blockedAssets map[AssetID]bool
	clocks        map[AssetID]*clockMap
	report        RestoreReport
	// lastResult and lastStaged (largest seen sizes) size the outcome
	// reservation per asset.
	lastResult map[AssetID]int
	lastStaged map[AssetID]int
}

// NewRecorder validates the configuration and builds a Recorder. It performs
// no store, hub or clock call.
func NewRecorder(opts RecorderOptions) (*Recorder, error) {
	if isNil(opts.Store) || isNil(opts.Hub) || isNil(opts.Clock) {
		return nil, fmt.Errorf("%w: recorder needs a store, a hub and a clock", ErrInvalidConfig)
	}
	if len(opts.Sources) == 0 || len(opts.Sources) > MaxManagedSources {
		return nil, fmt.Errorf("%w: managed source count must be 1 to %d", ErrInvalidConfig, MaxManagedSources)
	}
	if opts.MaxStreamBytes < 0 || opts.MaxStreamBytes > maxControlNamespaceBytes {
		return nil, fmt.Errorf("%w: invalid stream bound", ErrInvalidConfig)
	}
	r := &Recorder{store: opts.Store, hub: opts.Hub, clock: opts.Clock, onError: opts.OnError,
		maxBytes: opts.MaxStreamBytes, compactAfter: opts.CompactAfter,
		sources: map[SourceID]*managedSource{}, assets: map[AssetID]*managedAsset{},
		stream: newStream(), failedSources: map[SourceID]bool{}, failedAssets: map[AssetID]bool{},
		blocked: map[SourceID]bool{}, blockedAssets: map[AssetID]bool{}, clocks: map[AssetID]*clockMap{},
		lastResult: map[AssetID]int{}, lastStaged: map[AssetID]int{}}
	if r.maxBytes == 0 {
		r.maxBytes = DefaultRecoveryStreamBytes
	}
	if r.compactAfter == 0 {
		r.compactAfter = DefaultCompactAfter
	}
	drivers := map[DriverID]bool{}
	for _, src := range opts.Sources {
		if !canonicalDriverID(src.Driver) || !validSourceID(src.Source) {
			return nil, fmt.Errorf("%w: invalid managed driver or source", ErrInvalidConfig)
		}
		if _, dup := r.sources[src.Source]; dup {
			return nil, fmt.Errorf("%w: duplicate managed source %s", ErrInvalidConfig, src.Source)
		}
		if len(src.Assets) == 0 || len(src.Assets) > MaxStagedAssets {
			return nil, fmt.Errorf("%w: managed source %s needs 1 to %d assets", ErrInvalidConfig, src.Source, MaxStagedAssets)
		}
		ms := &managedSource{driver: src.Driver, id: src.Source, gate: make(gate, 1)}
		for _, id := range src.Assets {
			if id == "" {
				return nil, fmt.Errorf("%w: empty asset id", ErrInvalidConfig)
			}
			if _, dup := r.assets[id]; dup {
				return nil, fmt.Errorf("%w: asset %s owned twice", ErrInvalidConfig, id)
			}
			r.assets[id] = &managedAsset{id: id, source: src.Source, gate: make(gate, 1)}
			ms.assets = append(ms.assets, id)
		}
		sort.Slice(ms.assets, func(i, j int) bool { return ms.assets[i] < ms.assets[j] })
		r.sources[src.Source] = ms
		drivers[src.Driver] = true
	}
	for id := range drivers {
		r.drivers = append(r.drivers, id)
	}
	sort.Slice(r.drivers, func(i, j int) bool { return r.drivers[i] < r.drivers[j] })
	return r, nil
}

// maxControlNamespaceBytes mirrors the control namespace limit.
const maxControlNamespaceBytes = 64 << 20

func (r *Recorder) reportErr(err error) {
	if err != nil && r.onError != nil {
		r.onError(err)
	}
}

// Writer is the durable path of one managed source.
type Writer struct {
	r      *Recorder
	source *managedSource
}

// Writer returns the durable path of a managed source.
func (r *Recorder) Writer(source SourceID) (*Writer, error) {
	ms, ok := r.sources[source]
	if !ok {
		return nil, fmt.Errorf("%w: unmanaged source %s", ErrInvalidConfig, source)
	}
	return &Writer{r: r, source: ms}, nil
}

// Report returns a copy of the last restoration report.
func (r *Recorder) Report() RestoreReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	rep := r.report
	rep.Restored = append([]AssetID(nil), rep.Restored...)
	rep.Reopened = append([]AssetID(nil), rep.Reopened...)
	rep.Failed = append([]AssetID(nil), rep.Failed...)
	rep.FailedSource = append([]SourceID(nil), rep.FailedSource...)
	rep.Resubmitted = append([]StepRef(nil), rep.Resubmitted...)
	rep.ReadBack = append([]StepRef(nil), rep.ReadBack...)
	rep.Rejected = append([]StepRef(nil), rep.Rejected...)
	rep.Unsubmitted = append([]StepRef(nil), rep.Unsubmitted...)
	return rep
}

// admit refuses a submission before restoration or on a failed or blocked
// path. r.mu must not be held.
func (r *Recorder) admit(source SourceID, assets []AssetID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.restored {
		return &Refusal{Code: RefusalFailedPrecondition, Reason: ErrRecoveryNotRestored.Error()}
	}
	if r.failedSources[source] {
		return &Refusal{Code: RefusalFailedPrecondition, Reason: "managed source failed closed by recovery"}
	}
	if r.blocked[source] {
		return &Refusal{Code: RefusalUnavailable, Reason: "uncertain hub completion blocks the source"}
	}
	for _, id := range assets {
		if r.failedAssets[id] || (r.clocks[id] != nil && r.clocks[id].failed) {
			return &Refusal{Code: RefusalFailedPrecondition, Reason: "managed asset failed closed by recovery"}
		}
		if r.blockedAssets[id] {
			return &Refusal{Code: RefusalUnavailable, Reason: "uncertain hub completion blocks the asset"}
		}
	}
	return nil
}

// acquire takes the source gate, then every asset gate in bytewise order. The
// returned release frees them in reverse order exactly once.
func (r *Recorder) acquire(ctx context.Context, src *managedSource, assets []AssetID) (func(), error) {
	sorted := append([]AssetID(nil), assets...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	if err := src.gate.lock(ctx); err != nil {
		return nil, &Refusal{Code: RefusalDeadlineExceeded, Reason: "source gate wait ended"}
	}
	held := make([]gate, 0, len(sorted))
	release := func() {
		for i := len(held) - 1; i >= 0; i-- {
			held[i].unlock()
		}
		src.gate.unlock()
	}
	for _, id := range sorted {
		g := r.assets[id].gate
		if err := g.lock(ctx); err != nil {
			release()
			return nil, &Refusal{Code: RefusalDeadlineExceeded, Reason: "asset gate wait ended"}
		}
		held = append(held, g)
	}
	var once sync.Once
	return func() { once.Do(release) }, nil
}

// nextPoint computes the next point of every asset from one sample, with the
// mapping max(H + d + (m - m0), last + 1). It returns the operation point
// (the maximum) and the anchor for it. r.mu must not be held.
func (r *Recorder) nextPoint(assets []AssetID) (uint64, []byte, error) {
	m, err := r.clock.Sample()
	if err != nil {
		return 0, nil, &Refusal{Code: RefusalUnavailable, Reason: "monotonic sample unavailable"}
	}
	anchor, err := r.clock.Anchor(m)
	if err != nil {
		return 0, nil, &Refusal{Code: RefusalUnavailable, Reason: "elapsed-time anchor unavailable"}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var point uint64
	for _, id := range assets {
		cm := r.clocks[id]
		if cm == nil || cm.failed {
			return 0, nil, &Refusal{Code: RefusalFailedPrecondition, Reason: "asset clock continuity unavailable"}
		}
		if m < cm.m0 {
			cm.failed = true
			return 0, nil, &Refusal{Code: RefusalFailedPrecondition, Reason: "backward process clock"}
		}
		p, ok := addCounter(cm.origin, m-cm.m0)
		if !ok {
			return 0, nil, &Refusal{Code: RefusalCounterExhausted, Reason: "clock arithmetic overflow"}
		}
		floor, ok := addCounter(cm.last, 1)
		if !ok {
			return 0, nil, &Refusal{Code: RefusalCounterExhausted, Reason: "clock arithmetic overflow"}
		}
		p = max(p, floor)
		point = max(point, p)
	}
	return point, anchor, nil
}

func addCounter(v, n uint64) (uint64, bool) {
	if v > ^uint64(0)-n {
		return 0, false
	}
	return v + n, true
}

func (r *Recorder) notePoint(assets []AssetID, point uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range assets {
		if cm := r.clocks[id]; cm != nil && point > cm.last {
			cm.last = point
		}
	}
}

// commit stages mutate on a copy of the committed stream, refreshes the
// history frame, checks the stream bound with the outcome reservation of
// every pending asset held back (when guard is set) and
// durably writes the envelope. Memory changes only after the write
// succeeded, so a failed transaction exposes no staging later.
func (r *Recorder) commit(ctx context.Context, guard bool, mutate func(*walStream) error) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	r.mu.Lock()
	next := r.stream.clone()
	err := mutate(&next)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	frame, err := r.hub.Frame()
	if err != nil {
		return &Refusal{Code: RefusalUnavailable, Reason: "hub history frame unavailable"}
	}
	next.Frame = frame
	payload, err := next.encode()
	if err != nil {
		return &Refusal{Code: RefusalUnavailable, Reason: "stream encoding failed"}
	}
	reserve := 0
	if guard {
		reserve = r.pendingReserve(next)
	}
	if len(payload) > r.maxBytes-reserve {
		return &Refusal{Code: RefusalResourceExhausted, Reason: "managed recovery stream bound reached"}
	}
	if err := r.store.CommitRecovery(ctx, &RecoveryMaterial{DriverIDs: append([]DriverID(nil), r.drivers...), Payload: payload}); err != nil {
		return err
	}
	r.mu.Lock()
	r.stream = next
	r.noteSizes(next)
	r.mu.Unlock()
	return nil
}

// noteSizes raises the per-asset reservation estimates to the largest staged
// and accepted sizes the stream holds for each asset. r.mu must be held.
func (r *Recorder) noteSizes(s walStream) {
	for i := len(s.Entries) - 1; i >= 0; i-- {
		for _, a := range s.Entries[i].Assets {
			if a.Staged != nil && r.lastStaged[a.Asset] < len(a.Staged.Bytes) {
				r.lastStaged[a.Asset] = len(a.Staged.Bytes)
			}
			if a.Outcome != nil && a.Outcome.Accepted != nil && r.lastResult[a.Asset] < len(a.Outcome.Accepted.Result) {
				r.lastResult[a.Asset] = len(a.Outcome.Accepted.Result)
			}
		}
	}
}

// noteBaseSizes raises the estimates after a restart. A compacted stream
// keeps no entries, so the sizes its folded operations had are unknown; the
// checkpoint bounds every asset's staged and result bytes from above. r.mu
// must be held.
func (r *Recorder) noteBaseSizes(s walStream) {
	n := len(s.Base)
	if n == 0 {
		return
	}
	for id := range r.assets {
		if r.lastStaged[id] < n {
			r.lastStaged[id] = n
		}
		if r.lastResult[id] < n {
			r.lastResult[id] = n
		}
	}
}

// pendingReserve is the outcome reservation of every pending asset attempt in
// s: the accepted step repeats the staged bytes in the step and in the
// result, and the result carries the asset's canonical snapshot, estimated
// from the largest result recorded for the asset. A control attempt that is
// not yet staged is estimated from the request and the largest staged bytes
// seen for the asset. Every byte field is base64-encoded in the stream.
func (r *Recorder) pendingReserve(s walStream) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	reserve := 0
	for _, e := range s.Entries {
		for _, a := range e.Assets {
			if a.Outcome != nil {
				continue
			}
			size := len(e.Spec) + r.lastStaged[a.Asset]
			if a.Staged != nil {
				size = len(a.Staged.Bytes) + len(a.Staged.Meta)
			}
			reserve += 4*size + 2*r.lastResult[a.Asset] + outcomeOverhead
		}
	}
	return reserve
}

// intent persists the intent entry and returns its WAL sequence.
func (r *Recorder) intent(ctx context.Context, kind string, src *managedSource, assets []AssetID, point uint64, anchor []byte, spec []byte, staged map[AssetID]*StagedBatch) (uint64, error) {
	var wal uint64
	err := r.commit(ctx, true, func(s *walStream) error {
		if s.pending() >= MaxPendingOperations {
			return &Refusal{Code: RefusalResourceExhausted, Reason: "pending managed operation bound reached"}
		}
		next := s.NextWAL[src.id]
		if next == 0 {
			next = s.BaseWAL[src.id] + 1
		}
		after, ok := addCounter(next, 1)
		if !ok {
			return &Refusal{Code: RefusalCounterExhausted, Reason: "WAL sequence exhausted"}
		}
		entry := walEntry{Kind: kind, Source: src.id, WAL: next, Point: point, Spec: append([]byte(nil), spec...)}
		ordinals := map[AssetID]uint64{}
		for _, id := range assets {
			ord := s.NextOrdinal[id]
			if ord == 0 {
				ord = s.BaseOrdinal[id] + 1
			}
			if _, ok := addCounter(ord, 1); !ok {
				return &Refusal{Code: RefusalCounterExhausted, Reason: "asset intent ordinal exhausted"}
			}
			ordinals[id] = ord
			wa := walAsset{Asset: id, Ordinal: ord}
			if b := staged[id]; b != nil {
				c := cloneStaged(*b)
				wa.Staged = &c
			}
			entry.Assets = append(entry.Assets, wa)
		}
		for id, ord := range ordinals {
			s.NextOrdinal[id] = ord + 1
			s.Clocks[id] = clockMark{High: point, Anchor: append([]byte(nil), anchor...), Unit: clockUnit}
		}
		s.NextWAL[src.id] = after
		s.Entries = append(s.Entries, entry)
		wal = next
		return nil
	})
	return wal, err
}

func cloneStaged(b StagedBatch) StagedBatch {
	b.Bytes = append([]byte(nil), b.Bytes...)
	b.Meta = append([]byte(nil), b.Meta...)
	if len(b.Meta) == 0 {
		b.Meta = nil
	}
	return b
}

// cloneOutcomes deep-copies outcomes so a queued outcome never shares
// pointers or byte slices with the caller's returned slice.
func cloneOutcomes(in []AssetOutcome) []AssetOutcome {
	out := make([]AssetOutcome, len(in))
	for i, o := range in {
		o.Accepted = cloneStep(o.Accepted)
		o.Detail = append([]byte(nil), o.Detail...)
		out[i] = o
	}
	return out
}

func cloneStep(s *AcceptedStep) *AcceptedStep {
	if s == nil {
		return nil
	}
	c := *s
	c.Batch = cloneStaged(s.Batch)
	c.Result = append([]byte(nil), s.Result...)
	return &c
}

func validStaged(b StagedBatch, asset AssetID) error {
	if b.Asset != asset || b.Key == "" || b.Sequence == 0 || b.Digest == "" || len(b.Bytes) == 0 {
		return fmt.Errorf("%w: incomplete staged batch for %s", ErrInvalidStaging, asset)
	}
	if len(b.Bytes)+len(b.Meta) > MaxStagedBytes {
		return &Refusal{Code: RefusalResourceExhausted, Reason: "staged bytes exceed the operation bound"}
	}
	return nil
}

// Publish stages one publication on asset and submits its exact bytes. stage
// receives the reserved monotonic point and must return the exact envelope;
// it runs under the source and asset gates and must not block. The returned
// outcome is the hub's; an error with an accepted outcome wraps
// ErrOutcomeNotDurable.
func (w *Writer) Publish(ctx context.Context, asset AssetID, stage func(point uint64) (StagedBatch, error)) (AssetOutcome, error) {
	r := w.r
	ma, ok := r.assets[asset]
	if !ok || ma.source != w.source.id {
		return AssetOutcome{}, fmt.Errorf("%w: asset %s is not owned by source %s", ErrInvalidConfig, asset, w.source.id)
	}
	assets := []AssetID{asset}
	if err := r.admit(w.source.id, assets); err != nil {
		return AssetOutcome{}, err
	}
	release, err := r.acquire(ctx, w.source, assets)
	if err != nil {
		return AssetOutcome{}, err
	}
	defer release()
	if err := r.admit(w.source.id, assets); err != nil {
		return AssetOutcome{}, err
	}
	point, anchor, err := r.nextPoint(assets)
	if err != nil {
		return AssetOutcome{}, err
	}
	staged, err := stage(point)
	if err != nil {
		return AssetOutcome{}, err
	}
	if err := validStaged(staged, asset); err != nil {
		return AssetOutcome{}, err
	}
	staged = cloneStaged(staged)
	wal, err := r.intent(ctx, entryPublication, w.source, assets, point, anchor, nil,
		map[AssetID]*StagedBatch{asset: &staged})
	if err != nil {
		return AssetOutcome{}, err
	}
	r.notePoint(assets, point)
	out := r.hub.Publish(ctx, w.source.id, cloneStaged(staged), point)
	out.Asset = asset
	if out.Uncertain || (out.Accepted == nil && out.ErrorID == "") {
		r.block(w.source.id, assets)
		return out, ErrOutcomeUncertain
	}
	if out.Accepted != nil && !bytes.Equal(out.Accepted.Batch.Bytes, staged.Bytes) {
		r.block(w.source.id, assets)
		return out, fmt.Errorf("%w: accepted bytes differ from the staged intent", ErrOutcomeUncertain)
	}
	release()
	return out, r.recordOutcome(ctx, w.source.id, wal, []AssetOutcome{out})
}

// Control runs one control operation over every asset of the source. spec is
// the hub's opaque control request. Each asset's exact attempt is persisted
// before that asset is submitted.
func (w *Writer) Control(ctx context.Context, spec []byte) ([]AssetOutcome, error) {
	r := w.r
	assets := append([]AssetID(nil), w.source.assets...)
	if err := r.admit(w.source.id, assets); err != nil {
		return nil, err
	}
	release, err := r.acquire(ctx, w.source, assets)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := r.admit(w.source.id, assets); err != nil {
		return nil, err
	}
	point, anchor, err := r.nextPoint(assets)
	if err != nil {
		return nil, err
	}
	if len(spec) > MaxStagedBytes {
		return nil, &Refusal{Code: RefusalResourceExhausted, Reason: "control request exceeds the operation bound"}
	}
	wal, err := r.intent(ctx, entryControl, w.source, assets, point, anchor, spec, nil)
	if err != nil {
		return nil, err
	}
	r.notePoint(assets, point)
	owned := map[AssetID]bool{}
	for _, id := range assets {
		owned[id] = true
	}
	var stageMu sync.Mutex
	stagedBytes := len(spec)
	stagedSet := map[AssetID]bool{}
	before := func(b StagedBatch) error {
		stageMu.Lock()
		if !owned[b.Asset] || stagedSet[b.Asset] {
			stageMu.Unlock()
			return fmt.Errorf("%w: unexpected or repeated attempt for %s", ErrInvalidStaging, b.Asset)
		}
		if err := validStaged(b, b.Asset); err != nil {
			stageMu.Unlock()
			return err
		}
		if stagedBytes+len(b.Bytes)+len(b.Meta) > MaxStagedBytes {
			stageMu.Unlock()
			return &Refusal{Code: RefusalResourceExhausted, Reason: "staged bytes exceed the operation bound"}
		}
		stagedBytes += len(b.Bytes) + len(b.Meta)
		stageMu.Unlock()
		c := cloneStaged(b)
		err := r.commit(ctx, true, func(s *walStream) error {
			e := s.entry(w.source.id, wal)
			if e == nil {
				return fmt.Errorf("%w: intent entry missing", ErrInvalidStaging)
			}
			for i := range e.Assets {
				if e.Assets[i].Asset == c.Asset {
					cc := cloneStaged(c)
					e.Assets[i].Staged = &cc
					return nil
				}
			}
			return fmt.Errorf("%w: asset not in intent", ErrInvalidStaging)
		})
		if err == nil {
			stageMu.Lock()
			stagedSet[b.Asset] = true
			stageMu.Unlock()
		}
		return err
	}
	outs := r.hub.Control(ctx, w.source.id, append([]byte(nil), spec...), point, before)
	byAsset := map[AssetID]AssetOutcome{}
	for _, out := range outs {
		if owned[out.Asset] {
			byAsset[out.Asset] = out
		}
	}
	final := make([]AssetOutcome, 0, len(assets))
	uncertain := false
	for _, id := range assets {
		out, ok := byAsset[id]
		if !ok || out.Uncertain {
			out = AssetOutcome{Asset: id, Uncertain: true}
			uncertain = true
		}
		stageMu.Lock()
		staged := stagedSet[id]
		stageMu.Unlock()
		if out.Accepted != nil && !staged {
			// An acceptance whose attempt was never durably staged has no
			// write-ahead record: the path is blocked as uncertain.
			out, uncertain = AssetOutcome{Asset: id, Uncertain: true}, true
		}
		final = append(final, out)
	}
	if uncertain {
		r.block(w.source.id, assets)
		return final, ErrOutcomeUncertain
	}
	release()
	return final, r.recordOutcome(ctx, w.source.id, wal, final)
}

func (r *Recorder) block(source SourceID, assets []AssetID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blocked[source] = true
	for _, id := range assets {
		r.blockedAssets[id] = true
	}
}

func applyOutcomes(s *walStream, source SourceID, wal uint64, outs []AssetOutcome) error {
	e := s.entry(source, wal)
	if e == nil {
		return fmt.Errorf("%w: intent entry missing for outcome", ErrInvalidStaging)
	}
	for _, out := range outs {
		for i := range e.Assets {
			if e.Assets[i].Asset != out.Asset {
				continue
			}
			o := &walOutcome{Accepted: cloneStep(out.Accepted), ErrorID: out.ErrorID}
			if out.Accepted == nil && len(out.Detail) > 0 {
				o.Detail = append([]byte(nil), out.Detail...)
			}
			if out.Accepted == nil && out.ErrorID == "" {
				o.NoBatch = true
			}
			e.Assets[i].Outcome = o
		}
	}
	return nil
}

// recordOutcome runs the second transaction. A failed write queues the
// outcome for Flush and leaves the intent pending.
func (r *Recorder) recordOutcome(ctx context.Context, source SourceID, wal uint64, outs []AssetOutcome) error {
	err := r.commit(ctx, false, func(s *walStream) error { return applyOutcomes(s, source, wal, outs) })
	if err != nil {
		r.mu.Lock()
		r.queued = append(r.queued, queuedOutcome{source: source, wal: wal, outs: cloneOutcomes(outs)})
		r.mu.Unlock()
		r.reportErr(err)
		return fmt.Errorf("%w: %w", ErrOutcomeNotDurable, err)
	}
	r.mu.Lock()
	entries := len(r.stream.Entries)
	r.mu.Unlock()
	if r.compactAfter > 0 && entries >= r.compactAfter {
		if cerr := r.Compact(ctx); cerr != nil {
			r.reportErr(cerr)
		}
	}
	return nil
}

// Flush retries every queued outcome transaction once.
func (r *Recorder) Flush(ctx context.Context) error {
	// flushMu serializes the whole snapshot, commit and removal so that
	// concurrent flushes never remove the same queued outcomes twice.
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	r.mu.Lock()
	queued := append([]queuedOutcome(nil), r.queued...)
	r.mu.Unlock()
	if len(queued) == 0 {
		return nil
	}
	err := r.commit(ctx, false, func(s *walStream) error {
		for _, q := range queued {
			if err := applyOutcomes(s, q.source, q.wal, q.outs); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrOutcomeNotDurable, err)
	}
	r.mu.Lock()
	r.queued = r.queued[len(queued):]
	r.mu.Unlock()
	return nil
}

// Secure implements RecoveryAdapter. The managed stream is written ahead by
// the recorder itself, so the service never commits a second copy: Secure
// makes every queued outcome durable and returns no material.
func (r *Recorder) Secure(ctx context.Context, _ DriverID, _ Transition) (*RecoveryMaterial, error) {
	if err := r.Flush(ctx); err != nil {
		return nil, err
	}
	return nil, nil
}

// RestoreEmpty implements EmptyRestorer: no material is committed, so every
// asset starts with an empty kernel and a fresh clock mapping.
func (r *Recorder) RestoreEmpty(ctx context.Context) error {
	return r.Restore(ctx, nil)
}

func (r *Recorder) beginRestore() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.restored || r.restoring {
		return fmt.Errorf("%w: managed recovery already restored", ErrServiceState)
	}
	r.restoring = true
	return nil
}

// Restore implements RecoveryAdapter. It runs before any source admission and
// returns a *RestoreError naming exactly the drivers, sources and assets it
// failed closed; healthy unrelated paths are restored and reprojected.
func (r *Recorder) Restore(ctx context.Context, material *RecoveryMaterial) error {
	if err := r.beginRestore(); err != nil {
		return err
	}
	defer func() {
		r.mu.Lock()
		r.restoring = false
		r.mu.Unlock()
	}()
	m0, err := r.clock.Sample()
	if err != nil {
		return r.failAll(fmt.Errorf("monotonic sample unavailable: %w", err))
	}
	stream := newStream()
	if material != nil {
		decoded, derr := decodeStream(material.Payload)
		if derr != nil {
			return r.failAll(fmt.Errorf("managed stream: %w", derr))
		}
		stream = decoded
	}
	failedSources, failedAssets, verr := r.validateStream(stream)
	if verr != nil {
		return r.failAll(verr)
	}
	clocks := r.restoreClocks(stream, m0, failedAssets)
	report := RestoreReport{}
	installed := map[AssetID]bool{}
	if material != nil {
		replay, rerr := r.hub.Replay(stream.Base, stream.Frame, false)
		if rerr != nil {
			return r.failAll(fmt.Errorf("replay: %w", rerr))
		}
		for id := range replay.Failed() {
			if _, ok := r.assets[id]; ok {
				failedAssets[id] = true
			}
		}
		r.resolveEntries(stream, replay, failedSources, failedAssets, &report)
		for src := range failedSources {
			for _, id := range r.sources[src].assets {
				failedAssets[id] = true
			}
		}
		_, refused := replay.Install(failedAssets)
		for id := range refused {
			failedAssets[id] = true
		}
		for id := range r.assets {
			if !failedAssets[id] {
				installed[id] = true
			}
		}
	} else {
		for id := range r.assets {
			installed[id] = true
		}
	}
	for id := range r.assets {
		if !installed[id] {
			continue
		}
		report.Restored = append(report.Restored, id)
		if material == nil {
			continue
		}
		if rerr := r.hub.Reproject(id); rerr == nil {
			report.Reopened = append(report.Reopened, id)
		}
	}
	for id := range failedAssets {
		report.Failed = append(report.Failed, id)
	}
	for id := range failedSources {
		report.FailedSource = append(report.FailedSource, id)
	}
	sortReport(&report)
	r.mu.Lock()
	r.stream = stream
	r.noteSizes(stream)
	r.noteBaseSizes(stream)
	r.clocks = clocks
	r.failedSources = failedSources
	r.failedAssets = failedAssets
	r.resolved = len(stream.Entries)
	r.report = report
	r.restored = true
	r.mu.Unlock()
	if len(failedAssets) > 0 || len(failedSources) > 0 {
		return r.restoreError(failedSources, failedAssets, errors.New("affected paths failed closed"))
	}
	if material != nil && len(stream.Entries) > 0 {
		// Fold the resolved stream into a verified restore point; a failure
		// keeps the old stream, which resolves identically on the next start.
		if cerr := r.Compact(ctx); cerr != nil {
			r.reportErr(cerr)
		}
	}
	return nil
}

func sortReport(rep *RestoreReport) {
	sortIDs := func(ids []AssetID) { sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] }) }
	sortIDs(rep.Restored)
	sortIDs(rep.Reopened)
	sortIDs(rep.Failed)
	sort.Slice(rep.FailedSource, func(i, j int) bool { return rep.FailedSource[i] < rep.FailedSource[j] })
}

// failAll fails every managed path closed after an unreadable stream. The
// live hub receives no partial install: every managed asset is refused.
func (r *Recorder) failAll(cause error) error {
	failedSources := map[SourceID]bool{}
	failedAssets := map[AssetID]bool{}
	for id, src := range r.sources {
		failedSources[id] = true
		for _, a := range src.assets {
			failedAssets[a] = true
		}
	}
	if replay, err := r.hub.Replay(nil, nil, false); err == nil {
		replay.Install(failedAssets)
	}
	r.mu.Lock()
	r.failedSources, r.failedAssets = failedSources, failedAssets
	r.report = RestoreReport{}
	for id := range failedAssets {
		r.report.Failed = append(r.report.Failed, id)
	}
	for id := range failedSources {
		r.report.FailedSource = append(r.report.FailedSource, id)
	}
	sortReport(&r.report)
	r.restored = true
	r.mu.Unlock()
	return r.restoreError(failedSources, failedAssets, cause)
}

func (r *Recorder) restoreError(failedSources map[SourceID]bool, failedAssets map[AssetID]bool, cause error) error {
	drivers := map[DriverID]bool{}
	out := &RestoreError{Err: cause}
	for id := range failedSources {
		drivers[r.sources[id].driver] = true
		out.Sources = append(out.Sources, id)
	}
	for id := range failedAssets {
		drivers[r.sources[r.assets[id].source].driver] = true
		out.Assets = append(out.Assets, id)
	}
	for id := range drivers {
		out.Drivers = append(out.Drivers, id)
	}
	sort.Slice(out.Drivers, func(i, j int) bool { return out.Drivers[i] < out.Drivers[j] })
	sort.Slice(out.Sources, func(i, j int) bool { return out.Sources[i] < out.Sources[j] })
	sort.Slice(out.Assets, func(i, j int) bool { return out.Assets[i] < out.Assets[j] })
	return out
}

// validateStream checks ownership and ordering material. A stream naming an
// unmanaged source or asset is refused as a whole; a WAL gap fails its source
// closed and an ordinal gap fails its asset closed.
func (r *Recorder) validateStream(s walStream) (map[SourceID]bool, map[AssetID]bool, error) {
	failedSources := map[SourceID]bool{}
	failedAssets := map[AssetID]bool{}
	for id := range s.BaseWAL {
		if r.sources[id] == nil {
			return nil, nil, fmt.Errorf("stream names unmanaged source %s", id)
		}
	}
	for id := range s.NextWAL {
		if r.sources[id] == nil {
			return nil, nil, fmt.Errorf("stream names unmanaged source %s", id)
		}
	}
	for _, m := range []map[AssetID]uint64{s.BaseOrdinal, s.NextOrdinal} {
		for id := range m {
			if r.assets[id] == nil {
				return nil, nil, fmt.Errorf("stream names unmanaged asset %s", id)
			}
		}
	}
	for id := range s.Clocks {
		if r.assets[id] == nil {
			return nil, nil, fmt.Errorf("stream names unmanaged asset %s", id)
		}
	}
	expectWAL := map[SourceID]uint64{}
	expectOrd := map[AssetID]uint64{}
	for id := range r.sources {
		expectWAL[id] = s.BaseWAL[id] + 1
	}
	for id := range r.assets {
		expectOrd[id] = s.BaseOrdinal[id] + 1
	}
	for _, e := range s.Entries {
		src := r.sources[e.Source]
		if src == nil {
			return nil, nil, fmt.Errorf("stream names unmanaged source %s", e.Source)
		}
		if e.WAL != expectWAL[e.Source] {
			failedSources[e.Source] = true
		}
		expectWAL[e.Source] = e.WAL + 1
		if (e.Kind != entryPublication && e.Kind != entryControl) || len(e.Assets) == 0 || (e.Kind == entryPublication && len(e.Assets) != 1) {
			failedSources[e.Source] = true
			continue
		}
		seen := map[AssetID]bool{}
		for _, a := range e.Assets {
			ma := r.assets[a.Asset]
			if ma == nil {
				return nil, nil, fmt.Errorf("stream names unmanaged asset %s", a.Asset)
			}
			if ma.source != e.Source || seen[a.Asset] {
				failedSources[e.Source] = true
				continue
			}
			seen[a.Asset] = true
			if a.Ordinal != expectOrd[a.Asset] {
				failedAssets[a.Asset] = true
			}
			expectOrd[a.Asset] = a.Ordinal + 1
			if !wellFormedAsset(e, a) {
				failedAssets[a.Asset] = true
			}
		}
	}
	for id := range r.sources {
		next := s.NextWAL[id]
		if next == 0 {
			next = s.BaseWAL[id] + 1
		}
		if next != expectWAL[id] {
			failedSources[id] = true
		}
	}
	for id := range r.assets {
		next := s.NextOrdinal[id]
		if next == 0 {
			next = s.BaseOrdinal[id] + 1
		}
		if next != expectOrd[id] {
			failedAssets[id] = true
		}
	}
	return failedSources, failedAssets, nil
}

func wellFormedAsset(e walEntry, a walAsset) bool {
	if e.Kind == entryPublication && a.Staged == nil {
		return false
	}
	if a.Staged != nil && (a.Staged.Asset != a.Asset || validStaged(*a.Staged, a.Asset) != nil) {
		return false
	}
	if a.Outcome == nil {
		return true
	}
	o := a.Outcome
	set := 0
	if o.Accepted != nil {
		set++
	}
	if o.ErrorID != "" {
		set++
	}
	if o.NoBatch {
		set++
	}
	if set != 1 {
		return false
	}
	if o.Accepted != nil {
		if a.Staged == nil || !bytes.Equal(o.Accepted.Batch.Bytes, a.Staged.Bytes) || o.Accepted.Point != e.Point ||
			o.Accepted.Incarnation == "" || o.Accepted.HubSeq == 0 || len(o.Accepted.Result) == 0 {
			return false
		}
	}
	return true
}

// restoreClocks rebuilds each asset's clock mapping from the durable mark and
// the verified elapsed time. An asset with recorded points but no verifiable
// continuity fails closed.
func (r *Recorder) restoreClocks(s walStream, m0 uint64, failedAssets map[AssetID]bool) map[AssetID]*clockMap {
	clocks := map[AssetID]*clockMap{}
	for id := range r.assets {
		mark, ok := s.Clocks[id]
		used := s.NextOrdinal[id] > s.BaseOrdinal[id]+1 || s.BaseOrdinal[id] > 0
		cm := &clockMap{m0: m0}
		switch {
		case !ok && !used:
		case !ok || mark.Unit != clockUnit || len(mark.Anchor) == 0:
			cm.failed = true
		default:
			d, err := r.clock.Elapsed(mark.Anchor, m0)
			origin, fits := addCounter(mark.High, d)
			if err != nil || !fits {
				cm.failed = true
			} else {
				cm.origin, cm.last = origin, mark.High
			}
		}
		if cm.failed {
			failedAssets[id] = true
		}
		clocks[id] = cm
	}
	return clocks
}

// resolveEntries replays the stream in intent order. Intent order respects
// both the per-source WAL order and the per-asset ordinal order, because
// every intent is persisted under its source gate and all its asset gates,
// so every pending predecessor is resolved before a dependent outcome.
func (r *Recorder) resolveEntries(s walStream, replay Replay, failedSources map[SourceID]bool, failedAssets map[AssetID]bool, report *RestoreReport) {
	failSource := func(src SourceID) {
		failedSources[src] = true
		for _, id := range r.sources[src].assets {
			failedAssets[id] = true
		}
	}
	for _, e := range s.Entries {
		if failedSources[e.Source] {
			continue
		}
		partial := false
		for _, a := range e.Assets {
			partial = partial || (e.Kind == entryControl && a.Staged != nil && a.Outcome == nil)
		}
		for _, a := range e.Assets {
			if failedAssets[a.Asset] {
				continue
			}
			ref := StepRef{Source: e.Source, WAL: e.WAL, Asset: a.Asset, Ordinal: a.Ordinal}
			switch {
			case a.Outcome != nil && a.Outcome.Accepted != nil:
				if err := replay.Apply(*cloneStep(a.Outcome.Accepted)); err != nil {
					failedAssets[a.Asset] = true
				}
			case a.Outcome != nil:
				replay.Rejected(e.Kind, a.Asset, a.Staged, e.Point, a.Outcome.ErrorID, a.Outcome.Detail)
				if a.Outcome.ErrorID != "" {
					ref.ErrorID = a.Outcome.ErrorID
					report.Rejected = append(report.Rejected, ref)
				}
			case a.Staged == nil && partial:
				// A sibling asset of this control was already staged and
				// may have been submitted, so the control's outstanding gate
				// for this asset cannot be restored: fail the asset closed.
				failedAssets[a.Asset] = true
			case a.Staged == nil:
				// The attempt was never durably staged, so it was never
				// submitted: the ordinal is consumed with no result.
				report.Unsubmitted = append(report.Unsubmitted, ref)
			default:
				seq, digest, err := replay.Cursor(a.Asset, a.Staged.Key)
				switch {
				case err != nil:
					failSource(e.Source)
				case seq == a.Staged.Sequence && digest == a.Staged.Digest:
					report.ReadBack = append(report.ReadBack, ref)
				case seq+1 == a.Staged.Sequence:
					out := replay.Resubmit(e.Kind, cloneStaged(*a.Staged), e.Point)
					switch {
					case out.Accepted != nil:
						report.Resubmitted = append(report.Resubmitted, ref)
					case out.ErrorID != "":
						ref.ErrorID = out.ErrorID
						report.Rejected = append(report.Rejected, ref)
					default:
						failSource(e.Source)
					}
				default:
					failSource(e.Source)
				}
			}
			if failedSources[e.Source] {
				break
			}
		}
	}
}

// Compact replaces the stream with a verified restore point. It quiesces
// every managed source and asset gate, refuses while any intent lacks a
// durable or restore-resolved outcome or any asset is failed closed, exports
// a checkpoint, restores it into a scratch hub and compares every asset's
// exact state with the live hub before one atomic replacement. Counters and
// clock marks carry over unchanged. Any refusal or verification failure
// leaves the previous stream durable and unchanged.
func (r *Recorder) Compact(ctx context.Context) error {
	if err := r.Flush(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrCompaction, err)
	}
	sources := make([]SourceID, 0, len(r.sources))
	for id := range r.sources {
		sources = append(sources, id)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
	assets := make([]AssetID, 0, len(r.assets))
	for id := range r.assets {
		assets = append(assets, id)
	}
	sort.Slice(assets, func(i, j int) bool { return assets[i] < assets[j] })
	var held []gate
	defer func() {
		for i := len(held) - 1; i >= 0; i-- {
			held[i].unlock()
		}
	}()
	for _, id := range sources {
		if err := r.sources[id].gate.lock(ctx); err != nil {
			return fmt.Errorf("%w: %w", ErrCompaction, err)
		}
		held = append(held, r.sources[id].gate)
	}
	for _, id := range assets {
		if err := r.assets[id].gate.lock(ctx); err != nil {
			return fmt.Errorf("%w: %w", ErrCompaction, err)
		}
		held = append(held, r.assets[id].gate)
	}
	r.mu.Lock()
	ok := r.restored && len(r.queued) == 0 && len(r.failedAssets) == 0 && len(r.failedSources) == 0 && len(r.blocked) == 0
	for i, e := range r.stream.Entries {
		if i < r.resolved {
			continue
		}
		for _, a := range e.Assets {
			if a.Outcome == nil {
				ok = false
			}
		}
	}
	r.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: unresolved, failed or blocked recovery material must be retained", ErrCompaction)
	}
	base, err := r.hub.Checkpoint()
	if err != nil {
		return fmt.Errorf("%w: checkpoint: %w", ErrCompaction, err)
	}
	scratch, err := r.hub.Replay(base, nil, true)
	if err != nil {
		return fmt.Errorf("%w: scratch replay: %w", ErrCompaction, err)
	}
	if failed := scratch.Failed(); len(failed) > 0 {
		return fmt.Errorf("%w: restore point failed verification", ErrCompaction)
	}
	got, refused := scratch.Install(nil)
	if len(refused) > 0 {
		return fmt.Errorf("%w: restore point refused by the scratch hub", ErrCompaction)
	}
	want, err := r.hub.Fingerprint()
	if err != nil {
		return fmt.Errorf("%w: live fingerprint: %w", ErrCompaction, err)
	}
	for _, id := range assets {
		if !bytes.Equal(got[id], want[id]) || got[id] == nil {
			return fmt.Errorf("%w: restore point does not reproduce asset %s", ErrCompaction, id)
		}
	}
	if err := r.commit(ctx, false, func(s *walStream) error {
		for _, id := range sources {
			next := s.NextWAL[id]
			if next == 0 {
				next = s.BaseWAL[id] + 1
			}
			s.BaseWAL[id] = next - 1
			s.NextWAL[id] = next
		}
		for _, id := range assets {
			next := s.NextOrdinal[id]
			if next == 0 {
				next = s.BaseOrdinal[id] + 1
			}
			s.BaseOrdinal[id] = next - 1
			s.NextOrdinal[id] = next
		}
		s.Base = append([]byte(nil), base...)
		s.Entries = []walEntry{}
		return nil
	}); err != nil {
		return fmt.Errorf("%w: %w", ErrCompaction, err)
	}
	r.mu.Lock()
	r.resolved = 0
	r.mu.Unlock()
	return nil
}

// Stream returns a detached copy of the committed stream's entry count and
// whether it holds a base restore point. It is a diagnostic seam.
func (r *Recorder) Stream() (entries int, hasBase bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.stream.Entries), len(r.stream.Base) > 0
}
