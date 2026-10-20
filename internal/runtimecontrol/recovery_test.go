package runtimecontrol

// Tests for service write-ahead recovery of service-routed managed writers
// (S3.7 to S3.9). Every fixture is synthetic: a two-asset qualified publisher
// of a synthetic pack, generic family driver IDs (fam.*), a synthetic
// restart-comparable clock and a temporary state file. The bridge from the
// in-process publication hub to the ManagedHub seam lives here, as the store
// bridge does in service_test.go: test imports stay outside the package's
// protocol-neutral production import graph. "Process loss" destroys the
// service, the recorder, the hub and the store manager together and rebuilds
// fresh instances with no shared memory over the same state file.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
)

const (
	synthFamily               = "synthetic"
	synthDriver      DriverID = "fam.synthetic"
	synthSource      SourceID = "source:synthetic"
	secondDriver     DriverID = "fam.second"
	secondSource     SourceID = "source:second"
	otherDriver               = "fam.other"
	otherSource               = "source:other"
	otherAsset                = "asset:other"
	synthClockID              = "clock:managed-durable"
	controlExportRef          = "semhub.control_export_refused"
)

var (
	synthAssets = []AssetID{"asset:synthetic-a", "asset:synthetic-b"}
	secondAsset = AssetID("asset:second-a")
	synthPack   = semreg.PackRef{ID: "helianthus.pack.recoverytest", Version: "1.0.0"}
)

// --- synthetic qualified publisher ---

func synthRef(id semreg.DefinitionID) semreg.DefinitionRef {
	return semreg.DefinitionRef{Pack: synthPack, ID: id, Version: "1.0.0"}
}

type synthValidator struct{ changed bool }

func (synthValidator) Pack() semreg.PackRef { return synthPack }
func (synthValidator) Definitions() semreg.DefinitionIndex {
	return semreg.DefinitionIndex{Pack: synthPack, Fields: []semreg.DefinitionRef{synthRef("recoverytest.value")},
		Services: []semreg.DefinitionRef{synthRef("recoverytest.service.main")}, Capabilities: []semreg.DefinitionRef{synthRef("recoverytest.capability.read")},
		Operations: []semreg.DefinitionRef{}, EffectRules: []semreg.DefinitionRef{}}
}
func (v synthValidator) ValidateFact(semreg.FactKey, *semreg.Value) error {
	if v.changed {
		return errors.New("synthetic validator changed")
	}
	return nil
}
func (synthValidator) ValidateService(semreg.ServiceInstance) error                { return nil }
func (synthValidator) ValidateCapability(semreg.CapabilityInstance) error          { return nil }
func (synthValidator) ValidateField(semreg.DefinitionRef, semreg.TypedField) error { return nil }
func (synthValidator) MatchConstraints(semreg.CapabilityInstance, []semreg.TypedField) error {
	return nil
}
func (synthValidator) EvaluatePredicate(semreg.FactCandidate, semreg.PredicateOp, semreg.Value) (bool, error) {
	return false, errors.New("unsupported")
}

func h32(parts ...string) string {
	d := sha256.New()
	for _, p := range parts {
		d.Write([]byte(p))
		d.Write([]byte{0})
	}
	return hex.EncodeToString(d.Sum(nil))[:32]
}

func wallAt(point uint64) semreg.TimePoint {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatUint(1_800_000_000_000_000_000+point, 10)), ClockID: "wall.utc", UncertaintyNS: "0"}
}

func monoAt(point uint64) semreg.MonotonicPoint {
	return semreg.MonotonicPoint{ClockEpochID: synthClockID, Nanoseconds: semreg.Uint64(strconv.FormatUint(point, 10))}
}

func synthEvidence(kind semreg.DefinitionID) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: synthPack.ID, Kind: kind, Digest: semreg.Digest("sha256:" + h32("evidence", string(kind)) + h32("evidence-2", string(kind))),
		Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

func decimalOf(value int64) semreg.Decimal {
	if value == 0 {
		return semreg.Decimal{Coefficient: "0"}
	}
	exponent := int32(0)
	for value%10 == 0 {
		value /= 10
		exponent++
	}
	return semreg.Decimal{Coefficient: strconv.FormatInt(value, 10), Exponent10: exponent}
}

func cursorOf(snapshot semreg.Snapshot, src semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64) (semreg.PublicationCursor, bool) {
	for _, c := range snapshot.Cursors {
		if c.SourceID == src && c.SourceEpochID == epoch && c.DriverGeneration == generation {
			return c, true
		}
	}
	return semreg.PublicationCursor{}, false
}

// synthBatch builds one publication of the synthetic family from a snapshot
// read outside any hub transaction: identity objects on the first batch of
// an epoch, one refreshed fact on every batch, and the retirement of every
// other current epoch of the source.
func synthBatch(t *testing.T, snapshot semreg.Snapshot, asset semreg.AssetID, h *semhub.Handle, seq semreg.Uint64, value int64, point uint64) semreg.PublicationBatch {
	t.Helper()
	epoch, source, generation := h.Epoch(), h.Source(), h.Generation()
	gen := semreg.Uint64(strconv.FormatUint(generation, 10))
	binding, err := semhub.NativeBindingID(synthFamily, asset, source, "recoverytest.profile.v1", epoch, generation)
	if err != nil {
		t.Fatal(err)
	}
	serviceID, err := semhub.NativeID(semhub.KindService, synthFamily, string(asset), "recoverytest.service.main", string(epoch), string(gen))
	if err != nil {
		t.Fatal(err)
	}
	capabilityID, err := semhub.NativeID(semhub.KindCapability, synthFamily, string(asset), "recoverytest.capability.read", string(epoch), string(gen))
	if err != nil {
		t.Fatal(err)
	}
	expected := semreg.Uint64("0")
	if snapshot.SnapshotID != "" {
		expected = snapshot.Revisions.Semantic
	}
	hasBinding, hasSource := false, false
	retirements := []semreg.SourceEpochID{}
	for _, b := range snapshot.Bindings {
		hasBinding = hasBinding || b.BindingID == binding
	}
	for _, s := range snapshot.Sources {
		hasSource = hasSource || s.SourceID == source && s.SourceEpochID == epoch
		if s.SourceID == source && s.State == semreg.SourceCurrent && s.SourceEpochID != epoch {
			retirements = append(retirements, s.SourceEpochID)
		}
	}
	sort.Slice(retirements, func(i, j int) bool { return retirements[i] < retirements[j] })
	evidence := synthEvidence("recoverytest.observation")
	candidateID := semreg.CandidateID("candidate:" + synthFamily + ":" + h32("candidate", string(asset), string(binding)))
	revision := semreg.Uint64("1")
	for _, envelope := range snapshot.Facts {
		for _, c := range envelope.Candidates {
			if c.CandidateID == candidateID {
				n, _ := strconv.ParseUint(string(c.Revision), 10, 64)
				revision = semreg.Uint64(strconv.FormatUint(n+1, 10))
			}
		}
	}
	dimension := string(asset)
	number := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: decimalOf(value), Unit: "unit.watt"}}
	fact := semreg.FactCandidate{CandidateID: candidateID, Key: semreg.FactKey{PackID: synthPack.ID, PackVersion: synthPack.Version, FactID: "recoverytest.value",
		Dimensions: []semreg.Dimension{{ID: "recoverytest.dimension.asset", Value: semreg.Value{Kind: semreg.ValueText, Text: &dimension}}}}, Value: &number,
		Quality: semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted,
			Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Times:           semreg.Times{ReceivedAt: wallAt(point), ReceiptMonotonic: monoAt(point), EvaluatedAt: wallAt(point), EvaluateMonotonic: monoAt(point)},
		FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:recovery-test", Version: "1.0.0", FreshForNS: "60000000000", RetainForNS: "300000000000", MaxWallUncertaintyNS: "0"},
		BindingID:       &binding, SourceEpochID: &epoch, DriverGeneration: &gen,
		Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:" + synthFamily + ":" + h32("origin", string(candidateID))), Kind: semreg.OriginNativeObservation,
			SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}},
		Evidence: []semreg.EvidenceRef{evidence}, Revision: revision}
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1,
		BatchID: semreg.BatchID("batch:" + synthFamily + ":" + h32("batch", string(asset), string(epoch), string(gen), string(seq))),
		AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: gen, Sequence: seq, ExpectedSemanticRevision: expected, ObservedAt: wallAt(point),
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: retirements, BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{},
		FactUpserts: []semreg.FactCandidate{fact}, FactWithdrawals: []semreg.CandidateID{}, ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{},
		CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	if !hasSource {
		batch.SourceUpserts = []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: "recoverytest_protocol", ProfileID: "recoverytest.profile.v1",
			ProfileVersion: "1", RegistryEvidence: synthEvidence("recoverytest.registry"), StartedAt: wallAt(0), State: semreg.SourceCurrent, Revision: "1"}}
	}
	if !hasBinding {
		batch.BindingUpserts = []semreg.NativeBinding{{BindingID: binding, AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: gen,
			NativeResource: synthEvidence("recoverytest.native"), State: semreg.BindingCurrent, Revision: "1"}}
		batch.IdentityLinkUpserts = []semreg.IdentityLink{{AssetID: asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: "1"}}
		batch.ServiceUpserts = []semreg.ServiceInstance{{InstanceID: semreg.ServiceInstanceID(serviceID), AssetID: asset, Definition: synthRef("recoverytest.service.main"),
			BindingID: binding, SourceEpochID: epoch, DriverGeneration: gen, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: "1"}}
		batch.CapabilityUpserts = []semreg.CapabilityInstance{{InstanceID: semreg.CapabilityInstanceID(capabilityID), AssetID: asset, ServiceInstance: semreg.ServiceInstanceID(serviceID),
			Definition: synthRef("recoverytest.capability.read"), BindingID: binding, SourceEpochID: epoch, DriverGeneration: gen, Qualification: semreg.QualificationQualified,
			Availability: semreg.AvailabilityAvailable, Constraints: []semreg.TypedField{}, ActivationEvidence: []semreg.EvidenceRef{evidence}, Revision: "1"}}
	}
	sealBatch(t, &batch)
	return batch
}

func sealBatch(t *testing.T, batch *semreg.PublicationBatch) {
	t.Helper()
	batch.BatchDigest = ""
	digest, err := batch.ComputedDigest()
	if err != nil {
		t.Fatal(err)
	}
	batch.BatchDigest = digest
}

func cursorKey(src semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64) string {
	return string(src) + "|" + string(epoch) + "|" + string(generation)
}

func parseKey(key string) (semreg.SourceID, semreg.SourceEpochID, semreg.Uint64, error) {
	parts := strings.SplitN(key, "|", 3)
	if len(parts) != 3 {
		return "", "", "", errors.New("malformed cursor key")
	}
	return semreg.SourceID(parts[0]), semreg.SourceEpochID(parts[1]), semreg.Uint64(parts[2]), nil
}

func stagedOf(t *testing.T, batch semreg.PublicationBatch) StagedBatch {
	t.Helper()
	raw, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := strconv.ParseUint(string(batch.Sequence), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return StagedBatch{Asset: AssetID(batch.AssetID), Key: cursorKey(batch.SourceID, batch.SourceEpochID, batch.DriverGeneration),
		Sequence: seq, Digest: string(batch.BatchDigest), Bytes: raw}
}

func errID(err error) string {
	var semErr *semreg.Error
	if errors.As(err, &semErr) && semErr.ID != "" {
		return string(semErr.ID)
	}
	return "recoverytest.hub_refused"
}

// --- the ManagedHub bridge over the in-process publication hub ---

type controlSpec struct {
	Kind       semhub.ControlKind   `json:"kind"`
	Epoch      semreg.SourceEpochID `json:"epoch"`
	Generation uint64               `json:"generation"`
}

type controlMeta struct {
	ID         semhub.ControlID     `json:"id"`
	Kind       semhub.ControlKind   `json:"kind"`
	Epoch      semreg.SourceEpochID `json:"epoch"`
	Generation uint64               `json:"generation"`
	Semantic   semhub.SemanticState `json:"semantic,omitempty"`
}

type collector struct {
	mu       sync.Mutex
	before   func(StagedBatch) error
	staged   map[AssetID]StagedBatch
	accepted map[AssetID]semhub.AcceptedBatch
}

// bridgeFrame is the hub-only history of the managed sources.
type bridgeFrame struct {
	Epochs  []semhub.EpochRecord                    `json:"epochs"`
	Regimes map[semreg.SourceID]semhub.LegacyFamily `json:"regimes"`
	Unfixed []semreg.SourceID                       `json:"unfixed"`
	Drivers map[semreg.SourceID]string              `json:"drivers"`
}

type hubBridge struct {
	t          *testing.T
	hub        *semhub.Hub
	sources    map[SourceID][]AssetID
	managed    map[AssetID]bool
	validators func(AssetID) []semreg.PackValidator
	withOther  bool

	mu       sync.Mutex
	handles  map[SourceID]*semhub.Handle
	collect  map[AssetID]*collector
	reproj   map[AssetID]int
	openSeen map[AssetID]bool

	// Test hooks. beforePublish and afterAccept may panic to model process
	// loss before the hub returns and after acceptance; onStaged runs after
	// a control attempt's intent is durable and before its submission;
	// onBeforeControl may refuse an attempt before the recorder sees it.
	beforePublish   func(StagedBatch)
	afterAccept     func(StagedBatch)
	onStaged        func(StagedBatch)
	onBeforeControl func(StagedBatch) error
	corruptScratch  bool
}

func newBridge(t *testing.T, sources map[SourceID][]AssetID, validators func(AssetID) []semreg.PackValidator, withOther bool) *hubBridge {
	t.Helper()
	b := &hubBridge{t: t, sources: sources, managed: map[AssetID]bool{}, validators: validators, withOther: withOther,
		handles: map[SourceID]*semhub.Handle{}, collect: map[AssetID]*collector{}, reproj: map[AssetID]int{}, openSeen: map[AssetID]bool{}}
	for _, assets := range sources {
		for _, id := range assets {
			b.managed[id] = true
		}
	}
	hub, err := semhub.New(semhub.Options{Recovery: &semhub.RecoveryHooks{BeforeControl: b.beforeControl, Accepted: b.accepted}})
	if err != nil {
		t.Fatal(err)
	}
	b.declare(hub, true)
	b.hub = hub
	return b
}

func (b *hubBridge) driverOf(src SourceID) string {
	if src == secondSource {
		return string(secondDriver)
	}
	return string(synthDriver)
}

// declare declares every managed asset (and the unmanaged direct publisher's
// asset) with the reprojection callback.
func (b *hubBridge) declare(hub *semhub.Hub, live bool) {
	b.t.Helper()
	ids := make([]SourceID, 0, len(b.sources))
	for src := range b.sources {
		ids = append(ids, src)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, src := range ids {
		for _, id := range b.sources[src] {
			asset := id
			_, err := hub.Declare(semhub.AssetSpec{AssetID: semreg.AssetID(asset), SourceID: semreg.SourceID(src), Driver: b.driverOf(src),
				Validators: b.validators(asset), OnControl: func(event semhub.ControlEvent) error {
					if live {
						b.mu.Lock()
						b.reproj[asset]++
						b.openSeen[asset] = b.openSeen[asset] || hub.Readable(event.Asset)
						b.mu.Unlock()
					}
					return nil
				}})
			if err != nil {
				b.t.Fatal(err)
			}
		}
	}
	if b.withOther {
		if _, err := hub.Declare(semhub.AssetSpec{AssetID: otherAsset, SourceID: otherSource, Driver: otherDriver,
			Validators: []semreg.PackValidator{synthValidator{}}, OnControl: func(semhub.ControlEvent) error { return nil }}); err != nil {
			b.t.Fatal(err)
		}
	}
}

func (b *hubBridge) setHandle(src SourceID, h *semhub.Handle) {
	b.mu.Lock()
	b.handles[src] = h
	b.mu.Unlock()
}

func (b *hubBridge) handle(src SourceID) *semhub.Handle {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.handles[src]
}

func (b *hubBridge) collector(id AssetID) *collector {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.collect[id]
}

func (b *hubBridge) beforeControl(at semhub.ControlAttempt) error {
	id := AssetID(at.Asset)
	col := b.collector(id)
	if col == nil {
		return nil
	}
	var batch semreg.PublicationBatch
	if err := json.Unmarshal(at.Attempt.Batch, &batch); err != nil {
		return err
	}
	gen, _ := strconv.ParseUint(string(batch.DriverGeneration), 10, 64)
	seq, _ := strconv.ParseUint(string(at.Attempt.Sequence), 10, 64)
	meta, _ := json.Marshal(controlMeta{ID: at.Control, Kind: at.Kind, Epoch: batch.SourceEpochID, Generation: gen})
	staged := StagedBatch{Asset: id, Key: cursorKey(batch.SourceID, batch.SourceEpochID, batch.DriverGeneration), Sequence: seq,
		Digest: string(at.Attempt.Digest), Bytes: append([]byte(nil), at.Attempt.Batch...), Meta: meta}
	col.mu.Lock()
	col.staged[id] = staged
	col.mu.Unlock()
	if b.onBeforeControl != nil {
		if err := b.onBeforeControl(staged); err != nil {
			return err
		}
	}
	if err := col.before(staged); err != nil {
		return err
	}
	if b.onStaged != nil {
		b.onStaged(staged)
	}
	return nil
}

func (b *hubBridge) accepted(rec semhub.AcceptedBatch) {
	var batch semreg.PublicationBatch
	if json.Unmarshal(rec.Batch, &batch) != nil {
		return
	}
	if col := b.collector(AssetID(batch.AssetID)); col != nil {
		col.mu.Lock()
		col.accepted[AssetID(batch.AssetID)] = rec
		col.mu.Unlock()
	}
}

func (b *hubBridge) Publish(_ context.Context, source SourceID, staged StagedBatch, point uint64) (out AssetOutcome) {
	out.Asset = staged.Asset
	defer func() {
		if recover() != nil {
			out = AssetOutcome{Asset: staged.Asset, Uncertain: true}
		}
	}()
	if b.beforePublish != nil {
		b.beforePublish(staged)
	}
	var batch semreg.PublicationBatch
	if err := json.Unmarshal(staged.Bytes, &batch); err != nil {
		out.ErrorID = "recoverytest.decode"
		return out
	}
	tx, err := b.hub.Begin(b.handle(source), semreg.AssetID(staged.Asset))
	if err != nil {
		out.ErrorID = errID(err)
		return out
	}
	defer tx.Abort()
	if _, _, err := tx.Apply(batch, monoAt(point)); err != nil {
		out.ErrorID = errID(err)
		return out
	}
	draft, err := tx.Export()
	if err != nil || !bytes.Equal(draft.Batch, staged.Bytes) {
		out.ErrorID = "recoverytest.bytes_changed"
		return out
	}
	var seq uint64
	outcome, err := tx.Commit(func(c semhub.Committed) { seq = c.HubSeq })
	if outcome != semhub.Accepted {
		if err == nil {
			err = errors.New("replayed")
		}
		out.ErrorID = errID(err)
		return out
	}
	if b.afterAccept != nil {
		b.afterAccept(staged)
	}
	draft.HubSeq = seq
	result, _ := json.Marshal(draft)
	out.Accepted = &AcceptedStep{Batch: staged, Point: point, Result: result, Incarnation: draft.Incarnation, HubSeq: HubSeq(seq)}
	return out
}

func (b *hubBridge) Control(_ context.Context, source SourceID, spec []byte, point uint64, before func(StagedBatch) error) (outs []AssetOutcome) {
	assets := b.sources[source]
	col := &collector{before: before, staged: map[AssetID]StagedBatch{}, accepted: map[AssetID]semhub.AcceptedBatch{}}
	b.mu.Lock()
	for _, id := range assets {
		b.collect[id] = col
	}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		for _, id := range assets {
			delete(b.collect, id)
		}
		b.mu.Unlock()
	}()
	uncertain := func() []AssetOutcome {
		all := make([]AssetOutcome, 0, len(assets))
		for _, id := range assets {
			all = append(all, AssetOutcome{Asset: id, Uncertain: true})
		}
		return all
	}
	defer func() {
		if recover() != nil {
			outs = uncertain()
		}
	}()
	var cs controlSpec
	if err := json.Unmarshal(spec, &cs); err != nil {
		return uncertain()
	}
	var res semhub.ControlResult
	var err error
	evidence := []semreg.EvidenceRef{synthEvidence("recoverytest.control")}
	switch cs.Kind {
	case semhub.ControlFence:
		res, err = b.hub.Fence(semhub.FenceRequest{Source: semreg.SourceID(source), Epoch: cs.Epoch, Generation: cs.Generation,
			Reason: "lifecycle.generation_replaced", Evidence: evidence, ObservedAt: wallAt(point), Monotonic: monoAt(point)})
	case semhub.ControlRetire:
		res, err = b.hub.RetireEpoch(semhub.RetireRequest{Source: semreg.SourceID(source), Epoch: cs.Epoch,
			Reason: "lifecycle.source_stopped", Evidence: evidence, ObservedAt: wallAt(point), Monotonic: monoAt(point)})
	default:
		return uncertain()
	}
	if len(res.Assets) == 0 {
		all := make([]AssetOutcome, 0, len(assets))
		for _, id := range assets {
			all = append(all, AssetOutcome{Asset: id, ErrorID: errID(err)})
		}
		return all
	}
	col.mu.Lock()
	defer col.mu.Unlock()
	for _, a := range res.Assets {
		id := AssetID(a.Asset)
		out := AssetOutcome{Asset: id}
		switch a.Outcome {
		case semhub.SemanticAccepted, semhub.SemanticReplayed:
			acc, okA := col.accepted[id]
			staged, okS := col.staged[id]
			if !okA || !okS {
				out.Uncertain = true
				break
			}
			result, _ := json.Marshal(acc)
			out.Accepted = &AcceptedStep{Batch: staged, Point: point, Result: result, Incarnation: acc.Incarnation, HubSeq: HubSeq(acc.HubSeq)}
		case semhub.SemanticRejected:
			out.ErrorID = errID(a.Err)
			if staged, ok := col.staged[id]; ok {
				out.Detail = staged.Meta
			}
		case semhub.SemanticRevokedUnpublished:
		case semhub.SemanticUnbuilt:
			// The operation stopped before this asset: nothing was staged or
			// submitted, and the hub keeps the asset behind the open control.
			out.Detail, _ = json.Marshal(controlMeta{ID: res.ID, Kind: res.Kind, Epoch: cs.Epoch, Generation: cs.Generation, Semantic: semhub.SemanticUnbuilt})
		default:
			out.Uncertain = true
		}
		outs = append(outs, out)
	}
	return outs
}

func (b *hubBridge) export(hub *semhub.Hub) (semhub.RecoveryRecord, error) {
	rec := hub.ExportRecovery()
	if rec.Version != 1 {
		return rec, errors.New("hub has no coherent checkpoint")
	}
	return rec, nil
}

func (b *hubBridge) managedSource(src semreg.SourceID) bool {
	_, ok := b.sources[SourceID(src)]
	return ok
}

func (b *hubBridge) frameOf(rec semhub.RecoveryRecord) bridgeFrame {
	f := bridgeFrame{Epochs: []semhub.EpochRecord{}, Regimes: map[semreg.SourceID]semhub.LegacyFamily{}, Unfixed: []semreg.SourceID{}, Drivers: map[semreg.SourceID]string{}}
	for _, e := range rec.Epochs {
		if b.managedSource(e.Source) {
			f.Epochs = append(f.Epochs, e)
		}
	}
	for id, fam := range rec.Regimes {
		if b.managedSource(id) {
			f.Regimes[id] = fam
		}
	}
	for _, id := range rec.Unfixed {
		if b.managedSource(id) {
			f.Unfixed = append(f.Unfixed, id)
		}
	}
	for id, d := range rec.Drivers {
		if b.managedSource(id) {
			f.Drivers[id] = d
		}
	}
	return f
}

func (b *hubBridge) Frame() ([]byte, error) {
	rec, err := b.export(b.hub)
	if err != nil {
		return nil, err
	}
	return json.Marshal(b.frameOf(rec))
}

func (b *hubBridge) Checkpoint() ([]byte, error) {
	rec, err := b.export(b.hub)
	if err != nil {
		return nil, err
	}
	f := b.frameOf(rec)
	out := semhub.RecoveryRecord{Version: 1, Epochs: f.Epochs, Regimes: f.Regimes, Unfixed: f.Unfixed, Drivers: f.Drivers}
	for _, a := range rec.Assets {
		if b.managed[AssetID(a.Asset)] {
			out.Assets = append(out.Assets, a)
		}
	}
	return json.Marshal(out)
}

type assetFingerprint struct {
	Accepted    []semhub.AcceptedBatch `json:"accepted"`
	SnapshotID  semreg.SnapshotID      `json:"snapshot_id"`
	Revisions   semreg.RevisionVector  `json:"revisions"`
	HubSeq      uint64                 `json:"hub_seq"`
	CommitCount int                    `json:"commit_count"`
	Control     *gateFingerprint       `json:"control,omitempty"`
	Canonical   []byte                 `json:"canonical"`
	Present     bool                   `json:"present"`
}

type gateFingerprint struct {
	Monotonic  semreg.MonotonicPoint `json:"monotonic"`
	Abandoned  bool                  `json:"abandoned"`
	ID         *semhub.ControlID     `json:"id"`
	Semantic   semhub.SemanticState  `json:"semantic"`
	ErrorID    semreg.ErrorID        `json:"error_id"`
	Attempts   []semhub.Attempt      `json:"attempts"`
	Kind       semhub.ControlKind    `json:"kind"`
	Epoch      semreg.SourceEpochID  `json:"epoch"`
	Generation uint64                `json:"generation"`
}

func (b *hubBridge) fingerprints(hub *semhub.Hub) (map[AssetID][]byte, error) {
	rec, err := b.export(hub)
	if err != nil {
		return nil, err
	}
	out := map[AssetID][]byte{}
	for _, a := range rec.Assets {
		id := AssetID(a.Asset)
		if !b.managed[id] || a.Failed {
			continue
		}
		view, _ := hub.Inspect(a.Asset)
		fp := assetFingerprint{Accepted: a.Accepted, SnapshotID: a.SnapshotID, Revisions: a.Revisions, HubSeq: a.HubSeq, CommitCount: a.CommitCount,
			Canonical: view.Canonical, Present: view.Present}
		if a.Control != nil {
			fp.Control = &gateFingerprint{Monotonic: a.Control.Monotonic, Abandoned: a.Control.Abandoned, ID: a.Control.Status.ID, Semantic: a.Control.Status.Semantic,
				ErrorID: a.Control.Status.ErrorID, Attempts: a.Control.Attempts, Kind: a.Control.Kind, Epoch: a.Control.Epoch, Generation: a.Control.Generation}
		}
		raw, _ := json.Marshal(fp)
		if b.corruptScratch && hub != b.hub {
			raw = append(raw, ' ')
		}
		out[id] = raw
	}
	return out, nil
}

func (b *hubBridge) Fingerprint() (map[AssetID][]byte, error) { return b.fingerprints(b.hub) }

func (b *hubBridge) Reproject(asset AssetID) error { return b.hub.Resettle(semreg.AssetID(asset)) }

type replayAsset struct {
	kernel *semreg.PublicationKernel
	log    []semhub.AcceptedBatch
	gate   *semhub.RecoveryControl
}

type bridgeReplay struct {
	b       *hubBridge
	target  *semhub.Hub
	history bridgeFrame
	states  map[AssetID]*replayAsset
	failed  map[AssetID]error
	inc     string
	seq     uint64
}

func (b *hubBridge) Replay(base, frame []byte, scratch bool) (Replay, error) {
	target := b.hub
	if scratch {
		fresh, err := semhub.New(semhub.Options{Recovery: &semhub.RecoveryHooks{}})
		if err != nil {
			return nil, err
		}
		b.declare(fresh, false)
		target = fresh
	}
	r := &bridgeReplay{b: b, target: target, states: map[AssetID]*replayAsset{}, failed: map[AssetID]error{}, inc: target.Incarnation() + ".resolved",
		history: bridgeFrame{Epochs: []semhub.EpochRecord{}, Regimes: map[semreg.SourceID]semhub.LegacyFamily{}, Unfixed: []semreg.SourceID{}, Drivers: map[semreg.SourceID]string{}}}
	for id := range b.managed {
		kernel, err := semreg.NewPublicationKernel(semreg.AssetID(id), b.validators(id)...)
		if err != nil {
			return nil, err
		}
		r.states[id] = &replayAsset{kernel: kernel}
	}
	if len(base) > 0 {
		var rec semhub.RecoveryRecord
		if err := json.Unmarshal(base, &rec); err != nil {
			return nil, err
		}
		r.history = bridgeFrame{Epochs: rec.Epochs, Regimes: rec.Regimes, Unfixed: rec.Unfixed, Drivers: rec.Drivers}
		for _, entry := range rec.Assets {
			id := AssetID(entry.Asset)
			st := r.states[id]
			if st == nil {
				return nil, fmt.Errorf("checkpoint names unmanaged asset %s", id)
			}
			st.gate = entry.Control
			if entry.Failed {
				r.failed[id] = errors.New("failed checkpoint")
				continue
			}
			for _, acc := range entry.Accepted {
				if err := r.applyAccepted(st, acc); err != nil {
					r.failed[id] = err
					break
				}
			}
		}
	}
	if len(frame) > 0 {
		var f bridgeFrame
		if err := json.Unmarshal(frame, &f); err != nil {
			return nil, err
		}
		r.history = f
	}
	return r, nil
}

func (r *bridgeReplay) applyAccepted(st *replayAsset, acc semhub.AcceptedBatch) error {
	var batch semreg.PublicationBatch
	if err := json.Unmarshal(acc.Batch, &batch); err != nil {
		return err
	}
	snap, canon, err := st.kernel.Apply(batch, acc.Monotonic)
	if err != nil {
		return err
	}
	cursor, _ := cursorOf(snap, batch.SourceID, batch.SourceEpochID, batch.DriverGeneration)
	if snap.SnapshotID != acc.SnapshotID || !reflect.DeepEqual(snap.Revisions, acc.Revisions) || !bytes.Equal(canon, acc.Canonical) || !reflect.DeepEqual(cursor, acc.Cursor) {
		return errors.New("accepted result changed")
	}
	st.log = append(st.log, acc)
	return nil
}

func (r *bridgeReplay) Failed() map[AssetID]error { return r.failed }

func (r *bridgeReplay) Cursor(asset AssetID, key string) (uint64, string, error) {
	st := r.states[asset]
	src, epoch, gen, err := parseKey(key)
	if st == nil || err != nil {
		return 0, "", errors.New("unknown asset or key")
	}
	snap, _, ok := st.kernel.Current()
	if !ok {
		return 0, "", nil
	}
	c, found := cursorOf(snap, src, epoch, gen)
	if !found {
		return 0, "", nil
	}
	seq, err := strconv.ParseUint(string(c.LastSequence), 10, 64)
	return seq, string(c.LastBatchDigest), err
}

func gateFrom(meta []byte, staged *StagedBatch, point uint64, semantic semhub.SemanticState, errorID string) (*semhub.RecoveryControl, error) {
	var m controlMeta
	if err := json.Unmarshal(meta, &m); err != nil {
		return nil, err
	}
	id := m.ID
	g := &semhub.RecoveryControl{Monotonic: monoAt(point), Status: semhub.ControlStatus{ID: &id, Semantic: semantic, ErrorID: semreg.ErrorID(errorID)},
		Kind: m.Kind, Epoch: m.Epoch, Generation: m.Generation, Attempts: []semhub.Attempt{}}
	if staged != nil {
		g.Attempts = append(g.Attempts, semhub.Attempt{Batch: append([]byte(nil), staged.Bytes...), Sequence: semreg.Uint64(strconv.FormatUint(staged.Sequence, 10)), Digest: semreg.Digest(staged.Digest)})
	}
	g.Status.Attempts = len(g.Attempts)
	return g, nil
}

func (r *bridgeReplay) Apply(step AcceptedStep) error {
	st := r.states[step.Batch.Asset]
	if st == nil {
		return errors.New("unknown asset")
	}
	var acc semhub.AcceptedBatch
	if err := json.Unmarshal(step.Result, &acc); err != nil {
		return err
	}
	if !bytes.Equal(acc.Batch, step.Batch.Bytes) || acc.Monotonic != monoAt(step.Point) || acc.HubSeq != uint64(step.HubSeq) || acc.Incarnation != step.Incarnation {
		return errors.New("accepted step does not match its result")
	}
	if err := r.applyAccepted(st, acc); err != nil {
		return err
	}
	if len(step.Batch.Meta) > 0 {
		g, err := gateFrom(step.Batch.Meta, &step.Batch, step.Point, semhub.SemanticAccepted, "")
		if err != nil {
			return err
		}
		st.gate = g
	}
	return nil
}

func (r *bridgeReplay) Rejected(kind string, asset AssetID, staged *StagedBatch, point uint64, errorID string, detail []byte) {
	st := r.states[asset]
	if st == nil || kind != entryControl {
		return
	}
	meta := detail
	if staged != nil && len(staged.Meta) > 0 {
		meta = staged.Meta
	}
	semantic := semhub.SemanticRejected
	if errorID == "" {
		var m controlMeta
		if json.Unmarshal(meta, &m) != nil || m.Semantic != semhub.SemanticUnbuilt {
			return
		}
		semantic = semhub.SemanticUnbuilt
	}
	if g, err := gateFrom(meta, staged, point, semantic, errorID); err == nil {
		st.gate = g
	}
}

func (r *bridgeReplay) Resubmit(kind string, staged StagedBatch, point uint64) AssetOutcome {
	st := r.states[staged.Asset]
	out := AssetOutcome{Asset: staged.Asset}
	var batch semreg.PublicationBatch
	if st == nil || json.Unmarshal(staged.Bytes, &batch) != nil {
		out.Uncertain = true
		return out
	}
	fork, err := st.kernel.Fork()
	if err != nil {
		out.Uncertain = true
		return out
	}
	snap, canon, err := fork.Apply(batch, monoAt(point))
	if err != nil {
		out.ErrorID = errID(err)
		if kind == entryControl {
			if g, gerr := gateFrom(staged.Meta, &staged, point, semhub.SemanticRejected, out.ErrorID); gerr == nil {
				st.gate = g
			}
		}
		return out
	}
	st.kernel = fork
	cursor, _ := cursorOf(snap, batch.SourceID, batch.SourceEpochID, batch.DriverGeneration)
	r.seq++
	acc := semhub.AcceptedBatch{Incarnation: r.inc, Batch: append([]byte(nil), staged.Bytes...), Monotonic: monoAt(point), SnapshotID: snap.SnapshotID,
		Canonical: canon, Revisions: snap.Revisions, Cursor: cursor, HubSeq: r.seq}
	st.log = append(st.log, acc)
	if kind == entryControl {
		if g, gerr := gateFrom(staged.Meta, &staged, point, semhub.SemanticAccepted, ""); gerr == nil {
			st.gate = g
		}
	}
	result, _ := json.Marshal(acc)
	out.Accepted = &AcceptedStep{Batch: staged, Point: point, Result: result, Incarnation: r.inc, HubSeq: HubSeq(r.seq)}
	return out
}

func (r *bridgeReplay) Install(failed map[AssetID]bool) (map[AssetID][]byte, map[AssetID]error) {
	refused := map[AssetID]error{}
	template, err := r.b.export(r.target)
	if err != nil {
		for id := range r.states {
			refused[id] = err
		}
		return nil, refused
	}
	record := semhub.RecoveryRecord{Version: 1, Epochs: r.history.Epochs, Regimes: r.history.Regimes, Unfixed: r.history.Unfixed, Drivers: r.history.Drivers}
	for _, entry := range template.Assets {
		id := AssetID(entry.Asset)
		if st, ok := r.states[id]; ok {
			entry.Accepted = st.log
			entry.CommitCount = len(st.log)
			if len(st.log) > 0 {
				head := st.log[len(st.log)-1]
				entry.SnapshotID, entry.Revisions, entry.HubSeq = head.SnapshotID, head.Revisions, head.HubSeq
			}
			entry.Control = st.gate
			entry.Failed = failed[id] || r.failed[id] != nil
		}
		record.Assets = append(record.Assets, entry)
	}
	restoreErr := r.target.Restore(record)
	after, err := r.b.export(r.target)
	if err != nil {
		for id := range r.states {
			refused[id] = err
		}
		return nil, refused
	}
	for _, entry := range after.Assets {
		id := AssetID(entry.Asset)
		if _, ok := r.states[id]; !ok || failed[id] {
			continue
		}
		// An installed asset holds a restored gate that awaits a fresh
		// reprojection, so its reads are closed; an asset the hub left
		// untouched (a refused record) still has no gate.
		if entry.Failed || r.target.ReadAdmitted(entry.Asset) {
			refused[id] = fmt.Errorf("hub refused asset: %v", restoreErr)
		}
	}
	fps, err := r.b.fingerprints(r.target)
	if err != nil {
		for id := range r.states {
			refused[id] = err
		}
		return nil, refused
	}
	return fps, refused
}

// --- synthetic restart-comparable clock ---

type synthHost struct {
	mu   sync.Mutex
	boot string
	now  uint64
}

func (h *synthHost) advance(d time.Duration) {
	h.mu.Lock()
	h.now += uint64(d)
	h.mu.Unlock()
}

func (h *synthHost) read() (string, uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.boot, h.now
}

// synthClock is one process's clock: its samples start at 0 when the process
// starts, below every persisted point, and anchors carry host elapsed time.
type synthClock struct {
	host   *synthHost
	offset uint64
	boot   string
}

func newSynthClock(host *synthHost) *synthClock {
	boot, now := host.read()
	return &synthClock{host: host, offset: now, boot: boot}
}

type synthAnchor struct {
	Boot string `json:"boot"`
	Host uint64 `json:"host"`
}

func (c *synthClock) Sample() (uint64, error) {
	_, now := c.host.read()
	return now - c.offset, nil
}

func (c *synthClock) Anchor(sample uint64) ([]byte, error) {
	return json.Marshal(synthAnchor{Boot: c.boot, Host: c.offset + sample})
}

func (c *synthClock) Elapsed(anchor []byte, sample uint64) (uint64, error) {
	var a synthAnchor
	if err := json.Unmarshal(anchor, &a); err != nil {
		return 0, err
	}
	if a.Boot != c.boot {
		return 0, errors.New("anchor from another host boot")
	}
	now := c.offset + sample
	if now < a.Host {
		return 0, errors.New("backward elapsed time")
	}
	return now - a.Host, nil
}

// --- store wrapper with injectable recovery-write faults and stalls ---

type faultStore struct {
	Store
	mu      sync.Mutex
	writes  int
	fail    func(payload []byte) error
	stall   func(payload []byte) bool
	entered chan struct{}
	release chan struct{}
	// after runs once the manager returned, before the recorder sees the
	// result. readbackEntered and readbackRelease hold every readback.
	after           func(err error)
	readbacks       int
	readbackEntered chan struct{}
	readbackRelease chan struct{}
}

func (s *faultStore) ResolveReadback(ctx context.Context) error {
	s.mu.Lock()
	s.readbacks++
	entered, release := s.readbackEntered, s.readbackRelease
	s.readbackEntered = nil
	s.mu.Unlock()
	if entered != nil {
		close(entered)
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Store.ResolveReadback(ctx)
}

func (s *faultStore) holdReadback() (entered, release chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readbackEntered, s.readbackRelease = make(chan struct{}), make(chan struct{})
	return s.readbackEntered, s.readbackRelease
}

func (s *faultStore) CommitRecovery(ctx context.Context, m *RecoveryMaterial) error {
	err := s.commitRecovery(ctx, m)
	s.mu.Lock()
	after := s.after
	s.after = nil
	s.mu.Unlock()
	if after != nil {
		after(err)
	}
	return err
}

func (s *faultStore) commitRecovery(ctx context.Context, m *RecoveryMaterial) error {
	s.mu.Lock()
	s.writes++
	fail, stall := s.fail, s.stall
	entered, release := s.entered, s.release
	s.mu.Unlock()
	if m != nil && fail != nil {
		if err := fail(m.Payload); err != nil {
			return err
		}
	}
	if m != nil && stall != nil && stall(m.Payload) {
		s.mu.Lock()
		s.stall = nil
		s.mu.Unlock()
		close(entered)
		<-release
	}
	return s.Store.CommitRecovery(ctx, m)
}

func (s *faultStore) setFail(fn func([]byte) error) {
	s.mu.Lock()
	s.fail = fn
	s.mu.Unlock()
}

func (s *faultStore) armStall(fn func([]byte) bool) (entered, release chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stall, s.entered, s.release = fn, make(chan struct{}), make(chan struct{})
	return s.entered, s.release
}

func (s *faultStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}

// --- the world: store manager, hub, recorder and optional service ---

type worldOpts struct {
	second   bool
	other    bool
	changed  map[AssetID]bool
	maxBytes int
	compact  int
}

type world struct {
	t      *testing.T
	path   string
	host   *synthHost
	opts   worldOpts
	m      *runtimestate.Manager
	hooks  *blockHooks
	store  *faultStore
	bridge *hubBridge
	clock  *synthClock
	rec    *Recorder
	svc    *Service
	log    *orderLog
	errs   *errSink
	value  int64
	closed bool
}

func newWorld(t *testing.T, opts worldOpts) *world {
	t.Helper()
	path := t.TempDir() + "/runtime_state.json"
	writeBase(t, path)
	w := &world{t: t, path: path, host: &synthHost{boot: "boot-1", now: uint64(1000 * time.Second)}, opts: opts}
	w.open()
	return w
}

func (w *world) sources() map[SourceID][]AssetID {
	s := map[SourceID][]AssetID{synthSource: append([]AssetID(nil), synthAssets...)}
	if w.opts.second {
		s[secondSource] = []AssetID{secondAsset}
	}
	return s
}

func (w *world) drivers() []runtimestate.ControlDriverConfig {
	ids := []string{string(synthDriver)}
	if w.opts.second {
		ids = append(ids, string(secondDriver))
	}
	return enabled(ids...)
}

// open builds fresh instances over the state file: nothing is shared with
// any earlier process.
func (w *world) open() {
	w.t.Helper()
	w.hooks = &blockHooks{}
	w.m = openManager(w.t, w.path, w.hooks, 0, w.drivers())
	w.store = &faultStore{Store: &managerStore{m: w.m}}
	changed := w.opts.changed
	w.bridge = newBridge(w.t, w.sources(), func(id AssetID) []semreg.PackValidator {
		return []semreg.PackValidator{synthValidator{changed: changed[id]}}
	}, w.opts.other)
	w.clock = newSynthClock(w.host)
	w.log, w.errs = &orderLog{}, &errSink{}
	var managed []ManagedSource
	for src, assets := range w.sources() {
		managed = append(managed, ManagedSource{Driver: DriverID(w.bridge.driverOf(src)), Source: src, Assets: assets})
	}
	rec, err := NewRecorder(RecorderOptions{Store: w.store, Hub: w.bridge, Clock: w.clock, Sources: managed,
		MaxStreamBytes: w.opts.maxBytes, CompactAfter: w.opts.compact, OnError: w.errs.add})
	if err != nil {
		w.t.Fatal(err)
	}
	w.rec = rec
	w.closed = false
	w.svc = nil
	m := w.m
	w.t.Cleanup(func() {
		_ = m.Stop(context.Background())
	})
}

// loseProcess destroys the service, recorder, hub and store manager memory
// and reopens fresh instances over the same state file and host clock.
func (w *world) loseProcess(downtime time.Duration) {
	w.t.Helper()
	if w.svc != nil {
		w.svc.Close()
	}
	_ = w.m.Stop(context.Background())
	w.host.advance(downtime)
	w.open()
}

func (w *world) material() *RecoveryMaterial { return w.store.Recovery() }

func (w *world) restore() error {
	w.t.Helper()
	if m := w.material(); m != nil {
		return w.rec.Restore(context.Background(), m)
	}
	return w.rec.RestoreEmpty(context.Background())
}

func (w *world) mustRestore() {
	w.t.Helper()
	if err := w.restore(); err != nil {
		w.t.Fatalf("restore: %v", err)
	}
}

func (w *world) admit(src SourceID) *semhub.Handle {
	w.t.Helper()
	h, err := w.bridge.hub.AdmitSource(semhub.Admission{Source: semreg.SourceID(src), Driver: w.bridge.driverOf(src)})
	if err != nil {
		w.t.Fatalf("admit %s: %v", src, err)
	}
	w.bridge.setHandle(src, h)
	return h
}

func (w *world) writer(src SourceID) *Writer {
	w.t.Helper()
	wr, err := w.rec.Writer(src)
	if err != nil {
		w.t.Fatal(err)
	}
	return wr
}

// stage returns the publisher's staging function: the batch is built from
// the asset's current snapshot and cursor outside any hub transaction.
func (w *world) stage(src SourceID, asset AssetID, mutate func(*semreg.PublicationBatch)) func(uint64) (StagedBatch, error) {
	w.value++
	value := w.value
	return func(point uint64) (StagedBatch, error) {
		h := w.bridge.handle(src)
		if h == nil {
			return StagedBatch{}, errors.New("no admission")
		}
		view, err := w.bridge.hub.Inspect(semreg.AssetID(asset))
		if err != nil {
			return StagedBatch{}, err
		}
		seq, err := w.bridge.hub.NextSequence(semreg.AssetID(asset), h.Source(), h.Epoch(), h.Generation())
		if err != nil {
			return StagedBatch{}, err
		}
		batch := synthBatch(w.t, view.Snapshot, semreg.AssetID(asset), h, seq, value*10, point)
		if mutate != nil {
			mutate(&batch)
			sealBatch(w.t, &batch)
		}
		return stagedOf(w.t, batch), nil
	}
}

func (w *world) publish(src SourceID, asset AssetID) (AssetOutcome, error) {
	w.t.Helper()
	w.host.advance(time.Second)
	return w.writer(src).Publish(context.Background(), asset, w.stage(src, asset, nil))
}

func (w *world) mustPublish(src SourceID, asset AssetID) AssetOutcome {
	w.t.Helper()
	out, err := w.publish(src, asset)
	if err != nil || out.Accepted == nil {
		w.t.Fatalf("publish %s: %+v %v", asset, out, err)
	}
	return out
}

func (w *world) control(src SourceID, kind semhub.ControlKind) ([]AssetOutcome, error) {
	w.t.Helper()
	w.host.advance(time.Second)
	h := w.bridge.handle(src)
	spec, _ := json.Marshal(controlSpec{Kind: kind, Epoch: h.Epoch(), Generation: h.Generation()})
	return w.writer(src).Control(context.Background(), spec)
}

func (w *world) stream() walStream {
	w.t.Helper()
	m := w.material()
	if m == nil {
		return newStream()
	}
	s, err := decodeStream(m.Payload)
	if err != nil {
		w.t.Fatalf("decode durable stream: %v", err)
	}
	return s
}

// rewrite replaces the durable stream directly through the store, as a
// corrupting writer or a removed record would.
func (w *world) rewrite(mutate func(*walStream)) {
	w.t.Helper()
	s := w.stream()
	mutate(&s)
	raw, err := json.Marshal(s)
	if err != nil {
		w.t.Fatal(err)
	}
	m := w.material()
	if err := w.store.Store.CommitRecovery(context.Background(), &RecoveryMaterial{DriverIDs: m.DriverIDs, Payload: raw}); err != nil {
		w.t.Fatal(err)
	}
}

// state is the comparable accepted state of one asset.
type assetState struct {
	Present    bool
	SnapshotID semreg.SnapshotID
	Canonical  []byte
	Revisions  semreg.RevisionVector
	Cursors    []semreg.PublicationCursor
	Batches    [][]byte
	Results    []semreg.SnapshotID
}

func stateOf(t *testing.T, hub *semhub.Hub, id AssetID) assetState {
	t.Helper()
	view, err := hub.Inspect(semreg.AssetID(id))
	if err != nil {
		t.Fatal(err)
	}
	st := assetState{Present: view.Present, SnapshotID: view.Snapshot.SnapshotID, Canonical: view.Canonical, Revisions: view.Revisions,
		Cursors: append([]semreg.PublicationCursor(nil), view.Snapshot.Cursors...)}
	rec := hub.ExportRecovery()
	for _, a := range rec.Assets {
		if AssetID(a.Asset) == id {
			for _, acc := range a.Accepted {
				st.Batches = append(st.Batches, acc.Batch)
				st.Results = append(st.Results, acc.SnapshotID)
			}
		}
	}
	return st
}

func (w *world) states(ids ...AssetID) map[AssetID]assetState {
	out := map[AssetID]assetState{}
	for _, id := range ids {
		out[id] = stateOf(w.t, w.bridge.hub, id)
	}
	return out
}

func assertStates(t *testing.T, what string, got, want map[AssetID]assetState) {
	t.Helper()
	for id, w := range want {
		g := got[id]
		if !reflect.DeepEqual(g, w) {
			t.Fatalf("%s: asset %s state differs\n got  present=%v id=%s rev=%+v cursors=%+v batches=%d\n want present=%v id=%s rev=%+v cursors=%+v batches=%d",
				what, id, g.Present, g.SnapshotID, g.Revisions, g.Cursors, len(g.Batches), w.Present, w.SnapshotID, w.Revisions, w.Cursors, len(w.Batches))
		}
	}
}

// --- synthetic lifecycle executor over the managed writer ---

type synthExec struct {
	w     *world
	src   SourceID
	mu    sync.Mutex
	calls []execCall
	start func(ctx context.Context, c LifecycleCorrelation) error
	stop  func(ctx context.Context, c LifecycleCorrelation) StopOutcome
}

func (e *synthExec) Start(ctx context.Context, c LifecycleCorrelation) error {
	e.mu.Lock()
	e.calls = append(e.calls, execCall{"start", c})
	fn := e.start
	e.mu.Unlock()
	e.w.log.add("start:" + string(e.src))
	if fn != nil {
		return fn(ctx, c)
	}
	e.w.admit(e.src)
	for _, id := range e.w.sources()[e.src] {
		if out, err := e.w.publish(e.src, id); err != nil || out.Accepted == nil {
			return fmt.Errorf("publish %s: %v %+v", id, err, out)
		}
	}
	return nil
}

func (e *synthExec) Stop(ctx context.Context, c LifecycleCorrelation) StopOutcome {
	e.mu.Lock()
	e.calls = append(e.calls, execCall{"stop", c})
	fn := e.stop
	e.mu.Unlock()
	e.w.log.add("stop:" + string(e.src))
	if fn != nil {
		return fn(ctx, c)
	}
	return StopOutcome{NativeClosed: true, SemanticCleaned: true}
}

func (e *synthExec) snapshot() []execCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]execCall(nil), e.calls...)
}

type loggingRecorder struct {
	*Recorder
	log *orderLog
}

func (l loggingRecorder) Restore(ctx context.Context, m *RecoveryMaterial) error {
	l.log.add("restore")
	return l.Recorder.Restore(ctx, m)
}

func (l loggingRecorder) RestoreEmpty(ctx context.Context) error {
	l.log.add("restore")
	return l.Recorder.RestoreEmpty(ctx)
}

// startService builds the desired-state service of this process with the
// recorder as its D5 adapter and starts it.
func (w *world) startService(inc string, execs map[DriverID]*synthExec) error {
	w.t.Helper()
	var cfgs []DriverConfig
	ids := make([]DriverID, 0, len(execs))
	for id := range execs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		cfgs = append(cfgs, DriverConfig{DriverID: id, Executor: execs[id]})
	}
	svc, err := New(Options{Store: w.store, Recovery: loggingRecorder{Recorder: w.rec, log: w.log}, Drivers: cfgs, Incarnation: inc,
		RetryDelay: func(int) time.Duration { return 2 * time.Millisecond }, OnError: w.errs.add})
	if err != nil {
		w.t.Fatal(err)
	}
	w.svc = svc
	return svc.Start(context.Background())
}

func (w *world) status(id DriverID) DriverStatus {
	w.t.Helper()
	st, err := w.svc.Get(id)
	if err != nil {
		w.t.Fatal(err)
	}
	return st
}

func (w *world) record(id DriverID) ControlRecord {
	w.t.Helper()
	rec, err := w.store.Driver(id)
	if err != nil {
		w.t.Fatal(err)
	}
	return rec
}

func fenced(st assetState, epoch semreg.SourceEpochID) bool {
	for _, c := range st.Cursors {
		if c.SourceEpochID == epoch && c.Fenced {
			return true
		}
	}
	return false
}

func lastEntry(t *testing.T, s walStream) walEntry {
	t.Helper()
	if len(s.Entries) == 0 {
		t.Fatal("durable stream has no entry")
	}
	return s.Entries[len(s.Entries)-1]
}

func refusalCode(err error) RefusalCode {
	var r *Refusal
	if errors.As(err, &r) {
		return r.Code
	}
	return ""
}

// --- acceptance tests ---

// TestRecoveryIntentBeforeHubOutcomeAfter pins the two durable steps: the
// exact staged bytes, the per-source WAL sequence, the per-asset ordinal and
// the reserved point with its clock mark are durable before the hub sees the
// publication, and the accepted result, original point, incarnation, HubSeq
// and cursor follow in a second transaction. For a control, each asset's
// exact attempt is durable before that asset is submitted.
func TestRecoveryIntentBeforeHubOutcomeAfter(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.mustRestore()
	w.admit(synthSource)
	var staged StagedBatch
	hubCalls := 0
	w.bridge.beforePublish = func(st StagedBatch) {
		hubCalls++
		staged = st
		e := lastEntry(t, w.stream())
		a := e.Assets[0]
		if e.Kind != entryPublication || e.Source != synthSource || e.WAL != 1 || a.Asset != synthAssets[0] || a.Ordinal != 1 || a.Outcome != nil {
			t.Errorf("intent not durable before the hub call: %+v", e)
		}
		if a.Staged == nil || !bytes.Equal(a.Staged.Bytes, st.Bytes) || a.Staged.Sequence != 1 || a.Staged.Digest != st.Digest {
			t.Error("durable intent does not hold the exact staged bytes")
		}
		if mark := w.stream().Clocks[a.Asset]; mark.High != e.Point || len(mark.Anchor) == 0 || mark.Unit != "ns" {
			t.Errorf("clock mark not persisted with the intent: %+v point %d", mark, e.Point)
		}
	}
	before := w.store.count()
	out := w.mustPublish(synthSource, synthAssets[0])
	w.bridge.beforePublish = nil
	if hubCalls != 1 {
		t.Fatalf("hub calls = %d", hubCalls)
	}
	if writes := w.store.count() - before; writes != 2 {
		t.Fatalf("durable writes per publication = %d, want 2 (intent, outcome)", writes)
	}
	s := w.stream()
	e := s.Entries[0]
	o := e.Assets[0].Outcome
	if o == nil || o.Accepted == nil {
		t.Fatalf("outcome not durable: %+v", e)
	}
	var acc semhub.AcceptedBatch
	if err := json.Unmarshal(o.Accepted.Result, &acc); err != nil {
		t.Fatal(err)
	}
	view, _ := w.bridge.hub.Inspect(semreg.AssetID(synthAssets[0]))
	if o.Accepted.Point != e.Point || o.Accepted.HubSeq != out.Accepted.HubSeq || o.Accepted.HubSeq == 0 ||
		o.Accepted.Incarnation != w.bridge.hub.Incarnation() || acc.SnapshotID != view.Snapshot.SnapshotID ||
		acc.Cursor.LastSequence != "1" || !bytes.Equal(o.Accepted.Batch.Bytes, staged.Bytes) || acc.Monotonic != monoAt(e.Point) {
		t.Fatalf("outcome lacks the accepted result, point, incarnation, HubSeq or cursor: %+v", o.Accepted)
	}
	if s.NextWAL[synthSource] != 2 || s.NextOrdinal[synthAssets[0]] != 2 {
		t.Fatalf("counters: %+v %+v", s.NextWAL, s.NextOrdinal)
	}
	w.mustPublish(synthSource, synthAssets[1])
	if e := lastEntry(t, w.stream()); e.WAL != 2 || e.Assets[0].Ordinal != 1 {
		t.Fatalf("WAL is per source and ordinals are per asset: %+v", e)
	}
	w.mustPublish(synthSource, synthAssets[1])

	stagedSeen := map[AssetID]bool{}
	w.bridge.onStaged = func(st StagedBatch) {
		e := lastEntry(t, w.stream())
		for _, a := range e.Assets {
			if a.Asset != st.Asset {
				continue
			}
			if e.Kind != entryControl || a.Staged == nil || !bytes.Equal(a.Staged.Bytes, st.Bytes) || a.Outcome != nil {
				t.Errorf("control attempt for %s not durable before submission: %+v", st.Asset, e)
			}
		}
		next, _ := w.bridge.hub.NextSequence(semreg.AssetID(st.Asset), semreg.SourceID(synthSource), w.bridge.handle(synthSource).Epoch(), 1)
		if next != semreg.Uint64(strconv.FormatUint(st.Sequence, 10)) {
			t.Errorf("attempt %s submitted before its intent: next %s", st.Asset, next)
		}
		stagedSeen[st.Asset] = true
	}
	outs, err := w.control(synthSource, semhub.ControlFence)
	w.bridge.onStaged = nil
	if err != nil || len(stagedSeen) != 2 {
		t.Fatalf("fence: %v %+v staged %v", err, outs, stagedSeen)
	}
	e = lastEntry(t, w.stream())
	if e.WAL != 4 || len(e.Assets) != 2 {
		t.Fatalf("control entry: %+v", e)
	}
	for _, a := range e.Assets {
		if a.Outcome == nil || a.Outcome.Accepted == nil || a.Outcome.Accepted.Point != e.Point {
			t.Fatalf("control outcome not durable for %s: %+v", a.Asset, a)
		}
	}
}

// TestRecoveryStalledDurableWriteKeepsOtherFamilyAdmittedReads stalls a
// managed source's durable write inside the store's file write (the fsync
// path) and, separately, inside a control attempt's write-ahead step. In both
// cases another family's Admitted and ReadAdmitted reads, a managed asset's
// Inspect, another family's direct publication and the store's committed
// control read complete before the test releases either stalled write.
// The watchdog only diagnoses hangs; it does not measure read latency.
func TestRecoveryStalledDurableWriteKeepsOtherFamilyAdmittedReads(t *testing.T) {
	w := newWorld(t, worldOpts{other: true})
	w.mustRestore()
	w.admit(synthSource)
	w.mustPublish(synthSource, synthAssets[0])
	w.mustPublish(synthSource, synthAssets[1])
	hub := w.bridge.hub
	otherHandle, err := hub.AdmitSource(semhub.Admission{Source: otherSource, Driver: otherDriver})
	if err != nil {
		t.Fatal(err)
	}
	direct := func(at uint64) {
		t.Helper()
		tx, err := hub.Begin(otherHandle, otherAsset)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Abort()
		snap, _ := tx.Current()
		seq := tx.NextSequence()
		batch := synthBatch(t, snap, otherAsset, otherHandle, seq, int64(at), at)
		if _, _, err := tx.Apply(batch, monoAt(at)); err != nil {
			t.Fatal(err)
		}
		if outcome, err := tx.Commit(nil); err != nil || outcome != semhub.Accepted {
			t.Fatalf("direct publication: %v %v", outcome, err)
		}
	}
	direct(uint64(time.Second))
	checks := func(at uint64, release <-chan struct{}) {
		assertStalled := func() {
			t.Helper()
			select {
			case <-release:
				t.Fatal("write released before read checks completed")
			default:
			}
		}
		assertStalled()
		for i := 0; i < 20; i++ {
			if !hub.Admitted(otherAsset) || !hub.ReadAdmitted(otherAsset) {
				t.Fatal("other family read closed")
			}
			assertStalled()
			if view, err := hub.Inspect(semreg.AssetID(synthAssets[1])); err != nil || !view.Present {
				t.Fatal("managed inspect failed")
			}
			assertStalled()
			if _, err := w.m.ControlSnapshot(); err != nil {
				t.Fatal(err)
			}
			assertStalled()
		}
		direct(at)
		assertStalled()
	}
	runStalled := func(phase string, at uint64, entered, release chan struct{}, write func() error) {
		t.Helper()
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		wait := func(done <-chan struct{}, what string) bool {
			t.Helper()
			select {
			case <-done:
				return true
			case <-time.After(5 * time.Second):
				t.Errorf("%s: timed out waiting for %s", phase, what)
				return false
			}
		}
		writeDone := make(chan struct{})
		var writeErr error
		go func() {
			defer close(writeDone)
			writeErr = write()
		}()
		var readsDone chan struct{}
		defer func() {
			// Release first on every failure path so blocked readers and the
			// writer can finish before the fixture's cleanup runs.
			unblock()
			if wait(writeDone, "writer to join") && writeErr != nil {
				t.Errorf("%s: durable write: %v", phase, writeErr)
			}
			if readsDone != nil {
				wait(readsDone, "read checks to join")
			}
		}()
		if !wait(entered, "write to reach store") {
			t.FailNow()
		}
		readsDone = make(chan struct{})
		go func() {
			defer close(readsDone)
			checks(at, release)
		}()
		if !wait(readsDone, "read checks while write remains stalled") {
			t.FailNow()
		}
		// Both the operations and their ordering sentinels have completed.
		unblock()
	}

	entered, release := w.hooks.arm()
	runStalled("stalled intent write", uint64(2*time.Second), entered, release, func() error {
		_, err := w.publish(synthSource, synthAssets[0])
		return err
	})

	controlEntered, controlRelease := w.store.armStall(func(p []byte) bool {
		s, err := decodeStream(p)
		if err != nil || len(s.Entries) == 0 {
			return false
		}
		e := s.Entries[len(s.Entries)-1]
		return e.Kind == entryControl && len(e.Assets) == 2 && e.Assets[1].Staged != nil && e.Assets[1].Outcome == nil
	})
	runStalled("stalled control write-ahead", uint64(3*time.Second), controlEntered, controlRelease, func() error {
		_, err := w.control(synthSource, semhub.ControlFence)
		return err
	})
}

// TestRecoveryS39AcceptedPublicationsAndFence covers S3.9 "accepted
// publications followed by an accepted fence". Both service-side recorder
// and hub memory are destroyed; the replay includes the control batch, the
// exact snapshot bytes, IDs, revision vector, cursors and retained history
// (with the fence tombstone) survive, reads stay closed inside the fresh
// reprojection and reopen only after it, and the new epoch's first batch
// retires the prior epoch on both assets while the old handle stays dead.
func TestRecoveryS39AcceptedPublicationsAndFence(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.mustRestore()
	old := w.admit(synthSource)
	for _, id := range synthAssets {
		w.mustPublish(synthSource, id)
		w.mustPublish(synthSource, id)
	}
	outs, err := w.control(synthSource, semhub.ControlFence)
	if err != nil {
		t.Fatalf("fence: %v %+v", err, outs)
	}
	for _, out := range outs {
		if out.Accepted == nil {
			t.Fatalf("fence not accepted on %s: %+v", out.Asset, out)
		}
	}
	want := w.states(synthAssets...)
	prior := w.bridge.hub.Incarnation()
	w.loseProcess(10 * time.Second)
	if w.bridge.hub.Incarnation() == prior {
		t.Fatal("hub incarnation reused")
	}
	w.mustRestore()
	got := w.states(synthAssets...)
	assertStates(t, "restored", got, want)
	rep := w.rec.Report()
	if !reflect.DeepEqual(rep.Restored, synthAssets) || !reflect.DeepEqual(rep.Reopened, synthAssets) || len(rep.Failed) != 0 || len(rep.Resubmitted) != 0 {
		t.Fatalf("report: %+v", rep)
	}
	for _, id := range synthAssets {
		if len(got[id].Batches) != 3 || !fenced(got[id], old.Epoch()) {
			t.Fatalf("%s lost the control batch or its tombstone", id)
		}
		if !w.bridge.hub.Readable(semreg.AssetID(id)) || w.bridge.openSeen[id] || w.bridge.reproj[id] == 0 {
			t.Fatalf("%s: reads must stay closed inside reprojection and reopen after it (seen open %v, callbacks %d)", id, w.bridge.openSeen[id], w.bridge.reproj[id])
		}
	}
	if info, _ := w.bridge.hub.Source(semreg.SourceID(synthSource)); info.Live {
		t.Fatal("historical admission restored as current")
	}
	if _, err := w.bridge.hub.Begin(old, semreg.AssetID(synthAssets[0])); err == nil {
		t.Fatal("prior-process handle is current after restore")
	}
	if entries, hasBase := w.rec.Stream(); entries != 0 || !hasBase {
		t.Fatalf("restore did not fold the resolved stream into a verified restore point: %d entries, base %v", entries, hasBase)
	}
	fresh := w.admit(synthSource)
	if fresh.Epoch() == old.Epoch() {
		t.Fatal("new process reused the prior epoch")
	}
	for _, id := range synthAssets {
		w.mustPublish(synthSource, id)
		view, _ := w.bridge.hub.Inspect(semreg.AssetID(id))
		retired := false
		for _, src := range view.Snapshot.Sources {
			if src.SourceEpochID == old.Epoch() {
				retired = src.State == semreg.SourceRetired
			}
		}
		if !retired {
			t.Fatalf("new epoch barrier did not retire the prior epoch on %s", id)
		}
	}
}

// TestRecoveryS39LossBeforeHubReturnAndAfterAcceptance covers S3.9 "one
// submitted envelope with no outcome", lost before the hub returned and lost
// after acceptance but before the outcome transaction. Only the pending
// intent survives; restoration compares the exact cursor, resubmits the
// exact stored bytes at their original point and never rebuilds them.
func TestRecoveryS39LossBeforeHubReturnAndAfterAcceptance(t *testing.T) {
	for _, phase := range []string{"before-hub-return", "after-accept-before-outcome"} {
		t.Run(phase, func(t *testing.T) {
			w := newWorld(t, worldOpts{})
			w.mustRestore()
			w.admit(synthSource)
			for _, id := range synthAssets {
				w.mustPublish(synthSource, id)
			}
			var lost StagedBatch
			loss := func(st StagedBatch) { lost = st; panic("process memory lost") }
			if phase == "before-hub-return" {
				w.bridge.beforePublish = loss
			} else {
				w.bridge.afterAccept = loss
			}
			out, err := w.publish(synthSource, synthAssets[0])
			if !errors.Is(err, ErrOutcomeUncertain) || !out.Uncertain {
				t.Fatalf("lost call: %+v %v", out, err)
			}
			if _, err := w.publish(synthSource, synthAssets[1]); refusalCode(err) != RefusalUnavailable {
				t.Fatalf("an ambiguous completion must block the source: %v", err)
			}
			e := lastEntry(t, w.stream())
			if e.WAL != 3 || e.Assets[0].Outcome != nil || !bytes.Equal(e.Assets[0].Staged.Bytes, lost.Bytes) {
				t.Fatalf("only the pending intent may survive: %+v", e)
			}
			pre := w.states(synthAssets...)
			w.loseProcess(time.Second)
			w.mustRestore()
			rep := w.rec.Report()
			want := []StepRef{{Source: synthSource, WAL: 3, Asset: synthAssets[0], Ordinal: 2}}
			if !reflect.DeepEqual(rep.Resubmitted, want) || len(rep.ReadBack) != 0 {
				t.Fatalf("resolution: %+v", rep)
			}
			got := w.states(synthAssets...)
			if phase == "after-accept-before-outcome" {
				assertStates(t, "accepted before loss", got, pre)
			} else {
				if len(pre[synthAssets[0]].Batches) != 1 {
					t.Fatal("hub must not have seen the batch")
				}
				assertStates(t, "untouched asset", map[AssetID]assetState{synthAssets[1]: got[synthAssets[1]]}, map[AssetID]assetState{synthAssets[1]: pre[synthAssets[1]]})
			}
			batches := got[synthAssets[0]].Batches
			if len(batches) != 2 || !bytes.Equal(batches[1], lost.Bytes) {
				t.Fatal("restoration rebuilt or dropped the exact pending bytes")
			}
			rec := w.bridge.hub.ExportRecovery()
			if rec.Assets[0].Accepted[1].Monotonic != monoAt(e.Point) {
				t.Fatal("resubmission did not keep the original monotonic point")
			}
		})
	}
}

// TestRecoveryS39TwoAssetsOneAcceptedOneRejectedControl covers S3.9
// "unfinished control: one asset accepts and the other rejects". The second
// asset's write-ahead transaction fails, so the hub refuses its submission;
// after memory loss both gates stay closed during reprojection, only the
// accepted asset reopens, the rejected cursor keeps its pre-rejection value
// and the original error identifier survives.
func TestRecoveryS39TwoAssetsOneAcceptedOneRejectedControl(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.mustRestore()
	old := w.admit(synthSource)
	for _, id := range synthAssets {
		w.mustPublish(synthSource, id)
		w.mustPublish(synthSource, id)
	}
	w.store.setFail(func(p []byte) error {
		s, err := decodeStream(p)
		if err != nil || len(s.Entries) == 0 {
			return nil
		}
		e := s.Entries[len(s.Entries)-1]
		if e.Kind == entryControl && e.Assets[1].Staged != nil && e.Assets[1].Outcome == nil {
			return errors.New("synthetic write-ahead failure")
		}
		return nil
	})
	outs, err := w.control(synthSource, semhub.ControlFence)
	w.store.setFail(nil)
	if err != nil || len(outs) != 2 || outs[0].Accepted == nil || outs[1].ErrorID != controlExportRef {
		t.Fatalf("mixed control: %+v %v", outs, err)
	}
	e := lastEntry(t, w.stream())
	if e.Assets[1].Staged != nil || e.Assets[1].Outcome == nil || e.Assets[1].Outcome.ErrorID != controlExportRef || len(e.Assets[1].Outcome.Detail) == 0 {
		t.Fatalf("rejection not durable with its identifier: %+v", e.Assets[1])
	}
	pre := w.states(synthAssets...)
	w.loseProcess(time.Second)
	w.mustRestore()
	assertStates(t, "mixed control", w.states(synthAssets...), pre)
	a, b := semreg.AssetID(synthAssets[0]), semreg.AssetID(synthAssets[1])
	if !w.bridge.hub.Readable(a) || w.bridge.openSeen[synthAssets[0]] {
		t.Fatal("accepted asset must reopen only after reprojection against its restored snapshot")
	}
	if w.bridge.hub.Readable(b) || w.bridge.reproj[synthAssets[1]] != 0 {
		t.Fatal("rejected asset reopened")
	}
	if err := w.bridge.hub.Resettle(b); !errors.Is(err, semhub.ErrSemanticOutstanding) {
		t.Fatalf("rejected gate resettled: %v", err)
	}
	if next, _ := w.bridge.hub.NextSequence(b, semreg.SourceID(synthSource), old.Epoch(), 1); next != "3" {
		t.Fatalf("rejected cursor moved: next %s", next)
	}
	if next, _ := w.bridge.hub.NextSequence(a, semreg.SourceID(synthSource), old.Epoch(), 1); next != "4" {
		t.Fatalf("accepted cursor: next %s", next)
	}
	rep := w.rec.Report()
	if len(rep.Rejected) != 1 || rep.Rejected[0].Asset != synthAssets[1] || rep.Rejected[0].ErrorID != controlExportRef {
		t.Fatalf("original error identifier lost: %+v", rep.Rejected)
	}
	for _, entry := range w.bridge.hub.ExportRecovery().Assets {
		if entry.Asset == b && (entry.Control == nil || entry.Control.Status.ErrorID != controlExportRef) {
			t.Fatalf("restored gate lost the error identifier: %+v", entry.Control)
		}
	}
}

// TestRecoveryS39RejectedPublicationAndControl covers S3.9 "rejected
// publication envelope and rejected control attempt": neither is applied as
// accepted, every cursor keeps its pre-rejection value and the verbatim error
// identifier survives, including a rejected publication whose outcome
// transaction was lost (its exact resubmission is rejected again, never
// accepted).
func TestRecoveryS39RejectedPublicationAndControl(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.mustRestore()
	w.admit(synthSource)
	w.mustPublish(synthSource, synthAssets[0])
	w.mustPublish(synthSource, synthAssets[1])
	stale := func(b *semreg.PublicationBatch) { b.ExpectedSemanticRevision = "99" }
	w.host.advance(time.Second)
	out, err := w.writer(synthSource).Publish(context.Background(), synthAssets[0], w.stage(synthSource, synthAssets[0], stale))
	if err != nil || out.Accepted != nil || out.ErrorID == "" || strings.HasPrefix(out.ErrorID, "recoverytest") {
		t.Fatalf("rejected publication: %+v %v", out, err)
	}
	recorded := out.ErrorID
	w.store.setFail(func(p []byte) error {
		s, _ := decodeStream(p)
		if e := s.entry(synthSource, 4); e != nil && e.Assets[0].Outcome != nil {
			return errors.New("synthetic outcome write failure")
		}
		return nil
	})
	w.host.advance(time.Second)
	out, err = w.writer(synthSource).Publish(context.Background(), synthAssets[0], w.stage(synthSource, synthAssets[0], stale))
	w.store.setFail(nil)
	if !errors.Is(err, ErrOutcomeNotDurable) || out.ErrorID != recorded {
		t.Fatalf("rejected publication with lost outcome: %+v %v", out, err)
	}
	w.bridge.onBeforeControl = func(StagedBatch) error { return errors.New("synthetic refusal") }
	outs, err := w.control(synthSource, semhub.ControlFence)
	w.bridge.onBeforeControl = nil
	// The hub stops a control at its first rejected asset; the second asset
	// is never attempted and stays behind the open control.
	if err != nil || outs[0].ErrorID != controlExportRef || outs[1].ErrorID != "" || outs[1].Accepted != nil || outs[1].Uncertain {
		t.Fatalf("rejected control: %+v %v", outs, err)
	}
	pre := w.states(synthAssets...)
	w.loseProcess(time.Second)
	w.mustRestore()
	assertStates(t, "rejections", w.states(synthAssets...), pre)
	if n := len(w.states(synthAssets[0])[synthAssets[0]].Batches); n != 1 {
		t.Fatalf("a rejected entry was applied as accepted: %d batches", n)
	}
	rep := w.rec.Report()
	want := []StepRef{
		{Source: synthSource, WAL: 3, Asset: synthAssets[0], Ordinal: 2, ErrorID: recorded},
		{Source: synthSource, WAL: 4, Asset: synthAssets[0], Ordinal: 3, ErrorID: recorded},
		{Source: synthSource, WAL: 5, Asset: synthAssets[0], Ordinal: 4, ErrorID: controlExportRef},
	}
	if !reflect.DeepEqual(rep.Rejected, want) || len(rep.Resubmitted) != 0 {
		t.Fatalf("rejections after restore: %+v", rep)
	}
	for _, id := range synthAssets {
		if w.bridge.hub.Readable(semreg.AssetID(id)) {
			t.Fatalf("%s reopened behind a rejected or unattempted control", id)
		}
	}
	gates := map[semreg.AssetID]semhub.SemanticState{}
	for _, entry := range w.bridge.hub.ExportRecovery().Assets {
		if entry.Control != nil {
			gates[entry.Asset] = entry.Control.Status.Semantic
		}
	}
	if gates[semreg.AssetID(synthAssets[0])] != semhub.SemanticRejected || gates[semreg.AssetID(synthAssets[1])] != semhub.SemanticUnbuilt {
		t.Fatalf("restored gates: %v", gates)
	}
}

// TestRecoveryS39WalGapResolvesPredecessorAndMissingIntentFailsSource covers
// S3.9 "WAL gap": publication A is accepted without a durable outcome, a
// dependent publication B on the same source and asset is accepted and
// recorded. Restoration resolves A by exact resubmission before replaying B
// (B's recorded result only verifies on top of A) and reproduces the
// pre-loss state. Removing A's intent fails the whole source closed, while an
// unrelated managed source stays recoverable and the stream is retained.
func TestRecoveryS39WalGapResolvesPredecessorAndMissingIntentFailsSource(t *testing.T) {
	setup := func(t *testing.T) *world {
		w := newWorld(t, worldOpts{second: true})
		w.mustRestore()
		w.admit(synthSource)
		w.admit(secondSource)
		w.mustPublish(secondSource, secondAsset)
		w.mustPublish(synthSource, synthAssets[0])
		w.store.setFail(func(p []byte) error {
			s, _ := decodeStream(p)
			if e := s.entry(synthSource, 2); e != nil && e.Assets[0].Outcome != nil {
				return errors.New("synthetic outcome write failure")
			}
			return nil
		})
		if out, err := w.publish(synthSource, synthAssets[0]); !errors.Is(err, ErrOutcomeNotDurable) || out.Accepted == nil {
			t.Fatalf("A: %+v %v", out, err)
		}
		w.mustPublish(synthSource, synthAssets[0])
		w.mustPublish(synthSource, synthAssets[1])
		w.store.setFail(nil)
		s := w.stream()
		if a := s.entry(synthSource, 2); a == nil || a.Assets[0].Outcome != nil {
			t.Fatal("A's outcome must be missing")
		}
		if b := s.entry(synthSource, 3); b == nil || b.Assets[0].Outcome == nil || b.Assets[0].Outcome.Accepted == nil {
			t.Fatal("B's outcome must be durable")
		}
		return w
	}
	t.Run("resolve-predecessor", func(t *testing.T) {
		w := setup(t)
		pre := w.states(append([]AssetID{secondAsset}, synthAssets...)...)
		w.loseProcess(time.Second)
		w.mustRestore()
		assertStates(t, "wal gap", w.states(append([]AssetID{secondAsset}, synthAssets...)...), pre)
		rep := w.rec.Report()
		if !reflect.DeepEqual(rep.Resubmitted, []StepRef{{Source: synthSource, WAL: 2, Asset: synthAssets[0], Ordinal: 2}}) {
			t.Fatalf("A not resolved before B: %+v", rep)
		}
	})
	t.Run("missing-intent", func(t *testing.T) {
		w := setup(t)
		pre := w.states(secondAsset)
		w.loseProcess(time.Second)
		w.rewrite(func(s *walStream) {
			kept := s.Entries[:0]
			for _, e := range s.Entries {
				if e.Source != synthSource || e.WAL != 2 {
					kept = append(kept, e)
				}
			}
			s.Entries = kept
		})
		payload := append([]byte(nil), w.material().Payload...)
		err := w.restore()
		var re *RestoreError
		if !errors.As(err, &re) || !reflect.DeepEqual(re.Drivers, []DriverID{synthDriver}) || !reflect.DeepEqual(re.Sources, []SourceID{synthSource}) ||
			!reflect.DeepEqual(re.Assets, synthAssets) {
			t.Fatalf("missing intent must fail the source closed: %v", err)
		}
		for _, id := range synthAssets {
			if view, _ := w.bridge.hub.Inspect(semreg.AssetID(id)); view.Present || w.bridge.hub.Readable(semreg.AssetID(id)) {
				t.Fatalf("%s opened without its predecessor", id)
			}
		}
		assertStates(t, "unrelated source", w.states(secondAsset), pre)
		if !w.bridge.hub.Readable(semreg.AssetID(secondAsset)) {
			t.Fatal("unrelated managed source not reopened")
		}
		if _, err := w.publish(synthSource, synthAssets[1]); refusalCode(err) != RefusalFailedPrecondition {
			t.Fatalf("failed source accepted a write: %v", err)
		}
		if err := w.rec.Compact(context.Background()); !errors.Is(err, ErrCompaction) {
			t.Fatalf("compaction must retain material of a failed source: %v", err)
		}
		if !bytes.Equal(w.material().Payload, payload) {
			t.Fatal("recovery material of a failed source changed")
		}
	})
}

// TestRecoveryS39MidCleanupKeepsAttemptsAndNativeCloseIndependent covers
// S3.9 "mid-cleanup" through the desired-state service. A STOP's fence has
// staged and submitted asset A and durably staged asset B when the process
// is lost. The original asset set, the exact pending attempts, the slot's
// budget, deadline and correlation survive; restoration runs before any
// admission and resolves both attempts by exact resubmission; the recovered
// STOP settles the historical correlation, and the new process's executor,
// which cannot prove the prior process's native close, leaves that
// obligation pending (quarantine) instead of a fabricated close proof.
func TestRecoveryS39MidCleanupKeepsAttemptsAndNativeCloseIndependent(t *testing.T) {
	w := newWorld(t, worldOpts{})
	hold := make(chan struct{})
	var holdOnce sync.Once
	t.Cleanup(func() { holdOnce.Do(func() { close(hold) }) })
	exec1 := &synthExec{w: w, src: synthSource}
	exec1.stop = func(ctx context.Context, c LifecycleCorrelation) StopOutcome {
		w.bridge.onStaged = func(st StagedBatch) {
			if st.Asset == synthAssets[1] {
				<-hold // process loss: B is durably staged and never submitted
			}
		}
		outs, err := w.control(synthSource, semhub.ControlFence)
		return StopOutcome{NativeClosed: true, SemanticCleaned: err == nil && len(outs) == 2}
	}
	if err := w.startService("inc1", map[DriverID]*synthExec{synthDriver: exec1}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "RUNNING", func() bool { return w.status(synthDriver).ObservedState == drivermanager.ObservedRunning })
	if log := w.log.list(); len(log) < 2 || log[0] != "restore" || log[1] != "start:"+string(synthSource) {
		t.Fatalf("restore must precede admission: %v", log)
	}
	oldEpoch := w.bridge.handle(synthSource).Epoch()
	if _, err := w.svc.Control(deadlineCtx(t), MethodStop, body(synthDriver, w.status(synthDriver).DesiredRevision)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B staged", func() bool {
		e := lastEntry(t, w.stream())
		return e.Kind == entryControl && e.Assets[1].Staged != nil
	})
	slot := w.record(synthDriver).Current
	if slot == nil || slot.Kind != KindStop || !slot.NativeClosePending || !slot.SemanticCleanupPending || slot.Epoch == "" {
		t.Fatalf("stop slot before loss: %+v", slot)
	}
	e := lastEntry(t, w.stream())
	if len(e.Assets) != 2 || e.Assets[0].Staged == nil || e.Assets[0].Outcome != nil || e.Assets[1].Outcome != nil {
		t.Fatalf("pending cleanup attempts: %+v", e)
	}
	stagedA, stagedB := e.Assets[0].Staged.Bytes, e.Assets[1].Staged.Bytes

	// Process loss: the old service stays blocked in its executor and is
	// abandoned with its hub; its store manager is stopped.
	_ = w.m.Stop(context.Background())
	abandoned := w.svc
	w.svc = nil
	t.Cleanup(func() { holdOnce.Do(func() { close(hold) }); abandoned.Close() })
	w.host.advance(time.Second)
	w.open()

	exec2 := &synthExec{w: w, src: synthSource}
	exec2.start = func(context.Context, LifecycleCorrelation) error {
		t.Error("STOPPED driver started")
		return nil
	}
	exec2.stop = func(_ context.Context, c LifecycleCorrelation) StopOutcome {
		cleaned := true
		for _, id := range synthAssets {
			cleaned = cleaned && fenced(stateOf(t, w.bridge.hub, id), oldEpoch)
		}
		// The prior process's native resource cannot be observed here: its
		// absence is not a close proof.
		return StopOutcome{NativeClosed: false, SemanticCleaned: cleaned}
	}
	if err := w.startService("inc2", map[DriverID]*synthExec{synthDriver: exec2}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "quarantine", func() bool { return w.status(synthDriver).ObservedState == drivermanager.ObservedFailed })
	rep := w.rec.Report()
	if len(rep.Resubmitted) != 2 || rep.Resubmitted[0].Asset != synthAssets[0] || rep.Resubmitted[1].Asset != synthAssets[1] {
		t.Fatalf("pending attempts not resolved by exact resubmission: %+v", rep)
	}
	for i, id := range synthAssets {
		st := stateOf(t, w.bridge.hub, id)
		if !fenced(st, oldEpoch) || !bytes.Equal(st.Batches[len(st.Batches)-1], [][]byte{stagedA, stagedB}[i]) {
			t.Fatalf("%s: cleanup attempt not restored byte-exactly", id)
		}
	}
	calls := exec2.snapshot()
	if len(calls) == 0 || calls[0].Op != "stop" || calls[0].Corr.Epoch != slot.Epoch || calls[0].Corr.Generation != slot.Generation {
		t.Fatalf("recovered STOP must settle the historical correlation: %+v", calls)
	}
	after := w.record(synthDriver).Current
	if after == nil || !after.NativeClosePending || after.SemanticCleanupPending || after.AttemptsUsed < slot.AttemptsUsed ||
		!after.DeadlineUTC.Equal(slot.DeadlineUTC) || after.Epoch != slot.Epoch || after.Generation != slot.Generation {
		t.Fatalf("budget, deadline, correlation or the independent native-close obligation changed: before %+v after %+v", slot, after)
	}
	if info, _ := w.bridge.hub.Source(semreg.SourceID(synthSource)); info.Live {
		t.Fatal("recovery admitted the source")
	}
}

// TestRecoveryRestoreBeforeAdmissionAndPerDriverFailure runs the recorder as
// the service's D5 adapter. Writers refuse before restoration; restoration
// precedes every executor start; a hub admission before restoration makes
// the restore fail closed; and a corrupt asset of one driver withholds only
// that driver while another managed driver restores and runs.
func TestRecoveryRestoreBeforeAdmissionAndPerDriverFailure(t *testing.T) {
	w := newWorld(t, worldOpts{second: true})
	if _, err := w.writer(synthSource).Publish(context.Background(), synthAssets[0], w.stage(synthSource, synthAssets[0], nil)); refusalCode(err) != RefusalFailedPrecondition {
		t.Fatalf("write before restore: %v", err)
	}
	execs := map[DriverID]*synthExec{synthDriver: {w: w, src: synthSource}, secondDriver: {w: w, src: secondSource}}
	if err := w.startService("inc1", execs); err != nil {
		t.Fatal(err)
	}
	for _, id := range []DriverID{synthDriver, secondDriver} {
		waitFor(t, string(id)+" RUNNING", func() bool { return w.status(id).ObservedState == drivermanager.ObservedRunning })
	}
	if log := w.log.list(); log[0] != "restore" {
		t.Fatalf("restore must precede every start: %v", log)
	}
	w.loseProcess(time.Second)
	w.rewrite(func(s *walStream) {
		for i := range s.Entries {
			e := &s.Entries[i]
			if e.Source == synthSource && e.Assets[0].Asset == synthAssets[0] && e.Assets[0].Outcome != nil {
				var acc semhub.AcceptedBatch
				_ = json.Unmarshal(e.Assets[0].Outcome.Accepted.Result, &acc)
				acc.Canonical[len(acc.Canonical)/2] ^= 1
				e.Assets[0].Outcome.Accepted.Result, _ = json.Marshal(acc)
			}
		}
	})
	exec2 := map[DriverID]*synthExec{synthDriver: {w: w, src: synthSource}, secondDriver: {w: w, src: secondSource}}
	err := w.startService("inc2", exec2)
	var re *RestoreError
	if !errors.As(err, &re) || !reflect.DeepEqual(re.Drivers, []DriverID{synthDriver}) || !reflect.DeepEqual(re.Assets, []AssetID{synthAssets[0]}) {
		t.Fatalf("restore failure must name only the affected driver and asset: %v", err)
	}
	waitFor(t, "second RUNNING", func() bool { return w.status(secondDriver).ObservedState == drivermanager.ObservedRunning })
	if st := w.status(synthDriver); st.ObservedState != drivermanager.ObservedFailed {
		t.Fatalf("affected driver: %+v", st)
	}
	if n := len(exec2[synthDriver].snapshot()); n != 0 {
		t.Fatalf("withheld driver executed %d calls", n)
	}
	if _, err := w.svc.Control(deadlineCtx(t), MethodRestart, body(synthDriver, w.status(synthDriver).DesiredRevision)); refusalCode(err) != RefusalFailedPrecondition {
		t.Fatalf("RESTART of a driver withheld by recovery: %v", err)
	}

	t.Run("admission-before-restore", func(t *testing.T) {
		w := newWorld(t, worldOpts{})
		w.mustRestore()
		w.admit(synthSource)
		w.mustPublish(synthSource, synthAssets[0])
		w.loseProcess(time.Second)
		w.admit(synthSource)
		var re *RestoreError
		if err := w.restore(); !errors.As(err, &re) || !reflect.DeepEqual(re.Assets, synthAssets) {
			t.Fatalf("restore after an admission must fail closed: %v", err)
		}
	})
}

// TestRecoveryCorruptAssetAndChangedValidatorFailClosed corrupts one asset's
// recorded result, and separately restarts with a changed validator on the
// other asset: each failure closes only its asset, the healthy asset is
// restored exactly and reopened by reprojection, and no empty snapshot or
// fallback is installed.
func TestRecoveryCorruptAssetAndChangedValidatorFailClosed(t *testing.T) {
	setup := func(t *testing.T, opts worldOpts) (*world, map[AssetID]assetState) {
		w := newWorld(t, opts)
		w.mustRestore()
		w.admit(synthSource)
		for _, id := range synthAssets {
			w.mustPublish(synthSource, id)
			w.mustPublish(synthSource, id)
		}
		return w, w.states(synthAssets...)
	}
	check := func(t *testing.T, w *world, pre map[AssetID]assetState, bad, good AssetID, err error) {
		t.Helper()
		var re *RestoreError
		if !errors.As(err, &re) || !reflect.DeepEqual(re.Assets, []AssetID{bad}) || !reflect.DeepEqual(re.Drivers, []DriverID{synthDriver}) || len(re.Sources) != 0 {
			t.Fatalf("restore: %v", err)
		}
		if view, _ := w.bridge.hub.Inspect(semreg.AssetID(bad)); view.Present || w.bridge.hub.Readable(semreg.AssetID(bad)) {
			t.Fatal("failed asset installed or reopened")
		}
		assertStates(t, "healthy asset", w.states(good), map[AssetID]assetState{good: pre[good]})
		if !w.bridge.hub.Readable(semreg.AssetID(good)) || !reflect.DeepEqual(w.rec.Report().Reopened, []AssetID{good}) {
			t.Fatalf("only the healthy restored asset may reopen: %+v", w.rec.Report())
		}
		if _, err := w.publish(synthSource, bad); refusalCode(err) != RefusalFailedPrecondition {
			t.Fatalf("failed asset accepted a write: %v", err)
		}
	}
	t.Run("corrupt-result", func(t *testing.T) {
		w, pre := setup(t, worldOpts{})
		w.loseProcess(time.Second)
		w.rewrite(func(s *walStream) {
			o := s.entry(synthSource, 2).Assets[0].Outcome.Accepted
			var acc semhub.AcceptedBatch
			_ = json.Unmarshal(o.Result, &acc)
			acc.SnapshotID = "snapshot:corrupt"
			o.Result, _ = json.Marshal(acc)
		})
		check(t, w, pre, synthAssets[0], synthAssets[1], w.restore())
	})
	t.Run("changed-validator", func(t *testing.T) {
		w, pre := setup(t, worldOpts{})
		w.opts.changed = map[AssetID]bool{synthAssets[1]: true}
		w.loseProcess(time.Second)
		check(t, w, pre, synthAssets[1], synthAssets[0], w.restore())
	})
}

// TestRecoveryCompactionCrashBeforeDuringAfterAndCapacity proves that
// compaction keeps a complete replay basis until a verified restore point
// reproduces the exact state: a crash before, during (the replacement write
// fails) and after compaction restores the same state; an unverifiable
// restore point or an outcome that is not durable refuses compaction and
// leaves the stream byte-identical; and capacity refusal rejects new work
// before acceptance with nothing consumed.
func TestRecoveryCompactionCrashBeforeDuringAfterAndCapacity(t *testing.T) {
	setup := func(t *testing.T, opts worldOpts) *world {
		w := newWorld(t, opts)
		w.mustRestore()
		w.admit(synthSource)
		for _, id := range synthAssets {
			w.mustPublish(synthSource, id)
			w.mustPublish(synthSource, id)
		}
		if _, err := w.control(synthSource, semhub.ControlFence); err != nil {
			t.Fatal(err)
		}
		return w
	}
	restoredEqual := func(t *testing.T, w *world, pre map[AssetID]assetState) {
		t.Helper()
		w.loseProcess(time.Second)
		w.mustRestore()
		assertStates(t, "after crash", w.states(synthAssets...), pre)
	}
	t.Run("crash-before", func(t *testing.T) {
		w := setup(t, worldOpts{})
		if entries, base := w.rec.Stream(); entries != 5 || base {
			t.Fatalf("stream before compaction: %d %v", entries, base)
		}
		restoredEqual(t, w, w.states(synthAssets...))
	})
	t.Run("crash-during", func(t *testing.T) {
		w := setup(t, worldOpts{})
		payload := append([]byte(nil), w.material().Payload...)
		w.store.setFail(func(p []byte) error {
			if s, err := decodeStream(p); err == nil && len(s.Base) > 0 {
				return errors.New("synthetic loss during replacement")
			}
			return nil
		})
		if err := w.rec.Compact(context.Background()); !errors.Is(err, ErrCompaction) {
			t.Fatalf("compaction: %v", err)
		}
		w.store.setFail(nil)
		if !bytes.Equal(w.material().Payload, payload) {
			t.Fatal("failed replacement changed the recovery stream")
		}
		restoredEqual(t, w, w.states(synthAssets...))
	})
	t.Run("unverified", func(t *testing.T) {
		w := setup(t, worldOpts{})
		payload := append([]byte(nil), w.material().Payload...)
		w.bridge.corruptScratch = true
		if err := w.rec.Compact(context.Background()); !errors.Is(err, ErrCompaction) {
			t.Fatalf("unverified compaction: %v", err)
		}
		if !bytes.Equal(w.material().Payload, payload) {
			t.Fatal("unverified restore point replaced the stream")
		}
	})
	t.Run("outcome-not-durable", func(t *testing.T) {
		w := newWorld(t, worldOpts{})
		w.mustRestore()
		w.admit(synthSource)
		w.mustPublish(synthSource, synthAssets[0])
		w.store.setFail(func(p []byte) error {
			if s, _ := decodeStream(p); s.entry(synthSource, 2) != nil && s.entry(synthSource, 2).Assets[0].Outcome != nil {
				return errors.New("synthetic outcome write failure")
			}
			return nil
		})
		if out, err := w.publish(synthSource, synthAssets[0]); !errors.Is(err, ErrOutcomeNotDurable) || out.Accepted == nil {
			t.Fatalf("publication with a failed outcome write: %+v %v", out, err)
		}
		payload := append([]byte(nil), w.material().Payload...)
		if err := w.rec.Compact(context.Background()); !errors.Is(err, ErrCompaction) {
			t.Fatalf("compaction with a pending outcome: %v", err)
		}
		if !bytes.Equal(w.material().Payload, payload) {
			t.Fatal("stream changed")
		}
		w.store.setFail(nil)
		if err := w.rec.Compact(context.Background()); err != nil {
			t.Fatalf("compaction after the outcome became durable: %v", err)
		}
	})
	t.Run("crash-after", func(t *testing.T) {
		w := setup(t, worldOpts{})
		before := w.stream()
		if err := w.rec.Compact(context.Background()); err != nil {
			t.Fatal(err)
		}
		s := w.stream()
		if len(s.Entries) != 0 || len(s.Base) == 0 || s.BaseWAL[synthSource] != 5 || s.NextWAL[synthSource] != 6 ||
			!reflect.DeepEqual(s.Clocks, before.Clocks) || s.BaseOrdinal[synthAssets[0]] != 3 {
			t.Fatalf("compacted stream: entries %d base %d wal %d/%d ord %d", len(s.Entries), len(s.Base), s.BaseWAL[synthSource], s.NextWAL[synthSource], s.BaseOrdinal[synthAssets[0]])
		}
		restoredEqual(t, w, w.states(synthAssets...))
		w.admit(synthSource)
		w.mustPublish(synthSource, synthAssets[0])
		if e := lastEntry(t, w.stream()); e.WAL != 6 || e.Assets[0].Ordinal != 4 {
			t.Fatalf("counters reused after compaction: %+v", e)
		}
	})
	t.Run("capacity", func(t *testing.T) {
		w := newWorld(t, worldOpts{maxBytes: 160 << 10, compact: -1})
		w.mustRestore()
		w.admit(synthSource)
		var refused error
		for i := 0; i < 200 && refused == nil; i++ {
			before := w.stream()
			hubCalls := 0
			w.bridge.beforePublish = func(StagedBatch) { hubCalls++ }
			id := synthAssets[i%2]
			next, _ := w.bridge.hub.NextSequence(semreg.AssetID(id), semreg.SourceID(synthSource), w.bridge.handle(synthSource).Epoch(), 1)
			_, err := w.publish(synthSource, id)
			if err == nil {
				continue
			}
			refused = err
			if refusalCode(err) != RefusalResourceExhausted || hubCalls != 0 {
				t.Fatalf("capacity refusal: %v (hub calls %d)", err, hubCalls)
			}
			if after := w.stream(); !reflect.DeepEqual(after, before) {
				t.Fatal("refused work consumed counters or left staging")
			}
			if again, _ := w.bridge.hub.NextSequence(semreg.AssetID(id), semreg.SourceID(synthSource), w.bridge.handle(synthSource).Epoch(), 1); again != next {
				t.Fatal("refused work reached the hub")
			}
		}
		if refused == nil {
			t.Fatal("stream bound never reached")
		}
		w.bridge.beforePublish = nil
		if err := w.rec.Compact(context.Background()); err != nil {
			t.Fatalf("compaction to make room: %v", err)
		}
		w.mustPublish(synthSource, synthAssets[0])
	})
}

// TestRecoveryClockContinuityAfterProcessClockReset resets the process clock
// below every persisted point, with an uncertain intent above the last
// accepted point: replay keeps original points, every new point exceeds all
// prior reservations by at least the verified downtime, a second restart
// repeats that, and missing or backward continuity fails the asset closed.
func TestRecoveryClockContinuityAfterProcessClockReset(t *testing.T) {
	w := newWorld(t, worldOpts{second: true})
	w.mustRestore()
	w.admit(synthSource)
	w.mustPublish(synthSource, synthAssets[0])
	w.bridge.beforePublish = func(StagedBatch) { panic("lost") }
	if _, err := w.publish(synthSource, synthAssets[0]); !errors.Is(err, ErrOutcomeUncertain) {
		t.Fatal(err)
	}
	uncertain := lastEntry(t, w.stream()).Point
	if accepted := w.stream().Entries[0].Point; uncertain <= accepted {
		t.Fatalf("uncertain point %d not above accepted %d", uncertain, accepted)
	}
	for round, downtime := range []time.Duration{5 * time.Second, 400 * time.Second} {
		w.loseProcess(downtime)
		if sample, _ := w.clock.Sample(); sample >= uncertain {
			t.Fatal("process clock was not reset below the persisted points")
		}
		w.mustRestore()
		high := w.stream().Clocks[synthAssets[0]].High
		w.admit(synthSource)
		out := w.mustPublish(synthSource, synthAssets[0])
		if out.Accepted.Point < high+uint64(downtime) {
			t.Fatalf("round %d: new point %d does not exceed %d plus the downtime", round, out.Accepted.Point, high)
		}
		uncertain = out.Accepted.Point
	}
	rec := w.bridge.hub.ExportRecovery()
	var points []semreg.MonotonicPoint
	for _, a := range rec.Assets {
		if a.Asset == semreg.AssetID(synthAssets[0]) {
			for _, acc := range a.Accepted {
				points = append(points, acc.Monotonic)
			}
		}
	}
	for i := 1; i < len(points); i++ {
		p, _ := strconv.ParseUint(string(points[i].Nanoseconds), 10, 64)
		q, _ := strconv.ParseUint(string(points[i-1].Nanoseconds), 10, 64)
		if p <= q {
			t.Fatalf("points not strictly increasing: %v", points)
		}
	}
	for _, mode := range []string{"foreign-boot", "backward"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "foreign-boot" {
				w.host.mu.Lock()
				w.host.boot = "boot-2"
				w.host.mu.Unlock()
				w.loseProcess(time.Second)
			} else {
				_ = w.m.Stop(context.Background())
				w.host.mu.Lock()
				w.host.boot = "boot-1"
				w.host.now -= uint64(100 * time.Second)
				w.host.mu.Unlock()
				w.open()
			}
			var re *RestoreError
			if err := w.restore(); !errors.As(err, &re) || !reflect.DeepEqual(re.Assets, []AssetID{synthAssets[0]}) {
				t.Fatalf("missing continuity must fail only the affected asset: %v", err)
			}
		})
	}
}

// fakeReplay records every call of the generic resolution.
type fakeReplay struct {
	cursors  map[AssetID][2]any
	resubmit func(StagedBatch) AssetOutcome
	calls    []string
}

func (f *fakeReplay) Failed() map[AssetID]error { return nil }
func (f *fakeReplay) Cursor(asset AssetID, _ string) (uint64, string, error) {
	c := f.cursors[asset]
	f.calls = append(f.calls, "cursor:"+string(asset))
	return c[0].(uint64), c[1].(string), nil
}
func (f *fakeReplay) Apply(step AcceptedStep) error {
	f.calls = append(f.calls, "apply:"+string(step.Batch.Asset))
	return nil
}
func (f *fakeReplay) Rejected(_ string, asset AssetID, _ *StagedBatch, _ uint64, id string, _ []byte) {
	f.calls = append(f.calls, "rejected:"+string(asset)+":"+id)
}
func (f *fakeReplay) Resubmit(_ string, b StagedBatch, point uint64) AssetOutcome {
	f.calls = append(f.calls, fmt.Sprintf("resubmit:%s:%s:%d", b.Asset, b.Bytes, point))
	return f.resubmit(b)
}
func (f *fakeReplay) Install(map[AssetID]bool) (map[AssetID][]byte, map[AssetID]error) {
	return nil, nil
}

// TestRecoveryCursorResolutionRules drives the generic resolution with a
// fake replay: a matching sequence and digest reads back without
// resubmission, an unchanged cursor resubmits the exact stored bytes at the
// original point before the dependent recorded outcome, a recorded rejection
// is never applied, an unstaged control asset without an outcome fails closed, and an
// incompatible cursor or an uncertain resubmission fails the source closed
// and skips its later entries.
func TestRecoveryCursorResolutionRules(t *testing.T) {
	r, err := NewRecorder(RecorderOptions{Store: &faultStore{}, Hub: &hubBridge{}, Clock: &synthClock{}, Sources: []ManagedSource{
		{Driver: synthDriver, Source: synthSource, Assets: synthAssets}, {Driver: secondDriver, Source: secondSource, Assets: []AssetID{secondAsset}}}})
	if err != nil {
		t.Fatal(err)
	}
	staged := func(asset AssetID, seq uint64, b string) *StagedBatch {
		return &StagedBatch{Asset: asset, Key: "k", Sequence: seq, Digest: "d-" + b, Bytes: []byte(b)}
	}
	accepted := func(asset AssetID, seq uint64, b string) *walOutcome {
		return &walOutcome{Accepted: &AcceptedStep{Batch: *staged(asset, seq, b), Point: 9, Result: []byte("r"), Incarnation: "i", HubSeq: 1}}
	}
	a, b2 := synthAssets[0], synthAssets[1]
	s := newStream()
	s.Entries = []walEntry{
		{Kind: entryPublication, Source: synthSource, WAL: 1, Point: 7, Assets: []walAsset{{Asset: a, Ordinal: 1, Staged: staged(a, 3, "readback")}}},
		{Kind: entryPublication, Source: synthSource, WAL: 2, Point: 8, Assets: []walAsset{{Asset: b2, Ordinal: 1, Staged: staged(b2, 5, "resubmit")}}},
		{Kind: entryPublication, Source: synthSource, WAL: 3, Point: 9, Assets: []walAsset{{Asset: b2, Ordinal: 2, Staged: staged(b2, 6, "dependent"), Outcome: accepted(b2, 6, "dependent")}}},
		{Kind: entryPublication, Source: synthSource, WAL: 4, Point: 10, Assets: []walAsset{{Asset: a, Ordinal: 2, Staged: staged(a, 4, "rejected"), Outcome: &walOutcome{ErrorID: "semreg.synthetic"}}}},
		{Kind: entryControl, Source: synthSource, WAL: 5, Point: 11, Assets: []walAsset{{Asset: a, Ordinal: 3}, {Asset: b2, Ordinal: 3, Staged: staged(b2, 7, "control"), Outcome: accepted(b2, 7, "control")}}},
		{Kind: entryPublication, Source: secondSource, WAL: 1, Point: 12, Assets: []walAsset{{Asset: secondAsset, Ordinal: 1, Staged: staged(secondAsset, 2, "incompatible")}}},
		{Kind: entryPublication, Source: secondSource, WAL: 2, Point: 13, Assets: []walAsset{{Asset: secondAsset, Ordinal: 2, Staged: staged(secondAsset, 3, "skipped"), Outcome: accepted(secondAsset, 3, "skipped")}}},
	}
	f := &fakeReplay{cursors: map[AssetID][2]any{a: {uint64(3), "d-readback"}, b2: {uint64(4), "x"}, secondAsset: {uint64(2), "d-other"}},
		resubmit: func(b StagedBatch) AssetOutcome {
			return AssetOutcome{Asset: b.Asset, Accepted: &AcceptedStep{Batch: b}}
		}}
	failedSources, failedAssets := map[SourceID]bool{}, map[AssetID]bool{}
	var rep RestoreReport
	r.resolveEntries(s, f, failedSources, failedAssets, &rep)
	want := []string{"cursor:" + string(a), "cursor:" + string(b2), "resubmit:" + string(b2) + ":resubmit:8", "apply:" + string(b2),
		"rejected:" + string(a) + ":semreg.synthetic", "apply:" + string(b2), "cursor:" + string(secondAsset)}
	if !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("resolution calls:\n got %v\nwant %v", f.calls, want)
	}
	if len(rep.ReadBack) != 1 || len(rep.Resubmitted) != 1 || len(rep.Rejected) != 1 {
		t.Fatalf("report: %+v", rep)
	}
	if !failedAssets[a] {
		t.Fatal("an unstaged control attempt without an outcome must fail its asset closed")
	}
	if !failedSources[secondSource] || !failedAssets[secondAsset] || failedSources[synthSource] {
		t.Fatalf("incompatible cursor must fail only its source: %v %v", failedSources, failedAssets)
	}

	f2 := &fakeReplay{cursors: map[AssetID][2]any{a: {uint64(2), "y"}}, resubmit: func(b StagedBatch) AssetOutcome { return AssetOutcome{Asset: b.Asset, Uncertain: true} }}
	s2 := newStream()
	s2.Entries = s.Entries[:1]
	failedSources, failedAssets = map[SourceID]bool{}, map[AssetID]bool{}
	r.resolveEntries(s2, f2, failedSources, failedAssets, &RestoreReport{})
	if !failedSources[synthSource] || !failedAssets[a] || !failedAssets[b2] {
		t.Fatal("an uncertain resubmission must fail the source closed")
	}
}

// TestRecoveryStreamHasNoReceiptsAndStaysBounded pins the closed stream
// vocabulary (no receipt, key, principal or completed-operation record) and
// shows that automatic compaction at verified restore points keeps the entry
// log bounded while every operation stays recoverable.
func TestRecoveryStreamHasNoReceiptsAndStaysBounded(t *testing.T) {
	tags := func(v any) []string {
		var out []string
		rt := reflect.TypeOf(v)
		for i := 0; i < rt.NumField(); i++ {
			out = append(out, strings.Split(rt.Field(i).Tag.Get("json"), ",")[0])
		}
		return out
	}
	closed := map[string][]string{
		"stream":   {"format", "base", "base_wal", "base_ordinal", "next_wal", "next_ordinal", "clocks", "frame", "entries"},
		"entry":    {"kind", "source", "wal", "point", "spec", "assets"},
		"asset":    {"asset", "ordinal", "staged", "outcome"},
		"outcome":  {"accepted", "error_id", "detail", "no_batch"},
		"accepted": {"batch", "point", "result", "incarnation", "hub_seq"},
		"staged":   {"asset", "key", "sequence", "digest", "bytes", "meta"},
		"clock":    {"high", "anchor", "unit"},
	}
	got := map[string][]string{"stream": tags(walStream{}), "entry": tags(walEntry{}), "asset": tags(walAsset{}), "outcome": tags(walOutcome{}),
		"accepted": tags(AcceptedStep{}), "staged": tags(StagedBatch{}), "clock": tags(clockMark{})}
	if !reflect.DeepEqual(got, closed) {
		t.Fatalf("stream vocabulary changed: %v", got)
	}
	w := newWorld(t, worldOpts{compact: 4})
	w.mustRestore()
	w.admit(synthSource)
	maxEntries := 0
	for i := 0; i < 12; i++ {
		w.mustPublish(synthSource, synthAssets[i%2])
		entries, _ := w.rec.Stream()
		maxEntries = max(maxEntries, entries)
		for _, word := range []string{"receipt", "idempot", "principal", "operation_id"} {
			if bytes.Contains(bytes.ToLower(w.material().Payload), []byte(word)) {
				t.Fatalf("stream carries %q", word)
			}
		}
	}
	if entries, base := w.rec.Stream(); maxEntries >= 4 || !base || entries >= 4 {
		t.Fatalf("entry log not bounded by compaction: max %d now %d base %v", maxEntries, entries, base)
	}
	pre := w.states(synthAssets...)
	w.loseProcess(time.Second)
	w.mustRestore()
	assertStates(t, "bounded stream", w.states(synthAssets...), pre)
}

// queueOneOutcome publishes on the first synthetic asset with its outcome
// write failing, leaving one accepted outcome queued for Flush.
func queueOneOutcome(t *testing.T, w *world) {
	t.Helper()
	w.store.setFail(func(p []byte) error {
		if s, err := decodeStream(p); err == nil {
			if e := s.entry(synthSource, 1); e != nil && e.Assets[0].Outcome != nil {
				return errors.New("synthetic outcome write failure")
			}
		}
		return nil
	})
	if out, err := w.publish(synthSource, synthAssets[0]); !errors.Is(err, ErrOutcomeNotDurable) || out.Accepted == nil {
		t.Fatalf("queued publication: %+v %v", out, err)
	}
	w.store.setFail(nil)
}

// TestRecoveryConcurrentFlushRemovesQueuedOutcomesOnce runs two flushes while
// the first is stalled in its durable write: the second must wait for the
// whole first flush instead of snapshotting and removing the same queue.
func TestRecoveryConcurrentFlushRemovesQueuedOutcomesOnce(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.mustRestore()
	w.admit(synthSource)
	queueOneOutcome(t, w)
	entered, release := w.store.armStall(func([]byte) bool { return true })
	errs := make(chan error, 2)
	go func() { errs <- w.rec.Flush(context.Background()) }()
	<-entered
	go func() { errs <- w.rec.Flush(context.Background()) }()
	time.Sleep(50 * time.Millisecond)
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	durable := w.stream()
	if e := durable.entry(synthSource, 1); e == nil || e.Assets[0].Outcome == nil || e.Assets[0].Outcome.Accepted == nil {
		t.Fatal("the queued outcome must be durable")
	}
	if err := w.rec.Flush(context.Background()); err != nil {
		t.Fatalf("flush of an empty queue: %v", err)
	}
}

// TestRecoveryPendingOutcomeReservationsSurviveLaterIntents holds an earlier
// pending outcome and bounds the stream so that a later intent fits only if
// it may consume the earlier outcome's reservation: it must be refused
// before the hub is reached.
func TestRecoveryPendingOutcomeReservationsSurviveLaterIntents(t *testing.T) {
	w := newWorld(t, worldOpts{compact: -1})
	w.mustRestore()
	w.admit(synthSource)
	queueOneOutcome(t, w)
	// Keep the first outcome pending in the durable stream and the queue.
	cur := w.material().Payload
	s := w.stream()
	if e := s.entry(synthSource, 1); e == nil || e.Assets[0].Outcome != nil {
		t.Fatal("the first outcome must be pending")
	}
	w.rec.maxBytes = len(cur) + 2*w.rec.pendingReserve(s) - 1
	hubCalls := 0
	w.bridge.beforePublish = func(StagedBatch) { hubCalls++ }
	_, err := w.publish(synthSource, synthAssets[1])
	w.bridge.beforePublish = nil
	if refusalCode(err) != RefusalResourceExhausted || hubCalls != 0 {
		t.Fatalf("later intent must not consume pending reservations: %v (hub calls %d)", err, hubCalls)
	}
}

// TestRecoveryPartialControlFailsUnstagedAssetClosed resolves a control whose
// first asset was staged (and may be submitted) while the second never was:
// the second asset's outstanding-control gate cannot be restored, so it
// fails closed.
func TestRecoveryPartialControlFailsUnstagedAssetClosed(t *testing.T) {
	r, err := NewRecorder(RecorderOptions{Store: &faultStore{}, Hub: &hubBridge{}, Clock: &synthClock{}, Sources: []ManagedSource{
		{Driver: synthDriver, Source: synthSource, Assets: synthAssets}}})
	if err != nil {
		t.Fatal(err)
	}
	a, b := synthAssets[0], synthAssets[1]
	st := &StagedBatch{Asset: a, Key: "k", Sequence: 3, Digest: "d", Bytes: []byte("x")}
	s := newStream()
	s.Entries = []walEntry{{Kind: entryControl, Source: synthSource, WAL: 1, Point: 7,
		Assets: []walAsset{{Asset: a, Ordinal: 1, Staged: st}, {Asset: b, Ordinal: 1}}}}
	f := &fakeReplay{cursors: map[AssetID][2]any{a: {uint64(3), "d"}}}
	failedSources, failedAssets := map[SourceID]bool{}, map[AssetID]bool{}
	var rep RestoreReport
	r.resolveEntries(s, f, failedSources, failedAssets, &rep)
	if !failedAssets[b] || failedAssets[a] || failedSources[synthSource] || len(rep.ReadBack) != 1 {
		t.Fatalf("partial control: failed %v source %v report %+v", failedAssets, failedSources, rep)
	}
}

// TestRecoveryQueuedOutcomeIsDetachedFromCaller mutates the returned outcome
// after a failed outcome write: the queued outcome that Flush makes durable
// must still carry the accepted evidence exactly as the hub returned it.
func TestRecoveryQueuedOutcomeIsDetachedFromCaller(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.mustRestore()
	w.admit(synthSource)
	w.store.setFail(func(p []byte) error {
		if s, _ := decodeStream(p); s.entry(synthSource, 1) != nil && s.entry(synthSource, 1).Assets[0].Outcome != nil {
			return errors.New("synthetic outcome write failure")
		}
		return nil
	})
	out, err := w.publish(synthSource, synthAssets[0])
	w.store.setFail(nil)
	if !errors.Is(err, ErrOutcomeNotDurable) || out.Accepted == nil {
		t.Fatalf("publication with a failed outcome write: %+v %v", out, err)
	}
	want := cloneStep(out.Accepted)
	for i := range out.Accepted.Result {
		out.Accepted.Result[i] ^= 0xff
	}
	for i := range out.Accepted.Batch.Bytes {
		out.Accepted.Batch.Bytes[i] ^= 0xff
	}
	out.Accepted.Incarnation = "mutated"
	out.Accepted.Point++
	if err := w.rec.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	durable := w.stream()
	e := durable.entry(synthSource, 1)
	if e == nil || e.Assets[0].Outcome == nil || !reflect.DeepEqual(e.Assets[0].Outcome.Accepted, want) {
		t.Fatalf("caller mutation altered the durable outcome: %+v want %+v", e, want)
	}
}

// TestRecoveryReservationEstimatesSurviveCompactedRestart restarts from a
// compacted checkpoint, which holds no entries to size the outcome
// reservation from: the estimates must still cover the checkpoint.
func TestRecoveryReservationEstimatesSurviveCompactedRestart(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.mustRestore()
	w.admit(synthSource)
	for _, id := range synthAssets {
		w.mustPublish(synthSource, id)
	}
	if err := w.rec.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	w.loseProcess(time.Second)
	w.mustRestore()
	s := w.stream()
	if len(s.Entries) != 0 || len(s.Base) == 0 {
		t.Fatalf("expected a compacted stream: %d entries, %d base", len(s.Entries), len(s.Base))
	}
	w.rec.mu.Lock()
	defer w.rec.mu.Unlock()
	for _, id := range synthAssets {
		if w.rec.lastResult[id] < len(s.Base) || w.rec.lastStaged[id] < len(s.Base) {
			t.Fatalf("%s estimates after compacted restart: result %d staged %d base %d", id, w.rec.lastResult[id], w.rec.lastStaged[id], len(s.Base))
		}
	}
}

// TestRecoveryUnstagedControlWithLostRejectionFailsClosed resolves a control
// whose first attempt never reached durable staging and whose rejection
// outcome was lost: no asset may reopen reads.
func TestRecoveryUnstagedControlWithLostRejectionFailsClosed(t *testing.T) {
	r, err := NewRecorder(RecorderOptions{Store: &faultStore{}, Hub: &hubBridge{}, Clock: &synthClock{}, Sources: []ManagedSource{
		{Driver: synthDriver, Source: synthSource, Assets: synthAssets}}})
	if err != nil {
		t.Fatal(err)
	}
	a, b := synthAssets[0], synthAssets[1]
	s := newStream()
	s.Entries = []walEntry{{Kind: entryControl, Source: synthSource, WAL: 1, Point: 7,
		Assets: []walAsset{{Asset: a, Ordinal: 1}, {Asset: b, Ordinal: 1}}}}
	failedSources, failedAssets := map[SourceID]bool{}, map[AssetID]bool{}
	r.resolveEntries(s, &fakeReplay{}, failedSources, failedAssets, &RestoreReport{})
	if !failedAssets[a] || !failedAssets[b] {
		t.Fatalf("unstaged control must fail every asset closed: %v", failedAssets)
	}
}

// TestRecoveryBackwardSampleWithinProcessFailsAssetClosed moves the process
// clock backward after samples advanced: the later sample is still above the
// restore-time sample, but it is below the last one used.
func TestRecoveryBackwardSampleWithinProcessFailsAssetClosed(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.mustRestore()
	w.admit(synthSource)
	w.host.advance(100 * time.Second)
	w.mustPublish(synthSource, synthAssets[0])
	w.host.mu.Lock()
	w.host.now -= uint64(50 * time.Second)
	w.host.mu.Unlock()
	if _, err := w.publish(synthSource, synthAssets[0]); refusalCode(err) != RefusalFailedPrecondition {
		t.Fatalf("backward sample must refuse: %v", err)
	}
	w.host.advance(200 * time.Second)
	if _, err := w.publish(synthSource, synthAssets[0]); refusalCode(err) != RefusalFailedPrecondition {
		t.Fatalf("the asset must stay failed closed: %v", err)
	}
}

// TestRecoveryAmbiguousRecorderCommitArmsReadback injects the post-rename
// directory-sync failure on a recorder transaction (the outcome write after
// the hub accepted, and separately the intent write before the hub call),
// with the file either keeping the staged bytes (persisted) or reverting to
// the previous bytes (not persisted). The service's bounded readback resolver
// runs without any further request; while it is pending, Secure and managed
// publications are refused retryably without reaching the hub; once it
// settles, memory matches the committed envelope (an intent whose hub call
// never ran is withdrawn by the next transaction), the next Secure and
// publication succeed, and no intent reaches the hub twice, including at the
// next restoration.
func TestRecoveryAmbiguousRecorderCommitArmsReadback(t *testing.T) {
	for _, tc := range []struct {
		name      string
		intent    bool
		persisted bool
	}{
		{"outcome-persisted", false, true},
		{"outcome-not-persisted", false, false},
		{"intent-persisted", true, true},
		{"intent-not-persisted", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t, worldOpts{})
			exec := &synthExec{w: w, src: synthSource}
			if err := w.startService("inc1", map[DriverID]*synthExec{synthDriver: exec}); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "RUNNING", func() bool { return w.status(synthDriver).ObservedState == drivermanager.ObservedRunning })
			asset := synthAssets[0]
			memory := func() []byte {
				w.rec.mu.Lock()
				defer w.rec.mu.Unlock()
				raw, err := w.rec.stream.encode()
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}
			retryable := func(what string, err error) {
				t.Helper()
				var rf *Refusal
				if !errors.As(err, &rf) || rf.Code != RefusalUnavailable || !rf.Retryable() {
					t.Fatalf("%s during the readback window must be refused retryably: %v", what, err)
				}
			}
			var previous []byte
			inject := func() {
				info, err := os.Stat(w.path)
				if err != nil {
					t.Fatal(err)
				}
				old, err := os.ReadFile(w.path)
				if err != nil {
					t.Fatal(err)
				}
				previous = w.material().Payload
				w.store.mu.Lock()
				w.store.after = func(error) {
					if !tc.persisted {
						// The rename did not survive: the file holds the
						// previous bytes when the readback runs.
						if err := os.WriteFile(w.path, old, info.Mode().Perm()); err != nil {
							t.Error(err)
						}
					}
				}
				w.store.mu.Unlock()
				w.hooks.mu.Lock()
				w.hooks.failDirSync = 1
				w.hooks.mu.Unlock()
			}
			hubCalls := 0
			w.bridge.beforePublish = func(StagedBatch) {
				hubCalls++
				if !tc.intent && hubCalls == 1 {
					inject()
				}
			}
			entered, release := w.store.holdReadback()
			if tc.intent {
				inject()
			}
			out, err := w.publish(synthSource, asset)
			// The ambiguous write is retained exactly as staged; the store
			// keeps exposing the previous envelope until the readback.
			w.rec.mu.Lock()
			amb := w.rec.amb
			w.rec.mu.Unlock()
			if amb == nil || amb.withdraw != tc.intent || !bytes.Equal(amb.prev, previous) || !bytes.Equal(w.material().Payload, previous) {
				t.Fatalf("ambiguous write not retained: %+v", amb)
			}
			staged := amb.payload
			switch {
			case tc.intent && (err == nil || out.Accepted != nil || hubCalls != 0):
				t.Fatalf("ambiguous intent must refuse before the hub: %+v %v hub %d", out, err, hubCalls)
			case !tc.intent && (!errors.Is(err, ErrOutcomeNotDurable) || out.Accepted == nil || hubCalls != 1):
				t.Fatalf("ambiguous outcome: %+v %v hub %d", out, err, hubCalls)
			}

			// (a) The resolver runs without any further request.
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("ambiguous recorder write did not arm the readback resolver")
			}
			// (c) Secure and publications are refused retryably meanwhile,
			// with no store write and no hub call.
			writes := w.store.count()
			_, serr := w.rec.Secure(context.Background(), synthDriver, Transition{})
			retryable("Secure", serr)
			_, perr := w.publish(synthSource, synthAssets[1])
			retryable("publication", perr)
			if hubCalls != boolInt(!tc.intent) || w.store.count() != writes {
				t.Fatalf("window reached the hub (%d) or the store (%d writes)", hubCalls, w.store.count()-writes)
			}

			close(release)
			waitFor(t, "readback settled", func() bool {
				w.rec.mu.Lock()
				defer w.rec.mu.Unlock()
				return w.rec.amb == nil && w.store.AdmissionOpen()
			})
			// (b) Memory matches the committed bytes.
			committed := w.material().Payload
			wantCommitted := previous
			if tc.persisted {
				wantCommitted = staged
			}
			if !bytes.Equal(committed, wantCommitted) {
				t.Fatal("readback settled on unexpected committed bytes")
			}
			w.rec.mu.Lock()
			recorded := w.rec.committed
			w.rec.mu.Unlock()
			if !bytes.Equal(recorded, committed) {
				t.Fatal("recorder's committed payload differs from the store")
			}
			switch {
			case tc.intent && tc.persisted:
				if bytes.Equal(memory(), committed) || !bytes.Equal(memory(), previous) {
					t.Fatal("an adopted intent whose hub call never ran must stay out of memory")
				}
			default:
				if !bytes.Equal(memory(), committed) {
					t.Fatal("memory does not match the committed envelope")
				}
			}

			// (c) The next Secure succeeds and makes the outcome durable.
			if _, err := w.rec.Secure(context.Background(), synthDriver, Transition{}); err != nil {
				t.Fatalf("Secure after the readback: %v", err)
			}
			if !tc.intent {
				s := w.stream()
				e := lastEntry(t, s)
				if o := e.Assets[0].Outcome; o == nil || o.Accepted == nil || o.Accepted.HubSeq != out.Accepted.HubSeq {
					t.Fatalf("outcome not durable after Secure: %+v", e)
				}
				if !bytes.Equal(memory(), w.material().Payload) {
					t.Fatal("memory diverged from the committed envelope after Secure")
				}
			}
			// (d) The next publication proceeds and reaches the hub once.
			calls := hubCalls
			w.mustPublish(synthSource, asset)
			if hubCalls != calls+1 {
				t.Fatalf("hub calls %d, want %d", hubCalls, calls+1)
			}
			s := w.stream()
			for _, e := range s.Entries {
				for _, a := range e.Assets {
					if a.Outcome == nil {
						t.Fatalf("pending intent left in the committed stream: %+v", e)
					}
				}
			}
			if !bytes.Equal(memory(), w.material().Payload) {
				t.Fatal("memory diverged from the committed envelope")
			}
			w.bridge.beforePublish = nil
			w.loseProcess(time.Second)
			w.mustRestore()
			if rep := w.rec.Report(); len(rep.Resubmitted) != 0 || len(rep.ReadBack) != 0 || len(rep.Failed) != 0 {
				t.Fatalf("restoration resubmitted or failed an intent: %+v", rep)
			}
		})
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
