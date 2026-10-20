package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

// Boiler flow meaning remains Hypothesis under the public M14 mapping. An
// exact native product/route/codec witness and policy must be supplied by the
// native owner independently. Production supplies neither today.
type boilerThermalIdentity struct {
	Asset                                           semreg.AssetID
	Source                                          semreg.SourceID
	Epoch                                           semreg.SourceEpochID
	Generation                                      semreg.Uint64
	Binding                                         semreg.NativeBindingID
	Service                                         semreg.ServiceInstanceID
	Product, Profile, Revision, Codec, ResponseForm string
	Initiator, Target, PB, SB, Operation            byte
	Selector                                        [2]byte
}

type boilerThermalBinding struct {
	Identity  boilerThermalIdentity
	Qualified bool
	Basis     []semreg.EvidenceRef
	Policy    semreg.FreshnessPolicy
}

type boilerThermalObservation struct {
	Identity       boilerThermalIdentity
	Request, Reply []byte
	Received       semreg.TimePoint
	Receipt        semreg.MonotonicPoint
	Valid          bool
}

type boilerThermalPublisher struct {
	mu     sync.Mutex
	hub    *semhub.Hub
	handle func() (*semhub.Handle, error)
	// No runtime qualification is inferred from family discovery or controller identity.
	binding          *boilerThermalBinding
	attempt, good    *boilerThermalObservation
	start            time.Time
	now              func() (semreg.TimePoint, semreg.MonotonicPoint)
	lastError        error
	published        *boilerThermalBinding
	legacyProjection projection.ProjectionDisposition
	onControl        func(semhub.ControlEvent) error
	withheldNative   *boilerThermalObservation
}

func (p *vaillantSemanticPoller) attachBoilerThermal(hub *semhub.Hub, handle func() (*semhub.Handle, error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.boilerThermal != nil {
		return
	}
	pub := &boilerThermalPublisher{hub: hub, handle: handle, start: time.Now()}
	pub.onControl = func(semhub.ControlEvent) error {
		p.boilerPublishMu.Lock()
		defer p.boilerPublishMu.Unlock()
		p.mu.Lock()
		if p.boiler != nil {
			p.boiler.FlowTemperatureC = nil
		}
		p.mu.Unlock()
		if p.provider != nil {
			status := p.provider.BoilerStatus()
			if status != nil && status.State.FlowTemperatureC != nil {
				status.State.FlowTemperatureC = nil
				p.provider.SetBoilerStatus(status)
				if p.hub != nil {
					p.hub.PublishBoilerStatusUpdate(status)
				}
			}
		}
		return nil
	}
	pub.now = func() (semreg.TimePoint, semreg.MonotonicPoint) {
		return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(time.Now().UnixNano(), 10)), ClockID: "clock.utc", UncertaintyNS: "0"}, semreg.MonotonicPoint{ClockEpochID: semreg.ClockEpochID("clock:boiler:" + strconv.FormatInt(pub.start.UnixNano(), 10)), Nanoseconds: semreg.Uint64(strconv.FormatInt(time.Since(pub.start).Nanoseconds(), 10))}
	}
	p.boilerThermal = pub
}

// Called only inside an actual existing scheduler fetch. Cache hits never
// acquire a new receipt or replace retained evidence.
func (p *vaillantSemanticPoller) retainBoilerThermalRead(request protocol.Frame, reply *protocol.Frame, err error) {
	p.mu.Lock()
	pub := p.boilerThermal
	p.mu.Unlock()
	if pub == nil {
		return
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	wall, mono := pub.now()
	id := boilerThermalIdentity{Initiator: request.Source, Target: request.Target, PB: request.Primary, SB: request.Secondary, Operation: vaillantB509OpcodeRead, Selector: [2]byte{0x18, 0}}
	if pub.binding != nil {
		id = pub.binding.Identity
		id.Initiator, id.Target, id.PB, id.SB = request.Source, request.Target, request.Primary, request.Secondary
	}
	o := &boilerThermalObservation{Identity: id, Request: append([]byte(nil), request.Data...), Received: wall, Receipt: mono}
	if reply != nil {
		o.Reply = append([]byte(nil), reply.Data...)
	}
	o.Valid = err == nil && reply != nil && request.EffectiveFrameType() == protocol.FrameTypeInitiatorTarget && request.Primary == 0xb5 && request.Secondary == 9
	pub.attempt = o
}

func boilerThermalValue(o *boilerThermalObservation, b *boilerThermalBinding) (*semreg.Value, bool) {
	if o == nil || b == nil || !b.Qualified || !o.Valid || o.Identity != b.Identity || len(b.Basis) == 0 || b.Policy.Validate() != nil {
		return nil, false
	}
	id := b.Identity
	if id.Asset.Validate() != nil || id.Source.Validate() != nil || id.Epoch.Validate() != nil || id.Generation.Validate() != nil || id.Generation == "0" || id.Binding.Validate() != nil || id.Service.Validate() != nil || id.Product == "" || id.Profile == "" || semreg.Uint64(id.Revision).Validate() != nil || id.Revision == "0" || id.Codec != "DATA2c" || id.Target == 0 || id.PB != 0xb5 || id.SB != 9 || id.Operation != 0x0d || id.Selector != [2]byte{0x18, 0} || o.Received.Validate() != nil || o.Receipt.Validate() != nil {
		return nil, false
	}
	for _, e := range b.Basis {
		if e.Validate() != nil {
			return nil, false
		}
	}
	if !bytes.Equal(o.Request, []byte{0x0d, 0x18, 0}) {
		return nil, false
	}
	prefix := []byte{0x18, 0}
	switch id.ResponseForm {
	case "payload":
		prefix = nil
	case "selector":
	case "operation_selector":
		prefix = []byte{0x0d, 0x18, 0}
	default:
		return nil, false
	}
	if len(o.Reply) != len(prefix)+2 || !bytes.Equal(o.Reply[:len(prefix)], prefix) {
		return nil, false
	}
	raw := o.Reply[len(prefix):]
	if raw[0] == 0 && raw[1] == 0x80 {
		return nil, false
	}
	n := int64(int16(binary.LittleEndian.Uint16(raw)))
	// n/16 = (n*625)*10^-4, exactly, including valid zero.
	coefficient, exponent := n*625, int32(-4)
	if coefficient == 0 {
		exponent = 0
	} else {
		for coefficient%10 == 0 {
			coefficient /= 10
			exponent++
		}
	}
	value := &semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: strconv.FormatInt(coefficient, 10), Exponent10: exponent}, Unit: "unit.celsius"}}
	if thermal.New().ValidateFact(boilerThermalKey(), value) != nil {
		return nil, false
	}
	return value, true
}

func boilerThermalKey() semreg.FactKey {
	role := "temperature:boiler_flow"
	pack := thermal.New().Pack()
	return semreg.FactKey{PackID: pack.ID, PackVersion: pack.Version, FactID: "thermal.measurement.water_temperature", Dimensions: []semreg.Dimension{{ID: "thermal.dimension.temperature", Value: semreg.Value{Kind: semreg.ValueText, Text: &role}}}}
}

// Same-epoch monotonic evaluation is conservative: stale observations retain
// their native evidence but this scalar policy publishes only fresh values.
func boilerThermalFresh(o *boilerThermalObservation, b *boilerThermalBinding, mono semreg.MonotonicPoint) bool {
	if _, ok := boilerThermalValue(o, b); !ok || mono.Validate() != nil || o.Receipt.ClockEpochID != mono.ClockEpochID {
		return false
	}
	receipt, e1 := strconv.ParseUint(string(o.Receipt.Nanoseconds), 10, 64)
	now, e2 := strconv.ParseUint(string(mono.Nanoseconds), 10, 64)
	fresh, e3 := strconv.ParseUint(string(b.Policy.FreshForNS), 10, 64)
	retain, e4 := strconv.ParseUint(string(b.Policy.RetainForNS), 10, 64)
	return e1 == nil && e2 == nil && e3 == nil && e4 == nil && now >= receipt && now-receipt < fresh && now-receipt < retain
}

func boilerThermalEvidence(o *boilerThermalObservation) semreg.EvidenceRef {
	h := sha256.New()
	// hash.Hash.Write always succeeds; formatting directly preserves the digest bytes.
	_, _ = fmt.Fprintf(h, "%#v", o.Identity)
	h.Write(o.Request)
	h.Write(o.Reply)
	return semreg.EvidenceRef{Owner: "helianthus.gateway.runtime", Kind: "boiler.native.observation", Digest: semreg.Digest(fmt.Sprintf("sha256:%x", h.Sum(nil))), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

func (p *boilerThermalPublisher) scalar(target byte) *float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	wall, mono := p.now()
	p.legacyProjection = projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: boilerThermalKey().FactID, Outcome: projection.ProjectionWithheld, SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{}}
	if p.published != nil && (p.binding == nil || !p.binding.Qualified || !reflect.DeepEqual(p.binding, p.published) || p.binding.Identity.Target != target) {
		handle, err := p.handle()
		if err != nil || handle == nil {
			p.lastError = err
			return nil
		}
		old := p.published.Identity
		// Lifecycle control retires candidates of predecessor epochs and
		// generations. Never send their identity through a successor handle.
		if handle.Source() == old.Source && handle.Epoch() == old.Epoch && handle.GenerationID() == old.Generation {
			current := p.binding
			p.binding = p.published
			p.lastError = p.publish(handle, wall, mono, false)
			p.binding = current
			if p.lastError != nil {
				return nil
			}
		}
		p.withheldNative = p.good
		p.good = nil
		if p.attempt != nil && (p.binding == nil || p.attempt.Identity != p.binding.Identity) {
			p.attempt = nil
		}
		p.published = nil
	}
	if p.binding == nil || !p.binding.Qualified || p.binding.Identity.Target != target {
		p.good = nil
		return nil
	}
	b := p.binding
	handle, err := p.handle()
	if err != nil || handle == nil || handle.Source() != b.Identity.Source || handle.Epoch() != b.Identity.Epoch || handle.GenerationID() != b.Identity.Generation {
		p.withheldNative = p.good
		p.good = nil
		return nil
	}
	if boilerThermalFresh(p.attempt, b, mono) {
		if p.good == nil || p.good.Receipt.ClockEpochID != p.attempt.Receipt.ClockEpochID || circuitTemperatureNS(p.attempt.Receipt.Nanoseconds) > circuitTemperatureNS(p.good.Receipt.Nanoseconds) {
			copy := *p.attempt
			copy.Request = append([]byte(nil), copy.Request...)
			copy.Reply = append([]byte(nil), copy.Reply...)
			p.good = &copy
		}
	}
	eligible := boilerThermalFresh(p.good, b, mono)
	// A failed acquisition cannot change receipt or seed retention.
	err = p.publish(handle, wall, mono, eligible)
	p.lastError = err
	if err == nil && eligible {
		copy := *b
		copy.Basis = append([]semreg.EvidenceRef(nil), b.Basis...)
		p.published = &copy
	}
	if err != nil || !eligible || !p.hub.ReadAdmitted(b.Identity.Asset) {
		return nil
	}
	view, err := p.hub.Inspect(b.Identity.Asset)
	if err != nil {
		return nil
	}
	for _, envelope := range view.Snapshot.Facts {
		for _, fact := range envelope.Candidates {
			if fact.CandidateID == p.candidateID() && fact.Value != nil && fact.Value.Quantity != nil && fact.Quality.Availability == semreg.AvailabilityAvailable {
				n, _ := strconv.ParseFloat(string(fact.Value.Quantity.Number.Coefficient), 64)
				for exp := fact.Value.Quantity.Number.Exponent10; exp < 0; exp++ {
					n /= 10
				}
				for exp := fact.Value.Quantity.Number.Exponent10; exp > 0; exp-- {
					n *= 10
				}
				p.legacyProjection.Outcome = projection.ProjectionTransformed
				p.legacyProjection.SourceKeys = []semreg.FactKey{fact.Key}
				for _, kind := range []projection.LossKind{projection.LossIdentity, projection.LossPolicy, projection.LossProvenance, projection.LossTime} {
					p.legacyProjection.Loss = append(p.legacyProjection.Loss, projection.LossDetail{Kind: kind, SourceItems: []semreg.DefinitionID{fact.Key.FactID}, Description: "legacy scalar omits native identity, evidence, receipt and freshness policy"})
				}
				return &n
			}
		}
	}
	return nil
}

func (p *boilerThermalPublisher) candidateID() semreg.CandidateID {
	return semreg.CandidateID("candidate:boiler-flow:" + string(p.binding.Identity.Binding))
}

func (p *boilerThermalPublisher) publish(handle *semhub.Handle, wall semreg.TimePoint, mono semreg.MonotonicPoint, eligible bool) error {
	b := p.binding
	id := b.Identity
	if _, err := p.hub.Declare(semhub.AssetSpec{AssetID: id.Asset, SourceID: id.Source, Driver: string(primaryEBusDriverID), Validators: []semreg.PackValidator{thermal.New()}, OnControl: p.onControl}); err != nil {
		return err
	}
	tx, err := p.hub.Begin(handle, id.Asset)
	if err != nil {
		return err
	}
	defer tx.Abort()
	current, exists := tx.Current()
	expected := semreg.Uint64("0")
	if exists {
		expected = current.Revisions.Semantic
	}
	sequence := tx.NextSequence()
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:boiler-flow:" + string(id.Epoch) + ":" + string(id.Generation) + ":" + string(sequence)), AssetID: id.Asset, SourceID: id.Source, SourceEpochID: id.Epoch, DriverGeneration: id.Generation, Sequence: sequence, ExpectedSemanticRevision: expected, ObservedAt: wall, SourceRetirements: tx.SourceRetirements()}
	batch.SourceUpserts = []semreg.SourceDescriptor{}
	batch.BindingUpserts = []semreg.NativeBinding{}
	batch.IdentityLinkUpserts = []semreg.IdentityLink{}
	batch.FactUpserts = []semreg.FactCandidate{}
	batch.FactWithdrawals = []semreg.CandidateID{}
	batch.ServiceUpserts = []semreg.ServiceInstance{}
	batch.ServiceWithdrawals = []semreg.ServiceInstanceID{}
	batch.CapabilityUpserts = []semreg.CapabilityInstance{}
	batch.CapabilityWithdrawals = []semreg.CapabilityInstanceID{}
	batch.GenerationFences = []semreg.GenerationFence{}
	var prior *semreg.FactCandidate
	for _, envelope := range current.Facts {
		for _, fact := range envelope.Candidates {
			if fact.CandidateID == p.candidateID() {
				copy := fact
				prior = &copy
			}
		}
	}
	if eligible && prior != nil && prior.Times.ReceivedAt == p.good.Received && prior.Times.ReceiptMonotonic == p.good.Receipt && reflect.DeepEqual(prior.Evidence, []semreg.EvidenceRef{boilerThermalEvidence(p.good)}) {
		return nil
	}
	if !eligible {
		if prior == nil {
			return nil
		}
		batch.FactWithdrawals = []semreg.CandidateID{prior.CandidateID}
	} else {
		o := p.good
		evidence := boilerThermalEvidence(o)
		hasSource, hasBinding := false, false
		for _, s := range current.Sources {
			hasSource = hasSource || s.SourceID == id.Source && s.SourceEpochID == id.Epoch
		}
		for _, v := range current.Bindings {
			hasBinding = hasBinding || v.BindingID == id.Binding
		}
		if !hasSource {
			batch.SourceUpserts = []semreg.SourceDescriptor{{SourceID: id.Source, SourceEpochID: id.Epoch, ProtocolID: "ebus.native", ProfileID: semreg.DefinitionID(id.Profile), ProfileVersion: "1", RegistryEvidence: b.Basis[0], StartedAt: o.Received, State: semreg.SourceCurrent, Revision: "1"}}
		}
		if !hasBinding {
			batch.BindingUpserts = []semreg.NativeBinding{{BindingID: id.Binding, AssetID: id.Asset, SourceID: id.Source, SourceEpochID: id.Epoch, DriverGeneration: id.Generation, NativeResource: evidence, State: semreg.BindingCurrent, Revision: "1"}}
			batch.IdentityLinkUpserts = []semreg.IdentityLink{{AssetID: id.Asset, BindingID: id.Binding, State: semreg.LinkQualified, Basis: b.Basis, Revision: "1"}}
		}
		value, _ := boilerThermalValue(o, b)
		revision := semreg.Uint64("1")
		if prior != nil {
			n, _ := strconv.ParseUint(string(prior.Revision), 10, 64)
			revision = semreg.Uint64(strconv.FormatUint(n+1, 10))
		}
		batch.FactUpserts = []semreg.FactCandidate{{CandidateID: p.candidateID(), Key: boilerThermalKey(), Value: value, Quality: semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}}, Times: semreg.Times{ReceivedAt: o.Received, ReceiptMonotonic: o.Receipt, EvaluatedAt: wall, EvaluateMonotonic: mono}, FreshnessPolicy: b.Policy, BindingID: &id.Binding, SourceEpochID: &id.Epoch, DriverGeneration: &id.Generation, Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:boiler-flow:" + string(id.Binding)), Kind: semreg.OriginNativeObservation, SourceID: &id.Source, SourceEpochID: &id.Epoch, BindingID: &id.Binding, Evidence: []semreg.EvidenceRef{evidence}}, Evidence: []semreg.EvidenceRef{evidence}, Revision: revision}}
	}
	batch.BatchDigest, err = batch.ComputedDigest()
	if err != nil {
		return err
	}
	if _, _, err = tx.Apply(batch, mono); err != nil {
		return err
	}
	_, err = tx.Commit(nil)
	return err
}
