package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

const leafPromotionReadTimeout = 3 * time.Second

type leafPromotionEEBusRuntime interface {
	Snapshot() (eebusruntime.SnapshotV1, error)
	FeaturesGet(context.Context, eebusraw.ReadAuthorizationV1, eebusraw.FeaturesGetRequestV1) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1)
	FeaturesDataGet(context.Context, eebusraw.ReadAuthorizationV1, eebusraw.FeatureDataGetRequestV1) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1)
}

type leafPromotionB524Reader interface {
	ReadB524(context.Context, promotioncapture.EBusSelector) ([]byte, time.Time, bool)
}

type leafPromotionB555Reader interface {
	ReadB555(context.Context, promotioncapture.B555Selector) ([]byte, time.Time, bool)
}

type leafPromotionSemanticB524Reader struct {
	poller *vaillantSemanticPoller
	now    func() time.Time
}

func (reader leafPromotionSemanticB524Reader) ReadB524(ctx context.Context, selector promotioncapture.EBusSelector) ([]byte, time.Time, bool) {
	if reader.poller == nil {
		return nil, time.Time{}, false
	}
	reader.poller.mu.Lock()
	controller := reader.poller.controller
	reader.poller.mu.Unlock()
	if controller == 0 || int(controller) != selector.TargetAddress {
		return nil, time.Time{}, false
	}
	readCtx, cancel := context.WithTimeout(ctx, leafPromotionReadTimeout)
	defer cancel()
	payload, ok := reader.poller.readB524ValueLive(
		readCtx, byte(selector.Opcode), byte(selector.GG), byte(selector.II), uint16(selector.RR),
	)
	now := reader.now
	if now == nil {
		now = time.Now
	}
	return bytes.Clone(payload), now().UTC(), ok
}

func (reader leafPromotionSemanticB524Reader) ReadB555(ctx context.Context, selector promotioncapture.B555Selector) ([]byte, time.Time, bool) {
	if reader.poller == nil || selector.Family != "B555" || selector.Operation != "TIMER_READ" ||
		selector.ScheduleProgram != "DHW" || selector.SlotIndex != 0 || selector.DayOfWeek != "MONDAY" {
		return nil, time.Time{}, false
	}
	reader.poller.mu.Lock()
	controller := reader.poller.controller
	reader.poller.mu.Unlock()
	if controller != 0x15 {
		return nil, time.Time{}, false
	}
	readCtx, cancel := context.WithTimeout(ctx, leafPromotionReadTimeout)
	defer cancel()
	slot := reader.poller.readB555Timer(readCtx, b555ZoneDHW, b555HCDHW, 0, byte(selector.SlotIndex))
	if slot == nil || slot.TemperatureRaw == nil || *slot.TemperatureRaw < 0 || *slot.TemperatureRaw > math.MaxUint16 {
		return nil, time.Time{}, false
	}
	payload := make([]byte, 2)
	binary.LittleEndian.PutUint16(payload, uint16(*slot.TemperatureRaw))
	now := reader.now
	if now == nil {
		now = time.Now
	}
	return payload, now().UTC(), true
}

type leafPromotionLiveSource struct {
	ebus           leafPromotionB524Reader
	ebusFallback   leafPromotionB555Reader
	eebus          leafPromotionEEBusRuntime
	admittedSource func() (byte, bool)
	windowLock     func() func()
	now            func() time.Time
}

type leafPromotionPreparedSource struct {
	localSKI          string
	localIdentityHash string
	trustStateID      string
	peerBindingID     string
	admittedSource    byte
	binding           eebusraw.RuntimeBindingV1
	candidates        map[string]leafPromotionPreparedCandidate
}

type leafPromotionPreparedCandidate struct {
	definition           promotioncapture.CandidateDefinition
	ebusIdentity         *promotioncapture.EBusIdentity
	ebusFallbackIdentity *promotioncapture.EBusIdentity
	eebusIdentity        *promotioncapture.EEBusIdentity
	locator              eebusraw.FeatureLocatorV1
}

type leafPromotionCandidateSamples struct {
	ebus         *promotioncapture.Sample
	eebus        *promotioncapture.Sample
	conflicts    []promotioncapture.Sample
	eebusRaw     []byte
	eebusResult  *eebusraw.ReadObservationV1
	ebusIdentity *promotioncapture.EBusIdentity
}

func newLeafPromotionLiveSource(
	poller *vaillantSemanticPoller,
	provider synchronizedEvidenceSnapshotProvider,
	router mcp.EEBusV1CommandRouter,
	admittedSource func() (byte, bool),
) *leafPromotionLiveSource {
	if poller == nil || synchronizedEvidenceNilInterface(provider) || synchronizedEvidenceNilInterface(router) || admittedSource == nil {
		return nil
	}
	return &leafPromotionLiveSource{
		ebus:           leafPromotionSemanticB524Reader{poller: poller},
		ebusFallback:   leafPromotionSemanticB524Reader{poller: poller},
		eebus:          &synchronizedEvidenceM625Runtime{provider: provider, router: router},
		admittedSource: admittedSource,
		windowLock: func() func() {
			poller.pollMu.Lock()
			return poller.pollMu.Unlock
		},
		now: time.Now,
	}
}

func (source *leafPromotionLiveSource) lockCaptureWindow() func() {
	if source == nil || source.windowLock == nil {
		return func() {}
	}
	return source.windowLock()
}

func (source *leafPromotionLiveSource) prepare(
	ctx context.Context,
	registry *promotioncapture.Registry,
) (leafPromotionPreparedSource, error) {
	if source == nil || source.ebus == nil || source.eebus == nil || source.admittedSource == nil || registry == nil {
		return leafPromotionPreparedSource{}, errors.New("leaf promotion live source unavailable")
	}
	admitted, ok := source.admittedSource()
	if !ok || admitted == 0 {
		return leafPromotionPreparedSource{}, errors.New("leaf promotion admitted eBUS source unavailable")
	}
	snapshot, err := source.eebus.Snapshot()
	if err != nil || snapshot.Validate() != nil || snapshot.Meta.MaskTier != eebusraw.MaskTierRaw ||
		snapshot.Status.State != eebusruntime.ObservedRuntimeStateV1Ready || snapshot.Meta.LocalSKI == "" {
		return leafPromotionPreparedSource{}, errors.New("leaf promotion eeBUS snapshot unavailable")
	}
	slots, err := leafPromotionResolveSlots(snapshot)
	if err != nil {
		return leafPromotionPreparedSource{}, err
	}
	prepared := leafPromotionPreparedSource{
		localSKI: snapshot.Meta.LocalSKI, admittedSource: admitted,
		candidates: make(map[string]leafPromotionPreparedCandidate, len(registry.Candidates())),
	}
	prepared.localIdentityHash, err = promotioncapture.CanonicalDigest(
		"HELIANTHUS:LEAF-PROMOTION:LOCAL-IDENTITY:V1\x00",
		map[string]any{"local_ski": snapshot.Meta.LocalSKI},
	)
	if err != nil {
		return leafPromotionPreparedSource{}, err
	}
	prepared.trustStateID, err = promotioncapture.CanonicalDigest(
		"HELIANTHUS:LEAF-PROMOTION:TRUST-STATE:V1\x00",
		leafPromotionTrustBinding(snapshot),
	)
	if err != nil {
		return leafPromotionPreparedSource{}, err
	}
	prepared.peerBindingID, err = promotioncapture.CanonicalDigest(
		"HELIANTHUS:LEAF-PROMOTION:PEER-BINDING:V1\x00",
		leafPromotionPeerBinding(snapshot),
	)
	if err != nil {
		return leafPromotionPreparedSource{}, err
	}
	functionCache := make(map[string]eebusraw.ReadObservationV1)
	featureCache := make(map[string]eebusraw.FeaturesGetDataV1)
	for _, candidate := range registry.Candidates() {
		item := leafPromotionPreparedCandidate{definition: candidate}
		if candidate.EBusSelector != nil {
			identity, identityErr := promotioncapture.NewB524Identity(*candidate.EBusSelector, int(admitted))
			if identityErr != nil {
				return leafPromotionPreparedSource{}, identityErr
			}
			item.ebusIdentity = &identity
		}
		if candidate.EBusFallback != nil {
			identity, identityErr := promotioncapture.NewB555Identity(
				*candidate.EBusFallback, candidate.EBusSelector.TargetAddress, int(admitted),
			)
			if identityErr != nil {
				return leafPromotionPreparedSource{}, identityErr
			}
			item.ebusFallbackIdentity = &identity
		}
		if candidate.EEBusSource != nil {
			locator, locatorErr := leafPromotionLocatorForSource(snapshot, slots, *candidate.EEBusSource)
			if locatorErr != nil {
				return leafPromotionPreparedSource{}, fmt.Errorf("%s: %w", candidate.CandidateID, locatorErr)
			}
			featureData, featureErr := source.readFeatureInventory(ctx, locator, featureCache)
			if featureErr != nil {
				return leafPromotionPreparedSource{}, fmt.Errorf("%s: %w", candidate.CandidateID, featureErr)
			}
			if prepared.binding == (eebusraw.RuntimeBindingV1{}) {
				prepared.binding = featureData.Runtime
			} else if featureData.Runtime != prepared.binding {
				return leafPromotionPreparedSource{}, errors.New("leaf promotion eeBUS generation changed during identity preflight")
			}
			if err := source.verifySourceProfile(ctx, candidate, locator, featureData, functionCache); err != nil {
				return leafPromotionPreparedSource{}, fmt.Errorf("%s: %w", candidate.CandidateID, err)
			}
			identity, identityErr := promotioncapture.NewEEBusIdentity(
				*candidate.EEBusSource, locator.RemoteSKI, locator.DeviceAddress,
				locator.EntityAddress, locator.FeatureAddress,
			)
			if identityErr != nil {
				return leafPromotionPreparedSource{}, identityErr
			}
			item.eebusIdentity = &identity
			item.locator = locator
		}
		prepared.candidates[candidate.CandidateID] = item
	}
	if prepared.binding.RuntimeEpoch == 0 || prepared.binding.ConnectionGeneration == 0 {
		return leafPromotionPreparedSource{}, errors.New("leaf promotion eeBUS runtime binding unavailable")
	}
	return prepared, nil
}

func (source *leafPromotionLiveSource) readFeatureInventory(
	ctx context.Context,
	locator eebusraw.FeatureLocatorV1,
	cache map[string]eebusraw.FeaturesGetDataV1,
) (eebusraw.FeaturesGetDataV1, error) {
	key := leafPromotionLocatorKey(locator)
	if cached, ok := cache[key]; ok {
		return cached.Clone(), nil
	}
	auth := eebusraw.ReadAuthorizationV1{
		PrincipalClass: "owner", Scope: eebusraw.AuthScopeV1RawRead,
		Tool: eebusraw.ToolV1FeaturesGet, MaskTier: eebusraw.MaskTierRaw,
	}
	request := eebusraw.FeaturesGetRequestV1{Target: locator.Clone()}
	data, terminal := source.eebus.FeaturesGet(ctx, auth, request)
	if terminal != nil || eebusraw.ValidateFeaturesGetDataV1(request, data) != nil {
		return eebusraw.FeaturesGetDataV1{}, errors.New("eeBUS feature inventory rejected")
	}
	cache[key] = data.Clone()
	return data, nil
}

func (source *leafPromotionLiveSource) verifySourceProfile(
	ctx context.Context,
	candidate promotioncapture.CandidateDefinition,
	locator eebusraw.FeatureLocatorV1,
	inventory eebusraw.FeaturesGetDataV1,
	cache map[string]eebusraw.ReadObservationV1,
) error {
	profile := candidate.EEBusSource
	if profile == nil {
		return errors.New("eeBUS source profile missing")
	}
	required := append([]string(nil), profile.DescriptionFunctions...)
	if profile.ConstraintsFunction != nil {
		required = append(required, *profile.ConstraintsFunction)
	}
	required = append(required, profile.ValueFunctions...)
	available := make(map[string]bool, len(inventory.Functions))
	for _, descriptor := range inventory.Functions {
		available[descriptor.Function] = descriptor.PossibleOperations.Read
	}
	for _, function := range required {
		if !available[function] {
			return fmt.Errorf("required function %s is not readable", function)
		}
	}

	values := make(map[string]any, len(profile.DescriptionFunctions)+1)
	for _, function := range profile.DescriptionFunctions {
		observation, err := source.readEEBusFunction(ctx, locator, function, cache)
		if err != nil {
			return err
		}
		if observation.Runtime != inventory.Runtime {
			return errors.New("eeBUS generation changed during source-profile admission")
		}
		values[function] = observation.Value.Value()
	}
	if len(profile.DescriptionFunctions) > 0 {
		actualDescriptor, err := leafPromotionDescriptor(candidate, values)
		if err != nil {
			return err
		}
		if !leafPromotionJSONEqual(profile.Descriptor, actualDescriptor) {
			return errors.New("observed SPINE descriptor differs from the catalog")
		}
	} else if candidate.ValidationMode == nil || *candidate.ValidationMode != promotioncapture.ValidationEEBusNativeMetadata {
		return errors.New("description-free SPINE source is not native metadata")
	}
	if candidate.ComparatorClass == promotioncapture.ComparatorNumeric {
		observation, readErr := source.readEEBusFunction(ctx, locator, *profile.ConstraintsFunction, cache)
		if readErr != nil {
			return readErr
		}
		if observation.Runtime != inventory.Runtime {
			return errors.New("eeBUS generation changed during source-profile admission")
		}
		constraints, constraintsErr := leafPromotionConstraints(candidate, observation.Value.Value())
		if constraintsErr != nil || *profile.DeclaredConstraints != constraints {
			return errors.New("observed SPINE constraints differ from the catalog")
		}
	}
	if candidate.ComparatorClass == promotioncapture.ComparatorEnum {
		if err := leafPromotionVerifyEnumDescription(*profile, values); err != nil {
			return err
		}
	}
	return nil
}

func (source *leafPromotionLiveSource) readEEBusFunction(
	ctx context.Context,
	locator eebusraw.FeatureLocatorV1,
	function string,
	cache map[string]eebusraw.ReadObservationV1,
) (eebusraw.ReadObservationV1, error) {
	key := leafPromotionLocatorKey(locator) + "\x00" + function
	if cached, ok := cache[key]; ok {
		return cached.Clone(), nil
	}
	target := eebusraw.FeatureTargetV1{
		RemoteSKI: locator.RemoteSKI, SHIPID: locator.SHIPID, DeviceAddress: locator.DeviceAddress,
		EntityAddress: append([]uint64(nil), locator.EntityAddress...), FeatureAddress: locator.FeatureAddress,
		FeatureType: locator.FeatureType, FeatureRole: locator.FeatureRole,
		Function: function, Operation: eebusraw.OperationV1Read,
	}
	request := eebusraw.FeatureDataGetRequestV1{Targets: []eebusraw.FeatureTargetV1{target}, TimeoutMS: uint64(leafPromotionReadTimeout / time.Millisecond)}
	auth := eebusraw.ReadAuthorizationV1{
		PrincipalClass: "owner", Scope: eebusraw.AuthScopeV1RawRead,
		Tool: eebusraw.ToolV1FeaturesDataGet, MaskTier: eebusraw.MaskTierRaw,
	}
	readCtx, cancel := context.WithTimeout(ctx, leafPromotionReadTimeout+time.Second)
	defer cancel()
	data, terminal := source.eebus.FeaturesDataGet(readCtx, auth, request)
	if eebusraw.ValidateFeatureDataGetDataV1(request, data, terminal) != nil || terminal != nil ||
		!data.Complete || len(data.Results) != 1 || len(data.Failures) != 0 {
		return eebusraw.ReadObservationV1{}, fmt.Errorf("eeBUS function %s read failed", function)
	}
	observation := data.Results[0]
	if observation.Target.Function != function || !leafPromotionLocatorEqual(observation.Target.Locator(), target.Locator()) {
		return eebusraw.ReadObservationV1{}, errors.New("eeBUS function read identity mismatch")
	}
	cache[key] = observation.Clone()
	return observation, nil
}

func (source *leafPromotionLiveSource) captureCandidate(
	ctx context.Context,
	prepared leafPromotionPreparedCandidate,
	captureGeneration, pollGeneration, pollID string,
) (leafPromotionCandidateSamples, error) {
	candidate := prepared.definition
	if candidate.ProtocolEligibility != promotioncapture.ProtocolCrossProtocol &&
		candidate.ProtocolEligibility != promotioncapture.ProtocolEEBusNative {
		return leafPromotionCandidateSamples{}, errors.New("candidate is not a real leaf")
	}
	if candidate.EEBusSource == nil || prepared.eebusIdentity == nil {
		return leafPromotionCandidateSamples{}, errors.New("candidate eeBUS identity is unavailable")
	}
	result := leafPromotionCandidateSamples{}
	if candidate.ProtocolEligibility == promotioncapture.ProtocolCrossProtocol {
		var err error
		result.ebus, result.ebusIdentity, result.eebusRaw, err = source.captureEBusCandidate(
			ctx, prepared, captureGeneration, pollGeneration, pollID,
		)
		if err != nil {
			return leafPromotionCandidateSamples{}, err
		}
	}

	cache := make(map[string]eebusraw.ReadObservationV1)
	var selected *eebusraw.ReadObservationV1
	for _, function := range candidate.EEBusSource.ValueFunctions {
		observation, err := source.readEEBusFunction(ctx, prepared.locator, function, cache)
		if err != nil {
			continue
		}
		if function == candidate.EEBusSource.ValueFunctions[0] {
			clone := observation.Clone()
			selected = &clone
		}
	}
	if selected != nil {
		sample, err := leafPromotionEEBusSample(candidate, *selected, captureGeneration)
		if err == nil {
			result.eebus = &sample
			result.eebusResult = selected
		}
	}
	return result, nil
}

func (source *leafPromotionLiveSource) captureEBusCandidate(
	ctx context.Context,
	prepared leafPromotionPreparedCandidate,
	captureGeneration, pollGeneration, pollID string,
) (*promotioncapture.Sample, *promotioncapture.EBusIdentity, []byte, error) {
	candidate := prepared.definition
	if source == nil || source.ebus == nil || candidate.EBusSelector == nil || prepared.ebusIdentity == nil {
		return nil, nil, nil, errors.New("candidate eBUS identity is unavailable")
	}
	identity := prepared.ebusIdentity
	payload, observedAt, ok := source.ebus.ReadB524(ctx, *candidate.EBusSelector)
	if !ok && candidate.EBusFallback != nil && prepared.ebusFallbackIdentity != nil && source.ebusFallback != nil {
		payload, observedAt, ok = source.ebusFallback.ReadB555(ctx, *candidate.EBusFallback)
		if ok {
			identity = prepared.ebusFallbackIdentity
		}
	}
	if !ok {
		return nil, identity, bytes.Clone(payload), nil
	}
	sample, err := leafPromotionEBusSample(candidate, *identity, payload, observedAt, captureGeneration, pollGeneration, pollID)
	if err != nil {
		return nil, identity, bytes.Clone(payload), nil
	}
	return &sample, identity, bytes.Clone(payload), nil
}

func leafPromotionEBusSample(
	candidate promotioncapture.CandidateDefinition,
	identity promotioncapture.EBusIdentity,
	payload []byte,
	observedAt time.Time,
	captureGeneration, pollGeneration, pollID string,
) (promotioncapture.Sample, error) {
	raw, value, unit, err := leafPromotionDecodeEBus(candidate, identity, payload)
	if err != nil {
		return promotioncapture.Sample{}, err
	}
	sample := promotioncapture.Sample{
		Source: promotioncapture.SourceEBus, ObservedAt: leafPromotionTimestamp(observedAt), Valid: true,
		CaptureGeneration: captureGeneration, PollID: &pollID, PollGeneration: &pollGeneration,
		RawValue: raw, Value: value, Unit: unit,
	}
	if err := sample.BindRawHash(); err != nil {
		return promotioncapture.Sample{}, err
	}
	return sample, nil
}

func leafPromotionEEBusSample(
	candidate promotioncapture.CandidateDefinition,
	observation eebusraw.ReadObservationV1,
	captureGeneration string,
) (promotioncapture.Sample, error) {
	raw, value, unit, err := leafPromotionDecodeEEBus(candidate, observation.Value.Value())
	if err != nil {
		return promotioncapture.Sample{}, err
	}
	runtimeEpoch := int64(observation.Runtime.RuntimeEpoch)
	connectionGeneration := int64(observation.Runtime.ConnectionGeneration)
	sample := promotioncapture.Sample{
		Source: promotioncapture.SourceEEBus, ObservedAt: leafPromotionTimestamp(observation.DataTimestamp), Valid: true,
		CaptureGeneration: captureGeneration, RuntimeEpoch: &runtimeEpoch, ConnectionGeneration: &connectionGeneration,
		RawValue: raw, Value: value, Unit: unit,
	}
	if err := sample.BindRawHash(); err != nil {
		return promotioncapture.Sample{}, err
	}
	return sample, nil
}

func leafPromotionDecodeEBus(candidate promotioncapture.CandidateDefinition, identity promotioncapture.EBusIdentity, payload []byte) (promotioncapture.TypedValue, promotioncapture.TypedValue, *string, error) {
	if len(payload) == 0 || candidate.EEBusSource == nil {
		return promotioncapture.TypedValue{}, promotioncapture.TypedValue{}, nil, errors.New("empty eBUS value")
	}
	if candidate.ComparatorClass == promotioncapture.ComparatorNumeric {
		var value float64
		switch identity.Family {
		case "B524":
			if len(payload) != 4 {
				return promotioncapture.TypedValue{}, promotioncapture.TypedValue{}, nil, errors.New("numeric B524 value is not float32")
			}
			value = float64(math.Float32frombits(binary.LittleEndian.Uint32(payload)))
		case "B555":
			if candidate.EBusFallback == nil || len(payload) != 2 {
				return promotioncapture.TypedValue{}, promotioncapture.TypedValue{}, nil, errors.New("numeric B555 timer value is not uint16")
			}
			raw := binary.LittleEndian.Uint16(payload)
			if raw == math.MaxUint16 {
				return promotioncapture.TypedValue{}, promotioncapture.TypedValue{}, nil, errors.New("numeric B555 timer value is unset")
			}
			value = float64(raw) / 10
		default:
			return promotioncapture.TypedValue{}, promotioncapture.TypedValue{}, nil, errors.New("unknown eBUS identity family")
		}
		decimal, err := leafPromotionDecimalString(strconv.FormatFloat(value, 'f', -1, 32))
		if err != nil {
			return promotioncapture.TypedValue{}, promotioncapture.TypedValue{}, nil, err
		}
		if candidate.Conversion == nil {
			return promotioncapture.TypedValue{}, promotioncapture.TypedValue{}, nil, errors.New("numeric conversion is unavailable")
		}
		unit := candidate.Conversion.SourceUnit
		typed := promotioncapture.NumericValue(decimal)
		return typed, typed, &unit, nil
	}
	rawNumber, err := leafPromotionUnsignedLittleEndian(payload)
	if err != nil {
		return promotioncapture.TypedValue{}, promotioncapture.TypedValue{}, nil, err
	}
	raw := promotioncapture.NumericValue(promotioncapture.Decimal{Number: int64(rawNumber)})
	normalized, err := leafPromotionMappedValue(candidate, raw, true)
	return raw, normalized, nil, err
}

func leafPromotionDecodeEEBus(candidate promotioncapture.CandidateDefinition, value any) (promotioncapture.TypedValue, promotioncapture.TypedValue, *string, error) {
	profile := candidate.EEBusSource
	if profile == nil {
		return promotioncapture.TypedValue{}, promotioncapture.TypedValue{}, nil, errors.New("missing eeBUS source")
	}
	field, err := leafPromotionSelectField(value, profile.FieldPath)
	if err != nil {
		return promotioncapture.TypedValue{}, promotioncapture.TypedValue{}, nil, err
	}
	if candidate.ComparatorClass == promotioncapture.ComparatorNumeric {
		decimal, ok := leafPromotionDecimal(field)
		if !ok {
			return promotioncapture.TypedValue{}, promotioncapture.TypedValue{}, nil, errors.New("SPINE numeric value is not a scaled number")
		}
		typed := promotioncapture.NumericValue(decimal)
		return typed, typed, cloneLeafPromotionString(profile.Unit), nil
	}
	if candidate.ComparatorClass == promotioncapture.ComparatorString {
		text, ok := field.(string)
		if !ok || text == "" || len(text) > 256 {
			return promotioncapture.TypedValue{}, promotioncapture.TypedValue{}, nil, errors.New("SPINE metadata value is not a non-empty string")
		}
		typed := promotioncapture.StringValue(text)
		return typed, typed, nil, nil
	}
	var raw promotioncapture.TypedValue
	switch typed := field.(type) {
	case bool:
		raw = promotioncapture.BooleanValue(typed)
	default:
		integer, ok := leafPromotionSignedInteger(typed)
		if !ok {
			return promotioncapture.TypedValue{}, promotioncapture.TypedValue{}, nil, errors.New("SPINE mapped value has unsupported type")
		}
		raw = promotioncapture.NumericValue(promotioncapture.Decimal{Number: integer})
	}
	normalized, err := leafPromotionMappedValue(candidate, raw, false)
	return raw, normalized, nil, err
}

func leafPromotionMappedValue(candidate promotioncapture.CandidateDefinition, raw promotioncapture.TypedValue, ebus bool) (promotioncapture.TypedValue, error) {
	profile := candidate.EEBusSource
	if profile == nil {
		return promotioncapture.TypedValue{}, errors.New("mapping profile unavailable")
	}
	var normalized json.RawMessage
	if ebus {
		if candidate.MappingProfile == nil {
			return promotioncapture.TypedValue{}, errors.New("eBUS mapping profile unavailable")
		}
		for _, pair := range candidate.MappingProfile.Pairs {
			if raw.Decimal != nil {
				if comparison, _ := raw.Decimal.Compare(pair.EBusRaw); comparison == 0 {
					normalized = pair.Normalized
					break
				}
			}
		}
	} else {
		rawJSON, err := leafPromotionRawProtocolJSON(raw)
		if err != nil {
			return promotioncapture.TypedValue{}, err
		}
		for _, pair := range profile.ExactMapping.Pairs {
			if leafPromotionJSONEqual(pair.Raw, rawJSON) {
				normalized = pair.Normalized
				break
			}
		}
	}
	if len(normalized) == 0 {
		return promotioncapture.TypedValue{}, errors.New("protocol value is not in the exact mapping")
	}
	if candidate.ComparatorClass == promotioncapture.ComparatorEnum {
		var value string
		if json.Unmarshal(normalized, &value) != nil || value == "" {
			return promotioncapture.TypedValue{}, errors.New("enum mapping is invalid")
		}
		return promotioncapture.EnumValue(value), nil
	}
	var value bool
	if json.Unmarshal(normalized, &value) != nil {
		return promotioncapture.TypedValue{}, errors.New("boolean mapping is invalid")
	}
	return promotioncapture.BooleanValue(value), nil
}

func leafPromotionRawProtocolJSON(value promotioncapture.TypedValue) ([]byte, error) {
	if value.Decimal != nil && value.Decimal.Scale == 0 {
		return json.Marshal(value.Decimal.Number)
	}
	if value.Boolean != nil {
		return json.Marshal(*value.Boolean)
	}
	return nil, errors.New("mapped protocol value is not scalar")
}

func leafPromotionDecimalString(value string) (promotioncapture.Decimal, error) {
	negative := strings.HasPrefix(value, "-")
	if negative {
		value = value[1:]
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 || len(parts) == 0 {
		return promotioncapture.Decimal{}, errors.New("invalid decimal")
	}
	digits := parts[0]
	scale := 0
	if len(parts) == 2 {
		digits += parts[1]
		scale = -len(parts[1])
	}
	number, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return promotioncapture.Decimal{}, err
	}
	if negative {
		number = -number
	}
	decimal := promotioncapture.Decimal{Number: number, Scale: scale}
	return decimal, decimal.Validate()
}

func leafPromotionUnsignedLittleEndian(payload []byte) (uint64, error) {
	if len(payload) == 0 || len(payload) > 8 {
		return 0, errors.New("invalid little-endian integer width")
	}
	var result uint64
	for index, value := range payload {
		result |= uint64(value) << (8 * index)
	}
	if result > 9_007_199_254_740_991 {
		return 0, errors.New("integer exceeds safe range")
	}
	return result, nil
}

func leafPromotionTimestamp(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func cloneLeafPromotionString(value *string) *string {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func leafPromotionLocatorKey(locator eebusraw.FeatureLocatorV1) string {
	parts := make([]string, len(locator.EntityAddress))
	for index, value := range locator.EntityAddress {
		parts[index] = strconv.FormatUint(value, 10)
	}
	return strings.Join([]string{
		locator.RemoteSKI, locator.SHIPID, locator.DeviceAddress, strings.Join(parts, ","),
		strconv.FormatUint(locator.FeatureAddress, 10), locator.FeatureType, string(locator.FeatureRole),
	}, "\x00")
}

func leafPromotionLocatorEqual(left, right eebusraw.FeatureLocatorV1) bool {
	if left.RemoteSKI != right.RemoteSKI || left.SHIPID != right.SHIPID ||
		left.DeviceAddress != right.DeviceAddress || left.FeatureAddress != right.FeatureAddress ||
		left.FeatureType != right.FeatureType || left.FeatureRole != right.FeatureRole ||
		len(left.EntityAddress) != len(right.EntityAddress) {
		return false
	}
	for index := range left.EntityAddress {
		if left.EntityAddress[index] != right.EntityAddress[index] {
			return false
		}
	}
	return true
}

func leafPromotionTrustBinding(snapshot eebusruntime.SnapshotV1) any {
	type pairingRecord struct {
		RemoteSKI string `json:"remote_ski"`
		State     string `json:"state"`
	}
	records := make([]pairingRecord, 0, len(snapshot.Pairing))
	for _, pairing := range snapshot.Pairing {
		records = append(records, pairingRecord{RemoteSKI: pairing.RemoteSKI, State: string(pairing.State)})
	}
	sort.Slice(records, func(left, right int) bool {
		if records[left].RemoteSKI != records[right].RemoteSKI {
			return records[left].RemoteSKI < records[right].RemoteSKI
		}
		return records[left].State < records[right].State
	})
	return map[string]any{"pairing": records}
}

func leafPromotionPeerBinding(snapshot eebusruntime.SnapshotV1) any {
	type peerRecord struct {
		RemoteSKI     string `json:"remote_ski"`
		SHIPID        string `json:"ship_id"`
		DeviceAddress string `json:"device_address"`
	}
	records := make([]peerRecord, 0, len(snapshot.Devices))
	for _, device := range snapshot.Devices {
		shipID := ""
		if device.SHIPID != nil {
			shipID = *device.SHIPID
		}
		records = append(records, peerRecord{
			RemoteSKI: device.SKI, SHIPID: shipID, DeviceAddress: device.Address,
		})
	}
	sort.Slice(records, func(left, right int) bool {
		if records[left].RemoteSKI != records[right].RemoteSKI {
			return records[left].RemoteSKI < records[right].RemoteSKI
		}
		if records[left].SHIPID != records[right].SHIPID {
			return records[left].SHIPID < records[right].SHIPID
		}
		return records[left].DeviceAddress < records[right].DeviceAddress
	})
	return map[string]any{"devices": records}
}

type leafPromotionSlots struct {
	byName map[string]eebusruntime.EntityV1
}

func leafPromotionResolveSlots(snapshot eebusruntime.SnapshotV1) (leafPromotionSlots, error) {
	byType := make(map[string][]eebusruntime.EntityV1)
	for _, entity := range snapshot.Entities {
		byType[entity.Type] = append(byType[entity.Type], entity)
	}
	for key := range byType {
		sort.Slice(byType[key], func(left, right int) bool {
			return byType[key][left].EntityAddress < byType[key][right].EntityAddress
		})
	}
	if len(byType["DeviceInformation"]) != 1 || len(byType["DHWCircuit"]) != 1 ||
		len(byType["HeatingZone"]) != 2 || len(byType["HVACRoom"]) != 2 || len(byType["TemperatureSensor"]) != 1 {
		return leafPromotionSlots{}, errors.New("VR940 candidate entity slots are ambiguous")
	}
	return leafPromotionSlots{byName: map[string]eebusruntime.EntityV1{
		"device_information": byType["DeviceInformation"][0], "dhw_circuit": byType["DHWCircuit"][0],
		"zone_1": byType["HeatingZone"][0], "zone_2": byType["HeatingZone"][1],
		"zone_1_room": byType["HVACRoom"][0],
		"zone_2_room": byType["HVACRoom"][1], "outside_sensor": byType["TemperatureSensor"][0],
	}}, nil
}

func leafPromotionLocatorForSource(snapshot eebusruntime.SnapshotV1, slots leafPromotionSlots, source promotioncapture.EEBusSource) (eebusraw.FeatureLocatorV1, error) {
	entity, ok := slots.byName[source.EntitySlot]
	if !ok || entity.Type != source.EntityType {
		return eebusraw.FeatureLocatorV1{}, errors.New("candidate entity slot is unavailable")
	}
	var matches []eebusruntime.FeatureV1
	for _, feature := range snapshot.Features {
		if feature.DeviceAddress == entity.DeviceAddress && feature.EntityAddress == entity.EntityAddress &&
			feature.Type == source.FeatureType && feature.Role == source.FeatureRole {
			matches = append(matches, feature)
		}
	}
	if len(matches) != 1 {
		return eebusraw.FeatureLocatorV1{}, errors.New("candidate feature is ambiguous")
	}
	var device *eebusruntime.DeviceV1
	for index := range snapshot.Devices {
		if snapshot.Devices[index].Address == entity.DeviceAddress {
			device = &snapshot.Devices[index]
			break
		}
	}
	if device == nil || device.SHIPID == nil || *device.SHIPID == "" {
		return eebusraw.FeatureLocatorV1{}, errors.New("candidate device identity is incomplete")
	}
	entityAddress, featureAddress, err := leafPromotionParseSnapshotAddress(entity.DeviceAddress, entity.EntityAddress, matches[0].FeatureAddress)
	if err != nil {
		return eebusraw.FeatureLocatorV1{}, err
	}
	locator := eebusraw.FeatureLocatorV1{
		RemoteSKI: device.SKI, SHIPID: *device.SHIPID, DeviceAddress: entity.DeviceAddress,
		EntityAddress: entityAddress, FeatureAddress: featureAddress,
		FeatureType: source.FeatureType, FeatureRole: eebusraw.FeatureRoleV1Server,
	}
	if terminal := eebusraw.ValidateFeaturesGetRequestV1(eebusraw.FeaturesGetRequestV1{Target: locator}); terminal != nil {
		return eebusraw.FeatureLocatorV1{}, errors.New("candidate feature locator is invalid")
	}
	return locator, nil
}

func leafPromotionParseSnapshotAddress(device, entityText, featureText string) ([]uint64, uint64, error) {
	prefix := device + ":["
	if device == "" || !strings.HasPrefix(entityText, prefix) || !strings.HasSuffix(entityText, "]:") || !strings.HasPrefix(featureText, entityText) {
		return nil, 0, errors.New("invalid SPINE snapshot address")
	}
	body := strings.TrimSuffix(strings.TrimPrefix(entityText, prefix), "]:")
	parts := strings.Split(body, ",")
	entity := make([]uint64, len(parts))
	for index, part := range parts {
		value, err := strconv.ParseUint(part, 10, 64)
		if err != nil || strconv.FormatUint(value, 10) != part {
			return nil, 0, errors.New("invalid SPINE entity address")
		}
		entity[index] = value
	}
	featureText = strings.TrimPrefix(featureText, entityText)
	feature, err := strconv.ParseUint(featureText, 10, 64)
	if err != nil || strconv.FormatUint(feature, 10) != featureText {
		return nil, 0, errors.New("invalid SPINE feature address")
	}
	return entity, feature, nil
}

func leafPromotionDescriptor(candidate promotioncapture.CandidateDefinition, values map[string]any) (json.RawMessage, error) {
	profile := candidate.EEBusSource
	if profile == nil {
		return nil, errors.New("source profile missing")
	}
	result := make(map[string]any)
	for _, function := range profile.DescriptionFunctions {
		root, ok := values[function].(map[string]any)
		if !ok {
			return nil, errors.New("description response is not an object")
		}
		var listName, idName string
		switch function {
		case "measurementDescriptionListData":
			listName, idName = "measurementDescriptionData", "measurementId"
		case "setpointDescriptionListData":
			listName, idName = "setpointDescriptionData", "setpointId"
		case "hvacSystemFunctionDescriptionListData":
			listName, idName = "hvacSystemFunctionDescriptionData", "systemFunctionId"
		case "hvacOverrunDescriptionListData":
			listName, idName = "hvacOverrunDescriptionData", "overrunId"
		default:
			continue
		}
		identity := int64(0)
		if function == "setpointDescriptionListData" {
			identity = 1
		}
		item, err := leafPromotionListItem(root[listName], idName, identity)
		if err != nil {
			return nil, err
		}
		for rawKey, rawValue := range item {
			key := leafPromotionSnakeCase(rawKey)
			if key == "affected_system_function_id" {
				key = "affected_system_function_ids"
			}
			result[key] = rawValue
		}
	}
	return promotioncapture.CanonicalJSON(result)
}

func leafPromotionConstraints(candidate promotioncapture.CandidateDefinition, value any) (promotioncapture.DeclaredConstraints, error) {
	root, ok := value.(map[string]any)
	if !ok || candidate.EEBusSource == nil {
		return promotioncapture.DeclaredConstraints{}, errors.New("constraints response is not an object")
	}
	listName, idName, minimumName, maximumName, stepName, identity :=
		"measurementConstraintsData", "measurementId", "valueRangeMin", "valueRangeMax", "valueStepSize", int64(0)
	if candidate.EEBusSource.FeatureType == "Setpoint" {
		listName, idName, minimumName, maximumName, stepName, identity =
			"setpointConstraintsData", "setpointId", "setpointRangeMin", "setpointRangeMax", "setpointStepSize", int64(1)
	}
	item, err := leafPromotionListItem(root[listName], idName, identity)
	if err != nil {
		return promotioncapture.DeclaredConstraints{}, err
	}
	minimum, okMin := leafPromotionDecimal(item[minimumName])
	maximum, okMax := leafPromotionDecimal(item[maximumName])
	step, okStep := leafPromotionDecimal(item[stepName])
	if !okMin || !okMax || !okStep {
		return promotioncapture.DeclaredConstraints{}, errors.New("constraints scaled numbers are incomplete")
	}
	return promotioncapture.DeclaredConstraints{Minimum: minimum, Maximum: maximum, Step: step}, nil
}

func leafPromotionVerifyEnumDescription(profile promotioncapture.EEBusSource, values map[string]any) error {
	modeRoot, modeOK := values["hvacOperationModeDescriptionListData"].(map[string]any)
	relationRoot, relationOK := values["hvacSystemFunctionOperationModeRelationListData"].(map[string]any)
	if !modeOK || !relationOK || profile.ExactMapping == nil {
		return errors.New("operation-mode descriptions are incomplete")
	}
	modes, ok := modeRoot["hvacOperationModeDescriptionData"].([]any)
	if !ok {
		return errors.New("operation-mode description list is invalid")
	}
	actual := make(map[int64]string)
	for _, raw := range modes {
		item, ok := raw.(map[string]any)
		id, idOK := leafPromotionSignedInteger(item["operationModeId"])
		mode, modeOK := item["operationModeType"].(string)
		if !ok || !idOK || !modeOK {
			return errors.New("operation-mode description row is invalid")
		}
		actual[id] = mode
	}
	relation, err := leafPromotionListItem(relationRoot["hvacSystemFunctionOperationModeRelationData"], "systemFunctionId", 0)
	if err != nil {
		return err
	}
	allowed, ok := relation["operationModeId"].([]any)
	if !ok {
		return errors.New("operation-mode relation is invalid")
	}
	allowedSet := make(map[int64]bool, len(allowed))
	for _, raw := range allowed {
		id, valid := leafPromotionSignedInteger(raw)
		if !valid {
			return errors.New("operation-mode relation id is invalid")
		}
		allowedSet[id] = true
	}
	for _, pair := range profile.ExactMapping.Pairs {
		var id int64
		var normalized string
		if json.Unmarshal(pair.Raw, &id) != nil || json.Unmarshal(pair.Normalized, &normalized) != nil || actual[id] != normalized || !allowedSet[id] {
			return errors.New("operation-mode description differs from the exact mapping")
		}
	}
	return nil
}

func leafPromotionListItem(value any, identityField string, identity int64) (map[string]any, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, errors.New("SPINE list data is invalid")
	}
	var result map[string]any
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		id, valid := leafPromotionSignedInteger(item[identityField])
		if ok && valid && id == identity {
			if result != nil {
				return nil, errors.New("duplicate SPINE descriptor identity")
			}
			result = item
		}
	}
	if result == nil {
		return nil, errors.New("SPINE descriptor identity not found")
	}
	return result, nil
}

func leafPromotionSelectField(value any, fieldPath string) (any, error) {
	root, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("SPINE value response is not an object")
	}
	if !strings.ContainsAny(fieldPath, "[].") {
		result, found := root[fieldPath]
		if !found {
			return nil, errors.New("catalog direct field is absent from SPINE value")
		}
		return result, nil
	}
	open := strings.IndexByte(fieldPath, '[')
	close := strings.IndexByte(fieldPath, ']')
	dot := strings.IndexByte(fieldPath, '.')
	if open <= 0 || close <= open || dot != close+1 {
		return nil, errors.New("catalog field path is invalid")
	}
	listName := fieldPath[:open]
	selector := fieldPath[open+1 : close]
	selectorParts := strings.Split(selector, "=")
	if len(selectorParts) != 2 {
		return nil, errors.New("catalog field selector is invalid")
	}
	identity, err := strconv.ParseInt(selectorParts[1], 10, 64)
	if err != nil {
		return nil, err
	}
	item, err := leafPromotionListItem(root[listName], selectorParts[0], identity)
	if err != nil {
		return nil, err
	}
	field := fieldPath[dot+1:]
	result, ok := item[field]
	if !ok {
		return nil, errors.New("catalog field is absent from SPINE value")
	}
	return result, nil
}

func leafPromotionDecimal(value any) (promotioncapture.Decimal, bool) {
	object, ok := value.(map[string]any)
	if !ok {
		return promotioncapture.Decimal{}, false
	}
	number, numberOK := leafPromotionSignedInteger(object["number"])
	scale, scaleOK := leafPromotionSignedInteger(object["scale"])
	decimal := promotioncapture.Decimal{Number: number, Scale: int(scale)}
	return decimal, numberOK && scaleOK && decimal.Validate() == nil
}

func leafPromotionSignedInteger(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case uint64:
		return int64(typed), typed <= math.MaxInt64
	case json.Number:
		parsed, err := strconv.ParseInt(string(typed), 10, 64)
		return parsed, err == nil
	default:
		reflected, err := json.Marshal(typed)
		if err != nil {
			return 0, false
		}
		var number json.Number
		if json.Unmarshal(reflected, &number) != nil {
			return 0, false
		}
		parsed, err := strconv.ParseInt(string(number), 10, 64)
		return parsed, err == nil
	}
}

func leafPromotionJSONEqual(left, right []byte) bool {
	leftCanonical, leftErr := promotioncapture.CanonicalJSON(json.RawMessage(left))
	rightCanonical, rightErr := promotioncapture.CanonicalJSON(json.RawMessage(right))
	return leftErr == nil && rightErr == nil && bytes.Equal(leftCanonical, rightCanonical)
}

func leafPromotionSnakeCase(value string) string {
	var result strings.Builder
	for index, char := range value {
		if char >= 'A' && char <= 'Z' {
			if index > 0 {
				result.WriteByte('_')
			}
			result.WriteRune(char + ('a' - 'A'))
		} else {
			result.WriteRune(char)
		}
	}
	return result.String()
}

var _ leafPromotionB524Reader = leafPromotionSemanticB524Reader{}
var _ leafPromotionB555Reader = leafPromotionSemanticB524Reader{}
var _ leafPromotionEEBusRuntime = (*synchronizedEvidenceM625Runtime)(nil)
