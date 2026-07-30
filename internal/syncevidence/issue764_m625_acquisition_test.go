package syncevidence

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

type issue764M625Runtime struct {
	mu sync.Mutex

	snapshot eebusruntime.SnapshotV1
	binding  eebusraw.RuntimeBindingV1
	driftAt  uint64

	snapshotCalls int
	featureCalls  []eebusraw.FeaturesGetRequestV1
	dataCalls     []eebusraw.FeatureDataGetRequestV1
	auth          []eebusraw.ReadAuthorizationV1
}

func (runtime *issue764M625Runtime) Snapshot() (eebusruntime.SnapshotV1, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.snapshotCalls++
	return runtime.snapshot, nil
}

func (runtime *issue764M625Runtime) FeaturesGet(
	_ context.Context,
	auth eebusraw.ReadAuthorizationV1,
	request eebusraw.FeaturesGetRequestV1,
) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.auth = append(runtime.auth, auth)
	runtime.featureCalls = append(runtime.featureCalls, request.Clone())
	binding := runtime.binding
	if request.Target.FeatureAddress == runtime.driftAt {
		binding.ConnectionGeneration++
	}
	data := eebusraw.FeaturesGetDataV1{
		Feature: request.Target.Clone(),
		Functions: []eebusraw.FunctionDescriptorV1{{
			Function:           "measurementListData",
			PossibleOperations: eebusraw.FullOperationsV1{Read: true},
			Changeable:         eebusraw.ChangeabilityV1False,
			Constraints: eebusraw.ConstraintSetV1{
				Status: eebusraw.ConstraintStatusV1Known,
				Unit:   "degC",
			},
		}},
		Runtime:       binding,
		DataTimestamp: time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC),
		Source:        eebusraw.ObservationSourceV1Cache,
	}
	hash, err := data.ComputeDataHash()
	if err != nil {
		panic(err)
	}
	data.DataHash = hash
	if terminal := eebusraw.ValidateFeaturesGetDataV1(request, data); terminal != nil {
		panic(fmt.Sprintf("invalid features data: %#v", terminal))
	}
	return data, nil
}

func (runtime *issue764M625Runtime) FeaturesDataGet(
	_ context.Context,
	auth eebusraw.ReadAuthorizationV1,
	request eebusraw.FeatureDataGetRequestV1,
) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.auth = append(runtime.auth, auth)
	runtime.dataCalls = append(runtime.dataCalls, request.Clone())
	results := make([]eebusraw.ReadObservationV1, 0, len(request.Targets))
	base := time.Date(2026, 7, 30, 12, 0, 1, 0, time.UTC)
	for index, target := range request.Targets {
		value, err := eebusraw.NewTypedValueV1(map[string]any{
			"measurementData": []any{map[string]any{
				"measurementId": int64(target.FeatureAddress),
				"value": map[string]any{
					"number": int64(215 + index),
					"scale":  int64(-1),
				},
			}},
		})
		if err != nil {
			panic(err)
		}
		observedAt := base.Add(time.Duration(target.FeatureAddress) * time.Millisecond)
		observation := eebusraw.ReadObservationV1{
			Target: target.Clone(), Runtime: runtime.binding,
			RawRequest: eebusraw.ProtocolMessageV1{
				Classifier: "READ", CorrelationKey: target.FeatureAddress, Function: target.Function,
			},
			RawResponse: eebusraw.ProtocolMessageV1{
				Classifier: "REPLY", CorrelationKey: target.FeatureAddress, Function: target.Function, Data: &value,
			},
			Value: value, RequestedAt: observedAt, ReceivedAt: observedAt,
			DataTimestamp: observedAt, Source: eebusraw.ObservationSourceV1Live,
			ReadToken: eebusraw.ReadTokenV1{
				ReadToken: strings.Repeat("E", 43), Reusable: true,
				ExpiresAt: observedAt.Add(time.Minute),
				BindingHash: eebusraw.HashV1(
					"sha256:" + strings.Repeat("1", 64),
				),
			},
		}
		hash, err := observation.ComputeDataHash()
		if err != nil {
			panic(err)
		}
		observation.DataHash = hash
		results = append(results, observation)
	}
	data := eebusraw.FeatureDataGetDataV1{Results: results, Complete: true}
	if terminal := eebusraw.ValidateFeatureDataGetDataV1(request, data, nil); terminal != nil {
		panic(fmt.Sprintf("invalid read data: %#v", terminal))
	}
	return data, nil
}

func issue764RawSnapshot(t *testing.T, eligible int) eebusruntime.SnapshotV1 {
	t.Helper()
	runtimeID, err := eebusraw.RedactID(eebusraw.IDKindPeer, "issue764-runtime")
	if err != nil {
		t.Fatal(err)
	}
	shipID := "issue764-ship"
	remoteSKI := strings.Repeat("a", 40)
	observed := time.Date(2026, 7, 30, 11, 59, 59, 0, time.UTC)
	features := make([]eebusruntime.FeatureV1, 0, eligible+2)
	for address := eligible; address >= 1; address-- {
		features = append(features, eebusruntime.FeatureV1{
			DeviceAddress:  "device-1",
			EntityAddress:  "device-1:[1]:",
			FeatureAddress: fmt.Sprintf("device-1:[1]:%d", address),
			Type:           "Measurement",
			Role:           "server",
		})
	}
	features = append(features,
		eebusruntime.FeatureV1{
			DeviceAddress: "device-1", EntityAddress: "device-1:[1]:",
			FeatureAddress: "device-1:[1]:99", Type: "Measurement", Role: "client",
		},
		eebusruntime.FeatureV1{
			DeviceAddress: "device-1", EntityAddress: "device-1:[1]:",
			FeatureAddress: "device-1:[1]:100", Type: "DeviceConfiguration", Role: "server",
		},
	)
	snapshot, err := eebusruntime.NewSnapshotV1(eebusruntime.SnapshotV1{
		Meta: eebusruntime.SnapshotMetaV1{
			Contract:   eebusruntime.SnapshotContractV1,
			Runtime:    runtimeID,
			LocalSKI:   strings.Repeat("b", 40),
			MaskTier:   eebusraw.MaskTierRaw,
			CapturedAt: observed.Add(time.Second), DataTimestamp: observed,
		},
		Status: eebusruntime.RuntimeObservationV1{State: eebusruntime.ObservedRuntimeStateV1Ready},
		Services: []eebusruntime.ServiceV1{{
			SKI: remoteSKI, SHIPID: &shipID, Kind: eebusruntime.ServiceKindV1Remote,
			Visible: true, Paired: true,
		}},
		Devices: []eebusruntime.DeviceV1{{
			SKI: remoteSKI, SHIPID: &shipID, Address: "device-1", Type: "heating",
		}},
		Entities: []eebusruntime.EntityV1{{
			DeviceAddress: "device-1", EntityAddress: "device-1:[1]:", Type: "zone",
		}},
		Features: features,
	})
	if err != nil {
		t.Fatalf("construct raw snapshot: %v", err)
	}
	return snapshot
}

func TestIssue764M625AcquisitionSelectsOrdersBatchesAndRedacts(t *testing.T) {
	runtime := &issue764M625Runtime{
		snapshot: issue764RawSnapshot(t, 17),
		binding:  eebusraw.RuntimeBindingV1{RuntimeEpoch: 7, ConnectionGeneration: 3},
	}
	reader, err := NewM625EEBusReader(runtime)
	if err != nil {
		t.Fatalf("NewM625EEBusReader: %v", err)
	}
	evidence, err := reader.ReadFeatureData(context.Background(), SourceRequest{
		Phase: PhasePre, Limits: DefaultLimitsV1(),
		OperationID: "eebus.v1.features.data.get", OperationScope: "feature-data", MaskTier: "redacted",
		AuthScope: AuthScopeV1{Authority: "effective", Permissions: []string{"eebus.raw.read"}},
	})
	if err != nil {
		t.Fatalf("ReadFeatureData: %v", err)
	}
	value, _, err := parseJSON(evidence.NormalizedEvidence, DefaultLimitsV1(), false)
	if err != nil {
		t.Fatal(err)
	}
	payload := value.(map[string]any)
	if err := validateM625Payload(payload, sourceCapture{
		sourceKind: SourceEEBus, sourceContract: M625EEBusContractV1, sourceSchemaVersion: 1,
		sourceObservedAt: evidence.SourceObservedAt,
	}); err != nil {
		t.Fatalf("normalized M6.25 payload: %v\n%s", err, evidence.NormalizedEvidence)
	}
	if got := len(payload["observations"].([]any)); got != 17 {
		t.Fatalf("observation count = %d, want 17", got)
	}

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.snapshotCalls != 1 || len(runtime.featureCalls) != 17 || len(runtime.dataCalls) != 2 {
		t.Fatalf("snapshot/features/data calls = %d/%d/%d, want 1/17/2",
			runtime.snapshotCalls, len(runtime.featureCalls), len(runtime.dataCalls))
	}
	if len(runtime.dataCalls[0].Targets) != 16 || len(runtime.dataCalls[1].Targets) != 1 {
		t.Fatalf("batch sizes = %d/%d, want 16/1", len(runtime.dataCalls[0].Targets), len(runtime.dataCalls[1].Targets))
	}
	for index, call := range runtime.featureCalls {
		if call.Target.FeatureAddress != uint64(index+1) {
			t.Fatalf("feature call %d address = %d, want complete native order", index, call.Target.FeatureAddress)
		}
	}
	for _, auth := range runtime.auth {
		if auth != (eebusraw.ReadAuthorizationV1{
			PrincipalClass: "owner",
			Scope:          eebusraw.AuthScopeV1RawRead,
			Tool:           auth.Tool,
			MaskTier:       eebusraw.MaskTierRaw,
		}) || (auth.Tool != eebusraw.ToolV1FeaturesGet && auth.Tool != eebusraw.ToolV1FeaturesDataGet) {
			t.Fatalf("runtime authorization = %#v", auth)
		}
	}
	for _, forbidden := range []string{
		strings.Repeat("a", 40),
		"issue764-ship",
		"device-1",
		"read_token",
		"raw_request",
		"candidate_ref",
	} {
		if strings.Contains(string(evidence.NormalizedEvidence), forbidden) {
			t.Fatalf("normalized evidence leaks %q: %s", forbidden, evidence.NormalizedEvidence)
		}
	}
}

func TestIssue764M625AcquisitionFailsWithoutEligibleServerRead(t *testing.T) {
	runtime := &issue764M625Runtime{
		snapshot: issue764RawSnapshot(t, 0),
		binding:  eebusraw.RuntimeBindingV1{RuntimeEpoch: 7, ConnectionGeneration: 3},
	}
	reader, err := NewM625EEBusReader(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadFeatureData(context.Background(), SourceRequest{
		Phase: PhasePre, Limits: DefaultLimitsV1(),
		OperationID: "eebus.v1.features.data.get", OperationScope: "feature-data", MaskTier: "redacted",
		AuthScope: AuthScopeV1{Authority: "effective", Permissions: []string{"eebus.raw.read"}},
	}); err == nil {
		t.Fatal("zero eligible server READ features succeeded")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if len(runtime.featureCalls) != 0 || len(runtime.dataCalls) != 0 {
		t.Fatalf("zero-selection runtime calls = %d/%d, want 0/0", len(runtime.featureCalls), len(runtime.dataCalls))
	}
}

func TestIssue764M625AcquisitionRejectsRuntimeBindingSubstitutionBeforeRead(t *testing.T) {
	runtime := &issue764M625Runtime{
		snapshot: issue764RawSnapshot(t, 3),
		binding:  eebusraw.RuntimeBindingV1{RuntimeEpoch: 7, ConnectionGeneration: 3},
		driftAt:  2,
	}
	reader, err := NewM625EEBusReader(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadFeatureData(context.Background(), SourceRequest{
		Phase: PhasePre, Limits: DefaultLimitsV1(),
		OperationID: "eebus.v1.features.data.get", OperationScope: "feature-data", MaskTier: "redacted",
		AuthScope: AuthScopeV1{Authority: "effective", Permissions: []string{"eebus.raw.read"}},
	}); err == nil {
		t.Fatal("cross-generation feature inventory succeeded")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if len(runtime.featureCalls) != 2 || len(runtime.dataCalls) != 0 {
		t.Fatalf("binding substitution calls = features:%d data:%d, want 2/0",
			len(runtime.featureCalls), len(runtime.dataCalls))
	}
}
