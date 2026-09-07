package modbusadapter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

type canonicalPVShadowDonor struct {
	Contract string `json:"contract"`
	Status   string `json:"status"`
	Clock    struct {
		ReceiptNS string `json:"receipt_ns"`
	} `json:"clock"`
	Provenance struct {
		AssetRef          string `json:"asset_ref"`
		Protocol          string `json:"protocol"`
		ProfileID         string `json:"profile_id"`
		ProfileVersion    string `json:"profile_version"`
		Validity          string `json:"validity"`
		RegistryDigest    string `json:"registry_digest"`
		ObservationDigest string `json:"observation_digest"`
		ShadowDigest      string `json:"shadow_digest"`
		EvidenceDigest    string `json:"evidence_digest"`
		EnergyWordHi      uint16 `json:"energy_word_hi"`
		EnergyWordLo      uint16 `json:"energy_word_lo"`
	} `json:"provenance"`
	Facts []struct {
		RequestRef      string `json:"request_ref"`
		RequestNativeID string `json:"request_native_id"`
		LegacyKey       string `json:"legacy_key"`
		LegacyID        string `json:"legacy_id"`
		Scope           string `json:"scope"`
		Phase           string `json:"phase"`
		SensorID        string `json:"sensor_id"`
		Coefficient     string `json:"coefficient"`
		Scale           int    `json:"scale"`
		Symbol          string `json:"symbol"`
		Unit            string `json:"unit"`
		FreshnessPolicy struct {
			ID                   string `json:"id"`
			Version              string `json:"version"`
			FreshForNS           string `json:"fresh_for_ns"`
			RetainForNS          string `json:"retain_for_ns"`
			MaxWallUncertaintyNS string `json:"max_wall_uncertainty_ns"`
		} `json:"freshness_policy"`
	} `json:"facts"`
	Withheld []struct {
		RequestRef      string `json:"request_ref"`
		RequestNativeID string `json:"request_native_id"`
		Outcome         string `json:"outcome"`
	} `json:"withheld"`
}

// canonicalPVShadowLegacySnapshot executes the retained canonical-PV mapper
// against the existing deterministic Fronius/SunSpec fixture. It uses only
// registry value objects; there is no socket, adapter startup, or native I/O.
func canonicalPVShadowLegacySnapshot(t *testing.T, words []uint16) pv.Snapshot {
	t.Helper()
	registry, err := modbusreg.NewStandardSunSpecDecoderRegistry(modbusreg.SunSpecModelsRevisionV1)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := modbusreg.NewSunSpecChainPlan(modbusreg.SunSpecChainPlanSpec{
		SchemaRevision: modbusreg.SunSpecModelsRevisionV1,
		BaseCandidates: []uint16{sunSpecBaseAddress},
		Limits:         modbusreg.SunSpecChainLimits{MaxTotalWords: sunSpecMaxTotalWords, MaxOccurrences: sunSpecMaxOccurrences},
		DecoderKeys:    registry.DecoderKeys(),
	})
	if err != nil {
		t.Fatal(err)
	}
	chain, offset := modbusreg.NewSunSpecChain(plan), 0
	var completed modbusreg.SunSpecChainSnapshot
	for id := uint64(1); ; id++ {
		requests := chain.NextRequests()
		if len(requests) != 1 {
			t.Fatalf("fixture replay requests=%d", len(requests))
		}
		request := requests[0]
		count := int(request.WordCount())
		if offset+count > len(words) {
			t.Fatal("fixture replay exceeded immutable words")
		}
		view, err := modbusreg.NewLogicalViewSnapshot(modbusreg.LogicalViewRecord{
			LogicalViewID: id, WireResponseID: id + 100, PhysicalRequestID: id + 200,
			Endpoint: "fixture", ConnectionID: 4, Transport: modbusreg.TransportTCP, TransportGeneration: 5,
			UnitID: 1, RequestedFunction: modbusreg.FunctionReadHoldingRegisters, ReceivedFunction: modbusreg.FunctionReadHoldingRegisters,
			Table: modbusreg.HoldingRegisters, PhysicalOffset: request.Address(), PhysicalWordCount: request.WordCount(),
			AuthorizationScope: "fixture:canonical-pv-shadow", PollGeneration: 6, DeadlineIdentity: 7,
			LogicalOffset: request.Address(), LogicalWordCount: request.WordCount(), SliceOffset: 0, SliceWordCount: request.WordCount(),
			Words: append([]uint16(nil), words[offset:offset+count]...), WireResponseBytes: []byte{byte(id)},
		})
		if err != nil {
			t.Fatal(err)
		}
		offset += count
		completed, err = chain.AdmitReplay(request, view)
		if err != nil {
			t.Fatal(err)
		}
		if len(completed.RawWords()) != 0 {
			break
		}
	}
	if offset != len(words) {
		t.Fatalf("fixture replay consumed=%d words=%d", offset, len(words))
	}
	observation, err := modbusreg.NewSunSpecQualificationObservation(registry, completed)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		t.Fatal(err)
	}
	mapper, err := NewCanonicalPVMapper()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := mapper.Map(observation, encoded, 100)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestCanonicalPVShadowLegacyProducerFixture(t *testing.T) {
	donor := canonicalPVShadowLoadDonor(t)
	words := observedFroniusFloatControlsWords()
	words[2+67+2+30], words[2+67+2+31] = donor.Provenance.EnergyWordHi, donor.Provenance.EnergyWordLo
	snapshot := canonicalPVShadowLegacySnapshot(t, words)
	if donor.Contract != "helianthus.semantic.compatibility.canonical-pv/v1" || donor.Status != "test_only_non_runtime" || len(donor.Facts) != 11 || len(donor.Withheld) != 3 {
		t.Fatalf("invalid immutable donor fixture: %+v", donor)
	}
	if snapshot.ContractID != pv.ContractV1 || snapshot.AssetRef != donor.Provenance.AssetRef || len(snapshot.Facts) != 11 || len(snapshot.RequestedOutputs) != 14 || len(snapshot.ProjectionReport) != 14 {
		t.Fatalf("legacy mapper accounting contract=%q facts=%d requested=%d projections=%d", snapshot.ContractID, len(snapshot.Facts), len(snapshot.RequestedOutputs), len(snapshot.ProjectionReport))
	}
	if snapshot.Evaluated != pv.MonotonicNanos(100) || snapshot.Source.Protocol != donor.Provenance.Protocol || snapshot.Source.ProfileID != donor.Provenance.ProfileID || snapshot.Source.ProfileVersion != donor.Provenance.ProfileVersion || snapshot.Source.Validity != pv.SourceTerminalVerified || donor.Provenance.Validity != string(pv.SourceTerminalVerified) || string(snapshot.Source.SourceRegistryRef) != donor.Provenance.RegistryDigest || string(snapshot.Source.SourceObservationRef) != donor.Provenance.ObservationDigest || string(snapshot.Source.SourceShadowRef) != donor.Provenance.ShadowDigest || string(snapshot.Source.EvidenceRef) != donor.Provenance.EvidenceDigest {
		t.Fatalf("legacy producer provenance/lifecycle drift: %+v", snapshot.Source)
	}
	type expectedProjection struct {
		outcome    pv.ProjectionOutcome
		factID     pv.FactID
		dimensions pv.Dimensions
	}
	expectedByRef := make(map[string]expectedProjection, 14)
	for _, expected := range donor.Facts {
		dimensions := pv.Dimensions{Scope: pv.Scope(expected.Scope), Phase: pv.Phase(expected.Phase), SensorID: expected.SensorID}
		if expected.RequestRef == "" || expected.RequestNativeID == "" {
			t.Fatalf("mapped donor fact %s has no request ref", expected.LegacyKey)
		}
		if want := domainDigest("canonical-pv-request-v1", []byte(expected.RequestNativeID)); expected.RequestRef != want {
			t.Fatalf("mapped donor request ref %s=%s want production digest %s", expected.RequestNativeID, expected.RequestRef, want)
		}
		if _, duplicate := expectedByRef[expected.RequestRef]; duplicate {
			t.Fatalf("duplicate donor request ref %s", expected.RequestRef)
		}
		expectedByRef[expected.RequestRef] = expectedProjection{outcome: pv.ProjectionMapped, factID: pv.FactID(expected.LegacyID), dimensions: dimensions}
	}
	for _, withheld := range donor.Withheld {
		if withheld.RequestRef == "" || withheld.RequestNativeID == "" || withheld.Outcome != "withheld" {
			t.Fatalf("invalid withheld donor output: %+v", withheld)
		}
		if want := domainDigest("canonical-pv-request-v1", []byte(withheld.RequestNativeID)); withheld.RequestRef != want {
			t.Fatalf("withheld donor request ref %s=%s want production digest %s", withheld.RequestNativeID, withheld.RequestRef, want)
		}
		if _, duplicate := expectedByRef[withheld.RequestRef]; duplicate {
			t.Fatalf("duplicate donor request ref %s", withheld.RequestRef)
		}
		expectedByRef[withheld.RequestRef] = expectedProjection{outcome: pv.ProjectionWithheld}
	}
	if len(expectedByRef) != 14 {
		t.Fatalf("donor request associations=%d", len(expectedByRef))
	}
	seenRequested := make(map[string]bool, 14)
	for _, requested := range snapshot.RequestedOutputs {
		if requested.SourceRef != snapshot.Source.SourceObservationRef {
			t.Fatalf("requested output lost donor observation: %+v", requested)
		}
		ref := string(requested.RequestedOutputRef)
		if _, ok := expectedByRef[ref]; !ok || seenRequested[ref] {
			t.Fatalf("unexpected or duplicate legacy requested output %s", ref)
		}
		seenRequested[ref] = true
	}
	if len(seenRequested) != len(expectedByRef) {
		t.Fatalf("legacy requested associations=%d want=%d", len(seenRequested), len(expectedByRef))
	}
	mapped, withheld := 0, 0
	seenProjection := make(map[string]bool, 14)
	for _, projection := range snapshot.ProjectionReport {
		if projection.SourceRef != snapshot.Source.SourceObservationRef {
			t.Fatalf("projection lost donor observation: %+v", projection)
		}
		ref := string(projection.RequestedOutputRef)
		expected, ok := expectedByRef[ref]
		if !ok || seenProjection[ref] || projection.Outcome != expected.outcome {
			t.Fatalf("legacy projection association ref=%s projection=%+v", ref, projection)
		}
		seenProjection[ref] = true
		if expected.outcome == pv.ProjectionMapped {
			mapped++
			if projection.FactID != expected.factID || projection.Dimensions == nil || *projection.Dimensions != expected.dimensions {
				t.Fatalf("mapped projection %s fact=%s dimensions=%+v", ref, projection.FactID, projection.Dimensions)
			}
		} else {
			withheld++
			if projection.FactID != "" || projection.Dimensions != nil {
				t.Fatalf("withheld projection %s unexpectedly maps fact=%s dimensions=%+v", ref, projection.FactID, projection.Dimensions)
			}
		}
	}
	if len(seenProjection) != len(expectedByRef) || mapped != 11 || withheld != 3 {
		t.Fatalf("legacy projection associations=%d mapped=%d withheld=%d", len(seenProjection), mapped, withheld)
	}
	for _, expected := range donor.Facts {
		key := pv.NewFactKey(pv.FactID(expected.LegacyID), pv.Dimensions{Scope: pv.Scope(expected.Scope), Phase: pv.Phase(expected.Phase), SensorID: expected.SensorID})
		fact, ok := snapshot.Facts[key]
		if !ok || string(fact.Unit) != expected.Unit || fact.Quality != pv.QualityGood || fact.Availability != pv.AvailabilityAvailable || fact.Freshness != pv.FreshnessFresh || fact.OriginRef != snapshot.Source.SourceObservationRef {
			t.Fatalf("legacy fact %s drifted: %#v", expected.LegacyKey, fact)
		}
		freshFor, freshErr := strconv.ParseInt(expected.FreshnessPolicy.FreshForNS, 10, 64)
		retainFor, retainErr := strconv.ParseInt(expected.FreshnessPolicy.RetainForNS, 10, 64)
		policy, policyOK := pv.PolicyByID(pv.PolicyID(expected.FreshnessPolicy.ID))
		if freshErr != nil || retainErr != nil || expected.FreshnessPolicy.Version != "1.0.0" || expected.FreshnessPolicy.MaxWallUncertaintyNS != "0" || !policyOK || policy.FreshFor != pv.MonotonicNanos(freshFor) || policy.RetainFor != pv.MonotonicNanos(retainFor) || fact.Temporal.Policy != policy.ID || fact.Temporal.FreshUntil != fact.Temporal.Receipt+policy.FreshFor || fact.Temporal.RetainUntil != fact.Temporal.Receipt+policy.RetainFor {
			t.Fatalf("legacy fact %s lifecycle drifted: fact=%#v accepted=%+v", expected.LegacyKey, fact.Temporal, expected.FreshnessPolicy)
		}
		if expected.Symbol != "" {
			if fact.Value.Symbol != expected.Symbol {
				t.Fatalf("legacy symbol %s=%q", expected.LegacyKey, fact.Value.Symbol)
			}
		} else if fact.Value.Decimal == nil || fact.Value.Decimal.Coefficient != expected.Coefficient || fact.Value.Decimal.Scale != expected.Scale {
			t.Fatalf("legacy decimal %s=%#v", expected.LegacyKey, fact.Value.Decimal)
		}
	}
}

func canonicalPVShadowLoadDonor(t *testing.T) canonicalPVShadowDonor {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate donor fixture")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(file))), "integrationtests", "canonicalpvshadow", "testdata", "donor.json"))
	if err != nil {
		t.Fatal(err)
	}
	var donor canonicalPVShadowDonor
	if err := json.Unmarshal(raw, &donor); err != nil {
		t.Fatal(err)
	}
	return donor
}
