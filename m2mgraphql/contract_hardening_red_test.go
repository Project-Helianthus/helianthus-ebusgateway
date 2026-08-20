package m2mgraphql

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
)

//go:embed testdata/public-graphql-m2m-v1.query.graphql
var canonicalM2MQuery string

func TestM2MCurrentSnapshot_RequiresTheOneCanonicalQueryForEveryValidRequest(t *testing.T) {
	handler, err := NewHandler(Config{
		SnapshotByAsset: func(context.Context, string) (pv.Snapshot, bool) { return m2mGoldenSnapshot(), true },
		AssetExists:     func(string) bool { return true }, AllowedAssets: map[string]struct{}{"pv-asset-golden": {}},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct{ name, query, code string }{
		{"canonical", canonicalM2MQuery, ""},
		{"reduced", strings.Replace(canonicalM2MQuery, " contractId canonicalContractId assetRef generation producedAt", " contractId", 1), "QUERY_REJECTED"},
		{"modified", strings.Replace(canonicalM2MQuery, "M2MCurrentSnapshot", "M2MCurrentSnapshotModified", 1), "QUERY_REJECTED"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", strings.NewReader(m2mCanonicalRequest(test.query, "pv-asset-golden")))
			request = request.WithContext(WithMTLSPrincipal(request.Context(), "principal-canonical"))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if test.code != "" {
				assertM2MError(t, response, test.code)
				return
			}
			if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"errors"`) {
				t.Fatalf("body=%s", response.Body.String())
			}
		})
	}
}

func TestM2MCurrentSnapshot_AcceptsCanonicalASTIndependentOfFormatting(t *testing.T) {
	handler, err := NewHandler(Config{
		SnapshotByAsset: func(context.Context, string) (pv.Snapshot, bool) { return m2mGoldenSnapshot(), true },
		AssetExists:     func(string) bool { return true }, AllowedAssets: map[string]struct{}{"pv-asset-golden": {}},
	})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", strings.NewReader(m2mCanonicalRequest(strings.Join(strings.Fields(canonicalM2MQuery), " "), "pv-asset-golden")))
	request = request.WithContext(WithMTLSPrincipal(request.Context(), "principal-canonical-ast"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"errors"`) {
		t.Fatalf("canonical AST with alternate whitespace was rejected: %s", response.Body.String())
	}
}

func TestM2MCurrentSnapshot_FullWireGoldenAndMCPParity(t *testing.T) {
	snapshot := m2mGoldenSnapshot()
	handler, err := NewHandler(Config{
		SnapshotByAsset: func(context.Context, string) (pv.Snapshot, bool) { return snapshot, true },
		AssetExists:     func(string) bool { return true }, AllowedAssets: map[string]struct{}{snapshot.AssetRef: {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", bytes.NewBufferString(m2mCanonicalRequest(canonicalM2MQuery, snapshot.AssetRef)))
	request = request.WithContext(WithMTLSPrincipal(request.Context(), "principal-golden"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}

	var envelope struct {
		Data struct {
			Snapshot map[string]any `json:"m2mCurrentSnapshot"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	got := envelope.Data.Snapshot
	assertM2MGoldenWire(t, got)

	// Both ingress projections must be lossless views of the same immutable snapshot.
	mcp, err := MCPCurrentSnapshot(snapshot)
	if err != nil {
		t.Fatalf("MCPCurrentSnapshot: %v", err)
	}
	graphqlWire, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	mcpWire, err := json.Marshal(mcp)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(graphqlWire, mcpWire) {
		t.Fatalf("MCP/GraphQL parity mismatch\ngraphql=%s\nmcp=%s", graphqlWire, mcpWire)
	}
}

func TestM2MCurrentSnapshot_ProjectsEachCanonicalContinuityVariant(t *testing.T) {
	variants := []struct {
		continuity pv.Continuity
		typename   string
	}{
		{pv.Continuity{State: pv.ContinuityBaseline}, "M2MBaselineContinuity"},
		{pv.Continuity{State: pv.ContinuityContiguous, Delta: m2mDecimal("1")}, "M2MContiguousContinuity"},
		{pv.Continuity{State: pv.ContinuityRollover, Delta: m2mDecimal("2"), Modulus: m2mDecimal("100"), EvidenceRef: m2mDigest(30)}, "M2MRolloverContinuity"},
		{pv.Continuity{State: pv.ContinuityReset, EvidenceRef: m2mDigest(31)}, "M2MResetContinuity"},
		{pv.Continuity{State: pv.ContinuityDiscontinuity}, "M2MDiscontinuityContinuity"},
	}
	for _, variant := range variants {
		t.Run(variant.typename, func(t *testing.T) {
			snapshot := m2mGoldenSnapshot()
			key := pv.NewFactKey(pv.FactEnergyActiveExportTotal, pv.Dimensions{Scope: pv.ScopeTotal})
			fact := snapshot.Facts[key]
			continuity := variant.continuity
			fact.Continuity = &continuity
			snapshot.Facts[key] = fact
			handler, err := NewHandler(Config{
				SnapshotByAsset: func(context.Context, string) (pv.Snapshot, bool) { return snapshot, true },
				AssetExists:     func(string) bool { return true }, AllowedAssets: map[string]struct{}{snapshot.AssetRef: {}},
			})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", strings.NewReader(m2mCanonicalRequest(canonicalM2MQuery, snapshot.AssetRef)))
			request = request.WithContext(WithMTLSPrincipal(request.Context(), "principal-continuity"))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			var envelope struct {
				Data struct {
					Snapshot struct {
						Facts []struct {
							FactID     string         `json:"factId"`
							Continuity map[string]any `json:"continuity"`
						} `json:"facts"`
					} `json:"m2mCurrentSnapshot"`
				} `json:"data"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			for _, item := range envelope.Data.Snapshot.Facts {
				if item.FactID == string(pv.FactEnergyActiveExportTotal) && item.Continuity["__typename"] != variant.typename {
					t.Fatalf("continuity=%#v want %s", item.Continuity, variant.typename)
				}
			}
		})
	}
}

func TestM2MCurrentSnapshot_EmitsExactClosedWireShapes(t *testing.T) {
	handler, err := NewHandler(Config{
		SnapshotByAsset: func(context.Context, string) (pv.Snapshot, bool) { return m2mGoldenSnapshot(), true },
		AssetExists:     func(string) bool { return true }, AllowedAssets: map[string]struct{}{"pv-asset-golden": {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", strings.NewReader(m2mCanonicalRequest(canonicalM2MQuery, "pv-asset-golden")))
	request = request.WithContext(WithMTLSPrincipal(request.Context(), "principal-wire-shape"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	var envelope struct {
		Data struct {
			Snapshot struct {
				Facts []map[string]any `json:"facts"`
			} `json:"m2mCurrentSnapshot"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	for _, fact := range envelope.Data.Snapshot.Facts {
		dimension := fact["dimension"].(map[string]any)
		value := fact["value"].(map[string]any)
		if _, exists := dimension["__typename"]; exists {
			t.Fatalf("dimension exposed unselected __typename: %#v", dimension)
		}
		if _, exists := value["__typename"]; exists {
			t.Fatalf("value exposed unselected __typename: %#v", value)
		}
		if scale, exists := value["scale"]; exists {
			if _, ok := scale.(float64); !ok {
				t.Fatalf("decimal scale is not a GraphQL Int: %#v", scale)
			}
		}
		continuity, exists := fact["continuity"]
		if !exists {
			t.Fatalf("selected continuity field was omitted: %#v", fact)
		}
		if fact["factId"] == string(pv.FactEnergyActiveExportTotal) {
			row := continuity.(map[string]any)
			if row["baseline"] != "BASELINE" {
				t.Fatalf("baseline marker=%#v want BASELINE", row["baseline"])
			}
		}
	}
}

func m2mCanonicalRequest(query, assetRef string) string {
	b, err := json.Marshal(map[string]any{"operationName": "M2MCurrentSnapshot", "query": query, "variables": map[string]any{"request": map[string]string{"contractId": "PUBLIC_GRAPHQL_M2M_V1", "assetRef": assetRef}}})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func assertM2MGoldenWire(t *testing.T, got map[string]any) {
	t.Helper()
	for key, want := range map[string]any{
		"contractId": "PUBLIC_GRAPHQL_M2M_V1", "canonicalContractId": "helianthus.canonical-pv/v1", "assetRef": "pv-asset-golden",
		"generation": "71", "evaluatedMonotonicNs": "990", "sourceTimeState": "UNAVAILABLE",
	} {
		if got[key] != want {
			t.Fatalf("%s=%#v want %#v", key, got[key], want)
		}
	}
	if producedAt, ok := got["producedAt"].(string); !ok || producedAt == "" {
		t.Fatalf("producedAt=%#v; want preserved publication time", got["producedAt"])
	}
	if _, leaked := got["sourceShadowRef"]; leaked {
		t.Fatal("public sourceShadowRef leaked")
	}
	for _, key := range []string{"facts", "capabilities", "provenance", "requestedOutputs", "projectionReport"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("missing %s", key)
		}
	}
	facts, ok := got["facts"].([]any)
	if !ok || len(facts) != 7 {
		t.Fatalf("facts=%#v", got["facts"])
	}
	var dimensions, values, continuities = map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, raw := range facts {
		fact := raw.(map[string]any)
		for k := range fact["dimension"].(map[string]any) {
			dimensions[k] = true
		}
		value := fact["value"].(map[string]any)
		if _, ok := value["coefficient"]; ok {
			values["decimal"] = true
		}
		if _, ok := value["symbol"]; ok {
			values["enum"] = true
		}
		if _, ok := value["symbols"]; ok {
			values["bitfield"] = true
		}
		if c, ok := fact["continuity"].(map[string]any); ok {
			continuities[c["__typename"].(string)] = true
		}
	}
	for _, key := range []string{"scope", "phase", "phasePair", "inputId", "sensorId"} {
		if !dimensions[key] {
			t.Fatalf("dimension union missing %s: %#v", key, dimensions)
		}
	}
	for _, key := range []string{"decimal", "enum", "bitfield"} {
		if !values[key] {
			t.Fatalf("value union missing %s: %#v", key, values)
		}
	}
	if !continuities["M2MBaselineContinuity"] || len(continuities) != 1 {
		t.Fatalf("golden continuity=%#v; want canonical baseline only", continuities)
	}
	provenance := got["provenance"].([]any)
	if len(provenance) != 2 {
		t.Fatalf("provenance=%#v", provenance)
	}
	for _, raw := range provenance {
		row := raw.(map[string]any)
		for _, key := range []string{"originRef", "sourceProtocol", "sourceProfileId", "sourceProfileVersion", "sourceValidity", "sourceRegistryRef", "sourceObservationRef", "evidenceRef"} {
			if _, ok := row[key]; !ok {
				t.Fatalf("provenance missing %s: %#v", key, row)
			}
		}
		if _, leaked := row["sourceShadowRef"]; leaked {
			t.Fatal("provenance leaked sourceShadowRef")
		}
	}
	reports := got["projectionReport"].([]any)
	kinds := map[string]bool{}
	for _, raw := range reports {
		kinds[raw.(map[string]any)["__typename"].(string)] = true
	}
	for _, key := range []string{"M2MMappedProjectionReportEntry", "M2MWithheldProjectionReportEntry", "M2MUnrepresentableProjectionReportEntry"} {
		if !kinds[key] {
			t.Fatalf("projection outcome missing %s", key)
		}
	}
}

func m2mGoldenSnapshot() pv.Snapshot {
	originA := m2mDigest(1)
	originB := m2mDigest(2)
	provenance := func(ref pv.Digest) pv.Provenance {
		return pv.Provenance{SourceIdentity: pv.SourceIdentity{Protocol: "sunspec_modbus", ProfileID: "sunspec.inverter.three_phase.monitoring@1.0.0", ProfileVersion: "1.0.0", Validity: pv.SourceTerminalVerified}, SourceRegistryRef: m2mDigest(3), SourceObservationRef: ref, SourceShadowRef: m2mDigest(4), EvidenceRef: m2mDigest(5)}
	}
	fact := func(id pv.FactID, dimensions pv.Dimensions, value pv.FactValue, unit pv.Unit, policy pv.PolicyID, continuity *pv.Continuity) pv.Fact {
		return pv.Fact{ID: id, Dimensions: dimensions, Value: value, Unit: unit, Quality: pv.QualityGood, Availability: pv.AvailabilityAvailable, Freshness: pv.FreshnessFresh, Temporal: pv.Temporal{Receipt: 100, FreshUntil: 200, RetainUntil: 300, Policy: policy}, OriginRef: originA, Continuity: continuity}
	}
	facts := map[pv.FactKey]pv.Fact{}
	add := func(f pv.Fact) { facts[pv.NewFactKey(f.ID, f.Dimensions)] = f }
	baseline := pv.Continuity{State: pv.ContinuityBaseline}
	add(fact(pv.FactEnergyActiveExportTotal, pv.Dimensions{Scope: pv.ScopeTotal}, pv.DecimalFactValue(*m2mDecimal("7310")), pv.UnitWattHour, pv.PolicyAccumulatorV1, &baseline))
	add(fact(pv.FactACCurrent, pv.Dimensions{Phase: pv.PhaseL1}, pv.DecimalFactValue(*m2mDecimal("11")), pv.UnitAmpere, pv.PolicyTelemetryFastV1, nil))
	add(fact(pv.FactACVoltageLineToLine, pv.Dimensions{PhasePair: pv.PhasePairL1L2}, pv.DecimalFactValue(*m2mDecimal("400")), pv.UnitVolt, pv.PolicyTelemetryFastV1, nil))
	add(fact(pv.FactDCVoltage, pv.Dimensions{InputID: "input-1"}, pv.DecimalFactValue(*m2mDecimal("600")), pv.UnitVolt, pv.PolicyTelemetryFastV1, nil))
	add(fact(pv.FactTemperature, pv.Dimensions{SensorID: "cabinet"}, pv.DecimalFactValue(*m2mDecimal("42")), pv.UnitCelsius, pv.PolicyTelemetryFastV1, nil))
	add(fact(pv.FactOperatingState, pv.Dimensions{Scope: pv.ScopeTotal}, pv.EnumFactValue(pv.OperatingStateOperating), pv.UnitOne, pv.PolicyStatusV1, nil))
	add(fact(pv.FactEventFlags, pv.Dimensions{Scope: pv.ScopeTotal}, pv.BitfieldFactValue("COMMUNICATION_FAULT"), pv.UnitOne, pv.PolicyStatusV1, nil))

	requested := make([]pv.RequestedOutput, 0, len(facts)+2)
	report := make([]pv.Projection, 0, len(facts)+2)
	index := 10
	for _, value := range facts {
		ref := m2mDigest(index)
		index++
		requested = append(requested, pv.RequestedOutput{SourceRef: originA, RequestedOutputRef: ref})
		dimensions := value.Dimensions
		report = append(report, pv.Projection{SourceRef: originA, RequestedOutputRef: ref, FactID: value.ID, Dimensions: &dimensions, Outcome: pv.ProjectionMapped})
	}
	for _, outcome := range []pv.ProjectionOutcome{pv.ProjectionWithheld, pv.ProjectionUnrepresentable} {
		ref := m2mDigest(index)
		index++
		requested = append(requested, pv.RequestedOutput{SourceRef: originA, RequestedOutputRef: ref})
		report = append(report, pv.Projection{SourceRef: originA, RequestedOutputRef: ref, Outcome: outcome})
	}
	return pv.Snapshot{ContractID: pv.ContractV1, AssetRef: "pv-asset-golden", Generation: 71, Evaluated: 990, SourceTimeState: pv.SourceTimeUnavailable, Source: provenance(originA), Origins: map[pv.Digest]pv.Provenance{originA: provenance(originA), originB: provenance(originB)}, Capability: pv.Capability{ID: pv.CapabilityThreePhaseTelemetryV1, Outcome: pv.CapabilityNotSatisfied}, Facts: facts, RequestedOutputs: requested, ProjectionReport: report}
}

func m2mDecimal(coefficient string) *pv.Decimal {
	value := pv.MustDecimal(coefficient, 0)
	return &value
}

func m2mDigest(index int) pv.Digest {
	return pv.Digest(fmt.Sprintf("sha256:%064x", index))
}
