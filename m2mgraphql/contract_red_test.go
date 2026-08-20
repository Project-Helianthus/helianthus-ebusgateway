package m2mgraphql

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
)

//go:embed testdata/contract.json
var contractFixture []byte

type contractFixtureShape struct {
	ContractID          string `json:"contractId"`
	CanonicalContractID string `json:"canonicalContractId"`
	Route               string `json:"route"`
	OperationName       string `json:"operationName"`
}

func TestM2MCurrentSnapshot_OnlyAcceptsTheDedicatedFixedContract(t *testing.T) {
	var contract contractFixtureShape
	if err := json.Unmarshal(contractFixture, &contract); err != nil {
		t.Fatalf("decode contract fixture: %v", err)
	}

	handler, err := NewHandler(Config{
		SnapshotByAsset: func(context.Context, string) (pv.Snapshot, bool) {
			return m2mFixtureSnapshot(), true
		},
		AssetExists:   func(string) bool { return true },
		AllowedAssets: map[string]struct{}{"pv-asset-fixture": {}},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, contract.Route, bytes.NewBufferString(m2mCanonicalRequest(canonicalM2MQuery, "pv-asset-fixture")))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(WithMTLSPrincipal(request.Context(), "fixture-principal"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Data struct {
			M2MCurrentSnapshot struct {
				ContractID          string `json:"contractId"`
				CanonicalContractID string `json:"canonicalContractId"`
				AssetRef            string `json:"assetRef"`
				Facts               []struct {
					Value map[string]any `json:"value"`
				} `json:"facts"`
			} `json:"m2mCurrentSnapshot"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Data.M2MCurrentSnapshot.ContractID != contract.ContractID ||
		body.Data.M2MCurrentSnapshot.CanonicalContractID != contract.CanonicalContractID ||
		body.Data.M2MCurrentSnapshot.AssetRef != "pv-asset-fixture" {
		t.Fatalf("contract projection=%+v", body.Data.M2MCurrentSnapshot)
	}
	if len(body.Data.M2MCurrentSnapshot.Facts) != 1 || body.Data.M2MCurrentSnapshot.Facts[0].Value["coefficient"] != "7310" {
		t.Fatalf("decimal projection=%#v; decimals must remain integer strings", body.Data.M2MCurrentSnapshot.Facts)
	}
}

func TestM2MCurrentSnapshot_HasNoGenericFallbackRawOrSubscriptionSurface(t *testing.T) {
	handler, err := NewHandler(Config{AllowedAssets: map[string]struct{}{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		request *http.Request
		code    string
	}{
		{httptest.NewRequest(http.MethodGet, "/graphql/m2m/v1", nil), "REQUEST_INVALID"},
		{httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(`{"query":"{ m2mCurrentSnapshot { assetRef } }"}`)), "QUERY_REJECTED"},
		{httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", bytes.NewBufferString(`{"query":"subscription M2MCurrentSnapshot { m2mCurrentSnapshot { assetRef } }"}`)), "REQUEST_INVALID"},
	} {
		request := test.request.WithContext(WithMTLSPrincipal(test.request.Context(), "fixture-principal"))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		assertM2MError(t, response, test.code)
	}
}

func m2mFixtureSnapshot() pv.Snapshot {
	dimensions := pv.Dimensions{Scope: pv.ScopeTotal}
	origin := pv.Digest("sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	requestedRef := pv.Digest("sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	provenance := pv.Provenance{SourceIdentity: pv.SourceIdentity{Protocol: "sunspec_modbus", ProfileID: "sunspec.inverter.three_phase.monitoring@1.0.0", ProfileVersion: "1.0.0", Validity: pv.SourceTerminalVerified}, SourceRegistryRef: pv.Digest("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), SourceObservationRef: origin, SourceShadowRef: pv.Digest("sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"), EvidenceRef: pv.Digest("sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")}
	key := pv.NewFactKey(pv.FactACActivePower, dimensions)
	return pv.Snapshot{ContractID: pv.ContractV1, AssetRef: "pv-asset-fixture", Generation: 1, Evaluated: 100, SourceTimeState: pv.SourceTimeUnavailable, Source: provenance, Origins: map[pv.Digest]pv.Provenance{origin: provenance}, Capability: pv.Capability{ID: pv.CapabilityThreePhaseTelemetryV1, Outcome: pv.CapabilityNotSatisfied}, Facts: map[pv.FactKey]pv.Fact{key: {ID: pv.FactACActivePower, Dimensions: dimensions, Value: pv.DecimalFactValue(pv.MustDecimal("7310", 0)), Unit: pv.UnitWatt, Quality: pv.QualityGood, Availability: pv.AvailabilityAvailable, Freshness: pv.FreshnessFresh, Temporal: pv.Temporal{Receipt: 100, FreshUntil: 200, RetainUntil: 300, Policy: pv.PolicyTelemetryFastV1}, OriginRef: origin}}, RequestedOutputs: []pv.RequestedOutput{{SourceRef: origin, RequestedOutputRef: requestedRef}}, ProjectionReport: []pv.Projection{{SourceRef: origin, RequestedOutputRef: requestedRef, FactID: pv.FactACActivePower, Dimensions: &dimensions, Outcome: pv.ProjectionMapped}}}
}

func assertM2MError(t *testing.T, response *httptest.ResponseRecorder, code string) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d; want 200 body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Data   any `json:"data"`
		Errors []struct {
			Message    string   `json:"message"`
			Path       []string `json:"path"`
			Extensions struct {
				Code string `json:"code"`
			} `json:"extensions"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if envelope.Data != nil || len(envelope.Errors) != 1 || envelope.Errors[0].Message != "M2M request failed" || len(envelope.Errors[0].Path) != 1 || envelope.Errors[0].Path[0] != "m2mCurrentSnapshot" || envelope.Errors[0].Extensions.Code != code {
		t.Fatalf("error envelope=%s", response.Body.String())
	}
}
