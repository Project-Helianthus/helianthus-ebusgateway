package m2mgraphql

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestSemanticPVCurrentUsesOneEvaluatedProjection(t *testing.T) {
	handler, err := NewHandler(Config{AllowedAssets: map[string]struct{}{"pv-asset-test": {}}, SemanticPVCurrent: func(context.Context, string) (json.RawMessage, bool) {
		return json.RawMessage(`{"snapshot":{"snapshot_id":"snapshot:test","facts":[{"key":{"fact_id":"pv.ac.aggregate_active_power"},"candidates":[{"candidate_id":"candidate:power","value":{"quantity":{"number":{"coefficient":"43215","exponent10":-1},"unit":"unit.watt"}}}]},{"key":{"fact_id":"pv.ac.frequency"},"candidates":[{"candidate_id":"candidate:frequency-retained"}]}]},"evaluation":{"facts":[{"candidate_id":"candidate:power","freshness":"fresh","effective_availability":"available"},{"candidate_id":"candidate:frequency-retained","freshness":"stale","effective_availability":"degraded"}]},"selections":[{"key":{"fact_id":"pv.ac.aggregate_active_power"},"selected_candidate_id":"candidate:power"}],"projection":{"manifest":{"target_id":"target:gateway-semantic-pv"},"dispositions":[{"item_id":"projection.gateway.pv.inverter.ac.power.active","outcome":"exact"},{"item_id":"projection.gateway.pv.inverter.ac.frequency","outcome":"withheld","reason":"mapping.field_invalid"}]}}`), true
	}})
	if err != nil {
		t.Fatal(err)
	}
	request := `{"operationName":"SemanticPVCurrent","query":` + strconv.Quote(semanticPVFixedQuery) + `,"variables":{"request":{"contractId":"PUBLIC_GRAPHQL_SEMANTIC_PV_V1","assetRef":"pv-asset-test"}}}`
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", strings.NewReader(request)).WithContext(WithMTLSPrincipal(context.Background(), "test")))
	if recorder.Code != http.StatusOK {
		t.Fatalf("semantic response=%d %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data map[string]map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	current := response.Data["semanticPVCurrent"]
	if len(current) != 4 || current["snapshot"] == nil || current["evaluation"] == nil || current["selections"] == nil || current["projection"] == nil || strings.Contains(recorder.Body.String(), `"snapshotId"`) {
		t.Fatalf("fixed GraphQL response shape=%s", recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"coefficient":"43215"`) || !strings.Contains(recorder.Body.String(), `"reason":"mapping.field_invalid"`) || strings.Contains(recorder.Body.String(), `"selected_candidate_id":"candidate:frequency-retained"`) {
		t.Fatalf("fixed GraphQL response lost PUBLIC-05 field isolation=%s", recorder.Body.String())
	}
	want, err := os.ReadFile("testdata/semantic_pv_current.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(recorder.Body.String()); got != strings.TrimSpace(string(want)) {
		t.Fatalf("semantic PV golden mismatch\nwant: %s\ngot:  %s", strings.TrimSpace(string(want)), got)
	}
}

func TestSemanticPVCurrentRejectsUndeclaredProviderFields(t *testing.T) {
	handler, err := NewHandler(Config{AllowedAssets: map[string]struct{}{"pv-asset-test": {}}, SemanticPVCurrent: func(context.Context, string) (json.RawMessage, bool) {
		return json.RawMessage(`{"snapshot":{},"evaluation":{},"selections":[],"projection":{},"legacy":true}`), true
	}})
	if err != nil {
		t.Fatal(err)
	}
	request := `{"operationName":"SemanticPVCurrent","query":` + strconv.Quote(semanticPVFixedQuery) + `,"variables":{"request":{"contractId":"PUBLIC_GRAPHQL_SEMANTIC_PV_V1","assetRef":"pv-asset-test"}}}`
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", strings.NewReader(request)).WithContext(WithMTLSPrincipal(context.Background(), "test")))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"SOURCE_UNAVAILABLE"`) {
		t.Fatalf("undeclared provider field response=%d %s", recorder.Code, recorder.Body.String())
	}
}

func TestSemanticStorageCurrentUsesOneEvaluatedProjection(t *testing.T) {
	handler, err := NewHandler(Config{AllowedAssets: map[string]struct{}{"asset:storage-test": {}}, SemanticStorageCurrent: func(context.Context, string) (json.RawMessage, bool) {
		return json.RawMessage(`{"snapshot":{"snapshot_id":"snapshot:storage"},"evaluation":{"evaluation_digest":"sha256:test"},"selections":[],"projection":{"manifest":{"target_id":"target:gateway-semantic-storage"}}}`), true
	}})
	if err != nil {
		t.Fatal(err)
	}
	request := `{"operationName":"SemanticStorageCurrent","query":` + strconv.Quote(semanticStorageFixedQuery) + `,"variables":{"request":{"contractId":"PUBLIC_GRAPHQL_SEMANTIC_STORAGE_V1","assetRef":"asset:storage-test"}}}`
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, route, strings.NewReader(request)).WithContext(WithMTLSPrincipal(context.Background(), "test-principal")))
	if response.Code != http.StatusOK {
		t.Fatalf("storage status=%d body=%s", response.Code, response.Body.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["data"].(map[string]any)["semanticStorageCurrent"]; !ok {
		t.Fatalf("storage GraphQL response=%s", response.Body.String())
	}
}

func TestSemanticEVSECurrentUsesOneEvaluatedProjection(t *testing.T) {
	called := 0
	handler, err := NewHandler(Config{AllowedAssets: map[string]struct{}{"asset:evse-test": {}}, SemanticEVSECurrent: func(context.Context, string) (json.RawMessage, bool) {
		called++
		return json.RawMessage(`{"snapshot":{"snapshot_id":"snapshot:evse"},"evaluation":{"evaluation_digest":"sha256:test"},"selections":[],"projection":{"manifest":{"target_id":"target:gateway-semantic-evse"}}}`), true
	}})
	if err != nil {
		t.Fatal(err)
	}
	request := `{"operationName":"SemanticEVSECurrent","query":` + strconv.Quote(semanticEVSEFixedQuery) + `,"variables":{"request":{"contractId":"PUBLIC_GRAPHQL_SEMANTIC_EVSE_V1","assetRef":"asset:evse-test"}}}`
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, route, strings.NewReader(request)).WithContext(WithMTLSPrincipal(context.Background(), "test-principal")))
	if response.Code != http.StatusOK || called != 1 {
		t.Fatalf("status=%d calls=%d response=%s", response.Code, called, response.Body.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["data"].(map[string]any)["semanticEVSECurrent"]; !ok {
		t.Fatalf("EVSE GraphQL response=%s", response.Body.String())
	}
}

func TestSemanticStorageErrorsUseStoragePathAndPreoperationErrorsAreNeutral(t *testing.T) {
	handler, err := NewHandler(Config{AllowedAssets: map[string]struct{}{"asset:storage-test": {}}, SemanticStorageCurrent: func(context.Context, string) (json.RawMessage, bool) { return nil, false }})
	if err != nil {
		t.Fatal(err)
	}
	storageRequest := func(contract, asset string) string {
		return `{"operationName":"SemanticStorageCurrent","query":` + strconv.Quote(semanticStorageFixedQuery) + `,"variables":{"request":{"contractId":` + strconv.Quote(contract) + `,"assetRef":` + strconv.Quote(asset) + `}}}`
	}
	assertM2MError(t, handler, storageRequest("wrong", "asset:storage-test"), "CONTRACT_INCOMPATIBLE", []string{"semanticStorageCurrent"})
	assertM2MError(t, handler, storageRequest(semanticStorageContractID, "asset:other"), "ASSET_FORBIDDEN", []string{"semanticStorageCurrent"})
	assertM2MError(t, handler, storageRequest(semanticStorageContractID, "asset:storage-test"), "SOURCE_UNAVAILABLE", []string{"semanticStorageCurrent"})
	assertM2MError(t, handler, `{`, "REQUEST_INVALID", []string{})

	pv, err := NewHandler(Config{AllowedAssets: map[string]struct{}{"pv-asset-test": {}}, SemanticPVCurrent: func(context.Context, string) (json.RawMessage, bool) { return nil, false }})
	if err != nil {
		t.Fatal(err)
	}
	pvRequest := `{"operationName":"SemanticPVCurrent","query":` + strconv.Quote(semanticPVFixedQuery) + `,"variables":{"request":{"contractId":"wrong","assetRef":"pv-asset-test"}}}`
	assertM2MError(t, pv, pvRequest, "CONTRACT_INCOMPATIBLE", []string{"semanticPVCurrent"})

	quota, err := NewHandler(Config{AllowedAssets: map[string]struct{}{"asset:storage-test": {}}, MonotonicMilliseconds: func() int64 { return 0 }, SemanticStorageCurrent: func(context.Context, string) (json.RawMessage, bool) {
		return json.RawMessage(`{"snapshot":{},"evaluation":{},"selections":[],"projection":{}}`), true
	}})
	if err != nil {
		t.Fatal(err)
	}
	assertM2MStorageSuccess(t, quota, storageRequest(semanticStorageContractID, "asset:storage-test"))
	assertM2MStorageSuccess(t, quota, storageRequest(semanticStorageContractID, "asset:storage-test"))
	assertM2MError(t, quota, storageRequest(semanticStorageContractID, "asset:storage-test"), "REQUEST_LIMIT_EXCEEDED", []string{"semanticStorageCurrent"})
}

func TestM2MHandlerRequiresNoPrincipal(t *testing.T) {
	handler, err := NewHandler(Config{AllowedAssets: map[string]struct{}{"asset:storage-test": {}}, MonotonicMilliseconds: func() int64 { return 0 }, SemanticStorageCurrent: func(context.Context, string) (json.RawMessage, bool) {
		return json.RawMessage(`{"snapshot":{},"evaluation":{},"selections":[],"projection":{}}`), true
	}})
	if err != nil {
		t.Fatal(err)
	}
	request := `{"operationName":"SemanticStorageCurrent","query":` + strconv.Quote(semanticStorageFixedQuery) + `,"variables":{"request":{"contractId":"PUBLIC_GRAPHQL_SEMANTIC_STORAGE_V1","assetRef":"asset:storage-test"}}}`
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, route, strings.NewReader(request)))
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"errors"`) {
		t.Fatalf("request without principal response=%d %s", response.Code, response.Body.String())
	}
	forbidden := `{"operationName":"SemanticStorageCurrent","query":` + strconv.Quote(semanticStorageFixedQuery) + `,"variables":{"request":{"contractId":"PUBLIC_GRAPHQL_SEMANTIC_STORAGE_V1","assetRef":"asset:storage-other"}}}`
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, route, strings.NewReader(forbidden)))
	if !strings.Contains(response.Body.String(), `"ASSET_FORBIDDEN"`) {
		t.Fatalf("asset allow-list not applied without principal: %s", response.Body.String())
	}
}

func TestM2MHandlerRateLimitIsKeyedByRemoteHost(t *testing.T) {
	handler, err := NewHandler(Config{AllowedAssets: map[string]struct{}{"asset:storage-test": {}}, MonotonicMilliseconds: func() int64 { return 0 }, SemanticStorageCurrent: func(context.Context, string) (json.RawMessage, bool) {
		return json.RawMessage(`{"snapshot":{},"evaluation":{},"selections":[],"projection":{}}`), true
	}})
	if err != nil {
		t.Fatal(err)
	}
	request := `{"operationName":"SemanticStorageCurrent","query":` + strconv.Quote(semanticStorageFixedQuery) + `,"variables":{"request":{"contractId":"PUBLIC_GRAPHQL_SEMANTIC_STORAGE_V1","assetRef":"asset:storage-test"}}}`
	serve := func(remote string) string {
		r := httptest.NewRequest(http.MethodPost, route, strings.NewReader(request))
		r.RemoteAddr = remote
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, r)
		if strings.Contains(response.Body.String(), `"REQUEST_LIMIT_EXCEEDED"`) {
			return "limited"
		}
		if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"errors"`) {
			t.Fatalf("remote %s response=%d %s", remote, response.Code, response.Body.String())
		}
		return "ok"
	}
	// Host 192.0.2.10 spends its two tokens on different source ports; the
	// port is not part of the key, so a third connection is limited.
	got := []string{serve("192.0.2.10:40001"), serve("192.0.2.10:40002"), serve("192.0.2.10:40003")}
	// Exhaust an IPv6 host across ports too, then verify that another IPv6
	// host has its own budget. Collapsing IPv6 hosts into one key must fail.
	got = append(got, serve("192.0.2.11:40001"),
		serve("[2001:db8::1]:40001"), serve("[2001:db8::1]:40002"), serve("[2001:db8::1]:40003"),
		serve("[2001:db8::2]:40001"))
	want := []string{"ok", "ok", "limited", "ok", "ok", "ok", "limited", "ok"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("per-remote-host admission=%v want=%v", got, want)
	}
}

func assertM2MStorageSuccess(t *testing.T, handler http.Handler, request string) {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, route, strings.NewReader(request)).WithContext(WithMTLSPrincipal(context.Background(), "test-principal")))
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"errors"`) {
		t.Fatalf("storage response=%d %s", response.Code, response.Body.String())
	}
}

func assertM2MError(t *testing.T, handler http.Handler, request, wantCode string, wantPath []string) {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, route, strings.NewReader(request)).WithContext(WithMTLSPrincipal(context.Background(), "test-principal")))
	var decoded struct {
		Errors []struct {
			Path       []string `json:"path"`
			Extensions struct {
				Code string `json:"code"`
			} `json:"extensions"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Errors) != 1 || decoded.Errors[0].Extensions.Code != wantCode || !reflect.DeepEqual(decoded.Errors[0].Path, wantPath) {
		t.Fatalf("error=%#v want=%s/%#v response=%s", decoded.Errors, wantCode, wantPath, response.Body.String())
	}
}
