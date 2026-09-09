package m2mgraphql

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestSemanticPVCurrentUsesOneEvaluatedProjection(t *testing.T) {
	handler, err := NewHandler(Config{AllowedAssets: map[string]struct{}{"pv-asset-test": {}}, SemanticPVCurrent: func(context.Context, string) (json.RawMessage, bool) {
		return json.RawMessage(`{"snapshot":{"snapshot_id":"snapshot:test"},"evaluation":{"evaluation_digest":"sha256:test"},"selections":[],"projection":{"manifest":{"target_id":"target:gateway-semantic-pv"}}}`), true
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
