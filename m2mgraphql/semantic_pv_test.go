package m2mgraphql

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestSemanticPVCurrentUsesOneEvaluatedProjection(t *testing.T) {
	handler, err := NewHandler(Config{AllowedAssets: map[string]struct{}{"pv-asset-test": {}}, SemanticPVCurrent: func(context.Context, string) (json.RawMessage, bool) {
		return json.RawMessage(`{"snapshot":{"snapshot_id":"snapshot:test"},"evaluation":{"evaluation_digest":"sha256:test"},"projection":{"manifest":{"target_id":"target:gateway-semantic-pv"}}}`), true
	}})
	if err != nil {
		t.Fatal(err)
	}
	request := `{"operationName":"SemanticPVCurrent","query":` + strconv.Quote(semanticPVFixedQuery) + `,"variables":{"request":{"contractId":"PUBLIC_GRAPHQL_SEMANTIC_PV_V1","assetRef":"pv-asset-test"}}}`
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", strings.NewReader(request)).WithContext(WithMTLSPrincipal(context.Background(), "test")))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"semanticPVCurrent"`) || !strings.Contains(recorder.Body.String(), `"snapshot:test"`) {
		t.Fatalf("semantic response=%d %s", recorder.Code, recorder.Body.String())
	}
}
