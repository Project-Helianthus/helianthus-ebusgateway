package m2mgraphql

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
)

func TestM2MAdmission_EnforcesPrecedenceAndWireBounds(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	handler, err := NewHandler(Config{
		SnapshotByAsset: func(context.Context, string) (pv.Snapshot, bool) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
			return m2mFixtureSnapshot(), true
		},
		AssetExists:           func(assetRef string) bool { return assetRef != "pv-asset-missing" },
		AllowedAssets:         map[string]struct{}{"pv-asset-fixture": {}},
		MonotonicMilliseconds: func() int64 { return 1_000 },
	})
	if err != nil {
		t.Fatal(err)
	}

	blocked := httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", bytes.NewBufferString(m2mRequest("PUBLIC_GRAPHQL_M2M_V1", "pv-asset-fixture")))
	blocked = blocked.WithContext(WithMTLSPrincipal(blocked.Context(), "principal-a"))
	go handler.ServeHTTP(httptest.NewRecorder(), blocked)
	<-entered
	for _, test := range []struct{ name, body, code string }{
		{"same-principal-overlap", m2mRequest("PUBLIC_GRAPHQL_M2M_V1", "pv-asset-fixture"), "REQUEST_LIMIT_EXCEEDED"},
		{"contract-before-asset", m2mRequest("OTHER", "not-allowed"), "CONTRACT_INCOMPATIBLE"},
		{"allowlist-before-existence", m2mRequest("PUBLIC_GRAPHQL_M2M_V1", "not-allowed"), "ASSET_FORBIDDEN"},
		{"unknown-allowed-asset", m2mRequest("PUBLIC_GRAPHQL_M2M_V1", "pv-asset-missing"), "ASSET_NOT_FOUND"},
		{"json-depth", strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65), "REQUEST_INVALID"},
		{"duplicate-json-key", `{"operationName":"M2MCurrentSnapshot","operationName":"M2MCurrentSnapshot"}`, "REQUEST_INVALID"},
		{"alias", `{"operationName":"M2MCurrentSnapshot","query":"query M2MCurrentSnapshot($request: M2MCurrentSnapshotRequest!) { alias: m2mCurrentSnapshot(request: $request) { assetRef } }","variables":{"request":{"contractId":"PUBLIC_GRAPHQL_M2M_V1","assetRef":"pv-asset-fixture"}}}`, "QUERY_REJECTED"},
		{"raw-body-limit", strings.Repeat("x", 16*1024+1), "REQUEST_LIMIT_EXCEEDED"},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", strings.NewReader(test.body))
			req = req.WithContext(WithMTLSPrincipal(req.Context(), "principal-a"))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			assertM2MError(t, response, test.code)
		})
	}
	close(release)
}

func TestM2MAdmission_ReturnsSourceUnavailableOnlyAfterAssetExistence(t *testing.T) {
	handler, err := NewHandler(Config{
		SnapshotByAsset: func(context.Context, string) (pv.Snapshot, bool) { return pv.Snapshot{}, false },
		AssetExists:     func(string) bool { return true },
		AllowedAssets:   map[string]struct{}{"pv-asset-fixture": {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertM2MErrorForRequest(t, handler, "principal-a", m2mRequest("PUBLIC_GRAPHQL_M2M_V1", "pv-asset-fixture"), "SOURCE_UNAVAILABLE")
}

func TestM2MAdmission_IsolatedByMTLSPrincipalAndDoesNotConsumeRejectedRateTokens(t *testing.T) {
	clock := int64(10_000)
	handler, err := NewHandler(Config{SnapshotByAsset: func(context.Context, string) (pv.Snapshot, bool) { return m2mFixtureSnapshot(), true }, AssetExists: func(string) bool { return true }, AllowedAssets: map[string]struct{}{"pv-asset-fixture": {}}, MonotonicMilliseconds: func() int64 { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	for _, principal := range []string{"principal-a", "principal-b"} {
		for range 2 {
			assertM2MSuccess(t, handler, principal)
		}
	}
	assertM2MErrorForRequest(t, handler, "principal-a", m2mRequest("PUBLIC_GRAPHQL_M2M_V1", "pv-asset-fixture"), "REQUEST_LIMIT_EXCEEDED")
	assertM2MErrorForRequest(t, handler, "principal-a", m2mRequest("OTHER", "pv-asset-fixture"), "CONTRACT_INCOMPATIBLE")
	clock += 1_000
	assertM2MSuccess(t, handler, "principal-a")
}

func m2mRequest(contractID, assetRef string) string {
	return `{"operationName":"M2MCurrentSnapshot","query":"query M2MCurrentSnapshot($request: M2MCurrentSnapshotRequest!) { m2mCurrentSnapshot(request: $request) { assetRef } }","variables":{"request":{"contractId":"` + contractID + `","assetRef":"` + assetRef + `"}}}`
}

func assertM2MSuccess(t *testing.T, handler http.Handler, principal string) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", strings.NewReader(m2mRequest("PUBLIC_GRAPHQL_M2M_V1", "pv-asset-fixture")))
	request = request.WithContext(WithMTLSPrincipal(request.Context(), principal))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"errors"`) {
		t.Fatalf("principal=%s body=%s", principal, response.Body.String())
	}
}

func assertM2MErrorForRequest(t *testing.T, handler http.Handler, principal, body, code string) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", strings.NewReader(body))
	request = request.WithContext(WithMTLSPrincipal(request.Context(), principal))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertM2MError(t, response, code)
}
