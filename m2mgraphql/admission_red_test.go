package m2mgraphql

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

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
		AllowedAssets:         map[string]struct{}{"pv-asset-fixture": {}, "pv-asset-missing": {}},
		MonotonicMilliseconds: func() int64 { return 1_000 },
	})
	if err != nil {
		t.Fatal(err)
	}

	blocked := httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", bytes.NewBufferString(m2mRequest("PUBLIC_GRAPHQL_M2M_V1", "pv-asset-fixture")))
	blocked = blocked.WithContext(WithMTLSPrincipal(blocked.Context(), "principal-a"))
	blockedDone := make(chan struct{})
	go func() {
		defer close(blockedDone)
		handler.ServeHTTP(httptest.NewRecorder(), blocked)
	}()
	<-entered
	assertM2MErrorForRequest(t, handler, "principal-a", m2mRequest("PUBLIC_GRAPHQL_M2M_V1", "pv-asset-fixture"), "REQUEST_LIMIT_EXCEEDED")
	close(release)
	<-blockedDone

	for _, test := range []struct{ name, body, code string }{
		{"contract-before-asset", m2mRequest("OTHER", "not-allowed"), "CONTRACT_INCOMPATIBLE"},
		{"allowlist-before-existence", m2mRequest("PUBLIC_GRAPHQL_M2M_V1", "not-allowed"), "ASSET_FORBIDDEN"},
		{"unknown-allowed-asset", m2mRequest("PUBLIC_GRAPHQL_M2M_V1", "pv-asset-missing"), "ASSET_NOT_FOUND"},
		{"json-depth", strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65), "REQUEST_INVALID"},
		{"duplicate-json-key", `{"operationName":"M2MCurrentSnapshot","operationName":"M2MCurrentSnapshot"}`, "REQUEST_INVALID"},
		{"alias", `{"operationName":"M2MCurrentSnapshot","query":"query M2MCurrentSnapshot($request: M2MCurrentSnapshotRequest!) { alias: m2mCurrentSnapshot(request: $request) { assetRef } }","variables":{"request":{"contractId":"PUBLIC_GRAPHQL_M2M_V1","assetRef":"pv-asset-fixture"}}}`, "QUERY_REJECTED"},
		{"query-depth-before-shape", m2mRequestWithQuery(m2mSchemaValidDepthQuery()), "REQUEST_LIMIT_EXCEEDED"},
		{"selected-fields-before-shape", m2mRequestWithQuery("query M2MCurrentSnapshot($request: M2MCurrentSnapshotRequest!) { m2mCurrentSnapshot(request: $request) { " + strings.Repeat("assetRef ", 257) + "} }"), "REQUEST_LIMIT_EXCEEDED"},
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

func TestM2MAdmission_RejectsMissingOrEmptyMTLSPrincipalBeforeSnapshotLookup(t *testing.T) {
	lookups := 0
	handler, err := NewHandler(Config{
		SnapshotByAsset: func(context.Context, string) (pv.Snapshot, bool) { lookups++; return m2mFixtureSnapshot(), true },
		AssetExists:     func(string) bool { return true }, AllowedAssets: map[string]struct{}{"pv-asset-fixture": {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, principal := range []string{"", "\t"} {
		req := httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", strings.NewReader(m2mRequest("PUBLIC_GRAPHQL_M2M_V1", "pv-asset-fixture")))
		if principal != "" {
			req = req.WithContext(WithMTLSPrincipal(req.Context(), principal))
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		assertM2MError(t, response, "REQUEST_INVALID")
	}
	if lookups != 0 {
		t.Fatalf("unauthenticated requests invoked SnapshotByAsset %d times", lookups)
	}
}

func TestM2MAdmission_RejectsOpenJSONEnvelopesBeforeSemanticAdmission(t *testing.T) {
	lookups := 0
	handler, err := NewHandler(Config{
		SnapshotByAsset: func(context.Context, string) (pv.Snapshot, bool) { lookups++; return m2mFixtureSnapshot(), true },
		AssetExists:     func(string) bool { lookups++; return true },
		AllowedAssets:   map[string]struct{}{"pv-asset-fixture": {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	base := m2mRequest("OTHER", "pv-asset-fixture")
	for _, body := range []string{
		strings.Replace(base, `"operationName":`, `"extra":true,"operationName":`, 1),
		strings.Replace(base, `"request":{`, `"extra":true,"request":{`, 1),
		strings.Replace(base, `"assetRef":`, `"extra":true,"assetRef":`, 1),
	} {
		assertM2MErrorForRequest(t, handler, "principal-closed-envelope", body, "REQUEST_INVALID")
	}
	if lookups != 0 {
		t.Fatalf("invalid request envelopes reached semantic admission %d times", lookups)
	}
}

func TestM2MAdmission_RejectsResponsesOverOneMiBBeforeWritingPartialData(t *testing.T) {
	handler, err := NewHandler(Config{
		SnapshotByAsset: func(context.Context, string) (pv.Snapshot, bool) { return m2mOversizedSnapshot(), true },
		AssetExists:     func(string) bool { return true }, AllowedAssets: map[string]struct{}{"pv-asset-fixture": {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertM2MErrorForRequest(t, handler, "principal-response-limit", m2mRequest("PUBLIC_GRAPHQL_M2M_V1", "pv-asset-fixture"), "REQUEST_LIMIT_EXCEEDED")
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

func TestM2MAdmission_TokenBucketPreservesPartialRefillIntervals(t *testing.T) {
	clock := int64(0)
	handler, err := NewHandler(Config{SnapshotByAssetAt: func(context.Context, string) (pv.Snapshot, time.Time, bool) {
		return m2mFixtureSnapshot(), time.Unix(100, 0).UTC(), true
	}, AssetExists: func(string) bool { return true }, AllowedAssets: map[string]struct{}{"pv-asset-fixture": {}}, MonotonicMilliseconds: func() int64 { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	assertM2MSuccess(t, handler, "principal-partial-refill")
	assertM2MSuccess(t, handler, "principal-partial-refill")
	clock = 1_500
	assertM2MSuccess(t, handler, "principal-partial-refill")
	clock = 2_000
	assertM2MSuccess(t, handler, "principal-partial-refill")
}

func m2mRequest(contractID, assetRef string) string {
	request := m2mCanonicalRequest(canonicalM2MQuery, assetRef)
	if contractID == "PUBLIC_GRAPHQL_M2M_V1" {
		return request
	}
	return strings.Replace(request, `"contractId":"PUBLIC_GRAPHQL_M2M_V1"`, `"contractId":"`+contractID+`"`, 1)
}

func m2mSchemaValidDepthQuery() string {
	nested := "coefficient"
	for range 9 {
		nested = "... on M2MDecimalValue { " + nested + " }"
	}
	return "query M2MCurrentSnapshot($request: M2MCurrentSnapshotRequest!) { " +
		"m2mCurrentSnapshot(request: $request) { facts { value { " + nested + " } } } }"
}

func m2mRequestWithQuery(query string) string {
	return `{"operationName":"M2MCurrentSnapshot","query":` + strconv.Quote(query) + `,"variables":{"request":{"contractId":"PUBLIC_GRAPHQL_M2M_V1","assetRef":"pv-asset-fixture"}}}`
}

func m2mOversizedSnapshot() pv.Snapshot {
	snapshot := m2mFixtureSnapshot()
	snapshot.Origins = make(map[pv.Digest]pv.Provenance, 256)
	snapshot.Origins[snapshot.Source.SourceObservationRef] = snapshot.Source
	for index := 0; index < 255; index++ {
		ref := pv.Digest("sha256:" + fmt.Sprintf("%064x", index+1))
		provenance := snapshot.Source
		provenance.SourceObservationRef = ref
		provenance.ProfileID = strings.Repeat("p", 5_000)
		snapshot.Origins[ref] = provenance
	}
	return snapshot
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
