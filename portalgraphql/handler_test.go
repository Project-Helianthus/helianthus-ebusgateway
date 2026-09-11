package portalgraphql

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
)

func TestPortalCatalogV1IsDedicatedAndBounded(t *testing.T) {
	h := Handler{Catalog: catalogv1.New(contributionv1.NewStaticIndex(), nil, nil, nil)}
	req := httptest.NewRequest(http.MethodPost, Path, bytes.NewBufferString(`{"operationName":"PortalCatalogV1","variables":{}}`))
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK || !bytes.Contains(res.Body.Bytes(), []byte(`"portalCatalogV1"`)) || !bytes.Contains(res.Body.Bytes(), []byte(`"domains"`)) {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	bad := httptest.NewRequest(http.MethodPost, Path, bytes.NewBufferString(`{"operationName":"IntrospectionQuery"}`))
	badRes := httptest.NewRecorder()
	h.ServeHTTP(badRes, bad)
	if badRes.Code != http.StatusBadRequest {
		t.Fatalf("unexpected operation status=%d", badRes.Code)
	}
}
