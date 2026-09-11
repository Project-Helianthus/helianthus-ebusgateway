package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
)

func TestPortalCatalogDescriptorsUseAcceptedSemRegMetadata(t *testing.T) {
	index, err := newGatewayPortalSemanticIndex()
	if err != nil {
		t.Fatal(err)
	}
	manifests := acceptedPortalCatalogDescriptors()
	if len(manifests) != 3 {
		t.Fatalf("descriptor count = %d; want PV, Storage and EVSE", len(manifests))
	}
	want := map[string]struct{}{"pv.ac.frequency": {}, "storage.state.soc": {}, "evse.limit.configured_current": {}}
	for _, manifest := range manifests {
		if _, err := contributionv1.CanonicalDigest(manifest, index); err != nil {
			t.Fatalf("descriptor %s does not validate against SemReg metadata: %v", manifest.ManifestID, err)
		}
		for _, field := range manifest.Fields {
			delete(want, field.Ref.ID)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing production descriptor fields: %v", want)
	}
}

func TestPortalCatalogRuntimeHandlerInstallsProductionDescriptors(t *testing.T) {
	h := newPortalCatalogV1Handler(newGatewayPortalCatalogSource(nil, nil, nil))
	req := httptest.NewRequest(http.MethodPost, "/graphql/portal/v1", bytes.NewBufferString(`{"operationName":"PortalCatalogV1","variables":{}}`))
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("handler status=%d body=%s", res.Code, res.Body.String())
	}
	for _, want := range []string{`"portal.pv"`, `"portal.storage"`, `"portal.evse"`} {
		if !bytes.Contains(res.Body.Bytes(), []byte(want)) {
			t.Fatalf("catalog omitted %s: %s", want, res.Body.String())
		}
	}
}
