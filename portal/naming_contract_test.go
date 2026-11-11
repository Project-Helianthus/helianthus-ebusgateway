package portal

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestNamingContractEmbeddedAssets(t *testing.T) {
	h := NewHandler(Options{})
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/assets/app.js", nil))
	if r.Code != 200 {
		t.Fatal(r.Code)
	}
	body := r.Body.String()
	for _, old := range []string{"?.semanticSnapshot", "snapshot_operation !== \"semanticSnapshot\"", "semanticSnapshot(assetIds:"} {
		if strings.Contains(body, old) {
			t.Errorf("old access %s", old)
		}
	}
	if !strings.Contains(body, "semantic_snapshot(asset_ids:") {
		t.Fatal("new query missing")
	}
	src, err := os.ReadFile("web/src/app.js")
	if err != nil {
		t.Fatal(err)
	}
	built, err := os.ReadFile("static/assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(built, append([]byte("// Generated from portal/web/src/app.js. DO NOT EDIT.\n"), src...)) {
		t.Fatal("source/build parity")
	}
}
