package portal

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPortalPVAndRawModbusRoutesAreClosedAndDisabledByDefault(t *testing.T) {
	handler := NewHandler(Options{})
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/api/v1/semantic/pv/current", nil),
		httptest.NewRequest(http.MethodPost, "/api/v1/explorer/modbus/raw-read", strings.NewReader(`{"unit_id":1,"function":3,"offset":0,"quantity":1}`)),
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("%s %s status=%d; want disabled 404", request.Method, request.URL.Path, recorder.Code)
		}
	}
}

func TestPortalBootstrapPublishesIndependentPVAndRawCapabilities(t *testing.T) {
	recorder := httptest.NewRecorder()
	NewHandler(Options{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/bootstrap", nil))
	if !strings.Contains(recorder.Body.String(), `"semantic_pv":false`) || !strings.Contains(recorder.Body.String(), `"modbus_raw_read":false`) {
		t.Fatalf("bootstrap lacks independent disabled capabilities: %s", recorder.Body.String())
	}
}
