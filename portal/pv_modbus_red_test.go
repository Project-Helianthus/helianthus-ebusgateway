package portal

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
)

type portalRawReadProvider struct {
	requests int
}

func (provider *portalRawReadProvider) RawRead(_ context.Context, request mcp.ModbusRawReadRequest) (mcp.ModbusRawReadResult, error) {
	provider.requests++
	if provider.requests > mcp.ModbusV1MaxRawReadsPerWindow {
		return mcp.ModbusRawReadResult{}, mcp.ErrModbusV1ResourceExhausted
	}
	return mcp.ModbusRawReadResult{UnitID: request.UnitID, Function: request.Function, Offset: request.Offset, Quantity: request.Quantity, Words: []uint16{42}}, nil
}

func (*portalRawReadProvider) ProfileObservation(context.Context, string, string) (mcp.ModbusProfileObservationResult, error) {
	return mcp.ModbusProfileObservationResult{}, errors.New("not used")
}
func (*portalRawReadProvider) CanonicalPV(context.Context, string, string) (mcp.ModbusCanonicalPVResult, error) {
	return mcp.ModbusCanonicalPVResult{Snapshot: pv.Snapshot{}}, errors.New("not used")
}

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

func TestPortalPVForwardsClosedM2MEnvelopeAndRawReadUsesMCPEnvelope(t *testing.T) {
	provider := &portalRawReadProvider{}
	audit := make([]string, 0, 2)
	handler := NewHandler(Options{
		SemanticPVEnabled: true,
		SemanticPV: func(context.Context) (ForwardedResponse, error) {
			return ForwardedResponse{Status: http.StatusOK, ContentType: "application/json", Body: []byte(`{"data":{"m2mCurrentSnapshot":{"contractId":"PUBLIC_GRAPHQL_M2M_V1"}}}`)}, nil
		},
		RawModbusEnabled: true,
		ModbusProvider:   provider,
		RawModbusAudit: func(event RawModbusAuditEvent) {
			audit = append(audit, event.Outcome)
		},
	})

	pvResponse := httptest.NewRecorder()
	handler.ServeHTTP(pvResponse, httptest.NewRequest(http.MethodGet, "/api/v1/semantic/pv/current", nil))
	if pvResponse.Code != http.StatusOK || pvResponse.Body.String() != `{"data":{"m2mCurrentSnapshot":{"contractId":"PUBLIC_GRAPHQL_M2M_V1"}}}` {
		t.Fatalf("semantic PV response=%d %s", pvResponse.Code, pvResponse.Body.String())
	}

	rawResponse := httptest.NewRecorder()
	handler.ServeHTTP(rawResponse, httptest.NewRequest(http.MethodPost, "/api/v1/explorer/modbus/raw-read", strings.NewReader(`{"unit_id":1,"function":3,"offset":0,"quantity":1}`)))
	if rawResponse.Code != http.StatusOK || !strings.Contains(rawResponse.Body.String(), `"name":"helianthus-modbus-mcp"`) || provider.requests != 1 {
		t.Fatalf("raw response=%d %s requests=%d", rawResponse.Code, rawResponse.Body.String(), provider.requests)
	}
	if len(audit) != 1 || audit[0] != "admitted" {
		t.Fatalf("raw audit=%v", audit)
	}
}

func TestPortalBootstrapPublishesIndependentPVAndRawCapabilities(t *testing.T) {
	recorder := httptest.NewRecorder()
	NewHandler(Options{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/bootstrap", nil))
	if !strings.Contains(recorder.Body.String(), `"semantic_pv":false`) || !strings.Contains(recorder.Body.String(), `"modbus_raw_read":false`) {
		t.Fatalf("bootstrap lacks independent disabled capabilities: %s", recorder.Body.String())
	}
}
