package portal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
)

type portalRawReadProvider struct {
	requests int
	rawErr   error
	result   mcp.ModbusRawReadResult
}

func (provider *portalRawReadProvider) RawRead(_ context.Context, request mcp.ModbusRawReadRequest) (mcp.ModbusRawReadResult, error) {
	provider.requests++
	if provider.rawErr != nil {
		return mcp.ModbusRawReadResult{}, provider.rawErr
	}
	if provider.requests > mcp.ModbusV1MaxRawReadsPerWindow {
		return mcp.ModbusRawReadResult{}, mcp.ErrModbusV1ResourceExhausted
	}
	if provider.result.EndpointRef != "" {
		return provider.result, nil
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
	rawRequest := httptest.NewRequest(http.MethodPost, "/api/v1/explorer/modbus/raw-read", strings.NewReader(`{"unit_id":1,"function":3,"offset":0,"quantity":1}`))
	rawRequest.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rawResponse, rawRequest)
	if rawResponse.Code != http.StatusOK || !strings.Contains(rawResponse.Body.String(), `"name":"helianthus-modbus-mcp"`) || provider.requests != 1 {
		t.Fatalf("raw response=%d %s requests=%d", rawResponse.Code, rawResponse.Body.String(), provider.requests)
	}
	if len(audit) != 1 || audit[0] != "admitted" {
		t.Fatalf("raw audit=%v", audit)
	}
}

func TestPortalRawModbusAdmissionRejectsNonClosedJSON(t *testing.T) {
	for _, test := range []struct {
		name, contentType, body string
		wantStatus              int
	}{
		{name: "wrong_content_type", contentType: "text/plain", body: `{"unit_id":1,"function":3,"offset":0,"quantity":1}`, wantStatus: http.StatusUnsupportedMediaType},
		{name: "duplicate_key", contentType: "application/json", body: `{"unit_id":1,"unit_id":2,"function":3,"offset":0,"quantity":1}`, wantStatus: http.StatusBadRequest},
		{name: "null", contentType: "application/json", body: `null`, wantStatus: http.StatusBadRequest},
		{name: "array", contentType: "application/json", body: `[]`, wantStatus: http.StatusBadRequest},
		{name: "trailing", contentType: "application/json", body: `{"unit_id":1,"function":3,"offset":0,"quantity":1} {}`, wantStatus: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &portalRawReadProvider{}
			events := make([]RawModbusAuditEvent, 0, 1)
			handler := NewHandler(Options{RawModbusEnabled: true, ModbusProvider: provider, RawModbusAudit: func(event RawModbusAuditEvent) { events = append(events, event) }})
			request := httptest.NewRequest(http.MethodPost, "/api/v1/explorer/modbus/raw-read", strings.NewReader(test.body))
			request.Header.Set("Content-Type", test.contentType)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus || provider.requests != 0 || len(events) != 1 {
				t.Fatalf("status=%d provider_requests=%d audit_events=%d", response.Code, provider.requests, len(events))
			}
		})
	}
}

func TestPortalRawModbusAuditIsCompleteAndSanitizedForEveryOutcome(t *testing.T) {
	for _, test := range []struct {
		name, body, outcome, errorCode string
		provider                       *portalRawReadProvider
	}{
		{name: "success", body: `{"unit_id":7,"function":4,"offset":12,"quantity":2}`, outcome: "admitted", provider: &portalRawReadProvider{result: mcp.ModbusRawReadResult{EndpointRef: "sha256:" + strings.Repeat("a", 64)}}},
		{name: "rejected", body: `{"unit_id":7,"function":6,"offset":12,"quantity":2}`, outcome: "rejected", errorCode: "INVALID_ARGUMENT", provider: &portalRawReadProvider{}},
		{name: "exhausted", body: `{"unit_id":7,"function":4,"offset":12,"quantity":2}`, outcome: "exhausted", errorCode: "RESOURCE_EXHAUSTED", provider: &portalRawReadProvider{rawErr: mcp.ErrModbusV1ResourceExhausted}},
		{name: "failed", body: `{"unit_id":7,"function":4,"offset":12,"quantity":2}`, outcome: "failed", errorCode: "PROVIDER_FAILURE", provider: &portalRawReadProvider{rawErr: errors.New("tcp://operator:secret@example.test:502 private/path client.pem 1234")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var events []RawModbusAuditEvent
			handler := NewHandler(Options{RawModbusEnabled: true, ModbusProvider: test.provider, RawModbusAudit: func(event RawModbusAuditEvent) { events = append(events, event) }})
			request := httptest.NewRequest(http.MethodPost, "/api/v1/explorer/modbus/raw-read", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if len(events) != 1 {
				t.Fatalf("audit events=%d", len(events))
			}
			encoded, err := json.Marshal(events[0])
			if err != nil {
				t.Fatal(err)
			}
			var payload map[string]any
			if err := json.Unmarshal(encoded, &payload); err != nil {
				t.Fatal(err)
			}
			for key, want := range map[string]any{"surface": "portal", "tool": mcp.ModbusV1RawReadTool, "outcome": test.outcome, "error_code": test.errorCode} {
				if payload[key] != want {
					t.Fatalf("audit %s=%v; want %v; payload=%s", key, payload[key], want, encoded)
				}
			}
			if requestID, _ := payload["request_id"].(string); requestID == "" || len(requestID) > 64 {
				t.Fatalf("request_id is absent or unbounded: %q", requestID)
			}
			auditedFields := make(map[string]any, len(payload)-2)
			for key, value := range payload {
				if key != "at" && key != "request_id" {
					auditedFields[key] = value
				}
			}
			encodedFields, err := json.Marshal(auditedFields)
			if err != nil {
				t.Fatal(err)
			}
			for _, forbidden := range []string{"words", "wire", "tcp://", "secret", "private/path", "client.pem", "1234"} {
				if strings.Contains(strings.ToLower(string(encodedFields)), strings.ToLower(forbidden)) {
					t.Fatalf("audit fields leaked %q: %s", forbidden, encodedFields)
				}
			}
			if test.name == "success" && fmt.Sprint(payload["endpoint_ref"]) != "sha256:"+strings.Repeat("a", 64) {
				t.Fatalf("safe endpoint_ref missing: %s", encoded)
			}
		})
	}
}

func TestSafeEndpointRefRequiresExactLowercaseSHA256Contract(t *testing.T) {
	valid := "sha256:" + strings.Repeat("a", 64)
	if !safeEndpointRef(valid) {
		t.Fatalf("valid endpoint ref rejected: %q", valid)
	}
	for _, value := range []string{
		"sha256:safe",
		"sha256:" + strings.Repeat("A", 64),
		"sha256:" + strings.Repeat("a", 63),
		"sha256:" + strings.Repeat("a", 65),
		"sha256:" + strings.Repeat("a", 63) + "/",
		"sha256:" + strings.Repeat("a", 64) + "@evil",
	} {
		if safeEndpointRef(value) {
			t.Fatalf("unsafe endpoint ref accepted: %q", value)
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
