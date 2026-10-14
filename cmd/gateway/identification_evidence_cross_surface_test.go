package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

type normalisedEvidence struct {
	Class, Qualification, Notice, SessionID string
	Decoded                                 bool
	Count                                   int
	Items                                   []string
}

func identificationEvidenceSharedStore(t *testing.T) {
	t.Helper()
	clock := time.Date(2026, 10, 14, 8, 0, 0, 0, time.UTC)
	store := ebusgateway.NewIdentificationEvidenceStore(func() time.Time { clock = clock.Add(time.Second); return clock })
	for _, target := range []byte{0x15, 0x08, 0x26, 0x08} {
		if err := store.OnBusEvent(protocol.BusEvent{
			Kind: protocol.BusEventAttemptComplete, Outcome: protocol.BusOutcomeSuccess, HasRequest: true, HasResponse: true,
			Request:  protocol.Frame{Source: 0x71, Target: target, Primary: 0x07, Secondary: 0x04},
			Response: protocol.Frame{Data: []byte{0xB5, target, 0x00, 0x01}},
		}); err != nil {
			t.Fatalf("OnBusEvent: %v", err)
		}
	}
	publishIdentificationEvidenceStore(store)
	t.Cleanup(func() { publishIdentificationEvidenceStore(nil) })
}

func mcpIdentificationEvidence(t *testing.T, limitArg string) (*normalisedEvidence, bool) {
	t.Helper()
	server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.SetBusObservabilityProvider(newMCPBusObservabilityProvider(nil))
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ebus.v1.bus.identification_evidence.list","arguments":` + limitArg + `}}`
	response := mcpRPC(t, server.Handler(), body)
	content, _ := response.Result["content"].([]any)
	if response.Error != nil || len(content) != 1 {
		return nil, true
	}
	var envelope map[string]any
	_ = json.Unmarshal([]byte(content[0].(map[string]any)["text"].(string)), &envelope)
	if envelope["error"] != nil {
		return nil, true
	}
	data := envelope["data"].(map[string]any)
	out := &normalisedEvidence{
		Class: data["evidence_class"].(string), Qualification: data["qualification"].(string),
		Notice: data["notice"].(string), SessionID: data["session_id"].(string),
		Decoded: data["decoded"].(bool), Count: int(data["count"].(float64)),
	}
	for _, raw := range data["items"].([]any) {
		item := raw.(map[string]any)
		out.Items = append(out.Items, strings.Join([]string{
			formatNum(item["address"]), formatNum(item["sequence"]), item["session_id"].(string),
			item["captured_at"].(string), item["request_hex"].(string), item["response_hex"].(string),
		}, "|"))
	}
	return out, false
}

func formatNum(v any) string { b, _ := json.Marshal(v); return string(b) }

func graphqlIdentificationEvidence(t *testing.T, limitArg string) (*normalisedEvidence, bool) {
	t.Helper()
	builder := graphql.NewBuilder(nil, nil)
	builder.SetBusObservabilityProvider(newGraphQLBusObservabilityProvider(nil))
	handler, err := graphql.NewHandler(builder)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	args := ""
	if limitArg != "" {
		args = "(limit: " + limitArg + ")"
	}
	query, _ := json.Marshal(map[string]string{"query": "{ busIdentificationEvidence" + args + " { evidenceClass decoded qualification notice sessionId count items { address sequence sessionId capturedAt requestHex responseHex } } }"})
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(string(query)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var out struct {
		Data struct {
			E *struct {
				EvidenceClass, Qualification, Notice, SessionID string
				Decoded                                         bool
				Count                                           int
				Items                                           []struct {
					Address, Sequence                              int
					SessionID, CapturedAt, RequestHex, ResponseHex string
				}
			} `json:"busIdentificationEvidence"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("graphql body: %v %s", err, rec.Body.String())
	}
	if len(out.Errors) > 0 || out.Data.E == nil {
		return nil, true
	}
	e := out.Data.E
	n := &normalisedEvidence{
		Class: e.EvidenceClass, Qualification: e.Qualification, SessionID: e.SessionID, Decoded: e.Decoded, Count: e.Count,
		Notice: strings.NewReplacer("requestHex", "request_hex", "responseHex", "response_hex").Replace(e.Notice),
	}
	for _, it := range e.Items {
		n.Items = append(n.Items, strings.Join([]string{
			formatNum(it.Address), formatNum(it.Sequence), it.SessionID, it.CapturedAt, it.RequestHex, it.ResponseHex,
		}, "|"))
	}
	return n, false
}

// TestIdentificationEvidenceMCPAndGraphQLAgreeOnSharedStore executes the MCP
// tool and the GraphQL query against the same store and requires identical
// normalised payloads (ordering, timestamps, session identity, count, limit)
// and identical accept/reject behaviour for invalid limits.
func TestIdentificationEvidenceMCPAndGraphQLAgreeOnSharedStore(t *testing.T) {
	identificationEvidenceSharedStore(t)
	cases := []struct {
		name, mcpArgs, gqlLimit string
		wantErr                 bool
		wantItems               int
	}{
		{"no_limit", `{}`, "", false, 4},
		{"limit_1", `{"limit":1}`, "1", false, 1},
		{"limit_3", `{"limit":3}`, "3", false, 3},
		{"limit_above_count", `{"limit":99}`, "99", false, 4},
		{"limit_zero", `{"limit":0}`, "0", true, 0},
		{"limit_negative", `{"limit":-1}`, "-1", true, 0},
		{"limit_string", `{"limit":"x"}`, `"x"`, true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, mErr := mcpIdentificationEvidence(t, tc.mcpArgs)
			g, gErr := graphqlIdentificationEvidence(t, tc.gqlLimit)
			if mErr != tc.wantErr || gErr != tc.wantErr {
				t.Fatalf("error mismatch: mcp=%v graphql=%v want=%v", mErr, gErr, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if len(m.Items) != tc.wantItems {
				t.Fatalf("items = %d, want %d", len(m.Items), tc.wantItems)
			}
			if !reflect.DeepEqual(m, g) {
				t.Fatalf("surface drift\nmcp:     %+v\ngraphql: %+v", m, g)
			}
		})
	}
}
