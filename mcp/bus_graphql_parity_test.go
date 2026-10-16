package mcp_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	gql "github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

// This #1020 slice maps the existing read schemas in query_root.go,
// bus_observability.go and identification_evidence.go. It does not graduate tools.
// The MCP envelope (contract, consistency, timestamp, hash, error), protocol
// specimens (no GraphQL counterpart), and bus summary aggregates (counters,
// errors, frames, busy, reconstructor, specimen counts) are outside this mapping.
// Status mapping is limited to retained age, transport and passive availability;
// publisher cadence, other capability flags, endpoint/tap state, warmup, timing
// quality, degraded reasons, admission, startup and feature flags are separate slices.
// Notice prose uses each surface's field spelling and is checked separately.
const busParityStatusQuery = `last_updated_at: lastUpdatedAt transport_class: transportClass
 capability { passive_available: passiveAvailable passive_state: passiveState passive_reason: passiveReason }`

const busParityQuery = `query($limit: Int) {
 summary: busSummary { last_updated_at: lastUpdatedAt messages { count capacity } periodicity { count capacity }
 status { ` + busParityStatusQuery + ` } }
 messages: busMessages(limit: $limit) { count capacity status { ` + busParityStatusQuery + ` }
 items { scope family frame_type: frameType outcome observed_at: observedAt source_address: sourceAddress
 target_address: targetAddress request_len: requestLen response_len: responseLen } }
 periodicity: busPeriodicity(limit: $limit) { count capacity status { ` + busParityStatusQuery + ` }
 items { source_bucket: sourceBucket target_bucket: targetBucket primary secondary family state last_seen: lastSeen
 sample_count: sampleCount last_interval: lastInterval mean_interval: meanInterval min_interval: minInterval max_interval: maxInterval } }
 identification: busIdentificationEvidence(limit: $limit) { evidence_class: evidenceClass decoded qualification notice
 session_id: sessionId count items { address sequence session_id: sessionId captured_at: capturedAt request_hex: requestHex response_hex: responseHex } }
}`

// One synthetic retained store; adapters only translate provider types. No
// acquisition interface is supplied to either read surface.
type busParityStore struct {
	bus                     mcp.BusObservabilitySnapshot
	evidence                mcp.IdentificationEvidenceSnapshot
	busReads, evidenceReads int
}

func (p *busParityStore) Snapshot() mcp.BusObservabilitySnapshot { p.busReads++; return p.bus }
func (p *busParityStore) ProtocolSpecimens(string) []mcp.BusProtocolSpecimen {
	panic("unexpected specimen acquisition")
}
func (p *busParityStore) IdentificationEvidence() mcp.IdentificationEvidenceSnapshot {
	p.evidenceReads++
	return p.evidence
}

type busParityGraphQL struct{ store *busParityStore }

func (p busParityGraphQL) Snapshot() gql.BusObservabilitySnapshot {
	s := p.store.Snapshot()
	out := gql.BusObservabilitySnapshot{}
	if s.Summary != nil {
		v := s.Summary
		out.Summary = &gql.BusSummary{LastUpdatedAt: v.LastUpdatedAt,
			Messages:    gql.BusBoundedListSummary{Count: v.Messages.Count, Capacity: v.Messages.Capacity},
			Periodicity: gql.BusBoundedListSummary{Count: v.Periodicity.Count, Capacity: v.Periodicity.Capacity}}
		if v.Status != nil {
			out.Summary.Status = &gql.BusObservabilityStatus{LastUpdatedAt: v.Status.LastUpdatedAt, TransportClass: v.Status.TransportClass,
				Capability: gql.BusObservabilityCapability{PassiveAvailable: v.Status.Capability.PassiveAvailable,
					PassiveState: v.Status.Capability.PassiveState, PassiveReason: v.Status.Capability.PassiveReason}}
		}
	}
	for _, v := range s.Messages {
		out.Messages = append(out.Messages, gql.BusMessage{Scope: v.Scope, Family: v.Family,
			FrameType: v.FrameType, Outcome: v.Outcome, ObservedAt: v.ObservedAt.UTC().Format(time.RFC3339Nano), SourceAddress: v.SourceAddress,
			TargetAddress: v.TargetAddress, RequestLen: v.RequestLen, ResponseLen: v.ResponseLen})
	}
	for _, v := range s.Periodicity {
		out.Periodicity = append(out.Periodicity, gql.BusPeriodicityEntry{
			SourceBucket: v.SourceBucket, TargetBucket: v.TargetBucket, Primary: v.Primary, Secondary: v.Secondary,
			Family: v.Family, State: v.State, LastSeen: v.LastSeen.UTC().Format(time.RFC3339Nano), SampleCount: v.SampleCount, LastInterval: v.LastInterval,
			MeanInterval: v.MeanInterval, MinInterval: v.MinInterval, MaxInterval: v.MaxInterval})
	}
	return out
}
func (p busParityGraphQL) IdentificationEvidence() gql.IdentificationEvidenceSnapshot {
	s := p.store.IdentificationEvidence()
	out := gql.IdentificationEvidenceSnapshot{SessionID: s.SessionID}
	for _, v := range s.Exchanges {
		out.Exchanges = append(out.Exchanges, gql.IdentificationEvidenceExchange{
			Address: v.Address, Sequence: v.Sequence, SessionID: v.SessionID, CapturedAt: v.CapturedAt, Request: v.Request, Response: v.Response})
	}
	return out
}

func busParityJSON(t *testing.T, handler http.Handler, limit int) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{"query": busParityQuery, "variables": map[string]any{"limit": limit}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || result["errors"] != nil {
		t.Fatalf("GraphQL response: %s", rec.Body.String())
	}
	return result["data"].(map[string]any)
}

func busParityMCP(t *testing.T, server *mcp.Server, name string, limit int) map[string]any {
	t.Helper()
	args := fmt.Sprintf(`{"name":%q,"arguments":{"limit":%d}}`, name, limit)
	if name == "ebus.v1.bus.summary.get" {
		args = fmt.Sprintf(`{"name":%q,"arguments":{}}`, name)
	}
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":` + args + `}`
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	var response struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil || response.Error != nil || len(response.Result.Content) != 1 {
		t.Fatalf("MCP dispatch: err=%v body=%s", err, rec.Body.String())
	}
	var env map[string]any
	if err := json.Unmarshal([]byte(response.Result.Content[0].Text), &env); err != nil {
		t.Fatal(err)
	}
	if env["error"] != nil {
		t.Fatalf("MCP %s error: %v", name, env["error"])
	}
	meta := env["meta"].(map[string]any)
	var rawEnvelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(response.Result.Content[0].Text), &rawEnvelope); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(rawEnvelope.Data)
	if meta["data_hash"] != hex.EncodeToString(sum[:]) {
		t.Fatalf("%s: data hash does not match payload", name)
	}
	return env
}

// Explicit selection preserves only the declared mapping. Missing optional MCP
// keys map to GraphQL null; omitted MCP items map to the GraphQL empty list.
func busParitySelect(value any, fields ...string) map[string]any {
	source, _ := value.(map[string]any)
	out := make(map[string]any, len(fields))
	for _, key := range fields {
		out[key] = source[key]
	}
	return out
}
func busParityStatus(value any) any {
	if value == nil {
		return nil
	}
	out := busParitySelect(value, "last_updated_at", "transport_class", "capability")
	out["capability"] = busParitySelect(out["capability"], "passive_available", "passive_state", "passive_reason")
	return out
}
func busParityMapped(name string, value any) any {
	if name == "summary" {
		if value == nil {
			return nil
		}
		out := busParitySelect(value, "last_updated_at", "messages", "periodicity", "status")
		out["status"] = busParityStatus(out["status"])
		return out
	}
	source := value.(map[string]any)
	out := busParitySelect(source, "count", "capacity", "status", "items")
	if name == "identification" {
		out = busParitySelect(source, "evidence_class", "decoded", "qualification", "session_id", "count", "items")
	} else {
		out["status"] = busParityStatus(out["status"])
	}
	items := []any{}
	if list, ok := source["items"].([]any); ok {
		for _, raw := range list {
			if name == "periodicity" {
				item := busParitySelect(raw, "source_bucket", "target_bucket", "primary", "secondary", "family", "state", "last_seen", "sample_count",
					"last_interval", "mean_interval", "min_interval", "max_interval")
				items = append(items, item)
			} else {
				items = append(items, raw)
			}
		}
	}
	out["items"] = items
	return out
}
func busParityCompare(name string, mcpValue, graphqlValue any) error {
	left, right := busParityMapped(name, mcpValue), busParityMapped(name, graphqlValue)
	if !reflect.DeepEqual(left, right) {
		return fmt.Errorf("%s parity: MCP=%v GraphQL=%v", name, left, right)
	}
	return nil
}

func TestBusGraphQLParityRetainedEvidence(t *testing.T) {
	// Synthetic logical clock, unrelated to any capture or installation.
	at := time.Unix(100, 123).UTC()
	provider := &busParityStore{}
	reg := registry.NewDeviceRegistry(nil)
	invoker := &busParityInvoker{}
	server, err := mcp.NewServer(reg, invoker)
	if err != nil {
		t.Fatal(err)
	}
	server.SetBusObservabilityProvider(provider)
	server.SetIdentificationEvidenceProvider(provider)
	builder := gql.NewBuilder(reg, nil)
	builder.SetBusObservabilityProvider(busParityGraphQL{provider})
	builder.SetIdentificationEvidenceProvider(busParityGraphQL{provider})
	handler, err := gql.NewHandler(builder)
	if err != nil {
		t.Fatal(err)
	}

	for _, state := range []string{"empty", "populated", "partial_unavailable"} {
		t.Run(state, func(t *testing.T) {
			switch state {
			case "empty":
				provider.bus.Summary = &mcp.BusSummary{}
			case "populated":
				provider.bus = mcp.BusObservabilitySnapshot{Summary: &mcp.BusSummary{LastUpdatedAt: &at,
					Status: &mcp.BusObservabilityStatus{LastUpdatedAt: &at, TransportClass: "synthetic", Capability: mcp.BusObservabilityCapability{
						PassiveAvailable: true, PassiveState: "available"}}, Messages: mcp.BusBoundedListSummary{Count: 2, Capacity: 8}, Periodicity: mcp.BusBoundedListSummary{Count: 2, Capacity: 4}},
					Messages: []mcp.BusMessage{{Scope: "passive", Family: "synthetic-a", ObservedAt: at},
						{Scope: "passive", Family: "synthetic-b", FrameType: "response", Outcome: "ok", ObservedAt: at.Add(time.Second), SourceAddress: 8, TargetAddress: 21, RequestLen: 5, ResponseLen: 2}},
					Periodicity: []mcp.BusPeriodicityEntry{{Family: "synthetic-a", LastSeen: at},
						{SourceBucket: "synthetic-source", TargetBucket: "synthetic-target", Primary: 7, Secondary: 4, Family: "synthetic-b", State: "observed", LastSeen: at.Add(time.Second), SampleCount: 2, LastInterval: "1s"}}}
				provider.evidence = mcp.IdentificationEvidenceSnapshot{SessionID: "synthetic-session", Exchanges: []mcp.IdentificationEvidenceExchange{
					{Address: 21, Sequence: 2, SessionID: "synthetic-session", CapturedAt: at.Add(time.Second), Request: []byte{0x71, 21, 7, 4, 0}, Response: []byte{1, 0}},
					{Address: 8, Sequence: 1, SessionID: "synthetic-session", CapturedAt: at, Request: []byte{0x71, 8, 7, 4, 0}, Response: []byte{1, 1}}}}
			case "partial_unavailable":
				// Failed acquisition is represented by availability, not replacement of
				// the retained evidence. The read projections must keep old identity/age.
				provider.bus.Summary.Status.Capability = mcp.BusObservabilityCapability{PassiveState: "unavailable", PassiveReason: "synthetic_failure"}
				provider.bus.Periodicity[1].State = "stale"
				provider.evidence.Exchanges = append(provider.evidence.Exchanges, mcp.IdentificationEvidenceExchange{
					Address: 31, Sequence: 3, SessionID: "synthetic-session", CapturedAt: at.Add(2 * time.Second), Request: []byte{0x71, 31, 7, 4, 0}})
			}
			before, _ := json.Marshal([]any{provider.bus, provider.evidence})
			for _, limit := range []int{1, 10} {
				graph := busParityJSON(t, handler, limit)
				for _, surface := range []struct{ name, tool string }{
					{"summary", "ebus.v1.bus.summary.get"}, {"messages", "ebus.v1.bus.messages.list"},
					{"periodicity", "ebus.v1.bus.periodicity.list"}, {"identification", "ebus.v1.bus.identification_evidence.list"}} {
					env := busParityMCP(t, server, surface.tool, limit)
					if err := busParityCompare(surface.name, env["data"], graph[surface.name]); err != nil {
						t.Fatal(err)
					}
					if surface.name == "identification" {
						data := env["data"].(map[string]any)
						other := graph[surface.name].(map[string]any)
						wantNotice := strings.NewReplacer("request_hex", "requestHex", "response_hex", "responseHex").Replace(busParityNotice)
						if data["notice"] != busParityNotice || other["notice"] != wantNotice {
							t.Fatal("evidence notice drift")
						}
						if data["evidence_class"] != "raw" || data["qualification"] != "none" || data["decoded"] != false {
							t.Fatal("raw evidence meaning changed")
						}
						if state != "empty" {
							items := data["items"].([]any)
							last := items[len(items)-1].(map[string]any)
							wantCount, wantSequence, age := float64(2), float64(2), time.Second
							if state == "partial_unavailable" {
								wantCount, wantSequence, age = 3, 3, 2*time.Second
							}
							if data["count"] != wantCount || last["session_id"] != "synthetic-session" || last["sequence"] != wantSequence || last["captured_at"] != at.Add(age).Format(time.RFC3339Nano) {
								t.Fatal("retained provenance/age changed")
							}
							if state == "partial_unavailable" && last["response_hex"] != "" {
								t.Fatal("missing response must remain empty raw evidence")
							}
						}
					}
					// A one-sided projection fault must be rejected by the same oracle used
					// above, for every surface and state (including empty retained state).
					damaged := busParitySelect(graph[surface.name])
					for key, value := range graph[surface.name].(map[string]any) {
						damaged[key] = value
					}
					if surface.name == "summary" {
						damaged["last_updated_at"] = "corrupted"
					} else {
						damaged["count"] = float64(-1)
					}
					if busParityCompare(surface.name, env["data"], damaged) == nil {
						t.Fatalf("%s fault was not detected", surface.name)
					}
				}
				if state != "empty" {
					summary := graph["summary"].(map[string]any)
					if summary["last_updated_at"] != at.Format(time.RFC3339Nano) {
						t.Fatal("last-known-good age lost")
					}
				}
			}
			after, _ := json.Marshal([]any{provider.bus, provider.evidence})
			if string(before) != string(after) {
				t.Fatal("read changed retained native evidence")
			}
			if invoker.calls != 0 {
				t.Fatal("read invoked native method")
			}
		})
	}
	if provider.busReads == 0 || provider.evidenceReads == 0 {
		t.Fatal("provider was not exercised")
	}
}

// Keep the native invoker observable independently of provider snapshot reads.
type busParityInvoker struct{ calls int }

func (i *busParityInvoker) Invoke(context.Context, router.Plane, string, map[string]any) (any, error) {
	i.calls++
	return nil, fmt.Errorf("unexpected native acquisition")
}

const busParityNotice = "Raw bytes of the directed 07 04 identification exchange as seen by the gateway bus observer. " +
	"request_hex is QQ ZZ PB SB NN [data]; response_hex is NN [data]; CRC and ACK bytes are not retained. " +
	"Nothing is decoded, normalised or qualified."

// This is a discrepancy handoff, not a claim of null parity. A contract owner
// must decide whether the empty GraphQL wrapper or the MCP null should change.
func TestBusGraphQLParityAbsentSummaryDiscrepancy(t *testing.T) {
	provider := &busParityStore{}
	reg := registry.NewDeviceRegistry(nil)
	server, err := mcp.NewServer(reg, &busParityInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	server.SetBusObservabilityProvider(provider)
	builder := gql.NewBuilder(reg, nil)
	builder.SetBusObservabilityProvider(busParityGraphQL{provider})
	builder.SetIdentificationEvidenceProvider(busParityGraphQL{provider})
	handler, err := gql.NewHandler(builder)
	if err != nil {
		t.Fatal(err)
	}
	graph := busParityJSON(t, handler, 1)
	env := busParityMCP(t, server, "ebus.v1.bus.summary.get", 1)
	if env["data"] != nil {
		t.Fatal("absent MCP summary no longer null; revisit discrepancy")
	}
	expected := map[string]any{"last_updated_at": nil, "status": nil,
		"messages":    map[string]any{"count": float64(0), "capacity": float64(0)},
		"periodicity": map[string]any{"count": float64(0), "capacity": float64(0)}}
	if !reflect.DeepEqual(graph["summary"], expected) {
		t.Fatalf("absent GraphQL summary: %v", graph["summary"])
	}
	if busParityCompare("summary", env["data"], graph["summary"]) == nil {
		t.Fatal("oracle concealed null/wrapper discrepancy")
	}
	for _, surface := range []struct{ name, tool string }{
		{"messages", "ebus.v1.bus.messages.list"}, {"periodicity", "ebus.v1.bus.periodicity.list"},
		{"identification", "ebus.v1.bus.identification_evidence.list"}} {
		data := busParityMCP(t, server, surface.tool, 1)["data"]
		if err := busParityCompare(surface.name, data, graph[surface.name]); err != nil {
			t.Fatal(err)
		}
	}
}
