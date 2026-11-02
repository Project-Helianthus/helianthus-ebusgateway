package mcp

// G5 PR 1a.1 characterization baseline (issue #1147).
//
// Pins the CURRENT MCP payloads that the G5 lifecycle work will touch:
// ebus.v1.runtime.status.get (daemon/adapter status and regulator capability
// projection), ebus.v1.bus.summary.get (bus admission and startup status) and
// ebus.v1.bus.identification_evidence.list (raw identification evidence).
// All inputs are synthetic and fixed. Each golden entry records, next to the
// envelope, the later production change that may legitimately alter it
// ("changes_when"); a later G5 PR that changes an envelope must update the
// golden and explain the difference in its PR body.
//
// Golden: testdata/runtime_control_characterization.json. Regenerate after a
// reviewed intentional change with either
//
//	go test ./mcp -run Characterization -count=1 -update-characterization
//	UPDATE=1 go test ./mcp -run Characterization -count=1
//
// Determinism: meta.data_timestamp is stamped from the wall clock by
// production code; the test asserts it is RFC 3339 and replaces it with a
// placeholder. meta.data_hash is never stored: the test recomputes SHA-256
// over the exact serialized data bytes, asserts equality, and stores a
// placeholder, so the golden carries no literal digest.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

var updateRuntimeControlCharacterization = flag.Bool("update-characterization", false, "rewrite testdata/runtime_control_characterization.json")

const runtimeControlCharacterizationGolden = "runtime_control_characterization.json"

type ctlStatusProvider struct {
	daemon  ServiceStatus
	adapter ServiceStatus
}

func (provider ctlStatusProvider) DaemonStatus() ServiceStatus  { return provider.daemon }
func (provider ctlStatusProvider) AdapterStatus() ServiceStatus { return provider.adapter }

type ctlCapabilityStatusProvider struct {
	ctlStatusProvider
	capability string
}

func (provider ctlCapabilityStatusProvider) VaillantRegulatorCapability() string {
	return provider.capability
}

type ctlBusProvider struct {
	snapshot BusObservabilitySnapshot
}

func (provider ctlBusProvider) Snapshot() BusObservabilitySnapshot             { return provider.snapshot }
func (provider ctlBusProvider) ProtocolSpecimens(string) []BusProtocolSpecimen { return nil }

type ctlEvidenceProvider struct {
	snapshot IdentificationEvidenceSnapshot
}

func (provider ctlEvidenceProvider) IdentificationEvidence() IdentificationEvidenceSnapshot {
	return provider.snapshot
}

func ctlUint8(value uint8) *uint8 { return &value }

func ctlBusSnapshot(admission *BusAdmissionSourceSelection, phase string) BusObservabilitySnapshot {
	at := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	return BusObservabilitySnapshot{Summary: &BusSummary{
		LastUpdatedAt: &at,
		Status: &BusObservabilityStatus{
			LastUpdatedAt:          &at,
			TransportClass:         "synthetic",
			PublisherCadenceSec:    5,
			PublisherCadenceSource: "default",
			Capability: BusObservabilityCapability{
				ActiveSupported:  true,
				PassiveSupported: true,
				PassiveState:     "warming_up",
				EndpointState:    "connected",
			},
			Warmup:        BusObservabilityWarmup{State: "warming_up", RequiredTransactions: 3},
			TimingQuality: BusObservabilityTimingQuality{Active: "estimated", Passive: "unavailable", Busy: "unavailable", Periodicity: "unavailable"},
			BusAdmission:  &BusAdmission{SourceSelection: admission},
			Startup:       &BusObservabilityStartup{LastUpdatedAt: &at, Phase: phase, CacheEpoch: 1, LiveEpoch: 0},
			FeatureFlags:  ObserveFirstFeatureFlagState{ObserveFirstEnabled: true, ExternalWritePolicy: "record_only"},
		},
		Messages:    BusBoundedListSummary{Count: 0, Capacity: 16},
		Periodicity: BusBoundedListSummary{Count: 0, Capacity: 16},
	}}
}

func ctlIdentificationEvidence() IdentificationEvidenceSnapshot {
	at := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	const session = "observer-synthetic-session"
	return IdentificationEvidenceSnapshot{
		SessionID: session,
		Exchanges: []IdentificationEvidenceExchange{
			// Deliberately out of address order to pin the projection sort.
			{Address: 0x3A, Sequence: 2, SessionID: session, CapturedAt: at.Add(time.Second),
				Request: []byte{0x1A, 0x3A, 0x07, 0x04, 0x00}, Response: []byte{0x05, 0xEE, 'F', 'A', 'M', 'B'}},
			{Address: 0x2A, Sequence: 1, SessionID: session, CapturedAt: at,
				Request: []byte{0x1A, 0x2A, 0x07, 0x04, 0x00}, Response: []byte{0x05, 0xEE, 'F', 'A', 'M', 'A'}},
		},
	}
}

type ctlCase struct {
	name        string
	changesWhen string
	tool        string
	args        string
	configure   func(*Server)
}

func ctlCases() []ctlCase {
	pre := ctlStatusProvider{
		daemon:  ServiceStatus{Status: "running", FirmwareVersion: "0.0.0-char", InitiatorAddress: "auto"},
		adapter: ServiceStatus{Status: "unknown"},
	}
	admitted := ctlStatusProvider{
		daemon:  ServiceStatus{Status: "running", FirmwareVersion: "0.0.0-char", UpdatesAvailable: true, InitiatorAddress: "0x1A"},
		adapter: ServiceStatus{Status: "running", FirmwareVersion: "char-adapter-1"},
	}
	// G5 shell integration (issue #1222): the production status provider
	// gates adapter status on current native admission. With eBUS STOPPED
	// (or not yet admitted) the retained firmware stays visible unchanged,
	// the adapter is not reported running, the admitted initiator is
	// withdrawn and daemon status still describes the process.
	stoppedRetained := ctlStatusProvider{
		daemon:  ServiceStatus{Status: "running", FirmwareVersion: "0.0.0-char", InitiatorAddress: "auto"},
		adapter: ServiceStatus{Status: "stopped", FirmwareVersion: "char-adapter-1"},
	}
	unavailableRetained := ctlStatusProvider{
		daemon:  ServiceStatus{Status: "running", FirmwareVersion: "0.0.0-char", InitiatorAddress: "auto"},
		adapter: ServiceStatus{Status: "unavailable", FirmwareVersion: "char-adapter-1"},
	}
	degradedTask := ctlStatusProvider{
		daemon:  ServiceStatus{Status: "running", FirmwareVersion: "0.0.0-char", InitiatorAddress: "0x1A"},
		adapter: ServiceStatus{Status: "degraded", FirmwareVersion: "char-adapter-1", Condition: "generation task ebus.passive-observe-first failed"},
	}
	return []ctlCase{
		{
			name:        "runtime_status_degraded_failed_startup_task",
			changesWhen: "a failed start-critical eBUS generation task stops being presented as a degraded adapter condition naming the task",
			tool:        toolRuntimeStatusGetName, args: `{}`,
			configure: func(server *Server) {
				server.SetStatusProvider(ctlCapabilityStatusProvider{ctlStatusProvider: degradedTask, capability: "PRESENT"})
			},
		},
		{
			name:        "runtime_status_stopped_retained_firmware",
			changesWhen: "the adapter status vocabulary for a natively unadmitted adapter changes, or retained firmware stops being shown while stopped",
			tool:        toolRuntimeStatusGetName, args: `{}`,
			configure: func(server *Server) {
				server.SetStatusProvider(ctlCapabilityStatusProvider{ctlStatusProvider: stoppedRetained, capability: "PRESENT"})
			},
		},
		{
			name:        "runtime_status_unavailable_retained_firmware",
			changesWhen: "the adapter status vocabulary for a starting, backing-off or failed adapter changes",
			tool:        toolRuntimeStatusGetName, args: `{}`,
			configure: func(server *Server) {
				server.SetStatusProvider(ctlCapabilityStatusProvider{ctlStatusProvider: unavailableRetained, capability: "PRESENT"})
			},
		},
		{
			name:        "bus_summary_retained_while_stopped",
			changesWhen: "STOP starts clearing, moving or relabelling process-scoped bus observability (S7 forbids it); capture pauses and the last admission record keeps its provenance and age",
			tool:        toolBusSummaryGetName, args: `{}`,
			configure: func(server *Server) {
				server.SetBusObservabilityProvider(ctlBusProvider{snapshot: ctlBusSnapshot(&BusAdmissionSourceSelection{
					State: "active", Mode: "source_selection", Outcome: "active_probe_passed",
					SelectedSource: ctlUint8(0x1A), CompanionTarget: ctlUint8(0x1F),
					ActiveProbe: &BusAdmissionActiveProbe{Target: ctlUint8(0x2A), Opcode: "07 04", Status: "passed"},
				}, "live")})
			},
		},
		{
			name:        "identification_evidence_retained_while_stopped",
			changesWhen: "STOP starts clearing or re-stamping retained identification evidence (S7 forbids it); records keep native identity, session, sequence and capture time",
			tool:        toolBusIdentificationEvidenceListName, args: `{}`,
			configure: func(server *Server) {
				server.SetIdentificationEvidenceProvider(ctlEvidenceProvider{snapshot: ctlIdentificationEvidence()})
			},
		},
		{
			name:        "runtime_status_default_provider",
			changesWhen: "1a.2 registry seam installs a non-static default status provider or adds driver lifecycle fields to runtime status",
			tool:        toolRuntimeStatusGetName, args: `{}`,
			configure: func(*Server) {},
		},
		{
			name:        "runtime_status_pre_admission",
			changesWhen: "1b durable control adds desired/observed driver state to runtime status or changes the pre-admission initiator projection",
			tool:        toolRuntimeStatusGetName, args: `{}`,
			configure: func(server *Server) {
				server.SetStatusProvider(ctlCapabilityStatusProvider{ctlStatusProvider: pre, capability: "PRESENT"})
			},
		},
		{
			name:        "runtime_status_admitted",
			changesWhen: "1b durable control or 1c bridge changes daemon/adapter status fields after admission",
			tool:        toolRuntimeStatusGetName, args: `{}`,
			configure: func(server *Server) {
				server.SetStatusProvider(ctlCapabilityStatusProvider{ctlStatusProvider: admitted, capability: "NONE"})
			},
		},
		{
			name:        "runtime_status_unrecognised_capability",
			changesWhen: "a capability value outside NONE/PRESENT stops collapsing to UNKNOWN",
			tool:        toolRuntimeStatusGetName, args: `{}`,
			configure: func(server *Server) {
				server.SetStatusProvider(ctlCapabilityStatusProvider{ctlStatusProvider: pre, capability: "MAYBE"})
			},
		},
		{
			name:        "runtime_status_without_capability_method",
			changesWhen: "the capability probe stops being optional on the status provider",
			tool:        toolRuntimeStatusGetName, args: `{}`,
			configure: func(server *Server) {
				server.SetStatusProvider(admitted)
			},
		},
		{
			name:        "bus_summary_admission_pending_warmup",
			changesWhen: "1b durable control changes bus admission/startup projection while source selection is pending",
			tool:        toolBusSummaryGetName, args: `{}`,
			configure: func(server *Server) {
				server.SetBusObservabilityProvider(ctlBusProvider{snapshot: ctlBusSnapshot(&BusAdmissionSourceSelection{
					State: "pending", Reason: "source_selection_warmup_in_progress", Retryable: true,
				}, "cache")})
			},
		},
		{
			name:        "bus_summary_admission_active",
			changesWhen: "1b durable control or 1c bridge changes the admitted-source projection",
			tool:        toolBusSummaryGetName, args: `{}`,
			configure: func(server *Server) {
				server.SetBusObservabilityProvider(ctlBusProvider{snapshot: ctlBusSnapshot(&BusAdmissionSourceSelection{
					State: "active", Mode: "source_selection", Outcome: "active_probe_passed",
					SelectedSource: ctlUint8(0x1A), CompanionTarget: ctlUint8(0x1F),
					ActiveProbe: &BusAdmissionActiveProbe{Target: ctlUint8(0x2A), Opcode: "07 04", Status: "passed"},
				}, "live")})
			},
		},
		{
			name:        "bus_summary_admission_degraded",
			changesWhen: "1b durable control changes degraded admission retry projection",
			tool:        toolBusSummaryGetName, args: `{}`,
			configure: func(server *Server) {
				server.SetBusObservabilityProvider(ctlBusProvider{snapshot: ctlBusSnapshot(&BusAdmissionSourceSelection{
					State: "degraded", Reason: "active_probe_failed", FailedSource: ctlUint8(0x1A),
					Retryable: true, NextAction: "retry", AutomaticRetryScheduled: false,
				}, "cache")})
			},
		},
		{
			name:        "bus_summary_without_provider",
			changesWhen: "1a.2 registry seam always installs a bus observability provider",
			tool:        toolBusSummaryGetName, args: `{}`,
			configure: func(*Server) {},
		},
		{
			name:        "identification_evidence_list",
			changesWhen: "identification evidence gains decoding/qualification or changes ordering (must stay raw, evidence grade separate from qualification)",
			tool:        toolBusIdentificationEvidenceListName, args: `{}`,
			configure: func(server *Server) {
				server.SetIdentificationEvidenceProvider(ctlEvidenceProvider{snapshot: ctlIdentificationEvidence()})
			},
		},
		{
			name:        "identification_evidence_list_limit_one",
			changesWhen: "limit stops keeping the newest sequence",
			tool:        toolBusIdentificationEvidenceListName, args: `{"limit":1}`,
			configure: func(server *Server) {
				server.SetIdentificationEvidenceProvider(ctlEvidenceProvider{snapshot: ctlIdentificationEvidence()})
			},
		},
	}
}

// ctlCallNormalized invokes one tool and returns the envelope with
// data_timestamp and data_hash replaced by verified placeholders.
func ctlCallNormalized(t *testing.T, server *Server, tool, args string) map[string]any {
	t.Helper()
	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + tool + `","arguments":` + args + `}`),
	})
	if res.Error != nil {
		// A tool that is not registered (for example bus tools without a
		// provider) answers with a JSON-RPC error; that is pinned as observed.
		return map[string]any{"rpc_error": map[string]any{"code": res.Error.Code, "message": res.Error.Message}}
	}
	result, _ := res.Result.(map[string]any)
	content, _ := result["content"].([]any)
	if len(content) != 1 {
		t.Fatalf("%s content = %#v", tool, result["content"])
	}
	text, _ := content[0].(map[string]any)["text"].(string)
	var raw struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		t.Fatalf("%s envelope text is not JSON: %v", tool, err)
	}
	var envelope map[string]any
	if err := json.Unmarshal([]byte(text), &envelope); err != nil {
		t.Fatalf("%s envelope text is not JSON: %v", tool, err)
	}
	meta, _ := envelope["meta"].(map[string]any)
	if meta == nil {
		t.Fatalf("%s meta missing", tool)
	}
	timestamp, _ := meta["data_timestamp"].(string)
	if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		t.Fatalf("%s data_timestamp %q is not RFC 3339: %v", tool, timestamp, err)
	}
	meta["data_timestamp"] = "<data_timestamp>"
	sum := sha256.Sum256(raw.Data)
	if got, want := meta["data_hash"], hex.EncodeToString(sum[:]); got != want {
		// Changes when: data_hash stops being SHA-256 over the serialized data.
		t.Fatalf("%s data_hash is not SHA-256 of the serialized data", tool)
	}
	meta["data_hash"] = "<sha256(data) verified>"
	// Keep the exact serialized data bytes (production field order) so a
	// struct field reorder or rename is visible in the golden.
	envelope["data"] = raw.Data
	if isError, _ := result["isError"].(bool); isError {
		envelope["is_error"] = true
	}
	return envelope
}

// TestRuntimeControlCharacterizationMCPPayloads pins the MCP runtime status,
// bus summary and identification evidence envelopes against one golden.
func TestRuntimeControlCharacterizationMCPPayloads(t *testing.T) {
	observed := map[string]any{}
	for _, tc := range ctlCases() {
		server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
		if err != nil {
			t.Fatalf("NewServer() error = %v", err)
		}
		tc.configure(server)
		first := ctlCallNormalized(t, server, tc.tool, tc.args)
		second := ctlCallNormalized(t, server, tc.tool, tc.args)
		firstBytes, _ := json.Marshal(first)
		secondBytes, _ := json.Marshal(second)
		// Changes when: never intentionally; identical inputs must produce
		// identical envelopes (pre-1.0 determinism rule).
		if string(firstBytes) != string(secondBytes) {
			t.Errorf("%s is not deterministic across identical calls", tc.name)
		}
		observed[tc.name] = map[string]any{"changes_when": tc.changesWhen, "tool": tc.tool, "arguments": json.RawMessage(tc.args), "envelope": first}
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(observed); err != nil {
		t.Fatalf("marshal observed: %v", err)
	}
	got := buffer.Bytes()
	path := filepath.Join("testdata", runtimeControlCharacterizationGolden)
	if *updateRuntimeControlCharacterization || os.Getenv("UPDATE") == "1" {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v (regenerate with -update-characterization)", err)
	}
	if string(got) == string(want) {
		return
	}
	var wantCases map[string]json.RawMessage
	if err := json.Unmarshal(want, &wantCases); err != nil {
		t.Fatalf("golden is not JSON: %v", err)
	}
	var gotCases map[string]json.RawMessage
	_ = json.Unmarshal(got, &gotCases)
	for name, gotCase := range gotCases {
		wantCase, ok := wantCases[name]
		var gotCompact, wantCompact []byte
		gotCompact, _ = json.Marshal(gotCase)
		if ok {
			wantCompact, _ = json.Marshal(wantCase)
		}
		if !ok || string(gotCompact) != string(wantCompact) {
			t.Errorf("case %s drifted from the golden; see its changes_when note\nwant: %s\n got: %s", name, wantCompact, gotCompact)
		}
	}
	for name := range wantCases {
		if _, ok := gotCases[name]; !ok {
			t.Errorf("golden case %s is no longer produced", name)
		}
	}
	t.Errorf("golden mismatch; regenerate with -update-characterization after review")
}

// TestRuntimeControlCharacterizationS7RetainedEvidenceAcrossStop pins the S7
// rule at the MCP surface: with capture paused (no admitted generation) the
// retained bus summary and identification evidence project exactly as they
// did while admitted, with the same shapes, classification, provenance and
// capture times; STOP adds no lifecycle field and relabels nothing.
func TestRuntimeControlCharacterizationS7RetainedEvidenceAcrossStop(t *testing.T) {
	cases := map[string]ctlCase{}
	for _, tc := range ctlCases() {
		cases[tc.name] = tc
	}
	for retained, admitted := range map[string]string{
		"bus_summary_retained_while_stopped":             "bus_summary_admission_active",
		"identification_evidence_retained_while_stopped": "identification_evidence_list",
	} {
		envelopes := map[string][]byte{}
		for _, name := range []string{retained, admitted} {
			tc := cases[name]
			server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}
			tc.configure(server)
			envelopes[name], _ = json.Marshal(ctlCallNormalized(t, server, tc.tool, tc.args))
		}
		if string(envelopes[retained]) != string(envelopes[admitted]) {
			t.Errorf("%s differs from %s:\n stopped: %s\nadmitted: %s", retained, admitted, envelopes[retained], envelopes[admitted])
		}
	}
}
