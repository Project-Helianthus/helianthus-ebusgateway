package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/m2mgraphql"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func TestTeslaGen3EVSESemanticPublicationMapsConfiguredAndAllocated(t *testing.T) {
	p := newTeslaGen3EVSESemanticFixture(t)
	view, err := p.TeslaGen3EVSESemanticCurrent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"evse.limit.configured_current", "evse.limit.allocated_current", "tesla.wc3_24_44_3"} {
		if !strings.Contains(string(encoded), want) {
			t.Fatalf("publication missing %q: %s", want, encoded)
		}
	}
	server, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, teslaGen3EVSESemanticFixtureProvider{modbusV1FixtureProvider: &modbusV1FixtureProvider{}, publication: p})
	result := msp06Call(t, server.Handler(), SemanticV1EVSECurrentGetTool, map[string]any{})
	if result.isError {
		t.Fatalf("semantic result=%#v", result)
	}
	if result.envelope["data"] == nil {
		t.Fatal("semantic data missing")
	}
}

func TestTeslaGen3EVSESemanticPublicationWithholdsOnlyAllocated(t *testing.T) {
	p, err := NewTeslaGen3EVSESemanticPublication(teslaSemanticConfig())
	if err != nil {
		t.Fatal(err)
	}
	source := teslaGen3EVSECurrentLimitV1FixtureSource(t)
	base := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	source.Provisional = nil
	if err := p.Publish(source, TeslaGen3EVSESemanticEvidence{ObservationID: "observation:one", ObservedAt: base, EvaluatedAt: base, MonotonicNS: 1, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	view, err := p.TeslaGen3EVSESemanticCurrent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(view)
	if !strings.Contains(string(encoded), "evse.limit.configured_current") || !strings.Contains(string(encoded), "withheld_provisional_missing") || strings.Contains(string(encoded), "evse.limit.allocated_current\",\"value") {
		t.Fatalf("unexpected partial projection: %s", encoded)
	}
}

func TestTeslaGen3EVSESemanticPublicationRejectsProvisionalVectorsFieldLocally(t *testing.T) {
	base := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, want string
		timeout    uint32
		inhibit    bool
		evaluated  time.Time
	}{
		{name: "zero timeout", want: "withheld_provisional_zero_timeout", timeout: 0},
		{name: "inhibited", want: "withheld_provisional_inhibited", timeout: 600, inhibit: true},
		{name: "expired", want: "withheld_provisional_expired", timeout: 1, evaluated: base.Add(time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := NewTeslaGen3EVSESemanticPublication(teslaSemanticConfig())
			if err != nil {
				t.Fatal(err)
			}
			source := teslaGen3EVSECurrentLimitV1FixtureSource(t)
			provisional, err := modbusreg.NewTeslaGen3ProvisionalCurrentLimit(modbusreg.TeslaGen3ProvisionalCurrentLimitSpec{OperationVersion: modbusreg.TeslaGen3CurrentLimitOperationVersion24443, LimitCurrentMaxAmps: 16, LimitTimeoutSeconds: tc.timeout, InhibitCharging: tc.inhibit, SetRequestPayload: source.Provisional.SetRequestPayload(), AckPayload: source.Provisional.AckPayload(), ReadbackRequestPayload: source.Provisional.ReadbackRequestPayload(), ReadbackTerminalPayload: source.Provisional.ReadbackTerminalPayload()})
			if err != nil {
				t.Fatal(err)
			}
			source.Provisional = &provisional
			evaluated := tc.evaluated
			if evaluated.IsZero() {
				evaluated = base
			}
			if err := p.Publish(source, TeslaGen3EVSESemanticEvidence{ObservationID: "observation:" + tc.name, ObservedAt: base, EvaluatedAt: evaluated, MonotonicNS: 1, Sequence: 1}); err != nil {
				t.Fatal(err)
			}
			view, err := p.TeslaGen3EVSESemanticCurrent(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			encoded, _ := json.Marshal(view)
			if !strings.Contains(string(encoded), "evse.limit.configured_current") || !strings.Contains(string(encoded), tc.want) {
				t.Fatalf("want configured and %q: %s", tc.want, encoded)
			}
		})
	}
}

func TestTeslaGen3EVSESemanticPublicationRejectsReplayAndPreservesLastKnownGood(t *testing.T) {
	p := newTeslaGen3EVSESemanticFixture(t)
	before, err := p.TeslaGen3EVSESemanticCurrent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	source := teslaGen3EVSECurrentLimitV1FixtureSource(t)
	when := time.Date(2026, 9, 10, 12, 1, 0, 0, time.UTC)
	if err := p.Publish(source, TeslaGen3EVSESemanticEvidence{ObservationID: "observation:replay", ObservedAt: when, EvaluatedAt: when, MonotonicNS: 2, Sequence: 1}); err == nil {
		t.Fatal("replay accepted")
	}
	after, err := p.TeslaGen3EVSESemanticCurrent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(before.(json.RawMessage)) != string(after.(json.RawMessage)) {
		t.Fatal("rejected replay replaced current publication")
	}
}

func TestTeslaGen3EVSESemanticPublicationAdvancesStableCandidateRevisions(t *testing.T) {
	p := newTeslaGen3EVSESemanticFixture(t)
	before := teslaGen3EVSECandidateRevisions(t, p.current)
	source := teslaGen3EVSECurrentLimitV1FixtureSource(t)
	when := time.Date(2026, 9, 10, 12, 2, 0, 0, time.UTC)
	if err := p.Publish(source, TeslaGen3EVSESemanticEvidence{ObservationID: "observation:second", ObservedAt: when, EvaluatedAt: when, MonotonicNS: 2, Sequence: 2}); err != nil {
		t.Fatalf("second accepted publication: %v", err)
	}
	after := teslaGen3EVSECandidateRevisions(t, p.current)
	if len(before) != 2 || !reflect.DeepEqual(teslaGen3EVSECandidateIDs(before), teslaGen3EVSECandidateIDs(after)) {
		t.Fatalf("candidate identities changed: before=%v after=%v", before, after)
	}
	for id, revision := range after {
		if revision != "2" || before[id] != "1" {
			t.Fatalf("candidate %s revision before=%q after=%q", id, before[id], revision)
		}
	}
	if p.current.Revisions.Semantic != "2" {
		t.Fatalf("semantic revision=%q", p.current.Revisions.Semantic)
	}
}

func TestTeslaGen3EVSESemanticPublicationReevaluatesProvisionalExpiryWithMCPGraphQLParity(t *testing.T) {
	base := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	p, err := NewTeslaGen3EVSESemanticPublication(teslaSemanticConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := base
	p.now = func() time.Time { return now }
	source := teslaGen3EVSECurrentLimitV1FixtureSource(t)
	provisional, err := modbusreg.NewTeslaGen3ProvisionalCurrentLimit(modbusreg.TeslaGen3ProvisionalCurrentLimitSpec{OperationVersion: modbusreg.TeslaGen3CurrentLimitOperationVersion24443, LimitCurrentMaxAmps: 16, LimitTimeoutSeconds: 1, SetRequestPayload: source.Provisional.SetRequestPayload(), AckPayload: source.Provisional.AckPayload(), ReadbackRequestPayload: source.Provisional.ReadbackRequestPayload(), ReadbackTerminalPayload: source.Provisional.ReadbackTerminalPayload()})
	if err != nil {
		t.Fatal(err)
	}
	source.Provisional = &provisional
	if err := p.Publish(source, TeslaGen3EVSESemanticEvidence{ObservationID: "observation:expiring", ObservedAt: base, EvaluatedAt: base, MonotonicNS: 1, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	mcpHandler := teslaGen3EVSEMCPHandler(t, p)
	graphqlHandler := teslaGen3EVSEGraphQLHandler(t, p)
	for _, tc := range []struct {
		name          string
		now           time.Time
		withheld      bool
		freshnessNeed string
	}{
		{name: "just before expiry", now: base.Add(time.Second - time.Nanosecond), freshnessNeed: `"freshness":"fresh"`},
		{name: "at expiry", now: base.Add(time.Second), withheld: true, freshnessNeed: `"freshness":"stale"`},
		{name: "after expiry", now: base.Add(time.Second + time.Nanosecond), withheld: true, freshnessNeed: `"freshness":"expired"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now = tc.now
			mcpData := teslaGen3EVSEMCPCurrent(t, mcpHandler)
			graphqlData := teslaGen3EVSEGraphQLCurrent(t, graphqlHandler, tc.name)
			var mcpJSON, graphqlJSON any
			if err := json.Unmarshal(mcpData, &mcpJSON); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(graphqlData, &graphqlJSON); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(mcpJSON, graphqlJSON) {
				t.Fatalf("MCP/GraphQL divergence\nMCP: %s\nGraphQL: %s", mcpData, graphqlData)
			}
			encoded := string(mcpData)
			if !strings.Contains(encoded, "evse.limit.configured_current") || !strings.Contains(encoded, tc.freshnessNeed) {
				t.Fatalf("configured current or lifecycle state missing: %s", encoded)
			}
			if strings.Contains(encoded, "withheld_provisional_expired") != tc.withheld {
				t.Fatalf("withheld=%t want=%t: %s", strings.Contains(encoded, "withheld_provisional_expired"), tc.withheld, encoded)
			}
		})
	}
}

func TestTeslaGen3EVSESemanticPublicationCountsDelayedIngestionInMonotonicAge(t *testing.T) {
	base := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	p, err := NewTeslaGen3EVSESemanticPublication(teslaSemanticConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := base.Add(30 * time.Second)
	p.now = func() time.Time { return now }
	source := teslaGen3EVSECurrentLimitV1FixtureSource(t)
	provisional, err := modbusreg.NewTeslaGen3ProvisionalCurrentLimit(modbusreg.TeslaGen3ProvisionalCurrentLimitSpec{OperationVersion: modbusreg.TeslaGen3CurrentLimitOperationVersion24443, LimitCurrentMaxAmps: 16, LimitTimeoutSeconds: 60, SetRequestPayload: source.Provisional.SetRequestPayload(), AckPayload: source.Provisional.AckPayload(), ReadbackRequestPayload: source.Provisional.ReadbackRequestPayload(), ReadbackTerminalPayload: source.Provisional.ReadbackTerminalPayload()})
	if err != nil {
		t.Fatal(err)
	}
	source.Provisional = &provisional
	if err := p.Publish(source, TeslaGen3EVSESemanticEvidence{ObservationID: "observation:delayed", ObservedAt: base, EvaluatedAt: now, MonotonicNS: 100, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	allocated := teslaGen3EVSECandidate(t, p.current, "evse.limit.allocated_current")
	if allocated.Times.ReceiptMonotonic.Nanoseconds != "100" || allocated.Times.EvaluateMonotonic.Nanoseconds != "30000000100" || string(allocated.Times.EvaluatedAt.UnixNanoseconds) != strconv.FormatInt(now.UnixNano(), 10) {
		t.Fatalf("delayed coordinates=%+v", allocated.Times)
	}
	mcpHandler := teslaGen3EVSEMCPHandler(t, p)
	graphqlHandler := teslaGen3EVSEGraphQLHandler(t, p)
	for _, tc := range []struct {
		name, freshness string
		now             time.Time
		withheld        bool
	}{
		{name: "before wall expiry", now: base.Add(59 * time.Second), freshness: `"freshness":"fresh"`},
		{name: "at wall expiry", now: base.Add(60 * time.Second), freshness: `"freshness":"stale"`, withheld: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now = tc.now
			mcpData := teslaGen3EVSEMCPCurrent(t, mcpHandler)
			graphqlData := teslaGen3EVSEGraphQLCurrent(t, graphqlHandler, "delayed-"+tc.name)
			var mcpJSON, graphqlJSON any
			if err := json.Unmarshal(mcpData, &mcpJSON); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(graphqlData, &graphqlJSON); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(mcpJSON, graphqlJSON) {
				t.Fatalf("MCP/GraphQL delayed-ingestion divergence\nMCP: %s\nGraphQL: %s", mcpData, graphqlData)
			}
			encoded := string(mcpData)
			if !strings.Contains(encoded, tc.freshness) || strings.Contains(encoded, "withheld_provisional_expired") != tc.withheld {
				t.Fatalf("delayed lifecycle freshness=%q withheld=%t: %s", tc.freshness, tc.withheld, encoded)
			}
		})
	}
}

func TestTeslaGen3EVSESemanticPublicationDoesNotRegressAfterWallClockRollback(t *testing.T) {
	base := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	p, err := NewTeslaGen3EVSESemanticPublication(teslaSemanticConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := base
	p.now = func() time.Time { return now }
	source := teslaGen3EVSECurrentLimitV1FixtureSource(t)
	provisional, err := modbusreg.NewTeslaGen3ProvisionalCurrentLimit(modbusreg.TeslaGen3ProvisionalCurrentLimitSpec{OperationVersion: modbusreg.TeslaGen3CurrentLimitOperationVersion24443, LimitCurrentMaxAmps: 16, LimitTimeoutSeconds: 60, SetRequestPayload: source.Provisional.SetRequestPayload(), AckPayload: source.Provisional.AckPayload(), ReadbackRequestPayload: source.Provisional.ReadbackRequestPayload(), ReadbackTerminalPayload: source.Provisional.ReadbackTerminalPayload()})
	if err != nil {
		t.Fatal(err)
	}
	source.Provisional = &provisional
	if err := p.Publish(source, TeslaGen3EVSESemanticEvidence{ObservationID: "observation:rollback", ObservedAt: base, EvaluatedAt: base, MonotonicNS: 1, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	mcpHandler := teslaGen3EVSEMCPHandler(t, p)
	graphqlHandler := teslaGen3EVSEGraphQLHandler(t, p)
	for _, tc := range []struct {
		name string
		now  time.Time
	}{
		{name: "expired", now: base.Add(61 * time.Second)},
		{name: "rolled back before expiry", now: base.Add(30 * time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now = tc.now
			mcpData := teslaGen3EVSEMCPCurrent(t, mcpHandler)
			graphqlData := teslaGen3EVSEGraphQLCurrent(t, graphqlHandler, "rollback-"+tc.name)
			var mcpJSON, graphqlJSON any
			if err := json.Unmarshal(mcpData, &mcpJSON); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(graphqlData, &graphqlJSON); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(mcpJSON, graphqlJSON) {
				t.Fatalf("MCP/GraphQL rollback divergence\nMCP: %s\nGraphQL: %s", mcpData, graphqlData)
			}
			encoded := string(mcpData)
			if !strings.Contains(encoded, "withheld_provisional_expired") || !strings.Contains(encoded, `"freshness":"expired"`) {
				t.Fatalf("rollback resurrected allocation: %s", encoded)
			}
		})
	}
}

func TestTeslaGen3EVSESemanticPublicationConcurrentExpiryReadsRemainStable(t *testing.T) {
	p := newTeslaGen3EVSESemanticFixture(t)
	now := time.Date(2026, 9, 10, 12, 11, 0, 0, time.UTC)
	p.now = func() time.Time { return now }
	want, err := p.TeslaGen3EVSESemanticCurrent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantJSON := string(want.(json.RawMessage))
	errs := make(chan error, 32)
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			got, err := p.TeslaGen3EVSESemanticCurrent(context.Background())
			if err != nil {
				errs <- err
				return
			}
			if string(got.(json.RawMessage)) != wantJSON {
				errs <- fmt.Errorf("concurrent read diverged")
			}
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestTeslaGen3EVSESemanticPublicationRejectsMalformedPersistent(t *testing.T) {
	p, err := NewTeslaGen3EVSESemanticPublication(teslaSemanticConfig())
	if err != nil {
		t.Fatal(err)
	}
	err = p.Publish(TeslaGen3EVSECurrentLimitV1Source{Persistent: &modbusreg.TeslaGen3PersistentCurrentLimit{}}, TeslaGen3EVSESemanticEvidence{ObservationID: "bad", ObservedAt: time.Now(), EvaluatedAt: time.Now(), Sequence: 1})
	if err == nil {
		t.Fatal("malformed persistent accepted")
	}
	if _, err := p.TeslaGen3EVSESemanticCurrent(context.Background()); err == nil {
		t.Fatal("invalid publication advanced state")
	}
}

func TestTeslaGen3EVSESemanticCapabilityActivationRequiresNativePublicationEvidence(t *testing.T) {
	p, err := NewTeslaGen3EVSESemanticPublication(teslaSemanticConfig())
	if err != nil {
		t.Fatal(err)
	}
	source := teslaGen3EVSECurrentLimitV1FixtureSource(t)
	base := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	first := TeslaGen3EVSESemanticEvidence{ObservationID: "observation:capability-first", ObservedAt: base, EvaluatedAt: base, MonotonicNS: 1, Sequence: 1}
	if err := p.Publish(source, first); err != nil {
		t.Fatal(err)
	}
	before := p.current
	if len(before.Capabilities) != 2 {
		t.Fatalf("initial capabilities=%d", len(before.Capabilities))
	}

	updated, err := modbusreg.NewTeslaGen3PersistentCurrentLimit(modbusreg.TeslaGen3PersistentCurrentLimitSpec{
		OperationVersion:     modbusreg.TeslaGen3CurrentLimitOperationVersion24443,
		MaxOutputCurrentAmps: 15,
		RequestPayload:       teslaGen3EVSECurrentLimitV1Request(t, modbusreg.TeslaFC100OperationWCConfigureSettings, []byte{0x08, 0x0f}),
		TerminalPayload:      teslaGen3EVSECurrentLimitV1Terminal(8, []byte{0x08, 0x0f}),
	})
	if err != nil {
		t.Fatal(err)
	}
	source.Persistent = &updated
	second := TeslaGen3EVSESemanticEvidence{ObservationID: "observation:capability-second", ObservedAt: base.Add(time.Second), EvaluatedAt: base.Add(time.Second), MonotonicNS: 2, Sequence: 2}
	if err := p.Publish(source, second); err != nil {
		t.Fatal(err)
	}
	after := p.current
	if len(after.Capabilities) != len(before.Capabilities) {
		t.Fatalf("capability count changed: before=%d after=%d", len(before.Capabilities), len(after.Capabilities))
	}
	wantPersistent := evseEvidence("native.tesla.wc3.current_limit.persistent", teslaGen3EVSEPersistentActivation{Persistent: source.Persistent, Evidence: second})
	wantProvisional := evseEvidence("native.tesla.wc3.current_limit.provisional", teslaGen3EVSEProvisionalActivation{Provisional: source.Provisional, Evidence: second})
	sourceOnly := evseDigestEvidence("native.tesla.wc3.current_limit.activation", p.cfg.SourceID)
	for index, capability := range after.Capabilities {
		if capability.InstanceID != before.Capabilities[index].InstanceID {
			t.Fatalf("capability identity changed: before=%q after=%q", before.Capabilities[index].InstanceID, capability.InstanceID)
		}
		if capability.Qualification != semreg.QualificationQualified || capability.Availability != semreg.AvailabilityAvailable {
			t.Fatalf("capability availability=%q qualification=%q", capability.Availability, capability.Qualification)
		}
		if len(capability.ActivationEvidence) != 2 || !slices.ContainsFunc(capability.ActivationEvidence, func(ref semreg.EvidenceRef) bool { return reflect.DeepEqual(ref, wantPersistent) }) || !slices.ContainsFunc(capability.ActivationEvidence, func(ref semreg.EvidenceRef) bool { return reflect.DeepEqual(ref, wantProvisional) }) {
			t.Fatalf("capability activation evidence=%#v", capability.ActivationEvidence)
		}
		if slices.ContainsFunc(capability.ActivationEvidence, func(ref semreg.EvidenceRef) bool { return ref.Digest == sourceOnly.Digest }) {
			t.Fatalf("capability activation was synthesized from source identity: %#v", capability.ActivationEvidence)
		}
	}

	invalid := TeslaGen3EVSECurrentLimitV1Source{Persistent: &modbusreg.TeslaGen3PersistentCurrentLimit{}, Provisional: source.Provisional}
	if err := p.Publish(invalid, TeslaGen3EVSESemanticEvidence{ObservationID: "observation:capability-invalid", ObservedAt: base.Add(2 * time.Second), EvaluatedAt: base.Add(2 * time.Second), MonotonicNS: 3, Sequence: 3}); err == nil {
		t.Fatal("invalid evidence advanced the existing capability")
	}
	if !reflect.DeepEqual(after, p.current) {
		t.Fatal("rejected evidence changed the current capability")
	}
	other, err := NewTeslaGen3EVSESemanticPublication(teslaSemanticConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Publish(invalid, TeslaGen3EVSESemanticEvidence{ObservationID: "observation:same-source-invalid", ObservedAt: base, EvaluatedAt: base, MonotonicNS: 1, Sequence: 1}); err == nil {
		t.Fatal("same source identity synthesized a capability from invalid evidence")
	}
	if len(other.current.Capabilities) != 0 {
		t.Fatalf("invalid publication produced capabilities: %#v", other.current.Capabilities)
	}
}

func teslaGen3EVSECandidateRevisions(t *testing.T, snapshot semreg.Snapshot) map[semreg.CandidateID]semreg.Uint64 {
	t.Helper()
	revisions := make(map[semreg.CandidateID]semreg.Uint64)
	for _, envelope := range snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			revisions[candidate.CandidateID] = candidate.Revision
		}
	}
	return revisions
}

func teslaGen3EVSECandidate(t *testing.T, snapshot semreg.Snapshot, factID semreg.DefinitionID) semreg.FactCandidate {
	t.Helper()
	for _, envelope := range snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			if candidate.Key.FactID == factID {
				return candidate
			}
		}
	}
	t.Fatalf("candidate %q missing", factID)
	return semreg.FactCandidate{}
}

func teslaGen3EVSECandidateIDs(revisions map[semreg.CandidateID]semreg.Uint64) []semreg.CandidateID {
	ids := make([]semreg.CandidateID, 0, len(revisions))
	for id := range revisions {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func teslaGen3EVSEMCPHandler(t *testing.T, publication *TeslaGen3EVSESemanticPublication) http.Handler {
	t.Helper()
	server, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, teslaGen3EVSESemanticFixtureProvider{modbusV1FixtureProvider: &modbusV1FixtureProvider{}, publication: publication})
	return server.Handler()
}

func teslaGen3EVSEGraphQLHandler(t *testing.T, publication *TeslaGen3EVSESemanticPublication) http.Handler {
	t.Helper()
	handler, err := m2mgraphql.NewHandler(m2mgraphql.Config{AllowedAssets: map[string]struct{}{"asset:tesla-wc3-a": {}}, SemanticEVSECurrent: func(ctx context.Context, asset string) (json.RawMessage, bool) {
		if asset != "asset:tesla-wc3-a" {
			return nil, false
		}
		value, err := publication.TeslaGen3EVSESemanticCurrent(ctx)
		if err != nil {
			return nil, false
		}
		return value.(json.RawMessage), true
	}})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func teslaGen3EVSEMCPCurrent(t *testing.T, handler http.Handler) []byte {
	t.Helper()
	result := msp06Call(t, handler, SemanticV1EVSECurrentGetTool, map[string]any{})
	if result.isError || result.envelope["data"] == nil {
		t.Fatalf("MCP result=%#v", result)
	}
	encoded, err := json.Marshal(result.envelope["data"])
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func teslaGen3EVSEGraphQLCurrent(t *testing.T, handler http.Handler, principal string) []byte {
	t.Helper()
	request := `{"operationName":"SemanticEVSECurrent","query":` + strconv.Quote(`query SemanticEVSECurrent($request: M2MCurrentSnapshotRequest!) { semanticEVSECurrent(request: $request) { snapshot evaluation selections projection } }`) + `,"variables":{"request":{"contractId":"PUBLIC_GRAPHQL_SEMANTIC_EVSE_V1","assetRef":"asset:tesla-wc3-a"}}}`
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", strings.NewReader(request)).WithContext(m2mgraphql.WithMTLSPrincipal(context.Background(), principal)))
	if response.Code != http.StatusOK {
		t.Fatalf("GraphQL response=%d %s", response.Code, response.Body.String())
	}
	var decoded struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded.Data["semanticEVSECurrent"]
}

type teslaGen3EVSESemanticFixtureProvider struct {
	*modbusV1FixtureProvider
	publication *TeslaGen3EVSESemanticPublication
}

func (p teslaGen3EVSESemanticFixtureProvider) TeslaGen3EVSESemanticCurrent(ctx context.Context) (any, error) {
	return p.publication.TeslaGen3EVSESemanticCurrent(ctx)
}
func teslaSemanticConfig() TeslaGen3EVSESemanticConfig {
	return TeslaGen3EVSESemanticConfig{AssetID: "asset:tesla-wc3-a", SourceID: "source:tesla-wc3-a", EVSEID: "evse-a", ConnectorID: "connector-a", SourceEpoch: "epoch-a", ClockEpoch: "clock-a", DriverGeneration: 1}
}
func newTeslaGen3EVSESemanticFixture(t *testing.T) *TeslaGen3EVSESemanticPublication {
	t.Helper()
	p, err := NewTeslaGen3EVSESemanticPublication(teslaSemanticConfig())
	if err != nil {
		t.Fatal(err)
	}
	source := teslaGen3EVSECurrentLimitV1FixtureSource(t)
	when := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	if err := p.Publish(source, TeslaGen3EVSESemanticEvidence{ObservationID: "observation:fixture", ObservedAt: when, EvaluatedAt: when.Add(time.Second), MonotonicNS: 1, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	return p
}
