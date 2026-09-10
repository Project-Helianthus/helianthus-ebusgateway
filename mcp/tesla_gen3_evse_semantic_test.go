package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
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
