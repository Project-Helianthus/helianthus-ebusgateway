package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/m2mgraphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

func TestTeslaHSCRetainedOwnerConfigurationFailsClosed(t *testing.T) {
	if owner, err := startTeslaHSCRetainedOwner(ebusgateway.TeslaGen3HSCRetainedConfig{}); err != nil || owner != nil {
		t.Fatalf("disabled zero config = %T, %v", owner, err)
	}
	for name, mutate := range map[string]func(*ebusgateway.TeslaGen3HSCRetainedConfig){
		"disabled active fields": func(c *ebusgateway.TeslaGen3HSCRetainedConfig) { c.Enabled = false },
		"wrong profile":          func(c *ebusgateway.TeslaGen3HSCRetainedConfig) { c.Profile = "wc3_other" },
		"missing endpoint":       func(c *ebusgateway.TeslaGen3HSCRetainedConfig) { c.EndpointID = "" },
		"same asset source":      func(c *ebusgateway.TeslaGen3HSCRetainedConfig) { c.SourceID = c.AssetID },
		"zero generation":        func(c *ebusgateway.TeslaGen3HSCRetainedConfig) { c.DriverGeneration = 0 },
		"broadcast node":         func(c *ebusgateway.TeslaGen3HSCRetainedConfig) { c.Node = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			config := teslaRetainedConfig()
			mutate(&config)
			if _, err := startTeslaHSCRetainedOwner(config); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestTeslaHSCRetainedOwnerIngestsCorrelatedOutcomesAndPublishesDetachedState(t *testing.T) {
	owner := startTeslaRetainedFixture(t, teslaRetainedConfig())
	persistent := teslaPersistentOutcome(t, 1, 16)
	if err := owner.IngestPersistent(context.Background(), persistent); err != nil {
		t.Fatal(err)
	}
	provisional := teslaProvisionalOutcome(t, 2, 16, 600, false, 32)
	if err := owner.IngestProvisional(context.Background(), provisional); err != nil {
		t.Fatal(err)
	}

	source, err := owner.TeslaGen3EVSECurrentLimitV1(context.Background())
	if err != nil || source.Persistent == nil || source.Provisional == nil ||
		source.Persistent.MaxOutputCurrentAmps() != 16 || source.Provisional.LimitTimeoutSeconds() != 600 {
		t.Fatalf("source/error = %#v / %v", source, err)
	}
	semantic, err := owner.TeslaGen3EVSESemanticCurrent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(semantic)
	if err != nil || !bytes.Contains(encoded, []byte(`evse.limit.configured_current`)) || !bytes.Contains(encoded, []byte(`evse.limit.allocated_current`)) {
		t.Fatalf("semantic = %s, error=%v", encoded, err)
	}
	evidence := owner.RetainedEvidence()
	if len(evidence) != 3 || evidence[0].EndpointID != "tesla-hsc-public-a" || evidence[0].TerminalOutcome != "success" {
		t.Fatalf("evidence = %#v", evidence)
	}

	// All exposed state and evidence must be detached from caller-owned bytes.
	persistent.Exchange.RequestPayload[0] ^= 0xff
	provisional.Set.ResponseFrames[0].ADU[0] ^= 0xff
	evidence[0].RequestADU[0] ^= 0xff
	again, _ := owner.TeslaGen3EVSECurrentLimitV1(context.Background())
	againEvidence := owner.RetainedEvidence()
	if again.Persistent.RequestPayload()[0] == persistent.Exchange.RequestPayload[0] || againEvidence[0].RequestADU[0] == evidence[0].RequestADU[0] {
		t.Fatal("retained records or evidence alias caller-owned bytes")
	}
}

func TestTeslaHSCRetainedOwnerFeedsDetachedPrometheusEVSEAndFencesLifecycle(t *testing.T) {
	owner := startTeslaRetainedFixture(t, teslaRetainedConfig())
	if err := owner.IngestPersistent(context.Background(), teslaPersistentOutcome(t, 1, 16)); err != nil {
		t.Fatal(err)
	}
	if err := owner.IngestProvisional(context.Background(), teslaProvisionalOutcome(t, 2, 12, 600, false, 32)); err != nil {
		t.Fatal(err)
	}

	at := time.Unix(1_700_000_010, 123).UTC()
	domains := semanticPrometheusDomains(nil, nil, owner, false, false, true, "", at)
	if len(domains) != 1 || domains[0].Name != "evse" || !domains[0].Available ||
		len(domains[0].Snapshot.Facts) == 0 || len(domains[0].Projection.Dispositions) == 0 {
		t.Fatalf("Tesla retained Prometheus domain = %#v", domains)
	}
	if err := owner.Fence(7, 8); err != nil {
		t.Fatal(err)
	}
	domains = semanticPrometheusDomains(nil, nil, owner, false, false, true, "", at.Add(time.Second))
	if len(domains) != 1 || domains[0].Name != "evse" || domains[0].Available {
		t.Fatalf("fenced Tesla retained Prometheus domain = %#v", domains)
	}
}

func TestTeslaHSCRetainedOwnerRejectsMismatchAndPreservesLastKnownGood(t *testing.T) {
	owner := startTeslaRetainedFixture(t, teslaRetainedConfig())
	if err := owner.IngestPersistent(context.Background(), teslaPersistentOutcome(t, 1, 16)); err != nil {
		t.Fatal(err)
	}
	before, _ := owner.TeslaGen3EVSECurrentLimitV1(context.Background())

	tests := map[string]func(*TeslaGen3PersistentOutcome){
		"generation":       func(o *TeslaGen3PersistentOutcome) { o.Exchange.DriverGeneration++ },
		"identity":         func(o *TeslaGen3PersistentOutcome) { o.Exchange.SourceEpoch = "epoch:other" },
		"request adu":      func(o *TeslaGen3PersistentOutcome) { o.Exchange.RequestADU[1] ^= 1 },
		"response adu":     func(o *TeslaGen3PersistentOutcome) { o.Exchange.ResponseFrames[0].ADU[1] ^= 1 },
		"typed value":      func(o *TeslaGen3PersistentOutcome) { o.MaxOutputCurrentAmps++ },
		"terminal outcome": func(o *TeslaGen3PersistentOutcome) { o.Exchange.TerminalOutcome = "timeout" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			outcome := teslaPersistentOutcome(t, 2, 20)
			mutate(&outcome)
			if err := owner.IngestPersistent(context.Background(), outcome); err == nil {
				t.Fatal("mismatched outcome accepted")
			}
			after, err := owner.TeslaGen3EVSECurrentLimitV1(context.Background())
			if err != nil || after.Persistent.MaxOutputCurrentAmps() != before.Persistent.MaxOutputCurrentAmps() {
				t.Fatalf("last known good changed: %#v / %v", after, err)
			}
		})
	}

	badProvisional := teslaProvisionalOutcome(t, 2, 20, 300, false, 32)
	badProvisional.Readback.CorrelationID = badProvisional.Set.CorrelationID
	if err := owner.IngestProvisional(context.Background(), badProvisional); err == nil {
		t.Fatal("cross-operation correlation collision accepted")
	}
	after, _ := owner.TeslaGen3EVSECurrentLimitV1(context.Background())
	if after.Persistent == nil || after.Provisional != nil {
		t.Fatalf("invalid provisional replaced persistent sibling: %#v", after)
	}
}

func TestTeslaHSCRetainedOwnerPersistentSiblingDoesNotRefreshProvisional(t *testing.T) {
	owner := startTeslaRetainedFixture(t, teslaRetainedConfig())
	if err := owner.IngestPersistent(context.Background(), teslaPersistentOutcome(t, 1, 16)); err != nil {
		t.Fatal(err)
	}
	if err := owner.IngestProvisional(context.Background(), teslaProvisionalOutcome(t, 2, 12, 60, false, 32)); err != nil {
		t.Fatal(err)
	}
	// Correlation 65 is 62 seconds after the retained provisional readback.
	// It updates configured current after the provisional's 60-second lifetime
	// without supplying any new provisional completed outcome.
	if err := owner.IngestPersistent(context.Background(), teslaPersistentOutcome(t, 65, 21)); err != nil {
		t.Fatal(err)
	}
	native, err := owner.TeslaGen3EVSECurrentLimitV1(context.Background())
	evidence := owner.RetainedEvidence()
	if err != nil || native.Provisional == nil || len(evidence) != 3 || evidence[2].CorrelationID != 3 {
		t.Fatalf("native provisional sibling/evidence = %#v / %#v / %v", native.Provisional, evidence, err)
	}
	provider := newGatewayModbusMCPProviderWithRuntimes(nil, nil, owner)
	semantic := provider.(mcp.TeslaGen3EVSESemanticProvider)
	mcpValue, err := semantic.TeslaGen3EVSESemanticCurrent(context.Background())
	mcpEncoded, marshalErr := json.Marshal(mcpValue)
	if err != nil || marshalErr != nil || !bytes.Contains(mcpEncoded, []byte(`"fact_id":"evse.limit.configured_current"`)) ||
		bytes.Contains(mcpEncoded, []byte(`"fact_id":"evse.limit.allocated_current"`)) ||
		!bytes.Contains(mcpEncoded, []byte(`"item_id":"evse.limit.allocated_current","outcome":"withheld"`)) {
		t.Fatalf("MCP semantic value/error = %s / %v / %v", mcpEncoded, err, marshalErr)
	}

	handler, err := m2mgraphql.NewHandler(m2mgraphql.Config{
		AllowedAssets: map[string]struct{}{owner.cfg.AssetID: {}},
		SemanticEVSECurrent: func(ctx context.Context, asset string) (json.RawMessage, bool) {
			value, currentErr := currentTeslaEVSEPublic(ctx, asset, owner)
			return value, currentErr == nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	query := `query SemanticEVSECurrent($request: M2MCurrentSnapshotRequest!) { semanticEVSECurrent(request: $request) { snapshot evaluation selections projection } }`
	request := `{"operationName":"SemanticEVSECurrent","query":` + strconv.Quote(query) + `,"variables":{"request":{"contractId":"PUBLIC_GRAPHQL_SEMANTIC_EVSE_V1","assetRef":"asset:tesla-wc3-a"}}}`
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/graphql/m2m/v1", strings.NewReader(request)).WithContext(m2mgraphql.WithMTLSPrincipal(context.Background(), "test-principal")))
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"fact_id":"evse.limit.configured_current"`)) ||
		bytes.Contains(response.Body.Bytes(), []byte(`"fact_id":"evse.limit.allocated_current"`)) ||
		!bytes.Contains(response.Body.Bytes(), []byte(`"item_id":"evse.limit.allocated_current","outcome":"withheld"`)) {
		t.Fatalf("authenticated GraphQL response=%d %s", response.Code, response.Body.String())
	}
}

func TestTeslaHSCRetainedOwnerPublishesRetainedProvisionalInInitialSemanticBatch(t *testing.T) {
	owner := startTeslaRetainedFixture(t, teslaRetainedConfig())
	if err := owner.IngestProvisional(context.Background(), teslaProvisionalOutcome(t, 1, 12, 600, false, 32)); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.TeslaGen3EVSESemanticCurrent(context.Background()); !errors.Is(err, errTeslaHSCRetainedUnavailable) {
		t.Fatalf("provisional-only semantic read error = %v", err)
	}
	if err := owner.IngestPersistent(context.Background(), teslaPersistentOutcome(t, 3, 16)); err != nil {
		t.Fatal(err)
	}
	semantic, err := owner.TeslaGen3EVSESemanticCurrent(context.Background())
	encoded, marshalErr := json.Marshal(semantic)
	if err != nil || marshalErr != nil ||
		!bytes.Contains(encoded, []byte(`"fact_id":"evse.limit.configured_current"`)) ||
		!bytes.Contains(encoded, []byte(`"fact_id":"evse.limit.allocated_current"`)) {
		t.Fatalf("initial combined semantic value/error = %s / %v / %v", encoded, err, marshalErr)
	}
}

func TestTeslaHSCRetainedOwnerProvisionalSiblingPreservesPersistentReceipt(t *testing.T) {
	owner := startTeslaRetainedFixture(t, teslaRetainedConfig())
	if err := owner.IngestPersistent(context.Background(), teslaPersistentOutcome(t, 1, 16)); err != nil {
		t.Fatal(err)
	}
	if err := owner.IngestProvisional(context.Background(), teslaProvisionalOutcome(t, 65, 12, 600, false, 32)); err != nil {
		t.Fatal(err)
	}
	value, err := owner.TeslaGen3EVSESemanticCurrent(context.Background())
	encoded, marshalErr := json.Marshal(value)
	if err != nil || marshalErr != nil {
		t.Fatalf("semantic value/error = %s / %v / %v", encoded, err, marshalErr)
	}
	var public struct {
		Snapshot struct {
			Facts []struct {
				Key struct {
					FactID string `json:"fact_id"`
				} `json:"key"`
				Candidates []struct {
					Times struct {
						ReceivedAt struct {
							UnixNanoseconds string `json:"unix_nanoseconds"`
						} `json:"received_at"`
						ReceiptMonotonic struct {
							Nanoseconds string `json:"nanoseconds"`
						} `json:"receipt_monotonic"`
					} `json:"times"`
				} `json:"candidates"`
			} `json:"facts"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal(encoded, &public); err != nil {
		t.Fatal(err)
	}
	for _, fact := range public.Snapshot.Facts {
		if fact.Key.FactID != "evse.limit.configured_current" || len(fact.Candidates) != 1 {
			continue
		}
		got := fact.Candidates[0].Times
		if got.ReceivedAt.UnixNanoseconds != "1700000001000000123" || got.ReceiptMonotonic.Nanoseconds != "1000000000" {
			t.Fatalf("configured-current receipt refreshed by provisional sibling: %+v", got)
		}
		return
	}
	t.Fatalf("configured-current fact missing: %s", encoded)
}

func TestTeslaHSCRetainedOwnerEqualMonotonicLaterWallPublishesWithoutRefresh(t *testing.T) {
	owner := startTeslaEqualMonotonicFixture(t)
	value, err := owner.TeslaGen3EVSESemanticCurrent(context.Background())
	encoded, marshalErr := json.Marshal(value)
	if err != nil || marshalErr != nil {
		t.Fatalf("equal-monotonic semantic value/error = %s / %v / %v", encoded, err, marshalErr)
	}
	var public struct {
		Snapshot struct {
			EvaluatedAt struct {
				UnixNanoseconds string `json:"unix_nanoseconds"`
			} `json:"evaluated_at"`
			Facts []struct {
				Key struct {
					FactID string `json:"fact_id"`
				} `json:"key"`
				Candidates []struct {
					Times struct {
						ReceivedAt struct {
							UnixNanoseconds string `json:"unix_nanoseconds"`
						} `json:"received_at"`
						ReceiptMonotonic struct {
							Nanoseconds string `json:"nanoseconds"`
						} `json:"receipt_monotonic"`
					} `json:"times"`
				} `json:"candidates"`
			} `json:"facts"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal(encoded, &public); err != nil {
		t.Fatal(err)
	}
	if public.Snapshot.EvaluatedAt.UnixNanoseconds != "1700000003000000123" {
		t.Fatalf("aggregate evaluated wall = %s, want later provisional wall", public.Snapshot.EvaluatedAt.UnixNanoseconds)
	}
	wantReceipts := map[string]string{
		"evse.limit.configured_current": "1700000001000000123",
		"evse.limit.allocated_current":  "1700000003000000123",
	}
	for _, fact := range public.Snapshot.Facts {
		want, ok := wantReceipts[fact.Key.FactID]
		if !ok {
			continue
		}
		if len(fact.Candidates) != 1 || fact.Candidates[0].Times.ReceivedAt.UnixNanoseconds != want ||
			fact.Candidates[0].Times.ReceiptMonotonic.Nanoseconds != "10000000000" {
			t.Fatalf("%s receipt refreshed or changed: %#v", fact.Key.FactID, fact.Candidates)
		}
		delete(wantReceipts, fact.Key.FactID)
	}
	if len(wantReceipts) != 0 {
		t.Fatalf("equal-monotonic facts missing: %v", wantReceipts)
	}

	beforeEvidence := owner.RetainedEvidence()
	beforeSequence := owner.sequence
	regressed := teslaPersistentOutcome(t, 4, 20)
	regressed.Exchange.ReceiptMonotonic = 9 * time.Second
	if err := owner.IngestPersistent(context.Background(), regressed); err == nil {
		t.Fatal("true monotonic regression accepted")
	}
	after, err := owner.TeslaGen3EVSECurrentLimitV1(context.Background())
	if err != nil || after.Persistent == nil || after.Provisional == nil ||
		after.Persistent.MaxOutputCurrentAmps() != 16 || after.Provisional.LimitCurrentMaxAmps() != 12 ||
		owner.sequence != beforeSequence || !reflect.DeepEqual(owner.RetainedEvidence(), beforeEvidence) {
		t.Fatalf("true regression mutated retained state: %#v / %v", after, err)
	}
}

func TestTeslaHSCRetainedOwnerEqualMonotonicConcurrentReadsRemainStable(t *testing.T) {
	owner := startTeslaEqualMonotonicFixture(t)
	wantEvidence := owner.RetainedEvidence()
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 20 {
				if _, err := owner.TeslaGen3EVSECurrentLimitV1(context.Background()); err != nil {
					t.Error(err)
				}
				if _, err := owner.TeslaGen3EVSESemanticCurrent(context.Background()); err != nil {
					t.Error(err)
				}
				if _, ok := owner.SemanticEVSECurrentAt(time.Unix(1_700_000_010, 123).UTC()); !ok {
					t.Error("equal-monotonic Prometheus read unavailable")
				}
			}
		}()
	}
	wait.Wait()
	if got := owner.RetainedEvidence(); !reflect.DeepEqual(got, wantEvidence) {
		t.Fatal("equal-monotonic concurrent reads mutated retained evidence")
	}
}

func TestTeslaHSCRetainedOwnerFencesGenerationBeforeSuccessor(t *testing.T) {
	config := teslaRetainedConfig()
	owner := startTeslaRetainedFixture(t, config)
	if err := owner.IngestPersistent(context.Background(), teslaPersistentOutcome(t, 1, 16)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Fence(config.DriverGeneration, config.DriverGeneration+1); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.TeslaGen3EVSECurrentLimitV1(context.Background()); err == nil {
		t.Fatal("native read survived generation fence")
	}
	if _, err := owner.TeslaGen3EVSESemanticCurrent(context.Background()); err == nil {
		t.Fatal("semantic read survived generation fence")
	}
	if err := owner.IngestPersistent(context.Background(), teslaPersistentOutcome(t, 2, 20)); err == nil {
		t.Fatal("fenced owner accepted outcome")
	}

	next := config
	next.SourceEpoch = "epoch:tesla-wc3-b"
	next.DriverGeneration++
	successor, err := owner.Successor(next)
	if err != nil {
		t.Fatal(err)
	}
	if successor == nil {
		t.Fatal("successor unavailable")
	}
	if duplicate, err := owner.Successor(next); err == nil || duplicate != nil {
		t.Fatalf("repeated successor = %T, %v", duplicate, err)
	}
	wrong := next
	wrong.DriverGeneration++
	if _, err := owner.Successor(wrong); err == nil {
		t.Fatal("non-contiguous successor generation accepted")
	}
}

func TestTeslaHSCRetainedOwnerSuccessorReservationRetriesAfterConstructionFailure(t *testing.T) {
	config := teslaRetainedConfig()
	owner := startTeslaRetainedFixture(t, config)
	if err := owner.Fence(config.DriverGeneration, config.DriverGeneration+1); err != nil {
		t.Fatal(err)
	}
	next := config
	next.SourceEpoch = "epoch:tesla-wc3-b"
	next.DriverGeneration++
	invalid := next
	invalid.Enabled = false
	if successor, err := owner.Successor(invalid); err == nil || successor != nil {
		t.Fatalf("invalid successor construction = %T, %v", successor, err)
	}
	if successor, err := owner.Successor(next); err != nil || successor == nil {
		t.Fatalf("retry successor = %T, %v", successor, err)
	}
}

func TestTeslaHSCRetainedOwnerSuccessorReservationIsAtomic(t *testing.T) {
	config := teslaRetainedConfig()
	owner := startTeslaRetainedFixture(t, config)
	if err := owner.Fence(config.DriverGeneration, config.DriverGeneration+1); err != nil {
		t.Fatal(err)
	}
	next := config
	next.SourceEpoch = "epoch:tesla-wc3-b"
	next.DriverGeneration++

	start := make(chan struct{})
	var wait sync.WaitGroup
	var successes atomic.Int32
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			if successor, err := owner.Successor(next); err == nil && successor != nil {
				successes.Add(1)
			}
		}()
	}
	close(start)
	wait.Wait()
	if got := successes.Load(); got != 1 {
		t.Fatalf("successful concurrent successors = %d, want 1", got)
	}
}

func TestTeslaHSCRetainedOwnerConcurrentReadsPerformNoIngestionOrIO(t *testing.T) {
	owner := startTeslaRetainedFixture(t, teslaRetainedConfig())
	if err := owner.IngestPersistent(context.Background(), teslaPersistentOutcome(t, 1, 16)); err != nil {
		t.Fatal(err)
	}
	if err := owner.IngestProvisional(context.Background(), teslaProvisionalOutcome(t, 2, 16, 600, false, 32)); err != nil {
		t.Fatal(err)
	}
	wantEvidence := owner.RetainedEvidence()
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 20 {
				if _, err := owner.TeslaGen3EVSECurrentLimitV1(context.Background()); err != nil {
					t.Error(err)
				}
				if _, err := owner.TeslaGen3EVSESemanticCurrent(context.Background()); err != nil {
					t.Error(err)
				}
				if _, ok := owner.SemanticEVSECurrentAt(time.Unix(1_700_000_010, 123).UTC()); !ok {
					t.Error("detached Prometheus EVSE read unavailable")
				}
			}
		}()
	}
	wait.Wait()
	if got := owner.RetainedEvidence(); fmt.Sprint(got) != fmt.Sprint(wantEvidence) {
		t.Fatal("detached reads advanced retained evidence")
	}
}

func TestTeslaHSCRetainedOwnerComposesExistingReadOnlySurfaces(t *testing.T) {
	owner := startTeslaRetainedFixture(t, teslaRetainedConfig())
	if err := owner.IngestPersistent(context.Background(), teslaPersistentOutcome(t, 1, 16)); err != nil {
		t.Fatal(err)
	}
	provider := newGatewayModbusMCPProviderWithRuntimes(nil, nil, owner)
	if provider == nil {
		t.Fatal("Tesla-only provider was omitted")
	}
	if core, ok := provider.(interface{ ModbusV1CoreAvailable() bool }); !ok || core.ModbusV1CoreAvailable() {
		t.Fatalf("Tesla-only provider advertised unrelated Modbus core: %T", provider)
	}
	native, nativeOK := provider.(mcp.TeslaGen3EVSECurrentLimitV1Provider)
	semantic, semanticOK := provider.(mcp.TeslaGen3EVSESemanticProvider)
	if !nativeOK || !semanticOK {
		t.Fatalf("provider interfaces native=%t semantic=%t type=%T", nativeOK, semanticOK, provider)
	}
	if value, err := native.TeslaGen3EVSECurrentLimitV1(context.Background()); err != nil || value.Persistent == nil {
		t.Fatalf("native value/error = %#v / %v", value, err)
	}
	if _, err := semantic.TeslaGen3EVSESemanticCurrent(context.Background()); err != nil {
		t.Fatal(err)
	}
	if public, err := currentTeslaEVSEPublic(context.Background(), owner.cfg.AssetID, owner); err != nil || !json.Valid(public) {
		t.Fatalf("GraphQL publication/error = %s / %v", public, err)
	}
	if _, err := currentTeslaEVSEPublic(context.Background(), "asset:other", owner); err == nil {
		t.Fatal("wrong GraphQL asset was accepted")
	}
}

func TestTeslaHSCRetainedOwnerRegistrationMatrix(t *testing.T) {
	owner := startTeslaRetainedFixture(t, teslaRetainedConfig())
	if err := owner.IngestPersistent(context.Background(), teslaPersistentOutcome(t, 1, 16)); err != nil {
		t.Fatal(err)
	}
	newServer := func(t *testing.T, provider mcp.ModbusV1Provider) *mcp.Server {
		t.Helper()
		server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		mcp.RegisterModbusV1Tools(server, provider)
		return server
	}
	assertTools := func(t *testing.T, server *mcp.Server, required, forbidden []string) {
		t.Helper()
		tools := mcpToolsList(t, server.Handler())
		for _, name := range required {
			if !mcpToolsContain(tools, name) {
				t.Errorf("tools/list missing %q", name)
			}
		}
		for _, name := range forbidden {
			if mcpToolsContain(tools, name) {
				t.Errorf("tools/list exposed %q", name)
			}
		}
	}
	coreTools := []string{mcp.ModbusV1RawReadTool, mcp.ModbusV1ProfileObservationGetTool, mcp.SemanticV1PVCurrentGetTool, mcp.TeslaFC100SummaryV1GetTool, mcp.TeslaHSCV1StatusGetTool, mcp.TeslaWCVitalsV1GetTool}
	growattTools := []string{mcp.GrowattBMSRS485V202StatusGetTool, mcp.SemanticV1GrowattStorageCurrentGetTool}
	teslaTools := []string{mcp.TeslaGen3EVSECurrentLimitV1GetTool, mcp.SemanticV1EVSECurrentGetTool}

	teslaOnly := newServer(t, newGatewayModbusMCPProviderWithRuntimes(nil, nil, owner))
	assertTools(t, teslaOnly, teslaTools, append(append([]string(nil), coreTools...), growattTools...))
	mcpCallToolEnvelope(t, teslaOnly.Handler(), mcp.TeslaGen3EVSECurrentLimitV1GetTool, `{}`)
	mcpCallToolEnvelope(t, teslaOnly.Handler(), mcp.SemanticV1EVSECurrentGetTool, `{}`)

	growatt := startGrowattRuntimeWithFake(t, growattProductionConfig(), &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1})
	composite := newServer(t, newGatewayModbusMCPProviderWithRuntimes(nil, growatt, owner))
	assertTools(t, composite, append(append([]string(nil), teslaTools...), growattTools...), coreTools)
}

func TestTeslaHSCRetainedFlagsBindOnlyNonSendConfiguration(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	flags := flag.NewFlagSet("tesla-retained", flag.ContinueOnError)
	bindFlags(flags, &cfg)
	args := []string{
		"-tesla-gen3-hsc-retained-enabled=true", "-tesla-gen3-hsc-endpoint-id=endpoint-a",
		"-tesla-gen3-hsc-asset-id=asset:tesla-a", "-tesla-gen3-hsc-source-id=source:tesla-a",
		"-tesla-gen3-hsc-source-epoch=epoch:a", "-tesla-gen3-hsc-clock-epoch=clock:a",
		"-tesla-gen3-hsc-evse-id=evse-a", "-tesla-gen3-hsc-connector-id=connector-a",
		"-tesla-gen3-hsc-profile=wc3_24_44_3", "-tesla-gen3-hsc-driver-generation=9", "-tesla-gen3-hsc-node=0x10",
	}
	if err := flags.Parse(args); err != nil {
		t.Fatal(err)
	}
	want := ebusgateway.TeslaGen3HSCRetainedConfig{Enabled: true, EndpointID: "endpoint-a", AssetID: "asset:tesla-a", SourceID: "source:tesla-a", SourceEpoch: "epoch:a", ClockEpoch: "clock:a", EVSEID: "evse-a", ConnectorID: "connector-a", Profile: "wc3_24_44_3", DriverGeneration: 9, Node: 0x10}
	if !reflect.DeepEqual(cfg.ModbusTCPConfig.TeslaGen3HSC, want) {
		t.Fatalf("config = %#v, want %#v", cfg.ModbusTCPConfig.TeslaGen3HSC, want)
	}
	for _, forbidden := range []string{"serial", "request", "write", "activate", "credential", "authorization"} {
		if flags.Lookup("tesla-gen3-hsc-"+forbidden) != nil {
			t.Fatalf("forbidden Tesla retained flag exists: %s", forbidden)
		}
	}
}

func TestTeslaHSCRetainedOwnerSourceContainsNoSendOrActivationPath(t *testing.T) {
	source, err := os.ReadFile("tesla_hsc_retained_owner.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"OpenRTUSerial(", ".Exchange(", "WriteRTU(", "BuildTeslaFC100OperationRequest(", "TESLA\\x00", "PASS\\x00"} {
		if strings.Contains(string(source), forbidden) {
			t.Fatalf("production retained owner contains forbidden send/activation path %q", forbidden)
		}
	}
}

func teslaRetainedConfig() ebusgateway.TeslaGen3HSCRetainedConfig {
	return ebusgateway.TeslaGen3HSCRetainedConfig{
		Enabled: true, EndpointID: "tesla-hsc-public-a", AssetID: "asset:tesla-wc3-a", SourceID: "source:tesla-wc3-a",
		SourceEpoch: "epoch:tesla-wc3-a", ClockEpoch: "clock:tesla-wc3-a", EVSEID: "evse-a", ConnectorID: "connector-a",
		Profile: modbusreg.TeslaGen3CurrentLimitOperationVersion24443, DriverGeneration: 7, Node: 0x10,
	}
}

func startTeslaRetainedFixture(t *testing.T, config ebusgateway.TeslaGen3HSCRetainedConfig) *teslaHSCRetainedOwner {
	t.Helper()
	owner, err := startTeslaHSCRetainedOwner(config)
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func startTeslaEqualMonotonicFixture(t *testing.T) *teslaHSCRetainedOwner {
	t.Helper()
	owner := startTeslaRetainedFixture(t, teslaRetainedConfig())
	persistent := teslaPersistentOutcome(t, 1, 16)
	persistent.Exchange.ReceiptMonotonic = 10 * time.Second
	if err := owner.IngestPersistent(context.Background(), persistent); err != nil {
		t.Fatal(err)
	}
	provisional := teslaProvisionalOutcome(t, 2, 12, 600, false, 32)
	provisional.Set.ReceiptMonotonic = 10 * time.Second
	provisional.Readback.ReceiptMonotonic = 10 * time.Second
	if err := owner.IngestProvisional(context.Background(), provisional); err != nil {
		t.Fatal(err)
	}
	return owner
}

func teslaPersistentOutcome(t *testing.T, correlation uint64, amps uint32) TeslaGen3PersistentOutcome {
	t.Helper()
	body := []byte{0x0a, 0x02, 0x08, byte(amps)}
	return TeslaGen3PersistentOutcome{MaxOutputCurrentAmps: amps, Exchange: teslaExchange(t, modbusreg.TeslaFC100OperationWCConfigureSettings, body, body, correlation)}
}

func teslaProvisionalOutcome(t *testing.T, correlation uint64, amps, timeout uint32, inhibit bool, configured uint32) TeslaGen3ProvisionalOutcome {
	t.Helper()
	inner := append([]byte{0x08, byte(amps), 0x10}, encodeTeslaTestVarint(timeout)...)
	inner = append(inner, 0x18)
	if inhibit {
		inner = append(inner, 1)
	} else {
		inner = append(inner, 0)
	}
	setBody := append([]byte{0x0a, byte(len(inner))}, inner...)
	readbackBody := append(append([]byte(nil), setBody...), 0x10, byte(configured))
	return TeslaGen3ProvisionalOutcome{
		LimitCurrentMaxAmps: amps, LimitTimeoutSeconds: timeout, InhibitCharging: inhibit,
		Set:      teslaExchange(t, modbusreg.TeslaFC100OperationWCSetProvisional, setBody, nil, correlation),
		Readback: teslaExchange(t, modbusreg.TeslaFC100OperationWCGetProvisional, nil, readbackBody, correlation+1),
	}
}

func teslaExchange(t *testing.T, operation modbusreg.TeslaFC100Operation, requestBody, responseBody []byte, correlation uint64) TeslaGen3CompletedExchange {
	t.Helper()
	request, err := modbusreg.BuildTeslaFC100OperationRequest(modbusreg.TeslaGen3CurrentLimitOperationVersion24443, operation, requestBody)
	if err != nil {
		t.Fatal(err)
	}
	responsePayload := teslaTestTerminal(operation, responseBody)
	requestADU, err := modbus.EncodeRTUPrivateFunctionADU(0x10, request)
	if err != nil {
		t.Fatal(err)
	}
	response, err := modbus.NewPrivateFunctionRequest(request.FunctionCode(), responsePayload)
	if err != nil {
		t.Fatal(err)
	}
	responseADU, err := modbus.EncodeRTUPrivateFunctionADU(0x10, response)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000+int64(correlation), 123).UTC()
	return TeslaGen3CompletedExchange{
		EndpointID: "tesla-hsc-public-a", SourceID: "source:tesla-wc3-a", SourceEpoch: "epoch:tesla-wc3-a",
		DriverGeneration: 7, Node: 0x10, OperationVersion: modbusreg.TeslaGen3CurrentLimitOperationVersion24443,
		Operation: operation, CorrelationID: correlation, RequestPayload: request.Payload(), RequestADU: requestADU,
		ResponseFrames: []TeslaGen3CompletedFrame{{Payload: responsePayload, ADU: responseADU}},
		ReceiptWall:    now, ReceiptMonotonic: time.Duration(correlation) * time.Second, TerminalOutcome: "success",
	}
}

func teslaTestTerminal(operation modbusreg.TeslaFC100Operation, body []byte) []byte {
	tags := map[modbusreg.TeslaFC100Operation]byte{
		modbusreg.TeslaFC100OperationWCConfigureSettings: 8,
		modbusreg.TeslaFC100OperationWCSetProvisional:    26,
		modbusreg.TeslaFC100OperationWCGetProvisional:    28,
	}
	inner := append(encodeTeslaTestVarint(uint32(tags[operation])<<3|2), encodeTeslaTestVarint(uint32(len(body)))...)
	inner = append(inner, body...)
	message := append([]byte{6<<3 | 2}, encodeTeslaTestVarint(uint32(len(inner)))...)
	message = append(message, inner...)
	return append([]byte{byte(len(message))}, message...)
}

func encodeTeslaTestVarint(value uint32) []byte {
	var out []byte
	for value >= 0x80 {
		out = append(out, byte(value)|0x80)
		value >>= 7
	}
	return append(out, byte(value))
}

var _ mcp.TeslaGen3EVSECurrentLimitV1Provider = (*teslaHSCRetainedOwner)(nil)
var _ mcp.TeslaGen3EVSESemanticProvider = (*teslaHSCRetainedOwner)(nil)
var _ mcp.SemanticEVSEPrometheusProvider = (*teslaHSCRetainedOwner)(nil)
